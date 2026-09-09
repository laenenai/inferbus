// Package e2e's M6 milestone acceptance tests (Task 6), fake-engine-based
// and CI-safe (see live_embed_test.go for the separate, env-gated
// real-hardware counterpart these deliberately do not duplicate):
//
//   - TestM6EmbeddingsFullStack: a static-IAM gateway plus a worker running
//     testutil.FakeEngine serve POST /v1/embeddings end to end — the client
//     gets an OpenAI-shaped vector body back, and a kind="embed" usage
//     event with the right token count lands on METERING.
//   - TestM6EmbeddingsNamedResolutions: the headline M6 feature, proved end
//     to end. A kv-mode gateway resolves TWO aliases ("embed-hd",
//     "embed-compact") that both point at the SAME concrete model but carry
//     different operator-pinned `dimensions` params (cpkv.AliasEntry.Params
//     -> mergeParams). One worker, one model, one engine instance: each
//     alias's request must still reach the engine with its own dimensions
//     value, proving the override happens per-request rather than being
//     baked into the worker or model config.
package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/laenenai/inferbus/internal/cpkv"
	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/gateway"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
	"github.com/laenenai/inferbus/internal/worker"
)

// doEmbedRequest fires one embeddings request against srv for alias,
// returning the response (body already drained and closed).
func doEmbedRequest(t *testing.T, client *http.Client, srv *httptest.Server, key, alias string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/embeddings",
		strings.NewReader(`{"model":"`+alias+`","input":"the quick brown fox"}`))
	if err != nil {
		t.Fatalf("new embeddings request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("embeddings request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, body
}

// TestM6EmbeddingsFullStack: a static-IAM gateway (the M2 default, no
// control plane involved) plus a worker serving one embedding model
// through testutil.FakeEngine. POST /v1/embeddings returns the fake's
// vector body, and exactly one kind="embed" usage event reaches METERING
// carrying the fake's configured prompt-token count and zero completion
// tokens (embeddings never generate).
func TestM6EmbeddingsFullStack(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	const key = "e2e-m6-embed-key"
	const model = "embed-model"
	gw := gateway.New(nc, js, gateway.Config{
		RequestTimeout: 10 * time.Second,
		Keys:           []gateway.KeyConfig{{Key: key, Name: "k1", Org: "acme", Project: "default", Allow: []string{"embed"}}},
		Aliases:        map[string]string{"embed": model},
	})
	gwSrv := httptest.NewServer(gw.Routes())
	t.Cleanup(gwSrv.Close)

	fake := &testutil.FakeEngine{EmbedUsage: wire.Usage{PromptTokens: 5}}
	w := worker.New(nc, js, map[string]ibengine.Engine{model: fake}, worker.Config{
		WorkerID: "embed-worker", Models: []worker.ModelConfig{{Name: model, MaxInflight: 2}},
	})
	startWorker(t, w)

	// Subscribe before publishing so the usage event can't be missed (same
	// discipline as m3/m5's own metering subscriptions).
	usageSub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatalf("subscribe metering.usage.>: %v", err)
	}
	defer usageSub.Unsubscribe()

	resp, body := doEmbedRequest(t, http.DefaultClient, gwSrv, key, "embed")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("embeddings status = %d, want 200, body=%s", resp.StatusCode, body)
	}
	var parsed struct {
		Object string `json:"object"`
		Data   []struct {
			Object    string    `json:"object"`
			Embedding []float64 `json:"embedding"`
			Index     int       `json:"index"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("decode embeddings body: %v, body=%s", err, body)
	}
	if len(parsed.Data) == 0 || len(parsed.Data[0].Embedding) == 0 {
		t.Fatalf("embeddings body carries no vector: %s", body)
	}

	msg, err := usageSub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("expected a usage event: %v", err)
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		t.Fatalf("decode usage event: %v", err)
	}
	if ev.Kind != "embed" {
		t.Fatalf("usage event kind = %q, want %q", ev.Kind, "embed")
	}
	if ev.Status != "ok" {
		t.Fatalf("usage event status = %q, want %q", ev.Status, "ok")
	}
	if ev.PromptTokens != 5 || ev.CompletionTokens != 0 {
		t.Fatalf("usage event tokens = %+v, want prompt=5 completion=0", ev.Usage)
	}
}

// dimensionsRecordingEngine is an ibengine.Engine whose Embed records the
// "dimensions" field of every request body it receives, in call order,
// so a test can prove that two different aliases resolving to this same
// engine instance (i.e. the same worker, the same concrete model) each
// carried their own operator-pinned value through mergeParams rather than
// sharing one baked-in config. Chat/ChatStream are never expected to be
// called by these tests; they return ErrUnsupported so a wiring mistake
// that accidentally routed a chat request here fails loudly instead of
// silently.
type dimensionsRecordingEngine struct {
	mu   sync.Mutex
	seen []string // dimensions field of each Embed call's body, in call order
}

func (e *dimensionsRecordingEngine) Chat(context.Context, string, json.RawMessage) (json.RawMessage, wire.Usage, error) {
	return nil, wire.Usage{}, ibengine.ErrUnsupported
}

func (e *dimensionsRecordingEngine) ChatStream(context.Context, string, json.RawMessage, func(json.RawMessage) error) (wire.Usage, error) {
	return wire.Usage{}, ibengine.ErrUnsupported
}

func (e *dimensionsRecordingEngine) Embed(_ context.Context, _ string, body json.RawMessage) (json.RawMessage, wire.Usage, error) {
	var probe struct {
		Dimensions json.Number `json:"dimensions"`
	}
	_ = json.Unmarshal(body, &probe)
	e.mu.Lock()
	e.seen = append(e.seen, probe.Dimensions.String())
	e.mu.Unlock()
	return json.RawMessage(`{"object":"list","data":[{"object":"embedding","embedding":[0.1,0.2],"index":0}]}`),
		wire.Usage{PromptTokens: 3}, nil
}

func (e *dimensionsRecordingEngine) callsSeen() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.seen...)
}

// TestM6EmbeddingsNamedResolutions is the headline M6 feature proved end to
// end: a kv-mode gateway over a seeded ALIASES bucket with TWO aliases —
// "embed-hd" ({"dimensions":"1024"}) and "embed-compact"
// ({"dimensions":"256"}) — both targeting the SAME concrete model, served
// by ONE worker running ONE dimensionsRecordingEngine instance for that
// model. Each alias's request must reach the engine carrying its own
// dimensions value: proof that mergeParams applies the alias's params
// per-request (gateway.go's dispatch, Resolution.Params from
// cpkv.AliasEntry.Params) rather than the worker/engine having any
// per-alias configuration of its own — the worker and engine are dumb
// about aliases by design (see dispatch's own doc comment).
func TestM6EmbeddingsNamedResolutions(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	const model = "embed-named-model"

	// Seed ALIASES directly (same helpers live_embed_test.go uses) rather
	// than assembling the full control-plane stack: this test's own
	// concern is the gateway/worker/engine data-plane behavior, not the
	// control-plane projector pipeline that m3_controlplane_test.go
	// already covers.
	aliasesKV := liveKV(t, js, cpkv.BucketAliases)
	livePut(t, aliasesKV, cpkv.GlobalScope+"/embed-hd", cpkv.AliasEntry{
		Target: model, Params: map[string]string{"dimensions": "1024"},
	})
	livePut(t, aliasesKV, cpkv.GlobalScope+"/embed-compact", cpkv.AliasEntry{
		Target: model, Params: map[string]string{"dimensions": "256"},
	})

	const plaintext = "e2e-m6-named-key"
	keysKV := liveKV(t, js, cpkv.BucketKeys)
	livePut(t, keysKV, cpkv.HashKey(plaintext), cpkv.KeyEntry{
		Id: "key-m6-named", Name: "named", Org: "acme", Project: "prod",
		Allow: []string{"embed-hd", "embed-compact"},
	})

	kviam, err := gateway.NewKVIAM(ctx, js)
	if err != nil {
		t.Fatalf("kviam: %v", err)
	}
	select {
	case <-kviam.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("KVIAM never became ready")
	}

	rec := &dimensionsRecordingEngine{}
	w := worker.New(nc, js, map[string]ibengine.Engine{model: rec}, worker.Config{
		WorkerID: "named-resolutions-worker", Models: []worker.ModelConfig{{Name: model, MaxInflight: 2}},
	})
	startWorker(t, w)

	g := gateway.NewWithIAM(nc, js, gateway.Config{
		RequestTimeout: 10 * time.Second,
		IAM:            gateway.IAMConfig{Mode: "kv"},
	}, kviam)
	srv := httptest.NewServer(g.Routes())
	defer srv.Close()

	// Each request is a synchronous HTTP round trip, so the engine's
	// call-order matches issue order deterministically — no polling needed
	// to correlate a response with which alias produced it.
	respHD, bodyHD := doEmbedRequest(t, http.DefaultClient, srv, plaintext, "embed-hd")
	if respHD.StatusCode != http.StatusOK {
		t.Fatalf("embed-hd status = %d, want 200, body=%s", respHD.StatusCode, bodyHD)
	}
	respCompact, bodyCompact := doEmbedRequest(t, http.DefaultClient, srv, plaintext, "embed-compact")
	if respCompact.StatusCode != http.StatusOK {
		t.Fatalf("embed-compact status = %d, want 200, body=%s", respCompact.StatusCode, bodyCompact)
	}

	seen := rec.callsSeen()
	if len(seen) != 2 {
		t.Fatalf("engine saw %d Embed calls, want 2 (one worker, one model, two named resolutions): %v", len(seen), seen)
	}
	if seen[0] != "1024" {
		t.Errorf("embed-hd: engine received dimensions=%q, want %q", seen[0], "1024")
	}
	if seen[1] != "256" {
		t.Errorf("embed-compact: engine received dimensions=%q, want %q", seen[1], "256")
	}
}
