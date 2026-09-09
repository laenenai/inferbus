package e2e

// Opportunistic live-hardware validation: drives the embeddings path and the
// named-resolutions feature against a REAL OpenAI-compatible embedding
// server (vLLM, Ollama, TEI, ...). Skipped unless both INFERBUS_LIVE_EMBED
// (base URL) and INFERBUS_LIVE_EMBED_MODEL (the engine's own model id) are
// set, so CI never depends on hardware.
//
// This test earns its keep: it is the only one that can catch a request
// reaching the engine with the wrong model name, because fake engines ignore
// the model field entirely. It caught exactly that — Embed shipping without
// the model rewrite Chat performs — against a real vLLM serving
// Qwen3-Embedding-8B, after the whole fake-based suite went green.
//
//	INFERBUS_LIVE_EMBED=http://host:8002 \
//	INFERBUS_LIVE_EMBED_MODEL=qwen3-embedding-8b \
//	go test ./internal/e2e/ -run TestLiveEmbeddings -v

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/cpkv"
	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/engine/openaihttp"
	"github.com/laenenai/inferbus/internal/gateway"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
	"github.com/laenenai/inferbus/internal/worker"
)

func liveKV(t *testing.T, js jetstream.JetStream, bucket string) jetstream.KeyValue {
	t.Helper()
	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}
	return kv
}

func livePut(t *testing.T, kv jetstream.KeyValue, key string, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	if _, err := kv.Put(context.Background(), key, b); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func liveEmbed(t *testing.T, srv *httptest.Server, key, model string) int {
	t.Helper()
	body := `{"model":"` + model + `","input":"the quick brown fox"}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/embeddings", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 90 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("embeddings request for %s: %v", model, err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: status %d, body=%s", model, resp.StatusCode, raw)
	}
	var parsed struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Usage struct {
			PromptTokens int `json:"prompt_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil || len(parsed.Data) == 0 {
		t.Fatalf("%s: unparseable response: %v body=%s", model, err, raw)
	}
	t.Logf("alias %-14s → vector len %d, prompt_tokens %d", model, len(parsed.Data[0].Embedding), parsed.Usage.PromptTokens)
	return len(parsed.Data[0].Embedding)
}

func TestLiveEmbeddingsNamedResolutions(t *testing.T) {
	engineURL := os.Getenv("INFERBUS_LIVE_EMBED")
	model := os.Getenv("INFERBUS_LIVE_EMBED_MODEL")
	if engineURL == "" || model == "" {
		t.Skip("INFERBUS_LIVE_EMBED / INFERBUS_LIVE_EMBED_MODEL not set")
	}

	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	// Three aliases over ONE real model: native (no params) plus two
	// operator-pinned "resolutions".
	aliasesKV := liveKV(t, js, cpkv.BucketAliases)
	livePut(t, aliasesKV, cpkv.GlobalScope+"/embed-native", cpkv.AliasEntry{Target: model})
	livePut(t, aliasesKV, cpkv.GlobalScope+"/embed-hd", cpkv.AliasEntry{Target: model, Params: map[string]string{"dimensions": "1024"}})
	livePut(t, aliasesKV, cpkv.GlobalScope+"/embed-compact", cpkv.AliasEntry{Target: model, Params: map[string]string{"dimensions": "256"}})

	const plaintext = "ib_live_embed_key"
	keysKV := liveKV(t, js, cpkv.BucketKeys)
	livePut(t, keysKV, cpkv.HashKey(plaintext), cpkv.KeyEntry{
		Id: "key-live-embed", Name: "live", Org: "acme", Project: "prod",
		Allow: []string{"embed-native", "embed-hd", "embed-compact"},
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

	cfg := worker.Config{
		WorkerID: "live-embed-worker",
		Models:   []worker.ModelConfig{{Name: model, Engine: "openai_http", URL: engineURL, MaxInflight: 2}},
	}
	cfg.Defaults()
	engines := map[string]ibengine.Engine{model: openaihttp.New(engineURL, nil)}
	w := worker.New(nc, js, engines, cfg)
	stop := startWorker(t, w)
	defer stop()

	g := gateway.NewWithIAM(nc, js, gateway.Config{
		RequestTimeout: 90 * time.Second,
		IAM:            gateway.IAMConfig{Mode: "kv"},
	}, kviam)
	srv := httptest.NewServer(g.Routes())
	defer srv.Close()

	native := liveEmbed(t, srv, plaintext, "embed-native")
	hd := liveEmbed(t, srv, plaintext, "embed-hd")
	compact := liveEmbed(t, srv, plaintext, "embed-compact")

	if hd != 1024 {
		t.Errorf("embed-hd returned %d dimensions, want 1024", hd)
	}
	if compact != 256 {
		t.Errorf("embed-compact returned %d dimensions, want 256", compact)
	}
	if native == hd || native == compact {
		t.Errorf("native (%d) should differ from the pinned resolutions (%d, %d)", native, hd, compact)
	}
}
