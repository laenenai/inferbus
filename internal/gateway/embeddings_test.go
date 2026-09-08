package gateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/cpkv"
	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/gateway"
	"github.com/laenenai/inferbus/internal/relay"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
	"github.com/laenenai/inferbus/internal/worker"
)

// postEmbeddings is the /v1/embeddings twin of post: same auth/content-type
// handling, different path. Kept separate rather than parameterizing post so
// the existing chat tests' call sites stay untouched.
func postEmbeddings(t *testing.T, srv *httptest.Server, key, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/embeddings", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// errorType decodes an OpenAI-shaped error envelope and returns error.type.
func errorType(t *testing.T, resp *http.Response) string {
	t.Helper()
	var out struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	return out.Error.Type
}

// TestEmbeddingsHappyPath: a request for an allowed alias must come back
// with the worker's embeddings JSON verbatim, and the message the gateway
// published must carry Ib-Kind: embed — that header is the ONLY thing that
// makes the worker call Engine.Embed instead of Engine.Chat.
//
// The published body's "model" field stays the client's alias: the gateway
// deliberately does not rewrite it (engines substitute their own concrete
// upstream name), and "model" is a reserved alias param for the same
// reason — see params.go's reservedParams and
// TestKVModeAliasParamsReachPublishedBody.
func TestEmbeddingsHappyPath(t *testing.T) {
	want := `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.25,0.5]}]}`
	srv, nc := startStack(t, &testutil.FakeEngine{
		EmbedResponse: json.RawMessage(want),
		EmbedUsage:    wire.Usage{PromptTokens: 5},
	})

	sub, err := nc.SubscribeSync("inference.req.>")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe() //nolint:errcheck // test cleanup

	resp := postEmbeddings(t, srv, "ib_test_123", `{"model":"smart","input":"hello"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != want {
		t.Fatalf("body = %s, want the engine's embeddings JSON %s", got, want)
	}

	m, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("expected a published inference request: %v", err)
	}
	if k := m.Header.Get(wire.HdrKind); k != "embed" {
		t.Fatalf("%s = %q, want %q", wire.HdrKind, k, "embed")
	}
	if got := m.Subject; got != wire.ReqSubject("llama-70b") {
		t.Fatalf("subject = %q, want the concrete model's %q", got, wire.ReqSubject("llama-70b"))
	}
	var published map[string]any
	if err := json.Unmarshal(m.Data, &published); err != nil {
		t.Fatalf("published body is not JSON: %v (%s)", err, m.Data)
	}
	if published["model"] != "smart" {
		t.Fatalf("published model = %v, want the client's alias untouched", published["model"])
	}
	if published["input"] != "hello" {
		t.Fatalf("published input = %v, want the client's own field preserved", published["input"])
	}
}

// TestEmbeddingsRejectsStream: /v1/embeddings has no streaming response
// shape at all (the worker answers an embed request with exactly one
// KindResult frame regardless of the body's stream flag), so a client that
// asks for one must be told the request is invalid — never silently served
// a non-streaming body, and never published.
func TestEmbeddingsRejectsStream(t *testing.T) {
	srv, nc := startStack(t, &testutil.FakeEngine{})

	sub, err := nc.SubscribeSync("inference.req.>")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe() //nolint:errcheck // test cleanup

	resp := postEmbeddings(t, srv, "ib_test_123", `{"model":"smart","input":"hi","stream":true}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if typ := errorType(t, resp); typ != "invalid_request_error" {
		t.Fatalf("error.type = %q, want invalid_request_error", typ)
	}
	if m, err := sub.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatalf("a rejected request was still published: %s", m.Data)
	}
}

func TestEmbeddingsUnauthorized(t *testing.T) {
	srv, _ := startStack(t, &testutil.FakeEngine{})
	resp := postEmbeddings(t, srv, "", `{"model":"smart","input":"hi"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if typ := errorType(t, resp); typ != "invalid_api_key" {
		t.Fatalf("error.type = %q, want invalid_api_key", typ)
	}
}

func TestEmbeddingsNotAllowed(t *testing.T) {
	srv, _ := startStack(t, &testutil.FakeEngine{})
	resp := postEmbeddings(t, srv, "ib_test_123", `{"model":"fast","input":"hi"}`) // exists, not allowed
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	if typ := errorType(t, resp); typ != "model_forbidden" {
		t.Fatalf("error.type = %q, want model_forbidden", typ)
	}
}

func TestEmbeddingsUnknownAlias(t *testing.T) {
	srv, _ := startStack(t, &testutil.FakeEngine{})
	resp := postEmbeddings(t, srv, "ib_test_123", `{"model":"llama-70b","input":"hi"}`) // concrete name, not an alias
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if typ := errorType(t, resp); typ != "model_not_found" {
		t.Fatalf("error.type = %q, want model_not_found", typ)
	}
}

// TestEmbeddingsBudgetExceeded mirrors the chat handler's Task 7 contract on
// the embeddings route: a kv-mode key whose BUDGETS entry says Exceeded must
// get a 402 budget_exhausted, and the request must never reach the data
// plane.
func TestEmbeddingsBudgetExceeded(t *testing.T) {
	nc, js := testutil.RunNATS(t)

	keysKV := createBucket(t, js, cpkv.BucketKeys)
	aliasesKV := createBucket(t, js, cpkv.BucketAliases)
	budgetsKV := createBucket(t, js, cpkv.BucketBudgets)
	putAliasEntry(t, aliasesKV, "_global/embed-fast", cpkv.AliasEntry{Target: "bge-small"})

	const plaintext = "ib_test_kv_embed_budget"
	putKeyEntry(t, keysKV, cpkv.HashKey(plaintext), cpkv.KeyEntry{
		Id: "key-embed-budget-1", Name: "budget-key", Org: "acme", Project: "prod",
		Allow: []string{"embed-fast"},
	})
	putBudgetEntry(t, budgetsKV, "key-embed-budget-1", cpkv.BudgetEntry{
		Used: 1000, Budget: 500, Exceeded: true, Month: "2026-09",
	})

	kviam := newTestKVIAM(t, js)
	pollUntil(t, 5*time.Second, func() bool { return kviam.BudgetExceeded("key-embed-budget-1") })

	g := gateway.NewWithIAM(nc, js, gateway.Config{
		RequestTimeout: 30 * time.Second,
		IAM:            gateway.IAMConfig{Mode: "kv"},
	}, kviam)
	srv := httptest.NewServer(g.Routes())
	t.Cleanup(srv.Close)

	sub, err := nc.SubscribeSync("inference.req.>")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe() //nolint:errcheck // test cleanup

	resp := postEmbeddings(t, srv, plaintext, `{"model":"embed-fast","input":"hi"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPaymentRequired {
		t.Fatalf("status = %d, want 402", resp.StatusCode)
	}
	if typ := errorType(t, resp); typ != "budget_exhausted" {
		t.Fatalf("error.type = %q, want budget_exhausted", typ)
	}
	if m, err := sub.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatalf("an over-budget request was still published: %s", m.Data)
	}
}

// TestEmbeddingsAdmission: admission control gates /v1/embeddings on the
// same concrete-model backlog the chat route uses, answering 429 with a
// Retry-After. The stack here deliberately has no worker — an admitted
// request would fall out as a 504 on the tiny RequestTimeout, which is
// still not a 429, so the assertion cannot pass by accident.
func TestEmbeddingsAdmission(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	if err := wire.EnsureStreams(context.Background(), js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}
	// The durable a worker for this model would create, so consumer Info()
	// reports a real backlog rather than erroring (admission fails open).
	if _, err := js.CreateOrUpdateConsumer(context.Background(), wire.StreamInference, jetstream.ConsumerConfig{
		Durable:       wire.Durable("bge-small"),
		FilterSubject: wire.ReqSubject("bge-small"),
		AckPolicy:     jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	for i := range 3 {
		if _, err := relay.Publish(context.Background(), js, relay.Request{
			Model: "bge-small", Org: "acme", Project: "prod", KeyID: "t",
			Alias: "embed-fast", ReqID: fmt.Sprintf("backlog-%d", i),
			Kind: "embed", Deadline: time.Now().Add(time.Minute), Body: []byte(`{}`),
		}); err != nil {
			t.Fatalf("publish backlog %d: %v", i, err)
		}
	}

	g := gateway.New(nc, js, gateway.Config{
		RequestTimeout: 300 * time.Millisecond,
		Keys: []gateway.KeyConfig{{
			Key: "ib_test_123", Name: "t", Org: "acme", Project: "prod",
			Allow: []string{"embed-fast"},
		}},
		Aliases:   map[string]string{"embed-fast": "bge-small"},
		Admission: gateway.AdmissionConfig{MaxBacklog: 2, RetryAfterSeconds: 7},
	})
	srv := httptest.NewServer(g.Routes())
	t.Cleanup(srv.Close)

	resp := postEmbeddings(t, srv, "ib_test_123", `{"model":"embed-fast","input":"hi"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "7" {
		t.Fatalf("Retry-After = %q, want %q", got, "7")
	}
	if typ := errorType(t, resp); typ != "overloaded" {
		t.Fatalf("error.type = %q, want overloaded", typ)
	}
}

// TestEmbeddingsAliasParams: the whole point of embeddings alias params —
// an operator pinning "dimensions" on the alias must have it reach the
// worker, since the worker and engines are deliberately dumb about aliases.
func TestEmbeddingsAliasParams(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	w := worker.New(nc, js, map[string]ibengine.Engine{"bge-small": &testutil.FakeEngine{}}, worker.Config{
		WorkerID: "w1",
		Models:   []worker.ModelConfig{{Name: "bge-small", MaxInflight: 2}},
	})
	ready := make(chan struct{})
	go func() { _ = w.RunReady(ctx, ready) }()
	<-ready

	keysKV := createBucket(t, js, cpkv.BucketKeys)
	aliasesKV := createBucket(t, js, cpkv.BucketAliases)
	putAliasEntry(t, aliasesKV, "_global/embed-fast", cpkv.AliasEntry{
		Target: "bge-small",
		Params: map[string]string{"dimensions": "256"},
	})

	const plaintext = "ib_test_kv_embed_params"
	putKeyEntry(t, keysKV, cpkv.HashKey(plaintext), cpkv.KeyEntry{
		Id: "key-embed-params-1", Name: "params-key", Org: "acme", Project: "prod",
		Allow: []string{"embed-fast"},
	})

	kviam := newTestKVIAM(t, js)
	g := gateway.NewWithIAM(nc, js, gateway.Config{
		RequestTimeout: 30 * time.Second,
		IAM:            gateway.IAMConfig{Mode: "kv"},
	}, kviam)
	srv := httptest.NewServer(g.Routes())
	t.Cleanup(srv.Close)

	sub, err := nc.SubscribeSync("inference.req.>")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe() //nolint:errcheck // test cleanup

	resp := postEmbeddings(t, srv, plaintext, `{"model":"embed-fast","input":"hi","dimensions":9}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	m, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("expected a published inference request: %v", err)
	}
	if k := m.Header.Get(wire.HdrKind); k != "embed" {
		t.Fatalf("%s = %q, want %q", wire.HdrKind, k, "embed")
	}
	var published map[string]any
	if err := json.Unmarshal(m.Data, &published); err != nil {
		t.Fatalf("published body is not a JSON object: %v (%s)", err, m.Data)
	}
	if published["dimensions"] != float64(256) {
		t.Fatalf("published dimensions = %v (%T), want the alias's 256 as a JSON number",
			published["dimensions"], published["dimensions"])
	}
	if published["input"] != "hi" {
		t.Fatalf("merge dropped the client's own fields: %s", m.Data)
	}
}

// TestEmbeddingsMetricsRoute: the route must be instrumented under the
// bounded "embeddings" label routeLabel already reserves — never a raw path,
// and never folded into "other".
func TestEmbeddingsMetricsRoute(t *testing.T) {
	srv, _ := startStack(t, &testutil.FakeEngine{})

	resp := postEmbeddings(t, srv, "ib_test_123", `{"model":"smart","input":"hi"}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	mresp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer mresp.Body.Close()
	body, err := io.ReadAll(mresp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `inferbus_requests_total{code="200",route="embeddings"}`) {
		t.Fatalf("metrics body missing inferbus_requests_total for route=\"embeddings\":\n%s", body)
	}
}
