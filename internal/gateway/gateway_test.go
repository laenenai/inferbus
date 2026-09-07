package gateway_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"net/http"

	"github.com/nats-io/nats.go"

	ibengine "github.com/infbus/infbus/internal/engine"
	"github.com/infbus/infbus/internal/gateway"
	"github.com/infbus/infbus/internal/testutil"
	"github.com/infbus/infbus/internal/wire"
	"github.com/infbus/infbus/internal/worker"
)

// startStack runs embedded NATS + a worker with a fake engine + the gateway
// mux. It also returns the shared *nats.Conn so tests can observe the
// data-plane traffic directly (cancel/usage subjects).
func startStack(t *testing.T, eng ibengine.Engine) (*httptest.Server, *nats.Conn) {
	t.Helper()
	nc, js := testutil.RunNATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := worker.New(nc, js, map[string]ibengine.Engine{"llama-70b": eng}, worker.Config{
		WorkerID: "w1",
		Models:   []worker.ModelConfig{{Name: "llama-70b", MaxInflight: 2}},
	})
	ready := make(chan struct{})
	go func() { _ = w.RunReady(ctx, ready) }()
	<-ready
	g := gateway.New(nc, js, gateway.Config{
		RequestTimeout: 30 * time.Second,
		Keys: []gateway.KeyConfig{{
			Key: "ib_test_123", Name: "t", Org: "acme", Project: "prod",
			Allow: []string{"smart"},
		}},
		Aliases: map[string]string{"smart": "llama-70b", "fast": "qwen-7b"},
	})
	srv := httptest.NewServer(g.Routes())
	t.Cleanup(srv.Close)
	return srv, nc
}

func post(t *testing.T, srv *httptest.Server, key, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(body))
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

func TestUnauthorized(t *testing.T) {
	srv, _ := startStack(t, &testutil.FakeEngine{})
	resp := post(t, srv, "", `{"model":"smart","messages":[]}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestUnknownAlias404(t *testing.T) {
	srv, _ := startStack(t, &testutil.FakeEngine{})
	resp := post(t, srv, "ib_test_123", `{"model":"llama-70b","messages":[]}`) // concrete name, not alias
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestForbiddenAlias403(t *testing.T) {
	srv, _ := startStack(t, &testutil.FakeEngine{})
	resp := post(t, srv, "ib_test_123", `{"model":"fast","messages":[]}`) // exists, not allowed
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestModelsListsAllowedAliases(t *testing.T) {
	srv, _ := startStack(t, &testutil.FakeEngine{})
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer ib_test_123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Data []struct{ ID string `json:"id"` } `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Data) != 1 || out.Data[0].ID != "smart" {
		t.Fatalf("models = %+v", out.Data)
	}
}

func TestStreamEndToEnd(t *testing.T) {
	eng := &testutil.FakeEngine{
		Chunks:     []string{`{"choices":[{"delta":{"content":"he"}}]}`, `{"choices":[{"delta":{"content":"y"}}]}`},
		FinalUsage: wire.Usage{PromptTokens: 4, CompletionTokens: 2},
	}
	srv, _ := startStack(t, eng)
	resp := post(t, srv, "ib_test_123", `{"model":"smart","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	var lines []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data: ") {
			lines = append(lines, strings.TrimPrefix(sc.Text(), "data: "))
		}
	}
	if len(lines) != 3 || lines[2] != "[DONE]" { // 2 chunks + [DONE]
		t.Fatalf("lines = %v", lines)
	}
	if !strings.Contains(lines[0], "he") {
		t.Fatalf("first chunk = %q", lines[0])
	}
}

func TestNonStreamEndToEnd(t *testing.T) {
	srv, _ := startStack(t, &testutil.FakeEngine{FinalUsage: wire.Usage{PromptTokens: 3, CompletionTokens: 1}})
	resp := post(t, srv, "ib_test_123", `{"model":"smart","messages":[{"role":"user","content":"hi"}]}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out struct{ Object string `json:"object"` }
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Object != "chat.completion" {
		t.Fatalf("object = %q", out.Object)
	}
}

func TestUpstreamErrorMapsToStatus(t *testing.T) {
	srv, _ := startStack(t, &testutil.FakeEngine{
		Chunks: []string{`{"c":0}`, `{"c":1}`},
		Err:    &ibengine.Error{Code: "upstream_error", Message: "boom", HTTPStatus: 502},
	})
	resp := post(t, srv, "ib_test_123", `{"model":"smart","messages":[]}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

// TestClientDisconnectCancels exercises the KNOWN GAP mitigation: when the
// client hangs up mid-stream, the gateway must publish a cancel frame (for
// a worker that already picked the request up) AND best-effort delete the
// queued message. It observes both effects directly on the NATS conn the
// gateway itself uses, rather than inferring them indirectly.
func TestClientDisconnectCancels(t *testing.T) {
	eng := &testutil.FakeEngine{
		Chunks:     []string{"{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}"},
		Delay:      150 * time.Millisecond,
		FinalUsage: wire.Usage{PromptTokens: 5, CompletionTokens: 8},
	}
	srv, nc := startStack(t, eng)

	cancelSub, err := nc.SubscribeSync("inference.cancel.>")
	if err != nil {
		t.Fatal(err)
	}
	defer cancelSub.Unsubscribe()
	usageSub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	defer usageSub.Unsubscribe()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"smart","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer ib_test_123")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() || !strings.HasPrefix(sc.Text(), "data: ") {
		t.Fatalf("expected a first data line, got %q (err=%v)", sc.Text(), sc.Err())
	}

	cancel() // simulate client disconnect mid-stream

	cancelMsg, err := cancelSub.NextMsg(20 * time.Second)
	if err != nil {
		t.Fatalf("expected a cancel message: %v", err)
	}
	if !strings.HasPrefix(cancelMsg.Subject, "inference.cancel.") {
		t.Fatalf("unexpected cancel subject %q", cancelMsg.Subject)
	}

	var usage wire.UsageEvent
	deadline := time.Now().Add(20 * time.Second)
	for {
		m, err := usageSub.NextMsg(time.Until(deadline))
		if err != nil {
			t.Fatalf("expected a canceled usage event: %v", err)
		}
		if err := json.Unmarshal(m.Data, &usage); err != nil {
			t.Fatal(err)
		}
		if usage.Status == "canceled" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for canceled usage event, last = %+v", usage)
		}
	}
	if usage.Status != "canceled" {
		t.Fatalf("usage.Status = %q", usage.Status)
	}
	if !usage.Estimated {
		t.Fatalf("usage.Estimated = %v, want true", usage.Estimated)
	}
}
