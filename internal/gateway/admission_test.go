package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/relay"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
)

// startAdmissionStack runs embedded NATS with the real INFERENCE stream and
// a gateway with *no worker* behind it: an admitted request therefore never
// gets an answer and falls out via the (deliberately tiny) RequestTimeout as
// a 504, which is exactly what these tests want — every assertion here is
// about 429-vs-not-429, never about a successful completion.
func startAdmissionStack(t *testing.T, adm AdmissionConfig) (*httptest.Server, jetstream.JetStream, *Gateway) {
	t.Helper()
	nc, js := testutil.RunNATS(t)
	if err := wire.EnsureStreams(context.Background(), js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}
	g := New(nc, js, Config{
		RequestTimeout: 300 * time.Millisecond,
		Keys: []KeyConfig{{
			Key: "ib_test_123", Name: "t", Org: "acme", Project: "prod",
			Allow: []string{"a1", "a2"},
		}},
		Aliases:   map[string]string{"a1": "m1", "a2": "m2"},
		Admission: adm,
	})
	srv := httptest.NewServer(g.Routes())
	t.Cleanup(srv.Close)
	return srv, js, g
}

// ensureDurable creates the same durable consumer a worker for model would
// create, so consumer Info() reports a real NumPending for it.
func ensureDurable(t *testing.T, js jetstream.JetStream, model string) {
	t.Helper()
	_, err := js.CreateOrUpdateConsumer(context.Background(), wire.StreamInference, jetstream.ConsumerConfig{
		Durable:       wire.Durable(model),
		FilterSubject: wire.ReqSubject(model),
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("create consumer for %s: %v", model, err)
	}
}

// fillQueue publishes n requests for model that nothing will ever consume.
func fillQueue(t *testing.T, js jetstream.JetStream, model string, n int) {
	t.Helper()
	for i := range n {
		_, err := relay.Publish(context.Background(), js, relay.Request{
			Model: model, Org: "acme", Project: "prod", KeyID: "t",
			Alias: model, ReqID: fmt.Sprintf("backlog-%s-%d", model, i),
			Kind: "chat", Deadline: time.Now().Add(time.Minute), Body: []byte(`{}`),
		})
		if err != nil {
			t.Fatalf("publish backlog %d: %v", i, err)
		}
	}
}

func postChat(t *testing.T, srv *httptest.Server, alias string) *http.Response {
	t.Helper()
	body := fmt.Sprintf(`{"model":%q,"messages":[]}`, alias)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer ib_test_123")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

// TestAdmissionDisabledByDefault: a config with no admission block builds no
// checker at all (nil = admit always, zero overhead) and a deep queue does
// not turn into a 429.
func TestAdmissionDisabledByDefault(t *testing.T) {
	srv, js, g := startAdmissionStack(t, AdmissionConfig{})
	if g.admission != nil {
		t.Fatalf("admission = %+v, want nil when max_backlog is unset", g.admission)
	}
	ensureDurable(t, js, "m1")
	fillQueue(t, js, "m1", 5)

	resp := postChat(t, srv, "a1")
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		t.Fatalf("status = 429 with admission disabled")
	}
}

// TestAdmissionRejectsOnBacklog: backlog at/over the limit is a 429 carrying
// the configured Retry-After and the "overloaded" OpenAI error type.
func TestAdmissionRejectsOnBacklog(t *testing.T) {
	srv, js, _ := startAdmissionStack(t, AdmissionConfig{MaxBacklog: 2, RetryAfterSeconds: 7})
	ensureDurable(t, js, "m1")
	fillQueue(t, js, "m1", 3)

	resp := postChat(t, srv, "a1")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "7" {
		t.Errorf("Retry-After = %q, want %q", got, "7")
	}
	var out struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if out.Error.Type != "overloaded" {
		t.Errorf("error.type = %q, want %q", out.Error.Type, "overloaded")
	}
}

// TestAdmissionAllowsUnderLimit: a backlog below the limit is admitted (the
// request then times out with no worker, which is fine — only the absence of
// a 429 is under test).
func TestAdmissionAllowsUnderLimit(t *testing.T) {
	srv, js, _ := startAdmissionStack(t, AdmissionConfig{MaxBacklog: 2})
	ensureDurable(t, js, "m1")
	fillQueue(t, js, "m1", 1)

	resp := postChat(t, srv, "a1")
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		t.Fatalf("status = 429 with backlog 1 under limit 2")
	}
}

// TestAdmissionMissingConsumerAllows: no durable consumer for the model at
// all (no worker has ever started) must not be read as "infinitely backed
// up" — a missing consumer is an error from Consumer(), and any error admits.
func TestAdmissionMissingConsumerAllows(t *testing.T) {
	srv, js, _ := startAdmissionStack(t, AdmissionConfig{MaxBacklog: 1})
	fillQueue(t, js, "m1", 3) // queued, but nothing has ever consumed m1

	resp := postChat(t, srv, "a1")
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		t.Fatalf("status = 429 with no consumer for the model")
	}
}

// TestAdmissionCache: consumer info is fetched at most once per model per
// TTL, so a burst of requests can't turn into a burst of JetStream API
// calls on the hot path.
func TestAdmissionCache(t *testing.T) {
	now := time.Now()
	a := newAdmissionChecker(nil, AdmissionConfig{MaxBacklog: 5})
	a.nowFn = func() time.Time { return now }
	calls := 0
	a.fetchFn = func(context.Context, string) (uint64, error) {
		calls++
		return 1, nil
	}

	if !a.allow(context.Background(), "m1") || !a.allow(context.Background(), "m1") {
		t.Fatalf("allow = false with backlog 1 under limit 5")
	}
	if calls != 1 {
		t.Fatalf("fetches = %d within the TTL, want 1", calls)
	}
	now = now.Add(1500 * time.Millisecond)
	if !a.allow(context.Background(), "m1") {
		t.Fatalf("allow = false after TTL expiry with backlog 1")
	}
	if calls != 2 {
		t.Fatalf("fetches = %d after TTL expiry, want 2", calls)
	}
}

// TestAdmissionErrorAdmitsAndCaches: a failing fetch admits, and the admit
// is itself cached for the TTL so a broken/slow JetStream doesn't make every
// single request pay the fetch timeout.
func TestAdmissionErrorAdmitsAndCaches(t *testing.T) {
	now := time.Now()
	a := newAdmissionChecker(nil, AdmissionConfig{MaxBacklog: 1})
	a.nowFn = func() time.Time { return now }
	calls := 0
	a.fetchFn = func(context.Context, string) (uint64, error) {
		calls++
		return 0, fmt.Errorf("consumer not found")
	}

	for range 3 {
		if !a.allow(context.Background(), "m1") {
			t.Fatalf("allow = false, want admit on fetch error")
		}
	}
	if calls != 1 {
		t.Fatalf("fetches = %d, want the admit to be cached (1)", calls)
	}
}

// TestAdmissionOverride: per-model overrides replace max_backlog, and an
// override of 0 exempts that model entirely (no fetch at all).
func TestAdmissionOverride(t *testing.T) {
	a := newAdmissionChecker(nil, AdmissionConfig{
		MaxBacklog: 5,
		Overrides:  map[string]int{"m1": 0, "m2": 1},
	})
	calls := 0
	a.fetchFn = func(context.Context, string) (uint64, error) {
		calls++
		return 2, nil
	}

	if !a.allow(context.Background(), "m1") {
		t.Errorf("allow(m1) = false, want admit (override 0 disables the check)")
	}
	if calls != 0 {
		t.Errorf("fetches = %d for an exempt model, want 0", calls)
	}
	if a.allow(context.Background(), "m2") {
		t.Errorf("allow(m2) = true with backlog 2 over override limit 1, want reject")
	}
}

// TestAdmissionRetryAfterDefault: a checker built from a config that never
// set retry_after_seconds still emits a usable Retry-After.
func TestAdmissionRetryAfterDefault(t *testing.T) {
	a := newAdmissionChecker(nil, AdmissionConfig{MaxBacklog: 1})
	if got := a.retryAfter(); got != 2 {
		t.Errorf("retryAfter = %d, want 2", got)
	}
}
