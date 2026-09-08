// Package e2e's M5 hardening tests (Task 7): three full-stack scenarios
// none of the M3/M4 acceptance tests exercise —
//
//   - TestM5Redelivery: a message whose first delivery is never acked gets
//     redelivered (worker.go's consumer: AckWait/MaxDeliver, defaulting to
//     30s/2 in production) to a second, healthy worker, and the original
//     client request completes exactly once.
//   - TestM5Backlog429: the gateway's per-model admission control
//     (internal/gateway/admission.go) rejects a request with 429 +
//     Retry-After once a model's JetStream backlog reaches its configured
//     limit, and admits again once a worker drains it.
//   - TestM5ZeroConfigWorker: worker.Discover (Task 2) builds a Config from
//     an engine's own /v1/models with no YAML at all, and the resulting
//     worker's MODELS KV advertisement appears and later expires.
//
// All three use gateway.New's default STATIC IAM mode (m3/m4's kv-mode
// control-plane assembly is unnecessary machinery for what these tests
// care about) plus the same embedded-NATS/FakeEngine fixtures m3/m4
// already established (testutil.RunNATS, testutil.FakeEngine).
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/engine/openaihttp"
	"github.com/laenenai/inferbus/internal/gateway"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
	"github.com/laenenai/inferbus/internal/worker"
)

// startWorker runs w in the background, waits (bounded) for it to become
// ready, and registers a bounded cleanup that cancels and joins it. Every
// worker in this file goes through this helper so each one's shutdown is
// symmetric with m3/m4's own inline pattern.
func startWorker(t *testing.T, w *worker.Worker) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- w.RunReady(ctx, ready) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never became ready")
	}
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

// doChatRequest fires one chat completion against gwSrv with key, returning
// the response (body already drained and closed, so Header is still safe
// to read but Body is not).
func doChatRequest(t *testing.T, client *http.Client, gwSrv *httptest.Server, key string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"fast","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("new chat request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("chat request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, body
}

// wedgeEngine is an ibengine.Engine whose Chat/ChatStream block until its
// release channel is closed — deliberately NOT selecting on ctx at all, so
// canceling the worker's own context (which TestM5Redelivery does purely
// to make worker.go's RunReady call the consumer's cc.Stop() and stop
// competing for a JetStream redelivery, without abandoning the message its
// handle() goroutine already holds) does not by itself unblock the call.
// Only closing release does — which TestM5Redelivery only does from its
// own cleanup, once its assertions are already done.
type wedgeEngine struct {
	release chan struct{}
}

func (e *wedgeEngine) Chat(context.Context, string, json.RawMessage) (json.RawMessage, wire.Usage, error) {
	<-e.release
	return json.RawMessage(`{"object":"chat.completion"}`), wire.Usage{}, nil
}

func (e *wedgeEngine) ChatStream(context.Context, string, json.RawMessage, func(json.RawMessage) error) (wire.Usage, error) {
	<-e.release
	return wire.Usage{}, nil
}

// TestM5Redelivery: a "rogue" worker picks up the only delivery of a
// request and never acks it (its FakeEngine is wedged — it blocks until
// this test's cleanup cancels its context, well after this test's own
// assertions are done). Once AckWait expires, JetStream redelivers the
// same message (MaxDeliver: 2, so exactly one redelivery is available) to
// a second, healthy worker, which completes it — and the original
// client's HTTP request, still blocked this whole time, receives its 200.
//
// Redelivery strategy: the brief offers two options — (a) pre-create the
// durable consumer with a short AckWait that the worker is supposed to
// preserve, or (b) start the real worker with a wedging engine first, then
// swap in a healthy one once AckWait expires naturally. Neither applies
// verbatim: an empirical probe (js.CreateOrUpdateConsumer called twice,
// once with AckWait 2s then again with 30s on the same durable name)
// confirmed option (a)'s premise fails — worker.go's RunReady always
// passed a literal AckWait to CreateOrUpdateConsumer, and JetStream
// applies that literal value to an existing consumer of the same durable
// name, overwriting any shorter AckWait a test pre-created. Rather than
// accept option (b)'s ~30s-per-run cost (worker.go's heartbeat also calls
// msg.InProgress() every 10s while a handler is in flight, which itself
// resets AckWait — so a naively "wedged forever" worker would in fact
// never let the message redeliver at all, needing yet another workaround
// to kill its own heartbeat), this is fixed at the wiring level: Config
// grew two test-only knobs (AckWait/ConsumerMaxDeliver, config.go — same
// shape and rationale as the pre-existing AdvertiseEvery/AdvertiseTTL)
// that RunReady's CreateOrUpdateConsumer now honors, defaulting to the
// unchanged production values (30s/2) when zero. This test sets AckWait
// to 2s, safely under the 10s heartbeat interval, so the message becomes
// eligible for redelivery before the rogue worker's own heartbeat could
// ever renew it.
//
// A second problem surfaced empirically once AckWait was short enough to
// actually fire during a test run: with two Worker processes bound to the
// same durable consumer, JetStream does not necessarily route a
// redelivery to the *other* one — the rogue's own pull loop is still
// active (it dispatched its handler to a goroutine and immediately went
// back to asking for more), so the redelivery can race right back to the
// rogue and get stuck behind the very same wedge, silently consuming both
// of MaxDeliver:2's attempts and starving the healthy worker entirely.
// wedgeEngine (below) exists to fix this deterministically: canceling the
// rogue's own worker context makes RunReady call its consumer's cc.Stop()
// (see worker.go) — "no more messages will be received" — which takes the
// rogue out of the redelivery race, while wedgeEngine's Chat/ChatStream
// (unlike testutil.FakeEngine) never watch ctx, so the message the rogue
// already holds stays genuinely un-acked throughout.
func TestM5Redelivery(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	const key = "e2e-m5-redelivery-key"
	gw := gateway.New(nc, js, gateway.Config{
		RequestTimeout: 20 * time.Second,
		Keys:           []gateway.KeyConfig{{Key: key, Name: "k1", Org: "acme", Project: "default", Allow: []string{"fast"}}},
		Aliases:        map[string]string{"fast": "m1"},
	})
	gwSrv := httptest.NewServer(gw.Routes())
	t.Cleanup(gwSrv.Close)

	// The rogue worker: wedgeEngine blocks until this test's cleanup
	// releases it, well after this test's own assertions are done. Its
	// AckWait is the 2s test override described above; both workers below
	// must agree on it (and on MaxDeliver), or the second worker's
	// CreateOrUpdateConsumer call would just reconfigure the shared
	// consumer out from under the first.
	wedge := &wedgeEngine{release: make(chan struct{})}
	rogueCtx, cancelRogue := context.WithCancel(context.Background())
	rogue := worker.New(nc, js, map[string]ibengine.Engine{"m1": wedge}, worker.Config{
		WorkerID: "rogue", Models: []worker.ModelConfig{{Name: "m1", MaxInflight: 1}},
		AckWait: 2 * time.Second,
	})
	rogueReady := make(chan struct{})
	rogueDone := make(chan error, 1)
	go func() { rogueDone <- rogue.RunReady(rogueCtx, rogueReady) }()
	t.Cleanup(func() {
		// Release the wedge FIRST: RunReady's own WaitGroup (worker.go)
		// cannot drain — and so RunReady itself cannot return — until the
		// handle() goroutine holding the original delivery comes back,
		// which (by wedgeEngine's design, see its doc comment) only
		// happens once release is closed, never merely from ctx being
		// canceled below.
		close(wedge.release)
		select {
		case <-rogueDone:
		case <-time.After(5 * time.Second):
			t.Fatal("rogue worker did not stop")
		}
	})
	select {
	case <-rogueReady:
	case <-time.After(5 * time.Second):
		t.Fatal("rogue worker never became ready")
	}

	// Subscribe to usage events before firing the request so nothing can
	// be missed (same "subscribe before publish" discipline as m3's
	// inference.req.> subscription).
	usageSub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatalf("subscribe metering.usage.>: %v", err)
	}
	defer usageSub.Unsubscribe()

	type chatResult struct {
		status int
		body   []byte
		err    error
	}
	resultCh := make(chan chatResult, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, gwSrv.URL+"/v1/chat/completions",
			strings.NewReader(`{"model":"fast","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			resultCh <- chatResult{err: err}
			return
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			resultCh <- chatResult{err: err}
			return
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		resultCh <- chatResult{status: resp.StatusCode, body: body}
	}()

	// Wait for the rogue worker to actually be holding the message
	// un-acked (it's the only consumer running yet, so this delivery is
	// deterministically its), then stop it from competing for the
	// eventual redelivery — see wedgeEngine's doc comment and the
	// function doc comment above for why this step is required.
	pollUntil(t, 5*time.Second, func() bool {
		cons, err := js.Consumer(ctx, wire.StreamInference, wire.Durable("m1"))
		if err != nil {
			return false
		}
		info, err := cons.Info(ctx)
		return err == nil && info.NumAckPending >= 1
	})
	cancelRogue()

	healthy := &testutil.FakeEngine{FinalUsage: wire.Usage{PromptTokens: 7, CompletionTokens: 3}}
	w2 := worker.New(nc, js, map[string]ibengine.Engine{"m1": healthy}, worker.Config{
		WorkerID: "healthy", Models: []worker.ModelConfig{{Name: "m1", MaxInflight: 1}},
		AckWait: 2 * time.Second,
	})
	startWorker(t, w2)

	// The redelivery only happens once the 2s test AckWait actually
	// expires; this deadline clears that plus real processing time with
	// comfortable margin.
	var res chatResult
	select {
	case res = <-resultCh:
	case <-time.After(15 * time.Second):
		t.Fatal("chat completion did not complete after redelivery")
	}
	if res.err != nil {
		t.Fatalf("chat request: %v", res.err)
	}
	if res.status != http.StatusOK {
		t.Fatalf("chat status = %d, want 200, body=%s", res.status, res.body)
	}

	// Exactly-once completion: exactly one usage event, and it must be
	// the healthy worker's (the rogue never returns from its wedge, so it
	// can never itself meter anything during this test).
	first, err := usageSub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("expected one usage event: %v", err)
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(first.Data, &ev); err != nil {
		t.Fatalf("decode usage event: %v", err)
	}
	if ev.WorkerID != "healthy" {
		t.Fatalf("usage event worker id = %q, want %q", ev.WorkerID, "healthy")
	}
	if ev.Status != "ok" {
		t.Fatalf("usage event status = %q, want %q", ev.Status, "ok")
	}
	if _, err := usageSub.NextMsg(2 * time.Second); err == nil {
		t.Fatal("a second usage event arrived — redelivery must complete exactly once")
	} else if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("waiting for a (non-existent) second usage event: %v", err)
	}
}

// TestM5Backlog429: with admission control on (max_backlog: 1) and no
// worker running, two requests queued via goroutines both get admitted and
// published (racing the SAME 1s admission cache entry — see
// admission.go's admissionCacheTTL: the first request's admission check
// populates the cache with the still-empty backlog reading, and the
// second, arriving within that window, reads the same stale cache rather
// than the real, just-incremented backlog). A third, later request sees
// the real backlog (>= the limit) once that cache entry has expired, and
// is rejected 429 with Retry-After and type "overloaded". Starting a
// worker then drains the two queued requests, and a later request is
// admitted again.
func TestM5Backlog429(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	const model = "m1"
	const key = "e2e-m5-backlog-key"
	const maxInflight = 2

	// Pre-create the model's durable consumer with the exact shape
	// worker.go's own CreateOrUpdateConsumer uses: (a) the admission
	// checker's fetchBacklog (admission.go) needs a real consumer to read
	// NumPending/NumAckPending from — with no consumer at all, every
	// fetch errors and admission fails open — and (b) a worker started
	// later in this test must be able to adopt the same consumer without
	// a config mismatch.
	if _, err := js.CreateOrUpdateConsumer(ctx, wire.StreamInference, jetstream.ConsumerConfig{
		Durable:       wire.Durable(model),
		FilterSubject: wire.ReqSubject(model),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    2,
		MaxAckPending: maxInflight,
	}); err != nil {
		t.Fatalf("pre-create consumer: %v", err)
	}

	gw := gateway.New(nc, js, gateway.Config{
		RequestTimeout: 30 * time.Second,
		Keys:           []gateway.KeyConfig{{Key: key, Name: "k1", Org: "acme", Project: "default", Allow: []string{"fast"}}},
		Aliases:        map[string]string{"fast": model},
		Admission:      gateway.AdmissionConfig{MaxBacklog: 1, RetryAfterSeconds: 3},
	})
	gwSrv := httptest.NewServer(gw.Routes())
	t.Cleanup(gwSrv.Close)

	// Two requests, fired concurrently: both are expected to be admitted
	// (see the cache-race comment on the test doc above) and then block
	// awaiting a response no one will send until a worker starts.
	const n = 2
	respCh := make(chan int, n)
	for i := 0; i < n; i++ {
		go func() {
			resp, _ := doChatRequest(t, &http.Client{Timeout: 60 * time.Second}, gwSrv, key)
			respCh <- resp.StatusCode
		}()
	}

	pollUntil(t, 5*time.Second, func() bool {
		cons, err := js.Consumer(ctx, wire.StreamInference, wire.Durable(model))
		if err != nil {
			return false
		}
		info, err := cons.Info(ctx)
		return err == nil && info.NumPending >= 2
	})

	// The admission cache entry the first two requests populated is up to
	// admissionCacheTTL (1s, unexported in package gateway) stale — wait
	// it out so the third request's check does a live fetch instead of
	// reusing that same reading. Bounded and commented, not used as a
	// synchronization primitive (see the top-level task constraints).
	time.Sleep(1200 * time.Millisecond)

	resp, body := doChatRequest(t, &http.Client{Timeout: 10 * time.Second}, gwSrv, key)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("third request status = %d, want 429, body=%s", resp.StatusCode, body)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "3" {
		t.Fatalf("Retry-After = %q, want %q", ra, "3")
	}
	var errBody struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &errBody); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if errBody.Error.Type != "overloaded" {
		t.Fatalf("error type = %q, want %q", errBody.Error.Type, "overloaded")
	}

	// Start a worker to drain the two originally-queued requests.
	healthy := &testutil.FakeEngine{FinalUsage: wire.Usage{PromptTokens: 2, CompletionTokens: 2}}
	w := worker.New(nc, js, map[string]ibengine.Engine{model: healthy}, worker.Config{
		WorkerID: "drain", Models: []worker.ModelConfig{{Name: model, MaxInflight: maxInflight}},
	})
	startWorker(t, w)

	for i := 0; i < n; i++ {
		select {
		case status := <-respCh:
			if status != http.StatusOK {
				t.Fatalf("queued request status = %d, want 200 once drained", status)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("queued requests never drained")
		}
	}

	// Backlog is now 0 — poll (again past the cache TTL) until a later
	// request is admitted rather than rejected.
	pollUntil(t, 10*time.Second, func() bool {
		resp, _ := doChatRequest(t, &http.Client{Timeout: 5 * time.Second}, gwSrv, key)
		return resp.StatusCode == http.StatusOK
	})
}

// TestM5ZeroConfigWorker: worker.Discover (Task 2) builds a worker Config
// with no YAML at all, from nothing but an OpenAI-compatible engine's own
// GET /v1/models. The resulting worker actually serves a request through
// the gateway (alias -> the discovered model), advertises itself in the
// MODELS KV bucket, and — once stopped, with the AdvertiseEvery/AdvertiseTTL
// test knobs (Task 2) turned down for fast expiry — that advertisement
// disappears.
func TestM5ZeroConfigWorker(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	// A minimal OpenAI-compatible engine: one model at /v1/models, and a
	// fixed non-streaming completion (with usage) at /v1/chat/completions
	// — enough for worker.Discover to build a Config, and for the
	// openai_http adapter (built the same way cmd/inferbus/roles.go does)
	// to actually serve a request.
	engineSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"object": "list",
				"data":   []map[string]string{{"id": "zc-1", "object": "model"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "cmpl-1", "object": "chat.completion",
				"choices": []map[string]any{{"message": map[string]string{"role": "assistant", "content": "zero-config hello"}}},
				"usage":   map[string]int{"prompt_tokens": 4, "completion_tokens": 2},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(engineSrv.Close)

	discoverCtx, cancelDiscover := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDiscover()
	cfg, err := worker.Discover(discoverCtx, engineSrv.URL, 2)
	if err != nil {
		t.Fatalf("worker.Discover: %v", err)
	}
	if len(cfg.Models) != 1 || cfg.Models[0].Name != "zc-1" {
		t.Fatalf("Discover models = %+v, want exactly [zc-1]", cfg.Models)
	}
	// Task 2's test-only knobs: fast MODELS heartbeat/TTL so this test
	// doesn't wait out the 15s/45s production defaults (wire.go's
	// ModelsHeartbeat/ModelsTTL).
	cfg.AdvertiseEvery = 500 * time.Millisecond
	cfg.AdvertiseTTL = 2 * time.Second

	engines := map[string]ibengine.Engine{}
	for _, mc := range cfg.Models {
		engines[mc.Name] = openaihttp.New(mc.URL, nil)
	}

	w := worker.New(nc, js, engines, cfg)
	stopWorker := startWorker(t, w)

	// MODELS KV entry appears (poll <= 5s), advertising zc-1.
	pollUntil(t, 5*time.Second, func() bool {
		kv, err := js.KeyValue(ctx, wire.BucketModels)
		if err != nil {
			return false
		}
		e, err := kv.Get(ctx, cfg.WorkerID)
		if err != nil {
			return false
		}
		var ad wire.WorkerAd
		if json.Unmarshal(e.Value(), &ad) != nil {
			return false
		}
		for _, m := range ad.Models {
			if m.Name == "zc-1" {
				return true
			}
		}
		return false
	})

	// A chat through the gateway (alias -> zc-1) succeeds.
	const key = "e2e-m5-zc-key"
	gw := gateway.New(nc, js, gateway.Config{
		RequestTimeout: 10 * time.Second,
		Keys:           []gateway.KeyConfig{{Key: key, Name: "k1", Org: "acme", Project: "default", Allow: []string{"fast"}}},
		Aliases:        map[string]string{"fast": "zc-1"},
	})
	gwSrv := httptest.NewServer(gw.Routes())
	t.Cleanup(gwSrv.Close)

	resp, body := doChatRequest(t, http.DefaultClient, gwSrv, key)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("chat status = %d, want 200, body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "zero-config hello") {
		t.Fatalf("chat response body = %s, want it to carry the engine's own content", body)
	}

	// Stop the worker; the MODELS entry should expire (poll <= 10s, the
	// brief's own deadline for this — well clear of the 2s AdvertiseTTL
	// above).
	stopWorker()
	pollUntil(t, 10*time.Second, func() bool {
		kv, err := js.KeyValue(ctx, wire.BucketModels)
		if err != nil {
			return false
		}
		_, err = kv.Get(ctx, cfg.WorkerID)
		return errors.Is(err, jetstream.ErrKeyNotFound)
	})
}
