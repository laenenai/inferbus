package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/relay"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
	"github.com/laenenai/inferbus/internal/worker"
)

func startWorker(t *testing.T, eng ibengine.Engine) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, js, _ := startWorkerCancelable(t, eng)
	return nc, js
}

// startWorkerCancelable is like startWorker but also returns the cancel
// func for the worker's Run context, for tests that need to trigger a
// graceful shutdown mid-request.
func startWorkerCancelable(t *testing.T, eng ibengine.Engine) (*nats.Conn, jetstream.JetStream, context.CancelFunc) {
	t.Helper()
	return startWorkerWithModel(t, eng, worker.ModelConfig{Name: "m1", MaxInflight: 2})
}

// startWorkerWithModel is like startWorkerCancelable but lets the caller
// supply the model's full config (engine/provider) rather than the bare
// {Name, MaxInflight} default — used by tests that need to observe how a
// specific engine/provider combination is attributed on usage events.
func startWorkerWithModel(t *testing.T, eng ibengine.Engine, mc worker.ModelConfig) (*nats.Conn, jetstream.JetStream, context.CancelFunc) {
	t.Helper()
	nc, js := testutil.RunNATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := worker.New(nc, js, map[string]ibengine.Engine{"m1": eng}, worker.Config{
		WorkerID: "w-test",
		Models:   []worker.ModelConfig{mc},
	})
	ready := make(chan struct{})
	go func() { _ = w.RunReady(ctx, ready) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("worker not ready")
	}
	return nc, js, cancel
}

func publishAndListen(t *testing.T, nc *nats.Conn, js jetstream.JetStream, reqID string, deadline time.Time) *relay.Listener {
	t.Helper()
	l, err := relay.Listen(nc, reqID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	_, err = relay.Publish(context.Background(), js, relay.Request{
		Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
		ReqID: reqID, Kind: "chat", Deadline: deadline,
		Body: []byte(`{"model":"fast","stream":true,"messages":[]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func drain(t *testing.T, l *relay.Listener) (msgs []wire.Message, terminal error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		m, err := l.Next(ctx)
		if errors.Is(err, io.EOF) {
			return msgs, nil
		}
		if err != nil {
			return msgs, err
		}
		msgs = append(msgs, m)
	}
}

func TestStreamHappyPath(t *testing.T) {
	eng := &testutil.FakeEngine{
		Chunks:     []string{`{"c":0}`, `{"c":1}`, `{"c":2}`},
		FinalUsage: wire.Usage{PromptTokens: 5, CompletionTokens: 3},
	}
	nc, js := startWorker(t, eng)
	l := publishAndListen(t, nc, js, "req-ok", time.Now().Add(time.Minute))
	msgs, err := drain(t, l)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 4 { // 3 chunks + done
		t.Fatalf("frames = %d: %+v", len(msgs), msgs)
	}
	last := msgs[len(msgs)-1]
	if last.Kind != wire.KindDone || last.Usage == nil || last.Usage.PromptTokens != 5 {
		t.Fatalf("done frame = %+v", last)
	}
	for i, m := range msgs {
		if m.Seq != i {
			t.Fatalf("seq[%d] = %d", i, m.Seq)
		}
	}
}

func TestEngineErrorPropagates(t *testing.T) {
	eng := &testutil.FakeEngine{
		Chunks: []string{`{"c":0}`, `{"c":1}`},
		Err:    &ibengine.Error{Code: "upstream_error", Message: "boom", HTTPStatus: 502},
	}
	nc, js := startWorker(t, eng)
	l := publishAndListen(t, nc, js, "req-err", time.Now().Add(time.Minute))
	_, err := drain(t, l)
	var re *relay.RemoteError
	if !errors.As(err, &re) || re.Err.HTTPStatus != 502 {
		t.Fatalf("err = %v", err)
	}
}

func TestCancelMidStream(t *testing.T) {
	eng := &testutil.FakeEngine{
		Chunks:     []string{`{"c":0}`, `{"c":1}`, `{"c":2}`, `{"c":3}`, `{"c":4}`},
		FinalUsage: wire.Usage{PromptTokens: 5, CompletionTokens: 5},
		Delay:      200 * time.Millisecond,
	}
	nc, js := startWorker(t, eng)

	// watch METERING for the canceled usage event
	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	l := publishAndListen(t, nc, js, "req-cancel", time.Now().Add(time.Minute))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := l.Next(ctx); err != nil { // wait for first chunk = generation started
		t.Fatal(err)
	}
	if err := relay.Cancel(nc, "req-cancel"); err != nil {
		t.Fatal(err)
	}
	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event after cancel")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "canceled" || !ev.Estimated {
		t.Fatalf("usage event = %+v", ev)
	}
}

func TestExpiredDeadlineDiscardedOnPickup(t *testing.T) {
	eng := &testutil.FakeEngine{Chunks: []string{`{"c":0}`}}
	nc, js := startWorker(t, eng)
	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	_ = publishAndListen(t, nc, js, "req-late", time.Now().Add(-time.Second))
	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event for expired request")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "canceled" || ev.PromptTokens != 0 {
		t.Fatalf("usage event = %+v", ev)
	}
}

func TestMidStreamDeadlineMetersCanceled(t *testing.T) {
	// wire.HdrDeadline now round-trips through relay.Publish/metaOf as
	// RFC3339Nano text (sub-second precision preserved — see
	// internal/relay/relay_test.go TestPublishDeadlinePrecision), so a
	// small offset is safe here. Keep some margin over typical pickup
	// latency (deadline > 0) and under the stream's total duration
	// (5 chunks * 150ms delay = 600ms) so the deadline reliably fires
	// mid-stream rather than either immediately (phase-1 backstop) or
	// after the stream has already finished naturally.
	eng := &testutil.FakeEngine{
		Chunks:     []string{`{"c":0}`, `{"c":1}`, `{"c":2}`, `{"c":3}`, `{"c":4}`},
		FinalUsage: wire.Usage{PromptTokens: 5, CompletionTokens: 5},
		Delay:      150 * time.Millisecond,
	}
	nc, js := startWorker(t, eng)

	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	_ = publishAndListen(t, nc, js, "req-deadline", time.Now().Add(300*time.Millisecond))

	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event after mid-stream deadline expiry")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "canceled" || !ev.Estimated || ev.ErrorCode != "" {
		t.Fatalf("usage event = %+v", ev)
	}
}

func TestShutdownMetersWorkerShutdown(t *testing.T) {
	eng := &testutil.FakeEngine{
		Chunks:     []string{`{"c":0}`, `{"c":1}`, `{"c":2}`, `{"c":3}`, `{"c":4}`},
		FinalUsage: wire.Usage{PromptTokens: 5, CompletionTokens: 5},
		Delay:      150 * time.Millisecond,
	}
	nc, js, cancelWorker := startWorkerCancelable(t, eng)

	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	l := publishAndListen(t, nc, js, "req-shutdown", time.Now().Add(time.Minute))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := l.Next(ctx); err != nil { // wait for first chunk = generation started
		t.Fatal(err)
	}

	cancelWorker() // simulate graceful worker shutdown mid-stream

	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event after worker shutdown")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "canceled" || ev.ErrorCode != "worker_shutdown" {
		t.Fatalf("usage event = %+v", ev)
	}
}

func TestNonStreamReturnsResultFrame(t *testing.T) {
	eng := &testutil.FakeEngine{FinalUsage: wire.Usage{PromptTokens: 2, CompletionTokens: 1}}
	nc, js := startWorker(t, eng)
	l, err := relay.Listen(nc, "req-sync")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	_, err = relay.Publish(context.Background(), js, relay.Request{
		Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
		ReqID: "req-sync", Kind: "chat", Deadline: time.Now().Add(time.Minute),
		Body: []byte(`{"model":"fast","messages":[]}`), // no "stream"
	})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := drain(t, l)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Kind != wire.KindResult {
		t.Fatalf("frames = %+v", msgs)
	}
	if msgs[0].Usage == nil || msgs[0].Usage.PromptTokens != 2 {
		t.Fatalf("result usage = %+v", msgs[0].Usage)
	}
}

func TestNonStreamEngineError(t *testing.T) {
	eng := &testutil.FakeEngine{
		Err: &ibengine.Error{Code: "upstream_error", Message: "boom", HTTPStatus: 502},
	}
	// mc.Engine ("bifrost") is the adapter name; mc.Provider ("anthropic")
	// is the actual model provider and must be what lands on the usage
	// event's Provider field, not the adapter name.
	nc, js, _ := startWorkerWithModel(t, eng, worker.ModelConfig{
		Name: "m1", MaxInflight: 2, Engine: "bifrost", Provider: "anthropic",
	})

	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	l, err := relay.Listen(nc, "req-sync-err")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	_, err = relay.Publish(context.Background(), js, relay.Request{
		Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
		ReqID: "req-sync-err", Kind: "chat", Deadline: time.Now().Add(time.Minute),
		Body: []byte(`{"model":"fast","messages":[]}`), // no "stream"
	})
	if err != nil {
		t.Fatal(err)
	}

	_, drainErr := drain(t, l)
	var re *relay.RemoteError
	if !errors.As(drainErr, &re) || re.Err.HTTPStatus != 502 {
		t.Fatalf("err = %v", drainErr)
	}

	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event for non-stream engine error")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "error" {
		t.Fatalf("usage event = %+v", ev)
	}
	if ev.Provider != "anthropic" {
		t.Fatalf("usage event Provider = %q, want %q (mc.Provider must win over mc.Engine)", ev.Provider, "anthropic")
	}
}

func TestNonStreamClientCancel(t *testing.T) {
	eng := &testutil.FakeEngine{
		FinalUsage: wire.Usage{PromptTokens: 5, CompletionTokens: 5},
		Delay:      300 * time.Millisecond,
	}
	nc, js := startWorker(t, eng)

	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	l, err := relay.Listen(nc, "req-sync-cancel")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	_, err = relay.Publish(context.Background(), js, relay.Request{
		Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
		ReqID: "req-sync-cancel", Kind: "chat", Deadline: time.Now().Add(time.Minute),
		Body: []byte(`{"model":"fast","messages":[]}`), // no "stream"
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond) // let the worker pick up and start eng.Chat
	if err := relay.Cancel(nc, "req-sync-cancel"); err != nil {
		t.Fatal(err)
	}

	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event after non-stream client cancel")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "canceled" || !ev.Estimated || ev.ErrorCode != "" {
		t.Fatalf("usage event = %+v", ev)
	}
}

func TestNonStreamDeadline(t *testing.T) {
	eng := &testutil.FakeEngine{
		FinalUsage: wire.Usage{PromptTokens: 5, CompletionTokens: 5},
		Delay:      400 * time.Millisecond,
	}
	nc, js := startWorker(t, eng)

	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	_, err = relay.Publish(context.Background(), js, relay.Request{
		Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
		ReqID: "req-sync-deadline", Kind: "chat", Deadline: time.Now().Add(150 * time.Millisecond),
		Body: []byte(`{"model":"fast","messages":[]}`), // no "stream"
	})
	if err != nil {
		t.Fatal(err)
	}

	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event after non-stream deadline expiry")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "canceled" || !ev.Estimated || ev.ErrorCode != "" {
		t.Fatalf("usage event = %+v", ev)
	}
}

func TestNonStreamShutdown(t *testing.T) {
	eng := &testutil.FakeEngine{
		FinalUsage: wire.Usage{PromptTokens: 5, CompletionTokens: 5},
		Delay:      400 * time.Millisecond,
	}
	nc, js, cancelWorker := startWorkerCancelable(t, eng)

	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	_, err = relay.Publish(context.Background(), js, relay.Request{
		Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
		ReqID: "req-sync-shutdown", Kind: "chat", Deadline: time.Now().Add(time.Minute),
		Body: []byte(`{"model":"fast","messages":[]}`), // no "stream"
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond) // let the worker pick up and start eng.Chat
	cancelWorker()                    // simulate graceful worker shutdown mid-request

	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event after non-stream worker shutdown")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "canceled" || ev.ErrorCode != "worker_shutdown" {
		t.Fatalf("usage event = %+v", ev)
	}
}

// TestStreamZeroUsageMarkedEstimated covers I5: a successful streaming
// request whose engine never reported usage (all-zero) must still meter as
// "ok", but with Estimated set so downstream accounting knows the token
// counts are not to be trusted as exact.
func TestStreamZeroUsageMarkedEstimated(t *testing.T) {
	eng := &testutil.FakeEngine{
		Chunks:     []string{`{"c":0}`, `{"c":1}`},
		FinalUsage: wire.Usage{}, // upstream never sent usage
	}
	nc, js := startWorker(t, eng)

	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	l := publishAndListen(t, nc, js, "req-zero-usage", time.Now().Add(time.Minute))
	if _, err := drain(t, l); err != nil {
		t.Fatal(err)
	}

	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event for zero-usage stream")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "ok" {
		t.Fatalf("usage event Status = %q, want ok", ev.Status)
	}
	if !ev.Estimated {
		t.Fatalf("usage event Estimated = false, want true for all-zero usage")
	}
}

// panicEngine is a minimal ibengine.Engine whose ChatStream always panics,
// used to exercise the worker's per-message panic recovery: the
// panicking goroutine must not take the worker process down, the client
// must get a normal RemoteError, and the worker must remain able to serve
// subsequent requests.
type panicEngine struct{}

func (panicEngine) Chat(ctx context.Context, model string, body json.RawMessage) (json.RawMessage, wire.Usage, error) {
	panic("boom: chat")
}

func (panicEngine) ChatStream(ctx context.Context, model string, body json.RawMessage, emit func(json.RawMessage) error) (wire.Usage, error) {
	panic("boom: chat stream")
}

func (panicEngine) Embed(ctx context.Context, model string, body json.RawMessage) (json.RawMessage, wire.Usage, error) {
	panic("boom: embed")
}

func TestHandlePanicRecovered(t *testing.T) {
	// One worker process serving two models: "m1" always panics, "m2" is a
	// normal FakeEngine. This lets the test prove the panic doesn't take
	// the worker process (or its other consumers) down with it, rather
	// than just proving a fresh worker can be started elsewhere.
	nc, js := testutil.RunNATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	okEngine := &testutil.FakeEngine{FinalUsage: wire.Usage{PromptTokens: 1, CompletionTokens: 1}}
	w := worker.New(nc, js, map[string]ibengine.Engine{
		"m1": panicEngine{},
		"m2": okEngine,
	}, worker.Config{
		WorkerID: "w-test",
		Models: []worker.ModelConfig{
			{Name: "m1", MaxInflight: 2},
			{Name: "m2", MaxInflight: 2},
		},
	})
	ready := make(chan struct{})
	go func() { _ = w.RunReady(ctx, ready) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("worker not ready")
	}

	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	publish := func(model, reqID string) *relay.Listener {
		t.Helper()
		l, err := relay.Listen(nc, reqID)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(l.Close)
		_, err = relay.Publish(context.Background(), js, relay.Request{
			Model: model, Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
			ReqID: reqID, Kind: "chat", Deadline: time.Now().Add(time.Minute),
			Body: []byte(`{"model":"fast","stream":true,"messages":[]}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		return l
	}

	l1 := publish("m1", "req-panic")
	_, drainErr := drain(t, l1)
	var re *relay.RemoteError
	if !errors.As(drainErr, &re) {
		t.Fatalf("err = %v, want a RemoteError", drainErr)
	}
	if re.Err.Code != "worker_panic" || re.Err.HTTPStatus != 500 {
		t.Fatalf("remote error = %+v", re.Err)
	}

	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event after panic")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "error" || ev.ErrorCode != "worker_panic" {
		t.Fatalf("usage event = %+v", ev)
	}

	// The worker process (and its other model's consumer) must still be
	// alive: a second request, on the same worker, for a different (sane)
	// model succeeds normally.
	l2 := publish("m2", "req-after-panic")
	msgs, err := drain(t, l2)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) == 0 || msgs[len(msgs)-1].Kind != wire.KindDone {
		t.Fatalf("post-panic request did not complete: %+v", msgs)
	}
}

func TestMissingEngineFailsFast(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	w := worker.New(nc, js, map[string]ibengine.Engine{}, worker.Config{
		WorkerID: "w-test",
		Models:   []worker.ModelConfig{{Name: "ghost", MaxInflight: 2}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- w.RunReady(ctx, nil) }()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "ghost") {
			t.Fatalf("err = %v, want an error mentioning %q", err, "ghost")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunReady did not return promptly for a missing engine")
	}
}
