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

	ibengine "github.com/infbus/infbus/internal/engine"
	"github.com/infbus/infbus/internal/relay"
	"github.com/infbus/infbus/internal/testutil"
	"github.com/infbus/infbus/internal/wire"
	"github.com/infbus/infbus/internal/worker"
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
	nc, js := testutil.RunNATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := worker.New(nc, js, map[string]ibengine.Engine{"m1": eng}, worker.Config{
		WorkerID: "w-test",
		Models:   []worker.ModelConfig{{Name: "m1", MaxInflight: 2}},
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
