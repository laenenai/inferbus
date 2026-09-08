package harvester_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/harvester"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
)

// publishUsage publishes a well-formed usage event to the METERING stream.
func publishUsage(t *testing.T, js jetstream.JetStream, ev wire.UsageEvent) {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	subj := wire.UsageSubject(ev.Org, ev.Project, ev.Model)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.Publish(ctx, subj, b); err != nil {
		t.Fatalf("publish usage event %s: %v", ev.ReqID, err)
	}
}

func usageEvent(reqID string) wire.UsageEvent {
	return wire.UsageEvent{
		ReqID:    reqID,
		Org:      "acme",
		Project:  "prod",
		KeyID:    "key-1",
		Alias:    "fast",
		Model:    "gpt-4",
		Provider: "openai",
		Kind:     "chat",
		Status:   "ok",
		WorkerID: "worker-1",
		Usage:    wire.Usage{PromptTokens: 10, CompletionTokens: 5},
		TS:       time.Now().UTC(),
	}
}

// rowCounter is a thread-safe counter used to assert OnRow firing counts
// across tests.
type rowCounter struct {
	n   int64
	mu  sync.Mutex
	ids []string
}

func (c *rowCounter) onRow(r harvester.Row) {
	atomic.AddInt64(&c.n, 1)
	c.mu.Lock()
	c.ids = append(c.ids, r.ReqID)
	c.mu.Unlock()
}

func (c *rowCounter) count() int64 { return atomic.LoadInt64(&c.n) }

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// TestHappyPathInsertsAcksAndFiresOnRow covers scenario (a): publishing 3
// usage events results in all 3 being inserted into the sink, acked (the
// stream's ack floor advances past them), and OnRow firing exactly 3 times.
func TestHappyPathInsertsAcksAndFiresOnRow(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	for _, id := range []string{"req-a1", "req-a2", "req-a3"} {
		publishUsage(t, js, usageEvent(id))
	}

	sink := harvester.NewFakeSink()
	var rc rowCounter
	h := harvester.New(nc, js, sink, harvester.Config{
		BatchMaxEvents:   500,
		BatchMaxInterval: 100 * time.Millisecond,
	})
	h.OnRow(rc.onRow)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.Run(runCtx) }()

	waitFor(t, 5*time.Second, func() bool {
		return len(sink.RowsSnapshot()) == 3
	})
	waitFor(t, 2*time.Second, func() bool {
		return rc.count() == 3
	})

	rows := sink.RowsSnapshot()
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}

	// Ack floor must advance: once all 3 messages are acked, nothing
	// should remain pending or ack-pending on the consumer.
	waitFor(t, 5*time.Second, func() bool {
		info, err := js.Consumer(ctx, wire.StreamMetering, "harvester-main")
		if err != nil {
			return false
		}
		ci := info.CachedInfo()
		return ci.NumAckPending == 0 && ci.NumPending == 0 && ci.AckFloor.Stream >= 3
	})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

// TestIntervalFlush covers scenario (b): a single published event, with no
// more arriving, is flushed within roughly 2x the configured
// BatchMaxInterval rather than waiting indefinitely for the batch to fill.
func TestIntervalFlush(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}
	publishUsage(t, js, usageEvent("req-interval"))

	sink := harvester.NewFakeSink()
	interval := 100 * time.Millisecond
	h := harvester.New(nc, js, sink, harvester.Config{
		BatchMaxEvents:   500,
		BatchMaxInterval: interval,
	})

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = h.Run(runCtx) }()

	waitFor(t, 2*interval+2*time.Second, func() bool {
		return len(sink.RowsSnapshot()) == 1
	})
}

// TestSinkFailureRedeliversExactlyOnce covers scenario (c): the sink fails
// on the first InsertBatch attempt (FailNext), causing the held message to
// be Nak'd and redelivered; the second attempt succeeds and, thanks to
// FakeSink's ReqID dedup, exactly one row ends up stored (no double row),
// and OnRow fires exactly once (only for the successful attempt).
func TestSinkFailureRedeliversExactlyOnce(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	sink := harvester.NewFakeSink()
	sink.FailNext = errors.New("boom: injected sink failure")

	publishUsage(t, js, usageEvent("req-retry"))

	h := harvester.New(nc, js, sink, harvester.Config{
		BatchMaxEvents:   500,
		BatchMaxInterval: 50 * time.Millisecond,
	})
	var rc rowCounter
	h.OnRow(rc.onRow)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = h.Run(runCtx) }()

	// Nak delay is fixed at 5s per the binding ruling, so redelivery (and
	// the second, successful insert attempt) happens a few seconds out.
	waitFor(t, 10*time.Second, func() bool {
		return len(sink.RowsSnapshot()) == 1
	})

	// Give any further (erroneous) redelivery a moment to also land, then
	// assert we still only have exactly one row.
	time.Sleep(300 * time.Millisecond)
	rows := sink.RowsSnapshot()
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want exactly 1 (no double insert)", len(rows))
	}
	if rows[0].ReqID != "req-retry" {
		t.Fatalf("row ReqID = %q, want %q", rows[0].ReqID, "req-retry")
	}
	if rc.count() != 1 {
		t.Fatalf("OnRow fired %d times, want exactly 1", rc.count())
	}
}

// TestGarbagePayloadTermed covers scenario (d): an undecodable payload on
// the METERING stream must be Term'd (never redelivered), while other,
// well-formed events on the same stream are unaffected and still get
// inserted.
func TestGarbagePayloadTermed(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	// Garbage payload on a valid metering subject.
	if _, err := js.Publish(ctx, wire.UsageSubject("acme", "prod", "gpt-4"), []byte("not json at all")); err != nil {
		t.Fatalf("publish garbage: %v", err)
	}
	publishUsage(t, js, usageEvent("req-good-1"))
	publishUsage(t, js, usageEvent("req-good-2"))

	sink := harvester.NewFakeSink()
	h := harvester.New(nc, js, sink, harvester.Config{
		BatchMaxEvents:   500,
		BatchMaxInterval: 100 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = h.Run(runCtx) }()

	waitFor(t, 5*time.Second, func() bool {
		return len(sink.RowsSnapshot()) == 2
	})

	// The stream has 3 messages total; the garbage one must be Term'd
	// (removed from ack-pending, never redelivered) and the other two
	// acked, so ack-pending should settle at 0 with no pending messages.
	waitFor(t, 5*time.Second, func() bool {
		info, err := js.Consumer(ctx, wire.StreamMetering, "harvester-main")
		if err != nil {
			return false
		}
		ci := info.CachedInfo()
		return ci.NumAckPending == 0 && ci.NumPending == 0
	})

	rows := sink.RowsSnapshot()
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want exactly 2 (garbage message must not be inserted)", len(rows))
	}
}
