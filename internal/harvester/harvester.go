// Package harvester (this file) implements the METERING consumer and
// batcher: a durable JetStream pull consumer that decodes wire.UsageEvent
// messages, accumulates them into batches, and flushes those batches to a
// Sink. Delivery is ack-after-insert: a message is only acked once the
// batch containing it has been successfully inserted, so a crash between
// receipt and insert simply results in redelivery (at-least-once), never
// silent loss. In-batch duplicates (same ReqID delivered twice within one
// held batch — e.g. via redelivery racing a fresh delivery) are collapsed
// before the Sink ever sees them; see chsink.go:166-169 for why the sink
// itself deliberately does not do this.
package harvester

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/wire"
)

const (
	// consumerDurable is the durable pull consumer name on StreamMetering.
	consumerDurable = "harvester-main"

	// ackWait bounds how long JetStream waits for an ack before
	// redelivering a message. It must comfortably exceed the time a
	// message can spend held-but-uninserted in the worst case; the
	// heldInProgress heartbeat below extends that window further by
	// resetting the redelivery timer for messages still held.
	ackWait = 60 * time.Second

	// heldInProgressInterval is how often every currently-held (batched
	// but not yet inserted) message gets an InProgress() ack-request,
	// resetting its redelivery timer. This narrows — but per the binding
	// ruling does not eliminate — the window in which a message could be
	// redelivered after its batch was already successfully inserted
	// (which would double-fire OnRow); budgets are advisory, so that
	// residual window is accepted.
	heldInProgressInterval = 20 * time.Second

	// nakRedeliverDelay is how long JetStream waits before redelivering a
	// message that was Nak'd because its batch failed to insert.
	nakRedeliverDelay = 5 * time.Second
)

// heldMsg pairs a decoded Row with the jetstream.Msg it came from, kept
// around (unacked) from the moment it's decoded until the batch containing
// it is flushed.
type heldMsg struct {
	row Row
	msg jetstream.Msg
}

// Harvester consumes wire.UsageEvent messages off StreamMetering, batches
// them, and flushes batches to a Sink.
type Harvester struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	sink Sink
	cfg  Config

	mu   sync.Mutex // guards held and flush execution
	held []heldMsg

	onRowMu sync.Mutex
	onRow   func(Row)
}

// New constructs a Harvester. cfg is completed with defaults for any
// unset field (see Config.withDefaults).
func New(nc *nats.Conn, js jetstream.JetStream, sink Sink, cfg Config) *Harvester {
	return &Harvester{
		nc:   nc,
		js:   js,
		sink: sink,
		cfg:  cfg.withDefaults(),
	}
}

// OnRow registers fn to be called once per successfully-inserted row,
// after that row's message has been acked. fn is called synchronously from
// the flush path — it must not block for long, and must be safe to call
// from a non-Run goroutine (registration itself is not synchronized with
// Run: call OnRow before Run starts consuming, as budget-ledger callers
// do).
func (h *Harvester) OnRow(fn func(Row)) {
	h.onRowMu.Lock()
	h.onRow = fn
	h.onRowMu.Unlock()
}

// Run provisions the METERING stream (idempotent) and a durable pull
// consumer on it, then consumes until ctx is done. It blocks until
// shutdown and returns ctx.Err().
func (h *Harvester) Run(ctx context.Context) error {
	if err := wire.EnsureStreams(ctx, h.js); err != nil {
		return err
	}

	cons, err := h.js.CreateOrUpdateConsumer(ctx, wire.StreamMetering, jetstream.ConsumerConfig{
		Durable:   consumerDurable,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   ackWait,
		// MaxDeliver left unset: the server defaults an unset (0) value to
		// -1 (unlimited redelivery attempts) for AckExplicit consumers.
		// MaxAckPending must comfortably exceed BatchMaxEvents since every
		// held-but-uninserted message stays un-acked (and thus
		// ack-pending) until its batch flushes.
		MaxAckPending: h.cfg.BatchMaxEvents * 4,
	})
	if err != nil {
		return err
	}

	cc, err := cons.Consume(h.handleMsg)
	if err != nil {
		return err
	}
	defer func() {
		cc.Stop()
		<-cc.Closed()
	}()

	flushTicker := time.NewTicker(h.cfg.BatchMaxInterval)
	defer flushTicker.Stop()
	hbTicker := time.NewTicker(heldInProgressInterval)
	defer hbTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Best-effort final flush of whatever is still held: if the
			// sink is reachable this avoids needless redelivery, and if
			// it isn't, the held messages simply remain unacked and get
			// redelivered to whichever consumer picks them up next.
			h.flush(context.Background())
			return ctx.Err()
		case <-flushTicker.C:
			h.flush(context.Background())
		case <-hbTicker.C:
			h.heldInProgress()
		}
	}
}

// handleMsg decodes one message and appends it to the held batch,
// flushing immediately if that fills the batch. An undecodable payload is
// terminated (never redelivered) rather than held, since redelivery of
// garbage can never succeed.
func (h *Harvester) handleMsg(msg jetstream.Msg) {
	var ev wire.UsageEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		slog.Warn("harvester: undecodable METERING payload, terminating", "err", err)
		if termErr := msg.Term(); termErr != nil {
			slog.Warn("harvester: term failed for undecodable message", "err", termErr)
		}
		return
	}

	row := RowFromEvent(ev)

	h.mu.Lock()
	h.held = append(h.held, heldMsg{row: row, msg: msg})
	full := len(h.held) >= h.cfg.BatchMaxEvents
	h.mu.Unlock()

	if full {
		h.flush(context.Background())
	}
}

// flush takes everything currently held, dedups it by ReqID (first
// occurrence wins), and inserts the deduped rows into the sink. On
// success every held message is acked, and OnRow fires once per row that
// was actually inserted (not for in-batch duplicates). On failure every
// held message is Nak'd with a fixed delay for redelivery, and nothing is
// acked or reported via OnRow.
func (h *Harvester) flush(ctx context.Context) {
	h.mu.Lock()
	if len(h.held) == 0 {
		h.mu.Unlock()
		return
	}
	batch := h.held
	h.held = nil
	h.mu.Unlock()

	seen := make(map[string]bool, len(batch))
	firstOccurrence := make([]bool, len(batch))
	rows := make([]Row, 0, len(batch))
	for i, item := range batch {
		if seen[item.row.ReqID] {
			firstOccurrence[i] = false
			continue
		}
		seen[item.row.ReqID] = true
		firstOccurrence[i] = true
		rows = append(rows, item.row)
	}

	if err := h.sink.InsertBatch(ctx, rows); err != nil {
		slog.Warn("harvester: insert batch failed, nak'ing for redelivery", "err", err, "held", len(batch))
		for _, item := range batch {
			if nakErr := item.msg.NakWithDelay(nakRedeliverDelay); nakErr != nil {
				slog.Warn("harvester: nak failed", "req_id", item.row.ReqID, "err", nakErr)
			}
		}
		return
	}

	h.onRowMu.Lock()
	onRow := h.onRow
	h.onRowMu.Unlock()

	for i, item := range batch {
		if ackErr := item.msg.Ack(); ackErr != nil {
			slog.Warn("harvester: ack failed after successful insert", "req_id", item.row.ReqID, "err", ackErr)
		}
		if firstOccurrence[i] && onRow != nil {
			onRow(item.row)
		}
	}
}

// heldInProgress sends an InProgress ack-request for every message
// currently held (batched but not yet flushed), resetting its redelivery
// timer per the binding ruling in the task brief.
func (h *Harvester) heldInProgress() {
	h.mu.Lock()
	snapshot := make([]heldMsg, len(h.held))
	copy(snapshot, h.held)
	h.mu.Unlock()

	for _, item := range snapshot {
		if err := item.msg.InProgress(); err != nil {
			slog.Warn("harvester: InProgress failed for held message", "req_id", item.row.ReqID, "err", err)
		}
	}
}
