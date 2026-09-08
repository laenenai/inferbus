// harvester.go implements the METERING consumer and batcher: a durable
// JetStream pull consumer that decodes wire.UsageEvent messages,
// accumulates them into batches, and flushes those batches to a Sink. See
// the package doc in sink.go for the package-level overview.
//
// Delivery is ack-after-insert: a message is only acked once the batch
// containing it has been successfully inserted, so a crash between receipt
// and insert simply results in redelivery (at-least-once), never silent
// loss. In-batch duplicates (same ReqID delivered twice within one held
// batch — e.g. via redelivery racing a fresh delivery) are collapsed
// before the Sink ever sees them; see (*CHSink).InsertBatch's doc comment
// for why the sink itself deliberately does not do this.

package harvester

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
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
	// message can spend held-or-in-flight in the worst case; the
	// heartbeat below extends that window further by resetting the
	// redelivery timer for messages still held or in flight.
	ackWait = 60 * time.Second

	// heldInProgressInterval is how often every currently-held or
	// currently-in-flight (batched, not yet acked/nak'd either way)
	// message gets an InProgress() ack-request, resetting its redelivery
	// timer. This narrows — but per the binding ruling does not eliminate
	// — the window in which a message could be redelivered after its
	// batch was already successfully inserted (which would double-fire
	// OnRow); budgets are advisory, so that residual window is accepted.
	heldInProgressInterval = 20 * time.Second

	// nakRedeliverDelay is how long JetStream waits before redelivering a
	// message that was Nak'd because its batch failed to insert (for any
	// reason, including a flushInsertTimeout expiry — a wedged sink is
	// just another insert failure from the batcher's point of view).
	nakRedeliverDelay = 5 * time.Second

	// flushInsertTimeout bounds every non-final InsertBatch call, derived
	// from Run's context. Without this a wedged sink would hang Run
	// indefinitely instead of surfacing as an ordinary Nak+retry cycle.
	flushInsertTimeout = 30 * time.Second

	// finalFlushTimeout bounds the last, best-effort flush performed
	// during shutdown. It's derived from context.Background() (Run's own
	// context is already Done() by the time this runs), so it needs its
	// own budget rather than inheriting one.
	finalFlushTimeout = 10 * time.Second
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
	// nc is retained (and accepted in New, mirroring worker.New's
	// constructor shape) for parity with the rest of the codebase and for
	// future direct-NATS needs (health checks, admin publishes); the
	// batcher itself only ever talks to JetStream via js.
	nc   *nats.Conn
	js   jetstream.JetStream
	sink Sink
	cfg  Config

	// runCtx is the context passed to Run, stashed so triggerFlush (called
	// from the consumer's delivery goroutine via handleMsg, not just from
	// Run's own select loop) can derive a bounded InsertBatch context from
	// it. It's written once, before cons.Consume starts delivering (and
	// therefore before handleMsg can ever read it) — the Go memory model
	// guarantees that write happens-before the goroutine Consume spawns,
	// so no further synchronization is needed for this field.
	runCtx context.Context

	// mu guards held, inFlight, and flushing. It does NOT serialize
	// InsertBatch calls themselves — those always run outside the lock
	// (see triggerFlush/finalFlush), which is what lets the heartbeat
	// ticker keep servicing held AND inFlight messages while a flush's
	// InsertBatch call is slow or wedged.
	mu       sync.Mutex
	held     []heldMsg // accumulated, not yet handed to a flush
	inFlight []heldMsg // handed to the one in-progress flush goroutine
	flushing bool      // true while a flush goroutine is running

	// flushWG tracks outstanding flush goroutines so shutdown can wait for
	// any in-progress flush to resolve (ack or nak) before performing the
	// final flush of whatever is left in held.
	flushWG sync.WaitGroup

	onRowMu sync.Mutex
	onRow   func(Row)

	// metrics is this Harvester's own private Prometheus registry (Task 6
	// / ADR-0028 metrics floor) — see metrics.go's package doc comment for
	// why per-instance, never global.
	metrics *hvMetrics
}

// New constructs a Harvester. cfg is completed with defaults for any
// unset field (see Config.withDefaults).
func New(nc *nats.Conn, js jetstream.JetStream, sink Sink, cfg Config) *Harvester {
	return &Harvester{
		nc:      nc,
		js:      js,
		sink:    sink,
		cfg:     cfg.withDefaults(),
		metrics: newHVMetrics(),
	}
}

// Handler serves this Harvester's Prometheus metrics
// (inferbus_usage_rows_inserted_total, inferbus_insert_failures_total, and
// inferbus_budget_entries once a BudgetLedger is wired via
// SetBudgetEntries/SetBudgetGaugeFunc) in the Prometheus text exposition
// format. The role's probe HTTP server (cmd/inferbus/roles.go) mounts this
// at /metrics.
func (h *Harvester) Handler() http.Handler {
	return h.metrics.Handler()
}

// SetBudgetEntries sets (never increments/decrements) this Harvester's
// inferbus_budget_entries{state="ok"|"exceeded"} gauge from a
// BudgetLedger's own authoritative current counts. Wire it via
// ledger.SetBudgetGaugeFunc(h.SetBudgetEntries) — metrics live on the
// Harvester instance, and the ledger is a separate object with no metrics
// of its own, so this is the seam that lets the two share one gauge
// without either depending on the other's concrete type.
func (h *Harvester) SetBudgetEntries(ok, exceeded int) {
	h.metrics.setBudgetEntries(ok, exceeded)
}

// OnRow registers fn to be called once per successfully-inserted row,
// after that row's message has been acked. fn is called synchronously from
// a flush goroutine — it must not block for long, and must be safe to call
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
// shutdown and returns ctx.Err() on a normal shutdown, or a non-nil error
// if the consumer dies unexpectedly while ctx is still live.
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
		// held-or-in-flight message stays un-acked (and thus ack-pending)
		// until its batch flushes. Note: MaxAckPending is a consumer-wide
		// budget shared across replicas; multi-replica tuning is future work.
		MaxAckPending: h.cfg.BatchMaxEvents * 4,
	})
	if err != nil {
		return err
	}

	// Safe to write unsynchronized: no messages can reach handleMsg until
	// cons.Consume below starts the delivery goroutine, and this write
	// happens-before that goroutine's creation (see the runCtx field doc).
	h.runCtx = ctx

	cc, err := cons.Consume(h.handleMsg)
	if err != nil {
		return err
	}

	flushTicker := time.NewTicker(h.cfg.BatchMaxInterval)
	defer flushTicker.Stop()
	hbTicker := time.NewTicker(heldInProgressInterval)
	defer hbTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Stop the consumer FIRST so no further messages arrive and
			// any handleMsg call already in progress finishes, THEN wait
			// for any flush goroutine that was already in flight, THEN do
			// one best-effort final flush of whatever is left in held.
			// This ordering (mirroring internal/worker/worker.go's
			// Stop-then-drain idiom) guarantees that by the time Run
			// returns, every message the consumer ever delivered has been
			// acked, nak'd, term'd, or covered by the final flush's own
			// outcome — never left dangling.
			cc.Stop()
			<-cc.Closed()
			h.flushWG.Wait()
			h.finalFlush()
			return ctx.Err()
		case <-flushTicker.C:
			h.triggerFlush()
		case <-hbTicker.C:
			h.heartbeatHeld()
		case <-cc.Closed():
			// The consumer's delivery loop exited on its own (e.g. the
			// consumer was deleted out from under us, or the connection
			// was permanently lost) while ctx is still live — this is not
			// the intentional shutdown path above (that path returns
			// directly and never re-enters this select). Surface it
			// loudly rather than silently stopping work.
			err := fmt.Errorf("harvester: metering consumer %q closed unexpectedly", consumerDurable)
			slog.Error(err.Error())
			return err
		}
	}
}

// handleMsg decodes one message and appends it to the held batch,
// triggering a flush if that fills the batch. An undecodable payload, or
// one with an empty ReqID, is terminated (never redelivered) rather than
// held: redelivery of garbage can never succeed, and an empty ReqID must
// never be allowed to reach the dedup-by-ReqID logic in flush, where it
// would incorrectly collapse distinct events that all happen to be
// missing a ReqID into a single row.
func (h *Harvester) handleMsg(msg jetstream.Msg) {
	var ev wire.UsageEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		h.termInvalid(msg, "undecodable METERING payload, terminating", err)
		return
	}
	if ev.ReqID == "" {
		h.termInvalid(msg, "METERING event with empty req_id, terminating", nil)
		return
	}

	row := RowFromEvent(ev)

	h.mu.Lock()
	h.held = append(h.held, heldMsg{row: row, msg: msg})
	full := len(h.held) >= h.cfg.BatchMaxEvents
	h.mu.Unlock()

	if full {
		h.triggerFlush()
	}
}

// termInvalid terminates msg and logs why. If Term itself fails (e.g. a
// transient connection hiccup), the message is simply left un-acked:
// JetStream redelivers it after AckWait, and this same code path — decode,
// fail the same way, attempt Term again — runs again on that redelivery.
// That's a bounded retry-on-redelivery loop, not a tight one: each attempt
// is at least AckWait (60s) apart, so a persistently failing Term degrades
// to "occasionally retries forever" rather than spinning.
func (h *Harvester) termInvalid(msg jetstream.Msg, reason string, err error) {
	if err != nil {
		slog.Warn("harvester: "+reason, "err", err)
	} else {
		slog.Warn("harvester: " + reason)
	}
	if termErr := msg.Term(); termErr != nil {
		slog.Warn("harvester: term failed, will retry on next redelivery", "err", termErr)
	}
}

// triggerFlush hands off everything currently held to a new flush
// goroutine, unless a flush is already in progress (flushing), in which
// case it's a no-op: held keeps accumulating and the next tick (interval
// ticker or a subsequent batch-full trigger) will pick up the larger
// accumulated batch once the in-progress flush finishes. This — spawning
// per flush behind a single-flight guard, rather than running flush
// inline on Run's select loop — is what lets hbTicker keep servicing
// (including the inFlight messages of the flush currently running) while
// InsertBatch is slow.
func (h *Harvester) triggerFlush() {
	h.mu.Lock()
	if len(h.held) == 0 || h.flushing {
		h.mu.Unlock()
		return
	}
	batch := h.held
	h.held = nil
	h.inFlight = batch
	h.flushing = true
	h.mu.Unlock()

	ctx, cancel := context.WithTimeout(h.runCtx, flushInsertTimeout)
	h.flushWG.Add(1)
	go func() {
		defer h.flushWG.Done()
		defer cancel()
		h.doFlush(ctx, batch)
		h.mu.Lock()
		h.inFlight = nil
		h.flushing = false
		h.mu.Unlock()
	}()
}

// finalFlush synchronously flushes whatever remains in held. It must only
// be called after the consumer has been fully stopped (so held can no
// longer grow) and after flushWG.Wait() (so no concurrent flush goroutine
// can be racing it for the flushing/inFlight state).
func (h *Harvester) finalFlush() {
	h.mu.Lock()
	if len(h.held) == 0 {
		h.mu.Unlock()
		return
	}
	batch := h.held
	h.held = nil
	h.inFlight = batch
	h.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), finalFlushTimeout)
	defer cancel()
	h.doFlush(ctx, batch)

	h.mu.Lock()
	h.inFlight = nil
	h.mu.Unlock()
}

// doFlush dedups batch by ReqID (first occurrence wins) and inserts the
// deduped rows into the sink. On success every message in batch is acked,
// and OnRow fires once per row that was actually inserted (not for
// in-batch duplicates, which are acked without ever reaching the sink or
// firing OnRow). On failure — including ctx expiring against
// flushInsertTimeout/finalFlushTimeout, a wedged sink looks exactly like
// any other insert failure here — every message in batch is Nak'd with a
// fixed delay for redelivery, and nothing is acked or reported via OnRow.
func (h *Harvester) doFlush(ctx context.Context, batch []heldMsg) {
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
		// Same deduped-row dimension as rowsInserted below, so the two
		// counters can be compared and summed meaningfully.
		h.metrics.insertFailures.Add(float64(len(rows)))
		for _, item := range batch {
			if nakErr := item.msg.NakWithDelay(nakRedeliverDelay); nakErr != nil {
				slog.Warn("harvester: nak failed", "req_id", item.row.ReqID, "err", nakErr)
			}
		}
		return
	}
	h.metrics.rowsInserted.Add(float64(len(rows)))

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

// heartbeatHeld sends an InProgress ack-request for every message
// currently held (batched but not yet handed to a flush) AND every
// message currently in flight (handed to a flush goroutine whose
// InsertBatch call hasn't returned yet), resetting each one's redelivery
// timer per the binding ruling in the task brief. Covering inFlight as
// well as held is what prevents a slow InsertBatch call from starving its
// own batch's messages of heartbeats.
func (h *Harvester) heartbeatHeld() {
	h.mu.Lock()
	snapshot := make([]heldMsg, 0, len(h.held)+len(h.inFlight))
	snapshot = append(snapshot, h.held...)
	snapshot = append(snapshot, h.inFlight...)
	h.mu.Unlock()

	for _, item := range snapshot {
		if err := item.msg.InProgress(); err != nil {
			slog.Warn("harvester: InProgress failed for held message", "req_id", item.row.ReqID, "err", err)
		}
	}
}
