// budget.go implements the M4 Task 5 budget ledger: it watches the KEYS KV
// bucket to learn which keys carry a non-zero monthly token budget, tracks
// each budgeted key's month-to-date usage (seeded from Sink.MonthToDate and
// then kept current via AddUsage, wired to Harvester.OnRow), and
// periodically flushes the result into the BUDGETS KV bucket as
// cpkv.BudgetEntry rows (docs/design-usage.md §"Budget ledger").
//
// KEYS is keyed by the API key's hash, not its stable id, and a key
// rotation can leave two hash entries carrying the same Id in the bucket at
// once (binding ruling #1). The ledger folds KEYS by that Id field: a
// logical key exists exactly while any hash entry carries its Id, and its
// budget is that entry's MonthlyTokenBudget (entries sharing an Id should
// agree; if they diverge, this uses the max and logs once).
//
// Error posture (binding ruling #2): every KV/Sink error encountered here
// is logged and retried on the next periodic tick. This is an advisory
// system, not a source of truth — it must never fail-stop, never crash, and
// AddUsage in particular must never block on I/O (it only ever touches an
// in-memory map under a mutex).
//
// Concurrency (Task 5 fix round, C1/I1): every mutating access to a
// ledgerRow bumps its version counter. flushOne snapshots a row's data and
// version before its KV Put/Delete round-trip, then only reconciles the
// ledger's in-memory state afterward if the version is unchanged — this is
// what stops a KEYS update racing a flush from either (C1) un-tracking a
// key that was resurrected mid-delete, or (I1) silently clearing the dirty
// flag on a row that AddUsage mutated while a stale Put was in flight.
//
// Freshness (I2/I3): a periodic full re-baseline (rebaselineAll, every
// rebaselineInterval) re-queries Sink.MonthToDate for every already-loaded
// budgeted row and corrects any drift — including the residual double-count
// window inherent in seeding a brand-new row's baseline concurrently with
// live AddUsage calls (see loadBaseline's doc comment). Separately,
// flushOne refuses to Put a row whose baseline for its CURRENT month hasn't
// landed yet (baselineLoaded == false) — this is what stops a month
// rollover from ever publishing an entry that mixes the new month's label
// with the old month's (or a mid-flight, partially-accumulated) Used value;
// AddUsage keeps accumulating in memory during that window regardless, and
// the entry is published in full once the new month's baseline lands.
//
// Watcher purity (I4): the KEYS watch goroutine (watchKeys and everything
// it calls — consumeKeys, applyKeysSnapshot, reconcileKeys) never performs
// Sink or KV I/O. It only ever mutates the in-memory ledger under mu; every
// actual MonthToDate/Put/Delete call happens on Run's own goroutine
// (flushTick's pending-baseline retry, checkMonthRollover, rebaselineAll,
// and flushOne), so a wedged sink or KV store can never stall live KEYS
// delivery.
package harvester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/cpkv"
)

// budgetOpTimeout bounds every individual Sink/KV call the ledger makes
// outside of AddUsage (MonthToDate baseline/re-baseline reads, BUDGETS
// Put/Delete) — without this a wedged sink or KV store would hang a flush
// tick indefinitely instead of surfacing as an ordinary log-and-retry-next-
// tick failure (binding ruling #2).
const budgetOpTimeout = 10 * time.Second

// defaultRebaselineInterval is how often rebaselineAll re-queries
// Sink.MonthToDate for every already-loaded budgeted row (I2/I3), absent an
// explicit SetRebaselineInterval override.
const defaultRebaselineInterval = time.Hour

// ledgerRow is the budget ledger's private per-key-id working state. It is
// only ever accessed under BudgetLedger.mu.
type ledgerRow struct {
	budget         int64
	used           int64
	month          string
	exceeded       bool
	baselineLoaded bool // true once a Sink.MonthToDate call has landed for the row's current month
	toDelete       bool // true once the key's budget dropped to 0 or the key vanished from KEYS entirely

	// version is bumped by every mutating access (AddUsage, reconcileKeys,
	// loadBaseline, checkMonthRollover). flushOne snapshots it alongside
	// the row's data before a KV round-trip and only reconciles
	// afterward if it's unchanged — see the package doc comment's C1/I1
	// paragraph.
	version uint64
}

// BudgetLedger projects the KEYS bucket's per-key monthly_token_budget,
// combined with usage rows observed via AddUsage/Sink.MonthToDate, into the
// BUDGETS KV bucket. See the package doc comment above for the overall
// design; NewBudgetLedger/Run/AddUsage are its only production-facing
// surface (SetNowFn/SetRebaselineInterval exist for deterministic testing).
type BudgetLedger struct {
	js      jetstream.JetStream
	sink    Sink
	refresh time.Duration

	// runCtx is Run's context, stashed so goroutines other than Run's own
	// select loop (the KEYS watch loop) can derive bounded operation
	// contexts from it. It's written once, before the watch goroutine is
	// spawned — mirroring harvester.go's runCtx field, whose doc comment
	// explains why that write needs no further synchronization: the write
	// happens-before the goroutine's creation.
	runCtx context.Context

	// kv is the BUDGETS bucket handle. It is only ever read or written on
	// Run's own goroutine (Run's setup line, and flushOne's M3
	// re-ensure-on-bucket-not-found path) — the KEYS watch goroutine never
	// touches it (I4) — so, like runCtx, it needs no mutex.
	kv jetstream.KeyValue

	mu                 sync.Mutex
	nowFn              func() time.Time
	rebaselineInterval time.Duration
	month              string // last wall-clock month ("2006-01") observed by checkMonthRollover; "" until the first tick
	entries            map[string]*ledgerRow
	dirty              map[string]bool // key ids with in-memory state not yet reflected in BUDGETS

	// testAfterKVOp, if non-nil, is invoked synchronously by flushOne
	// immediately after its KV Put/Delete call returns (success or
	// failure) but before flushOne re-locks to reconcile the ledger's
	// in-memory state against what was written. This is test-only
	// instrumentation (internal white-box tests) for deterministically
	// driving the C1/I1 race windows without relying on real goroutine
	// scheduling; it is always nil, and never invoked, in production.
	testAfterKVOp func()
}

// NewBudgetLedger constructs a BudgetLedger. It does nothing until Run is
// called; refresh controls both how often dirty entries are flushed to
// BUDGETS and how often month rollover is checked. A non-positive refresh
// (M1: a caller mistake, since time.NewTicker panics on <= 0) falls back to
// the harvester package's own documented default rather than panicking.
func NewBudgetLedger(js jetstream.JetStream, sink Sink, refresh time.Duration) *BudgetLedger {
	if refresh <= 0 {
		refresh = defaultBudgetRefreshInterval
	}
	return &BudgetLedger{
		js:                 js,
		sink:               sink,
		refresh:            refresh,
		rebaselineInterval: defaultRebaselineInterval,
		nowFn:              time.Now,
		entries:            map[string]*ledgerRow{},
		dirty:              map[string]bool{},
	}
}

// SetNowFn overrides the ledger's clock (default time.Now), letting tests
// drive month rollover deterministically (binding ruling #3). Safe to call
// concurrently with Run; call it before Run starts for a race-free initial
// month, since Run's first tick reads whatever nowFn is current at that
// moment.
func (l *BudgetLedger) SetNowFn(fn func() time.Time) {
	l.mu.Lock()
	l.nowFn = fn
	l.mu.Unlock()
}

func (l *BudgetLedger) now() time.Time {
	l.mu.Lock()
	fn := l.nowFn
	l.mu.Unlock()
	return fn()
}

// SetRebaselineInterval overrides the ledger's periodic full re-baseline
// interval (default 1h; see rebaselineAll's doc comment for why this
// exists), letting tests exercise I2/I3's drift correction without waiting
// an hour. Call before Run — like SetNowFn, Run reads this exactly once, at
// startup, to size its ticker; a non-positive duration is ignored.
func (l *BudgetLedger) SetRebaselineInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	l.mu.Lock()
	l.rebaselineInterval = d
	l.mu.Unlock()
}

func (l *BudgetLedger) getRebaselineInterval() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rebaselineInterval
}

// AddUsage is wired to Harvester.OnRow: it must stay fast and
// allocation-light, since it runs synchronously on the harvester's flush
// goroutine right after each row is inserted. If row's key currently
// carries a budget, its running total is bumped and the key is marked
// dirty for the next periodic flush; otherwise this is a no-op. No I/O
// happens here — ever.
func (l *BudgetLedger) AddUsage(row Row) {
	if row.KeyID == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.entries[row.KeyID]
	if !ok || entry.toDelete {
		return
	}
	entry.used += row.TotalTokens()
	entry.exceeded = entry.used >= entry.budget
	entry.version++
	l.dirty[row.KeyID] = true
}

// Run ensures the BUDGETS bucket exists (retrying with backoff rather than
// failing outright — M3, binding ruling #2), starts the KEYS watch loop,
// and then flushes dirty entries / checks for month rollover every refresh
// interval, and fully re-baselines every live entry every
// rebaselineInterval (I2/I3), until ctx is done. It returns ctx.Err() on
// ordinary shutdown.
func (l *BudgetLedger) Run(ctx context.Context) error {
	kv, err := l.ensureBudgetsBucketWithBackoff(ctx)
	if err != nil {
		return err
	}
	l.kv = kv

	// Safe to write unsynchronized: watchKeys (and everything it calls)
	// cannot start until the goroutine below is spawned, and this write
	// happens-before that goroutine's creation (see the runCtx field doc).
	l.runCtx = ctx

	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		l.watchKeys(ctx)
	}()

	flushTicker := time.NewTicker(l.refresh)
	defer flushTicker.Stop()

	rebaselineTicker := time.NewTicker(l.getRebaselineInterval())
	defer rebaselineTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			<-watchDone
			return ctx.Err()
		case <-flushTicker.C:
			l.flushTick()
		case <-rebaselineTicker.C:
			l.rebaselineAll()
		}
	}
}

// budgetsKVConfig pins the BUDGETS bucket's KeyValueConfig exactly like
// controlplane/kvproj.go's kvConfig pins ALIASES/KEYS/CP_MARKERS (same M3
// idiom; reused here rather than imported since internal/harvester must not
// depend on internal/controlplane): History 1 (this is a current-state
// projection, not an audit trail — the usage ledger in the Sink's backing
// store is the durable history) and FileStorage (durable across a
// restart). Replicas is deliberately left at its default (1), same
// rationale as kvproj.go: single-node JetStream in M3/M4; revisit once the
// control plane is clustered.
func budgetsKVConfig() jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket:  cpkv.BucketBudgets,
		History: 1,
		Storage: jetstream.FileStorage,
	}
}

// ensureBudgetsBucket idempotently creates (or adopts an existing) BUDGETS
// bucket.
func ensureBudgetsBucket(ctx context.Context, js jetstream.JetStream) (jetstream.KeyValue, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, budgetsKVConfig())
	if err != nil {
		return nil, fmt.Errorf("harvester: budget ledger: ensure BUDGETS bucket: %w", err)
	}
	return kv, nil
}

// ensureBudgetsBucketWithBackoff retries ensureBudgetsBucket, using the
// same backoff bounds as the KEYS watch loop, until it succeeds or ctx is
// done (M3 / binding ruling #2: a transient JetStream hiccup at Run
// startup must not fail-stop the whole harvester process — only ctx
// cancellation ends this loop).
func (l *BudgetLedger) ensureBudgetsBucketWithBackoff(ctx context.Context) (jetstream.KeyValue, error) {
	backoff := budgetKeysWatchMinBackoff
	attempt := 0
	for {
		attempt++
		kv, err := ensureBudgetsBucket(ctx, l.js)
		if err == nil {
			return kv, nil
		}
		logBudgetFailure(cpkv.BucketBudgets, "ensure", attempt, err)
		if !budgetSleepBackoff(ctx, &backoff) {
			return nil, ctx.Err()
		}
	}
}

// reensureBudgetsBucket re-creates the BUDGETS bucket after a Put/Delete
// reports it missing (M3 — e.g. an operator manually deleted it, or an
// as-yet-unknown admin operation reset it). flushOne is only ever called
// from Run's own goroutine, so mutating l.kv here needs no extra
// synchronization (see the kv field's doc comment).
func (l *BudgetLedger) reensureBudgetsBucket(ctx context.Context) {
	kv, err := ensureBudgetsBucket(ctx, l.js)
	if err != nil {
		slog.Warn("harvester: budget ledger: re-ensure BUDGETS bucket failed, will retry", "err", err)
		return
	}
	l.kv = kv
	slog.Info("harvester: budget ledger: BUDGETS bucket re-created, resuming")
}

// The KEYS-watch resilience shape below (backoff constants,
// budgetKeysStreamCreated, budgetProbeKeysIdentity, and the
// connect/watch/reconnect loop in watchKeys) is copied from
// internal/gateway/kviam.go's watchKV/probeBucketIdentity/kvStreamCreated
// (its sibling implementation) rather than imported — the task brief
// requires internal/harvester not depend on internal/gateway. The budget
// ledger needs the exact same resilience: an admin resync
// (controlplane.ResyncKV's resetKVBucket) destroys and recreates KEYS
// wholesale, which kills any live watch subscribed to it, and a missing
// KEYS bucket at startup (the control plane may start later) must be
// polled with backoff rather than treated as fatal. Unlike KVIAM this
// version has no need for a Ready()/atomic-map-swap surface — it drives
// applyKeysSnapshot(map[string]jetstream.KeyValueEntry) directly instead.
const (
	budgetKeysWatchMinBackoff = 200 * time.Millisecond
	budgetKeysWatchMaxBackoff = 10 * time.Second
	budgetKeysProbeInterval   = 5 * time.Second
)

// budgetLogAlwaysUntil/budgetLogEveryFailures rate-limit the warn-level
// logging of repeated connect/setup failures (I5 — restoring the pattern
// from internal/gateway/kviam.go's kvLogAlwaysUntil/kvLogEveryFailures):
// the first few attempts always log (an operator watching a fresh deploy
// wants to see trouble immediately), then only every
// budgetLogEveryFailures'th attempt after that — otherwise a bucket that
// stays missing/unreachable for a long time logs a warning forever.
const (
	budgetLogAlwaysUntil   = 3
	budgetLogEveryFailures = 10
)

// logBudgetFailure emits a rate-limited warn log for a failed connect/setup
// operation against bucket (op is e.g. "KeyValue", "stream info",
// "WatchAll", or "ensure") — see budgetLogAlwaysUntil/budgetLogEveryFailures's
// doc comment for the rate-limiting rule.
func logBudgetFailure(bucket, op string, attempt int, err error) {
	if attempt <= budgetLogAlwaysUntil || attempt%budgetLogEveryFailures == 0 {
		slog.Warn("harvester: budget ledger: connect attempt failed", "bucket", bucket, "op", op, "attempt", attempt, "err", err)
	}
}

// watchKeys runs the KEYS bucket's scan+watch+probe loop until ctx is done,
// applying a full snapshot fold (applyKeysSnapshot) on every change. See
// the const block doc comment above for why this mirrors
// internal/gateway/kviam.go's watchKV. Per I4, this function and everything
// it calls (consumeKeys, applyKeysSnapshot, reconcileKeys) must never
// perform Sink or KV I/O.
func (l *BudgetLedger) watchKeys(ctx context.Context) {
	backoff := budgetKeysWatchMinBackoff
	attempt := 0

	for ctx.Err() == nil {
		attempt++

		kv, err := l.js.KeyValue(ctx, cpkv.BucketKeys)
		if err != nil {
			logBudgetFailure(cpkv.BucketKeys, "KeyValue", attempt, err)
			if !budgetSleepBackoff(ctx, &backoff) {
				return
			}
			continue
		}

		created, err := budgetKeysStreamCreated(ctx, l.js)
		if err != nil {
			logBudgetFailure(cpkv.BucketKeys, "stream info", attempt, err)
			if !budgetSleepBackoff(ctx, &backoff) {
				return
			}
			continue
		}

		// connCtx bounds this one connection attempt: it dies either with
		// the parent ctx (real shutdown) or when the identity probe (or
		// the watcher's own channel closing) decides to reconnect.
		connCtx, cancelConn := context.WithCancel(ctx)
		watcher, err := kv.WatchAll(connCtx)
		if err != nil {
			cancelConn()
			logBudgetFailure(cpkv.BucketKeys, "WatchAll", attempt, err)
			if !budgetSleepBackoff(ctx, &backoff) {
				return
			}
			continue
		}
		slog.Info("harvester: budget ledger: connected to KEYS", "attempt", attempt)

		probeDone := make(chan struct{})
		go budgetProbeKeysIdentity(connCtx, l.js, created, cancelConn, probeDone)

		synced := l.consumeKeys(connCtx, watcher)

		cancelConn()
		<-probeDone // avoid leaking the probe goroutine across reconnects
		_ = watcher.Stop()

		if synced {
			// Only reset backoff/attempt bookkeeping once this connection
			// actually reached a successful first sync — mirrors kviam.go's
			// I1 ruling.
			backoff = budgetKeysWatchMinBackoff
			attempt = 0
		}
	}
}

// budgetKeysStreamCreated fetches the Created timestamp of the JetStream
// stream backing the KEYS bucket ("KV_KEYS") — see
// internal/gateway/kviam.go's kvStreamCreated for the full rationale
// (destroy+recreate always produces a new Created time).
func budgetKeysStreamCreated(ctx context.Context, js jetstream.JetStream) (time.Time, error) {
	strm, err := js.Stream(ctx, "KV_"+cpkv.BucketKeys)
	if err != nil {
		return time.Time{}, err
	}
	info, err := strm.Info(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return info.Created, nil
}

// budgetProbeKeysIdentity polls budgetKeysStreamCreated every
// budgetKeysProbeInterval and calls cancelConn (forcing watchKeys to
// reconnect from scratch) the moment it observes either an error or a
// Created timestamp different from baseline — see
// internal/gateway/kviam.go's probeBucketIdentity for the full rationale.
// It always closes done before returning.
func budgetProbeKeysIdentity(connCtx context.Context, js jetstream.JetStream, baseline time.Time, cancelConn context.CancelFunc, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(budgetKeysProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-connCtx.Done():
			return
		case <-ticker.C:
			created, err := budgetKeysStreamCreated(connCtx, js)
			if err != nil {
				if connCtx.Err() != nil {
					return // shutting down/reconnecting already
				}
				slog.Warn("harvester: budget ledger: KEYS identity probe failed, forcing reconnect", "err", err)
				cancelConn()
				return
			}
			if !created.Equal(baseline) {
				slog.Info("harvester: budget ledger: KEYS bucket identity changed, reconnecting")
				cancelConn()
				return
			}
		}
	}
}

// budgetSleepBackoff sleeps for *backoff (or returns false immediately if
// ctx is already done), then doubles *backoff up to budgetKeysWatchMaxBackoff.
func budgetSleepBackoff(ctx context.Context, backoff *time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(*backoff):
	}
	*backoff *= 2
	if *backoff > budgetKeysWatchMaxBackoff {
		*backoff = budgetKeysWatchMaxBackoff
	}
	return true
}

// consumeKeys reads watcher.Updates() into working, calling
// applyKeysSnapshot with a full snapshot of working every time it changes
// (including once at the end of the initial replay). It returns whether the
// initial replay ever completed on this connection — mirrors
// internal/gateway/kviam.go's consumeKV.
func (l *BudgetLedger) consumeKeys(ctx context.Context, watcher jetstream.KeyWatcher) bool {
	working := map[string]jetstream.KeyValueEntry{}
	synced := false
	for {
		select {
		case <-ctx.Done():
			return synced
		case entry, ok := <-watcher.Updates():
			if !ok {
				return synced
			}
			if entry == nil {
				// nil entry marks the end of the initial value replay.
				synced = true
				l.applyKeysSnapshot(working)
				continue
			}
			switch entry.Operation() {
			case jetstream.KeyValuePut:
				working[entry.Key()] = entry
			case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
				delete(working, entry.Key())
			}
			if synced {
				l.applyKeysSnapshot(working)
			}
		}
	}
}

// keyFold is applyKeysSnapshot's per-Id accumulator while folding a KEYS
// snapshot (binding ruling #1).
type keyFold struct {
	budget   int64
	diverged bool // true once a second, disagreeing MonthlyTokenBudget has been logged for this Id
}

// applyKeysSnapshot decodes every entry in a full KEYS snapshot and folds
// them by their Id field (binding ruling #1): multiple hash entries with
// the same Id collapse into one logical key, whose budget is that entry's
// MonthlyTokenBudget (entries sharing an Id should agree; if they diverge,
// the max is used and a warning is logged once per snapshot application,
// not once per divergent entry). Entries with no Id (pre-M4 KEYS rows,
// cpkv.KeyEntry's doc comment) carry nothing to attribute a budget to and
// are skipped — M6: such a key therefore receives no budget enforcement at
// all, by design, until its KEYS entry is rewritten with an Id.
func (l *BudgetLedger) applyKeysSnapshot(entries map[string]jetstream.KeyValueEntry) {
	folded := map[string]*keyFold{}
	for hash, e := range entries {
		var row cpkv.KeyEntry
		if err := json.Unmarshal(e.Value(), &row); err != nil {
			slog.Error("harvester: budget ledger: decode KEYS entry", "hash", truncateHash(hash), "err", err)
			continue
		}
		if row.Id == "" {
			continue
		}
		f, ok := folded[row.Id]
		if !ok {
			folded[row.Id] = &keyFold{budget: row.MonthlyTokenBudget}
			continue
		}
		if row.MonthlyTokenBudget != f.budget {
			if !f.diverged {
				slog.Warn("harvester: budget ledger: KEYS entries for key id disagree on monthly_token_budget, using max",
					"id", row.Id, "a", f.budget, "b", row.MonthlyTokenBudget)
				f.diverged = true
			}
			if row.MonthlyTokenBudget > f.budget {
				f.budget = row.MonthlyTokenBudget
			}
		}
	}
	l.reconcileKeys(folded)
}

// reconcileKeys diffs folded (this KEYS snapshot's Id -> budget fold)
// against the ledger's current entries: new budgeted ids get a fresh row
// (baselineLoaded starts false; its baseline is fetched later, off this
// goroutine — see loadBaseline's I4 note and flushTick's pending-baseline
// retry), ids whose budget changed get their Budget/Exceeded updated in
// place, and ids that dropped to budget 0 or vanished entirely from folded
// are marked for deletion from BUDGETS (binding ruling #4 / brief step e).
// Every mutation bumps the row's version (C1/I1's optimistic-concurrency
// guard against flushOne's KV round-trip).
//
// I4: this function must never perform Sink or KV I/O — it runs on the
// KEYS watch goroutine (via applyKeysSnapshot/consumeKeys/watchKeys), which
// must stay pure so a slow or wedged sink/KV call can never stall live
// KEYS delivery.
func (l *BudgetLedger) reconcileKeys(folded map[string]*keyFold) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for id, f := range folded {
		if f.budget <= 0 {
			if row, ok := l.entries[id]; ok && !row.toDelete {
				row.toDelete = true
				row.version++
				l.dirty[id] = true
			}
			continue
		}
		row, ok := l.entries[id]
		if !ok {
			l.entries[id] = &ledgerRow{budget: f.budget, month: l.currentMonthLocked()}
			continue
		}
		changed := false
		if row.toDelete {
			row.toDelete = false // a key can be resurrected before a pending delete flushes
			changed = true
		}
		if row.budget != f.budget {
			row.budget = f.budget
			row.exceeded = row.used >= row.budget
			changed = true
		}
		if changed {
			row.version++
			l.dirty[id] = true
		}
	}
	for id, row := range l.entries {
		if _, ok := folded[id]; !ok && !row.toDelete {
			row.toDelete = true
			row.version++
			l.dirty[id] = true
		}
	}
}

// currentMonthLocked returns the ledger's current month string, computing
// it from nowFn if this is the very first key the ledger has ever seen
// (l.month is only otherwise set by checkMonthRollover's periodic tick).
// Callers must hold l.mu.
func (l *BudgetLedger) currentMonthLocked() string {
	if l.month == "" {
		l.month = l.nowFn().Format("2006-01")
	}
	return l.month
}

// loadBaseline fetches id's month-to-date usage from the sink and, if the
// row is still live and still on the same month (not deleted, or rolled
// over again, out from under it while the fetch was in flight), sets its
// Used field from the result.
//
// version is only bumped, and dirty only (re)marked, when the fetched
// value actually differs from what's already recorded (or this is the
// row's first successful load) — UNLESS forceDirty is true. Initial-
// creation and rollover callers (flushTick's pending-baseline retry,
// checkMonthRollover) always pass forceDirty=true: a brand-new row or a
// freshly rolled-over month must publish once, even if the fetched value
// happens to numerically match what was already there (e.g. two
// consecutive zero-usage months). The periodic full re-baseline
// (rebaselineAll, I2/I3) passes forceDirty=false, so a stable row doesn't
// churn a needless KV write every rebaselineInterval — but a genuine
// difference (drift) still gets version-bumped and published exactly like
// any other mutation, correcting known races such as a brand-new row's
// baseline landing concurrently with an AddUsage call for a row that
// baseline already counted (a residual, documented double-count window;
// re-baseline self-heals it on its next pass since MonthToDate is always
// authoritative).
//
// On a Sink error this logs and leaves baselineLoaded false (if it wasn't
// already true), so flushTick's pending-baseline retry picks it up again
// next tick — binding ruling #2: log + retry next tick, never fail-stop.
//
// I4: this performs Sink I/O and must only ever be called from Run's own
// goroutine (flushTick's pending-retry loop, checkMonthRollover, or
// rebaselineAll) — never from the KEYS watch goroutine.
func (l *BudgetLedger) loadBaseline(id string, forceDirty bool) {
	l.mu.Lock()
	row, ok := l.entries[id]
	if !ok || row.toDelete {
		l.mu.Unlock()
		return
	}
	month := row.month
	l.mu.Unlock()

	ctx, cancel := context.WithTimeout(l.runCtx, budgetOpTimeout)
	defer cancel()
	used, err := l.sink.MonthToDate(ctx, id, month)
	if err != nil {
		slog.Warn("harvester: budget ledger: MonthToDate baseline failed, will retry", "id", id, "month", month, "err", err)
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	row, ok = l.entries[id]
	if !ok || row.toDelete || row.month != month {
		return // key vanished, or another rollover already moved it on
	}
	changed := row.used != used || !row.baselineLoaded
	row.used = used
	row.exceeded = row.used >= row.budget
	row.baselineLoaded = true
	if changed {
		row.version++
	}
	if changed || forceDirty {
		l.dirty[id] = true
	}
}

// rebaselineAll re-runs MonthToDate for every live, already-baselined
// budgeted row (I2/I3): a periodic, self-healing correction for any drift
// between the ledger's in-memory Used and the sink's true state — notably
// the documented AddUsage-vs-initial-baseline race (loadBaseline's doc
// comment). Rows still waiting on their first baseline, or mid-rollover,
// are left to flushTick's own pending-retry path, which already covers
// them; re-running MonthToDate for those here too would be redundant.
func (l *BudgetLedger) rebaselineAll() {
	l.mu.Lock()
	ids := make([]string, 0, len(l.entries))
	for id, row := range l.entries {
		if row.toDelete || !row.baselineLoaded {
			continue
		}
		ids = append(ids, id)
	}
	l.mu.Unlock()

	for _, id := range ids {
		l.loadBaseline(id, false)
	}
}

// flushTick is called once per refresh interval from Run's select loop. It
// checks for month rollover, retries any baseline fetch that failed (or
// hasn't happened yet), and flushes every dirty entry to BUDGETS.
func (l *BudgetLedger) flushTick() {
	l.checkMonthRollover()

	l.mu.Lock()
	var pending []string
	for id, row := range l.entries {
		if !row.baselineLoaded && !row.toDelete {
			pending = append(pending, id)
		}
	}
	dirtyIDs := make([]string, 0, len(l.dirty))
	for id := range l.dirty {
		dirtyIDs = append(dirtyIDs, id)
	}
	l.mu.Unlock()

	for _, id := range pending {
		l.loadBaseline(id, true)
	}
	for _, id := range dirtyIDs {
		l.flushOne(id)
	}
}

// checkMonthRollover compares the wall-clock month (via nowFn) against the
// last month it observed; on a change (binding ruling #3) it rewrites every
// live budgeted entry's month and schedules a fresh MonthToDate baseline
// for the new month (typically 0), discarding the prior month's Used only
// once that new baseline actually lands (flushOne refuses to publish a row
// with baselineLoaded == false — see the package doc comment's Freshness
// paragraph — so the stale month's entry stays untouched in BUDGETS in the
// meantime).
func (l *BudgetLedger) checkMonthRollover() {
	newMonth := l.now().Format("2006-01")

	l.mu.Lock()
	if l.month == "" {
		l.month = newMonth
		l.mu.Unlock()
		return
	}
	if l.month == newMonth {
		l.mu.Unlock()
		return
	}
	l.month = newMonth

	var ids []string
	for id, row := range l.entries {
		if row.toDelete {
			continue
		}
		row.month = newMonth
		row.baselineLoaded = false
		row.version++
		ids = append(ids, id)
	}
	l.mu.Unlock()

	for _, id := range ids {
		l.loadBaseline(id, true)
	}
}

// flushOne resolves one dirty key id: either deleting its BUDGETS entry (if
// marked toDelete) or Put-ing its current in-memory state — unless
// baselineLoaded is false for a non-delete row (I2/I3: never publish a row
// whose Used is known-incomplete for its current month; it stays dirty and
// gets retried, and AddUsage keeps accumulating in memory in the meantime).
//
// C1/I1: version is snapshotted alongside the row's data before the KV
// round-trip and re-checked afterward under the lock; if anything mutated
// the row in between (a KEYS update resurrecting a to-be-deleted key, or an
// AddUsage call), flushOne does NOT untrack/declare-clean based on stale
// data — the delete path leaves the (now-resurrected) row tracked, and the
// Put path leaves the row dirty so the next tick republishes the newer
// state.
//
// Any KV failure is logged and leaves the id dirty for the next tick
// (binding ruling #2); a "bucket not found" error additionally triggers an
// M3 re-ensure of the BUDGETS bucket before the next attempt.
func (l *BudgetLedger) flushOne(id string) {
	l.mu.Lock()
	row, ok := l.entries[id]
	if !ok {
		delete(l.dirty, id)
		l.mu.Unlock()
		return
	}
	version := row.version
	toDelete := row.toDelete || row.budget <= 0
	if !toDelete && !row.baselineLoaded {
		l.mu.Unlock()
		return
	}
	entry := cpkv.BudgetEntry{Used: row.used, Budget: row.budget, Exceeded: row.exceeded, Month: row.month}
	l.mu.Unlock()

	ctx, cancel := context.WithTimeout(l.runCtx, budgetOpTimeout)
	defer cancel()

	if toDelete {
		err := l.kv.Delete(ctx, id)
		if l.testAfterKVOp != nil {
			l.testAfterKVOp()
		}
		if err != nil && errors.Is(err, jetstream.ErrBucketNotFound) {
			l.reensureBudgetsBucket(ctx)
		}
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) && !errors.Is(err, jetstream.ErrBucketNotFound) {
			slog.Warn("harvester: budget ledger: delete BUDGETS entry failed, will retry", "id", id, "err", err)
			return
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		row, ok := l.entries[id]
		if !ok {
			return
		}
		if row.version != version || !row.toDelete {
			// C1: the row was resurrected (or otherwise mutated) after we
			// snapshotted it — do not untrack it based on a decision made
			// against stale data. It stays dirty (already true, since
			// whatever mutated it marked it dirty) for the next flush to
			// re-Put.
			return
		}
		delete(l.entries, id)
		delete(l.dirty, id)
		return
	}

	b, err := json.Marshal(entry)
	if err != nil {
		// Unrecoverable (a Go value that can't marshal to JSON never will):
		// log and drop it rather than retry forever.
		slog.Error("harvester: budget ledger: marshal BudgetEntry", "id", id, "err", err)
		l.mu.Lock()
		delete(l.dirty, id)
		l.mu.Unlock()
		return
	}
	_, err = l.kv.Put(ctx, id, b)
	if l.testAfterKVOp != nil {
		l.testAfterKVOp()
	}
	if err != nil && errors.Is(err, jetstream.ErrBucketNotFound) {
		l.reensureBudgetsBucket(ctx)
	}
	if err != nil {
		slog.Warn("harvester: budget ledger: put BUDGETS entry failed, will retry", "id", id, "err", err)
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	row, ok = l.entries[id]
	if !ok {
		return
	}
	if row.version != version {
		// I1: something mutated this row (e.g. AddUsage crossing the
		// budget) after we snapshotted it for the Put we just did — leave
		// it dirty so the next tick republishes the newer state, rather
		// than silently clearing dirty based on a decision made against
		// stale data.
		return
	}
	delete(l.dirty, id)
}

// truncateHash returns at most the first 8 characters of s — see
// internal/gateway/kviam.go's truncateHash: every KEYS bucket key is a
// SHA-256 hash of a real API key credential, so any hash reaching a log
// line for debugging/correlation purposes must be truncated first.
func truncateHash(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
