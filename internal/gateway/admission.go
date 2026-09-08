package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/wire"
)

const (
	// defaultRetryAfterSeconds is what a rejected client is told to wait
	// when the operator enabled admission control without naming a value.
	// Deliberately small: the point is to shed load for long enough that a
	// worker can make progress, not to park the caller.
	defaultRetryAfterSeconds = 2

	// admissionCacheTTL bounds how stale a backlog reading may be. One
	// second keeps the verdict honest under a burst while collapsing that
	// burst into a single JetStream API call per model.
	admissionCacheTTL = time.Second

	// admissionFetchTimeout caps the consumer-info round trip. Admission
	// control sits on the synchronous request path, so a JetStream that has
	// gone slow must never become the gateway's own latency: the fetch is
	// abandoned at this deadline and the request is admitted (any error
	// admits — see allow).
	admissionFetchTimeout = 500 * time.Millisecond
)

// admissionEntry is one model's cached backlog reading.
type admissionEntry struct {
	backlog uint64
	fetched time.Time
}

// admissionChecker answers "is this model's queue too deep to accept
// another request?" from JetStream consumer info, cached per model.
//
// The whole type is nil-safe: a Gateway with admission control disabled
// holds a nil *admissionChecker and allow returns true immediately, so the
// disabled path costs one nil comparison and touches no locks.
type admissionChecker struct {
	js  jetstream.JetStream
	cfg AdmissionConfig

	// nowFn and fetchFn are seams for tests: the clock so cache expiry can
	// be driven deterministically instead of by sleeping, and the fetch so
	// call counts are observable without a JetStream stand-in.
	nowFn   func() time.Time
	fetchFn func(ctx context.Context, model string) (uint64, error)

	mu    sync.Mutex
	cache map[string]admissionEntry
	// consumers memoizes the jetstream.Consumer handle per model. Resolving
	// a handle costs a CONSUMER.INFO round trip, so re-resolving it on every
	// reading doubled the RPCs — see fetchBacklog.
	consumers map[string]jetstream.Consumer
}

func newAdmissionChecker(js jetstream.JetStream, cfg AdmissionConfig) *admissionChecker {
	a := &admissionChecker{
		js:        js,
		cfg:       cfg,
		nowFn:     time.Now,
		cache:     make(map[string]admissionEntry),
		consumers: make(map[string]jetstream.Consumer),
	}
	a.fetchFn = a.fetchBacklog
	return a
}

// retryAfter is the Retry-After value (seconds) for a rejection. It
// re-applies the default that LoadConfig applies, so a Gateway built from a
// programmatically-assembled Config (tests, embedders) can never emit
// "Retry-After: 0" and invite an instant retry storm.
func (a *admissionChecker) retryAfter() int {
	if a.cfg.RetryAfterSeconds > 0 {
		return a.cfg.RetryAfterSeconds
	}
	return defaultRetryAfterSeconds
}

// limitFor returns model's backlog limit: its override if one is configured
// (including an explicit 0, which exempts the model), else MaxBacklog.
func (a *admissionChecker) limitFor(model string) int {
	if limit, ok := a.cfg.Overrides[model]; ok {
		return limit
	}
	return a.cfg.MaxBacklog
}

// allow reports whether a request for the concrete model (already resolved
// through the alias table) may be published. True admits.
//
// Failure is always open: a missing consumer, an unreachable JetStream, a
// fetch that outran admissionFetchTimeout — every error admits, and the
// admit is cached for the TTL like any other reading so a degraded
// JetStream doesn't make every request pay the timeout. Admission control
// is a backpressure signal, not a correctness gate; failing closed would
// turn a JetStream hiccup into a total outage.
func (a *admissionChecker) allow(ctx context.Context, model string) bool {
	if a == nil {
		return true
	}
	limit := a.limitFor(model)
	if limit <= 0 {
		return true
	}
	backlog, ok := a.cached(model)
	if !ok {
		fctx, cancel := context.WithTimeout(ctx, admissionFetchTimeout)
		got, err := a.fetchFn(fctx, model)
		cancel()
		if err != nil {
			got = 0
		}
		backlog = got
		a.store(model, backlog)
	}
	return backlog < uint64(limit)
}

// cached returns model's backlog reading if it is younger than the TTL.
func (a *admissionChecker) cached(model string) (uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.cache[model]
	if !ok || a.nowFn().Sub(e.fetched) >= admissionCacheTTL {
		return 0, false
	}
	return e.backlog, true
}

func (a *admissionChecker) store(model string, backlog uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cache[model] = admissionEntry{backlog: backlog, fetched: a.nowFn()}
}

// fetchBacklog reads the model's durable consumer — the same one its
// workers create (wire.Durable) — and reports its queue depth: messages
// not yet delivered plus messages delivered but not yet acked, i.e. the
// work already committed to this model that no new request can jump ahead
// of.
//
// Exactly one $JS.API.CONSUMER.INFO round trip per reading. js.Consumer()
// fetches consumer info to build its handle, so on the first reading for a
// model the handle's CachedInfo() is that same response and costs nothing
// extra; the handle is then memoized and every later reading refreshes it
// with a single Info(). This matters because both the resolve and the
// refresh shared one 500 ms admissionFetchTimeout budget — spending two
// RPCs made a degraded JetStream twice as likely to blow the deadline (which
// fails open, i.e. a longer window of un-throttled admits).
//
// A handle is dropped whenever Info fails, so a consumer that is deleted and
// recreated (a worker restart that rebuilds its durable) is picked up on the
// next reading rather than wedging behind a stale handle.
func (a *admissionChecker) fetchBacklog(ctx context.Context, model string) (uint64, error) {
	if cons := a.consumerHandle(model); cons != nil {
		info, err := cons.Info(ctx)
		if err != nil {
			a.forgetConsumer(model)
			return 0, err
		}
		return backlogOf(info), nil
	}
	cons, err := a.js.Consumer(ctx, wire.StreamInference, wire.Durable(model))
	if err != nil {
		return 0, err
	}
	info := cons.CachedInfo()
	if info == nil {
		// Defensive: nats.go always populates the handle's info from the
		// call above, but a nil here must not panic — pay the extra RPC.
		if info, err = cons.Info(ctx); err != nil {
			return 0, err
		}
	}
	a.rememberConsumer(model, cons)
	return backlogOf(info), nil
}

func backlogOf(info *jetstream.ConsumerInfo) uint64 {
	return info.NumPending + uint64(info.NumAckPending)
}

func (a *admissionChecker) consumerHandle(model string) jetstream.Consumer {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.consumers[model]
}

func (a *admissionChecker) rememberConsumer(model string, cons jetstream.Consumer) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.consumers[model] = cons
}

func (a *admissionChecker) forgetConsumer(model string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.consumers, model)
}
