// Runner assembles the whole controlplane process (Task 10): the relay
// (Task 6), the KV and SQL projectors (Tasks 7/8), the admin HTTP API
// (this task's admin.go), and authn/authz (Task 9), in the order the
// binding controller ruling requires:
//
//	EnsureControlStream -> start relay -> start projectors (KV + SQL) -> serve HTTP
//
// Shutdown reverses that: HTTP stops first, then the projectors, then the
// relay.
//
// Projector lifecycle is intentionally the smallest correct form (per the
// task brief's own escape hatch: "if runner lifecycle management balloons
// beyond ~150 lines, simplify"). runProjectors starts both RunKVProjectors
// and RunSQLProjector under one cancelable context and returns a stop
// func plus a channel that closes the moment either one fails for a real
// reason (not context.Canceled).
//
// health (review findings C1/I1/I2) is the single source of truth for
// "is this process actually able to serve authoritative auth/authz
// decisions right now" — it composes the relay's status (set once, never
// restarted) with whichever projector generation is currently running
// (swapped on every resync). /readyz and Admin's fail-closed authorization
// both read it through the same lock-free h.ok(), so a resync in progress
// (which holds a *separate* mutex for potentially minutes, see resyncFn)
// never blocks either of them from answering immediately.
package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/controlplane/alias"
	"github.com/laenenai/inferbus/internal/controlplane/apikey"
	"github.com/laenenai/inferbus/internal/controlplane/org"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
)

// shutdownTimeout bounds how long srv.Shutdown is allowed to wait for
// in-flight requests to drain before Run returns (review finding I2).
const shutdownTimeout = 10 * time.Second

type Runner struct {
	cfg   Config
	nc    *nats.Conn
	js    jetstream.JetStream
	store es.Store
}

func NewRunner(cfg Config, nc *nats.Conn, js jetstream.JetStream, store es.Store) *Runner {
	return &Runner{
		cfg:   cfg,
		nc:    nc,
		js:    js,
		store: store,
	}
}

// noOIDCVerifier rejects every token. It is the Authenticator's
// TokenVerifier when the config has no OIDC issuer configured, i.e. a
// bootstrap-token-only deployment (dev compose, CI) — bootstrap auth never
// consults the verifier at all (auth.go: it's checked first), so this is
// only ever reached by a caller presenting some other bearer token.
type noOIDCVerifier struct{}

func (noOIDCVerifier) Verify(context.Context, string) (string, error) {
	return "", errNoOIDCConfigured
}

var errNoOIDCConfigured = errors.New("controlplane: no OIDC verifier configured (set oidc.issuer or use the bootstrap token)")

// warnIfBootstrapTokenEnabled logs one loud slog.Warn line naming
// bootstrap_token as a static platform-admin credential when cfg has one
// configured (spec §3: "logged loudly"). It never logs the token's own
// value — only that the feature is enabled. Factored out of Run so it can
// be tested directly against a captured slog handler, without spinning up
// the rest of the runner.
func warnIfBootstrapTokenEnabled(logger *slog.Logger, cfg Config) {
	if cfg.BootstrapToken == "" {
		return
	}
	logger.Warn("bootstrap_token is enabled — a static platform-admin credential; disable it outside development")
}

// health composes the relay's status with the current projector
// generation's status into one lock-free "is everything up" signal
// (review findings C1, I1, I2).
//
//   - relayFailed is set exactly once, at construction: the relay is never
//     restarted (unlike the projectors, which resync can restart), so once
//     it fails the whole process is permanently unhealthy until an
//     operator restarts it.
//   - projFailed is swapped via atomic.Pointer on every projector
//     (re)start (initial Run, and every resyncFn call) — readers never
//     block on the mutex resyncFn holds for the whole resync operation
//     (which can run for minutes; see resyncTimeout in admin.go).
type health struct {
	relayFailed <-chan struct{}
	projFailed  atomic.Pointer[<-chan struct{}]
}

func newHealth(relayFailed <-chan struct{}) *health {
	h := &health{relayFailed: relayFailed}
	var never <-chan struct{} = make(chan struct{})
	h.projFailed.Store(&never)
	return h
}

func (h *health) setProjFailed(ch <-chan struct{}) {
	h.projFailed.Store(&ch)
}

// ok reports whether both the relay and the current projector generation
// are still running. It never blocks and never takes a lock.
func (h *health) ok() bool {
	select {
	case <-h.relayFailed:
		return false
	default:
	}
	pf := *h.projFailed.Load()
	select {
	case <-pf:
		return false
	default:
	}
	return true
}

// runProjectors starts RunKVProjectors and RunSQLProjector under one
// cancelable child of parent. stop cancels both and blocks until both
// have returned. failed is closed the first time either projector returns
// a non-context.Canceled error (i.e. a fail-stop per kvproj.go/
// readmodel.go's apply() contract).
func runProjectors(parent context.Context, store es.Store, js jetstream.JetStream, rs ReadStore) (stop func(), failed <-chan struct{}) {
	ctx, cancel := context.WithCancel(parent)

	kvDone := make(chan error, 1)
	sqlDone := make(chan error, 1)
	go func() { kvDone <- RunKVProjectors(ctx, store, js) }()
	go func() { sqlDone <- RunSQLProjector(ctx, store, js, rs) }()

	failedCh := make(chan struct{})
	var once sync.Once
	watch := func(done <-chan error, name string) {
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("controlplane: %s projector stopped: %v", name, err)
			once.Do(func() { close(failedCh) })
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); watch(kvDone, "kv") }()
	go func() { defer wg.Done(); watch(sqlDone, "sql") }()

	stop = func() {
		cancel()
		wg.Wait()
	}
	return stop, failedCh
}

// runRelay starts RunRelay under a cancelable child of parent and reports
// back three things: a cancel func, a channel closed once the relay's
// goroutine has fully returned (safe to wait on for shutdown), and a
// channel closed the moment RunRelay returns any error that isn't
// context.Canceled (review finding C1 — a config error causing an
// immediate return must be observable exactly like a later fail-stop, not
// silently dropped).
func runRelay(parent context.Context, store es.Store, js jetstream.JetStream) (cancel context.CancelFunc, stopped <-chan struct{}, failed <-chan struct{}) {
	relayCtx, cancelRelay := context.WithCancel(parent)
	stoppedCh := make(chan struct{})
	failedCh := make(chan struct{})
	go func() {
		defer close(stoppedCh)
		err := RunRelay(relayCtx, store, js)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("controlplane: relay stopped: %v", err)
			close(failedCh)
		}
	}()
	return cancelRelay, stoppedCh, failedCh
}

// Run assembles and serves the control plane until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	warnIfBootstrapTokenEnabled(slog.Default(), r.cfg)

	if err := EnsureControlStream(ctx, r.js); err != nil {
		return fmt.Errorf("controlplane: ensure control stream: %w", err)
	}

	cancelRelay, relayStopped, relayFailed := runRelay(ctx, r.store, r.js)
	stopRelay := func() {
		cancelRelay()
		<-relayStopped
	}

	pool, err := pgxpool.New(ctx, r.cfg.PostgresDSN)
	if err != nil {
		stopRelay()
		return fmt.Errorf("controlplane: read store pool: %w", err)
	}
	defer pool.Close()
	rs, err := NewPgReadStore(pool)
	if err != nil {
		stopRelay()
		return fmt.Errorf("controlplane: read store: %w", err)
	}

	orgRT := aggregate.NewRuntime(r.store, org.Decider, org.Codec())
	keyRT := aggregate.NewRuntime(r.store, apikey.Decider, apikey.Codec())
	aliasRT := aggregate.NewRuntime(r.store, alias.Decider, alias.Codec())

	h := newHealth(relayFailed)
	stop, failed := runProjectors(ctx, r.store, r.js, rs)
	h.setProjFailed(failed)

	// resyncMu serializes resyncFn invocations only — it is deliberately
	// NOT consulted by h.ok() (review finding I2: readyz and Admin's
	// fail-closed authz check must answer instantly even while a resync,
	// which can run for up to resyncTimeout, is in flight).
	var resyncMu sync.Mutex

	// resyncFn self-coordinates per the binding controller ruling: it owns
	// stopping the current projectors, running ResyncKV, and restarting
	// them, so the /admin/v1/projections/resync handler needs no
	// knowledge of projector goroutines at all. It always restarts the
	// projectors afterward, even on ResyncKV failure, so the process never
	// gets stuck with projections permanently offline. It captures ctx
	// (the runner's root context), not the rctx it's called with, for the
	// restarted projectors' lifetime — rctx is only used for the ResyncKV
	// call itself and may be (and per admin.go's C2 fix, is) already
	// detached from any one HTTP request's lifetime.
	resyncFn := func(rctx context.Context) error {
		resyncMu.Lock()
		defer resyncMu.Unlock()
		stop()
		resyncErr := ResyncKV(rctx, r.store, r.js)
		stop, failed = runProjectors(ctx, r.store, r.js, rs)
		h.setProjFailed(failed)
		return resyncErr
	}

	var verifier TokenVerifier = noOIDCVerifier{}
	if r.cfg.OIDC.Issuer != "" {
		v, err := NewOIDCVerifier(ctx, r.cfg.OIDC.Issuer, r.cfg.OIDC.Audience)
		if err != nil {
			resyncMu.Lock()
			stop()
			resyncMu.Unlock()
			stopRelay()
			return fmt.Errorf("controlplane: oidc verifier: %w", err)
		}
		verifier = v
	}
	authenticator := NewAuthenticator(r.cfg, verifier)

	admin := NewAdmin(authenticator, rs, orgRT, keyRT, aliasRT, resyncFn, h.ok)
	mux := admin.Routes()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if h.ok() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "OK")
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "unhealthy: relay or a projector has stopped")
		}
	})

	srv := &http.Server{Addr: r.cfg.Addr, Handler: mux}
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()

	shutdownHTTP := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		<-srvErr
	}

	var runErr error
	select {
	case <-ctx.Done():
		shutdownHTTP()
	case e := <-srvErr:
		if e != nil && e != http.ErrServerClosed {
			runErr = e
		}
	case <-relayFailed:
		// Review finding C1: an immediate config-error return from
		// RunRelay (or a later fail-stop) must not leave the process
		// serving green — /readyz already reports 503 via h.ok() the
		// instant this fires, but a relay this dead is fatal to the
		// control plane's whole reason for existing (nothing new ever
		// reaches CONTROL_EVENTS), so shut the HTTP server down too and
		// return an error rather than idling indefinitely.
		log.Printf("controlplane: relay failed; shutting down")
		runErr = errors.New("controlplane: relay failed (see prior log line for the cause)")
		shutdownHTTP()
	}

	// Shutdown reverses assembly order: HTTP is already down, so stop the
	// projectors next, then the relay.
	resyncMu.Lock()
	stop()
	resyncMu.Unlock()
	stopRelay()

	if runErr != nil {
		return runErr
	}
	return ctx.Err()
}
