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
// reason (not context.Canceled) — that channel is /readyz's health
// signal, and it is also what lets the injected resync closure know it
// has fully drained both projectors before it touches ResyncKV.
package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/controlplane/alias"
	"github.com/laenenai/inferbus/internal/controlplane/apikey"
	"github.com/laenenai/inferbus/internal/controlplane/org"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
)

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

// runProjectors starts RunKVProjectors and RunSQLProjector under one
// cancelable child of parent. stop cancels both and blocks until both
// have returned. failed is closed the first time either projector returns
// a non-context.Canceled error (i.e. a fail-stop per kvproj.go/
// readmodel.go's apply() contract) — /readyz treats an open failed
// channel as healthy.
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

// Run assembles and serves the control plane until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	if err := EnsureControlStream(ctx, r.js); err != nil {
		return fmt.Errorf("controlplane: ensure control stream: %w", err)
	}

	relayCtx, cancelRelay := context.WithCancel(ctx)
	relayDone := make(chan error, 1)
	go func() { relayDone <- RunRelay(relayCtx, r.store, r.js) }()

	pool, err := pgxpool.New(ctx, r.cfg.PostgresDSN)
	if err != nil {
		cancelRelay()
		<-relayDone
		return fmt.Errorf("controlplane: read store pool: %w", err)
	}
	defer pool.Close()
	rs, err := NewPgReadStore(pool)
	if err != nil {
		cancelRelay()
		<-relayDone
		return fmt.Errorf("controlplane: read store: %w", err)
	}

	orgRT := aggregate.NewRuntime(r.store, org.Decider, org.Codec())
	keyRT := aggregate.NewRuntime(r.store, apikey.Decider, apikey.Codec())
	aliasRT := aggregate.NewRuntime(r.store, alias.Decider, alias.Codec())

	var mu sync.Mutex
	stop, failed := runProjectors(ctx, r.store, r.js, rs)

	// resyncFn self-coordinates per the binding controller ruling: it owns
	// stopping the current projectors, running ResyncKV, and restarting
	// them, so the /admin/v1/projections/resync handler needs no
	// knowledge of projector goroutines at all. It always restarts the
	// projectors afterward, even on ResyncKV failure, so the process never
	// gets stuck with projections permanently offline.
	resyncFn := func(rctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		stop()
		resyncErr := ResyncKV(rctx, r.store, r.js)
		stop, failed = runProjectors(ctx, r.store, r.js, rs)
		return resyncErr
	}

	var verifier TokenVerifier = noOIDCVerifier{}
	if r.cfg.OIDC.Issuer != "" {
		v, err := NewOIDCVerifier(ctx, r.cfg.OIDC.Issuer, r.cfg.OIDC.Audience)
		if err != nil {
			mu.Lock()
			stop()
			mu.Unlock()
			cancelRelay()
			<-relayDone
			return fmt.Errorf("controlplane: oidc verifier: %w", err)
		}
		verifier = v
	}
	authenticator := NewAuthenticator(r.cfg, verifier)

	admin := NewAdmin(authenticator, rs, orgRT, keyRT, aliasRT, resyncFn)
	mux := admin.Routes()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		f := failed
		mu.Unlock()
		select {
		case <-f:
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "projector failed")
		default:
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "OK")
		}
	})

	srv := &http.Server{Addr: r.cfg.Addr, Handler: mux}
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()

	var runErr error
	select {
	case <-ctx.Done():
		_ = srv.Shutdown(context.Background())
		<-srvErr
	case e := <-srvErr:
		if e != nil && e != http.ErrServerClosed {
			runErr = e
		}
	}

	// Shutdown reverses assembly order: HTTP is already down, so stop the
	// projectors next, then the relay.
	mu.Lock()
	stop()
	mu.Unlock()
	cancelRelay()
	<-relayDone

	if runErr != nil {
		return runErr
	}
	return ctx.Err()
}
