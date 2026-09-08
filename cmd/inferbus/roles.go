package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/postgres"

	"github.com/laenenai/inferbus/internal/controlplane"
	"github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/engine/bifrostengine"
	"github.com/laenenai/inferbus/internal/engine/openaihttp"
	"github.com/laenenai/inferbus/internal/gateway"
	"github.com/laenenai/inferbus/internal/harvester"
	"github.com/laenenai/inferbus/internal/wire"
	"github.com/laenenai/inferbus/internal/worker"
)

// connect dials NATS for the given role ("gateway" or "worker"), wiring up
// handlers so connection trouble is observable instead of silent: a slow
// consumer or a dropped subscription (see internal/relay.Listen's 256-frame
// buffer) shows up in logs via ErrorHandler rather than just quietly losing
// frames, and disconnects/reconnects during the long-lived NATS session are
// logged too.
func connect(url, role string) (*nats.Conn, jetstream.JetStream, error) {
	if env := os.Getenv("INFERBUS_NATS_URL"); env != "" {
		url = env
	}
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.Name("inferbus-"+role),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}
			slog.Error("nats error", "role", role, "subject", subject, "err", err)
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			slog.Warn("nats disconnected", "role", role, "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("nats reconnected", "role", role, "url", nc.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	return ctx, stop
}

func runGateway(args []string, stdout io.Writer) int {
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.SetOutput(stdout)
	cfgPath := fs.String("config", "", "path to gateway YAML config (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *cfgPath == "" {
		fmt.Fprintln(stdout, "gateway: -config is required")
		return 2
	}
	cfg, err := gateway.LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(stdout, "gateway:", err)
		return 1
	}
	// I3 review ruling: a non-empty static `keys:` list alongside
	// `iam.mode: kv` is a hard startup error, not a silently-ignored
	// leftover — kv mode never consults cfg.Keys at all (KVIAM is the sole
	// source of truth), so a config with both looks like the operator
	// believes those static keys still work when they never will. Fail
	// loudly and fast (before ever touching NATS) instead of an auth
	// surface that silently downgrades to "kv only" without the operator
	// noticing.
	if cfg.IAM.Mode == "kv" && len(cfg.Keys) > 0 {
		fmt.Fprintln(stdout, "gateway: iam.mode is \"kv\" but config also lists static keys: — remove the keys: list (kv mode never uses it) or switch iam.mode to \"static\"")
		return 1
	}
	if cfg.IAM.Mode != "kv" && cfg.IAM.Mode != "static" {
		fmt.Fprintf(stdout, "gateway: unknown iam.mode %q\n", cfg.IAM.Mode)
		return 1
	}
	nc, js, err := connect("", "gateway")
	if err != nil {
		fmt.Fprintln(stdout, "gateway: nats:", err)
		return 1
	}
	defer nc.Close()
	ctx, stop := signalContext()
	defer stop()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		fmt.Fprintln(stdout, "gateway: streams:", err)
		return 1
	}
	var g *gateway.Gateway
	switch cfg.IAM.Mode {
	case "kv":
		// NewKVIAM starts its watch loops in the background and returns
		// immediately (review ruling I6) — it does not wait for the
		// control plane's ALIASES/KEYS buckets to exist. The HTTP server
		// below starts right away too; GET /readyz (and a 503 from
		// /v1/models, /v1/chat/completions) reports "not ready yet" until
		// KVIAM's first scan of both buckets completes.
		kv, err := gateway.NewKVIAM(ctx, js)
		if err != nil {
			fmt.Fprintln(stdout, "gateway: kv iam:", err)
			return 1
		}
		g = gateway.NewWithIAM(nc, js, cfg, kv)
	default: // "static" — LoadConfig already defaults empty Mode to this.
		g = gateway.New(nc, js, cfg)
	}
	srv := &http.Server{Addr: cfg.Addr, Handler: g.Routes()}
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
	fmt.Fprintf(stdout, "gateway listening on %s\n", cfg.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(stdout, "gateway:", err)
		return 1
	}
	return 0
}

func runWorker(args []string, stdout io.Writer) int {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.SetOutput(stdout)
	cfgPath := fs.String("config", "", "path to worker YAML config (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *cfgPath == "" {
		fmt.Fprintln(stdout, "worker: -config is required")
		return 2
	}
	cfg, err := worker.LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(stdout, "worker:", err)
		return 1
	}
	nc, js, err := connect(cfg.NATSURL, "worker")
	if err != nil {
		fmt.Fprintln(stdout, "worker: nats:", err)
		return 1
	}
	defer nc.Close()
	engines := map[string]engine.Engine{}
	for _, mc := range cfg.Models {
		switch mc.Engine {
		case "openai_http", "":
			engines[mc.Name] = openaihttp.New(mc.URL, nil)
		case "bifrost":
			e, err := bifrostengine.New(bifrostengine.Config{
				Provider: mc.Provider,
				Model:    mc.UpstreamModel,
				APIKey:   os.Getenv(mc.APIKeyEnv),
				BaseURL:  mc.URL,
			})
			if err != nil {
				fmt.Fprintf(stdout, "worker: model %s: %v\n", mc.Name, err)
				return 1
			}
			engines[mc.Name] = e
		default:
			fmt.Fprintf(stdout, "worker: model %s: unknown engine %q\n", mc.Name, mc.Engine)
			return 1
		}
	}
	fmt.Fprintf(stdout, "worker %s serving %d model(s)\n", cfg.WorkerID, len(cfg.Models))
	ctx, stop := signalContext()
	defer stop()
	if err := worker.New(nc, js, engines, cfg).Run(ctx); err != nil && err != context.Canceled {
		fmt.Fprintln(stdout, "worker:", err)
		return 1
	}
	return 0
}

func runControlplane(args []string, stdout io.Writer) int {
	fs := flag.NewFlagSet("controlplane", flag.ContinueOnError)
	fs.SetOutput(stdout)
	cfgPath := fs.String("config", "", "path to controlplane YAML config (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *cfgPath == "" {
		fmt.Fprintln(stdout, "controlplane: -config is required")
		return 2
	}
	cfg, err := controlplane.LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(stdout, "controlplane:", err)
		return 1
	}
	ctx, stop := signalContext()
	defer stop()
	pgStore, err := postgres.Open(ctx, cfg.PostgresDSN)
	if err != nil {
		fmt.Fprintln(stdout, "controlplane: postgres:", err)
		return 1
	}
	// M7: close the pool on every exit path, not just the happy one.
	defer pgStore.Close()
	store := pgStore.Workspace("controlplane")
	nc, js, err := connect(cfg.NATSURL, "controlplane")
	if err != nil {
		fmt.Fprintln(stdout, "controlplane: nats:", err)
		return 1
	}
	defer nc.Close()
	// I4 fix: the relay needs the RAW pgStore (implements delivery.Drainer
	// directly), not the workspace-scoped store used for aggregates and
	// projectors — see controlplane.RunRelay's doc comment for why the
	// latter can never drive the relay against real Postgres.
	runner := controlplane.NewRunner(cfg, nc, js, store, pgStore)
	fmt.Fprintf(stdout, "controlplane listening on %s\n", cfg.Addr)
	if err := runner.Run(ctx); err != nil && err != context.Canceled {
		fmt.Fprintln(stdout, "controlplane:", err)
		return 1
	}
	return 0
}

func runHarvester(args []string, stdout io.Writer) int {
	fs := flag.NewFlagSet("harvester", flag.ContinueOnError)
	fs.SetOutput(stdout)
	cfgPath := fs.String("config", "", "path to harvester YAML config (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *cfgPath == "" {
		fmt.Fprintln(stdout, "harvester: -config is required")
		return 2
	}
	cfg, err := harvester.LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(stdout, "harvester:", err)
		return 1
	}
	ctx, stop := signalContext()
	defer stop()

	// Connect to NATS.
	nc, js, err := connect(cfg.NATSURL, "harvester")
	if err != nil {
		fmt.Fprintln(stdout, "harvester: nats:", err)
		return 1
	}
	defer nc.Close()

	// Create ClickHouse sink.
	sink, err := harvester.NewCHSink(ctx, cfg.ClickHouseDSN)
	if err != nil {
		fmt.Fprintln(stdout, "harvester: clickhouse:", err)
		return 1
	}
	defer sink.Close()

	// Create the harvester and budget ledger.
	h := harvester.New(nc, js, sink, cfg)
	ledger := harvester.NewBudgetLedger(js, sink, cfg.BudgetRefreshInterval)

	// Wire the harvester's OnRow to the ledger's AddUsage.
	h.OnRow(ledger.AddUsage)

	// runCtx is a child of ctx that can be canceled independently when
	// either component fails fatally, allowing graceful HTTP shutdown.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	// Channels to report non-context errors from components.
	hDone := make(chan error, 1)
	ledgerDone := make(chan error, 1)

	go func() {
		if err := h.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			hDone <- err
		}
	}()

	go func() {
		if err := ledger.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			ledgerDone <- err
		}
	}()

	// Health check: both components running.
	failedCh := make(chan struct{})
	var once sync.Once
	watch := func(done <-chan error, name string) {
		if err := <-done; err != nil {
			slog.Error("harvester: component failed", "component", name, "err", err)
			once.Do(func() { close(failedCh) })
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); watch(hDone, "harvester") }()
	go func() { defer wg.Done(); watch(ledgerDone, "budget-ledger") }()

	// Start HTTP server for healthz/readyz.
	srv := &http.Server{
		Addr: cfg.Addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthz" {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, "OK")
			} else if r.URL.Path == "/readyz" {
				// readyz returns 503 if either component has failed,
				// 200 only while both are running.
				select {
				case <-failedCh:
					w.WriteHeader(http.StatusServiceUnavailable)
					fmt.Fprint(w, "unhealthy: harvester or budget ledger has stopped")
					return
				default:
				}
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, "OK")
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}),
	}

	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()

	shutdownHTTP := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		<-srvErr
	}

	fmt.Fprintf(stdout, "harvester listening on %s\n", cfg.Addr)

	var runErr error
	select {
	case <-ctx.Done():
		shutdownHTTP()
	case e := <-srvErr:
		if e != nil && e != http.ErrServerClosed {
			runErr = e
		}
	case <-failedCh:
		// One of the components failed fatally; shut down HTTP and return error.
		// The prior log line via slog.Error names which component and why.
		slog.Error("harvester: component failure; shutting down")
		runErr = errors.New("harvester: component failed (see prior log line for the cause)")
		shutdownHTTP()
		cancelRun()
	}

	// Wait for component watchers to finish.
	wg.Wait()

	// Cleanup: shutdown HTTP if not already done.
	select {
	case <-srvErr:
	default:
		shutdownHTTP()
	}

	if runErr != nil {
		fmt.Fprintln(stdout, runErr)
		return 1
	}
	return 0
}
