package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/engine/bifrostengine"
	"github.com/laenenai/inferbus/internal/engine/openaihttp"
	"github.com/laenenai/inferbus/internal/gateway"
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
	g := gateway.New(nc, js, cfg)
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
