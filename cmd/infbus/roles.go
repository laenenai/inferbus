package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/infbus/infbus/internal/engine"
	"github.com/infbus/infbus/internal/engine/openaihttp"
	"github.com/infbus/infbus/internal/gateway"
	"github.com/infbus/infbus/internal/wire"
	"github.com/infbus/infbus/internal/worker"
)

func connect(url string) (*nats.Conn, jetstream.JetStream, error) {
	if env := os.Getenv("INFBUS_NATS_URL"); env != "" {
		url = env
	}
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url, nats.MaxReconnects(-1))
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

func signalContext() context.Context {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	return ctx
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
	nc, js, err := connect("")
	if err != nil {
		fmt.Fprintln(stdout, "gateway: nats:", err)
		return 1
	}
	defer nc.Close()
	ctx := signalContext()
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
	nc, js, err := connect(cfg.NATSURL)
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
		default:
			fmt.Fprintf(stdout, "worker: model %s: unknown engine %q\n", mc.Name, mc.Engine)
			return 1
		}
	}
	fmt.Fprintf(stdout, "worker %s serving %d model(s)\n", cfg.WorkerID, len(cfg.Models))
	if err := worker.New(nc, js, engines, cfg).Run(signalContext()); err != nil && err != context.Canceled {
		fmt.Fprintln(stdout, "worker:", err)
		return 1
	}
	return 0
}
