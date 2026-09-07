package controlplane

import (
	"context"
	"fmt"
	"net/http"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

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

func (r *Runner) Run(ctx context.Context) error {
	// Create HTTP server with health check endpoints
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	})

	srv := &http.Server{
		Addr:    r.cfg.Addr,
		Handler: mux,
	}

	// Shutdown server when context is done
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	// Start listening
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
