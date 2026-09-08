package worker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/laenenai/inferbus/internal/worker"
)

// TestDiscover covers the happy path: the engine's /v1/models endpoint
// lists two models, and Discover turns that into a ready-to-run Config —
// one openai_http ModelConfig per model, pointed at the engine URL, with
// the caller's max-inflight applied to every model.
func TestDiscover(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"llama3-2"},{"id":"qwen3"}]}`))
	}))
	defer srv.Close()

	cfg, err := worker.Discover(context.Background(), srv.URL, 4)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(cfg.Models) != 2 {
		t.Fatalf("Models = %+v, want 2 entries", cfg.Models)
	}
	for i, wantName := range []string{"llama3-2", "qwen3"} {
		m := cfg.Models[i]
		if m.Name != wantName {
			t.Errorf("Models[%d].Name = %q, want %q", i, m.Name, wantName)
		}
		if m.Engine != "openai_http" {
			t.Errorf("Models[%d].Engine = %q, want openai_http", i, m.Engine)
		}
		if m.URL != srv.URL {
			t.Errorf("Models[%d].URL = %q, want %q", i, m.URL, srv.URL)
		}
		if m.MaxInflight != 4 {
			t.Errorf("Models[%d].MaxInflight = %d, want 4", i, m.MaxInflight)
		}
	}
}

// TestDiscoverSkipsInvalidSlugs: a discovered model id that isn't already
// NATS-subject-safe (wire.Slug would alter it) must be skipped rather than
// fail the whole discovery — the remaining valid model should still come
// through.
func TestDiscoverSkipsInvalidSlugs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"good-model"},{"id":"Bad/Name:8b"}]}`))
	}))
	defer srv.Close()

	cfg, err := worker.Discover(context.Background(), srv.URL, 4)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(cfg.Models) != 1 {
		t.Fatalf("Models = %+v, want exactly 1 (invalid id skipped)", cfg.Models)
	}
	if cfg.Models[0].Name != "good-model" {
		t.Errorf("Models[0].Name = %q, want good-model", cfg.Models[0].Name)
	}
}

// TestDiscoverEmpty: a /v1/models response with zero entries leaves the
// worker with nothing to serve — that must be a hard error, not a
// zero-model Config that silently starts up and never handles a request.
func TestDiscoverEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"object":"list","data":[]}`))
	}))
	defer srv.Close()

	_, err := worker.Discover(context.Background(), srv.URL, 4)
	if err == nil {
		t.Fatal("Discover: want error for empty model list, got nil")
	}
	if !strings.Contains(err.Error(), "no usable models") {
		t.Fatalf("err = %q, want it to contain %q", err.Error(), "no usable models")
	}
}

// TestDiscoverHTTPError: the engine responding with a non-2xx status must
// surface as an error, not an (incorrectly) empty or partial Config.
func TestDiscoverHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := worker.Discover(context.Background(), srv.URL, 4)
	if err == nil {
		t.Fatal("Discover: want error for HTTP 500, got nil")
	}
}

// TestDiscoverTimeout: a server that never responds within the caller's
// deadline must fail promptly, not hang until some much longer internal
// default.
func TestDiscoverTimeout(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	// Unblock the handler (letting its connection close) before Close()
	// waits for outstanding connections — reversed, httptest.Server.Close
	// would hang until its own internal timeout.
	defer srv.Close()
	defer close(block)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := worker.Discover(ctx, srv.URL, 4)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Discover: want error on context deadline, got nil")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Discover took %s, want it to return promptly after the 100ms deadline", elapsed)
	}
}
