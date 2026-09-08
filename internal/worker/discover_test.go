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

// TestDiscover covers the happy path with the model ids real engines
// actually report — Ollama's `name:tag` and vLLM's HuggingFace repo id —
// neither of which is NATS-subject-safe as-is. Discover must keep the
// engine's id VERBATIM as ModelConfig.Name (the subject layer,
// wire.ReqSubject/wire.Durable, does the slugging), turning the listing
// into a ready-to-run Config: one openai_http ModelConfig per model,
// pointed at the engine URL, with the caller's max-inflight applied to
// every model.
func TestDiscover(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"llama3.2:latest"},{"id":"meta-llama/Llama-3.2-1B-Instruct"}]}`))
	}))
	defer srv.Close()

	cfg, err := worker.Discover(context.Background(), srv.URL, 4)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(cfg.Models) != 2 {
		t.Fatalf("Models = %+v, want 2 entries", cfg.Models)
	}
	for i, wantName := range []string{"llama3.2:latest", "meta-llama/Llama-3.2-1B-Instruct"} {
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

// TestDiscoverTrimsTrailingSlash: an engine URL with a trailing slash
// (e.g. "-engine http://host:8000/") must not produce a "//v1/models"
// request path — vLLM/FastAPI/Starlette servers 404 on the double slash.
// It also checks that the ModelConfig.URL stored for the worker's engine
// client is the same normalized (no trailing slash) value, so later
// per-request calls are consistent with the discovery call.
func TestDiscoverTrimsTrailingSlash(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Fatalf("request path = %q, want /v1/models (no double slash)", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"llama3.2:latest"}]}`))
	}))
	defer srv.Close()

	cfg, err := worker.Discover(context.Background(), srv.URL+"/", 4)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(cfg.Models) != 1 {
		t.Fatalf("Models = %+v, want exactly 1", cfg.Models)
	}
	if cfg.Models[0].URL != srv.URL {
		t.Errorf("Models[0].URL = %q, want %q (trailing slash trimmed)", cfg.Models[0].URL, srv.URL)
	}
}

// TestDiscoverSkipsEmptySlug: an id made only of runes wire.Slug collapses
// away slugs to "" and is genuinely unroutable (subject "inference.req.",
// durable "model-"), so it must be skipped — but only it. A realistically
// shaped id alongside it still comes through untouched.
func TestDiscoverSkipsEmptySlug(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"qwen2.5-coder:7b"},{"id":"///"}]}`))
	}))
	defer srv.Close()

	cfg, err := worker.Discover(context.Background(), srv.URL, 4)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(cfg.Models) != 1 {
		t.Fatalf("Models = %+v, want exactly 1 (empty-slug id skipped)", cfg.Models)
	}
	if cfg.Models[0].Name != "qwen2.5-coder:7b" {
		t.Errorf("Models[0].Name = %q, want qwen2.5-coder:7b", cfg.Models[0].Name)
	}
}

// TestDiscoverSkipsSlugCollision: two distinct ids that collapse to the
// same subject token would silently cross-wire onto one durable consumer.
// The first id (in the engine's own listing order) keeps the token; every
// later claimant is skipped with a warning.
func TestDiscoverSkipsSlugCollision(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"llama3.2"},{"id":"llama3:2"},{"id":"qwen3"}]}`))
	}))
	defer srv.Close()

	cfg, err := worker.Discover(context.Background(), srv.URL, 4)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(cfg.Models) != 2 {
		t.Fatalf("Models = %+v, want exactly 2 (colliding id skipped)", cfg.Models)
	}
	if cfg.Models[0].Name != "llama3.2" {
		t.Errorf("Models[0].Name = %q, want llama3.2 (first claimant of the token wins)", cfg.Models[0].Name)
	}
	if cfg.Models[1].Name != "qwen3" {
		t.Errorf("Models[1].Name = %q, want qwen3", cfg.Models[1].Name)
	}
}

// TestDiscoverAllUnroutable: if every id the engine reports slugs to "",
// there is nothing usable to serve and that is still a hard error.
func TestDiscoverAllUnroutable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"///"},{"id":"..."}]}`))
	}))
	defer srv.Close()

	_, err := worker.Discover(context.Background(), srv.URL, 4)
	if err == nil {
		t.Fatal("Discover: want error when no id has a subject-safe form, got nil")
	}
	if !strings.Contains(err.Error(), "no usable models") {
		t.Fatalf("err = %q, want it to contain %q", err.Error(), "no usable models")
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
