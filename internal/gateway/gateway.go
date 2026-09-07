// Package gateway is the OpenAI-compatible HTTP front door: key auth,
// alias resolution, and the relay to the NATS data plane.
package gateway

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/infbus/infbus/internal/relay"
	"github.com/infbus/infbus/internal/wire"
)

type Gateway struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg Config
}

func New(nc *nats.Conn, js jetstream.JetStream, cfg Config) *Gateway {
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Minute
	}
	return &Gateway{nc: nc, js: js, cfg: cfg}
}

func (g *Gateway) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", g.chatCompletions)
	mux.HandleFunc("GET /v1/models", g.models)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func oaiError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"message": msg, "type": typ},
	})
}

func (g *Gateway) authenticate(r *http.Request) (KeyConfig, bool) {
	auth := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) || len(auth) <= len(prefix) {
		return KeyConfig{}, false
	}
	presented := []byte(auth[len(prefix):])
	for _, k := range g.cfg.Keys {
		if subtle.ConstantTimeCompare(presented, []byte(k.Key)) == 1 {
			return k, true
		}
	}
	return KeyConfig{}, false
}

func (g *Gateway) models(w http.ResponseWriter, r *http.Request) {
	key, ok := g.authenticate(r)
	if !ok {
		oaiError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
		return
	}
	type model struct {
		ID     string `json:"id"`
		Object string `json:"object"`
	}
	out := struct {
		Object string  `json:"object"`
		Data   []model `json:"data"`
	}{Object: "list", Data: []model{}}
	for _, alias := range key.Allow {
		if _, exists := g.cfg.Aliases[alias]; exists {
			out.Data = append(out.Data, model{ID: alias, Object: "model"})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func newReqID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (g *Gateway) chatCompletions(w http.ResponseWriter, r *http.Request) {
	key, ok := g.authenticate(r)
	if !ok {
		oaiError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			oaiError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the 1 MiB limit")
			return
		}
		oaiError(w, http.StatusBadRequest, "invalid_request_error", "unreadable body")
		return
	}
	var req struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Model == "" {
		oaiError(w, http.StatusBadRequest, "invalid_request_error", "body must be a JSON object with a model field")
		return
	}
	target, exists := g.cfg.Aliases[req.Model]
	if !exists {
		oaiError(w, http.StatusNotFound, "model_not_found", fmt.Sprintf("unknown model alias %q", req.Model))
		return
	}
	if !slices.Contains(key.Allow, req.Model) {
		oaiError(w, http.StatusForbidden, "model_forbidden", fmt.Sprintf("key is not allowed to use %q", req.Model))
		return
	}

	reqID := newReqID()
	ctx := r.Context()
	deadline := time.Now().Add(g.cfg.RequestTimeout)

	l, err := relay.Listen(g.nc, reqID) // subscribe BEFORE publish
	if err != nil {
		oaiError(w, http.StatusInternalServerError, "internal_error", "subscribe failed")
		return
	}
	defer l.Close()

	seq, err := relay.Publish(ctx, g.js, relay.Request{
		Model: target, Org: key.Org, Project: key.Project, KeyID: key.Name,
		Alias: req.Model, ReqID: reqID, Kind: "chat", Deadline: deadline, Body: body,
	})
	if err != nil {
		oaiError(w, http.StatusServiceUnavailable, "transport_error", "queue publish failed")
		return
	}

	// Client disconnect is detected synchronously, inline in the read loops
	// below, rather than with a separate goroutine watching ctx.Done(). An
	// earlier version of this handler raced a background goroutine's
	// <-ctx.Done() against a "done" signal closed after streamOut/resultOut
	// returned: the intent was "only clean up if ctx died before the
	// handler finished on its own". That doesn't work, because ctx dying is
	// *also* what makes streamOut/resultOut return early on a real
	// disconnect (their read loop is bounded by dctx, a child of ctx, so
	// canceling ctx unblocks l.Next(dctx) immediately) — so on a genuine
	// disconnect, both "ctx.Done()" and "done" become ready at essentially
	// the same instant, and which case a Go select picks between two ready
	// channels is undefined. Roughly half the time it picked "done" and
	// silently skipped the cleanup call — confirmed by go test -race
	// -count=5 failing 3/5 runs of TestClientDisconnectCancels with that
	// version.
	//
	// The fix: streamOut/resultOut already distinguish dctx's own deadline
	// firing from the *parent* ctx dying (client disconnect) by checking
	// ctx.Err() — nil means only the per-request deadline expired, non-nil
	// means the parent request context itself was canceled. That check is
	// deterministic (no data race, no goroutine) and happens exactly once,
	// in the same place the read loop already decides how to respond, so
	// cleanup is invoked from there directly instead of via a racing
	// watcher.
	if req.Stream {
		g.streamOut(ctx, w, l, deadline, reqID, seq)
	} else {
		g.resultOut(ctx, w, l, deadline, reqID, seq)
	}
}

// clientDisconnected reports whether ctx — the caller's original request
// context, not the per-call deadline context derived from it — was
// canceled. Used to tell "client hung up" apart from "our own deadline
// fired", which look identical from inside l.Next(dctx)'s returned error
// alone.
func clientDisconnected(ctx context.Context) bool { return ctx.Err() != nil }

// cleanupDisconnected fires the gateway's half of the two-phase client
// cancel: a best-effort delete of the message if it's still queued, and a
// cancel publish for a worker that already picked it up. Both are
// best-effort (see relay.DeleteQueued / relay.Cancel) — losing either race
// is fine, the worker's own deadline (KindDone/KindResult vs no terminal
// frame) is the backstop.
func (g *Gateway) cleanupDisconnected(reqID string, seq uint64) {
	_ = relay.Cancel(g.nc, reqID)
	_ = relay.DeleteQueued(context.Background(), g.js, seq)
}

func (g *Gateway) streamOut(ctx context.Context, w http.ResponseWriter, l *relay.Listener, deadline time.Time, reqID string, seq uint64) {
	fl, _ := w.(http.Flusher)
	wroteHeader := false
	writeHead := func() {
		if !wroteHeader {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(http.StatusOK)
			wroteHeader = true
		}
	}
	dctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for {
		m, err := l.Next(dctx)
		if errors.Is(err, io.EOF) {
			writeHead()
			fmt.Fprint(w, "data: [DONE]\n\n")
			if fl != nil {
				fl.Flush()
			}
			return
		}
		var re *relay.RemoteError
		if errors.As(err, &re) {
			if !wroteHeader {
				oaiError(w, re.Err.HTTPStatus, re.Err.Code, re.Err.Message)
				return
			}
			// Headers (and possibly chunks) are already on the wire, so an
			// HTTP status can no longer communicate the failure — emit an
			// SSE error event followed by [DONE] so the client can tell a
			// genuine failure apart from a clean, silent end of stream.
			b, _ := json.Marshal(map[string]any{
				"error": map[string]any{"message": re.Err.Message, "type": re.Err.Code},
			})
			fmt.Fprintf(w, "data: %s\n\n", b)
			fmt.Fprint(w, "data: [DONE]\n\n")
			if fl != nil {
				fl.Flush()
			}
			return
		}
		if err != nil {
			// Either the client hung up (ctx died) or the gateway's own
			// per-request deadline fired with no worker ever answering
			// (dctx died, ctx alive). Either way no worker is (or ever
			// will be) usefully serving this request, so clean it up: a
			// still-queued message deleted, a cancel published for a
			// worker that already picked it up. Without this, a model
			// with no consumer just leaves the message queued until the
			// stream's MaxAge eventually reaps it.
			g.cleanupDisconnected(reqID, seq)
			if !clientDisconnected(ctx) && !wroteHeader {
				oaiError(w, http.StatusGatewayTimeout, "timeout", "no response from worker")
			}
			return
		}
		if m.Kind == wire.KindChunk {
			writeHead()
			fmt.Fprintf(w, "data: %s\n\n", m.Payload)
			if fl != nil {
				fl.Flush()
			}
		}
		// done frame: loop once more to get io.EOF and emit [DONE]
	}
}

func (g *Gateway) resultOut(ctx context.Context, w http.ResponseWriter, l *relay.Listener, deadline time.Time, reqID string, seq uint64) {
	dctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for {
		m, err := l.Next(dctx)
		var re *relay.RemoteError
		switch {
		case errors.As(err, &re):
			oaiError(w, re.Err.HTTPStatus, re.Err.Code, re.Err.Message)
			return
		case errors.Is(err, io.EOF):
			oaiError(w, http.StatusBadGateway, "protocol_error", "stream ended without a result")
			return
		case err != nil:
			// Same reasoning as streamOut's equivalent branch: clean up
			// whether it was the client that disappeared or the gateway's
			// own deadline firing on a request no worker will ever serve.
			g.cleanupDisconnected(reqID, seq)
			if !clientDisconnected(ctx) {
				oaiError(w, http.StatusGatewayTimeout, "timeout", "no response from worker")
			}
			return
		}
		if m.Kind == wire.KindResult {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(m.Payload)
			return
		}
	}
}
