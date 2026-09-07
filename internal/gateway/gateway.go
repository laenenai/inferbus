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
	if len(auth) <= len(prefix) {
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
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

	// Client-disconnect watcher: race r.Context()'s cancellation against our
	// own "done" signal so we can tell a genuine mid-flight disconnect apart
	// from the request simply finishing normally. net/http cancels
	// r.Context() itself once the handler returns, so without the done race
	// this goroutine would fire relay.Cancel/DeleteQueued after *every*
	// request, successful or not. done is closed only after streamOut /
	// resultOut return, and always before chatCompletions itself returns
	// (and therefore before the server-side context cancellation can
	// happen), so the select below can only take the ctx.Done() branch on a
	// real disconnect while a read is still in flight. Either way the
	// goroutine exits promptly — it never outlives the request.
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			_ = relay.Cancel(g.nc, reqID)
			_ = relay.DeleteQueued(context.Background(), g.js, seq)
		}
	}()

	if req.Stream {
		g.streamOut(ctx, w, l, deadline)
	} else {
		g.resultOut(ctx, w, l, deadline)
	}
	close(done)
}

func (g *Gateway) streamOut(ctx context.Context, w http.ResponseWriter, l *relay.Listener, deadline time.Time) {
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
			} // mid-stream errors: connection just ends without [DONE]
			return
		}
		if err != nil {
			if !wroteHeader {
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

func (g *Gateway) resultOut(ctx context.Context, w http.ResponseWriter, l *relay.Listener, deadline time.Time) {
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
			oaiError(w, http.StatusGatewayTimeout, "timeout", "no response from worker")
			return
		}
		if m.Kind == wire.KindResult {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(m.Payload)
			return
		}
	}
}
