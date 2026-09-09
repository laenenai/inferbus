// Package gateway is the OpenAI-compatible HTTP front door: key auth,
// alias resolution, and the relay to the NATS data plane.
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/relay"
	"github.com/laenenai/inferbus/internal/wire"
)

type Gateway struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg Config
	iam iamProvider
	// admission is nil whenever admission control is disabled, which is the
	// default. A nil *admissionChecker admits every request (allow is
	// nil-safe), so the disabled path costs a single nil comparison.
	admission *admissionChecker
	// metrics holds this Gateway's own private Prometheus registry (Task 6
	// / ADR-0028 metrics floor) — see metrics.go's package doc comment for
	// why per-instance, never global.
	metrics *gwMetrics
}

// New builds a Gateway in the default STATIC IAM mode: key auth and alias
// resolution both come from cfg's own Keys/Aliases, exactly as before
// Task 11 introduced the iamProvider seam. Every pre-existing gateway test
// keeps using this constructor unchanged.
func New(nc *nats.Conn, js jetstream.JetStream, cfg Config) *Gateway {
	return newGateway(nc, js, cfg, newStaticIAM(cfg))
}

// NewWithIAM builds a Gateway backed by an explicit iamProvider — used for
// `iam.mode: kv` (cmd/inferbus/roles.go constructs a *KVIAM and passes it
// here) and by tests that want to exercise KVIAM directly through the
// gateway's HTTP surface.
func NewWithIAM(nc *nats.Conn, js jetstream.JetStream, cfg Config, iam iamProvider) *Gateway {
	return newGateway(nc, js, cfg, iam)
}

func newGateway(nc *nats.Conn, js jetstream.JetStream, cfg Config, iam iamProvider) *Gateway {
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Minute
	}
	g := &Gateway{nc: nc, js: js, cfg: cfg, iam: iam, metrics: newGWMetrics()}
	// max_backlog is the on/off switch: overrides alone don't enable the
	// feature, they only reshape it once a default limit exists.
	if cfg.Admission.MaxBacklog > 0 {
		g.admission = newAdmissionChecker(js, cfg.Admission)
	} else if len(cfg.Admission.Overrides) > 0 {
		slog.Warn("gateway: admission.overrides set but admission.max_backlog is 0 — admission control is DISABLED; set max_backlog to enable")
	}
	return g
}

func (g *Gateway) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", g.withRequestMetrics(routeLabel("/v1/chat/completions"), g.withInflight(g.chatCompletions)))
	mux.HandleFunc("POST /v1/embeddings", g.withRequestMetrics(routeLabel("/v1/embeddings"), g.withInflight(g.embeddings)))
	mux.HandleFunc("GET /v1/models", g.withRequestMetrics(routeLabel("/v1/models"), g.models))
	mux.HandleFunc("GET /healthz", g.withRequestMetrics(routeLabel("/healthz"), func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	mux.HandleFunc("GET /readyz", g.withRequestMetrics(routeLabel("/readyz"), g.readyz))
	mux.Handle("GET /metrics", g.metrics.Handler())
	return mux
}

// readinessChecker is satisfied by an iamProvider that has a startup
// window during which it isn't yet safe to serve requests — currently
// only *KVIAM (review ruling I6: NewKVIAM returns immediately rather than
// blocking, so there is a real gap between "gateway process started" and
// "KVIAM has a real snapshot of the KEYS/ALIASES buckets"). staticIAM
// does not implement this interface at all, so isReady()'s type
// assertion simply fails for it and the gateway is always considered
// ready in static mode — there is no startup window to speak of, the
// static config is already fully loaded by the time Gateway exists.
type readinessChecker interface {
	Ready() <-chan struct{}
}

// budgetChecker is satisfied by an iamProvider that can report a key's
// monthly token budget as exhausted — currently only *KVIAM, sourcing the
// optional BUDGETS bucket (Task 7). staticIAM does not implement this
// interface at all, so budgetExceeded's type assertion simply fails for it
// and static-mode gateways never run a budget check — M2's static Config
// has no notion of usage tracking, and Task 7 deliberately leaves it that
// way rather than bolting a permanently-false BudgetExceeded onto
// staticIAM.
type budgetChecker interface {
	BudgetExceeded(keyID string) bool
}

// budgetExceeded reports whether key's budget is exhausted, for iam
// implementations that track one at all (kv mode only — see budgetChecker).
func (g *Gateway) budgetExceeded(key KeyConfig) bool {
	bc, ok := g.iam.(budgetChecker)
	return ok && bc.BudgetExceeded(keyID(key))
}

// isReady reports whether g.iam is either not a readinessChecker at all
// (static mode) or has completed its startup Ready() signal (kv mode,
// once both buckets have their first snapshot).
func (g *Gateway) isReady() bool {
	rc, ok := g.iam.(readinessChecker)
	if !ok {
		return true
	}
	select {
	case <-rc.Ready():
		return true
	default:
		return false
	}
}

// readyz backs GET /readyz: 200 once the gateway is able to make real
// auth/allowlist decisions, 503 while a kv-mode KVIAM is still doing its
// first scan of the KEYS/ALIASES buckets (review ruling I6).
func (g *Gateway) readyz(w http.ResponseWriter, _ *http.Request) {
	if g.isReady() {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprint(w, "unhealthy: kv iam not ready")
}

// checkReady writes a 503 and returns false if the gateway can't yet make
// a real auth decision (review ruling I6) — callers that authenticate
// (models, chatCompletions) must check this first, since answering 401 to
// every request during a kv-mode startup window would look identical to
// "every key was revoked" from the caller's side, which is a much worse
// failure mode than a transient 503.
func (g *Gateway) checkReady(w http.ResponseWriter) bool {
	if g.isReady() {
		return true
	}
	oaiError(w, http.StatusServiceUnavailable, "service_unavailable", "gateway is still loading key/alias state, try again shortly")
	return false
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
	return g.iam.AuthenticateKey(auth[len(prefix):])
}

func (g *Gateway) models(w http.ResponseWriter, r *http.Request) {
	if !g.checkReady(w) {
		return
	}
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
		if _, exists := g.iam.ResolveAlias(key.Org, alias); exists {
			// Only existence matters here — /v1/models lists the alias names
			// a key may use, never what they resolve to or with.
			out.Data = append(out.Data, model{ID: alias, Object: "model"})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// keyID returns key's stable attribution id for relay.Request.KeyID (M4
// Task 1, design-usage.md §3): key.ID when set — the apikey aggregate's
// stream id in kv mode, or an operator-supplied `id:` in static mode — or
// key.Name otherwise. The fallback covers both static config with no `id:`
// configured and KV entries projected before this field existed (old
// entries decode Id as "").
func keyID(key KeyConfig) string {
	if key.ID != "" {
		return key.ID
	}
	return key.Name
}

func newReqID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Request kinds, as carried in the Ib-Kind header (wire.HdrKind) that tells
// the worker which engine method answers a message: Chat/ChatStream for
// kindChat, Embed for kindEmbed. Only kindChat may stream.
const (
	kindChat  = "chat"
	kindEmbed = "embed"
)

// dispatched is what the shared request pipeline hands back to a handler
// once the request has been authorized, admitted, and published: everything
// the response half needs and nothing more.
type dispatched struct {
	// l is a live listener the caller MUST Close (defer d.l.Close()).
	l        *relay.Listener
	seq      uint64
	reqID    string
	deadline time.Time
	// stream is the client's requested response shape. Only ever true for
	// kindChat — dispatch rejects a streaming request for any other kind
	// before publishing it.
	stream bool
}

// dispatch runs the entire request pipeline shared by every inference
// endpoint: ready-gate → authenticate → read/limit body → parse model →
// resolve alias → allowlist → budget → admission → merge alias params →
// subscribe → publish. It writes the error response itself and reports
// false whenever the request must not proceed, so a handler's only job on
// false is to return.
//
// This is one function rather than a per-handler copy on purpose: the order
// of those gates is security-relevant (a forbidden alias must 403 before a
// budget check leaks whether the org is over quota; nothing may reach the
// data plane before admission), and two copies of an ordering is how one of
// them silently rots. Handlers differ only in the RESPONSE half — SSE for
// streaming chat, a single result frame for everything else — which stays
// in the handlers.
func (g *Gateway) dispatch(w http.ResponseWriter, r *http.Request, kind string) (dispatched, bool) {
	if !g.checkReady(w) {
		return dispatched{}, false
	}
	key, ok := g.authenticate(r)
	if !ok {
		oaiError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
		return dispatched{}, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			oaiError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the 1 MiB limit")
			return dispatched{}, false
		}
		oaiError(w, http.StatusBadRequest, "invalid_request_error", "unreadable body")
		return dispatched{}, false
	}
	var req struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Model == "" {
		oaiError(w, http.StatusBadRequest, "invalid_request_error", "body must be a JSON object with a model field")
		return dispatched{}, false
	}
	// Only chat has a streaming response shape. An embed request is answered
	// by exactly one result frame no matter what the body's stream flag says
	// (the worker ignores it outright), so honoring "stream":true silently
	// would hand the client a plain JSON body where it is parsing SSE —
	// telling it the request is invalid is the only honest answer. Checked
	// before any gate with a side effect, so a rejected request never counts
	// against a budget or a queue.
	if kind != kindChat && req.Stream {
		oaiError(w, http.StatusBadRequest, "invalid_request_error", fmt.Sprintf("streaming is not supported on %s", r.URL.Path))
		return dispatched{}, false
	}
	res, exists := g.iam.ResolveAlias(key.Org, req.Model)
	if !exists {
		oaiError(w, http.StatusNotFound, "model_not_found", fmt.Sprintf("unknown model alias %q", req.Model))
		return dispatched{}, false
	}
	target := res.Target
	if !slices.Contains(key.Allow, req.Model) {
		oaiError(w, http.StatusForbidden, "model_forbidden", fmt.Sprintf("key is not allowed to use %q", req.Model))
		return dispatched{}, false
	}
	if g.budgetExceeded(key) {
		oaiError(w, http.StatusPaymentRequired, "budget_exhausted", "monthly token budget exhausted")
		return dispatched{}, false
	}
	// Admission control is the last gate before the queue, and it is keyed
	// on the CONCRETE model (target), not the alias: backlog is a property
	// of the durable consumer a worker serves, which several aliases may
	// point at. Retry-After must be set before oaiError, which writes the
	// status immediately.
	if !g.admission.allow(r.Context(), target) {
		g.metrics.admissionRejected.WithLabelValues(target).Inc()
		w.Header().Set("Retry-After", strconv.Itoa(g.admission.retryAfter()))
		oaiError(w, http.StatusTooManyRequests, "overloaded", "model queue is full, retry later")
		return dispatched{}, false
	}

	// Alias params (kv mode only) are baked into the body the gateway
	// publishes, not carried as a side channel on relay.Request: the worker
	// and the engines are deliberately dumb about aliases — whatever reaches
	// them is already the request the operator meant. With no params this is
	// a no-op that returns the very same byte slice.
	body, err = mergeParams(body, res.Params)
	if err != nil {
		oaiError(w, http.StatusBadRequest, "invalid_request_error", "body must be a JSON object with a model field")
		return dispatched{}, false
	}

	reqID := newReqID()
	ctx := r.Context()
	deadline := time.Now().Add(g.cfg.RequestTimeout)

	l, err := relay.Listen(g.nc, reqID) // subscribe BEFORE publish
	if err != nil {
		oaiError(w, http.StatusInternalServerError, "internal_error", "subscribe failed")
		return dispatched{}, false
	}

	seq, err := relay.Publish(ctx, g.js, relay.Request{
		Model: target, Org: key.Org, Project: key.Project, KeyID: keyID(key),
		Alias: req.Model, ReqID: reqID, Kind: kind, Deadline: deadline, Body: body,
	})
	if err != nil {
		// The listener never reaches a handler on this path, so close it
		// here rather than leaking the subscription.
		l.Close()
		oaiError(w, http.StatusServiceUnavailable, "transport_error", "queue publish failed")
		return dispatched{}, false
	}
	return dispatched{l: l, seq: seq, reqID: reqID, deadline: deadline, stream: req.Stream}, true
}

func (g *Gateway) chatCompletions(w http.ResponseWriter, r *http.Request) {
	d, ok := g.dispatch(w, r, kindChat)
	if !ok {
		return
	}
	defer d.l.Close()

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
	if d.stream {
		g.streamOut(r.Context(), w, d.l, d.deadline, d.reqID, d.seq)
	} else {
		g.resultOut(r.Context(), w, d.l, d.deadline, d.reqID, d.seq)
	}
}

// embeddings backs POST /v1/embeddings. It is chatCompletions minus the
// streaming half: the same pipeline (see dispatch) publishes the request
// with Ib-Kind: embed, and the worker answers with exactly one result frame
// — which is precisely what resultOut already reads, including its
// disconnect/deadline cleanup.
func (g *Gateway) embeddings(w http.ResponseWriter, r *http.Request) {
	d, ok := g.dispatch(w, r, kindEmbed)
	if !ok {
		return
	}
	defer d.l.Close()
	g.resultOut(r.Context(), w, d.l, d.deadline, d.reqID, d.seq)
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
