// metrics.go: gwMetrics is the Gateway's private, per-instance Prometheus
// metrics registry. Every metric is registered against its own
// prometheus.NewRegistry() (via promauto.With(reg)) rather than the global
// default registry — the test suite constructs many Gateways within a
// single process, and registering the same metric names twice against the
// default registry panics. newGateway is gwMetrics' sole constructor, and
// Routes' GET /metrics is its sole consumer.
package gateway

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// gwMetrics holds the gateway's Prometheus instrumentation. Metric names
// are pinned exactly per the inferbus M5 hardening spec §6 — do not rename
// without updating the spec and any dashboards/alerts built on them.
// Labels are deliberately bounded (route/code/model — see routeLabel and
// the admission-rejected call site) so no key id or org name can ever
// become a label value and blow up cardinality.
type gwMetrics struct {
	reg               *prometheus.Registry
	requests          *prometheus.CounterVec
	admissionRejected *prometheus.CounterVec
	inflight          prometheus.Gauge
}

// newGWMetrics builds a fresh gwMetrics bound to its own private registry.
func newGWMetrics() *gwMetrics {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	return &gwMetrics{
		reg: reg,
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "inferbus_requests_total",
			Help: "Total HTTP requests handled by the gateway, by route and response status code.",
		}, []string{"route", "code"}),
		admissionRejected: f.NewCounterVec(prometheus.CounterOpts{
			Name: "inferbus_admission_rejected_total",
			Help: "Total chat completion requests rejected by admission control (429), by concrete model.",
		}, []string{"model"}),
		inflight: f.NewGauge(prometheus.GaugeOpts{
			Name: "inferbus_inflight_requests",
			Help: "Number of chat completion requests currently being handled by the gateway.",
		}),
	}
}

// Handler serves this gateway's metrics in the Prometheus text exposition
// format, scoped to gwMetrics' own private registry — never the global
// DefaultGatherer.
func (m *gwMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// routeLabel maps an HTTP path to the bounded route label the cardinality
// guard requires: route ∈ {chat, embeddings, models, other}. There is
// deliberately no embeddings HTTP handler in this repo yet — the label
// value exists for forward compatibility with spec §6, not because
// anything emits it today. Every other path (healthz, readyz, and any
// future route) folds into "other" rather than ever emitting a raw path.
func routeLabel(path string) string {
	switch path {
	case "/v1/chat/completions":
		return "chat"
	case "/v1/embeddings":
		return "embeddings"
	case "/v1/models":
		return "models"
	default:
		return "other"
	}
}

// statusRecorder wraps an http.ResponseWriter to capture the status code
// ultimately written, defaulting to 200 if the handler never calls
// WriteHeader explicitly (net/http applies the same default on a bare
// Write). It forwards http.Flusher so streamOut's SSE flushing keeps
// working through the metrics middleware below.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wrote {
		r.status = code
		r.wrote = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wrote {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the wrapped ResponseWriter to the net/http machinery that
// reaches the real writer by unwrapping rather than by type assertion:
// http.NewResponseController, and http.MaxBytesReader's probe for the
// unexported requestTooLarge() interface that tells the server to close the
// connection after an oversized body (gateway.go's 1 MiB body limit).
// Without it those probes stop at this wrapper and silently no-op.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// withRequestMetrics wraps h so every response increments
// inferbus_requests_total{route=route, code=<final status>} exactly once,
// regardless of which of a handler's many write sites (oaiError,
// streamOut, resultOut, a plain 200 from models) ultimately decided the
// status — instrumenting every one of those sites individually would mean
// keeping this metric in sync with every future error path chatCompletions
// grows; wrapping the ResponseWriter once here can't drift out of sync.
func (g *Gateway) withRequestMetrics(route string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		// Deferred so a panicking handler is still counted: net/http's
		// per-connection recover turns it into a 500 the client sees, and a
		// counter documented as "exactly once per response" must not have a
		// hole exactly where things went most wrong.
		defer func() {
			g.metrics.requests.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
		}()
		h(rec, r)
	}
}

// withInflight wraps h so inferbus_inflight_requests reflects exactly the
// requests currently executing inside h: incremented before, decremented
// after via defer (so it still decrements on an early return or panic).
func (g *Gateway) withInflight(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		g.metrics.inflight.Inc()
		defer g.metrics.inflight.Dec()
		h(w, r)
	}
}
