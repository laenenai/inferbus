package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/laenenai/inferbus/internal/testutil"
)

// scrapeMetrics GETs /metrics on srv and returns the response body.
func scrapeMetrics(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("get /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics body: %v", err)
	}
	return string(body)
}

// TestMetricsRecordsRequestsByRouteAndCode is the Step 1 required test: a
// 401 from missing auth on /v1/models must show up on /metrics as
// inferbus_requests_total with a code="401" label.
func TestMetricsRecordsRequestsByRouteAndCode(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	g := New(nc, js, Config{})
	srv := httptest.NewServer(g.Routes())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/v1/models")
	if err != nil {
		t.Fatalf("get /v1/models: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}

	body := scrapeMetrics(t, srv)
	if !strings.Contains(body, "inferbus_requests_total") {
		t.Fatalf("metrics body missing inferbus_requests_total:\n%s", body)
	}
	if !strings.Contains(body, `code="401"`) {
		t.Fatalf("metrics body missing code=\"401\" label:\n%s", body)
	}
	if !strings.Contains(body, `route="models"`) {
		t.Fatalf("metrics body missing route=\"models\" label:\n%s", body)
	}
}

// TestMultipleGatewaysNoPanic is the HARD RULE regression test: the test
// suite constructs many Gateways per process, so metric registration must
// never hit the global default registry (which panics on a second
// registration of the same metric names). Each Gateway must own a private
// *prometheus.Registry.
func TestMultipleGatewaysNoPanic(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	for range 3 {
		g := New(nc, js, Config{})
		srv := httptest.NewServer(g.Routes())
		resp, err := http.Get(srv.URL + "/metrics")
		if err != nil {
			t.Fatalf("get /metrics: %v", err)
		}
		resp.Body.Close()
		srv.Close()
	}
}

// TestAdmissionRejectedMetric verifies a 429 from admission control
// increments inferbus_admission_rejected_total{model=<concrete model>},
// visible on /metrics.
func TestAdmissionRejectedMetric(t *testing.T) {
	srv, js, _ := startAdmissionStack(t, AdmissionConfig{MaxBacklog: 2, RetryAfterSeconds: 1})
	ensureDurable(t, js, "m1")
	fillQueue(t, js, "m1", 3)

	resp := postChat(t, srv, "a1")
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}

	body := scrapeMetrics(t, srv)
	if !strings.Contains(body, "inferbus_admission_rejected_total") {
		t.Fatalf("metrics body missing inferbus_admission_rejected_total:\n%s", body)
	}
	if !strings.Contains(body, `model="m1"`) {
		t.Fatalf("metrics body missing model=\"m1\" label:\n%s", body)
	}
}

// TestInflightGaugeExposed verifies inferbus_inflight_requests is present
// on /metrics (its steady-state value at rest is 0, not asserted here
// since driving a mid-flight sample deterministically needs no other seam
// this task adds).
func TestInflightGaugeExposed(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	g := New(nc, js, Config{})
	srv := httptest.NewServer(g.Routes())
	t.Cleanup(srv.Close)

	body := scrapeMetrics(t, srv)
	if !strings.Contains(body, "inferbus_inflight_requests") {
		t.Fatalf("metrics body missing inferbus_inflight_requests:\n%s", body)
	}
}

// TestStatusRecorderUnwrap: statusRecorder must be transparent to the
// net/http machinery that reaches the real ResponseWriter by unwrapping —
// http.NewResponseController (and, via the same unexported interface probe,
// http.MaxBytesReader's connection-close signal). Without Unwrap, the
// controller cannot find a Flusher and SSE through this middleware would
// silently stop flushing the day the handler switches to it.
func TestStatusRecorderUnwrap(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	g := New(nc, js, Config{})

	var flushErr error
	h := g.withRequestMetrics("chat", func(w http.ResponseWriter, r *http.Request) {
		if rec, ok := w.(*statusRecorder); !ok {
			t.Errorf("handler saw %T, want *statusRecorder", w)
		} else if rec.Unwrap() != rec.ResponseWriter {
			t.Errorf("Unwrap() did not return the wrapped ResponseWriter")
		}
		w.WriteHeader(http.StatusOK)
		flushErr = http.NewResponseController(w).Flush()
	})

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if flushErr != nil {
		t.Fatalf("ResponseController.Flush through statusRecorder: %v, want nil", flushErr)
	}
}

// TestRequestMetricsCountsPanickingHandler: inferbus_requests_total's
// contract is "exactly once per response". A handler that panics unwinds
// past a non-deferred Inc, so the request would never be counted even though
// net/http's per-connection recover still emits a 500. Defer makes the
// contract true.
func TestRequestMetricsCountsPanickingHandler(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	g := New(nc, js, Config{})

	h := g.withRequestMetrics("chat", func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})

	func() {
		defer func() {
			if recover() == nil {
				t.Error("handler panic did not propagate; the middleware must not swallow it")
			}
		}()
		h(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	}()

	srv := httptest.NewServer(g.Routes())
	t.Cleanup(srv.Close)
	body := scrapeMetrics(t, srv)
	if !strings.Contains(body, `inferbus_requests_total{code="200",route="chat"} 1`) {
		t.Fatalf("panicking request was not counted; /metrics body:\n%s", body)
	}
}
