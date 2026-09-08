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
