package harvester_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/laenenai/inferbus/internal/harvester"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
)

// scrapeHarvester GETs h.Handler() directly (no real HTTP listener needed)
// and returns the response body.
func scrapeHarvester(t *testing.T, h *harvester.Harvester) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	h.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

// counterValue extracts the value of an unlabeled counter named `name` from
// a Prometheus text-exposition scrape body, e.g. "inferbus_foo_total 3".
func counterValue(t *testing.T, body, name string) float64 {
	t.Helper()
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `\s+([0-9eE+\-.]+)$`)
	m := re.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("metric %q not found in body:\n%s", name, body)
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		t.Fatalf("parse %q value %q: %v", name, m[1], err)
	}
	return v
}

// TestMetricsRowsInsertedAfterSuccessfulFlush is the Step 1 required test:
// after a FakeSink batch insert, Handler()'s scrape body must contain
// inferbus_usage_rows_inserted_total with a value at least the batch size.
func TestMetricsRowsInsertedAfterSuccessfulFlush(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	ids := []string{"met-r1", "met-r2", "met-r3"}
	for _, id := range ids {
		publishUsage(t, js, usageEvent(id))
	}

	sink := harvester.NewFakeSink()
	h := harvester.New(nc, js, sink, harvester.Config{
		BatchMaxEvents:   500,
		BatchMaxInterval: 50 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = h.Run(runCtx) }()

	waitFor(t, 5*time.Second, func() bool { return len(sink.RowsSnapshot()) >= len(ids) })

	body := scrapeHarvester(t, h)
	if !strings.Contains(body, "inferbus_usage_rows_inserted_total") {
		t.Fatalf("metrics body missing inferbus_usage_rows_inserted_total:\n%s", body)
	}
	if got := counterValue(t, body, "inferbus_usage_rows_inserted_total"); got < float64(len(ids)) {
		t.Errorf("inferbus_usage_rows_inserted_total = %v, want >= %d", got, len(ids))
	}
}

// TestMetricsInsertFailuresAfterFailNext is the Step 1 required test: after
// a FakeSink.FailNext-induced InsertBatch error, Handler()'s scrape body
// must contain inferbus_insert_failures_total >= 1.
func TestMetricsInsertFailuresAfterFailNext(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	publishUsage(t, js, usageEvent("met-fail-1"))

	sink := harvester.NewFakeSink()
	sink.FailNext = errors.New("injected insert failure")
	h := harvester.New(nc, js, sink, harvester.Config{
		BatchMaxEvents:   500,
		BatchMaxInterval: 30 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = h.Run(runCtx) }()

	waitFor(t, 5*time.Second, func() bool {
		body := scrapeHarvester(t, h)
		return strings.Contains(body, "inferbus_insert_failures_total") &&
			counterValue(t, body, "inferbus_insert_failures_total") >= 1
	})
}

// TestMultipleHarvestersNoPanic is the HARD RULE regression test: many
// Harvesters get built per test process, so metric registration must never
// hit the global default registry.
func TestMultipleHarvestersNoPanic(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	for range 3 {
		h := harvester.New(nc, js, harvester.NewFakeSink(), harvester.Config{})
		_ = scrapeHarvester(t, h)
	}
}
