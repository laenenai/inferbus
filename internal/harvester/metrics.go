// metrics.go: hvMetrics is the Harvester's private, per-instance
// Prometheus metrics registry — see internal/gateway/metrics.go's package
// doc comment for why per-instance (never the global default registry) is
// a hard requirement here too: the test suite builds many Harvesters per
// process. New is hvMetrics' sole constructor; Harvester.Handler is its
// sole exported consumer, and doFlush (harvester.go) and reportBudgetGauge
// (budget.go, via Harvester.SetBudgetEntries) are its writers.
package harvester

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// hvMetrics holds the harvester's Prometheus instrumentation. Metric names
// are pinned exactly per the inferbus M5 hardening spec §6 — do not rename
// without updating the spec and any dashboards/alerts built on them. The
// only label in use, "state" on inferbus_budget_entries, is bounded to
// "ok"/"exceeded" — never a key id or org name.
type hvMetrics struct {
	reg            *prometheus.Registry
	rowsInserted   prometheus.Counter
	insertFailures prometheus.Counter
	budgetEntries  *prometheus.GaugeVec
}

// newHVMetrics builds a fresh hvMetrics bound to its own private registry.
func newHVMetrics() *hvMetrics {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	return &hvMetrics{
		reg: reg,
		rowsInserted: f.NewCounter(prometheus.CounterOpts{
			Name: "inferbus_usage_rows_inserted_total",
			Help: "Total usage rows successfully inserted into the sink.",
		}),
		insertFailures: f.NewCounter(prometheus.CounterOpts{
			Name: "inferbus_insert_failures_total",
			Help: "Total usage rows affected by a failed Sink.InsertBatch call (nak'd for redelivery).",
		}),
		budgetEntries: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "inferbus_budget_entries",
			Help: "Current count of budget ledger entries, by state (ok or exceeded).",
		}, []string{"state"}),
	}
}

// Handler serves this harvester's metrics in the Prometheus text
// exposition format, scoped to hvMetrics' own private registry.
func (m *hvMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// setBudgetEntries sets (never increments/decrements) the
// inferbus_budget_entries gauge's "ok" and "exceeded" states from the
// budget ledger's own authoritative current counts — see
// BudgetLedger.reportBudgetGauge in budget.go, reached via
// Harvester.SetBudgetEntries, its only caller.
func (m *hvMetrics) setBudgetEntries(ok, exceeded int) {
	m.budgetEntries.WithLabelValues("ok").Set(float64(ok))
	m.budgetEntries.WithLabelValues("exceeded").Set(float64(exceeded))
}
