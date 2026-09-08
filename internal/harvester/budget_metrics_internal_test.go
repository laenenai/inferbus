package harvester

// This file is package harvester (white-box), not harvester_test, so it can
// manipulate BudgetLedger.entries directly rather than driving a full
// KEYS-watch + Sink.MonthToDate harness just to get rows into ok/exceeded
// states — mirroring budget_race_test.go's rationale for living in this
// package.

import (
	"testing"
	"time"
)

// TestReportBudgetGaugeCountsOkAndExceeded verifies reportBudgetGauge sums
// live (non-toDelete) entries into ok/exceeded counts and reports them via
// the registered fn — the mechanism NewBudgetLedger's production caller
// wires to a Harvester's inferbus_budget_entries gauge via
// SetBudgetGaugeFunc/Harvester.SetBudgetEntries.
func TestReportBudgetGaugeCountsOkAndExceeded(t *testing.T) {
	l := NewBudgetLedger(nil, NewFakeSink(), time.Hour)
	l.entries = map[string]*ledgerRow{
		"k1": {budget: 100, used: 50, exceeded: false},
		"k2": {budget: 100, used: 150, exceeded: true},
		"k3": {budget: 100, used: 10, exceeded: false},
		"k4": {budget: 100, used: 200, exceeded: true, toDelete: true}, // must be excluded
	}

	var gotOK, gotExceeded int
	calls := 0
	l.SetBudgetGaugeFunc(func(ok, exceeded int) {
		calls++
		gotOK, gotExceeded = ok, exceeded
	})

	l.reportBudgetGauge()

	if calls != 1 {
		t.Fatalf("fn called %d times, want 1", calls)
	}
	if gotOK != 2 {
		t.Errorf("ok = %d, want 2", gotOK)
	}
	if gotExceeded != 1 {
		t.Errorf("exceeded = %d, want 1 (toDelete entry must be excluded)", gotExceeded)
	}
}

// TestReportBudgetGaugeNilFnNoop verifies reportBudgetGauge is a safe no-op
// when no gauge fn has been registered — every pre-existing BudgetLedger
// test (and any standalone use of the type) never calls SetBudgetGaugeFunc
// at all.
func TestReportBudgetGaugeNilFnNoop(t *testing.T) {
	l := NewBudgetLedger(nil, NewFakeSink(), time.Hour)
	l.entries = map[string]*ledgerRow{"k1": {budget: 100, used: 50}}
	l.reportBudgetGauge() // must not panic
}
