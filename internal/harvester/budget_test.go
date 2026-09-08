package harvester_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/cpkv"
	"github.com/laenenai/inferbus/internal/harvester"
	"github.com/laenenai/inferbus/internal/testutil"
)

// createKeysBucket creates the KEYS bucket (idempotently) directly against
// js — these tests feed BudgetLedger's KEYS watch with raw Puts against the
// same schema controlplane's keys projector writes (cpkv.KeyEntry), never
// through the projector itself, mirroring
// internal/gateway/kviam_test.go's createBucket/putKeyEntry pattern (the
// task brief's explicit model for this style of test).
func createKeysBucket(t *testing.T, js jetstream.JetStream) jetstream.KeyValue {
	t.Helper()
	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: cpkv.BucketKeys})
	if err != nil {
		t.Fatalf("create KEYS bucket: %v", err)
	}
	return kv
}

func putKeyEntry(t *testing.T, kv jetstream.KeyValue, hash string, entry cpkv.KeyEntry) {
	t.Helper()
	b, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(context.Background(), hash, b); err != nil {
		t.Fatalf("put key entry %q: %v", hash, err)
	}
}

// getBudgetEntry reads keyID's BUDGETS entry directly off js, mirroring how
// the gateway's KV-mode budget enforcement (design-usage.md §"Gateway
// enforcement") would read it. ok is false if the BUDGETS bucket or the
// entry doesn't exist yet.
func getBudgetEntry(t *testing.T, js jetstream.JetStream, keyID string) (cpkv.BudgetEntry, bool) {
	t.Helper()
	kv, err := js.KeyValue(context.Background(), cpkv.BucketBudgets)
	if err != nil {
		return cpkv.BudgetEntry{}, false
	}
	entry, err := kv.Get(context.Background(), keyID)
	if err != nil {
		return cpkv.BudgetEntry{}, false
	}
	var v cpkv.BudgetEntry
	if err := json.Unmarshal(entry.Value(), &v); err != nil {
		t.Fatalf("unmarshal budget entry %q: %v", keyID, err)
	}
	return v, true
}

// pollUntil polls check every 20ms until it returns true or timeout elapses
// (failing the test on timeout) — the codebase's established poll-based
// wait idiom (see internal/gateway/kviam_test.go's pollUntil), never a bare
// sleep as the sole synchronization for an async watcher/flush update.
func pollUntil(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startBudgetLedger runs l.Run in the background and arranges for it to be
// stopped (context canceled, Run's return observed) at test cleanup.
func startBudgetLedger(t *testing.T, l *harvester.BudgetLedger) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("BudgetLedger.Run did not return after context cancel")
		}
	})
}

// insertRows seeds sink with rows directly (bypassing the harvester's own
// consumer), simulating usage that already landed in the ledger database
// before the budget ledger ever looked at this key — this is what
// MonthToDate baselines are computed from.
func insertRows(t *testing.T, sink *harvester.FakeSink, rows ...harvester.Row) {
	t.Helper()
	if err := sink.InsertBatch(context.Background(), rows); err != nil {
		t.Fatalf("seed sink rows: %v", err)
	}
}

// TestBudgetLedger_NewBudgetedKey_BaselineFromMonthToDate covers scenario
// (a): a KEYS entry with a monthly budget appearing (via the ledger's
// initial KEYS scan) must produce a BUDGETS entry whose Used field is
// seeded from Sink.MonthToDate for the current month — not zero — so
// pre-existing usage this month is immediately reflected.
func TestBudgetLedger_NewBudgetedKey_BaselineFromMonthToDate(t *testing.T) {
	_, js := testutil.RunNATS(t)
	keysKV := createKeysBucket(t, js)

	sink := harvester.NewFakeSink()
	insertRows(t, sink,
		harvester.Row{ReqID: "r1", TS: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), KeyID: "key-a", PromptTokens: 1000, CompletionTokens: 500},
		harvester.Row{ReqID: "r2", TS: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC), KeyID: "key-a", PromptTokens: 300, CompletionTokens: 200},
	)
	// 1500 + 500 = 2000 tokens already on the books for key-a this month.

	putKeyEntry(t, keysKV, cpkv.HashKey("plaintext-a"), cpkv.KeyEntry{Id: "key-a", MonthlyTokenBudget: 5000})

	l := harvester.NewBudgetLedger(js, sink, 100*time.Millisecond)
	l.SetNowFn(func() time.Time { return time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC) })
	startBudgetLedger(t, l)

	pollUntil(t, 5*time.Second, func() bool {
		e, ok := getBudgetEntry(t, js, "key-a")
		return ok && e.Used == 2000 && e.Budget == 5000 && e.Month == "2026-09" && !e.Exceeded
	})
}

// TestBudgetLedger_AddUsageCrossesBudget_FlipsExceededOnFlush covers
// scenario (b): AddUsage (the Harvester.OnRow hook) must accumulate onto
// the in-memory ledger and, on the next periodic flush, the BUDGETS entry's
// Exceeded field must flip true once used reaches budget.
func TestBudgetLedger_AddUsageCrossesBudget_FlipsExceededOnFlush(t *testing.T) {
	_, js := testutil.RunNATS(t)
	keysKV := createKeysBucket(t, js)
	putKeyEntry(t, keysKV, cpkv.HashKey("plaintext-b"), cpkv.KeyEntry{Id: "key-b", MonthlyTokenBudget: 1000})

	sink := harvester.NewFakeSink()
	l := harvester.NewBudgetLedger(js, sink, 100*time.Millisecond)
	l.SetNowFn(func() time.Time { return time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC) })
	startBudgetLedger(t, l)

	pollUntil(t, 5*time.Second, func() bool {
		e, ok := getBudgetEntry(t, js, "key-b")
		return ok && e.Used == 0 && e.Budget == 1000 && !e.Exceeded
	})

	l.AddUsage(harvester.Row{KeyID: "key-b", PromptTokens: 600, CompletionTokens: 500}) // 1100 >= 1000

	pollUntil(t, 5*time.Second, func() bool {
		e, ok := getBudgetEntry(t, js, "key-b")
		return ok && e.Used == 1100 && e.Exceeded
	})
}

// TestBudgetLedger_BudgetRaisedViaKEYSUpdate_ExceededClears covers scenario
// (c): once a key is over budget, a KEYS update raising its
// MonthlyTokenBudget (simulating apikey.LimitsChanged, kvproj.go's
// LimitsChanged arm, which re-Puts the entry under the SAME hash) must
// clear Exceeded on the next flush, without touching Used.
func TestBudgetLedger_BudgetRaisedViaKEYSUpdate_ExceededClears(t *testing.T) {
	_, js := testutil.RunNATS(t)
	keysKV := createKeysBucket(t, js)
	hash := cpkv.HashKey("plaintext-c")
	putKeyEntry(t, keysKV, hash, cpkv.KeyEntry{Id: "key-c", MonthlyTokenBudget: 100})

	sink := harvester.NewFakeSink()
	l := harvester.NewBudgetLedger(js, sink, 100*time.Millisecond)
	l.SetNowFn(func() time.Time { return time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC) })
	startBudgetLedger(t, l)

	pollUntil(t, 5*time.Second, func() bool {
		_, ok := getBudgetEntry(t, js, "key-c")
		return ok
	})

	l.AddUsage(harvester.Row{KeyID: "key-c", PromptTokens: 100, CompletionTokens: 50}) // 150 >= 100
	pollUntil(t, 5*time.Second, func() bool {
		e, ok := getBudgetEntry(t, js, "key-c")
		return ok && e.Exceeded && e.Used == 150
	})

	// Raise the budget via a re-Put under the same hash, as LimitsChanged does.
	putKeyEntry(t, keysKV, hash, cpkv.KeyEntry{Id: "key-c", MonthlyTokenBudget: 1000})

	pollUntil(t, 5*time.Second, func() bool {
		e, ok := getBudgetEntry(t, js, "key-c")
		return ok && !e.Exceeded && e.Used == 150 && e.Budget == 1000
	})
}

// TestBudgetLedger_RotationTwoHashesSameID_FoldedIntoOneEntry covers
// scenario (d) and binding ruling #1: KEYS is keyed by hash, but a key
// rotation transiently leaves two hash entries carrying the same Id in the
// bucket at once. The ledger must fold these into exactly one BUDGETS
// entry (keyed by Id), and usage attributed to that Id via AddUsage must be
// counted exactly once — not doubled because two hash entries exist.
func TestBudgetLedger_RotationTwoHashesSameID_FoldedIntoOneEntry(t *testing.T) {
	_, js := testutil.RunNATS(t)
	keysKV := createKeysBucket(t, js)

	sink := harvester.NewFakeSink()
	insertRows(t, sink,
		harvester.Row{ReqID: "r1", TS: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), KeyID: "key-d", PromptTokens: 80, CompletionTokens: 20},
	) // baseline: 100

	putKeyEntry(t, keysKV, cpkv.HashKey("plaintext-d-old"), cpkv.KeyEntry{Id: "key-d", MonthlyTokenBudget: 500})
	putKeyEntry(t, keysKV, cpkv.HashKey("plaintext-d-new"), cpkv.KeyEntry{Id: "key-d", MonthlyTokenBudget: 500})

	l := harvester.NewBudgetLedger(js, sink, 100*time.Millisecond)
	l.SetNowFn(func() time.Time { return time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC) })
	startBudgetLedger(t, l)

	pollUntil(t, 5*time.Second, func() bool {
		e, ok := getBudgetEntry(t, js, "key-d")
		return ok && e.Used == 100 && e.Budget == 500
	})

	l.AddUsage(harvester.Row{KeyID: "key-d", PromptTokens: 30, CompletionTokens: 20}) // +50

	pollUntil(t, 5*time.Second, func() bool {
		e, ok := getBudgetEntry(t, js, "key-d")
		return ok && e.Used == 150 // 100 + 50, exactly once despite two hash entries
	})

	// Give any (erroneous) double-accounting a moment to also land, then
	// re-assert Used is still exactly 150.
	time.Sleep(300 * time.Millisecond)
	e, ok := getBudgetEntry(t, js, "key-d")
	if !ok || e.Used != 150 {
		t.Fatalf("BudgetEntry = %+v, ok=%v, want Used == 150 exactly (single accounting)", e, ok)
	}
}

// TestBudgetLedger_KeyRemoved_BudgetEntryDeleted covers scenario (e) and
// binding ruling #4: once every hash entry carrying a key's Id is gone from
// KEYS (revoked, or rotated away with the old hash finally deleted), the
// ledger must delete the corresponding BUDGETS entry rather than leave it
// stale.
func TestBudgetLedger_KeyRemoved_BudgetEntryDeleted(t *testing.T) {
	_, js := testutil.RunNATS(t)
	keysKV := createKeysBucket(t, js)
	hash := cpkv.HashKey("plaintext-e")
	putKeyEntry(t, keysKV, hash, cpkv.KeyEntry{Id: "key-e", MonthlyTokenBudget: 300})

	sink := harvester.NewFakeSink()
	l := harvester.NewBudgetLedger(js, sink, 100*time.Millisecond)
	l.SetNowFn(func() time.Time { return time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC) })
	startBudgetLedger(t, l)

	pollUntil(t, 5*time.Second, func() bool {
		_, ok := getBudgetEntry(t, js, "key-e")
		return ok
	})

	if err := keysKV.Delete(context.Background(), hash); err != nil {
		t.Fatalf("delete key entry: %v", err)
	}

	pollUntil(t, 5*time.Second, func() bool {
		_, ok := getBudgetEntry(t, js, "key-e")
		return !ok
	})
}

// TestBudgetLedger_MonthRollover_RecomputesUsedFromNewMonth covers scenario
// (f) and binding ruling #3: a wall-clock month change (detected by the
// ledger's periodic tick against its injectable nowFn) must recompute every
// budgeted key's Used via Sink.MonthToDate for the new month and rewrite
// its BUDGETS entry — proven here by seeding a distinct, nonzero
// October value so a passing test can't be explained by the ledger simply
// zeroing Used on rollover instead of actually querying the new month.
func TestBudgetLedger_MonthRollover_RecomputesUsedFromNewMonth(t *testing.T) {
	_, js := testutil.RunNATS(t)
	keysKV := createKeysBucket(t, js)
	putKeyEntry(t, keysKV, cpkv.HashKey("plaintext-f"), cpkv.KeyEntry{Id: "key-f", MonthlyTokenBudget: 10000})

	sink := harvester.NewFakeSink()
	insertRows(t, sink,
		harvester.Row{ReqID: "sep-1", TS: time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC), KeyID: "key-f", PromptTokens: 400, CompletionTokens: 100},
	) // September baseline: 500

	var mu sync.Mutex
	now := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	l := harvester.NewBudgetLedger(js, sink, 100*time.Millisecond)
	l.SetNowFn(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	})
	startBudgetLedger(t, l)

	pollUntil(t, 5*time.Second, func() bool {
		e, ok := getBudgetEntry(t, js, "key-f")
		return ok && e.Used == 500 && e.Month == "2026-09"
	})

	// Seed a distinct October value before rolling the clock over, so the
	// post-rollover assertion actually proves a fresh MonthToDate query for
	// the new month happened.
	insertRows(t, sink,
		harvester.Row{ReqID: "oct-1", TS: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC), KeyID: "key-f", PromptTokens: 30, CompletionTokens: 12},
	) // October baseline: 42

	mu.Lock()
	now = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	mu.Unlock()

	pollUntil(t, 5*time.Second, func() bool {
		e, ok := getBudgetEntry(t, js, "key-f")
		return ok && e.Used == 42 && e.Month == "2026-10"
	})
}
