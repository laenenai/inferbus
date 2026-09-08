package harvester

// This file is package harvester (white-box), not harvester_test, because
// C1 and I1 require deterministically injecting a mutation into the exact
// race window flushOne's version check guards: after its KV Put/Delete
// round-trip returns, but before it re-locks to reconcile in-memory state.
// That window is only reachable from outside the package by winning a real
// goroutine-scheduling race, which would make these tests flaky by
// construction; testAfterKVOp (an unexported, always-nil-in-production
// hook — see its field doc comment in budget.go) lets us hit it exactly
// once, deterministically, instead. budget_test.go (package harvester_test)
// covers every other required scenario across the ordinary public API.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/laenenai/inferbus/internal/testutil"
)

// TestBudgetLedger_FlushOneDeleteRace_ResurrectDuringKVRoundTrip covers C1:
// a KEYS update resurrecting a key (row.toDelete flips back to false, via
// reconcileKeys) that races flushOne's delete round-trip must NOT be
// untracked by flushOne once the round-trip returns — the row must stay in
// l.entries, stay dirty, and its BUDGETS entry must reappear on the very
// next flush, rather than the key silently vanishing from the ledger while
// KEYS still (once again) carries it.
func TestBudgetLedger_FlushOneDeleteRace_ResurrectDuringKVRoundTrip(t *testing.T) {
	_, js := testutil.RunNATS(t)
	ctx := context.Background()

	kv, err := ensureBudgetsBucket(ctx, js)
	if err != nil {
		t.Fatalf("ensure BUDGETS bucket: %v", err)
	}

	l := NewBudgetLedger(js, NewFakeSink(), time.Hour)
	l.runCtx = ctx
	l.kv = kv

	const id = "key-c1"
	l.mu.Lock()
	l.entries[id] = &ledgerRow{budget: 500, used: 100, month: "2026-09", baselineLoaded: true, toDelete: true}
	l.dirty[id] = true
	l.mu.Unlock()

	// Fire exactly once, between flushOne's KV Delete call returning and its
	// re-lock: a KEYS update lands that resurrects the key (budget still
	// present under some hash) before flushOne finalizes the delete.
	fired := false
	l.testAfterKVOp = func() {
		if fired {
			return
		}
		fired = true
		l.reconcileKeys(map[string]*keyFold{id: {budget: 500}})
	}

	l.flushOne(id)

	l.mu.Lock()
	row, tracked := l.entries[id]
	stillDirty := l.dirty[id]
	l.mu.Unlock()

	if !tracked {
		t.Fatal("C1 regression: key was untracked by flushOne despite being resurrected mid-delete")
	}
	if row.toDelete {
		t.Fatal("C1 regression: row still marked toDelete after being resurrected mid-delete")
	}
	if !stillDirty {
		t.Fatal("row must stay dirty so the next flush republishes the resurrected state")
	}

	// Next flush (the "next tick"): now that the race is over, this Put
	// must actually land, and the BUDGETS entry must reappear.
	l.testAfterKVOp = nil
	l.flushOne(id)

	entry, err := kv.Get(ctx, id)
	if err != nil {
		t.Fatalf("C1 regression: BUDGETS entry did not reappear after resurrection: %v", err)
	}
	if len(entry.Value()) == 0 {
		t.Fatal("BUDGETS entry reappeared empty")
	}
}

// TestBudgetLedger_FlushOnePutRace_AddUsageDuringKVRoundTrip_StaysDirty
// covers I1: an AddUsage call crossing the budget, landing in the same race
// window (after flushOne's KV Put round-trip returns, before it re-locks),
// must NOT have the dirty flag cleared based on the stale (pre-race)
// snapshot flushOne already published — the row must stay dirty so the very
// next flush republishes the newer, exceeded state.
func TestBudgetLedger_FlushOnePutRace_AddUsageDuringKVRoundTrip_StaysDirty(t *testing.T) {
	_, js := testutil.RunNATS(t)
	ctx := context.Background()

	kv, err := ensureBudgetsBucket(ctx, js)
	if err != nil {
		t.Fatalf("ensure BUDGETS bucket: %v", err)
	}

	l := NewBudgetLedger(js, NewFakeSink(), time.Hour)
	l.runCtx = ctx
	l.kv = kv

	const id = "key-i1"
	l.mu.Lock()
	l.entries[id] = &ledgerRow{budget: 1000, used: 900, month: "2026-09", baselineLoaded: true, exceeded: false}
	l.dirty[id] = true
	l.mu.Unlock()

	// Fire exactly once, between the Put that publishes {used:900,
	// exceeded:false} returning and flushOne's re-lock: usage crosses the
	// budget mid-flight.
	fired := false
	l.testAfterKVOp = func() {
		if fired {
			return
		}
		fired = true
		l.AddUsage(Row{KeyID: id, PromptTokens: 150, CompletionTokens: 50}) // 900+200=1100 >= 1000
	}

	l.flushOne(id)

	l.mu.Lock()
	stillDirty := l.dirty[id]
	row := l.entries[id]
	l.mu.Unlock()

	if !stillDirty {
		t.Fatal("I1 regression: dirty flag was cleared despite AddUsage racing the Put's KV round-trip")
	}
	if !row.exceeded {
		t.Fatal("row's in-memory Exceeded should already reflect the race (AddUsage always applies synchronously)")
	}

	// The published entry after the raced Put must still be the stale,
	// pre-race snapshot: flushOne isn't allowed to have picked up the
	// racing mutation for the Put it already issued.
	entry, err := kv.Get(ctx, id)
	if err != nil {
		t.Fatalf("get BUDGETS entry: %v", err)
	}
	var published struct {
		Used     int64 `json:"used"`
		Exceeded bool  `json:"exceeded"`
	}
	if err := json.Unmarshal(entry.Value(), &published); err != nil {
		t.Fatalf("unmarshal published entry: %v", err)
	}
	if published.Used != 900 || published.Exceeded {
		t.Fatalf("published entry = %+v, want the stale pre-race snapshot {900,false}", published)
	}

	// Next flush ("next tick"): must now publish the exceeded state.
	l.testAfterKVOp = nil
	l.flushOne(id)

	entry, err = kv.Get(ctx, id)
	if err != nil {
		t.Fatalf("get BUDGETS entry after second flush: %v", err)
	}
	if err := json.Unmarshal(entry.Value(), &published); err != nil {
		t.Fatalf("unmarshal published entry: %v", err)
	}
	if published.Used != 1100 || !published.Exceeded {
		t.Fatalf("I1 regression: next flush published %+v, want {1100,true}", published)
	}
}
