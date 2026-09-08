package harvester

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laenenai/inferbus/internal/wire"
)

// TestRowFromEventMapping verifies all fields are correctly mapped from
// wire.UsageEvent to Row, including int to int64 widening and Estimated.
func TestRowFromEventMapping(t *testing.T) {
	now := time.Date(2026, 9, 8, 15, 30, 45, 123456789, time.UTC)
	ev := wire.UsageEvent{
		ReqID:     "req-123",
		Org:       "acme",
		Project:   "project-1",
		KeyID:     "key-456",
		Alias:     "alias-prod",
		Model:     "claude-opus-4",
		Provider:  "anthropic",
		Kind:      "chat",
		Status:    "ok",
		ErrorCode: "",
		WorkerID:  "worker-789",
		Usage: wire.Usage{
			PromptTokens:     100,
			CompletionTokens: 50,
			CachedTokens:     10,
		},
		TTFTMillis:     150,
		DurationMillis: 500,
		QueueMillis:    50,
		Estimated:      true,
		TS:             now,
	}

	row := RowFromEvent(ev)

	if row.ReqID != "req-123" {
		t.Errorf("ReqID: got %q, want %q", row.ReqID, "req-123")
	}
	if row.Org != "acme" {
		t.Errorf("Org: got %q, want %q", row.Org, "acme")
	}
	if row.Project != "project-1" {
		t.Errorf("Project: got %q, want %q", row.Project, "project-1")
	}
	if row.KeyID != "key-456" {
		t.Errorf("KeyID: got %q, want %q", row.KeyID, "key-456")
	}
	if row.Alias != "alias-prod" {
		t.Errorf("Alias: got %q, want %q", row.Alias, "alias-prod")
	}
	if row.Model != "claude-opus-4" {
		t.Errorf("Model: got %q, want %q", row.Model, "claude-opus-4")
	}
	if row.Provider != "anthropic" {
		t.Errorf("Provider: got %q, want %q", row.Provider, "anthropic")
	}
	if row.Kind != "chat" {
		t.Errorf("Kind: got %q, want %q", row.Kind, "chat")
	}
	if row.Status != "ok" {
		t.Errorf("Status: got %q, want %q", row.Status, "ok")
	}
	if row.ErrorCode != "" {
		t.Errorf("ErrorCode: got %q, want %q", row.ErrorCode, "")
	}
	if row.WorkerID != "worker-789" {
		t.Errorf("WorkerID: got %q, want %q", row.WorkerID, "worker-789")
	}
	if row.PromptTokens != 100 {
		t.Errorf("PromptTokens: got %d, want %d", row.PromptTokens, 100)
	}
	if row.CompletionTokens != 50 {
		t.Errorf("CompletionTokens: got %d, want %d", row.CompletionTokens, 50)
	}
	if row.CachedTokens != 10 {
		t.Errorf("CachedTokens: got %d, want %d", row.CachedTokens, 10)
	}
	if row.TTFTMillis != 150 {
		t.Errorf("TTFTMillis: got %d, want %d", row.TTFTMillis, 150)
	}
	if row.DurationMillis != 500 {
		t.Errorf("DurationMillis: got %d, want %d", row.DurationMillis, 500)
	}
	if row.QueueMillis != 50 {
		t.Errorf("QueueMillis: got %d, want %d", row.QueueMillis, 50)
	}
	if row.Estimated != true {
		t.Errorf("Estimated: got %v, want %v", row.Estimated, true)
	}
	if row.TS != now {
		t.Errorf("TS: got %v, want %v", row.TS, now)
	}
}

// TestRowTotalTokens verifies TotalTokens returns PromptTokens + CompletionTokens.
func TestRowTotalTokens(t *testing.T) {
	row := Row{
		PromptTokens:     100,
		CompletionTokens: 50,
		CachedTokens:     10,
	}

	total := row.TotalTokens()
	expected := int64(150)
	if total != expected {
		t.Errorf("TotalTokens: got %d, want %d", total, expected)
	}
}

// TestFakeSinkInsertBatch verifies FakeSink stores rows.
func TestFakeSinkInsertBatch(t *testing.T) {
	sink := NewFakeSink()
	ctx := context.Background()

	rows := []Row{
		{ReqID: "req-1", Org: "org-1", PromptTokens: 100, CompletionTokens: 50},
		{ReqID: "req-2", Org: "org-1", PromptTokens: 200, CompletionTokens: 100},
	}

	err := sink.InsertBatch(ctx, rows)
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	snapshot := sink.RowsSnapshot()
	if len(snapshot) != 2 {
		t.Errorf("stored rows: got %d, want %d", len(snapshot), 2)
	}
}

// TestFakeSinkDedup verifies FakeSink deduplicates by ReqID within a single batch.
func TestFakeSinkDedup(t *testing.T) {
	sink := NewFakeSink()
	ctx := context.Background()

	rows := []Row{
		{ReqID: "req-1", Org: "org-1", PromptTokens: 100, CompletionTokens: 50},
		{ReqID: "req-1", Org: "org-1", PromptTokens: 200, CompletionTokens: 100}, // duplicate ReqID
	}

	err := sink.InsertBatch(ctx, rows)
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	snapshot := sink.RowsSnapshot()
	if len(snapshot) != 1 {
		t.Errorf("stored rows after dedup: got %d, want %d", len(snapshot), 1)
	}

	// The first one should win
	if snapshot[0].PromptTokens != 100 {
		t.Errorf("dedup should keep first: got %d, want %d", snapshot[0].PromptTokens, 100)
	}
}

// TestFakeSinkCrossBatchDedup verifies FakeSink deduplicates across multiple batches.
func TestFakeSinkCrossBatchDedup(t *testing.T) {
	sink := NewFakeSink()
	ctx := context.Background()

	// First batch with req-1
	err := sink.InsertBatch(ctx, []Row{
		{ReqID: "req-1", Org: "org-1", PromptTokens: 100, CompletionTokens: 50},
	})
	if err != nil {
		t.Fatalf("first InsertBatch: %v", err)
	}

	// Second batch with same req-1 (should be deduplicated)
	err = sink.InsertBatch(ctx, []Row{
		{ReqID: "req-1", Org: "org-1", PromptTokens: 200, CompletionTokens: 100},
	})
	if err != nil {
		t.Fatalf("second InsertBatch: %v", err)
	}

	snapshot := sink.RowsSnapshot()
	if len(snapshot) != 1 {
		t.Errorf("stored rows after cross-batch dedup: got %d, want %d", len(snapshot), 1)
	}

	// The first one should still win
	if snapshot[0].PromptTokens != 100 {
		t.Errorf("cross-batch dedup should keep first: got %d, want %d", snapshot[0].PromptTokens, 100)
	}
}

// TestFakeSinkMonthToDate verifies MonthToDate sums TotalTokens for matching keyID+month.
func TestFakeSinkMonthToDate(t *testing.T) {
	sink := NewFakeSink()
	ctx := context.Background()

	augTime := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	sepTime := time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)

	rows := []Row{
		{
			ReqID:            "req-1",
			KeyID:            "key-1",
			TS:               augTime,
			PromptTokens:     100,
			CompletionTokens: 50,
		},
		{
			ReqID:            "req-2",
			KeyID:            "key-1",
			TS:               augTime,
			PromptTokens:     50,
			CompletionTokens: 25,
		},
		{
			ReqID:            "req-3",
			KeyID:            "key-1",
			TS:               sepTime,
			PromptTokens:     200,
			CompletionTokens: 100,
		},
		{
			ReqID:            "req-4",
			KeyID:            "key-2",
			TS:               augTime,
			PromptTokens:     1000,
			CompletionTokens: 500,
		},
	}

	err := sink.InsertBatch(ctx, rows)
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	// August 2026 month string is "2026-08"
	augTotal, err := sink.MonthToDate(ctx, "key-1", "2026-08")
	if err != nil {
		t.Fatalf("MonthToDate: %v", err)
	}
	// 100+50 + 50+25 = 225
	if augTotal != 225 {
		t.Errorf("August total for key-1: got %d, want %d", augTotal, 225)
	}

	// September 2026 month string is "2026-09"
	sepTotal, err := sink.MonthToDate(ctx, "key-1", "2026-09")
	if err != nil {
		t.Fatalf("MonthToDate: %v", err)
	}
	// 200+100 = 300
	if sepTotal != 300 {
		t.Errorf("September total for key-1: got %d, want %d", sepTotal, 300)
	}

	// August for key-2
	augTotal2, err := sink.MonthToDate(ctx, "key-2", "2026-08")
	if err != nil {
		t.Fatalf("MonthToDate: %v", err)
	}
	// 1000+500 = 1500
	if augTotal2 != 1500 {
		t.Errorf("August total for key-2: got %d, want %d", augTotal2, 1500)
	}

	// Non-existent key
	nonTotal, err := sink.MonthToDate(ctx, "key-3", "2026-08")
	if err != nil {
		t.Fatalf("MonthToDate: %v", err)
	}
	if nonTotal != 0 {
		t.Errorf("Non-existent key total: got %d, want %d", nonTotal, 0)
	}
}

// TestFakeSinkFailNext verifies FailNext field returns error once then succeeds.
func TestFakeSinkFailNext(t *testing.T) {
	sink := NewFakeSink()
	ctx := context.Background()
	expectedErr := errors.New("injected error")
	sink.FailNext = expectedErr

	rows := []Row{
		{ReqID: "req-1", Org: "org-1"},
	}

	// First call should return the error
	err := sink.InsertBatch(ctx, rows)
	if err != expectedErr {
		t.Errorf("first call: got %v, want %v", err, expectedErr)
	}

	// Row should not be inserted
	snapshot := sink.RowsSnapshot()
	if len(snapshot) != 0 {
		t.Errorf("row should not be inserted on failure: got %d, want %d", len(snapshot), 0)
	}

	// Second call should succeed
	err = sink.InsertBatch(ctx, rows)
	if err != nil {
		t.Errorf("second call: got %v, want %v", err, nil)
	}

	// Row should be inserted now
	snapshot = sink.RowsSnapshot()
	if len(snapshot) != 1 {
		t.Errorf("row should be inserted on success: got %d, want %d", len(snapshot), 1)
	}
}

// TestFakeSinkClose verifies Close() returns nil without error.
func TestFakeSinkClose(t *testing.T) {
	sink := NewFakeSink()
	err := sink.Close()
	if err != nil {
		t.Errorf("Close: got %v, want nil", err)
	}
}

// TestFakeSinkConcurrency verifies FakeSink is thread-safe under concurrent
// InsertBatch and MonthToDate calls. This test runs with -race.
func TestFakeSinkConcurrency(t *testing.T) {
	sink := NewFakeSink()
	ctx := context.Background()
	done := make(chan bool, 5)

	baseTime := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	// 4 goroutines inserting batches concurrently
	for i := 0; i < 4; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				rows := []Row{
					{
						ReqID:            "req-" + string(rune(id*10+j)),
						KeyID:            "key-1",
						TS:               baseTime.Add(time.Duration(j) * time.Hour),
						PromptTokens:     int64(100 * (id + 1)),
						CompletionTokens: int64(50 * (id + 1)),
					},
				}
				_ = sink.InsertBatch(ctx, rows)
			}
			done <- true
		}(i)
	}

	// 1 goroutine polling MonthToDate concurrently
	go func() {
		for i := 0; i < 20; i++ {
			_, _ = sink.MonthToDate(ctx, "key-1", "2026-09")
			time.Sleep(1 * time.Millisecond)
		}
		done <- true
	}()

	// Wait for all goroutines to finish
	for i := 0; i < 5; i++ {
		<-done
	}

	// Verify final state: should have 40 rows (4 goroutines * 10 rows each)
	snapshot := sink.RowsSnapshot()
	if len(snapshot) != 40 {
		t.Errorf("final row count: got %d, want %d", len(snapshot), 40)
	}

	// Verify MonthToDate still works correctly
	total, err := sink.MonthToDate(ctx, "key-1", "2026-09")
	if err != nil {
		t.Fatalf("MonthToDate: %v", err)
	}
	// Each of 4 goroutines contributes: 100*5 + 100*6 + 100*7 + 100*8 tokens per batch
	// (50*5 + 50*6 + 50*7 + 50*8) + (100*5 + 100*6 + 100*7 + 100*8) total
	// = 10*(50*5 + 50*6 + 50*7 + 50*8 + 100*5 + 100*6 + 100*7 + 100*8)/10
	// Total should be > 0
	if total <= 0 {
		t.Errorf("MonthToDate total: got %d, want > 0", total)
	}
}
