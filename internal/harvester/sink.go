// Package harvester implements the M4 usage pipeline's harvester process:
// a durable JetStream pull consumer (harvester.go) that decodes METERING
// events, batches them, and flushes each batch through the Sink contract
// (this file) to a backing store (the budget-ledger database). This file
// defines the Row type (mapped from wire.UsageEvent), the Sink interface
// (InsertBatch, MonthToDate, Close), and the FakeSink in-memory
// implementation used by tests; chsink.go provides the production
// ClickHouse-backed implementation.
package harvester

import (
	"context"
	"sync"
	"time"

	"github.com/laenenai/inferbus/internal/wire"
)

// Row is one accounting record persisted to the usage ledger. It is mapped
// from wire.UsageEvent with all fields extracted, int token counts widened
// to int64, and timing fields preserved as int64 milliseconds.
type Row struct {
	ReqID            string
	TS               time.Time
	Org              string
	Project          string
	KeyID            string
	Alias            string
	Model            string
	Provider         string
	Kind             string
	Status           string
	ErrorCode        string
	WorkerID         string
	PromptTokens     int64
	CompletionTokens int64
	CachedTokens     int64
	TTFTMillis       int64
	DurationMillis   int64
	QueueMillis      int64
	Estimated        bool
}

// TotalTokens returns the sum of PromptTokens and CompletionTokens (excluding cached).
func (r Row) TotalTokens() int64 {
	return r.PromptTokens + r.CompletionTokens
}

// RowFromEvent maps every field from wire.UsageEvent to Row, widening int
// token counts to int64.
func RowFromEvent(ev wire.UsageEvent) Row {
	return Row{
		ReqID:            ev.ReqID,
		TS:               ev.TS,
		Org:              ev.Org,
		Project:          ev.Project,
		KeyID:            ev.KeyID,
		Alias:            ev.Alias,
		Model:            ev.Model,
		Provider:         ev.Provider,
		Kind:             ev.Kind,
		Status:           ev.Status,
		ErrorCode:        ev.ErrorCode,
		WorkerID:         ev.WorkerID,
		PromptTokens:     int64(ev.Usage.PromptTokens),
		CompletionTokens: int64(ev.Usage.CompletionTokens),
		CachedTokens:     int64(ev.Usage.CachedTokens),
		TTFTMillis:       ev.TTFTMillis,
		DurationMillis:   ev.DurationMillis,
		QueueMillis:      ev.QueueMillis,
		Estimated:        ev.Estimated,
	}
}

// Sink defines the contract for persisting usage rows to a backing store.
// Implementations must be safe for concurrent use: the harvester calls
// InsertBatch from at most one flush goroutine at a time, but a caller
// outside the harvester (e.g. the Task 5 budget ledger reading
// MonthToDate) may call other methods concurrently with an in-progress
// InsertBatch or with each other.
type Sink interface {
	// InsertBatch inserts a batch of rows into the sink.
	InsertBatch(ctx context.Context, rows []Row) error

	// MonthToDate returns the total tokens (PromptTokens + CompletionTokens)
	// for a given keyID and month (in "2006-01" format). It sums across all
	// rows matching both keyID and month.
	MonthToDate(ctx context.Context, keyID, month string) (int64, error)

	// Close closes the sink and cleans up any resources.
	Close() error
}

// FakeSink is an in-memory implementation of Sink for testing. It deduplicates
// rows by ReqID (first one wins) and tracks an optional FailNext error to
// inject failures on demand for retry testing. FakeSink is thread-safe.
type FakeSink struct {
	mu   sync.Mutex
	rows []Row

	FailNext error

	// monthToDateErr, if non-nil, makes every MonthToDate call return this
	// error (0, err) instead of computing a sum, until a test clears it back
	// to nil via SetMonthToDateErr. Unlike FailNext (which fires once, for
	// InsertBatch), this persists across calls — the budget ledger's Task 5
	// fix round (I3) needs a sink that stays down across an entire failed
	// rollover attempt (both checkMonthRollover's initial loadBaseline call
	// and flushTick's same-tick pending-baseline retry), not just the very
	// first call. It is behind SetMonthToDateErr/mu (not a bare exported
	// field like FailNext) because, unlike FailNext, it is read
	// concurrently by the budget ledger's own goroutine while a test sets
	// it from the test goroutine.
	monthToDateErr error

	// Delay, if set, makes InsertBatch sleep this long before committing
	// rows — with the lock released during the sleep, so RowsSnapshot and
	// concurrent InsertBatch callers are never blocked by it. Used to
	// simulate a slow sink in tests exercising the harvester's in-flight
	// heartbeat path.
	Delay time.Duration
}

// SetMonthToDateErr sets (or, passed nil, clears) the error every MonthToDate
// call returns. Safe to call concurrently with MonthToDate itself.
func (fs *FakeSink) SetMonthToDateErr(err error) {
	fs.mu.Lock()
	fs.monthToDateErr = err
	fs.mu.Unlock()
}

// NewFakeSink creates a new FakeSink.
func NewFakeSink() *FakeSink {
	return &FakeSink{
		rows: []Row{},
	}
}

// InsertBatch inserts rows into the FakeSink, deduplicating by ReqID. If
// FailNext is set, it returns that error once and clears it; otherwise, the
// first occurrence of each ReqID is kept. If Delay is set, InsertBatch
// sleeps that long — with the lock released — before committing rows, to
// simulate a slow sink.
func (fs *FakeSink) InsertBatch(ctx context.Context, rows []Row) error {
	fs.mu.Lock()
	if fs.FailNext != nil {
		err := fs.FailNext
		fs.FailNext = nil
		fs.mu.Unlock()
		return err
	}
	delay := fs.Delay
	fs.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Deduplicate by ReqID: build a map of ReqIDs already seen.
	seen := make(map[string]bool)
	for _, existing := range fs.rows {
		seen[existing.ReqID] = true
	}

	for _, row := range rows {
		if !seen[row.ReqID] {
			fs.rows = append(fs.rows, row)
			seen[row.ReqID] = true
		}
	}

	return nil
}

// RowsSnapshot returns a copy of all rows currently stored in the sink. It is
// safe to call concurrently and should be used for test assertions after
// concurrent operations.
func (fs *FakeSink) RowsSnapshot() []Row {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	snapshot := make([]Row, len(fs.rows))
	copy(snapshot, fs.rows)
	return snapshot
}

// MonthToDate returns the sum of TotalTokens for all rows matching keyID and
// month (in "2006-01" format). Month boundaries are respected: a row in August
// and a row in September are counted separately even if they share the same
// keyID.
func (fs *FakeSink) MonthToDate(ctx context.Context, keyID, month string) (int64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.monthToDateErr != nil {
		return 0, fs.monthToDateErr
	}

	var total int64
	for _, row := range fs.rows {
		if row.KeyID == keyID {
			// Check if the row's timestamp matches the month in "2006-01" format.
			rowMonth := row.TS.Format("2006-01")
			if rowMonth == month {
				total += row.TotalTokens()
			}
		}
	}
	return total, nil
}

// Close closes the FakeSink (no-op for in-memory implementation).
func (fs *FakeSink) Close() error {
	return nil
}
