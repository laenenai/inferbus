// Package harvester holds the Sink contract for persisting usage rows to a
// backing store (the budget-ledger database). It defines the Row type (mapped
// from wire.UsageEvent), the Sink interface (InsertBatch, MonthToDate, Close),
// and the FakeSink in-memory implementation for testing.
package harvester

import (
	"context"
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
// inject failures on demand for retry testing.
type FakeSink struct {
	Rows    []Row
	FailNext error
}

// NewFakeSink creates a new FakeSink.
func NewFakeSink() *FakeSink {
	return &FakeSink{
		Rows: []Row{},
	}
}

// InsertBatch inserts rows into the FakeSink, deduplicating by ReqID. If
// FailNext is set, it returns that error once and clears it; otherwise, the
// first occurrence of each ReqID is kept.
func (fs *FakeSink) InsertBatch(ctx context.Context, rows []Row) error {
	if fs.FailNext != nil {
		err := fs.FailNext
		fs.FailNext = nil
		return err
	}

	// Deduplicate by ReqID: build a map of ReqIDs already seen.
	seen := make(map[string]bool)
	for _, existing := range fs.Rows {
		seen[existing.ReqID] = true
	}

	for _, row := range rows {
		if !seen[row.ReqID] {
			fs.Rows = append(fs.Rows, row)
			seen[row.ReqID] = true
		}
	}

	return nil
}

// MonthToDate returns the sum of TotalTokens for all rows matching keyID and
// month (in "2006-01" format). Month boundaries are respected: a row in August
// and a row in September are counted separately even if they share the same
// keyID.
func (fs *FakeSink) MonthToDate(ctx context.Context, keyID, month string) (int64, error) {
	var total int64
	for _, row := range fs.Rows {
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
