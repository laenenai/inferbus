package harvester

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// CHSink is a Sink backed by ClickHouse. It owns two tables it creates
// idempotently on construction: usage_events (the ledger of record, a
// ReplacingMergeTree deduplicated on (org, project, key_id, ts, req_id)) and
// usage_hourly (a SummingMergeTree rollup fed by a materialized view), per
// the DDL in docs/design-usage.md §2. See ch_api_notes.md for the
// clickhouse-go/v2 API this file relies on — that file is the authority for
// field/method names, not this comment.
type CHSink struct {
	conn driver.Conn
}

// NewCHSink connects to ClickHouse at dsn (e.g.
// "clickhouse://host:9000/dbname") and applies the usage_events /
// usage_hourly / usage_hourly_mv DDL idempotently (CREATE ... IF NOT
// EXISTS). It does not create the database named in the DSN's path — that
// database must already exist (see ch_api_notes.md's "Divergences" note:
// this matches the brief's Produces line, which only asks for table/MV
// DDL).
func NewCHSink(ctx context.Context, dsn string) (*CHSink, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("harvester: parse clickhouse dsn: %w", err)
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("harvester: open clickhouse: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("harvester: ping clickhouse: %w", err)
	}

	sink := &CHSink{conn: conn}
	if err := sink.applyDDL(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return sink, nil
}

// applyDDL creates usage_events, usage_hourly, and the materialized view
// that feeds the latter from the former, all idempotently. Statement order
// matters: usage_hourly must exist before the materialized view that
// targets it via TO.
func (s *CHSink) applyDDL(ctx context.Context) error {
	stmts := []string{ddlUsageEvents, ddlUsageHourly, ddlUsageHourlyMV}
	for _, stmt := range stmts {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("harvester: apply ddl: %w", err)
		}
	}
	return nil
}

// ddlUsageEvents is the ledger of record. ReplacingMergeTree deduplicates
// rows whose full ORDER BY tuple matches exactly, which is what makes
// at-least-once redelivery of the same METERING message (same event
// payload, hence identical ts) collapse to one row. Enum-ish columns
// (alias/model/provider/kind/status/error_code) use LowCardinality(String)
// per docs/design-usage.md §2 ("enums as LowCardinality(String)"); org,
// project, key_id, req_id, worker_id stay plain String since they are
// effectively unbounded identifiers, not enums.
const ddlUsageEvents = `
CREATE TABLE IF NOT EXISTS usage_events (
	req_id            String,
	ts                DateTime64(3, 'UTC'),
	org               String,
	project           String,
	key_id            String,
	alias             LowCardinality(String),
	model             LowCardinality(String),
	provider          LowCardinality(String),
	kind              LowCardinality(String),
	status            LowCardinality(String),
	error_code        LowCardinality(String),
	worker_id         String,
	prompt_tokens     Int64,
	completion_tokens Int64,
	cached_tokens     Int64,
	ttft_millis       Int64,
	duration_millis   Int64,
	queue_millis      Int64,
	estimated         Bool
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (org, project, key_id, ts, req_id)
TTL toDateTime(ts) + INTERVAL 13 MONTH
`

// ddlUsageHourly is the SummingMergeTree target of usage_hourly_mv. Its
// ORDER BY is the group-by key of the view: rows sharing that key sum their
// measure columns (prompt/completion/cached tokens, requests, errors) on
// merge.
const ddlUsageHourly = `
CREATE TABLE IF NOT EXISTS usage_hourly (
	org               String,
	project           String,
	key_id            String,
	alias             LowCardinality(String),
	model             LowCardinality(String),
	provider          LowCardinality(String),
	status            LowCardinality(String),
	hour              DateTime,
	prompt_tokens     Int64,
	completion_tokens Int64,
	cached_tokens     Int64,
	requests          Int64,
	errors            Int64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (org, project, key_id, alias, model, provider, status, hour)
TTL hour + INTERVAL 13 MONTH
`

// ddlUsageHourlyMV feeds usage_hourly from usage_events. "errors" counts
// rows whose wire status is exactly "error" (see internal/wire.UsageEvent's
// Status doc: "ok|error|canceled") so downstream queries can sum(errors)
// without needing to know the status vocabulary.
const ddlUsageHourlyMV = `
CREATE MATERIALIZED VIEW IF NOT EXISTS usage_hourly_mv TO usage_hourly AS
SELECT
	org,
	project,
	key_id,
	alias,
	model,
	provider,
	status,
	toStartOfHour(ts) AS hour,
	sum(prompt_tokens) AS prompt_tokens,
	sum(completion_tokens) AS completion_tokens,
	sum(cached_tokens) AS cached_tokens,
	count() AS requests,
	sumIf(1, status = 'error') AS errors
FROM usage_events
GROUP BY org, project, key_id, alias, model, provider, status, hour
`

// insertUsageEventsSQL's column list order must match the positional
// arguments passed to Batch.Append in InsertBatch.
const insertUsageEventsSQL = `INSERT INTO usage_events (
	req_id, ts, org, project, key_id, alias, model, provider, kind, status,
	error_code, worker_id, prompt_tokens, completion_tokens, cached_tokens,
	ttft_millis, duration_millis, queue_millis, estimated
)`

// InsertBatch writes rows to usage_events via a single prepared Batch, per
// ch_api_notes.md's driver.Batch section (PrepareBatch, Append per row,
// defer Close, then Send). It does not deduplicate by ReqID itself —
// ReplacingMergeTree handles cross-batch replays of the identical event,
// and dedup of literal duplicates within one caller-supplied batch is the
// batching layer's job (docs/design-usage.md §2), not the sink's.
func (s *CHSink) InsertBatch(ctx context.Context, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, insertUsageEventsSQL)
	if err != nil {
		return fmt.Errorf("harvester: prepare batch: %w", err)
	}
	// Safe to defer immediately after PrepareBatch per ch_api_notes.md:
	// Close after a successful Send is a documented no-op on the sent
	// batch, not a double-send.
	defer batch.Close()

	for _, row := range rows {
		if err := batch.Append(
			row.ReqID,
			row.TS,
			row.Org,
			row.Project,
			row.KeyID,
			row.Alias,
			row.Model,
			row.Provider,
			row.Kind,
			row.Status,
			row.ErrorCode,
			row.WorkerID,
			row.PromptTokens,
			row.CompletionTokens,
			row.CachedTokens,
			row.TTFTMillis,
			row.DurationMillis,
			row.QueueMillis,
			row.Estimated,
		); err != nil {
			return fmt.Errorf("harvester: append row %s: %w", row.ReqID, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("harvester: send batch: %w", err)
	}
	return nil
}

// monthToDateSQL intentionally omits FINAL: per docs/design-usage.md §2,
// month-to-date budget reads tolerate the rare pre-merge duplicate because
// budgets are advisory throttles, not invoices. sum() over zero matching
// rows returns SQL NULL, hence scanning into sql.NullInt64 below.
const monthToDateSQL = `SELECT sum(prompt_tokens + completion_tokens) FROM usage_events WHERE key_id = ? AND toYYYYMM(ts) = ?`

// MonthToDate returns the sum of prompt+completion tokens for keyID in the
// given month ("2006-01" format), matching the Sink interface contract.
func (s *CHSink) MonthToDate(ctx context.Context, keyID, month string) (int64, error) {
	t, err := time.Parse("2006-01", month)
	if err != nil {
		return 0, fmt.Errorf("harvester: parse month %q: %w", month, err)
	}
	yyyymm, err := strconv.Atoi(t.Format("200601"))
	if err != nil {
		return 0, fmt.Errorf("harvester: format month %q: %w", month, err)
	}

	var total sql.NullInt64
	if err := s.conn.QueryRow(ctx, monthToDateSQL, keyID, yyyymm).Scan(&total); err != nil {
		return 0, fmt.Errorf("harvester: month to date: %w", err)
	}
	return total.Int64, nil
}

// Close closes the underlying ClickHouse connection.
func (s *CHSink) Close() error {
	return s.conn.Close()
}
