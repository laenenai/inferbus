package harvester

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TestCHSink_InsertMonthToDateAndHourlyMV is the env-gated ClickHouse
// integration test for CHSink, per the M4 Task 3 brief. It exercises:
//   - NewCHSink's DDL (usage_events, usage_hourly, usage_hourly_mv) applied
//     idempotently against a throwaway per-run database;
//   - InsertBatch across two calls, one of which redelivers a byte-identical
//     row (same org/project/key_id/ts/req_id — the only way ReplacingMergeTree
//     considers two inserts "the same row", matching how at-least-once
//     redelivery of one METERING message looks in practice);
//   - SELECT count() ... FINAL == 3 distinct rows despite 4 inserted;
//   - MonthToDate's per-key sums, after forcing a real merge (OPTIMIZE ...
//     FINAL) so the FINAL-free MonthToDate query — which docs/design-usage.md
//     §2 explicitly allows to tolerate the "rare pre-merge duplicate" because
//     budget reads are advisory, not invoices — is deterministic in this test;
//   - usage_hourly_mv populated usage_hourly with at least one row.
//
// Skips cleanly when CP_TEST_CH_DSN is unset. CP_TEST_CH_DSN must point at
// an existing/default database the test process is allowed to create and
// drop sibling databases from (e.g. "clickhouse://localhost:19000/default");
// this test never starts a ClickHouse container itself.
func TestCHSink_InsertMonthToDateAndHourlyMV(t *testing.T) {
	baseDSN := os.Getenv("CP_TEST_CH_DSN")
	if baseDSN == "" {
		t.Skip("CP_TEST_CH_DSN not set; skipping ClickHouse sink integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin := mustCHConn(t, baseDSN)
	t.Cleanup(func() { _ = admin.Close() })

	dbName := fmt.Sprintf("harvester_test_%d", time.Now().UnixNano())
	if err := admin.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+dbName); err != nil {
		t.Fatalf("create database %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		if err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
	})

	testDSN, err := withDatabase(baseDSN, dbName)
	if err != nil {
		t.Fatalf("withDatabase: %v", err)
	}

	sink, err := NewCHSink(ctx, testDSN)
	if err != nil {
		t.Fatalf("NewCHSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })

	ts1 := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 9, 6, 9, 30, 0, 0, time.UTC)
	ts3 := time.Date(2026, 9, 7, 18, 15, 0, 0, time.UTC)

	r1 := Row{ // key-1
		ReqID: "req-1", TS: ts1, Org: "acme", Project: "proj-a", KeyID: "key-1",
		Alias: "prod", Model: "gpt-5", Provider: "openai", Kind: "chat",
		Status: "ok", ErrorCode: "", WorkerID: "worker-1",
		PromptTokens: 100, CompletionTokens: 50, CachedTokens: 0,
		TTFTMillis: 200, DurationMillis: 800, QueueMillis: 5, Estimated: false,
	}
	r2 := Row{ // key-1
		ReqID: "req-2", TS: ts2, Org: "acme", Project: "proj-a", KeyID: "key-1",
		Alias: "prod", Model: "gpt-5", Provider: "openai", Kind: "chat",
		Status: "ok", ErrorCode: "", WorkerID: "worker-1",
		PromptTokens: 40, CompletionTokens: 20, CachedTokens: 0,
		TTFTMillis: 150, DurationMillis: 400, QueueMillis: 2, Estimated: false,
	}
	r3 := Row{ // key-2
		ReqID: "req-3", TS: ts3, Org: "acme", Project: "proj-a", KeyID: "key-2",
		Alias: "staging", Model: "gpt-5-mini", Provider: "openai", Kind: "chat",
		Status: "error", ErrorCode: "UPSTREAM_TIMEOUT", WorkerID: "worker-2",
		PromptTokens: 10, CompletionTokens: 0, CachedTokens: 0,
		TTFTMillis: 0, DurationMillis: 30000, QueueMillis: 1, Estimated: true,
	}

	if err := sink.InsertBatch(ctx, []Row{r1, r2, r3}); err != nil {
		t.Fatalf("InsertBatch (batch 1): %v", err)
	}
	// Redeliver r1 byte-for-byte, simulating at-least-once METERING
	// redelivery, in a second, separate batch.
	if err := sink.InsertBatch(ctx, []Row{r1}); err != nil {
		t.Fatalf("InsertBatch (batch 2, duplicate): %v", err)
	}

	// Force a real merge so usage_events physically drops the duplicate
	// before we exercise MonthToDate's intentionally FINAL-free query.
	if err := admin.Exec(ctx, "OPTIMIZE TABLE "+dbName+".usage_events FINAL"); err != nil {
		t.Fatalf("OPTIMIZE usage_events: %v", err)
	}

	var count uint64
	if err := admin.QueryRow(ctx, "SELECT count() FROM "+dbName+".usage_events FINAL").Scan(&count); err != nil {
		t.Fatalf("count usage_events FINAL: %v", err)
	}
	if count != 3 {
		t.Fatalf("count() FROM usage_events FINAL = %d, want 3", count)
	}

	gotKey1, err := sink.MonthToDate(ctx, "key-1", "2026-09")
	if err != nil {
		t.Fatalf("MonthToDate key-1: %v", err)
	}
	if want := r1.TotalTokens() + r2.TotalTokens(); gotKey1 != want {
		t.Fatalf("MonthToDate(key-1, 2026-09) = %d, want %d", gotKey1, want)
	}

	gotKey2, err := sink.MonthToDate(ctx, "key-2", "2026-09")
	if err != nil {
		t.Fatalf("MonthToDate key-2: %v", err)
	}
	if want := r3.TotalTokens(); gotKey2 != want {
		t.Fatalf("MonthToDate(key-2, 2026-09) = %d, want %d", gotKey2, want)
	}

	gotOtherMonth, err := sink.MonthToDate(ctx, "key-1", "2026-08")
	if err != nil {
		t.Fatalf("MonthToDate key-1 (other month): %v", err)
	}
	if gotOtherMonth != 0 {
		t.Fatalf("MonthToDate(key-1, 2026-08) = %d, want 0", gotOtherMonth)
	}

	var hourlyRows uint64
	if err := admin.QueryRow(ctx, "SELECT count() FROM "+dbName+".usage_hourly").Scan(&hourlyRows); err != nil {
		t.Fatalf("count usage_hourly: %v", err)
	}
	if hourlyRows == 0 {
		t.Fatal("usage_hourly has no rows; expected the materialized view to have populated it")
	}
}

// mustCHConn opens a raw driver.Conn for admin/verification queries the
// Sink interface doesn't expose (CREATE/DROP DATABASE, OPTIMIZE, FINAL
// counts). Per ch_api_notes.md's Conn section.
func mustCHConn(t *testing.T, dsn string) driver.Conn {
	t.Helper()
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return conn
}

// withDatabase rewrites dsn's path to name, so the same host/port/auth
// point at a different (throwaway) database.
func withDatabase(dsn, name string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	u.Path = "/" + name
	return u.String(), nil
}

// TestNewCHSink_MalformedDSNDoesNotLeakPassword is the regression test for
// final-review I8: clickhouse.ParseDSN fails with a *net/url.Error whose
// Error() reproduces the entire URL, userinfo included (net/url does not
// redact passwords in error strings), and cmd/inferbus prints this error
// straight to stdout — i.e. container logs and every aggregator downstream
// of them. A fat-fingered clickhouse_dsn must never put the ClickHouse
// password in a log.
func TestNewCHSink_MalformedDSNDoesNotLeakPassword(t *testing.T) {
	const password = "sup3rs3cret"
	// A raw DEL control character makes ParseDSN's URL parse fail.
	dsn := "clickhouse://admin:" + password + "@host\x7f:9000/inferbus"

	// Sanity: the driver's own error really does contain the password, so
	// this test would fail if the wrapping ever regressed to %w.
	if _, err := clickhouse.ParseDSN(dsn); err == nil || !strings.Contains(err.Error(), password) {
		t.Fatalf("fixture no longer reproduces the leak (ParseDSN err = %v)", err)
	}

	_, err := NewCHSink(context.Background(), dsn)
	if err == nil {
		t.Fatal("NewCHSink with a malformed DSN: want error, got nil")
	}
	if strings.Contains(err.Error(), password) {
		t.Fatalf("error leaks the clickhouse password: %v", err)
	}
	if strings.Contains(err.Error(), "admin:") || strings.Contains(err.Error(), "clickhouse://") {
		t.Fatalf("error echoes the DSN: %v", err)
	}
}
