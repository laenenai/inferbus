// Tests for the admin usage endpoint (Task 8): GET /admin/v1/usage.
//
// Unlike admin_test.go's adminFixture (a full live sqlite+relay+SQL
// projector stack), these tests build the lightest Admin that can exercise
// the usage handler: a MemReadStore seeded directly via UpsertOrg/
// UpsertMember (no aggregate runtimes are touched by the usage handler, so
// orgRT/keyRT/aliasRT are nil), plus a fakeUsageReader test double for
// UsageReader.
package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/laenenai/inferbus/internal/harvester"
)

// fakeUsageReader is a hermetic UsageReader test double.
type fakeUsageReader struct {
	buckets []UsageBucket
	err     error
	// gotOrg/gotWindow record the last call's arguments for assertions.
	gotOrg    string
	gotWindow string
}

func (f *fakeUsageReader) OrgUsage(_ context.Context, org, window string) ([]UsageBucket, error) {
	f.gotOrg = org
	f.gotWindow = window
	if f.err != nil {
		return nil, f.err
	}
	return f.buckets, nil
}

// newUsageTestAdmin builds a minimal Admin wired with a MemReadStore
// (seeded with orgID and, if memberSub != "", a membership row for it at
// memberRole) and the given UsageReader (nil is a valid, meaningful input:
// it exercises the "not configured" 501 path).
func newUsageTestAdmin(t *testing.T, cfg Config, verifier TokenVerifier, orgID, memberSub, memberRole string, usage UsageReader) *Admin {
	t.Helper()
	rs := NewMemReadStore()
	if err := rs.UpsertOrg(context.Background(), OrgRow{ID: orgID, Name: orgID}); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	if memberSub != "" {
		if err := rs.UpsertMember(context.Background(), orgID, memberSub, memberRole); err != nil {
			t.Fatalf("seed member: %v", err)
		}
	}
	return NewAdmin(NewAuthenticator(cfg, verifier), rs, nil, nil, nil, func(context.Context) error { return nil }, nil, usage)
}

func TestAdmin_Usage_MemberGetsBuckets(t *testing.T) {
	cfg := Config{}
	verifier := &fakeVerifier{subs: map[string]string{"jwt-viewer": "viewer-sub"}}
	want := []UsageBucket{
		{Model: "gpt-5", Alias: "prod", Provider: "openai", Status: "ok", Requests: 10, PromptTokens: 1000, CompletionTokens: 500},
	}
	reader := &fakeUsageReader{buckets: want}
	admin := newUsageTestAdmin(t, cfg, verifier, "acme", "viewer-sub", "viewer", reader)
	mux := admin.Routes()

	rec := doRequest(t, mux, http.MethodGet, "/admin/v1/usage?org=acme&window=7d", "jwt-viewer", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if reader.gotOrg != "acme" || reader.gotWindow != "7d" {
		t.Fatalf("OrgUsage called with (%q, %q), want (\"acme\", \"7d\")", reader.gotOrg, reader.gotWindow)
	}

	var body usageResponse
	if err := decodeJSONBody(t, rec, &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Org != "acme" || body.Window != "7d" {
		t.Fatalf("response org/window = %q/%q, want acme/7d", body.Org, body.Window)
	}
	if len(body.Buckets) != 1 || body.Buckets[0] != want[0] {
		t.Fatalf("buckets = %+v, want %+v", body.Buckets, want)
	}
}

func TestAdmin_Usage_PlatformAdminAnyOrg(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	reader := &fakeUsageReader{buckets: []UsageBucket{{Model: "m", Status: "ok"}}}
	// No membership seeded at all for "acme" — platform admin must still
	// be authorized (binding ruling: platform admin short-circuits).
	admin := newUsageTestAdmin(t, cfg, &fakeVerifier{}, "acme", "", "", reader)
	mux := admin.Routes()

	rec := doRequest(t, mux, http.MethodGet, "/admin/v1/usage?org=acme&window=24h", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
}

func TestAdmin_Usage_NonMemberForbidden(t *testing.T) {
	cfg := Config{}
	verifier := &fakeVerifier{subs: map[string]string{"jwt-stranger": "stranger-sub"}}
	reader := &fakeUsageReader{buckets: []UsageBucket{{Model: "m"}}}
	// "acme" exists but stranger-sub is not a member of it.
	admin := newUsageTestAdmin(t, cfg, verifier, "acme", "someone-else", "owner", reader)
	mux := admin.Routes()

	rec := doRequest(t, mux, http.MethodGet, "/admin/v1/usage?org=acme&window=7d", "jwt-stranger", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypePermission {
		t.Fatalf("error.type = %q, want %q", typ, errTypePermission)
	}
}

func TestAdmin_Usage_BadWindow(t *testing.T) {
	cfg := Config{}
	verifier := &fakeVerifier{subs: map[string]string{"jwt-viewer": "viewer-sub"}}
	reader := &fakeUsageReader{buckets: []UsageBucket{{Model: "m"}}}
	admin := newUsageTestAdmin(t, cfg, verifier, "acme", "viewer-sub", "viewer", reader)
	mux := admin.Routes()

	rec := doRequest(t, mux, http.MethodGet, "/admin/v1/usage?org=acme&window=90d", "jwt-viewer", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypeInvalidRequest {
		t.Fatalf("error.type = %q, want %q", typ, errTypeInvalidRequest)
	}
}

func TestAdmin_Usage_MissingOrg(t *testing.T) {
	cfg := Config{}
	verifier := &fakeVerifier{subs: map[string]string{"jwt-viewer": "viewer-sub"}}
	reader := &fakeUsageReader{buckets: []UsageBucket{{Model: "m"}}}
	admin := newUsageTestAdmin(t, cfg, verifier, "acme", "viewer-sub", "viewer", reader)
	mux := admin.Routes()

	rec := doRequest(t, mux, http.MethodGet, "/admin/v1/usage?window=7d", "jwt-viewer", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypeInvalidRequest {
		t.Fatalf("error.type = %q, want %q", typ, errTypeInvalidRequest)
	}
}

func TestAdmin_Usage_NilReaderNotConfigured(t *testing.T) {
	cfg := Config{}
	verifier := &fakeVerifier{subs: map[string]string{"jwt-viewer": "viewer-sub"}}
	admin := newUsageTestAdmin(t, cfg, verifier, "acme", "viewer-sub", "viewer", nil)
	mux := admin.Routes()

	rec := doRequest(t, mux, http.MethodGet, "/admin/v1/usage?org=acme&window=7d", "jwt-viewer", nil)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypeNotConfigured {
		t.Fatalf("error.type = %q, want %q", typ, errTypeNotConfigured)
	}
}

// --- env-gated ClickHouse integration test ----------------------------------

// TestNewCHUsageReader_OrgUsage inserts a handful of usage_events rows via a
// directly-constructed harvester.CHSink (same module, per the task brief's
// allowance) and asserts CHUsageReader.OrgUsage reads them back grouped by
// model/alias/provider/status for the requested window.
//
// One row is inserted twice, byte-identical, to reproduce a JetStream
// redelivery (an Ack that failed after a successful insert): reading
// usage_events FINAL must count it once. The
// old usage_hourly read path counted it twice, permanently.
//
// Skips cleanly when CP_TEST_CH_DSN is unset, exactly like
// internal/harvester's TestCHSink_InsertMonthToDateAndHourlyMV. CP_TEST_CH_DSN
// must point at an existing/default database the test process is allowed to
// create and drop sibling databases from (e.g.
// "clickhouse://localhost:19000/default"); this test never starts a
// ClickHouse container itself.
func TestNewCHUsageReader_OrgUsage(t *testing.T) {
	dsn := requireCHTestDSN(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testDSN, cleanup := chTestDatabase(t, ctx, dsn)
	defer cleanup()

	sink, err := harvester.NewCHSink(ctx, testDSN)
	if err != nil {
		t.Fatalf("NewCHSink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })

	now := time.Now().UTC()
	recent := now.Add(-time.Hour)        // within every window
	old := now.Add(-40 * 24 * time.Hour) // outside 30d

	rows := []harvester.Row{
		{
			ReqID: "u-1", TS: recent, Org: "acme", Project: "proj-a", KeyID: "key-1",
			Alias: "prod", Model: "gpt-5", Provider: "openai", Kind: "chat", Status: "ok",
			PromptTokens: 100, CompletionTokens: 50,
		},
		{
			ReqID: "u-2", TS: recent, Org: "acme", Project: "proj-a", KeyID: "key-2",
			Alias: "prod", Model: "gpt-5", Provider: "openai", Kind: "chat", Status: "ok",
			PromptTokens: 40, CompletionTokens: 20,
		},
		{
			ReqID: "u-3", TS: recent, Org: "acme", Project: "proj-a", KeyID: "key-2",
			Alias: "staging", Model: "gpt-5-mini", Provider: "openai", Kind: "chat", Status: "error",
			PromptTokens: 10, CompletionTokens: 0,
		},
		{
			// Different org — must never appear in "acme"'s usage.
			ReqID: "u-4", TS: recent, Org: "other-org", Project: "proj-a", KeyID: "key-9",
			Alias: "prod", Model: "gpt-5", Provider: "openai", Kind: "chat", Status: "ok",
			PromptTokens: 999, CompletionTokens: 999,
		},
		{
			// Too old for the 30d window — must be excluded from every
			// window this test checks.
			ReqID: "u-5", TS: old, Org: "acme", Project: "proj-a", KeyID: "key-1",
			Alias: "prod", Model: "gpt-5", Provider: "openai", Kind: "chat", Status: "ok",
			PromptTokens: 12345, CompletionTokens: 6789,
		},
	}
	if err := sink.InsertBatch(ctx, rows); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	// I1: redeliver u-1 as a second physical insert of the identical row.
	if err := sink.InsertBatch(ctx, rows[:1]); err != nil {
		t.Fatalf("InsertBatch (redelivery): %v", err)
	}

	reader, err := NewCHUsageReader(ctx, testDSN)
	if err != nil {
		t.Fatalf("NewCHUsageReader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })

	var buckets []UsageBucket
	deadline := time.Now().Add(15 * time.Second)
	for {
		buckets, err = reader.OrgUsage(ctx, "acme", "30d")
		if err != nil {
			t.Fatalf("OrgUsage: %v", err)
		}
		if len(buckets) >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if len(buckets) != 2 {
		t.Fatalf("OrgUsage(acme, 30d) returned %d buckets, want 2: %+v", len(buckets), buckets)
	}
	var gotProd, gotStaging *UsageBucket
	for i := range buckets {
		b := &buckets[i]
		switch b.Alias {
		case "prod":
			gotProd = b
		case "staging":
			gotStaging = b
		}
	}
	if gotProd == nil {
		t.Fatalf("no prod/gpt-5/ok bucket in %+v", buckets)
	}
	if gotProd.Requests != 2 || gotProd.PromptTokens != 140 || gotProd.CompletionTokens != 70 {
		t.Fatalf("prod bucket = %+v, want Requests=2 PromptTokens=140 CompletionTokens=70 (the redelivered u-1 must be deduplicated, not double-counted)", gotProd)
	}
	if gotStaging == nil {
		t.Fatalf("no staging/gpt-5-mini/error bucket in %+v", buckets)
	}
	if gotStaging.Requests != 1 || gotStaging.PromptTokens != 10 || gotStaging.Status != "error" {
		t.Fatalf("staging bucket = %+v, want Requests=1 PromptTokens=10 Status=error", gotStaging)
	}

	// A tight window that excludes "recent" (1h old, e.g. checked via 24h)
	// still finds it; only the old row is ever excluded. Sanity-check the
	// invalid-window error path here too since it's cheap and colocated
	// with a live reader.
	if _, err := reader.OrgUsage(ctx, "acme", "bogus"); err == nil {
		t.Fatal("OrgUsage with an invalid window: want error, got nil")
	}
}

// decodeJSONBody unmarshals rec's body into v.
func decodeJSONBody(t *testing.T, rec *httptest.ResponseRecorder, v any) error {
	t.Helper()
	return json.Unmarshal(rec.Body.Bytes(), v)
}

// requireCHTestDSN returns CP_TEST_CH_DSN or skips the test cleanly, per
// internal/harvester/chsink_test.go's established pattern.
func requireCHTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("CP_TEST_CH_DSN")
	if dsn == "" {
		t.Skip("CP_TEST_CH_DSN not set; skipping ClickHouse usage reader integration test")
	}
	return dsn
}

// chTestDatabase creates a throwaway database off baseDSN's host/auth and
// returns a DSN pointing at it plus a cleanup func that drops it.
func chTestDatabase(t *testing.T, ctx context.Context, baseDSN string) (dsn string, cleanup func()) {
	t.Helper()
	opts, err := clickhouse.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	admin, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open admin conn: %v", err)
	}
	if err := admin.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	dbName := fmt.Sprintf("controlplane_test_%d", time.Now().UnixNano())
	if err := admin.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+dbName); err != nil {
		t.Fatalf("create database %s: %v", dbName, err)
	}

	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + dbName
	dsn = u.String()

	cleanup = func() {
		if err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName); err != nil {
			t.Errorf("drop database %s: %v", dbName, err)
		}
		_ = admin.Close()
	}
	return dsn, cleanup
}

// TestNewCHUsageReader_MalformedDSNDoesNotLeakPassword is a regression
// test: clickhouse.ParseDSN fails with a *net/url.Error whose Error()
// reproduces the entire URL, userinfo
// included (net/url does not redact passwords in error strings), and
// cmd/inferbus prints this error straight to stdout — i.e. container logs
// and every aggregator downstream. The returned error must never contain
// the DSN or its password.
func TestNewCHUsageReader_MalformedDSNDoesNotLeakPassword(t *testing.T) {
	const password = "sup3rs3cret"
	// A raw DEL control character makes ParseDSN's URL parse fail.
	dsn := "clickhouse://admin:" + password + "@host\x7f:9000/inferbus"

	_, err := NewCHUsageReader(context.Background(), dsn)
	if err == nil {
		t.Fatal("NewCHUsageReader with a malformed DSN: want error, got nil")
	}
	if strings.Contains(err.Error(), password) {
		t.Fatalf("error leaks the clickhouse password: %v", err)
	}
	if strings.Contains(err.Error(), "clickhouse://") {
		t.Fatalf("error echoes the DSN: %v", err)
	}
	if !strings.Contains(err.Error(), "clickhouse_dsn") {
		t.Fatalf("error %v should still name the offending config key", err)
	}
}

// TestNewUsageReader_UnreachableClickHouseDegradesToNil covers the case
// where an unreachable (or malformed) clickhouse_dsn must degrade GET
// /admin/v1/usage to its documented 501 not_configured path, not abort
// Runner.Run — which would take the relay, the KV projectors and the whole
// admin API down with it, so an analytics dependency could stop key
// management from starting.
func TestNewUsageReader_UnreachableClickHouseDegradesToNil(t *testing.T) {
	// Port 1 is reserved and refuses connections immediately.
	if ur := newUsageReader(context.Background(), "clickhouse://127.0.0.1:1/inferbus"); ur != nil {
		t.Fatalf("newUsageReader with an unreachable DSN = %v, want nil (degraded)", ur)
	}
	if ur := newUsageReader(context.Background(), "clickhouse://admin:pw@host\x7f:9000/db"); ur != nil {
		t.Fatalf("newUsageReader with a malformed DSN = %v, want nil (degraded)", ur)
	}
	if ur := newUsageReader(context.Background(), ""); ur != nil {
		t.Fatalf("newUsageReader with no DSN = %v, want nil", ur)
	}
}

// TestOrgUsageSQL_ReadsDedupedEventsWithinBounds pins the query shape: reads
// come from usage_events
// (the ReplacingMergeTree ledger of record, with FINAL) rather than the
// usage_hourly SummingMergeTree rollup — which permanently double-counts a
// redelivered event, since a materialized view is an INSERT trigger with no
// dedup of its own — and every query carries a row LIMIT plus a
// server-side execution-time cap.
func TestOrgUsageSQL_ReadsDedupedEventsWithinBounds(t *testing.T) {
	q := fmt.Sprintf(orgUsageSQL, usageWindowIntervals["7d"], usageQueryMaxRows, int(usageQueryTimeout/time.Second))
	for _, want := range []string{
		"FROM usage_events FINAL",
		"toStartOfHour(now()) - INTERVAL 7 DAY",
		"LIMIT 10000",
		"max_execution_time = 10",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("orgUsageSQL missing %q:\n%s", want, q)
		}
	}
	if strings.Contains(q, "usage_hourly") {
		t.Errorf("orgUsageSQL still reads the double-counting rollup:\n%s", q)
	}
}
