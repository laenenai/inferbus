// Admin usage endpoint (Task 8): GET /admin/v1/usage?org=X&window=7d.
//
// This file owns the UsageReader seam and its production implementation,
// CHUsageReader, which queries ClickHouse's usage_events table (the
// ReplacingMergeTree ledger of record — see internal/harvester/chsink.go's
// DDL, which is the schema's source of truth) grouped by
// model/alias/provider/status for one org over a caller-selected recency
// window. It deliberately does NOT read the usage_hourly rollup: that
// SummingMergeTree is fed by an INSERT-triggered materialized view and so
// permanently double-counts any redelivered METERING event.
//
// The control plane MAY import clickhouse-go (it owns databases, per the
// M4 usage design); internal/gateway must not — that's asserted at review
// time via `go list -deps ./internal/gateway | grep -c clickhouse`, not by
// any code in this file, since Go's module graph makes an accidental
// gateway dependency on this package's import a compile-visible fact, not
// a runtime one.
//
// UsageReader is nil-able: NewAdmin's usage param is optional (deployments
// without a ClickHouse DSN configured never wire one — see runner.go), and
// the handler below reports 501 not_configured rather than panicking when
// it's unset.
package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// UsageBucket is one aggregated row of an org's usage over a window,
// grouped by (model, alias, provider, status).
type UsageBucket struct {
	Model            string `json:"model"`
	Alias            string `json:"alias"`
	Provider         string `json:"provider"`
	Status           string `json:"status"`
	Requests         int64  `json:"requests"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
}

// UsageReader answers the admin usage endpoint's query: an org's usage,
// broken into UsageBuckets, over a window ("24h", "7d", or "30d").
// CHUsageReader is the production implementation; tests use a fake.
type UsageReader interface {
	OrgUsage(ctx context.Context, org, window string) ([]UsageBucket, error)
}

// usageWindowIntervals maps the wire ?window= values this endpoint accepts
// onto the ClickHouse INTERVAL expression OrgUsage's query uses to bound
// usage_events.ts. This is the single source of truth for which window
// values are valid — validUsageWindow and OrgUsage both consult it, so
// there is no way for the two to drift apart.
var usageWindowIntervals = map[string]string{
	"24h": "24 HOUR",
	"7d":  "7 DAY",
	"30d": "30 DAY",
}

// validUsageWindow reports whether w is one of the accepted window values.
func validUsageWindow(w string) bool {
	_, ok := usageWindowIntervals[w]
	return ok
}

// CHUsageReader implements UsageReader against ClickHouse's usage_events
// table (owned/created by internal/harvester.CHSink; this reader never
// applies DDL of its own).
type CHUsageReader struct {
	conn driver.Conn
}

// NewCHUsageReader connects to ClickHouse at dsn (same DSN shape as
// harvester.NewCHSink: "clickhouse://host:port/dbname") and pings it to
// fail fast on a bad DSN/unreachable server, per ch_api_notes.md's
// documented Open/Ping convention.
func NewCHUsageReader(ctx context.Context, dsn string) (*CHUsageReader, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		// ParseDSN's error is a *net/url.Error that reproduces the whole
		// URL, password included, and this error is
		// printed to stdout by cmd/inferbus. Never propagate it, and never
		// echo the DSN.
		return nil, errClickhouseDSN
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("controlplane: open clickhouse %s (database %q): %w", strings.Join(opts.Addr, ","), opts.Auth.Database, err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("controlplane: ping clickhouse %s (database %q): %w", strings.Join(opts.Addr, ","), opts.Auth.Database, err)
	}
	return &CHUsageReader{conn: conn}, nil
}

// errClickhouseDSN is what a malformed clickhouse_dsn produces: a fixed
// string that cannot carry the DSN's credentials into a log.
var errClickhouseDSN = errors.New("controlplane: invalid clickhouse_dsn: could not be parsed; check its syntax (the DSN is deliberately not echoed here — it may carry a password)")

// Query bounds for the admin usage endpoint. The query carried no LIMIT,
// no execution-time cap, and the handler passed a bare
// request context, so a `viewer` — the lowest-privileged role that clears
// the "read" gate — could hold the driver's 10-connection pool open on
// 30-day scans indefinitely. Every OrgUsage call is now bounded three ways:
// a server-side context deadline, ClickHouse's own max_execution_time, and
// a row LIMIT.
const (
	usageQueryTimeout = 10 * time.Second
	usageQueryMaxRows = 10000
)

// orgUsageSQL groups usage_events — the ReplacingMergeTree ledger of
// record, read with FINAL — by the endpoint's response shape (model, alias,
// provider, status) for one org.
//
// This used to read the usage_hourly rollup. A ClickHouse materialized
// view is an INSERT trigger, so usage_hourly_mv
// fires on every physical insert block, before and independent of any
// merge, and its SummingMergeTree target has no dedup concept at all — a
// redelivered METERING message (an Ack that failed after a successful
// insert, or an insert that timed out client-side but committed) is
// deduplicated in usage_events and permanently DOUBLE-COUNTED in
// usage_hourly. Reading usage_events FINAL makes admin reads inherit the
// ReplacingMergeTree dedup that the events table already guarantees.
// usage_hourly still exists and is still maintained, for dashboards that
// prefer speed over exactness (see docs/design-usage.md §2).
//
// The INTERVAL text is substituted rather than bound because ClickHouse's
// `INTERVAL ?` placeholder form does not accept a parameterized
// unit+quantity pair; validUsageWindow has already confirmed it is one of
// the fixed usageWindowIntervals values, never caller-supplied SQL text.
// The window is anchored on toStartOfHour(now()) so two calls minutes apart
// return the same totals for unchanged data. count() is cast to
// Int64 explicitly: it is UInt64 in ClickHouse, and the driver refuses to
// scan a UInt64 into UsageBucket.Requests (int64).
const orgUsageSQL = `
SELECT model, alias, provider, status,
       toInt64(count()) AS requests,
       sum(prompt_tokens) AS prompt_tokens,
       sum(completion_tokens) AS completion_tokens
FROM usage_events FINAL
WHERE org = ? AND ts >= toStartOfHour(now()) - INTERVAL %s
GROUP BY model, alias, provider, status
ORDER BY requests DESC
LIMIT %d
SETTINGS max_execution_time = %d
`

// OrgUsage queries usage_events for org's usage over window, grouped by
// model/alias/provider/status. window must be one of "24h", "7d", or
// "30d" (validUsageWindow); any other value is an error — the HTTP handler
// validates this itself before ever calling OrgUsage, but OrgUsage
// re-validates so it is safe to call directly (as the env-gated
// integration test does) without going through the handler.
//
// The caller's context is further bounded by usageQueryTimeout here,
// server-side, so the endpoint's cost does not depend on the client
// keeping its socket open.
func (r *CHUsageReader) OrgUsage(ctx context.Context, org, window string) ([]UsageBucket, error) {
	interval, ok := usageWindowIntervals[window]
	if !ok {
		return nil, fmt.Errorf("controlplane: invalid usage window %q", window)
	}
	ctx, cancel := context.WithTimeout(ctx, usageQueryTimeout)
	defer cancel()

	query := fmt.Sprintf(orgUsageSQL, interval, usageQueryMaxRows, int(usageQueryTimeout/time.Second))
	rows, err := r.conn.Query(ctx, query, org)
	if err != nil {
		return nil, fmt.Errorf("controlplane: query usage_events: %w", err)
	}
	defer rows.Close()

	// A non-nil empty slice, so an org with no usage serializes as
	// "buckets": [] rather than "buckets": null — matching every sibling
	// list handler in this package.
	out := make([]UsageBucket, 0)
	for rows.Next() {
		var b UsageBucket
		if err := rows.Scan(&b.Model, &b.Alias, &b.Provider, &b.Status, &b.Requests, &b.PromptTokens, &b.CompletionTokens); err != nil {
			return nil, fmt.Errorf("controlplane: scan usage_events row: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("controlplane: usage_events rows: %w", err)
	}
	return out, nil
}

// Close closes the underlying ClickHouse connection.
func (r *CHUsageReader) Close() error {
	return r.conn.Close()
}

// newUsageReader builds the optional ClickHouse usage reader for
// Runner.Run. A connect/ping failure is NOT fatal: it
// logs loudly and returns a nil UsageReader, which makes GET
// /admin/v1/usage answer 501 not_configured — the degraded path the design
// already specifies — instead of aborting Runner.Run and taking the relay,
// the KV projectors and the whole admin API down with it. An analytics
// endpoint must not be able to stop key management from starting; a
// ClickHouse that is merely restarting while the control plane rolls is
// exactly the case that used to wedge it.
//
// The returned reader is nil-able by design; callers must nil-check before
// closing it. The ping is bounded so a black-holed ClickHouse address
// delays startup by usageConnectTimeout at most, rather than hanging it.
func newUsageReader(ctx context.Context, dsn string) UsageReader {
	if dsn == "" {
		return nil
	}
	connectCtx, cancel := context.WithTimeout(ctx, usageConnectTimeout)
	defer cancel()
	ur, err := NewCHUsageReader(connectCtx, dsn)
	if err != nil {
		slog.Error("controlplane: clickhouse usage reader unavailable; GET /admin/v1/usage will report 501 until the control plane is restarted with a reachable clickhouse_dsn", "err", err)
		return nil
	}
	return ur
}

// usageConnectTimeout bounds the startup ping in newUsageReader.
const usageConnectTimeout = 10 * time.Second

// usageResponse is the GET /admin/v1/usage response body.
type usageResponse struct {
	Org     string        `json:"org"`
	Window  string        `json:"window"`
	Buckets []UsageBucket `json:"buckets"`
}

// getUsage handles GET /admin/v1/usage?org=X&window=7d. Authorization
// mirrors listKeys/getOrg's "read" need on org (platform admin any org,
// per authorizeOrg). Ordering matters for the tests this handler is bound
// by: org-missing and authorization are both checked before window
// validity and the not-configured check, so a non-member never learns
// whether the window they passed was well-formed, and a member is only
// ever told "not configured" once they've already cleared authorization.
func (a *Admin) getUsage(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}

	orgID := r.URL.Query().Get("org")
	if !validSlug(orgID) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "org query parameter is required")
		return
	}
	if !a.authorizeOrg(w, r, id, orgID, "read") {
		return
	}

	window := r.URL.Query().Get("window")
	if !validUsageWindow(window) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, `window must be one of "24h", "7d", "30d"`)
		return
	}

	if a.usage == nil {
		writeAdminError(w, http.StatusNotImplemented, errTypeNotConfigured, "usage reporting is not configured (no clickhouse_dsn set)")
		return
	}

	buckets, err := a.usage.OrgUsage(r.Context(), orgID, window)
	if err != nil {
		writeInternalError(w, "getUsage", err)
		return
	}
	writeJSON(w, http.StatusOK, usageResponse{Org: orgID, Window: window, Buckets: buckets})
}
