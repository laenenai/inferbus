// Admin usage endpoint (Task 8): GET /admin/v1/usage?org=X&window=7d.
//
// This file owns the UsageReader seam and its production implementation,
// CHUsageReader, which queries ClickHouse's usage_hourly rollup table (the
// SummingMergeTree table fed by usage_hourly_mv — see
// internal/harvester/chsink.go's DDL, which is the schema's source of
// truth) grouped by model/alias/provider/status for one org over a
// caller-selected recency window.
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
	"fmt"
	"net/http"

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
// usage_hourly.hour. This is the single source of truth for which window
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

// CHUsageReader implements UsageReader against ClickHouse's usage_hourly
// rollup table (owned/created by internal/harvester.CHSink; this reader
// never applies DDL of its own).
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
		return nil, fmt.Errorf("controlplane: parse clickhouse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("controlplane: open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("controlplane: ping clickhouse: %w", err)
	}
	return &CHUsageReader{conn: conn}, nil
}

// orgUsageSQL groups usage_hourly by the endpoint's response shape (model,
// alias, provider, status) for one org, bounded by an INTERVAL that
// validUsageWindow has already confirmed is one of the fixed
// usageWindowIntervals values (never caller-supplied SQL text, so no
// injection risk from substituting it directly into the query string —
// ClickHouse's `INTERVAL ?` placeholder form does not accept a
// parameterized unit+quantity pair the way `WHERE org = ?` does for a
// plain value).
const orgUsageSQL = `
SELECT model, alias, provider, status,
       sum(requests) AS requests,
       sum(prompt_tokens) AS prompt_tokens,
       sum(completion_tokens) AS completion_tokens
FROM usage_hourly
WHERE org = ? AND hour >= now() - INTERVAL %s
GROUP BY model, alias, provider, status
`

// OrgUsage queries usage_hourly for org's usage over window, grouped by
// model/alias/provider/status. window must be one of "24h", "7d", or
// "30d" (validUsageWindow); any other value is an error — the HTTP handler
// validates this itself before ever calling OrgUsage, but OrgUsage
// re-validates so it is safe to call directly (as the env-gated
// integration test does) without going through the handler.
func (r *CHUsageReader) OrgUsage(ctx context.Context, org, window string) ([]UsageBucket, error) {
	interval, ok := usageWindowIntervals[window]
	if !ok {
		return nil, fmt.Errorf("controlplane: invalid usage window %q", window)
	}
	query := fmt.Sprintf(orgUsageSQL, interval)
	rows, err := r.conn.Query(ctx, query, org)
	if err != nil {
		return nil, fmt.Errorf("controlplane: query usage_hourly: %w", err)
	}
	defer rows.Close()

	var out []UsageBucket
	for rows.Next() {
		var b UsageBucket
		if err := rows.Scan(&b.Model, &b.Alias, &b.Provider, &b.Status, &b.Requests, &b.PromptTokens, &b.CompletionTokens); err != nil {
			return nil, fmt.Errorf("controlplane: scan usage_hourly row: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("controlplane: usage_hourly rows: %w", err)
	}
	return out, nil
}

// Close closes the underlying ClickHouse connection.
func (r *CHUsageReader) Close() error {
	return r.conn.Close()
}

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
