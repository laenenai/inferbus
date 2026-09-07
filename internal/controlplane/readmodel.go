// SQL admin read models (Task 8).
//
// This file is the third and last read-model consumer of the control
// plane's event log (after Task 7's ALIASES/KEYS KV projectors): a plain
// SQL-shaped ReadStore — an org/member/project/key/alias set of tables
// meant for the admin API's list/get endpoints, not the gateway's
// request-time hot path (that's what the KV buckets are for). Two
// interchangeable implementations satisfy the same ReadStore interface:
// NewMemReadStore (map-backed, for tests and the admin-API tests) and
// NewPgReadStore (real Postgres tables, plain UPSERTs).
//
// RunSQLProjector follows the exact same "Replay-then-live + marker"
// idiom as kvproj.go's RunKVProjectors (crib notes there for the full
// rationale): projection.Replay off the DB log is the authoritative
// rebuild source, a durable NATS consumer tails CONTROL_EVENTS for live
// delivery, and a KVMarker (CP_MARKERS bucket, key "proj-sql-admin")
// records the cutover position. The same fail-stop discipline applies:
// apply() latches a sticky error under its mutex the instant anything
// goes wrong, so no later envelope (redelivered or new) can ever advance
// the marker past a failure.
//
// Unlike the KV keys projector, the SQL projector needs no private,
// unpersisted id -> hash map and no warmup replay. KeyRow/ProjectRow are
// keyed by their aggregate's own id (not by a hash that changes over
// time), and events that don't carry every field the read model needs
// (KeyDisabled/KeyAllowlistChanged/KeyLimitsChanged carry no
// org/project/name; ProjectArchived carries no name) are handled by
// reloading the aggregate's current state with aggregate.Runtime.Load
// (which folds the whole stream via the same Evolve already used to
// build that state) rather than caching it ourselves — the persisted SQL
// row is not itself a valid source for this because ReadStore has no
// get-by-id query, only get-by-org. KeyRow deliberately has no hash
// field at all (constructor of Task 8's binding requirement: admin GETs
// must never leak a hash), so KeyRotated needs no read-model update
// whatsoever — the row already reflects everything about the key that
// is ever shown to an admin.
package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/controlplane/alias"
	"github.com/laenenai/inferbus/internal/controlplane/apikey"
	"github.com/laenenai/inferbus/internal/controlplane/org"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/natsjs"
	"github.com/laenenai/es-lite/projection"
)

// ErrOrgNotFound is returned by ReadStore.GetOrg when no org with the
// given id has ever been upserted.
var ErrOrgNotFound = errors.New("controlplane: org not found")

// --- row schemas ---------------------------------------------------------

// OrgRow is one row of the cp_orgs admin read model.
type OrgRow struct {
	ID   string
	Name string
}

// MemberRow is one row of the cp_org_members admin read model, always
// scoped to a particular org (see ReadStore.GetOrg).
type MemberRow struct {
	Sub  string
	Role string
}

// ProjectRow is one row of the cp_projects admin read model.
type ProjectRow struct {
	ID       string
	Org      string
	Name     string
	Archived bool
}

// KeyRow is one row of the cp_api_keys admin read model. It deliberately
// has NO hash field: admin GETs must never leak a key's hash (binding
// requirement, Task 8), so nothing in this package's write path may ever
// populate one.
type KeyRow struct {
	ID           string
	Org          string
	Project      string
	Name         string
	Allow        []string
	RateLimitRPM int
	Disabled     bool
}

// AliasRow is one row of the cp_aliases admin read model.
type AliasRow struct {
	Scope  string
	Name   string
	Target string
	Params map[string]string
}

// ReadStore is the admin API's SQL-shaped read model: one write method
// per event the projector needs to apply, plus the query methods the
// admin API's list/get endpoints read from. Two implementations satisfy
// it: NewMemReadStore (tests) and NewPgReadStore (production).
type ReadStore interface {
	UpsertOrg(ctx context.Context, row OrgRow) error
	UpsertMember(ctx context.Context, org, sub, role string) error
	RemoveMember(ctx context.Context, org, sub string) error
	UpsertProject(ctx context.Context, row ProjectRow) error
	UpsertKey(ctx context.Context, row KeyRow) error
	UpsertAlias(ctx context.Context, row AliasRow) error
	DeleteAlias(ctx context.Context, scope, name string) error

	ListOrgs(ctx context.Context) ([]OrgRow, error)
	GetOrg(ctx context.Context, id string) (OrgRow, []MemberRow, []ProjectRow, error)
	ListKeys(ctx context.Context, org string) ([]KeyRow, error)
	ListAliases(ctx context.Context, scope string) ([]AliasRow, error)
}

// --- MemReadStore: map-backed, for tests and the admin-API tests --------

// MemReadStore is an in-memory ReadStore. All query methods return
// results sorted for deterministic assertions.
type MemReadStore struct {
	mu       sync.Mutex
	orgs     map[string]OrgRow                // org id -> row
	members  map[string]map[string]string     // org id -> sub -> role
	projects map[string]map[string]ProjectRow // org id -> project id -> row
	keys     map[string]KeyRow                // key id -> row
	aliases  map[string]AliasRow              // "<scope>/<name>" -> row
}

var _ ReadStore = (*MemReadStore)(nil)

// NewMemReadStore returns an empty, ready-to-use in-memory ReadStore.
func NewMemReadStore() *MemReadStore {
	return &MemReadStore{
		orgs:     map[string]OrgRow{},
		members:  map[string]map[string]string{},
		projects: map[string]map[string]ProjectRow{},
		keys:     map[string]KeyRow{},
		aliases:  map[string]AliasRow{},
	}
}

func (s *MemReadStore) UpsertOrg(_ context.Context, row OrgRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orgs[row.ID] = row
	return nil
}

func (s *MemReadStore) UpsertMember(_ context.Context, orgID, sub, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.members[orgID]
	if !ok {
		m = map[string]string{}
		s.members[orgID] = m
	}
	m[sub] = role
	return nil
}

func (s *MemReadStore) RemoveMember(_ context.Context, orgID, sub string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.members[orgID], sub)
	return nil
}

func (s *MemReadStore) UpsertProject(_ context.Context, row ProjectRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.projects[row.Org]
	if !ok {
		m = map[string]ProjectRow{}
		s.projects[row.Org] = m
	}
	m[row.ID] = row
	return nil
}

func (s *MemReadStore) UpsertKey(_ context.Context, row KeyRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Defensive copy: Allow is caller-owned; don't let a later mutation
	// of the caller's slice reach back into the stored row.
	row.Allow = append([]string(nil), row.Allow...)
	s.keys[row.ID] = row
	return nil
}

func (s *MemReadStore) UpsertAlias(_ context.Context, row AliasRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row.Params = copyStringMap(row.Params)
	s.aliases[row.Scope+"/"+row.Name] = row
	return nil
}

func (s *MemReadStore) DeleteAlias(_ context.Context, scope, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.aliases, scope+"/"+name)
	return nil
}

func (s *MemReadStore) ListOrgs(_ context.Context) ([]OrgRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([]OrgRow, 0, len(s.orgs))
	for _, row := range s.orgs {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows, nil
}

func (s *MemReadStore) GetOrg(_ context.Context, id string) (OrgRow, []MemberRow, []ProjectRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.orgs[id]
	if !ok {
		return OrgRow{}, nil, nil, ErrOrgNotFound
	}

	members := make([]MemberRow, 0, len(s.members[id]))
	for sub, role := range s.members[id] {
		members = append(members, MemberRow{Sub: sub, Role: role})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Sub < members[j].Sub })

	projects := make([]ProjectRow, 0, len(s.projects[id]))
	for _, p := range s.projects[id] {
		projects = append(projects, p)
	}
	sort.Slice(projects, func(i, j int) bool { return projects[i].ID < projects[j].ID })

	return row, members, projects, nil
}

func (s *MemReadStore) ListKeys(_ context.Context, orgID string) ([]KeyRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rows []KeyRow
	for _, row := range s.keys {
		if row.Org != orgID {
			continue
		}
		row.Allow = append([]string(nil), row.Allow...)
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows, nil
}

func (s *MemReadStore) ListAliases(_ context.Context, scope string) ([]AliasRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rows []AliasRow
	for _, row := range s.aliases {
		if row.Scope != scope {
			continue
		}
		row.Params = copyStringMap(row.Params)
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows, nil
}

func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// --- PgReadStore: real Postgres tables, plain UPSERTs --------------------

// PgReadStore is a Postgres-backed ReadStore.
type PgReadStore struct {
	pool *pgxpool.Pool
}

var _ ReadStore = (*PgReadStore)(nil)

// pgSchema is the idempotent DDL for the five admin read-model tables.
// Split into one statement per table because pgx's default (extended
// query protocol) Exec cannot run multiple statements in a single call.
var pgSchema = []string{
	`CREATE TABLE IF NOT EXISTS cp_orgs (
		id   TEXT PRIMARY KEY,
		name TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS cp_org_members (
		org  TEXT NOT NULL,
		sub  TEXT NOT NULL,
		role TEXT NOT NULL,
		PRIMARY KEY (org, sub)
	)`,
	`CREATE TABLE IF NOT EXISTS cp_projects (
		id       TEXT PRIMARY KEY,
		org      TEXT NOT NULL,
		name     TEXT NOT NULL,
		archived BOOLEAN NOT NULL DEFAULT false
	)`,
	`CREATE TABLE IF NOT EXISTS cp_api_keys (
		id             TEXT PRIMARY KEY,
		org            TEXT NOT NULL,
		project        TEXT NOT NULL,
		name           TEXT NOT NULL,
		allow          TEXT[] NOT NULL DEFAULT '{}',
		rate_limit_rpm INTEGER NOT NULL DEFAULT 0,
		disabled       BOOLEAN NOT NULL DEFAULT false
	)`,
	`CREATE TABLE IF NOT EXISTS cp_aliases (
		scope  TEXT NOT NULL,
		name   TEXT NOT NULL,
		target TEXT NOT NULL,
		params JSONB NOT NULL DEFAULT '{}'::jsonb,
		PRIMARY KEY (scope, name)
	)`,
}

// NewPgReadStore wraps pool as a ReadStore, creating (idempotently, via
// CREATE TABLE IF NOT EXISTS) the cp_orgs/cp_org_members/cp_projects/
// cp_api_keys/cp_aliases tables if they don't already exist. Schema setup
// uses context.Background() internally (table creation is a one-time,
// not caller-cancellable concern), matching the brief's constructor
// signature of pool alone; the error return is the natural Go idiom for
// a constructor that does I/O.
func NewPgReadStore(pool *pgxpool.Pool) (*PgReadStore, error) {
	ctx := context.Background()
	for _, stmt := range pgSchema {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return nil, fmt.Errorf("controlplane: pg read store: create schema: %w", err)
		}
	}
	return &PgReadStore{pool: pool}, nil
}

func (s *PgReadStore) UpsertOrg(ctx context.Context, row OrgRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO cp_orgs (id, name) VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
		row.ID, row.Name)
	if err != nil {
		return fmt.Errorf("controlplane: pg read store: upsert org %q: %w", row.ID, err)
	}
	return nil
}

func (s *PgReadStore) UpsertMember(ctx context.Context, org, sub, role string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO cp_org_members (org, sub, role) VALUES ($1, $2, $3)
		ON CONFLICT (org, sub) DO UPDATE SET role = EXCLUDED.role`,
		org, sub, role)
	if err != nil {
		return fmt.Errorf("controlplane: pg read store: upsert member %q/%q: %w", org, sub, err)
	}
	return nil
}

func (s *PgReadStore) RemoveMember(ctx context.Context, org, sub string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM cp_org_members WHERE org = $1 AND sub = $2`, org, sub)
	if err != nil {
		return fmt.Errorf("controlplane: pg read store: remove member %q/%q: %w", org, sub, err)
	}
	return nil
}

func (s *PgReadStore) UpsertProject(ctx context.Context, row ProjectRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO cp_projects (id, org, name, archived) VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET
			org = EXCLUDED.org, name = EXCLUDED.name, archived = EXCLUDED.archived`,
		row.ID, row.Org, row.Name, row.Archived)
	if err != nil {
		return fmt.Errorf("controlplane: pg read store: upsert project %q: %w", row.ID, err)
	}
	return nil
}

func (s *PgReadStore) UpsertKey(ctx context.Context, row KeyRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO cp_api_keys (id, org, project, name, allow, rate_limit_rpm, disabled)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE SET
			org = EXCLUDED.org, project = EXCLUDED.project, name = EXCLUDED.name,
			allow = EXCLUDED.allow, rate_limit_rpm = EXCLUDED.rate_limit_rpm,
			disabled = EXCLUDED.disabled`,
		row.ID, row.Org, row.Project, row.Name, row.Allow, row.RateLimitRPM, row.Disabled)
	if err != nil {
		return fmt.Errorf("controlplane: pg read store: upsert key %q: %w", row.ID, err)
	}
	return nil
}

func (s *PgReadStore) UpsertAlias(ctx context.Context, row AliasRow) error {
	params, err := json.Marshal(row.Params)
	if err != nil {
		return fmt.Errorf("controlplane: pg read store: marshal alias params %q/%q: %w", row.Scope, row.Name, err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO cp_aliases (scope, name, target, params) VALUES ($1, $2, $3, $4::jsonb)
		ON CONFLICT (scope, name) DO UPDATE SET target = EXCLUDED.target, params = EXCLUDED.params`,
		row.Scope, row.Name, row.Target, string(params))
	if err != nil {
		return fmt.Errorf("controlplane: pg read store: upsert alias %q/%q: %w", row.Scope, row.Name, err)
	}
	return nil
}

func (s *PgReadStore) DeleteAlias(ctx context.Context, scope, name string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM cp_aliases WHERE scope = $1 AND name = $2`, scope, name)
	if err != nil {
		return fmt.Errorf("controlplane: pg read store: delete alias %q/%q: %w", scope, name, err)
	}
	return nil
}

func (s *PgReadStore) ListOrgs(ctx context.Context) ([]OrgRow, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name FROM cp_orgs ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("controlplane: pg read store: list orgs: %w", err)
	}
	defer rows.Close()

	var out []OrgRow
	for rows.Next() {
		var row OrgRow
		if err := rows.Scan(&row.ID, &row.Name); err != nil {
			return nil, fmt.Errorf("controlplane: pg read store: list orgs: scan: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("controlplane: pg read store: list orgs: %w", err)
	}
	return out, nil
}

func (s *PgReadStore) GetOrg(ctx context.Context, id string) (OrgRow, []MemberRow, []ProjectRow, error) {
	var row OrgRow
	err := s.pool.QueryRow(ctx, `SELECT id, name FROM cp_orgs WHERE id = $1`, id).Scan(&row.ID, &row.Name)
	if errors.Is(err, pgx.ErrNoRows) {
		return OrgRow{}, nil, nil, ErrOrgNotFound
	}
	if err != nil {
		return OrgRow{}, nil, nil, fmt.Errorf("controlplane: pg read store: get org %q: %w", id, err)
	}

	memberRows, err := s.pool.Query(ctx, `SELECT sub, role FROM cp_org_members WHERE org = $1 ORDER BY sub`, id)
	if err != nil {
		return OrgRow{}, nil, nil, fmt.Errorf("controlplane: pg read store: get org %q: members: %w", id, err)
	}
	var members []MemberRow
	for memberRows.Next() {
		var m MemberRow
		if err := memberRows.Scan(&m.Sub, &m.Role); err != nil {
			memberRows.Close()
			return OrgRow{}, nil, nil, fmt.Errorf("controlplane: pg read store: get org %q: members scan: %w", id, err)
		}
		members = append(members, m)
	}
	memberRows.Close()
	if err := memberRows.Err(); err != nil {
		return OrgRow{}, nil, nil, fmt.Errorf("controlplane: pg read store: get org %q: members: %w", id, err)
	}

	projRows, err := s.pool.Query(ctx, `SELECT id, org, name, archived FROM cp_projects WHERE org = $1 ORDER BY id`, id)
	if err != nil {
		return OrgRow{}, nil, nil, fmt.Errorf("controlplane: pg read store: get org %q: projects: %w", id, err)
	}
	var projects []ProjectRow
	for projRows.Next() {
		var p ProjectRow
		if err := projRows.Scan(&p.ID, &p.Org, &p.Name, &p.Archived); err != nil {
			projRows.Close()
			return OrgRow{}, nil, nil, fmt.Errorf("controlplane: pg read store: get org %q: projects scan: %w", id, err)
		}
		projects = append(projects, p)
	}
	projRows.Close()
	if err := projRows.Err(); err != nil {
		return OrgRow{}, nil, nil, fmt.Errorf("controlplane: pg read store: get org %q: projects: %w", id, err)
	}

	return row, members, projects, nil
}

func (s *PgReadStore) ListKeys(ctx context.Context, org string) ([]KeyRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, org, project, name, allow, rate_limit_rpm, disabled
		FROM cp_api_keys WHERE org = $1 ORDER BY id`, org)
	if err != nil {
		return nil, fmt.Errorf("controlplane: pg read store: list keys %q: %w", org, err)
	}
	defer rows.Close()

	var out []KeyRow
	for rows.Next() {
		var row KeyRow
		if err := rows.Scan(&row.ID, &row.Org, &row.Project, &row.Name, &row.Allow, &row.RateLimitRPM, &row.Disabled); err != nil {
			return nil, fmt.Errorf("controlplane: pg read store: list keys %q: scan: %w", org, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("controlplane: pg read store: list keys %q: %w", org, err)
	}
	return out, nil
}

func (s *PgReadStore) ListAliases(ctx context.Context, scope string) ([]AliasRow, error) {
	rows, err := s.pool.Query(ctx, `SELECT scope, name, target, params FROM cp_aliases WHERE scope = $1 ORDER BY name`, scope)
	if err != nil {
		return nil, fmt.Errorf("controlplane: pg read store: list aliases %q: %w", scope, err)
	}
	defer rows.Close()

	var out []AliasRow
	for rows.Next() {
		var row AliasRow
		var params []byte
		if err := rows.Scan(&row.Scope, &row.Name, &row.Target, &params); err != nil {
			return nil, fmt.Errorf("controlplane: pg read store: list aliases %q: scan: %w", scope, err)
		}
		if len(params) > 0 {
			if err := json.Unmarshal(params, &row.Params); err != nil {
				return nil, fmt.Errorf("controlplane: pg read store: list aliases %q: unmarshal params: %w", scope, err)
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("controlplane: pg read store: list aliases %q: %w", scope, err)
	}
	return out, nil
}

// --- RunSQLProjector -------------------------------------------------------

// durableProjSQL is both the natsjs.Consume durable consumer name AND the
// Marker's per-projection key for the SQL admin read model.
const durableProjSQL = "proj-sql-admin"

// filterSubjectSQL is deliberately unfiltered by aggregate type (unlike
// kvproj.go's per-aggregate filterSubjectAliases/filterSubjectKeys):
// RunSQLProjector consumes every org/apikey/alias event needed to build
// the admin read model off a single durable. The workspace segment is
// still wildcarded (binding controller ruling, same rationale as
// kvproj.go: sqlite tests publish "evt.default.<agg>.<event>", postgres
// publishes "evt.<workspace>.<agg>.<event>").
const filterSubjectSQL = "evt.*.>"

// sqlProjector holds the SQL admin read model's live projector state. It
// reuses the org/apikey aggregates' own Runtime.Load to recover fields an
// event doesn't carry itself (see the package doc comment) rather than
// keeping any private cache of its own — unlike kvproj.go's keysProjector,
// there is no warmup needed here.
type sqlProjector struct {
	rs     ReadStore
	orgRT  *aggregate.Runtime[*controlplanev1.Org, *controlplanev1.OrgCommand, *controlplanev1.OrgEvent]
	keyRT  *aggregate.Runtime[*controlplanev1.ApiKey, *controlplanev1.ApiKeyCommand, *controlplanev1.ApiKeyEvent]
	marker Marker

	mu     sync.Mutex
	pos    uint64
	failed error // sticky: once set, every future apply() call is a no-op that returns it
}

func newSQLProjector(store es.Store, rs ReadStore, marker Marker) *sqlProjector {
	return &sqlProjector{
		rs:     rs,
		orgRT:  aggregate.NewRuntime(store, org.Decider, org.Codec()),
		keyRT:  aggregate.NewRuntime(store, apikey.Decider, apikey.Codec()),
		marker: marker,
	}
}

// err returns the sticky fail-stop error, if any (see apply).
func (p *sqlProjector) err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.failed
}

// apply is a projection.Apply, reused verbatim for the replay catch-up
// and (wrapped per-envelope) the live natsjs consumer. Fail-stop
// semantics are identical to kvproj.go's aliasProjector.apply/
// keysProjector.apply (see their shared doc comment, C1 ruling): a sticky
// p.failed latch, checked and set under the same mutex that guards pos,
// guarantees no envelope after a failure can advance the marker past it.
func (p *sqlProjector) apply(ctx context.Context, batch []es.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.failed != nil {
		return p.failed
	}

	for _, e := range batch {
		if e.GlobalPosition <= p.pos {
			continue // idempotent skip: already applied
		}

		var err error
		switch e.StreamID.Type {
		case org.StreamType:
			err = p.applyOrgEvent(ctx, e)
		case apikey.StreamType:
			err = p.applyApiKeyEvent(ctx, e)
		case alias.StreamType:
			err = p.applyAliasEvent(ctx, e)
		}
		if err != nil {
			p.failed = err
			return err
		}

		p.pos = e.GlobalPosition
		if err := p.marker.Save(ctx, durableProjSQL, p.pos); err != nil {
			p.failed = err
			return err
		}
	}
	return nil
}

func (p *sqlProjector) applyOrgEvent(ctx context.Context, e es.Envelope) error {
	evt, err := org.Codec().Decode(es.EncodedEvent{
		TypeURL:       e.TypeURL,
		SchemaVersion: e.SchemaVersion,
		Payload:       e.Payload,
	})
	if err != nil {
		return fmt.Errorf("controlplane: sql projector: decode %s: %w", e.TypeURL, err)
	}

	orgID := e.StreamID.ID // stream id IS the org id (org.Decide/CreateOrg convention)

	switch k := evt.GetKind().(type) {
	case *controlplanev1.OrgEvent_Created:
		if err := p.rs.UpsertOrg(ctx, OrgRow{ID: orgID, Name: k.Created.GetName()}); err != nil {
			return err
		}
		return p.rs.UpsertMember(ctx, orgID, k.Created.GetOwnerSub(), "owner")

	case *controlplanev1.OrgEvent_Renamed:
		return p.rs.UpsertOrg(ctx, OrgRow{ID: orgID, Name: k.Renamed.GetName()})

	case *controlplanev1.OrgEvent_MemberUpserted:
		return p.rs.UpsertMember(ctx, orgID, k.MemberUpserted.GetSub(), k.MemberUpserted.GetRole())

	case *controlplanev1.OrgEvent_MemberRemoved:
		return p.rs.RemoveMember(ctx, orgID, k.MemberRemoved.GetSub())

	case *controlplanev1.OrgEvent_ProjectCreated:
		return p.rs.UpsertProject(ctx, ProjectRow{
			ID:   k.ProjectCreated.GetId(),
			Org:  orgID,
			Name: k.ProjectCreated.GetName(),
		})

	case *controlplanev1.OrgEvent_ProjectArchived:
		// ProjectArchived carries only the project id, not its name — so
		// reload the org aggregate's current state (post this very event,
		// since apply always runs after the event is committed) and read
		// the project back from it. The decider's own ErrNoSuchProject
		// guard means ArchiveProject can never be issued for a project
		// that doesn't exist, so a missing entry here is an invariant
		// violation, not a normal case to paper over (mirrors kvproj.go's
		// AllowlistChanged/LimitsChanged fail-stop, not its Rotated
		// defensive-synthesize arm).
		id := k.ProjectArchived.GetId()
		state, _, err := p.orgRT.Load(ctx, e.StreamID)
		if err != nil {
			return fmt.Errorf("controlplane: sql projector: load org %q for project archive: %w", orgID, err)
		}
		proj, ok := state.GetProjects()[id]
		if !ok {
			return fmt.Errorf("controlplane: sql projector: project archived for org %q project %q: no such project in aggregate state (invariant violation)", orgID, id)
		}
		return p.rs.UpsertProject(ctx, ProjectRow{
			ID:       proj.GetId(),
			Org:      orgID,
			Name:     proj.GetName(),
			Archived: proj.GetArchived(),
		})
	}
	return nil
}

// applyApiKeyEvent handles every ApiKeyEvent variant uniformly: it
// reloads the apikey aggregate's current (post-event) state and upserts
// the full KeyRow from it. This works identically for Created, Rotated
// (a genuine no-op for the read model — the hash never appears in
// KeyRow), Disabled, AllowlistChanged, and LimitsChanged alike, so no
// event-kind switch or private id -> hash cache (kvproj.go's keysProjector
// approach) is needed here: KeyRow is keyed by the key's own id, which is
// always e.StreamID.ID, and every field it needs is a plain field of the
// apikey aggregate's own state.
func (p *sqlProjector) applyApiKeyEvent(ctx context.Context, e es.Envelope) error {
	state, _, err := p.keyRT.Load(ctx, e.StreamID)
	if err != nil {
		return fmt.Errorf("controlplane: sql projector: load apikey %q: %w", e.StreamID.ID, err)
	}
	return p.rs.UpsertKey(ctx, KeyRow{
		ID:           e.StreamID.ID,
		Org:          state.GetOrg(),
		Project:      state.GetProject(),
		Name:         state.GetName(),
		Allow:        append([]string(nil), state.GetAllow()...),
		RateLimitRPM: int(state.GetRateLimitRpm()),
		Disabled:     state.GetDisabled(),
	})
}

func (p *sqlProjector) applyAliasEvent(ctx context.Context, e es.Envelope) error {
	scope, name, err := SplitAliasStreamID(e.StreamID.ID)
	if err != nil {
		return fmt.Errorf("controlplane: sql projector: %w", err)
	}

	evt, err := alias.Codec().Decode(es.EncodedEvent{
		TypeURL:       e.TypeURL,
		SchemaVersion: e.SchemaVersion,
		Payload:       e.Payload,
	})
	if err != nil {
		return fmt.Errorf("controlplane: sql projector: decode %s: %w", e.TypeURL, err)
	}

	switch k := evt.GetKind().(type) {
	case *controlplanev1.AliasEvent_Set:
		return p.rs.UpsertAlias(ctx, AliasRow{
			Scope:  scope,
			Name:   name,
			Target: k.Set.GetTarget(),
			Params: k.Set.GetParams(),
		})

	case *controlplanev1.AliasEvent_Deleted:
		return p.rs.DeleteAlias(ctx, scope, name)
	}
	return nil
}

// RunSQLProjector creates the CP_MARKERS bucket if needed, replays the SQL
// admin projector from its marker straight off store's log
// (projection.Replay — the authoritative rebuild source, not JetStream
// retention), then live-consumes the durable "proj-sql-admin"
// (evt.*.>) off the CONTROL_EVENTS stream until ctx is done, mapping every
// org/apikey/alias event onto rs's ReadStore calls.
func RunSQLProjector(ctx context.Context, store es.Store, js jetstream.JetStream, rs ReadStore) error {
	markersKV, err := ensureKVBucket(ctx, js, bucketMarkers)
	if err != nil {
		return err
	}
	marker := NewKVMarker(markersKV)

	p := newSQLProjector(store, rs, marker)
	p.pos, err = marker.Load(ctx, durableProjSQL)
	if err != nil {
		return err
	}

	if _, err := projection.Replay(ctx, store, p.pos, replayBatchSize, p.apply); err != nil {
		return fmt.Errorf("controlplane: replay sql admin projector: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	consumeErr := natsjs.Consume(runCtx, js, natsjs.ConsumerConfig{
		Stream:        StreamControlEvents,
		Durable:       durableProjSQL,
		FilterSubject: filterSubjectSQL,
	}, func(ctx context.Context, e es.Envelope) error {
		if err := p.apply(ctx, []es.Envelope{e}); err != nil {
			// natsjs.Consume never surfaces a handler error itself (it
			// just Naks and keeps pulling — api-notes natsjs.Consume doc:
			// "Returning an error naks it for redelivery"), so cancelling
			// runCtx here is what actually stops delivery (same fail-stop
			// wiring as kvproj.go's RunKVProjectors consume wrapper).
			cancel()
			return err
		}
		return nil
	})

	// A projector's own fail-stop error (set by apply, under its mutex)
	// takes priority: it is the actual cause, whereas consumeErr here
	// would only ever be an infra-level natsjs.Consume setup failure or
	// that same apply error surfacing a second time through Consume's
	// return (kvproj.go's RunKVProjectors follows the same priority).
	if err := p.err(); err != nil {
		return err
	}
	if consumeErr != nil && !errors.Is(consumeErr, context.Canceled) {
		return consumeErr
	}
	return ctx.Err()
}
