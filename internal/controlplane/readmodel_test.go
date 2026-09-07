package controlplane

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/controlplane/alias"
	"github.com/laenenai/inferbus/internal/controlplane/apikey"
	"github.com/laenenai/inferbus/internal/controlplane/org"
	"github.com/laenenai/inferbus/internal/testutil"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// --- test-only Marker ------------------------------------------------------

// memMarker is a trivial in-memory Marker for handler-level tests that
// don't want to stand up a real JetStream KV bucket.
type memMarker struct {
	mu  sync.Mutex
	pos map[string]uint64
}

func newMemMarker() *memMarker { return &memMarker{pos: map[string]uint64{}} }

func (m *memMarker) Load(_ context.Context, name string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pos[name], nil
}

func (m *memMarker) Save(_ context.Context, name string, pos uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pos[name] = pos
	return nil
}

// --- Step 1: handler-level table test against NewMemReadStore -------------

// TestSQLProjector_Handler_Lifecycle feeds a scripted envelope sequence
// (org create, member upsert/remove, project create/archive, key
// create/rotate/allowlist/limits/disable, alias set/delete) through the
// sqlProjector's apply handler directly (no NATS/relay involved — this is
// the fast, handler-level layer per the brief) and asserts the final
// ReadStore rows.
func TestSQLProjector_Handler_Lifecycle(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	orgRT := aggregate.NewRuntime(store, org.Decider, org.Codec())
	keyRT := aggregate.NewRuntime(store, apikey.Decider, apikey.Codec())
	aliasRT := aggregate.NewRuntime(store, alias.Decider, alias.Codec())

	orgStream, err := es.NewStreamID(org.StreamType, "acme")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	keyStream, err := es.NewStreamID(apikey.StreamType, "key-1")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	globalFast, err := es.NewStreamID(alias.StreamType, "g_fast")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	acmeSmart, err := es.NewStreamID(alias.StreamType, "o_acme_smart")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}

	// --- org: create, upsert a second member, create + archive a project
	if _, err := orgRT.Handle(ctx, orgStream, &controlplanev1.OrgCommand{
		Kind: &controlplanev1.OrgCommand_Create{Create: &controlplanev1.CreateOrg{
			Id: "acme", Name: "Acme", OwnerSub: "u1",
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("create org: %v", err)
	}
	if _, err := orgRT.Handle(ctx, orgStream, &controlplanev1.OrgCommand{
		Kind: &controlplanev1.OrgCommand_UpsertMember{UpsertMember: &controlplanev1.UpsertMember{
			Sub: "u2", Role: "admin",
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("upsert member: %v", err)
	}
	if _, err := orgRT.Handle(ctx, orgStream, &controlplanev1.OrgCommand{
		Kind: &controlplanev1.OrgCommand_CreateProject{CreateProject: &controlplanev1.CreateProject{
			Id: "proj-1", Name: "Prod",
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := orgRT.Handle(ctx, orgStream, &controlplanev1.OrgCommand{
		Kind: &controlplanev1.OrgCommand_ArchiveProject{ArchiveProject: &controlplanev1.ArchiveProject{
			Id: "proj-1",
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("archive project: %v", err)
	}
	if _, err := orgRT.Handle(ctx, orgStream, &controlplanev1.OrgCommand{
		Kind: &controlplanev1.OrgCommand_RemoveMember{RemoveMember: &controlplanev1.RemoveMember{
			Sub: "u2",
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("remove member: %v", err)
	}

	// --- apikey: create, rotate, change allowlist, change limits, disable
	const hash1 = "1111111111111111111111111111111111111111111111111111111111111111"
	const hash2 = "2222222222222222222222222222222222222222222222222222222222222222"
	if _, err := keyRT.Handle(ctx, keyStream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_Create{Create: &controlplanev1.CreateKey{
			Id: "key-1", Org: "acme", Project: "proj-1", Name: "prod",
			Hash: hash1, Allow: []string{"gpt-4"}, RateLimitRpm: 60,
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("create key: %v", err)
	}
	if _, err := keyRT.Handle(ctx, keyStream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_Rotate{Rotate: &controlplanev1.RotateKey{NewHash: hash2}},
	}, es.Meta{}); err != nil {
		t.Fatalf("rotate key: %v", err)
	}
	if _, err := keyRT.Handle(ctx, keyStream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_SetAllowlist{SetAllowlist: &controlplanev1.SetAllowlist{
			Allow: []string{"gpt-4", "claude-3"},
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set allowlist: %v", err)
	}
	if _, err := keyRT.Handle(ctx, keyStream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_SetLimits{SetLimits: &controlplanev1.SetLimits{
			RateLimitRpm: 120,
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set limits: %v", err)
	}
	if _, err := keyRT.Handle(ctx, keyStream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_Disable{Disable: &controlplanev1.DisableKey{}},
	}, es.Meta{}); err != nil {
		t.Fatalf("disable key: %v", err)
	}

	// --- alias: set global, set org-scoped (with params), delete global
	if _, err := aliasRT.Handle(ctx, globalFast, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Set{Set: &controlplanev1.SetAlias{Target: "llama"}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set g_fast: %v", err)
	}
	if _, err := aliasRT.Handle(ctx, acmeSmart, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Set{Set: &controlplanev1.SetAlias{
			Target: "claude", Params: map[string]string{"temperature": "0-2"},
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set o_acme_smart: %v", err)
	}
	if _, err := aliasRT.Handle(ctx, globalFast, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Delete{Delete: &controlplanev1.DeleteAlias{}},
	}, es.Meta{}); err != nil {
		t.Fatalf("delete g_fast: %v", err)
	}

	// Feed every committed envelope, in global order, through the
	// handler in one batch — exercising apply()'s batch path, not just
	// one-at-a-time delivery.
	envelopes, err := store.ReadAll(ctx, 0, 1000)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(envelopes) == 0 {
		t.Fatal("no envelopes committed")
	}

	rs := NewMemReadStore()
	p := newSQLProjector(store, rs, newMemMarker())
	if err := p.apply(ctx, envelopes); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// --- assert org/members/projects
	orgRow, members, projects, err := rs.GetOrg(ctx, "acme")
	if err != nil {
		t.Fatalf("get org: %v", err)
	}
	if orgRow != (OrgRow{ID: "acme", Name: "Acme"}) {
		t.Fatalf("org row = %+v, want {acme Acme}", orgRow)
	}
	if want := []MemberRow{{Sub: "u1", Role: "owner"}}; !reflect.DeepEqual(members, want) {
		t.Fatalf("members = %+v, want %+v (u2 removed)", members, want)
	}
	if want := []ProjectRow{{ID: "proj-1", Org: "acme", Name: "Prod", Archived: true}}; !reflect.DeepEqual(projects, want) {
		t.Fatalf("projects = %+v, want %+v", projects, want)
	}

	orgs, err := rs.ListOrgs(ctx)
	if err != nil {
		t.Fatalf("list orgs: %v", err)
	}
	if want := []OrgRow{{ID: "acme", Name: "Acme"}}; !reflect.DeepEqual(orgs, want) {
		t.Fatalf("list orgs = %+v, want %+v", orgs, want)
	}

	// --- assert key row: survived rotation (still present, same id),
	// carries the post-rotation allowlist/limits, and shows Disabled.
	keys, err := rs.ListKeys(ctx, "acme")
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	wantKey := KeyRow{
		ID: "key-1", Org: "acme", Project: "proj-1", Name: "prod",
		Allow: []string{"gpt-4", "claude-3"}, RateLimitRPM: 120, Disabled: true,
	}
	if want := []KeyRow{wantKey}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("list keys = %+v, want %+v", keys, want)
	}

	// KeyRow structurally cannot carry a hash — verify via reflection that
	// no field on the type could ever leak one (binding requirement).
	kt := reflect.TypeOf(KeyRow{})
	for i := 0; i < kt.NumField(); i++ {
		name := kt.Field(i).Name
		if name == "Hash" || name == "CurrentHash" || name == "PreviousHash" {
			t.Fatalf("KeyRow has a hash-shaped field %q, want none", name)
		}
	}

	// --- assert aliases: _global/fast deleted, acme/smart present
	globalAliases, err := rs.ListAliases(ctx, "_global")
	if err != nil {
		t.Fatalf("list global aliases: %v", err)
	}
	if len(globalAliases) != 0 {
		t.Fatalf("global aliases = %+v, want none (g_fast was deleted)", globalAliases)
	}
	acmeAliases, err := rs.ListAliases(ctx, "acme")
	if err != nil {
		t.Fatalf("list acme aliases: %v", err)
	}
	wantAlias := []AliasRow{{Scope: "acme", Name: "smart", Target: "claude", Params: map[string]string{"temperature": "0-2"}}}
	if !reflect.DeepEqual(acmeAliases, wantAlias) {
		t.Fatalf("acme aliases = %+v, want %+v", acmeAliases, wantAlias)
	}
}

// --- Step 1: integration — sqlite store + relay + RunSQLProjector --------

func setupSQLProjTest(t *testing.T, relayCtx context.Context) (es.Store, jetstream.JetStream) {
	t.Helper()

	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	store, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	_, js := testutil.RunNATS(t)

	if err := EnsureControlStream(relayCtx, js); err != nil {
		t.Fatalf("ensure control stream: %v", err)
	}

	relayDone := make(chan error, 1)
	go func() { relayDone <- RunRelay(relayCtx, store, js) }()
	t.Cleanup(func() {
		select {
		case <-relayDone:
		case <-time.After(5 * time.Second):
			t.Fatal("relay did not stop")
		}
	})

	return store, js
}

func waitForSQL(t *testing.T, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if err != nil {
			lastErr = err
		} else if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met before timeout (last error: %v)", lastErr)
}

func TestRunSQLProjector_Integration(t *testing.T) {
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()
	store, js := setupSQLProjTest(t, relayCtx)

	orgRT := aggregate.NewRuntime(store, org.Decider, org.Codec())
	orgStream, err := es.NewStreamID(org.StreamType, "acme")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	if _, err := orgRT.Handle(context.Background(), orgStream, &controlplanev1.OrgCommand{
		Kind: &controlplanev1.OrgCommand_Create{Create: &controlplanev1.CreateOrg{
			Id: "acme", Name: "Acme", OwnerSub: "u1",
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("create org: %v", err)
	}

	rs := NewMemReadStore()
	projCtx, cancelProj := context.WithCancel(context.Background())
	defer cancelProj()
	projDone := make(chan error, 1)
	go func() { projDone <- RunSQLProjector(projCtx, store, js, rs) }()
	t.Cleanup(func() {
		cancelProj()
		select {
		case <-projDone:
		case <-time.After(5 * time.Second):
			t.Fatal("RunSQLProjector did not stop")
		}
	})

	waitForSQL(t, func() (bool, error) {
		orgs, err := rs.ListOrgs(context.Background())
		if err != nil {
			return false, err
		}
		return len(orgs) == 1, nil
	})

	orgs, err := rs.ListOrgs(context.Background())
	if err != nil {
		t.Fatalf("list orgs: %v", err)
	}
	if want := []OrgRow{{ID: "acme", Name: "Acme"}}; !reflect.DeepEqual(orgs, want) {
		t.Fatalf("orgs via live NATS delivery = %+v, want %+v", orgs, want)
	}

	// Now exercise a second aggregate type over the same live durable to
	// prove the single "evt.*.>"-filtered consumer really does route
	// every aggregate type onto the ReadStore, not just org.
	keyRT := aggregate.NewRuntime(store, apikey.Decider, apikey.Codec())
	keyStream, err := es.NewStreamID(apikey.StreamType, "key-1")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	const hash1 = "3333333333333333333333333333333333333333333333333333333333333333"
	if _, err := keyRT.Handle(context.Background(), keyStream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_Create{Create: &controlplanev1.CreateKey{
			Id: "key-1", Org: "acme", Project: "proj-1", Name: "prod",
			Hash: hash1, Allow: []string{"gpt-4"}, RateLimitRpm: 30,
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("create key: %v", err)
	}

	waitForSQL(t, func() (bool, error) {
		keys, err := rs.ListKeys(context.Background(), "acme")
		if err != nil {
			return false, err
		}
		return len(keys) == 1, nil
	})
	keys, err := rs.ListKeys(context.Background(), "acme")
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 1 || keys[0].ID != "key-1" || keys[0].RateLimitRPM != 30 {
		t.Fatalf("keys via live NATS delivery = %+v, want one key-1 row with RateLimitRPM=30", keys)
	}
}

// --- Step 1: env-gated Postgres integration test --------------------------

// TestPgReadStore_Integration exercises NewPgReadStore's UpsertOrg/GetOrg
// round-trip against a real Postgres database, named by CP_TEST_PG_DSN.
// It skips cleanly (not a failure) when that env var is unset — this
// suite never starts a Postgres container itself.
func TestPgReadStore_Integration(t *testing.T) {
	dsn := os.Getenv("CP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("CP_TEST_PG_DSN not set; skipping Postgres read-store integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	rs, err := NewPgReadStore(pool)
	if err != nil {
		t.Fatalf("NewPgReadStore: %v", err)
	}

	orgID := "pg-readmodel-test-" + t.Name()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM cp_org_members WHERE org = $1`, orgID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM cp_orgs WHERE id = $1`, orgID)
	})

	if err := rs.UpsertOrg(ctx, OrgRow{ID: orgID, Name: "PG Test Org"}); err != nil {
		t.Fatalf("UpsertOrg: %v", err)
	}
	if err := rs.UpsertMember(ctx, orgID, "u1", "owner"); err != nil {
		t.Fatalf("UpsertMember: %v", err)
	}

	row, members, projects, err := rs.GetOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("GetOrg: %v", err)
	}
	if row != (OrgRow{ID: orgID, Name: "PG Test Org"}) {
		t.Fatalf("GetOrg row = %+v, want {%s PG Test Org}", row, orgID)
	}
	if want := []MemberRow{{Sub: "u1", Role: "owner"}}; !reflect.DeepEqual(members, want) {
		t.Fatalf("GetOrg members = %+v, want %+v", members, want)
	}
	if len(projects) != 0 {
		t.Fatalf("GetOrg projects = %+v, want none", projects)
	}

	// Rename round-trips too.
	if err := rs.UpsertOrg(ctx, OrgRow{ID: orgID, Name: "Renamed"}); err != nil {
		t.Fatalf("UpsertOrg rename: %v", err)
	}
	row, _, _, err = rs.GetOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("GetOrg after rename: %v", err)
	}
	if row.Name != "Renamed" {
		t.Fatalf("GetOrg row after rename = %+v, want Name=Renamed", row)
	}

	// Unknown org id surfaces ErrOrgNotFound.
	if _, _, _, err := rs.GetOrg(ctx, orgID+"-does-not-exist"); !errors.Is(err, ErrOrgNotFound) {
		t.Fatalf("GetOrg unknown id error = %v, want ErrOrgNotFound", err)
	}

	// --- UpsertProject round-trip (review round 2 coverage extension)
	projectID := "proj-" + orgID
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM cp_projects WHERE id = $1`, projectID)
	})
	if err := rs.UpsertProject(ctx, ProjectRow{ID: projectID, Org: orgID, Name: "Prod"}); err != nil {
		t.Fatalf("UpsertProject: %v", err)
	}
	_, _, projects, err = rs.GetOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("GetOrg after UpsertProject: %v", err)
	}
	if want := []ProjectRow{{ID: projectID, Org: orgID, Name: "Prod"}}; !reflect.DeepEqual(projects, want) {
		t.Fatalf("GetOrg projects after UpsertProject = %+v, want %+v", projects, want)
	}
	if err := rs.UpsertProject(ctx, ProjectRow{ID: projectID, Org: orgID, Name: "Prod", Archived: true}); err != nil {
		t.Fatalf("UpsertProject archive: %v", err)
	}
	_, _, projects, err = rs.GetOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("GetOrg after archive: %v", err)
	}
	if len(projects) != 1 || !projects[0].Archived {
		t.Fatalf("GetOrg projects after archive = %+v, want Archived=true", projects)
	}

	// --- UpsertKey/ListKeys round-trip, INCLUDING an empty-allowlist key
	// (Critical review finding round 2: cp_api_keys.allow is TEXT[] NOT
	// NULL, and a nil Go []string used to be sent as a bare SQL NULL,
	// which the column rejects — this must not error the INSERT).
	keyID := "key-" + orgID
	emptyAllowKeyID := "key-empty-allow-" + orgID
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM cp_api_keys WHERE id IN ($1, $2)`, keyID, emptyAllowKeyID)
	})
	if err := rs.UpsertKey(ctx, KeyRow{
		ID: keyID, Org: orgID, Project: projectID, Name: "prod",
		Allow: []string{"gpt-4", "claude-3"}, RateLimitRPM: 60,
	}); err != nil {
		t.Fatalf("UpsertKey: %v", err)
	}
	if err := rs.UpsertKey(ctx, KeyRow{
		ID: emptyAllowKeyID, Org: orgID, Project: projectID, Name: "wide-open",
		Allow: nil, RateLimitRPM: 0,
	}); err != nil {
		t.Fatalf("UpsertKey with nil/empty Allow: %v", err)
	}

	keys, err := rs.ListKeys(ctx, orgID)
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("ListKeys = %+v, want 2 rows", keys)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].ID < keys[j].ID })
	// keys[0] is emptyAllowKeyID ("key-empty-allow-..." < "key-<orgID>"
	// lexicographically only if orgID doesn't start with "empty-allow-";
	// resolve by ID explicitly instead of relying on sort order.
	var normalKey, emptyAllowKey KeyRow
	for _, k := range keys {
		switch k.ID {
		case keyID:
			normalKey = k
		case emptyAllowKeyID:
			emptyAllowKey = k
		}
	}
	if len(normalKey.Allow) != 2 {
		t.Fatalf("normal key Allow = %+v, want 2 entries", normalKey.Allow)
	}
	if emptyAllowKey.Allow == nil {
		t.Fatal("empty-allowlist key's Allow = nil after Pg round-trip, want non-nil empty slice")
	}
	if len(emptyAllowKey.Allow) != 0 {
		t.Fatalf("empty-allowlist key's Allow = %+v, want empty", emptyAllowKey.Allow)
	}

	// --- UpsertAlias/ListAliases round-trip
	aliasScope := "alias-scope-" + orgID
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM cp_aliases WHERE scope = $1`, aliasScope)
	})
	if err := rs.UpsertAlias(ctx, AliasRow{
		Scope: aliasScope, Name: "smart", Target: "claude",
		Params: map[string]string{"temperature": "0-2"},
	}); err != nil {
		t.Fatalf("UpsertAlias: %v", err)
	}
	aliases, err := rs.ListAliases(ctx, aliasScope)
	if err != nil {
		t.Fatalf("ListAliases: %v", err)
	}
	wantAlias := []AliasRow{{Scope: aliasScope, Name: "smart", Target: "claude", Params: map[string]string{"temperature": "0-2"}}}
	if !reflect.DeepEqual(aliases, wantAlias) {
		t.Fatalf("ListAliases = %+v, want %+v", aliases, wantAlias)
	}
	if err := rs.DeleteAlias(ctx, aliasScope, "smart"); err != nil {
		t.Fatalf("DeleteAlias: %v", err)
	}
	aliases, err = rs.ListAliases(ctx, aliasScope)
	if err != nil {
		t.Fatalf("ListAliases after delete: %v", err)
	}
	if aliases == nil {
		t.Fatal("ListAliases after delete = nil, want non-nil empty slice")
	}
	if len(aliases) != 0 {
		t.Fatalf("ListAliases after delete = %+v, want empty", aliases)
	}
}

// --- Fix round 2, item 1: non-gated unit assertion that applyApiKeyEvent
// pins a non-nil Allow even for a key created with an empty/unset
// allowlist. This is the fast, Postgres-independent half of the Critical
// fix's coverage (the Postgres half lives in TestPgReadStore_Integration
// above): it exercises the actual projector handler, not just a
// hand-built KeyRow, so a regression in applyApiKeyEvent's own
// nonNilStrings coercion fails this test even without CP_TEST_PG_DSN set.
func TestSQLProjector_ApplyApiKeyEvent_EmptyAllowlistIsNonNil(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	keyRT := aggregate.NewRuntime(store, apikey.Decider, apikey.Codec())
	keyStream, err := es.NewStreamID(apikey.StreamType, "key-empty-allow")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	const hash1 = "6666666666666666666666666666666666666666666666666666666666666666"
	if _, err := keyRT.Handle(ctx, keyStream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_Create{Create: &controlplanev1.CreateKey{
			Id: "key-empty-allow", Org: "acme", Project: "proj-1", Name: "wide-open",
			Hash: hash1, RateLimitRpm: 10,
			// Allow intentionally left unset (nil).
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("create key: %v", err)
	}

	envelopes, err := store.ReadAll(ctx, 0, 10)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	rs := NewMemReadStore()
	p := newSQLProjector(store, rs, newMemMarker())
	if err := p.apply(ctx, envelopes); err != nil {
		t.Fatalf("apply: %v", err)
	}

	keys, err := rs.ListKeys(ctx, "acme")
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("list keys = %+v, want exactly one row", keys)
	}
	if keys[0].Allow == nil {
		t.Fatal("KeyRow.Allow = nil for an empty-allowlist key, want non-nil empty slice (applyApiKeyEvent contract)")
	}
	if len(keys[0].Allow) != 0 {
		t.Fatalf("KeyRow.Allow = %+v, want empty", keys[0].Allow)
	}
}

// --- Fix round 2, item 2: fail-stop test for RunSQLProjector -------------

// TestRunSQLProjector_FailStop ports TestKVProjectors_FailStop's shape
// (kvproj_test.go) to RunSQLProjector: an alias event with an
// unparseable stream id ("badid" has neither a "g_" nor "o_..._" prefix,
// so SplitAliasStreamID rejects it) must fail the whole run rather than
// silently skipping the bad event and letting the marker advance past
// it. The bad event is the only event in the store, so the failure
// surfaces during RunSQLProjector's initial replay, before live
// consumption ever starts.
func TestRunSQLProjector_FailStop(t *testing.T) {
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()
	store, js := setupSQLProjTest(t, relayCtx)

	rt := aggregate.NewRuntime(store, alias.Decider, alias.Codec())
	bad, err := es.NewStreamID(alias.StreamType, "badid")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	if _, err := rt.Handle(context.Background(), bad, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Set{Set: &controlplanev1.SetAlias{Target: "llama"}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set badid: %v", err)
	}

	rs := NewMemReadStore()
	runCtx, cancelRun := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRun()
	runErr := RunSQLProjector(runCtx, store, js, rs)

	if runErr == nil {
		t.Fatal("RunSQLProjector returned nil, want a fail-stop error for the unparseable stream id")
	}
	if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
		t.Fatalf("RunSQLProjector returned %v, want the actual SplitAliasStreamID failure (a timeout/cancellation here means fail-stop did not trigger)", runErr)
	}
	if !strings.Contains(runErr.Error(), "missing scope prefix separator") {
		t.Fatalf("RunSQLProjector error = %v, want it to mention the SplitAliasStreamID failure", runErr)
	}

	// The marker must not have advanced past the failed (only) event: no
	// proj-sql-admin key should exist in CP_MARKERS at all.
	kv, err := js.KeyValue(context.Background(), bucketMarkers)
	if err != nil {
		t.Fatalf("bind CP_MARKERS: %v", err)
	}
	if _, err := kv.Get(context.Background(), durableProjSQL); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("proj-sql-admin marker present after fail-stop on the only event (err=%v), want no marker saved at all", err)
	}

	// And the ReadStore must not contain any row derived from the bad
	// event.
	if orgs, _ := rs.ListOrgs(context.Background()); len(orgs) != 0 {
		t.Fatalf("orgs after fail-stop = %+v, want none", orgs)
	}
}

// --- Fix round 2, item 3: redelivery idempotency regression test --------

// countingReadStore wraps a ReadStore and counts every write-method call,
// so a test can assert the sticky idempotency skip (apply()'s
// "GlobalPosition <= p.pos" check) actually prevented a second round of
// writes on redelivery — not merely that whatever writes did happen were
// themselves idempotent (which, given this projector's Load-then-upsert
// design, would often be true anyway and so wouldn't catch a regression
// in the skip check itself).
type countingReadStore struct {
	ReadStore
	mu     sync.Mutex
	writes int
}

func newCountingReadStore(rs ReadStore) *countingReadStore {
	return &countingReadStore{ReadStore: rs}
}

func (c *countingReadStore) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

func (c *countingReadStore) bump() {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
}

func (c *countingReadStore) UpsertOrg(ctx context.Context, row OrgRow) error {
	c.bump()
	return c.ReadStore.UpsertOrg(ctx, row)
}

func (c *countingReadStore) UpsertMember(ctx context.Context, org, sub, role string) error {
	c.bump()
	return c.ReadStore.UpsertMember(ctx, org, sub, role)
}

func (c *countingReadStore) RemoveMember(ctx context.Context, org, sub string) error {
	c.bump()
	return c.ReadStore.RemoveMember(ctx, org, sub)
}

func (c *countingReadStore) UpsertProject(ctx context.Context, row ProjectRow) error {
	c.bump()
	return c.ReadStore.UpsertProject(ctx, row)
}

func (c *countingReadStore) UpsertKey(ctx context.Context, row KeyRow) error {
	c.bump()
	return c.ReadStore.UpsertKey(ctx, row)
}

func (c *countingReadStore) UpsertAlias(ctx context.Context, row AliasRow) error {
	c.bump()
	return c.ReadStore.UpsertAlias(ctx, row)
}

func (c *countingReadStore) DeleteAlias(ctx context.Context, scope, name string) error {
	c.bump()
	return c.ReadStore.DeleteAlias(ctx, scope, name)
}

func TestSQLProjector_Handler_RedeliveryIsIdempotent(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	orgRT := aggregate.NewRuntime(store, org.Decider, org.Codec())
	keyRT := aggregate.NewRuntime(store, apikey.Decider, apikey.Codec())
	aliasRT := aggregate.NewRuntime(store, alias.Decider, alias.Codec())

	orgStream, err := es.NewStreamID(org.StreamType, "acme")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	keyStream, err := es.NewStreamID(apikey.StreamType, "key-1")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	aliasStream, err := es.NewStreamID(alias.StreamType, "g_fast")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}

	if _, err := orgRT.Handle(ctx, orgStream, &controlplanev1.OrgCommand{
		Kind: &controlplanev1.OrgCommand_Create{Create: &controlplanev1.CreateOrg{
			Id: "acme", Name: "Acme", OwnerSub: "u1",
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("create org: %v", err)
	}
	const hash1 = "7777777777777777777777777777777777777777777777777777777777777777"
	if _, err := keyRT.Handle(ctx, keyStream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_Create{Create: &controlplanev1.CreateKey{
			Id: "key-1", Org: "acme", Project: "proj-1", Name: "prod",
			Hash: hash1, Allow: []string{"gpt-4"}, RateLimitRpm: 60,
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("create key: %v", err)
	}
	if _, err := aliasRT.Handle(ctx, aliasStream, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Set{Set: &controlplanev1.SetAlias{Target: "llama"}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set alias: %v", err)
	}

	envelopes, err := store.ReadAll(ctx, 0, 1000)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(envelopes) == 0 {
		t.Fatal("no envelopes committed")
	}

	counting := newCountingReadStore(NewMemReadStore())
	p := newSQLProjector(store, counting, newMemMarker())

	if err := p.apply(ctx, envelopes); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	firstWrites := counting.count()
	if firstWrites == 0 {
		t.Fatal("expected write calls on the first apply, got 0")
	}

	orgsFirst, err := counting.ListOrgs(ctx)
	if err != nil {
		t.Fatalf("ListOrgs after first apply: %v", err)
	}
	keysFirst, err := counting.ListKeys(ctx, "acme")
	if err != nil {
		t.Fatalf("ListKeys after first apply: %v", err)
	}
	aliasesFirst, err := counting.ListAliases(ctx, "_global")
	if err != nil {
		t.Fatalf("ListAliases after first apply: %v", err)
	}

	// Redeliver the exact same batch (at-least-once semantics: this is
	// what a NAK'd-and-retried or restarted-mid-batch delivery looks
	// like). Every envelope's GlobalPosition is now <= p.pos, so apply()
	// must skip all of them without touching the ReadStore again.
	if err := p.apply(ctx, envelopes); err != nil {
		t.Fatalf("second (redelivered) apply: %v", err)
	}

	if got := counting.count(); got != firstWrites {
		t.Fatalf("write calls after redelivery = %d, want unchanged %d (idempotency skip should have prevented any new ReadStore writes)", got, firstWrites)
	}

	orgsSecond, err := counting.ListOrgs(ctx)
	if err != nil {
		t.Fatalf("ListOrgs after redelivery: %v", err)
	}
	keysSecond, err := counting.ListKeys(ctx, "acme")
	if err != nil {
		t.Fatalf("ListKeys after redelivery: %v", err)
	}
	aliasesSecond, err := counting.ListAliases(ctx, "_global")
	if err != nil {
		t.Fatalf("ListAliases after redelivery: %v", err)
	}

	if !reflect.DeepEqual(orgsFirst, orgsSecond) {
		t.Fatalf("orgs changed after redelivery: before=%+v after=%+v", orgsFirst, orgsSecond)
	}
	if !reflect.DeepEqual(keysFirst, keysSecond) {
		t.Fatalf("keys changed after redelivery: before=%+v after=%+v", keysFirst, keysSecond)
	}
	if !reflect.DeepEqual(aliasesFirst, aliasesSecond) {
		t.Fatalf("aliases changed after redelivery: before=%+v after=%+v", aliasesFirst, aliasesSecond)
	}
}

// --- Fix round 2, item 4: nil-vs-empty parity between both ReadStore
// implementations -----------------------------------------------------

// TestReadStore_EmptyResultsAreNonNil pins the nil-vs-empty-slice parity
// ruling (fold-in minor, review round 2): Task 10's admin API will
// JSON-serialize these results directly, and a bare `null` where an
// empty JSON array `[]` is expected is a worse API shape — so every
// ReadStore query method must return a non-nil (possibly zero-length)
// slice for an empty result, never nil. Checked against MemReadStore
// unconditionally, and against PgReadStore too when CP_TEST_PG_DSN is
// set (both subtests exercise the same four query methods so a
// divergence between the two implementations is caught directly, not
// just each implementation's own self-consistency).
func TestReadStore_EmptyResultsAreNonNil(t *testing.T) {
	ctx := context.Background()

	t.Run("mem", func(t *testing.T) {
		rs := NewMemReadStore()

		orgs, err := rs.ListOrgs(ctx)
		if err != nil {
			t.Fatalf("ListOrgs: %v", err)
		}
		if orgs == nil {
			t.Fatal("ListOrgs on an empty store = nil, want non-nil empty slice")
		}

		if err := rs.UpsertOrg(ctx, OrgRow{ID: "empty-org", Name: "Empty"}); err != nil {
			t.Fatalf("UpsertOrg: %v", err)
		}
		_, members, projects, err := rs.GetOrg(ctx, "empty-org")
		if err != nil {
			t.Fatalf("GetOrg: %v", err)
		}
		if members == nil {
			t.Fatal("GetOrg members = nil for an org with none, want non-nil empty slice")
		}
		if projects == nil {
			t.Fatal("GetOrg projects = nil for an org with none, want non-nil empty slice")
		}

		keys, err := rs.ListKeys(ctx, "empty-org")
		if err != nil {
			t.Fatalf("ListKeys: %v", err)
		}
		if keys == nil {
			t.Fatal("ListKeys for an org with no keys = nil, want non-nil empty slice")
		}

		aliases, err := rs.ListAliases(ctx, "_global")
		if err != nil {
			t.Fatalf("ListAliases: %v", err)
		}
		if aliases == nil {
			t.Fatal("ListAliases for an empty scope = nil, want non-nil empty slice")
		}
	})

	t.Run("pg", func(t *testing.T) {
		dsn := os.Getenv("CP_TEST_PG_DSN")
		if dsn == "" {
			t.Skip("CP_TEST_PG_DSN not set; skipping Postgres half of the nil-vs-empty parity check")
		}

		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("pgxpool.New: %v", err)
		}
		t.Cleanup(pool.Close)

		rs, err := NewPgReadStore(pool)
		if err != nil {
			t.Fatalf("NewPgReadStore: %v", err)
		}

		orgID := "pg-empty-parity-" + t.Name()
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DELETE FROM cp_org_members WHERE org = $1`, orgID)
			_, _ = pool.Exec(context.Background(), `DELETE FROM cp_orgs WHERE id = $1`, orgID)
		})

		if err := rs.UpsertOrg(ctx, OrgRow{ID: orgID, Name: "Empty"}); err != nil {
			t.Fatalf("UpsertOrg: %v", err)
		}
		_, members, projects, err := rs.GetOrg(ctx, orgID)
		if err != nil {
			t.Fatalf("GetOrg: %v", err)
		}
		if members == nil {
			t.Fatal("GetOrg members = nil for an org with none, want non-nil empty slice")
		}
		if projects == nil {
			t.Fatal("GetOrg projects = nil for an org with none, want non-nil empty slice")
		}

		keys, err := rs.ListKeys(ctx, orgID)
		if err != nil {
			t.Fatalf("ListKeys: %v", err)
		}
		if keys == nil {
			t.Fatal("ListKeys for an org with no keys = nil, want non-nil empty slice")
		}

		aliases, err := rs.ListAliases(ctx, orgID+"-scope-with-nothing")
		if err != nil {
			t.Fatalf("ListAliases: %v", err)
		}
		if aliases == nil {
			t.Fatal("ListAliases for an empty scope = nil, want non-nil empty slice")
		}
	})
}
