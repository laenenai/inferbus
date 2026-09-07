package controlplane

import (
	"context"
	"errors"
	"os"
	"reflect"
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
}
