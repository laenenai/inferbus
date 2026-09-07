package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laenenai/inferbus/internal/controlplane/alias"
	"github.com/laenenai/inferbus/internal/controlplane/apikey"
	"github.com/laenenai/inferbus/internal/controlplane/org"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
)

// --- shared test plumbing ---------------------------------------------------

// adminFixture is a full live stack: sqlite es.Store + embedded NATS +
// RunRelay + RunSQLProjector feeding a MemReadStore, exactly as the task
// brief specifies ("httptest over Admin.Routes(), sqlite store,
// MemReadStore fed live by RunSQLProjector running live").
type adminFixture struct {
	admin   *Admin
	rs      *MemReadStore
	orgRT   OrgRuntime
	keyRT   KeyRuntime
	aliasRT AliasRuntime
}

func newAdminFixture(t *testing.T, cfg Config, verifier TokenVerifier, resync func(context.Context) error, healthy func() bool) *adminFixture {
	t.Helper()

	relayCtx, cancelRelay := context.WithCancel(context.Background())
	store, js := setupSQLProjTest(t, relayCtx)
	// t.Cleanup runs LIFO: registering cancelRelay AFTER
	// setupSQLProjTest's own cleanup (which blocks waiting for the relay
	// to actually stop) means cancelRelay fires FIRST, so the relay has
	// already been told to stop by the time that wait begins.
	t.Cleanup(cancelRelay)

	rs := NewMemReadStore()
	projCtx, cancelProj := context.WithCancel(context.Background())
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

	orgRT := aggregate.NewRuntime(store, org.Decider, org.Codec())
	keyRT := aggregate.NewRuntime(store, apikey.Decider, apikey.Codec())
	aliasRT := aggregate.NewRuntime(store, alias.Decider, alias.Codec())

	if resync == nil {
		resync = func(context.Context) error { return nil }
	}
	admin := NewAdmin(NewAuthenticator(cfg, verifier), rs, orgRT, keyRT, aliasRT, resync, healthy)
	return &adminFixture{admin: admin, rs: rs, orgRT: orgRT, keyRT: keyRT, aliasRT: aliasRT}
}

// doRequest fires one request through mux and returns the recorded
// response. body, if non-nil, is JSON-marshaled and sent with a matching
// Content-Type.
func doRequest(t *testing.T, mux http.Handler, method, path, bearer string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func decodeErrorType(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error body: %v (body=%s)", err, rec.Body.String())
	}
	return body.Error.Type
}

// --- Step 1 required cases --------------------------------------------------

func TestAdmin_Unauthenticated401(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodGet, "/admin/v1/orgs", "", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypeAuthn {
		t.Fatalf("error.type = %q, want %q", typ, errTypeAuthn)
	}
}

func TestAdmin_CreateOrg_BootstrapRequiresOwnerSub(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	// Bootstrap has no real subject of its own: owner_sub is required.
	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{"id": "acme", "name": "Acme"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing owner_sub: status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}

	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "Acme", "owner_sub": "dev-owner",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}

	waitForSQL(t, func() (bool, error) {
		_, _, _, err := f.rs.GetOrg(context.Background(), "acme")
		return err == nil, nil
	})

	// GET org (as the still-platform-admin bootstrap identity) shows the
	// creator-less org with the supplied owner_sub as a member.
	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/orgs/acme", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get org: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var got orgResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	found := false
	for _, m := range got.Members {
		if m.Sub == "dev-owner" && m.Role == "owner" {
			found = true
		}
	}
	if !found {
		t.Fatalf("members = %+v, want dev-owner as owner", got.Members)
	}
}

func TestAdmin_CreateOrg_OIDCDefaultsOwnerSubToCaller(t *testing.T) {
	cfg := Config{PlatformAdmins: []string{"admin-oidc-sub"}}
	verifier := &fakeVerifier{subs: map[string]string{"jwt-for-admin": "admin-oidc-sub"}}
	f := newAdminFixture(t, cfg, verifier, nil, nil)
	mux := f.admin.Routes()

	// No owner_sub in the body: an OIDC caller's own sub is the default.
	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "jwt-for-admin", map[string]any{"id": "beta", "name": "Beta"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var got orgResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	found := false
	for _, m := range got.Members {
		if m.Sub == "admin-oidc-sub" && m.Role == "owner" {
			found = true
		}
	}
	if !found {
		t.Fatalf("members = %+v, want admin-oidc-sub as owner", got.Members)
	}
}

// TestAdmin_RequiresNonEmptyName is finding M1: createOrg, renameOrg, and
// createProject must all 400 on a blank (empty or whitespace-only) name
// rather than silently accepting one and letting it flow through to the
// event log.
func TestAdmin_RequiresNonEmptyName(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "  ", "owner_sub": "dev",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("createOrg blank name: status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypeInvalidRequest {
		t.Fatalf("createOrg blank name: error.type = %q, want %q", typ, errTypeInvalidRequest)
	}

	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "Acme", "owner_sub": "dev",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}

	rec = doRequest(t, mux, http.MethodPatch, "/admin/v1/orgs/acme", "s3cret", map[string]any{"name": ""})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("renameOrg blank name: status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypeInvalidRequest {
		t.Fatalf("renameOrg blank name: error.type = %q, want %q", typ, errTypeInvalidRequest)
	}

	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/orgs/acme/projects", "s3cret", map[string]any{
		"id": "proj-1", "name": "   ",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("createProject blank name: status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypeInvalidRequest {
		t.Fatalf("createProject blank name: error.type = %q, want %q", typ, errTypeInvalidRequest)
	}
}

func TestAdmin_CreateKey_PlaintextOnce_ListHidesHashAndPlaintext(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "Acme", "owner_sub": "dev",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: %d %s", rec.Code, rec.Body.String())
	}
	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/orgs/acme/projects", "s3cret", map[string]any{
		"id": "proj-1", "name": "Prod",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create project: %d %s", rec.Code, rec.Body.String())
	}

	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/keys", "s3cret", map[string]any{
		"org": "acme", "project": "proj-1", "name": "prod-key",
		"allow": []string{"gpt-4"}, "rate_limit_rpm": 60,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create key: %d %s", rec.Code, rec.Body.String())
	}
	var created keyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.HasPrefix(created.Key, "ib_live_") {
		t.Fatalf("key = %q, want ib_live_ prefix", created.Key)
	}
	if created.ID == "" {
		t.Fatalf("id must not be empty")
	}
	hash := HashKey(created.Key)

	waitForSQL(t, func() (bool, error) {
		rows, err := f.rs.ListKeys(context.Background(), "acme")
		if err != nil {
			return false, err
		}
		return len(rows) == 1, nil
	})

	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/keys?org=acme", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list keys: %d %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, created.Key) {
		t.Fatalf("list keys body leaked plaintext: %s", body)
	}
	if strings.Contains(body, hash) {
		t.Fatalf("list keys body leaked hash: %s", body)
	}
	if strings.Contains(body, `"key"`) {
		t.Fatalf(`list keys body must never contain a "key" field: %s`, body)
	}
	var listed []keyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("listed = %+v, want one row matching created id %q", listed, created.ID)
	}
}

// TestAdmin_ListKeys_SurfacesMonthlyTokenBudget is finding I5: a key's
// monthly_token_budget (set at creation, and later changed via
// PUT .../limits) must be visible through GET /admin/v1/keys — the admin
// read model previously dropped it entirely (KeyRow had no such field), so
// an admin auditing a key's limits couldn't see its budget at all.
func TestAdmin_ListKeys_SurfacesMonthlyTokenBudget(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "Acme", "owner_sub": "dev",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: %d %s", rec.Code, rec.Body.String())
	}
	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/orgs/acme/projects", "s3cret", map[string]any{
		"id": "proj-1", "name": "Prod",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create project: %d %s", rec.Code, rec.Body.String())
	}

	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/keys", "s3cret", map[string]any{
		"org": "acme", "project": "proj-1", "name": "prod-key",
		"allow": []string{"gpt-4"}, "rate_limit_rpm": 60, "monthly_token_budget": 1_000_000,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create key: %d %s", rec.Code, rec.Body.String())
	}
	var created keyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if created.MonthlyTokenBudget != 1_000_000 {
		t.Fatalf("createKey response MonthlyTokenBudget = %d, want 1000000", created.MonthlyTokenBudget)
	}

	waitForSQL(t, func() (bool, error) {
		rows, err := f.rs.ListKeys(context.Background(), "acme")
		if err != nil {
			return false, err
		}
		return len(rows) == 1, nil
	})

	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/keys?org=acme", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list keys: %d %s", rec.Code, rec.Body.String())
	}
	var listed []keyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(listed) != 1 || listed[0].MonthlyTokenBudget != 1_000_000 {
		t.Fatalf("listed keys after create = %+v, want one row with MonthlyTokenBudget=1000000", listed)
	}

	// Changing the limit is reflected too.
	rec = doRequest(t, mux, http.MethodPut, "/admin/v1/keys/"+created.ID+"/limits", "s3cret", map[string]any{
		"rate_limit_rpm": 60, "monthly_token_budget": 2_000_000,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("set limits: %d %s", rec.Code, rec.Body.String())
	}

	waitForSQL(t, func() (bool, error) {
		rows, err := f.rs.ListKeys(context.Background(), "acme")
		if err != nil {
			return false, err
		}
		return len(rows) == 1 && rows[0].MonthlyTokenBudget == 2_000_000, nil
	})

	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/keys?org=acme", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list keys after set limits: %d %s", rec.Code, rec.Body.String())
	}
	listed = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("unmarshal list after set limits: %v", err)
	}
	if len(listed) != 1 || listed[0].MonthlyTokenBudget != 2_000_000 {
		t.Fatalf("listed keys after set limits = %+v, want one row with MonthlyTokenBudget=2000000", listed)
	}
}

func TestAdmin_NonMemberForbidden_ViewerForbiddenOnManage(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	verifier := &fakeVerifier{subs: map[string]string{
		"jwt-stranger": "stranger",
		"jwt-viewer":   "viewer-1",
	}}
	f := newAdminFixture(t, cfg, verifier, nil, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "Acme", "owner_sub": "dev",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: %d %s", rec.Code, rec.Body.String())
	}
	rec = doRequest(t, mux, http.MethodPut, "/admin/v1/orgs/acme/members/viewer-1", "s3cret", map[string]any{"role": "viewer"})
	if rec.Code != http.StatusOK {
		t.Fatalf("upsert member: %d %s", rec.Code, rec.Body.String())
	}

	waitForSQL(t, func() (bool, error) {
		_, members, _, err := f.rs.GetOrg(context.Background(), "acme")
		if err != nil {
			return false, err
		}
		for _, m := range members {
			if m.Sub == "viewer-1" {
				return true, nil
			}
		}
		return false, nil
	})

	// A non-member gets 403 on an org-scoped route (even a read one).
	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/orgs/acme", "jwt-stranger", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("stranger GET org: status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}

	// A viewer gets 403 on a manage_org route...
	rec = doRequest(t, mux, http.MethodPatch, "/admin/v1/orgs/acme", "jwt-viewer", map[string]any{"name": "Acme Renamed"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("viewer rename: status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
	// ...but can still read.
	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/orgs/acme", "jwt-viewer", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("viewer GET org: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
}

func TestAdmin_AliasGlobalScope_MutationsRequirePlatformAdmin(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret", PlatformAdmins: []string{"admin-sub"}}
	verifier := &fakeVerifier{subs: map[string]string{
		"jwt-nonadmin": "someone",
		"jwt-admin":    "admin-sub",
	}}
	f := newAdminFixture(t, cfg, verifier, nil, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodPut, "/admin/v1/aliases/_global/fast", "jwt-nonadmin", map[string]any{"target": "gpt-4"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-admin PUT _global alias: status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}

	rec = doRequest(t, mux, http.MethodDelete, "/admin/v1/aliases/_global/fast", "jwt-nonadmin", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-admin DELETE _global alias: status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}

	rec = doRequest(t, mux, http.MethodPut, "/admin/v1/aliases/_global/fast", "jwt-admin", map[string]any{"target": "gpt-4"})
	if rec.Code != http.StatusOK {
		t.Fatalf("platform admin (OIDC) PUT _global alias: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	rec = doRequest(t, mux, http.MethodPut, "/admin/v1/aliases/_global/other", "s3cret", map[string]any{"target": "gpt-4"})
	if rec.Code != http.StatusOK {
		t.Fatalf("bootstrap PUT _global alias: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
}

// TestAdmin_AliasGlobalScope_ReadsNeedOnlyRead is review finding I4(d): a
// non-platform-admin, non-org-member-of-anything-relevant caller may still
// GET the "_global" alias scope — global aliases are non-secret, system-
// wide routing config, not an org's private data, so reads only need
// "read" (which every authenticated caller trivially has), while mutations
// (covered above) still require platform admin outright.
func TestAdmin_AliasGlobalScope_ReadsNeedOnlyRead(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret", PlatformAdmins: []string{"admin-sub"}}
	verifier := &fakeVerifier{subs: map[string]string{
		"jwt-admin":  "admin-sub",
		"jwt-member": "org-member-1",
	}}
	f := newAdminFixture(t, cfg, verifier, nil, nil)
	mux := f.admin.Routes()

	// Seed a global alias as platform admin.
	rec := doRequest(t, mux, http.MethodPut, "/admin/v1/aliases/_global/fast", "jwt-admin", map[string]any{"target": "gpt-4"})
	if rec.Code != http.StatusOK {
		t.Fatalf("seed _global alias: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	waitForSQL(t, func() (bool, error) {
		rows, err := f.rs.ListAliases(context.Background(), globalScope)
		if err != nil {
			return false, err
		}
		return len(rows) == 1, nil
	})

	// An ordinary org member (no platform admin, not a member of any org
	// relevant to "_global" — there is none) can still read it.
	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/aliases/_global", "jwt-member", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("non-admin GET _global aliases: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var aliases []aliasResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &aliases); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	found := false
	for _, a := range aliases {
		if a.Name == "fast" && a.Target == "gpt-4" {
			found = true
		}
	}
	if !found {
		t.Fatalf("aliases = %+v, want fast->gpt-4", aliases)
	}
}

// flakyStore wraps a real es.Store and makes its Append return
// es.ErrConflict a configurable number of times before delegating for
// real — a deterministic "double-write scripted through the runtime" that
// forces the exact es.ErrConflict path admin.go's handleWithRetry exists
// for, without depending on real goroutine-timing luck.
type flakyStore struct {
	es.Store
	mu        sync.Mutex
	conflicts int
}

func (f *flakyStore) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	f.mu.Lock()
	if f.conflicts > 0 {
		f.conflicts--
		f.mu.Unlock()
		return es.AppendResult{}, es.ErrConflict
	}
	f.mu.Unlock()
	return f.Store.Append(ctx, p)
}

func (f *flakyStore) setConflicts(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conflicts = n
}

func TestAdmin_ConflictRetryOnce_ThenFailOn409(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	real, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { real.Close() })

	flaky := &flakyStore{Store: real}
	orgRT := aggregate.NewRuntime(flaky, org.Decider, org.Codec())
	keyRT := aggregate.NewRuntime(flaky, apikey.Decider, apikey.Codec())
	aliasRT := aggregate.NewRuntime(flaky, alias.Decider, alias.Codec())

	cfg := Config{BootstrapToken: "s3cret"}
	// Bootstrap is PlatformAdmin, so authorizeOrg short-circuits without
	// ever consulting a ReadStore — this test needs none of the
	// SQL-projector machinery, just the retry-then-409 dispatch path.
	admin := NewAdmin(NewAuthenticator(cfg, &fakeVerifier{}), NewMemReadStore(), orgRT, keyRT, aliasRT, func(context.Context) error { return nil }, nil)
	mux := admin.Routes()

	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "Acme", "owner_sub": "dev",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: %d %s", rec.Code, rec.Body.String())
	}

	// One conflict: handleWithRetry's single retry absorbs it -> 200.
	flaky.setConflicts(1)
	rec = doRequest(t, mux, http.MethodPatch, "/admin/v1/orgs/acme", "s3cret", map[string]any{"name": "Acme Renamed Once"})
	if rec.Code != http.StatusOK {
		t.Fatalf("rename with 1 injected conflict: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	// Two conflicts: the retry is exhausted -> 409 conflict_error.
	flaky.setConflicts(2)
	rec = doRequest(t, mux, http.MethodPatch, "/admin/v1/orgs/acme", "s3cret", map[string]any{"name": "Acme Renamed Twice"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("rename with 2 injected conflicts: status = %d, want 409; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypeConflict {
		t.Fatalf("error.type = %q, want %q", typ, errTypeConflict)
	}
}

func TestAdmin_Resync_PlatformAdminOnly(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	verifier := &fakeVerifier{subs: map[string]string{"jwt-nonadmin": "someone"}}
	var called bool
	resync := func(context.Context) error { called = true; return nil }
	f := newAdminFixture(t, cfg, verifier, resync, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/projections/resync", "jwt-nonadmin", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-admin resync: status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
	if called {
		t.Fatalf("resync must not be invoked for a non-admin caller")
	}

	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/projections/resync", "s3cret", nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("platform-admin resync: status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if !called {
		t.Fatalf("resync must be invoked for a platform-admin caller")
	}
}

// TestAdmin_UnmatchedRouteReturnsJSON is the folded-in "404/405 fallback"
// minor: an unknown path and a known path with the wrong method must both
// get the OpenAI-style JSON error body, not net/http's default plain text.
func TestAdmin_UnmatchedRouteReturnsJSON(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	for _, c := range []struct {
		name   string
		method string
		path   string
	}{
		{"unknown path", http.MethodGet, "/admin/v1/nonexistent"},
		{"known path wrong method", http.MethodDelete, "/admin/v1/orgs"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := doRequest(t, mux, c.method, c.path, "s3cret", nil)
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json; body=%s", ct, rec.Body.String())
			}
			if typ := decodeErrorType(t, rec); typ != errTypeNotFound {
				t.Fatalf("error.type = %q, want %q; status=%d", typ, errTypeNotFound, rec.Code)
			}
		})
	}
}

// --- review round: I4 authz tests -------------------------------------------

// TestAdmin_OrgAdminCannotMutateAnotherOrgsKey is I4(a): an admin of org A
// has no standing whatsoever over org B's keys — every mutation route must
// refuse (403, or 404 if the SQL projector hasn't caught org B up yet —
// either is acceptable per the review note, but never success).
func TestAdmin_OrgAdminCannotMutateAnotherOrgsKey(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	verifier := &fakeVerifier{subs: map[string]string{"jwt-admin-a": "admin-a"}}
	f := newAdminFixture(t, cfg, verifier, nil, nil)
	mux := f.admin.Routes()

	// Org A, with admin-a as an "admin" member.
	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "orga", "name": "Org A", "owner_sub": "owner-a",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org A: %d %s", rec.Code, rec.Body.String())
	}
	rec = doRequest(t, mux, http.MethodPut, "/admin/v1/orgs/orga/members/admin-a", "s3cret", map[string]any{"role": "admin"})
	if rec.Code != http.StatusOK {
		t.Fatalf("upsert admin-a into org A: %d %s", rec.Code, rec.Body.String())
	}

	// Org B, with its own project and key, admin-a has no role in it.
	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "orgb", "name": "Org B", "owner_sub": "owner-b",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org B: %d %s", rec.Code, rec.Body.String())
	}
	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/orgs/orgb/projects", "s3cret", map[string]any{"id": "proj-b", "name": "Proj B"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create project in org B: %d %s", rec.Code, rec.Body.String())
	}
	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/keys", "s3cret", map[string]any{
		"org": "orgb", "project": "proj-b", "name": "secret-key",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create key in org B: %d %s", rec.Code, rec.Body.String())
	}
	var key keyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &key); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Let both orgs' membership catch up so a denial is a genuine 403
	// (role lookup succeeded, role was insufficient), not incidentally a
	// 404 from projector lag — both are acceptable per the review note,
	// but this makes the assertion meaningful either way.
	waitForSQL(t, func() (bool, error) {
		_, members, _, err := f.rs.GetOrg(context.Background(), "orga")
		if err != nil {
			return false, err
		}
		for _, m := range members {
			if m.Sub == "admin-a" {
				return true, nil
			}
		}
		return false, nil
	})
	waitForSQL(t, func() (bool, error) {
		_, _, _, err := f.rs.GetOrg(context.Background(), "orgb")
		return err == nil, nil
	})

	cases := []struct {
		name   string
		method string
		path   string
		body   any
	}{
		{"rotate", http.MethodPost, "/admin/v1/keys/" + key.ID + "/rotate", nil},
		{"disable", http.MethodPost, "/admin/v1/keys/" + key.ID + "/disable", nil},
		{"allowlist", http.MethodPut, "/admin/v1/keys/" + key.ID + "/allowlist", map[string]any{"allow": []string{"*"}}},
		{"limits", http.MethodPut, "/admin/v1/keys/" + key.ID + "/limits", map[string]any{"rate_limit_rpm": 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := doRequest(t, mux, c.method, c.path, "jwt-admin-a", c.body)
			if rec.Code != http.StatusForbidden && rec.Code != http.StatusNotFound {
				t.Fatalf("org A admin %s org B's key: status = %d, want 403 or 404 (never success); body=%s", c.name, rec.Code, rec.Body.String())
			}
		})
	}
}

// TestAdmin_RotateKey_PlaintextOnceNeverHash is I4(b).
func TestAdmin_RotateKey_PlaintextOnceNeverHash(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "Acme", "owner_sub": "dev",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: %d %s", rec.Code, rec.Body.String())
	}
	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/orgs/acme/projects", "s3cret", map[string]any{"id": "proj-1", "name": "Prod"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create project: %d %s", rec.Code, rec.Body.String())
	}
	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/keys", "s3cret", map[string]any{
		"org": "acme", "project": "proj-1", "name": "prod-key",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create key: %d %s", rec.Code, rec.Body.String())
	}
	var created keyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	originalHash := HashKey(created.Key)

	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/keys/"+created.ID+"/rotate", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("rotate key: %d %s", rec.Code, rec.Body.String())
	}
	var rotated keyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &rotated); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.HasPrefix(rotated.Key, "ib_live_") {
		t.Fatalf("rotated key = %q, want ib_live_ prefix", rotated.Key)
	}
	if rotated.Key == created.Key {
		t.Fatalf("rotate returned the same plaintext as create")
	}
	newHash := HashKey(rotated.Key)

	body := rec.Body.String()
	if strings.Contains(body, originalHash) || strings.Contains(body, newHash) {
		t.Fatalf("rotate response leaked a hash: %s", body)
	}

	// A second rotate response must not still be carrying the first
	// rotation's plaintext either (exactly once, not "sticky").
	rec = doRequest(t, mux, http.MethodPost, "/admin/v1/keys/"+created.ID+"/rotate", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("second rotate: %d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), rotated.Key) {
		t.Fatalf("second rotate response leaked the first rotation's plaintext: %s", rec.Body.String())
	}
}

// TestAdmin_ValidationRejectsBadSlug is I4(c).
func TestAdmin_ValidationRejectsBadSlug(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	t.Run("bad org id", func(t *testing.T) {
		rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
			"id": "Not A Slug!", "name": "Bad", "owner_sub": "dev",
		})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("bad org id: status = %d, want 400; body=%s", rec.Code, rec.Body.String())
		}
		if typ := decodeErrorType(t, rec); typ != errTypeInvalidRequest {
			t.Fatalf("error.type = %q, want %q", typ, errTypeInvalidRequest)
		}
	})

	t.Run("bad alias name", func(t *testing.T) {
		rec := doRequest(t, mux, http.MethodPut, "/admin/v1/aliases/_global/Not_A_Slug", "s3cret", map[string]any{"target": "gpt-4"})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("bad alias name: status = %d, want 400; body=%s", rec.Code, rec.Body.String())
		}
		if typ := decodeErrorType(t, rec); typ != errTypeInvalidRequest {
			t.Fatalf("error.type = %q, want %q", typ, errTypeInvalidRequest)
		}
	})
}

// TestAdmin_OrgScopedAliasPUT_ProducesExpectedStreamID is the stream-id
// half of I4(d): PUT /admin/v1/aliases/{org}/{name} must compose the
// es-lite stream id as "o_<org>_<name>" (binding ruling 1) — checked two
// ways: SplitAliasStreamID round-trips that exact literal id back to
// (scope, name), and the alias aggregate's stream under that exact id
// really was written (proving admin.go, not just the encoding helper in
// isolation, used it).
func TestAdmin_OrgScopedAliasPUT_ProducesExpectedStreamID(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "Acme", "owner_sub": "dev",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: %d %s", rec.Code, rec.Body.String())
	}
	rec = doRequest(t, mux, http.MethodPut, "/admin/v1/aliases/acme/fast", "s3cret", map[string]any{"target": "gpt-4"})
	if rec.Code != http.StatusOK {
		t.Fatalf("set org-scoped alias: %d %s", rec.Code, rec.Body.String())
	}

	const wantStreamID = "o_acme_fast"
	scope, name, err := SplitAliasStreamID(wantStreamID)
	if err != nil {
		t.Fatalf("SplitAliasStreamID(%q): %v", wantStreamID, err)
	}
	if scope != "acme" || name != "fast" {
		t.Fatalf("SplitAliasStreamID(%q) = (%q, %q), want (acme, fast)", wantStreamID, scope, name)
	}

	sid, err := es.NewStreamID(alias.StreamType, wantStreamID)
	if err != nil {
		t.Fatalf("NewStreamID: %v", err)
	}
	state, _, err := f.aliasRT.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("load alias stream %q: %v", wantStreamID, err)
	}
	if state.GetTarget() != "gpt-4" {
		t.Fatalf("alias stream %q target = %q, want gpt-4 (PUT did not land on the expected stream id)", wantStreamID, state.GetTarget())
	}
}

// --- review round: I1 fail-closed authz -------------------------------------

// TestAdmin_FailClosedWhenProjectionsUnhealthy is I1: once the injected
// healthy() reports false (simulating a dead relay or fail-stopped
// projector), every non-platform-admin org-scoped request must get 503
// service_unavailable — never authorize off whatever the role table
// happened to be frozen at. Platform admins are unaffected.
func TestAdmin_FailClosedWhenProjectionsUnhealthy(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	verifier := &fakeVerifier{subs: map[string]string{"jwt-member": "member-1"}}
	var healthy atomic.Bool
	healthy.Store(true)
	f := newAdminFixture(t, cfg, verifier, nil, healthy.Load)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodPost, "/admin/v1/orgs", "s3cret", map[string]any{
		"id": "acme", "name": "Acme", "owner_sub": "dev",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org: %d %s", rec.Code, rec.Body.String())
	}
	rec = doRequest(t, mux, http.MethodPut, "/admin/v1/orgs/acme/members/member-1", "s3cret", map[string]any{"role": "owner"})
	if rec.Code != http.StatusOK {
		t.Fatalf("upsert member: %d %s", rec.Code, rec.Body.String())
	}
	waitForSQL(t, func() (bool, error) {
		_, members, _, err := f.rs.GetOrg(context.Background(), "acme")
		if err != nil {
			return false, err
		}
		for _, m := range members {
			if m.Sub == "member-1" {
				return true, nil
			}
		}
		return false, nil
	})

	// Sanity: while healthy, the now-owner member can read/manage normally.
	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/orgs/acme", "jwt-member", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("member GET org while healthy: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	healthy.Store(false)

	// Now unhealthy: the very same owner-role member is refused outright,
	// not authorized off the (potentially stale-forever) frozen role
	// table.
	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/orgs/acme", "jwt-member", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("member GET org while unhealthy: status = %d, want 503; body=%s", rec.Code, rec.Body.String())
	}
	if typ := decodeErrorType(t, rec); typ != errTypeServiceUnavailable {
		t.Fatalf("error.type = %q, want %q", typ, errTypeServiceUnavailable)
	}

	// Platform admin (bootstrap) is unaffected by the unhealthy flag.
	rec = doRequest(t, mux, http.MethodGet, "/admin/v1/orgs/acme", "s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("bootstrap GET org while unhealthy: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
}
