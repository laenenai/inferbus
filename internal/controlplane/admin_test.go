package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	admin *Admin
	rs    *MemReadStore
}

func newAdminFixture(t *testing.T, cfg Config, verifier TokenVerifier, resync func(context.Context) error) *adminFixture {
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
	admin := NewAdmin(NewAuthenticator(cfg, verifier), rs, orgRT, keyRT, aliasRT, resync)
	return &adminFixture{admin: admin, rs: rs}
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
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil)
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
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil)
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
	f := newAdminFixture(t, cfg, verifier, nil)
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

func TestAdmin_CreateKey_PlaintextOnce_ListHidesHashAndPlaintext(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	f := newAdminFixture(t, cfg, &fakeVerifier{}, nil)
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

func TestAdmin_NonMemberForbidden_ViewerForbiddenOnManage(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	verifier := &fakeVerifier{subs: map[string]string{
		"jwt-stranger": "stranger",
		"jwt-viewer":   "viewer-1",
	}}
	f := newAdminFixture(t, cfg, verifier, nil)
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

func TestAdmin_AliasGlobalScope_RequiresPlatformAdmin(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret", PlatformAdmins: []string{"admin-sub"}}
	verifier := &fakeVerifier{subs: map[string]string{
		"jwt-nonadmin": "someone",
		"jwt-admin":    "admin-sub",
	}}
	f := newAdminFixture(t, cfg, verifier, nil)
	mux := f.admin.Routes()

	rec := doRequest(t, mux, http.MethodPut, "/admin/v1/aliases/_global/fast", "jwt-nonadmin", map[string]any{"target": "gpt-4"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-admin PUT _global alias: status = %d, want 403; body=%s", rec.Code, rec.Body.String())
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
	admin := NewAdmin(NewAuthenticator(cfg, &fakeVerifier{}), NewMemReadStore(), orgRT, keyRT, aliasRT, func(context.Context) error { return nil })
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
	f := newAdminFixture(t, cfg, verifier, resync)
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
