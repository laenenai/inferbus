// Package controlplane: the /admin/v1 HTTP API (spec §3).
//
// Admin wires an Authenticator (Task 9), the three aggregate runtimes
// (Tasks 2-4), the SQL admin ReadStore (Task 8), and an injected resync
// function (owned by the runner, Task 6/7/8's projector lifecycles) into
// one *http.ServeMux. It is the only place in the control plane that
// dispatches commands: every write handler builds a command, calls
// handleWithRetry (one retry on es.ErrConflict, per the api-notes "the
// caller reloads and retries" contract), and maps the aggregate's sentinel
// errors onto HTTP status codes with OpenAI-style
// {"error":{"message":...,"type":...}} bodies (matching the gateway's
// oaiError shape, api-notes-adjacent convention carried from
// internal/gateway/gateway.go).
//
// AuthZ follows the binding controller ruling: org-scoped routes resolve
// the caller's role via ReadStore.GetOrg (never the aggregate itself —
// platform admins short-circuit before ever touching the ReadStore, so
// they are never blocked by projector lag); key/alias routes "under an
// org" need manage_keys; member/project/org routes need manage_org; org
// creation and the "_global" alias scope need PlatformAdmin outright.
//
// Input validation (binding ruling): org ids, project ids, alias names,
// and alias scopes (when not "_global") must be non-empty and
// wire.Slug(x) == x — the deciders themselves don't validate emptiness, so
// the handler layer is where that's enforced, before any command is
// dispatched. Key ids are never caller-supplied: generateKeyID mints a
// short random id server-side. Member subs need only be non-empty (OIDC
// subjects are free-form, not slug-shaped).
package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/controlplane/alias"
	"github.com/laenenai/inferbus/internal/controlplane/apikey"
	"github.com/laenenai/inferbus/internal/controlplane/org"
	"github.com/laenenai/inferbus/internal/cpkv"
	"github.com/laenenai/inferbus/internal/wire"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
)

// globalScope is the HTTP alias-scope segment that maps onto the alias
// aggregate's "g_<name>" stream-id encoding (binding controller ruling 1).
// Re-exports cpkv.GlobalScope (I5 ruling) under admin.go's own established
// name so every existing reference here is unchanged.
const globalScope = cpkv.GlobalScope

// OrgRuntime, KeyRuntime, and AliasRuntime name the three
// aggregate.Runtime instantiations NewAdmin and the runner share, so
// neither call site has to spell out the full generic instantiation.
type (
	OrgRuntime   = *aggregate.Runtime[*controlplanev1.Org, *controlplanev1.OrgCommand, *controlplanev1.OrgEvent]
	KeyRuntime   = *aggregate.Runtime[*controlplanev1.ApiKey, *controlplanev1.ApiKeyCommand, *controlplanev1.ApiKeyEvent]
	AliasRuntime = *aggregate.Runtime[*controlplanev1.Alias, *controlplanev1.AliasCommand, *controlplanev1.AliasEvent]
)

// Admin implements the /admin/v1 HTTP API.
type Admin struct {
	auth    Authenticator
	rs      ReadStore
	orgRT   OrgRuntime
	keyRT   KeyRuntime
	aliasRT AliasRuntime
	resync  func(ctx context.Context) error
	// healthy reports whether the runner's relay + projectors are
	// currently up (review finding I1). nil means "always healthy" — the
	// zero value is safe for callers (e.g. simple unit tests) that don't
	// care about this. See isHealthy/authorizeOrgRole.
	healthy func() bool
	// usage answers GET /admin/v1/usage (Task 8's ClickHouse-backed usage
	// reporting). nil means "not configured" — deployments with no
	// clickhouse_dsn set (runner.go) never wire one, and getUsage
	// (usage.go) reports 501 not_configured rather than panicking.
	usage UsageReader
	// js is the shared JetStream context, used by listWorkers (workers.go,
	// M5 Task 5) to read the MODELS KV bucket directly — Admin otherwise
	// has no NATS handle of its own, since every other read goes through
	// rs (the SQL ReadStore) or an aggregate.Runtime.
	js jetstream.JetStream
}

// NewAdmin wires an Admin. resync is called by POST
// /admin/v1/projections/resync (PlatformAdmin-only): the runner owns
// projector lifecycles, so this closure is expected to stop them, run
// ResyncKV, and restart them (binding controller ruling 3) — Admin itself
// knows nothing about projector goroutines.
//
// healthy reports whether the relay and both KV/SQL projectors are
// currently running (the same signal /readyz uses). A nil healthy treats
// the process as always healthy. Non-platform-admin authorization fails
// closed (503) while healthy reports false — see authorizeOrgRole.
//
// usage is Task 8's optional ClickHouse-backed usage reader; nil means GET
// /admin/v1/usage reports 501 not_configured (see usage.go's getUsage).
//
// js is the shared JetStream context (M5 Task 5): listWorkers uses it
// directly to read the MODELS KV bucket. A nil js is only safe for tests
// that never exercise GET /admin/v1/workers.
func NewAdmin(auth Authenticator, rs ReadStore, orgRT OrgRuntime, keyRT KeyRuntime, aliasRT AliasRuntime, resync func(ctx context.Context) error, healthy func() bool, usage UsageReader, js jetstream.JetStream) *Admin {
	return &Admin{auth: auth, rs: rs, orgRT: orgRT, keyRT: keyRT, aliasRT: aliasRT, resync: resync, healthy: healthy, usage: usage, js: js}
}

// isHealthy is the nil-safe accessor for Admin.healthy.
func (a *Admin) isHealthy() bool {
	return a.healthy == nil || a.healthy()
}

// Routes returns the /admin/v1 mux (spec §3's endpoint table, plus its "GET
// variants of all of the above" as query/path-scoped list endpoints).
func (a *Admin) Routes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /admin/v1/orgs", a.createOrg)
	mux.HandleFunc("GET /admin/v1/orgs", a.listOrgs)
	mux.HandleFunc("GET /admin/v1/orgs/{org}", a.getOrg)
	mux.HandleFunc("PATCH /admin/v1/orgs/{org}", a.renameOrg)
	mux.HandleFunc("PUT /admin/v1/orgs/{org}/members/{sub}", a.upsertMember)
	mux.HandleFunc("DELETE /admin/v1/orgs/{org}/members/{sub}", a.removeMember)
	mux.HandleFunc("POST /admin/v1/orgs/{org}/projects", a.createProject)
	mux.HandleFunc("POST /admin/v1/orgs/{org}/projects/{id}/archive", a.archiveProject)

	mux.HandleFunc("POST /admin/v1/keys", a.createKey)
	mux.HandleFunc("GET /admin/v1/keys", a.listKeys)
	mux.HandleFunc("POST /admin/v1/keys/{id}/rotate", a.rotateKey)
	mux.HandleFunc("POST /admin/v1/keys/{id}/disable", a.disableKey)
	mux.HandleFunc("PUT /admin/v1/keys/{id}/allowlist", a.setAllowlist)
	mux.HandleFunc("PUT /admin/v1/keys/{id}/limits", a.setLimits)

	mux.HandleFunc("PUT /admin/v1/aliases/{scope}/{name}", a.setAlias)
	mux.HandleFunc("DELETE /admin/v1/aliases/{scope}/{name}", a.deleteAlias)
	mux.HandleFunc("GET /admin/v1/aliases/{scope}", a.listAliases)

	mux.HandleFunc("POST /admin/v1/projections/resync", a.resyncProjections)

	mux.HandleFunc("GET /admin/v1/usage", a.getUsage)

	mux.HandleFunc("GET /admin/v1/workers", a.listWorkers)

	// Catch-all: any /admin/v1/* request that doesn't match one of the
	// patterns above (unknown path, or a known path with the wrong
	// method) gets our OpenAI-style JSON body instead of net/http's
	// default plain-text 404/405 (folded-in minor). Go's ServeMux
	// precedence rules mean this subtree pattern only ever fires when no
	// more specific (and therefore method-matching) pattern above applies
	// — verified empirically: registering it alongside exact
	// method-specific patterns does not shadow them.
	mux.HandleFunc("/admin/v1/", func(w http.ResponseWriter, r *http.Request) {
		writeAdminError(w, http.StatusNotFound, errTypeNotFound, "no such admin route: "+r.Method+" "+r.URL.Path)
	})

	return mux
}

// --- error/response plumbing ------------------------------------------------

const (
	errTypeInvalidRequest     = "invalid_request_error"
	errTypeAuthn              = "authentication_error"
	errTypePermission         = "permission_error"
	errTypeNotFound           = "not_found_error"
	errTypeConflict           = "conflict_error"
	errTypeInternal           = "internal_error"
	errTypeServiceUnavailable = "service_unavailable"
	// errTypeNotConfigured is getUsage's (usage.go) 501 error type when no
	// UsageReader is wired (no clickhouse_dsn configured).
	errTypeNotConfigured = "not_configured"
)

// ErrProjectionsUnhealthy is authorizeOrgRole's fail-closed sentinel
// (review finding I1): returned instead of resolving a non-platform-admin
// caller's role when the runner reports the relay/projectors unhealthy.
// admin.go maps it to 503 service_unavailable.
var ErrProjectionsUnhealthy = errors.New("controlplane: projections unhealthy; refusing non-platform-admin authorization")

// writeAdminError matches the gateway's oaiError shape exactly
// (internal/gateway/gateway.go): {"error":{"message":...,"type":...}}.
func writeAdminError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"message": msg, "type": typ},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeInternalError logs the real error server-side — which may contain
// backend-specific detail (SQL driver text, es-lite internals) that must
// never reach a client — and writes a generic, safe 500 body instead
// (folded-in minor).
func writeInternalError(w http.ResponseWriter, context string, err error) {
	log.Printf("controlplane: admin: %s: %v", context, err)
	writeAdminError(w, http.StatusInternalServerError, errTypeInternal, "internal error")
}

// writeAggregateError maps a Decide sentinel (or a retry-exhausted
// es.ErrConflict) onto an HTTP status + OpenAI-style body.
func writeAggregateError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, org.ErrAlreadyExists), errors.Is(err, apikey.ErrAlreadyExists):
		writeAdminError(w, http.StatusConflict, errTypeConflict, err.Error())
	case errors.Is(err, org.ErrNotFound), errors.Is(err, apikey.ErrNotFound), errors.Is(err, org.ErrNoSuchProject):
		writeAdminError(w, http.StatusNotFound, errTypeNotFound, err.Error())
	case errors.Is(err, org.ErrLastOwner), errors.Is(err, apikey.ErrDisabled):
		writeAdminError(w, http.StatusConflict, errTypeConflict, err.Error())
	case errors.Is(err, org.ErrInvalidRole), errors.Is(err, alias.ErrInvalidTarget), errors.Is(err, alias.ErrReservedParam):
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, err.Error())
	case errors.Is(err, es.ErrConflict):
		// handleWithRetry already retried once; this is the second,
		// unresolved conflict.
		writeAdminError(w, http.StatusConflict, errTypeConflict, "concurrent modification; retry the request")
	default:
		writeInternalError(w, "aggregate dispatch", err)
	}
}

// maxRequestBody caps every admin request body (folded-in minor): an
// unauthenticated-until-parsed, unbounded body is a trivial memory-exhaustion
// vector for a JSON API.
const maxRequestBody = 1 << 20 // 1 MiB

// decodeJSON decodes r's body into v, writing a 400 invalid_request_error
// and returning false on any failure (missing body, malformed JSON, body
// too large).
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Body == nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "request body is required")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// validSlug reports whether s is non-empty and already NATS-subject-safe
// (wire.Slug(s) == s) — the shape required of org ids, project ids, alias
// names, and non-"_global" alias scopes (binding ruling 2).
func validSlug(s string) bool {
	return s != "" && wire.Slug(s) == s
}

// handleWithRetry calls rt.Handle once and, on es.ErrConflict (optimistic
// concurrency clash), reloads and retries exactly once more (api-notes:
// "the caller reloads and retries") before giving up. Every write handler
// in this file goes through this one retry policy.
func handleWithRetry[S, C, E any](ctx context.Context, rt *aggregate.Runtime[S, C, E], sid es.StreamID, cmd C, meta es.Meta) (aggregate.Result[S, E], error) {
	res, err := rt.Handle(ctx, sid, cmd, meta)
	if errors.Is(err, es.ErrConflict) {
		res, err = rt.Handle(ctx, sid, cmd, meta)
	}
	return res, err
}

// actorMeta builds the es.Meta every dispatch carries: Actor identifies
// the authenticated caller, CorrelationID ties this request's events
// together for the audit trail.
func actorMeta(id Identity) es.Meta {
	return es.Meta{
		Actor:         es.Actor{Type: "user", ID: id.Sub},
		CorrelationID: uuid.New(),
	}
}

// generateKeyID mints a short, server-generated key id (binding ruling 2:
// key ids are never caller-supplied) — 8 random bytes, hex-encoded.
func generateKeyID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read only fails if the OS entropy source is
		// unavailable, unrecoverable here just as in GenerateKey.
		panic("controlplane: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// aliasStreamID composes the es-lite stream id for one (scope, name) alias
// per binding controller ruling 1: "g_<name>" for the global scope,
// "o_<orgid>_<name>" otherwise. SplitAliasStreamID (kvproj.go) is its
// inverse.
func aliasStreamID(scope, name string) string {
	if scope == globalScope {
		return "g_" + name
	}
	return "o_" + scope + "_" + name
}

// --- authentication/authorization helpers -----------------------------------

func (a *Admin) authenticate(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	id, err := a.auth.Authenticate(r)
	if err != nil {
		writeAdminError(w, http.StatusUnauthorized, errTypeAuthn, "missing or invalid credentials")
		return Identity{}, false
	}
	return id, true
}

// authorizeOrgRole resolves id's role within orgID via ReadStore.GetOrg and
// reports whether that role satisfies need.
//
// Platform admins short-circuit to true without ever consulting the
// ReadStore (binding ruling: they must never be blocked by projector lag
// or downtime) and are the ONE class of caller unaffected by isHealthy.
//
// Every other caller fails CLOSED (ErrProjectionsUnhealthy, mapped to 503
// by authorizeOrg — review finding I1) the instant isHealthy reports
// false, i.e. once the relay or either projector has fail-stopped. This is
// deliberately stricter than "the role table might be a little stale":
// ReadStore.GetOrg is fed by an at-least-once, eventually-consistent SQL
// projector (Task 8), so under NORMAL operation a just-added member or a
// just-created org can lag the write that produced it by low milliseconds
// — that is expected, not a bug, and every admin_test.go case that depends
// on it polls (waitForSQL) rather than asserting instantaneous visibility.
// (One visible corollary: a caller who JUST created an org, or was JUST
// added as a member, can see a spurious 404/403 for a few milliseconds
// until the projector catches up.) What isHealthy actually guards against
// is different in kind: once the feed is KNOWN to be stopped, the role
// table isn't merely a few milliseconds behind, it is frozen at whatever
// arbitrary position it reached before failing — authorizing against it
// forever after would silently keep honoring stale roles (e.g. a removed
// member, or a demoted owner) instead of visibly refusing service.
func (a *Admin) authorizeOrgRole(ctx context.Context, id Identity, orgID, need string) (ok bool, err error) {
	if id.PlatformAdmin {
		return true, nil
	}
	if !a.isHealthy() {
		return false, ErrProjectionsUnhealthy
	}
	_, members, _, err := a.rs.GetOrg(ctx, orgID)
	if err != nil {
		return false, err
	}
	role := ""
	for _, m := range members {
		if m.Sub == id.Sub {
			role = m.Role
			break
		}
	}
	return Authorize(id, role, need), nil
}

// authorizeOrg is authorizeOrgRole plus the HTTP response: it writes
// 503/404/500/403 as appropriate and returns false when the request must
// stop.
func (a *Admin) authorizeOrg(w http.ResponseWriter, r *http.Request, id Identity, orgID, need string) bool {
	ok, err := a.authorizeOrgRole(r.Context(), id, orgID, need)
	if err != nil {
		switch {
		case errors.Is(err, ErrProjectionsUnhealthy):
			writeAdminError(w, http.StatusServiceUnavailable, errTypeServiceUnavailable, "control plane projections are currently unhealthy; try again shortly")
		case errors.Is(err, ErrOrgNotFound):
			writeAdminError(w, http.StatusNotFound, errTypeNotFound, "org not found")
		default:
			writeInternalError(w, "authorizeOrg", err)
		}
		return false
	}
	if !ok {
		writeAdminError(w, http.StatusForbidden, errTypePermission, "insufficient role")
		return false
	}
	return true
}

// authorizeAliasScope is authorizeOrg's alias-scope counterpart.
//
// CONTROLLER RULING (review finding I4d, supersedes the original "_global
// requires PlatformAdmin outright" reading): reading the "_global" scope
// only needs "read" — global aliases are non-secret, system-wide routing
// config, not an org's private data, so any authenticated caller (not just
// an org member — there's no org to be a member OF for the global scope)
// may list them. Mutating "_global" (set/delete) still requires
// PlatformAdmin outright, since it changes routing for every org at once.
// Any other scope is an org id, authorized exactly like authorizeOrg.
func (a *Admin) authorizeAliasScope(w http.ResponseWriter, r *http.Request, id Identity, scope, need string) bool {
	if scope == globalScope {
		if need == "read" {
			return true
		}
		if !id.PlatformAdmin {
			writeAdminError(w, http.StatusForbidden, errTypePermission, `mutating the "_global" alias scope requires platform admin`)
			return false
		}
		return true
	}
	return a.authorizeOrg(w, r, id, scope, need)
}

// loadKeyOrg loads the apikey aggregate's current state to recover the
// key's org (key mutation routes carry no org in their URL, unlike alias
// routes) and its stream id. found is false when the key stream has no
// KeyCreated yet (a fresh/nonexistent id).
func (a *Admin) loadKeyOrg(ctx context.Context, keyID string) (orgID string, sid es.StreamID, found bool, err error) {
	sid, err = es.NewStreamID(apikey.StreamType, keyID)
	if err != nil {
		return "", sid, false, err
	}
	state, _, err := a.keyRT.Load(ctx, sid)
	if err != nil {
		return "", sid, false, err
	}
	if state.GetId() == "" {
		return "", sid, false, nil
	}
	return state.GetOrg(), sid, true, nil
}

// --- response DTOs -----------------------------------------------------------

type orgSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type memberResponse struct {
	Sub  string `json:"sub"`
	Role string `json:"role"`
}

type projectResponse struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Archived bool   `json:"archived"`
}

type orgResponse struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Members  []memberResponse  `json:"members"`
	Projects []projectResponse `json:"projects"`
}

// orgResponseFromState builds an orgResponse straight from a just-mutated
// aggregate.Result.State, giving write handlers read-your-writes
// consistency independent of the (eventually-consistent) SQL projector.
func orgResponseFromState(s *controlplanev1.Org) orgResponse {
	members := make([]memberResponse, 0, len(s.GetMembers()))
	for sub, role := range s.GetMembers() {
		members = append(members, memberResponse{Sub: sub, Role: role})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Sub < members[j].Sub })

	projects := make([]projectResponse, 0, len(s.GetProjects()))
	for _, p := range s.GetProjects() {
		projects = append(projects, projectResponse{ID: p.GetId(), Name: p.GetName(), Archived: p.GetArchived()})
	}
	sort.Slice(projects, func(i, j int) bool { return projects[i].ID < projects[j].ID })

	return orgResponse{ID: s.GetId(), Name: s.GetName(), Members: members, Projects: projects}
}

// orgDetailFromRows builds an orgResponse from the SQL ReadStore's GetOrg
// rows — used by the GET handler, which (unlike the write handlers) has no
// fresh aggregate state to read from.
func orgDetailFromRows(row OrgRow, members []MemberRow, projects []ProjectRow) orgResponse {
	mr := make([]memberResponse, 0, len(members))
	for _, m := range members {
		mr = append(mr, memberResponse{Sub: m.Sub, Role: m.Role})
	}
	pr := make([]projectResponse, 0, len(projects))
	for _, p := range projects {
		pr = append(pr, projectResponse{ID: p.ID, Name: p.Name, Archived: p.Archived})
	}
	return orgResponse{ID: row.ID, Name: row.Name, Members: mr, Projects: pr}
}

// keyResponse never carries a hash field (binding constraint: admin GETs
// must never leak a hash). Key carries the one-time plaintext and is only
// ever populated by createKey/rotateKey.
type keyResponse struct {
	ID                 string   `json:"id"`
	Org                string   `json:"org"`
	Project            string   `json:"project"`
	Name               string   `json:"name"`
	Allow              []string `json:"allow"`
	RateLimitRPM       int32    `json:"rate_limit_rpm"`
	MonthlyTokenBudget int64    `json:"monthly_token_budget,omitempty"`
	Disabled           bool     `json:"disabled"`
	Key                string   `json:"key,omitempty"`
}

func keyResponseFromState(s *controlplanev1.ApiKey) keyResponse {
	return keyResponse{
		ID:                 s.GetId(),
		Org:                s.GetOrg(),
		Project:            s.GetProject(),
		Name:               s.GetName(),
		Allow:              nonNilStrings(append([]string(nil), s.GetAllow()...)),
		RateLimitRPM:       s.GetRateLimitRpm(),
		MonthlyTokenBudget: s.GetMonthlyTokenBudget(),
		Disabled:           s.GetDisabled(),
	}
}

func keyResponseFromRow(row KeyRow) keyResponse {
	return keyResponse{
		ID:                 row.ID,
		Org:                row.Org,
		Project:            row.Project,
		Name:               row.Name,
		Allow:              nonNilStrings(row.Allow),
		RateLimitRPM:       int32(row.RateLimitRPM),
		MonthlyTokenBudget: row.MonthlyTokenBudget,
		Disabled:           row.Disabled,
	}
}

type aliasResponse struct {
	Scope  string            `json:"scope"`
	Name   string            `json:"name"`
	Target string            `json:"target"`
	Params map[string]string `json:"params,omitempty"`
}

// --- org handlers ------------------------------------------------------------

type createOrgRequest struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	OwnerSub string `json:"owner_sub"`
}

// createOrg requires PlatformAdmin (binding ruling: org creation is a
// bootstrapping operation, not gated by any org role — there is no org
// yet). owner_sub is required in the body for the bootstrap identity
// (which has no real subject of its own) and defaults to the caller's own
// sub for OIDC callers.
func (a *Admin) createOrg(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	if !id.PlatformAdmin {
		writeAdminError(w, http.StatusForbidden, errTypePermission, "creating an org requires platform admin")
		return
	}

	var body createOrgRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	if !validSlug(body.ID) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "id must be a non-empty slug")
		return
	}
	if strings.TrimSpace(body.Name) == "" {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "name must not be empty")
		return
	}

	ownerSub := strings.TrimSpace(body.OwnerSub)
	if ownerSub == "" {
		if id.Bootstrap {
			writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "owner_sub is required when authenticated via the bootstrap token")
			return
		}
		ownerSub = id.Sub
	}

	sid, err := es.NewStreamID(org.StreamType, body.ID)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, err.Error())
		return
	}
	cmd := &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_Create{Create: &controlplanev1.CreateOrg{
		Id: body.ID, Name: body.Name, OwnerSub: ownerSub,
	}}}
	res, err := handleWithRetry(r.Context(), a.orgRT, sid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, orgResponseFromState(res.State))
}

// listOrgs requires PlatformAdmin: it is a system-wide view across every
// org, not scoped to any single one a caller might have a role in.
func (a *Admin) listOrgs(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	if !id.PlatformAdmin {
		writeAdminError(w, http.StatusForbidden, errTypePermission, "listing all orgs requires platform admin")
		return
	}
	rows, err := a.rs.ListOrgs(r.Context())
	if err != nil {
		writeInternalError(w, "listOrgs", err)
		return
	}
	out := make([]orgSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, orgSummary{ID: row.ID, Name: row.Name})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *Admin) getOrg(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	orgID := r.PathValue("org")
	if !a.authorizeOrg(w, r, id, orgID, "read") {
		return
	}
	row, members, projects, err := a.rs.GetOrg(r.Context(), orgID)
	if err != nil {
		if errors.Is(err, ErrOrgNotFound) {
			writeAdminError(w, http.StatusNotFound, errTypeNotFound, "org not found")
		} else {
			writeInternalError(w, "getOrg", err)
		}
		return
	}
	writeJSON(w, http.StatusOK, orgDetailFromRows(row, members, projects))
}

type renameOrgRequest struct {
	Name string `json:"name"`
}

func (a *Admin) renameOrg(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	orgID := r.PathValue("org")
	if !a.authorizeOrg(w, r, id, orgID, "manage_org") {
		return
	}
	var body renameOrgRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.Name) == "" {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "name must not be empty")
		return
	}
	sid, err := es.NewStreamID(org.StreamType, orgID)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, err.Error())
		return
	}
	cmd := &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_Rename{Rename: &controlplanev1.RenameOrg{Name: body.Name}}}
	res, err := handleWithRetry(r.Context(), a.orgRT, sid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, orgResponseFromState(res.State))
}

type upsertMemberRequest struct {
	Role string `json:"role"`
}

func (a *Admin) upsertMember(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	orgID := r.PathValue("org")
	sub := r.PathValue("sub")
	if !a.authorizeOrg(w, r, id, orgID, "manage_org") {
		return
	}
	if sub == "" {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "sub must not be empty")
		return
	}
	var body upsertMemberRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	sid, err := es.NewStreamID(org.StreamType, orgID)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, err.Error())
		return
	}
	cmd := &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_UpsertMember{UpsertMember: &controlplanev1.UpsertMember{
		Sub: sub, Role: body.Role,
	}}}
	res, err := handleWithRetry(r.Context(), a.orgRT, sid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, orgResponseFromState(res.State))
}

func (a *Admin) removeMember(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	orgID := r.PathValue("org")
	sub := r.PathValue("sub")
	if !a.authorizeOrg(w, r, id, orgID, "manage_org") {
		return
	}
	if sub == "" {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "sub must not be empty")
		return
	}
	sid, err := es.NewStreamID(org.StreamType, orgID)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, err.Error())
		return
	}
	cmd := &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_RemoveMember{RemoveMember: &controlplanev1.RemoveMember{Sub: sub}}}
	if _, err := handleWithRetry(r.Context(), a.orgRT, sid, cmd, actorMeta(id)); err != nil {
		writeAggregateError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type createProjectRequest struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (a *Admin) createProject(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	orgID := r.PathValue("org")
	if !a.authorizeOrg(w, r, id, orgID, "manage_org") {
		return
	}
	var body createProjectRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	if !validSlug(body.ID) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "id must be a non-empty slug")
		return
	}
	if strings.TrimSpace(body.Name) == "" {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "name must not be empty")
		return
	}
	sid, err := es.NewStreamID(org.StreamType, orgID)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, err.Error())
		return
	}
	cmd := &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_CreateProject{CreateProject: &controlplanev1.CreateProject{
		Id: body.ID, Name: body.Name,
	}}}
	res, err := handleWithRetry(r.Context(), a.orgRT, sid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}
	proj := res.State.GetProjects()[body.ID]
	writeJSON(w, http.StatusCreated, projectResponse{ID: proj.GetId(), Name: proj.GetName(), Archived: proj.GetArchived()})
}

func (a *Admin) archiveProject(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	orgID := r.PathValue("org")
	projID := r.PathValue("id")
	if !a.authorizeOrg(w, r, id, orgID, "manage_org") {
		return
	}
	sid, err := es.NewStreamID(org.StreamType, orgID)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, err.Error())
		return
	}
	cmd := &controlplanev1.OrgCommand{Kind: &controlplanev1.OrgCommand_ArchiveProject{ArchiveProject: &controlplanev1.ArchiveProject{Id: projID}}}
	res, err := handleWithRetry(r.Context(), a.orgRT, sid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}
	proj := res.State.GetProjects()[projID]
	writeJSON(w, http.StatusOK, projectResponse{ID: proj.GetId(), Name: proj.GetName(), Archived: proj.GetArchived()})
}

// --- key handlers ------------------------------------------------------------

type createKeyRequest struct {
	Org                string   `json:"org"`
	Project            string   `json:"project"`
	Name               string   `json:"name"`
	Allow              []string `json:"allow"`
	RateLimitRPM       int32    `json:"rate_limit_rpm"`
	MonthlyTokenBudget int64    `json:"monthly_token_budget"`
}

// createKey mints a fresh plaintext+hash (GenerateKey) and a server-side
// id (generateKeyID), dispatches CreateKey with the hash, and responds
// with the plaintext exactly once (binding constraint: this is the only
// place — besides rotateKey — the plaintext ever appears).
//
// Before dispatch it confirms org and project both exist in the org
// aggregate's own current state (design.md §2: "a key's org/project must
// exist at creation (checked via a read on the org aggregate before
// dispatch — cross-aggregate, best-effort")) — the apikey Decider itself
// has no way to check this, since it never touches the org stream.
func (a *Admin) createKey(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	var body createKeyRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	if !validSlug(body.Org) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "org must be a non-empty slug")
		return
	}
	if !validSlug(body.Project) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "project must be a non-empty slug")
		return
	}
	if strings.TrimSpace(body.Name) == "" {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "name must not be empty")
		return
	}
	if !a.authorizeOrg(w, r, id, body.Org, "manage_keys") {
		return
	}

	orgSid, err := es.NewStreamID(org.StreamType, body.Org)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, err.Error())
		return
	}
	orgState, _, err := a.orgRT.Load(r.Context(), orgSid)
	if err != nil {
		writeInternalError(w, "createKey: load org", err)
		return
	}
	if orgState.GetId() == "" {
		writeAdminError(w, http.StatusNotFound, errTypeNotFound, "org not found")
		return
	}
	if _, ok := orgState.GetProjects()[body.Project]; !ok {
		writeAdminError(w, http.StatusNotFound, errTypeNotFound, "project not found in org")
		return
	}

	keyID := generateKeyID()
	plaintext, hash := GenerateKey()

	keySid, err := es.NewStreamID(apikey.StreamType, keyID)
	if err != nil {
		writeInternalError(w, "createKey: new stream id", err)
		return
	}
	cmd := &controlplanev1.ApiKeyCommand{Kind: &controlplanev1.ApiKeyCommand_Create{Create: &controlplanev1.CreateKey{
		Id: keyID, Org: body.Org, Project: body.Project, Name: body.Name,
		Hash: hash, Allow: body.Allow, RateLimitRpm: body.RateLimitRPM, MonthlyTokenBudget: body.MonthlyTokenBudget,
	}}}
	res, err := handleWithRetry(r.Context(), a.keyRT, keySid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}

	resp := keyResponseFromState(res.State)
	resp.Key = plaintext
	writeJSON(w, http.StatusCreated, resp)
}

func (a *Admin) listKeys(w http.ResponseWriter, r *http.Request) {
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
	rows, err := a.rs.ListKeys(r.Context(), orgID)
	if err != nil {
		writeInternalError(w, "listKeys", err)
		return
	}
	out := make([]keyResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, keyResponseFromRow(row))
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *Admin) rotateKey(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	keyID := r.PathValue("id")
	orgID, sid, found, err := a.loadKeyOrg(r.Context(), keyID)
	if err != nil {
		writeInternalError(w, "rotateKey: loadKeyOrg", err)
		return
	}
	if !found {
		writeAdminError(w, http.StatusNotFound, errTypeNotFound, apikey.ErrNotFound.Error())
		return
	}
	if !a.authorizeOrg(w, r, id, orgID, "manage_keys") {
		return
	}

	plaintext, hash := GenerateKey()
	cmd := &controlplanev1.ApiKeyCommand{Kind: &controlplanev1.ApiKeyCommand_Rotate{Rotate: &controlplanev1.RotateKey{NewHash: hash}}}
	res, err := handleWithRetry(r.Context(), a.keyRT, sid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}
	resp := keyResponseFromState(res.State)
	resp.Key = plaintext
	writeJSON(w, http.StatusOK, resp)
}

func (a *Admin) disableKey(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	keyID := r.PathValue("id")
	orgID, sid, found, err := a.loadKeyOrg(r.Context(), keyID)
	if err != nil {
		writeInternalError(w, "disableKey: loadKeyOrg", err)
		return
	}
	if !found {
		writeAdminError(w, http.StatusNotFound, errTypeNotFound, apikey.ErrNotFound.Error())
		return
	}
	if !a.authorizeOrg(w, r, id, orgID, "manage_keys") {
		return
	}
	cmd := &controlplanev1.ApiKeyCommand{Kind: &controlplanev1.ApiKeyCommand_Disable{Disable: &controlplanev1.DisableKey{}}}
	res, err := handleWithRetry(r.Context(), a.keyRT, sid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, keyResponseFromState(res.State))
}

type setAllowlistRequest struct {
	Allow []string `json:"allow"`
}

func (a *Admin) setAllowlist(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	keyID := r.PathValue("id")
	orgID, sid, found, err := a.loadKeyOrg(r.Context(), keyID)
	if err != nil {
		writeInternalError(w, "setAllowlist: loadKeyOrg", err)
		return
	}
	if !found {
		writeAdminError(w, http.StatusNotFound, errTypeNotFound, apikey.ErrNotFound.Error())
		return
	}
	if !a.authorizeOrg(w, r, id, orgID, "manage_keys") {
		return
	}
	var body setAllowlistRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	cmd := &controlplanev1.ApiKeyCommand{Kind: &controlplanev1.ApiKeyCommand_SetAllowlist{SetAllowlist: &controlplanev1.SetAllowlist{Allow: body.Allow}}}
	res, err := handleWithRetry(r.Context(), a.keyRT, sid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, keyResponseFromState(res.State))
}

type setLimitsRequest struct {
	RateLimitRPM       int32 `json:"rate_limit_rpm"`
	MonthlyTokenBudget int64 `json:"monthly_token_budget"`
}

func (a *Admin) setLimits(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	keyID := r.PathValue("id")
	orgID, sid, found, err := a.loadKeyOrg(r.Context(), keyID)
	if err != nil {
		writeInternalError(w, "setLimits: loadKeyOrg", err)
		return
	}
	if !found {
		writeAdminError(w, http.StatusNotFound, errTypeNotFound, apikey.ErrNotFound.Error())
		return
	}
	if !a.authorizeOrg(w, r, id, orgID, "manage_keys") {
		return
	}
	var body setLimitsRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	cmd := &controlplanev1.ApiKeyCommand{Kind: &controlplanev1.ApiKeyCommand_SetLimits{SetLimits: &controlplanev1.SetLimits{
		RateLimitRpm: body.RateLimitRPM, MonthlyTokenBudget: body.MonthlyTokenBudget,
	}}}
	res, err := handleWithRetry(r.Context(), a.keyRT, sid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, keyResponseFromState(res.State))
}

// --- alias handlers ----------------------------------------------------------

type setAliasRequest struct {
	Target string            `json:"target"`
	Params map[string]string `json:"params"`
}

func (a *Admin) setAlias(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	scope := r.PathValue("scope")
	name := r.PathValue("name")
	if scope != globalScope && !validSlug(scope) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, `scope must be "_global" or a non-empty slug org id`)
		return
	}
	if !validSlug(name) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "name must be a non-empty slug")
		return
	}
	if !a.authorizeAliasScope(w, r, id, scope, "manage_keys") {
		return
	}

	var body setAliasRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	sid, err := es.NewStreamID(alias.StreamType, aliasStreamID(scope, name))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, err.Error())
		return
	}
	cmd := &controlplanev1.AliasCommand{Kind: &controlplanev1.AliasCommand_Set{Set: &controlplanev1.SetAlias{
		Target: body.Target, Params: body.Params,
	}}}
	res, err := handleWithRetry(r.Context(), a.aliasRT, sid, cmd, actorMeta(id))
	if err != nil {
		writeAggregateError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, aliasResponse{Scope: scope, Name: name, Target: res.State.GetTarget(), Params: res.State.GetParams()})
}

func (a *Admin) deleteAlias(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	scope := r.PathValue("scope")
	name := r.PathValue("name")
	if scope != globalScope && !validSlug(scope) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, `scope must be "_global" or a non-empty slug org id`)
		return
	}
	if !validSlug(name) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, "name must be a non-empty slug")
		return
	}
	if !a.authorizeAliasScope(w, r, id, scope, "manage_keys") {
		return
	}
	sid, err := es.NewStreamID(alias.StreamType, aliasStreamID(scope, name))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, err.Error())
		return
	}
	cmd := &controlplanev1.AliasCommand{Kind: &controlplanev1.AliasCommand_Delete{Delete: &controlplanev1.DeleteAlias{}}}
	if _, err := handleWithRetry(r.Context(), a.aliasRT, sid, cmd, actorMeta(id)); err != nil {
		writeAggregateError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Admin) listAliases(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	scope := r.PathValue("scope")
	if scope != globalScope && !validSlug(scope) {
		writeAdminError(w, http.StatusBadRequest, errTypeInvalidRequest, `scope must be "_global" or a non-empty slug org id`)
		return
	}
	if !a.authorizeAliasScope(w, r, id, scope, "read") {
		return
	}
	rows, err := a.rs.ListAliases(r.Context(), scope)
	if err != nil {
		writeInternalError(w, "listAliases", err)
		return
	}
	out := make([]aliasResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, aliasResponse{Scope: row.Scope, Name: row.Name, Target: row.Target, Params: row.Params})
	}
	writeJSON(w, http.StatusOK, out)
}

// --- resync handler ----------------------------------------------------------

// resyncTimeout bounds how long a resync (bucket destroy/recreate + full
// replay + projector restart) is allowed to run once the triggering HTTP
// request's own context has been detached (see resyncProjections) — a
// generous ceiling so a genuinely large event log doesn't get cut off, but
// still finite so a wedged resync doesn't hang the process forever.
const resyncTimeout = 10 * time.Minute

// resyncProjections is PlatformAdmin-only (binding ruling).
//
// Review finding C2: the request's own context MUST NOT be able to cancel
// ResyncKV — a client disconnect between the KV bucket destroy and the
// marker reset would otherwise abort mid-resync, permanently leaving
// KEYS/ALIASES empty until an operator notices and reruns it. So the
// context handed to a.resync is context.WithoutCancel(r.Context()) (drops
// the client's cancellation entirely, keeps any request-scoped values)
// wrapped in its own bounded resyncTimeout, not the request's lifetime.
//
// Review finding I3: success is 202 (the injected resync closure
// self-coordinates the projector stop/ResyncKV/restart sequence per
// binding ruling 3, and has already finished — successfully — by the time
// this returns); failure is 500, with the real error logged server-side
// and never echoed to the client. Even on failure the closure has already
// restarted the projectors (see runner.go's resyncFn), so /readyz reflects
// reality on its own via the same health signal this 500 doesn't need to
// duplicate.
func (a *Admin) resyncProjections(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	if !id.PlatformAdmin {
		writeAdminError(w, http.StatusForbidden, errTypePermission, "projection resync requires platform admin")
		return
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), resyncTimeout)
	defer cancel()
	if err := a.resync(ctx); err != nil {
		writeInternalError(w, "resync", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status": "accepted",
		"note":   "ALIASES/KEYS KV projections are briefly unavailable while resync runs",
	})
}
