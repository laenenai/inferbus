# inferbus control plane — M3 design

**Status:** Approved design (2026-09-07). Supersedes §6.1's "planned" control-plane
sketch and §9.1's table-based schema in [design.md](design.md): the control plane
is **event-sourced on [es-lite](https://github.com/laenenai/es-lite) ≥ v0.5.0**
with Postgres as the event-store backend. Everything else in design.md
(subjects, data plane, usage plane) stands.

## 1. Shape

A fourth role, **`inferbus controlplane`**, owns the entire control plane:

```
                          ┌──────────────── inferbus controlplane ────────────────┐
 console (v2) ──OIDC──▶   │  /admin/v1 HTTP  ──commands──▶  es-lite aggregates    │
 operators    ──OIDC──▶   │                                    │ append           │
                          │                              Postgres event log       │
                          │                                    │ relay (Drainer)  │
                          │            JetStream CONTROL_EVENTS (evt.>)           │
                          │        │                  │                 │         │
                          │  proj-kv-aliases   proj-kv-keys      proj-sql-admin   │
                          └────────│──────────────────│─────────────────│─────────┘
                                   ▼                  ▼                 ▼
                            NATS KV ALIASES     NATS KV KEYS     Postgres read tables
                                   ▲                  ▲             (admin GET/list)
                                   └── watch ─ gateways ─ watch ──┘
```

- The **append-only event log in Postgres is the source of truth**. NATS KV
  buckets and SQL read tables are projections — rebuildable, never
  authoritative (the same invariant design.md §6.1 stated for KV, now
  structural).
- **Gateways stay NATS-only**: they watch the `ALIASES` and `KEYS` KV buckets
  into memory and never touch Postgres or the control plane at request time.
  The control plane can be down; the data plane keeps serving with the last
  projected state.
- Workers are untouched by M3.

## 2. Aggregates and events

es-lite `Decider[S, C, E]` aggregates with proto-defined state/commands/events
(`api/controlplane/v1/*.proto`, generated with buf; generated code committed).
Single es-lite workspace `controlplane`.

| Aggregate | Stream | Events |
|---|---|---|
| `org` | one per org | `OrgCreated`, `OrgRenamed`, `MemberUpserted` (OIDC sub → owner\|admin\|viewer), `MemberRemoved`, `ProjectCreated`, `ProjectArchived` |
| `apikey` | one per key id | `KeyCreated` (SHA-256 hash, org, project, name, allowlist, rpm, budget), `KeyRotated` (new hash + previous hash), `KeyDisabled` (current hash), `KeyAllowlistChanged`, `KeyLimitsChanged` — rotate/disable events carry the hash they retire so the KV projector needs no side state |
| `alias` | one per scope+name (`_global/fast`, `acme/smart`) | `AliasSet` (target model, param overrides), `AliasDeleted` |

Invariants enforced in `Decide`: an org's last owner cannot be removed; a
project must belong to a live org; a key's org/project must exist at creation
(checked via a read on the org aggregate before dispatch — cross-aggregate,
best-effort, acceptable for admin-rate operations); alias targets are
validated as NATS-subject-safe (`wire.Slug` identity).

**Privacy rule (hard):** events never contain PII or secrets. Members are
opaque OIDC subjects (no emails; display names live only in read models
sourced from the IdP at render time). Keys are stored as SHA-256 hashes of
32-byte random secrets; the plaintext exists only in the HTTP response of
create/rotate, once. es-lite is zero-knowledge with no crypto-shredding (its
ADR 0025), so nothing in the log may ever need erasure.

## 3. Commands and the admin API

`/admin/v1` (same HTTP server as the role, gateway untouched):

| Endpoint | Command |
|---|---|
| `POST /admin/v1/orgs` | create org (caller becomes owner) |
| `PATCH /admin/v1/orgs/{org}` | rename |
| `PUT /admin/v1/orgs/{org}/members/{sub}` | upsert member role |
| `DELETE /admin/v1/orgs/{org}/members/{sub}` | remove member |
| `POST /admin/v1/orgs/{org}/projects` | create project |
| `POST /admin/v1/orgs/{org}/projects/{id}/archive` | archive |
| `POST /admin/v1/keys` | create key → returns `ib_live_…` plaintext ONCE |
| `POST /admin/v1/keys/{id}/rotate` | rotate → new plaintext once |
| `POST /admin/v1/keys/{id}/disable` | disable |
| `PUT /admin/v1/keys/{id}/allowlist` · `PUT …/limits` | update |
| `PUT /admin/v1/aliases/{scope}/{name}` | set alias (scope = `_global` or org id) |
| `DELETE /admin/v1/aliases/{scope}/{name}` | delete alias |
| `GET` variants of all of the above | served from SQL read tables |
| `POST /admin/v1/projections/resync` | rebuild KV projections from the log |

Commands dispatch through `aggregate.Runtime.Handle` with
`es.Meta{Actor: <oidc sub>, CorrelationID: <request id>}`. `es.ErrConflict`
(optimistic concurrency) retries once, then returns 409.

**AuthN/AuthZ:** OIDC bearer JWTs — configured issuer + audience, JWKS
discovery, cached. Roles: `platform_admins` (list of OIDC subs in
controlplane YAML — solves the bootstrap problem; sees everything) and org
roles from the `org` aggregate (owner: members+keys+aliases+projects; admin:
keys+aliases; viewer: read). A `bootstrap_token` config option (single static
bearer, platform-admin-equivalent, logged loudly at startup) exists for dev
compose and CI only. API keys are never valid on `/admin/v1`; OIDC tokens are
never valid on the data plane.

## 4. Event delivery and projections

- **Relay (embedded):** es-lite `delivery.Drainer` (Postgres claim-drain,
  `SKIP LOCKED`) → `natsjs.Publisher` onto JetStream stream
  `CONTROL_EVENTS` (subjects `evt.>`, file storage, limits retention,
  `Duplicates: 2m`; dedup by `Nats-Msg-Id = global_position`). Runs inside
  the controlplane process — no separate `es-relayd` deployment.
- **Projectors** (also in-process, each an `natsjs.Consume` durable):
  - `proj-kv-aliases` (filter `evt.controlplane.alias.>`) → KV bucket
    `ALIASES`, key `<scope>/<name>`, value JSON `{target, params}`.
  - `proj-kv-keys` (filter `evt.controlplane.apikey.>`) → KV bucket `KEYS`,
    key = SHA-256 hex of the secret, value JSON
    `{org, project, name, allow, rpm, disabled}`. Rotation writes the new
    hash and deletes the old; disable deletes the entry.
  - `proj-sql-admin` (filter `evt.controlplane.>`) → Postgres read tables
    (`cp_orgs`, `cp_org_members`, `cp_projects`, `cp_api_keys`,
    `cp_aliases`) for admin GETs and lists.
- **Checkpointing:** the `CP_MARKERS` NATS KV bucket (key per projection,
  e.g. `proj-sql-admin`, `proj-kv-aliases`; value the highest-applied
  `global_position`); handlers are idempotent by comparing positions.
  At-least-once delivery is the contract.
- **Startup & rebuild:** on boot each projector runs `projection.Replay`
  against the store's `ReadAll` from its marker, then switches to live
  `Consume`. `resync` clears a KV bucket and replays from position 0 — this
  is an offline maintenance operation, not something safe to trigger
  casually: the caller must stop the running projectors first (the admin
  handler does this itself), and while it runs the ALIASES/KEYS buckets do
  not reflect a consistent snapshot, so a cold gateway (no cached KV watch
  yet) sees brief auth/alias unavailability until the rebuild completes; a
  warm gateway keeps serving its last good snapshot in the meantime.

## 5. Gateway integration

New gateway config block:

```yaml
iam:
  mode: kv          # kv | static (static = today's inline keys/aliases YAML)
```

In `kv` mode the gateway KV-watches `ALIASES` and `KEYS` into atomic
in-memory maps (initial load via bucket scan, then watch updates).
Request-time behavior: hash the presented bearer key (SHA-256), constant-time
lookup in the KEYS map; resolve alias by org override (`<org>/<alias>`) then
global (`_global/<alias>`); the rest of the request path is unchanged.
`static` mode remains for dev and tests. The data-plane wire contract does
not change in M3.

## 6. Deployment, testing, milestones

- **Compose:** add a `controlplane` service (depends on postgres+nats
  healthy), `bootstrap_token: dev_admin_change_me` in its example config,
  quickstart docs showing key/alias creation via curl before the first chat
  request.
- **Backend:** Postgres-only in production (`postgres.Open` with
  `WithoutAutoMigrate` in multi-replica setups; single replica in M3
  auto-migrates). Aggregate/projector unit tests run on es-lite's SQLite
  store + embedded NATS (`es.Store` is the seam); one env-gated integration
  test covers the Postgres path end to end.
- **Dependency:** `github.com/laenenai/es-lite v0.5.0` (first public-deps
  tag). Branding check already allows the `laenenai` org path.
- **Out of scope for M3** (unchanged from design.md): harvester/ClickHouse
  (M4), admission control (M5), console app (v2 — the mock in
  [design/console-mock](design/console-mock) maps 1:1 to this admin API),
  per-org es-lite workspaces, OpenBao-style key sealing.
