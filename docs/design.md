# Open Inference Bus — Design Spec

**Date:** 2026-09-07
**Status:** Draft for review
**Working name:** `inferbus` (used throughout; final naming is a mechanical find/replace before the first public push)

## 1. Context & motivation

This project was designed from scratch as a public, NATS-native inference platform, informed by an earlier private scaffold. This spec defines the project that realizes the target vision:

> An OpenAI-compatible gateway that accepts **alias-based** model requests with **full IAM**, forwards work over **NATS** to **workers** that route through **embedded Bifrost** to local or remote LLMs, with workers publishing **usage events** onto the bus and a **harvester** persisting them into **ClickHouse** for observability and accounting.

The original private scaffold served as the porting source, not the destination; upstream consumers depend on this public project.

## 2. Goals

- OpenAI-compatible HTTP surface: `/v1/chat/completions` (SSE streaming), `/v1/embeddings`, `/v1/models`.
- Alias-based model addressing with per-org overrides, editable at runtime.
- Gateway-owned IAM: API keys under orgs/projects with per-key alias allowlists, rate limits, and token budgets.
- JetStream work-queue transport with observable per-model backlogs and queue-depth admission control.
- Workers that need **nothing but NATS** plus their engine endpoints — no database, no control-plane access.
- Embedded Bifrost in the worker for provider routing (vLLM, llama.cpp, Ollama, MLX, OpenAI, Anthropic, …).
- Usage pipeline: worker → `METERING` stream → harvester → ClickHouse; dashboards and admin usage reads come from ClickHouse, while budget state reaches gateways as a NATS KV projection (see [docs/design-usage.md](docs/design-usage.md)).
- Single Go module, single multi-command binary, one-command `docker compose` quickstart.
- Zero references to internal platforms; zero private dependencies.

## 3. Non-goals (v1)

- **No E2E payload encryption / trust tiers.** Security = TLS + NATS accounts/permissions. The keyd/natskit crypto plane is not ported. Payload encryption may return later as a plugin.
- **No direct-NATS public client library.** The HTTP gateway is the only supported entry point, so IAM cannot be bypassed. (The old `pkg/inferclient` is not ported as a public API; its relay logic moves to `internal/relay`.)
- **No functional web console.** V1 ships the admin HTTP API plus a visual mockup of the console; the working console is a v2 milestone.
- **No extraction/toolrun stack.** `pkg/extract`, `pkg/normalize`, `pkg/eval`, `toolrun`, and the GLiNER engine stay in the private repo for now.
- **No priority queues in v1** (designed for, added in v1.5 — see §8).
- **No multi-region routing.** Subject layout leaves room for it; not built.

## 4. Decision log

| # | Decision | Choice |
|---|----------|--------|
| 1 | OSS strategy | Fresh public repo/org; internal platform consumes upstream |
| 2 | Crypto plane | Dropped; TLS + NATS accounts |
| 3 | IAM (data plane) | Gateway-managed API keys under orgs/projects, Postgres-backed |
| 4 | IAM (control plane) | OIDC for humans on console/admin API (two-tier auth) |
| 5 | Transport | JetStream work-queue request leg + core-NATS streamed responses |
| 6 | Worker engine layer | Embedded Bifrost library replaces hand-rolled `HTTPEngine` |
| 7 | Aliases | Postgres source of truth, projected to NATS KV, gateways watch |
| 8 | Usage storage | ClickHouse only (Postgres rollup dropped); harvester service |
| 9 | Scope | Chat completions + embeddings |
| 10 | Repo shape | Monorepo, one multi-command binary (`inferbus gateway|worker|harvester`) |
| 11 | Go client | Not public; relay logic is gateway-internal |
| 12 | Console | Mock in v1, functional app in v2 |

## 5. Architecture overview

```
                     ┌────────────── control plane ───────────────────┐
 console (v2) ─OIDC─▶ admin API ──▶ Postgres (orgs, projects, keys,   │
                     │                        aliases, memberships)   │
                     │                  │ write-through projection    │
                     │                  ▼                             │
                     │        NATS KV: ALIASES ◀── watch ── gateways  │
                     └────────────────────────────────────────────────┘

 client ──HTTP/SSE──▶ gateway ──publish──▶ JetStream INFERENCE ──pull──▶ worker ─▶ Bifrost ─▶ engines/providers
 (OpenAI SDK)          │ ▲                 (consumer per model)           │
                       │ └── core NATS response stream (chunk|done|error)─┘
                       │                                                  │ publish usage
                       └── 429 on backlog                                 ▼
                                                       JetStream METERING ──pull──▶ harvester ──▶ ClickHouse
                                                                                         ▲
                                             NATS KV: MODELS (worker advertise + heartbeat)
```

**Trust model:** the gateway is the trust terminator. Everything behind it (NATS, workers, harvester) lives in a private network segment secured by NATS accounts + TLS. Workers and harvester never see API keys or org data beyond what rides in message headers.

## 6. Components

### 6.1 `inferbus gateway`

> This section is superseded by [docs/design-controlplane.md](docs/design-controlplane.md) for the control-plane implementation details.

- **Data-plane HTTP:** `POST /v1/chat/completions`, `POST /v1/embeddings`, `GET /v1/models`, `GET /healthz`, `GET /readyz`. OpenAI-compatible request/response bodies; streaming via SSE (`data: <chunk>` … `data: [DONE]`).
- **Admin HTTP:** `/admin/v1/{orgs,projects,keys,aliases,usage}` — CRUD plus usage summaries proxied from ClickHouse. Auth: OIDC bearer tokens (any standard issuer; issuer/audience configured). Role model in §9.
- **Request lifecycle:**
  1. Authenticate API key (constant-time hash lookup, in-memory cache over Postgres).
  2. Resolve alias → concrete model from the KV-watched alias map (org override first, then global; a request naming a concrete model directly is treated as an alias lookup miss → 404 unless an alias of that name exists).
  3. Authorize: key's alias allowlist, rate limit (token bucket per key), budget check (`BUDGETS` KV projection, §11 and [docs/design-usage.md](docs/design-usage.md)).
  4. Admission: check per-model backlog (JetStream consumer info, cached ~1s); above threshold → `429` with `Retry-After`.
  5. Publish request to `inference.req.<model>` with a unique reply subject; relay response messages to the client as SSE; forward client disconnect as a cancel message.
- **Alias projection:** on every alias mutation the admin API writes Postgres transactionally, then upserts the KV entry. On startup the gateway reconciles the `ALIASES` bucket from Postgres (KV is a pure, rebuildable projection — never authoritative, never hand-edited). `POST /admin/v1/aliases/resync` forces reconciliation.
- **`GET /v1/models`** returns the *aliases* visible to the calling key (not concrete models) — aliases are the public vocabulary.

### 6.2 `inferbus worker`

- **Config:** one local YAML file; no database, no control-plane access. Maps each served concrete model name to an engine config.
- **Loop:** one JetStream pull consumer per served model (durable `model-<slug>`), bounded concurrent handlers per model (config: `max_inflight`). In-progress ack extension (heartbeat) while a request runs; `MaxDeliver: 2` so a crashed worker's request is retried once.
- **Engine layer (built — two adapters, not Bifrost-only):** `openai_http` talks to any OpenAI-compatible HTTP server (Ollama, vLLM, llama.cpp) directly; embedded **Bifrost** (Go library) handles multi-provider routing (OpenAI, Anthropic, Ollama, vLLM, MLX, ...) for models that need it. Each model in config picks one engine. The worker translates wire requests → engine calls → streamed chunks on the reply subject.
- **Usage:** on completion (or error/cancel) publish one usage event to `metering.usage.<org>.<project>.<model>` (fields in §10, superseded by [docs/design-usage.md](docs/design-usage.md)). Aborted streams set `estimated: true`.
- **Advertisement (planned, not built):** a heartbeat entry in the `MODELS` KV bucket (`worker.<worker_id>` → served models, versions, last-seen) was designed for gateway `/readyz`-style checks and ops visibility with TTL-expired entries dropping out. This does not exist yet: a worker's model list is read once at startup, and changing it (or its config) requires restarting the worker process — there is no live reload and nothing to invalidate.

### 6.3 `inferbus harvester`

- Durable JetStream consumer on `METERING`; batches (size- and time-bounded) inserts into ClickHouse; acks after successful insert. At-least-once delivery + ClickHouse dedup (§10) = effectively exactly-once accounting.
- Owns ClickHouse schema; migrations embedded and applied on startup.
- No HTTP surface beyond `/healthz`/`/readyz` and metrics.

### 6.4 Shared internals

- `internal/wire` — subjects, stream/consumer definitions, message and usage-event types (ported from `pkg/inferwire`, de-branded). The single wire contract.
- `internal/relay` — publish + response-relay logic (ported from `pkg/inferclient`'s transport core; not a public API).
- `internal/platform` — small replacements for natskit: NATS connect w/ retry, health endpoints, OTEL setup (~200 lines total).

## 7. Wire contract

- **Stream `INFERENCE`** (work-queue retention): subjects `inference.req.<model>`. Headers carry `org`, `project`, `key_id`, `alias`, `req_id`, `kind` (`chat|embed`), `reply`, `deadline`.
- **Response leg (core NATS):** `inference.resp.<req_id>`; messages `{kind: chunk|done|result|error, seq, payload}`. `chunk` payloads are OpenAI-format SSE chunk JSON verbatim; `result` carries a complete non-streamed response; `done` closes the stream and includes final usage; `error` carries `{code, message, http_status}`.
- **Cancel:** core NATS `inference.cancel.<req_id>`; workers subscribe while a request is in flight. Full semantics in §7.1.
- **Stream `METERING`** (limits retention, e.g. 7 days): subjects `metering.usage.<org>.<project>.<model>`.
- **KV buckets:** `ALIASES` (control-plane projection), `MODELS` (worker advertisement, TTL).
- **Large payloads (v1.5, optional):** claim-check via object store `INFERENCE_BLOBS` for requests above the NATS max-payload threshold; the v1 gateway rejects oversized bodies with `413`.

### 7.1 Cancellation semantics

JetStream makes requests durable, which decouples them from the requester's liveness; cancellation re-establishes that tie across three phases:

1. **Queued, not yet picked up:** the gateway records `req_id → stream sequence` from the JetStream publish ack. On cancel it deletes the queued message (`DeleteMsg(seq)`) so it never reaches a worker. If deletion races a worker pickup, phase 2 covers it. Backstop: every request carries a `deadline` header; workers discard expired messages on pickup (ack + `canceled` usage event, zero tokens).
2. **Mid-generation:** the worker subscribes to `inference.cancel.<req_id>` per in-flight request. On SSE client disconnect or explicit abort the gateway publishes cancel; the worker aborts the Bifrost stream, **acks** the JetStream message, and publishes usage with `status: canceled`, `estimated: true`.
3. **Requester silently gone** (gateway crash, partition): no cancel arrives. The deadline hard-caps wasted generation and `MaxDeliver: 2` caps replays. V1.5 option: per-request liveness KV entry TTL-refreshed by the gateway, spot-checked by workers before starting expensive work.

**Ack invariant:** workers ack on every terminal outcome (`done`, `error`, `canceled`, expired-on-pickup); redelivery occurs only on worker death (no ack). Canceled requests must never redeliver.

## 8. Admission control & priority

- **V1:** gateway polls consumer backlog (`num_pending`) per model, cached ~1s; configurable per-model threshold; breach → `429` + `Retry-After`. Backlog depth is exported as a metric (autoscaling signal).
- **V1.5 — priority tiers:** either subject-tiered (`inference.req.<prio>.<model>`, workers drain `high` before `normal`) or NATS 2.11+ consumer priority groups. Tier assignment becomes a property of the API key. The v1 subject layout keeps the model token last-segment-stable so the tier segment can be inserted without breaking consumers.

## 9. IAM

### 9.1 Data plane — API keys

> This section is superseded by [docs/design-controlplane.md](docs/design-controlplane.md) for the control-plane implementation details.

Postgres schema (control plane):

- `orgs(id, name, created_at)`
- `projects(id, org_id, name, created_at)`
- `api_keys(id, project_id, name, key_hash, alias_allowlist text[], rate_limit_rpm int, max_inflight int, monthly_token_budget bigint null, disabled bool, expires_at null, created_at)` — key shown once at creation; only the hash stored.
- `aliases(id, scope_org_id null, name, target_model, params jsonb null, updated_at)` — `scope_org_id = null` ⇒ global; org-scoped rows override global on name collision. `params` allows default overrides (e.g. temperature caps) merged into requests.
- `org_members(org_id, subject, email, role)` — OIDC subjects; roles: `owner`, `admin`, `viewer`.
- `platform_admins(subject, email)` — cross-org administration.

Enforcement (all in-gateway): allowlist check at resolve time; rate limiting via per-key token bucket (in-memory per replica in v1; documented as per-replica — NATS-KV-backed shared counters are a later refinement); budgets per §11.

### 9.2 Control plane — OIDC

Admin API and (v2) console authenticate humans via OIDC bearer JWTs — configured issuer, audience, and JWKS discovery; no vendored IdP. Role resolution: `platform_admins` ⇒ everything; `org_members.role` scopes org-level access (owners manage members/keys/aliases; admins manage keys/aliases; viewers read usage). API keys are **never** valid on admin routes and OIDC tokens are **never** valid on data-plane routes.

## 10. Usage pipeline & ClickHouse

> This section is superseded by [docs/design-usage.md](docs/design-usage.md) for the M4 implementation details.

**Usage event** (published by workers): `req_id`, `ts`, `org`, `project`, `key_id`, `alias`, `model` (concrete), `provider` (Bifrost provider actually used), `kind` (`chat|embed`), `prompt_tokens`, `completion_tokens`, `cached_tokens`, `ttft_ms`, `duration_ms`, `queue_ms`, `status` (`ok|error|canceled`), `error_code`, `estimated` (bool), `worker_id`.

**ClickHouse schema (sketch):**

- `usage_events` — `ReplacingMergeTree` keyed `(org, ts, req_id)` (replays from at-least-once delivery collapse on `req_id`), partitioned monthly, TTL configurable (default 13 months).
- Materialized views: `usage_hourly_by_org_model`, `usage_hourly_by_project`, `usage_daily_by_key` — SummingMergeTree aggregates of tokens/requests/errors/latency quantiles.

**Consumers of ClickHouse:** harvester writes (and its budget ledger reads month-to-date sums — gateways consume the `BUDGETS` KV projection, never ClickHouse); admin API `usage` endpoints read the MVs; Grafana dashboards (a starter dashboard JSON ships in `deploy/`).

## 11. Budgets

> This section is superseded by [docs/design-usage.md](docs/design-usage.md) for the M4 implementation details (notably: budget state reaches gateways via a NATS KV projection, not a direct ClickHouse read — see that doc's amendment).

V1 budgets are **token-denominated** (`monthly_token_budget` per key). The gateway checks a cached month-to-date sum from ClickHouse (cache TTL ~30s) and rejects with `402`-styled error when exhausted. This is deliberately eventually-consistent: worst-case overshoot ≈ cache TTL × throughput, acceptable for v1 and documented. Currency budgets (price table per model) are a later addition on the same read path.

## 12. Management console (v1: mockup)

V1 delivers a **visual mockup** (design canvas) covering: OIDC login; org/project switcher; API-key management (create/rotate/disable, allowlist editor); alias editor (global + org overrides with live projection status); usage dashboards (tokens by model/alias/key over time, error rates, queue latency); worker fleet view (from `MODELS` KV). The mock validates the admin API surface — every screen must be buildable from `/admin/v1/*` endpoints defined in §6.1. Functional implementation is the v2 headline.

## 13. Repo layout & deployment

```
inferbus/
  cmd/inferbus/            # single binary: gateway|worker|harvester subcommands
  internal/gateway/      # HTTP, auth, alias resolution, admission, relay glue
  internal/worker/       # pull loop, Bifrost integration, usage publishing
  internal/harvester/    # METERING consumer, ClickHouse writer + migrations
  internal/wire/         # subjects, streams, message + usage types
  internal/relay/        # publish/response-relay transport core
  internal/platform/     # NATS connect, health, OTEL helpers
  internal/controlplane/ # Postgres store, admin API, KV projection
  deploy/                # Dockerfile, docker-compose quickstart, Grafana dashboard
  docs/                  # self-contained design docs (this spec's content, ported)
```

- One Docker image; role chosen by subcommand.
- `docker compose up` quickstart: NATS (JetStream), Postgres, ClickHouse, gateway, one worker configured for a local Ollama, harvester. First-run bootstrap creates an org, a project, one API key (printed once), and a starter alias.
- Observability: OTEL traces/metrics on all roles (no-op without `OTEL_EXPORTER_OTLP_ENDPOINT`), Prometheus-format `/metrics` optional.

## 14. Porting plan (from the private scaffold)

| Source | Destination | Notes |
|---|---|---|
| `pkg/inferwire` | `internal/wire` | De-brand; drop classification headers; add org/project/alias headers |
| `internal/worker` loop | `internal/worker` | Keep pull/ack/heartbeat/cancel; replace `HTTPEngine` with Bifrost |
| `internal/gateway`, `cmd/inference-gateway` | `internal/gateway` | Replace `headerAuth` with key auth + OIDC admin; add alias/admission layers |
| `pkg/inferclient` transport core | `internal/relay` | Not a public API |
| `internal/rollup`, `cmd/metering-rollup` | — | Dropped (ClickHouse harvester replaces; consumer skeleton reusable) |
| `pkg/extract`, `pkg/normalize`, `pkg/eval`, `toolrun`, `engines/gliner` | — | Stay private |
| keyd/natskit usage | `internal/platform` | Rewritten helpers; no crypto |
| `cmd/inference-e2e` | `test/e2e` | Port harness; scrub internal prompt strings |

**De-branding checklist:** new module path; remove internal-caller comments and internal strings in e2e prompts and image names; README rewritten standalone; internal ADR/spec references replaced by `docs/` in-repo.

## 15. Testing

- **Unit:** alias resolution precedence, key auth, token buckets, admission thresholds, wire codecs, ClickHouse batching.
- **Integration:** gateway↔worker↔harvester against real NATS + ClickHouse + Postgres via testcontainers; Bifrost pointed at a mock OpenAI-shape engine.
- **E2E:** ported harness — streaming, cancel, worker-crash redelivery, budget exhaustion, alias hot-update via KV, 429 under synthetic backlog.
- TDD throughout per the development workflow.

## 16. Milestones

1. **M1 — Skeleton:** repo, binary + subcommands, wire contract, compose file with NATS/Postgres/ClickHouse.
2. **M2 — Data path:** gateway (key auth, static aliases) → worker (Bifrost, one provider) → SSE; e2e streaming test green.
3. **M3 — Control plane:** Postgres schema, admin API, KV alias projection, OIDC on admin routes.
4. **M4 — Usage:** worker usage events, harvester, ClickHouse schema, budget enforcement, Grafana starter.
5. **M5 — Hardening + launch:** admission control, cancel/redelivery e2e, docs, console mockup, first public release.
6. **V1.5:** priority tiers, claim-check blobs, shared rate counters. **V2:** functional console.

## 17. Open questions

- Final project name/org (working name `inferbus`).
- Bifrost config surface: how much of Bifrost's provider config to expose verbatim in worker YAML vs. wrap (decide at M2 with real Bifrost API in hand).
- Whether a private crypto plane returns as a middleware seam upstream or stays a downstream-fork concern.
