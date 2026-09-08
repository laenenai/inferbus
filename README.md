# inferbus

**The inference bus — an OpenAI-compatible gateway that queues, routes, and
meters LLM traffic over [NATS](https://nats.io) JetStream.**

[![ci](https://github.com/laenenai/inferbus/actions/workflows/ci.yml/badge.svg)](https://github.com/laenenai/inferbus/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/laenenai/inferbus.svg)](https://pkg.go.dev/github.com/laenenai/inferbus)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Clients speak the standard OpenAI HTTP API against a **gateway**, which
resolves a caller's API key to an org/project and translates a caller-facing
**alias** (e.g. `fast`) to a concrete backend model, then queues the request
as a durable work item. A fleet of **workers** pulls requests off that queue,
runs them through a pluggable **engine** — a plain OpenAI-compatible HTTP
endpoint (Ollama, vLLM, llama.cpp) or the embedded
[Bifrost](https://github.com/maximhq/bifrost) multi-provider router — and
streams the response back to the gateway over core NATS, which relays it to
the client as SSE. Workers also emit per-request usage events onto a
`METERING` stream, destined for a ClickHouse-backed accounting pipeline.

## Why inferbus

- **Drop-in OpenAI surface** — point any OpenAI SDK at the gateway; streaming
  and non-streaming chat completions work unchanged.
- **A queue, not a proxy** — requests are durable JetStream work items:
  bursts buffer instead of failing, a crashed worker's request is retried
  exactly once, and per-model backlogs are observable (the basis for
  admission control and autoscaling).
- **Aliases as the public vocabulary** — clients ask for `fast` or `smart`;
  operators re-point aliases at concrete models without touching callers.
- **Workers with zero control-plane coupling** — a GPU node needs a NATS URL
  and its engine endpoints. No database, no credentials beyond its providers'.
- **Three-phase cancellation** — queued requests are deleted before pickup,
  mid-generation requests are stopped via a cancel subject, and a deadline
  header backstops everything; canceled work never redelivers.
- **Metering built in** — every request (success, error, or cancel) emits a
  usage event with tokens, TTFT, queue time, and provider attribution.

**Status:** pre-alpha (M4 — data path, control plane, and usage pipeline
shipped). See [docs/design.md](docs/design.md)
for the full design spec and milestone plan, and
[docs/design/console-mock](docs/design/console-mock) for the management
console design (Design Component artboards for the planned v2 console).

## Architecture

```
              control plane (built) — event-sourced on es-lite
   console (v2) ──OIDC──▶ admin API ──append──▶ Postgres event log
                          │                              │ relay
                          │                              ▼
                          │            NATS KV: ALIASES · KEYS ◀── watch ── gateways
                          └────────────────────────────────────────────────────────

   client (OpenAI SDK)
        │ HTTP/SSE
        ▼
   ┌───────────┐  publish  ┌──────────────────────┐  pull  ┌────────┐  Chat/ChatStream ┌───────────────────────┐
   │ gateway   │──────────▶│ JetStream INFERENCE  │───────▶│ worker │─────────────────▶│ engine                │
   │  auth     │           │ (work-queue,         │        │        │                  │ openai_http: Ollama,  │
   │  alias    │           │  consumer per model) │        │        │                  │  vLLM, llama.cpp      │
   │  resolve  │           └──────────────────────┘        │        │                  │ bifrost: embedded     │
   │  budget   │                                           │        │                  │  multi-provider router│
   └───────────┘                                           │        │                  └───────────────────────┘
        │  ▲                                               │        │
        │  └──────────────── core NATS ────────────────────┘        │
        │      inference.resp.<req_id> {chunk|done|result|error}    │ publish usage
        ▼                                                           ▼
      SSE to client                    JetStream METERING ──pull──▶ harvester ──▶ ClickHouse
                                                                         │
                                                                         └─▶ NATS KV: BUDGETS
                                                                             ◀── watch ── gateways (402 when exceeded)
```

**Data plane (built).** The gateway authenticates the caller's API key —
either against a static YAML key list or, in `iam.mode: kv`, against the
control plane's projected `KEYS` KV state — resolves the request's model
alias to a concrete model (static YAML map, or the projected `ALIASES` KV in
kv mode), and publishes the request onto the `INFERENCE` JetStream stream
(work-queue retention, one durable consumer per concrete model). A worker
pulls the message, runs it through its configured engine, and streams
response frames back over a per-request core-NATS subject; the gateway
relays those frames to the client, either as SSE (`stream: true`) or as a
single JSON body. Client disconnects and per-request deadlines are handled
as cancellation (see Wire contract below).

**Control plane (built).** Event-sourced on
[es-lite](https://github.com/laenenai/es-lite), with Postgres as the durable
event log — orgs, projects, API keys, and aliases are aggregates, not
tables. An OIDC-gated admin HTTP API (plus a bootstrap token for dev/CI)
dispatches commands; a relay projects committed events onto the `ALIASES`
and `KEYS` NATS KV buckets that gateways watch live, and onto Postgres read
tables for admin GETs. A gateway running `iam.mode: kv` picks up org/key/alias
changes with no restart and no control-plane round trip per request; `static`
mode (inline YAML keys/aliases) remains for dev and tests. Workers never talk
to the control plane or to Postgres — they need only a NATS URL and their
engine endpoints. See [docs/design-controlplane.md](docs/design-controlplane.md).

**Usage plane (built).** Every worker request — success, error, or
cancellation — publishes a `wire.UsageEvent` to the `METERING` stream, keyed
by org/project/model. The **harvester** consumes it, batches inserts into
ClickHouse, and its budget ledger folds each key's month-to-date usage into
the `BUDGETS` NATS KV bucket; a gateway in kv mode watches that bucket and
rejects further requests from an exhausted key with `402`. See
[docs/design-usage.md](docs/design-usage.md) and the Budgets section below.

## Status

| Area | Status |
|---|---|
| Chat completions (`/v1/chat/completions`, streaming + non-streaming) | done |
| Embeddings (`/v1/embeddings`) | planned |
| Cancel / deadline semantics (queued-delete, mid-stream cancel, deadline backstop) | done |
| Bifrost engine (multi-provider: OpenAI, Anthropic, Ollama, ...) | done |
| Static YAML keys/aliases | done |
| Postgres control plane + KV alias projection | done — [see design-controlplane.md](docs/design-controlplane.md) |
| Harvester + ClickHouse usage pipeline + budget enforcement | done — [see design-usage.md](docs/design-usage.md) |
| Admission control / priority tiers | planned (M5 / v1.5) |
| OIDC admin API + console | planned (v2) |

## Quickstart

```sh
docker compose -f deploy/docker-compose.yaml up -d --build
```

The compose file brings up the whole stack: NATS (JetStream), Postgres,
ClickHouse, the gateway (port 8080), one worker, the control plane (port
8081), and the harvester (port 8082). Every service is load-bearing —
nothing in it is idle scaffolding.

The compose gateway runs in `iam.mode: kv` (`deploy/gateway.kv.example.yaml`),
so keys and aliases come from the control plane rather than a checked-in
YAML credential, and monthly token budgets are enforced. Create an org, a
project, an alias, and a key first (steps 2–5 below), then call the gateway
with the key that step 5 returns:

```sh
curl http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $IB_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"fast","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

The worker's example config (`deploy/worker.example.yaml`) points at
`http://host.docker.internal:11434` so the containerized worker can reach a
model server (e.g. Ollama running `llama3.2`) on the Docker host; if your
engine runs in-network instead (its own compose service, or another
container), change that URL to the service's name.

Until the gateway's first KEYS/ALIASES scan completes (i.e. while the
control plane is still starting), `GET /readyz` and every authenticated
request answer 503 rather than 401.

## Control plane

1. Start the stack (the control plane service starts automatically):
```sh
docker compose -f deploy/docker-compose.yaml up -d --build
```

2. Create an org:
```sh
curl -X POST http://localhost:8081/admin/v1/orgs \
  -H "Authorization: Bearer dev_admin_change_me" \
  -H "Content-Type: application/json" \
  -d '{"id":"acme","name":"Acme","owner_sub":"dev"}'
```

3. Create a project within the org:
```sh
curl -X POST http://localhost:8081/admin/v1/orgs/acme/projects \
  -H "Authorization: Bearer dev_admin_change_me" \
  -H "Content-Type: application/json" \
  -d '{"id":"default","name":"Default"}'
```

4. Set an alias. The target must be NATS-subject-safe (lowercase
`[a-z0-9-]`), which is the same token a worker's model name resolves to —
so a worker serving `llama3.2` is reached by the target `llama3-2`:
```sh
curl -X PUT http://localhost:8081/admin/v1/aliases/acme/fast \
  -H "Authorization: Bearer dev_admin_change_me" \
  -H "Content-Type: application/json" \
  -d '{"target":"llama3-2"}'
```

5. Create an API key:
```sh
curl -X POST http://localhost:8081/admin/v1/keys \
  -H "Authorization: Bearer dev_admin_change_me" \
  -H "Content-Type: application/json" \
  -d '{"org":"acme","project":"default","name":"dev","allow":["fast"]}'
```
The response includes the plaintext key (shown once); export it as `IB_KEY`
and use it in the Quickstart's chat curl.

6. Read an org's usage (requires the control plane's optional
`clickhouse_dsn`, which the compose config sets; otherwise this answers 501):
```sh
curl "http://localhost:8081/admin/v1/usage?org=acme&window=7d" \
  -H "Authorization: Bearer dev_admin_change_me"
```
`window` is one of `24h`, `7d`, `30d`; the response buckets an org's usage by
model, alias, provider, and status.

**Static mode (no control plane).** To run the gateway against a checked-in
YAML key list instead, mount `deploy/gateway.example.yaml` in the compose
gateway service (it ships the example key `ib_dev_change_me` allowed to use
the alias `fast` → `llama3.2`) and restart it. **Change that key before
exposing the gateway to anything but your own machine** — it is a public,
checked-in credential. KV mode forbids a static `keys:` list and errors on
startup if it finds one; static mode has no budget enforcement.

## Budgets (optional)

Requires the control plane, the harvester, and a gateway in `iam.mode: kv`
— the default compose stack is exactly that (a static-mode gateway has no
`BUDGETS` projection to watch and never returns 402). Create a key with a
monthly token budget:

```sh
curl -X POST http://localhost:8081/admin/v1/keys \
  -H "Authorization: Bearer dev_admin_change_me" \
  -H "Content-Type: application/json" \
  -d '{"org":"acme","project":"default","name":"dev","allow":["fast"],"monthly_token_budget":100000}'
```

The harvester's budget ledger folds `METERING` usage into that key's
month-to-date token total and republishes it to the `BUDGETS` NATS KV bucket
on every `budget_refresh_interval` tick (default 10s — see
`deploy/harvester.example.yaml`). Once a key's month-to-date usage reaches
its budget, a gateway in kv mode rejects further requests from it with:

```json
{"error":{"type":"budget_exhausted","message":"monthly token budget exhausted"}}
```

Raise (or clear, with `0`) the budget with:

```sh
curl -X PUT http://localhost:8081/admin/v1/keys/<id>/limits \
  -H "Authorization: Bearer dev_admin_change_me" \
  -H "Content-Type: application/json" \
  -d '{"monthly_token_budget":1000000}'
```

**Upgrading an M3 deployment.** KEYS entries projected before M4 carry no
key id, and the budget ledger can only attribute a budget to an entry that
has one. Any key you touch afterwards (limits, allowlist, rotation) is
backfilled automatically; to heal every entry at once, run
`POST /admin/v1/projections/resync` on the control plane after upgrading.

Enforcement is eventually consistent by design (docs/design-usage.md's
amendment to §11): a request in the window between crossing the budget and
the next ledger flush plus gateway KV-watch propagation may still succeed.
Static mode has no budget enforcement — there is no `BUDGETS` projection to
watch.

## Configuration

Gateway — the compose stack uses `deploy/gateway.kv.example.yaml` (control
plane as the source of keys and aliases, budgets enforced). The static
alternative, `deploy/gateway.example.yaml`, carries its own IAM and alias
table:

```yaml
addr: ":8080"
request_timeout: 5m
keys:
  - key: ib_dev_change_me
    name: dev
    org: dev-org
    project: default
    allow: [fast]        # aliases this key may use
aliases:
  fast: llama3.2          # alias -> concrete model (NATS subject token)
```

A request's `model` field is looked up in `aliases`; the concrete model on
the right-hand side is what gets published to `inference.req.<model>` and
what a worker must have configured to serve. Requesting a concrete model name
directly (bypassing its alias) is a 404 unless that name is also an alias.

Worker (`deploy/worker.example.yaml`) — no database, no control-plane access,
just NATS and per-model engine config:

```yaml
worker_id: gpu-node-01
nats_url: nats://localhost:4222
models:
  - name: llama3.2          # concrete model name = NATS subject token
    engine: openai_http     # any OpenAI-compatible server (ollama, vllm, llama.cpp)
    url: http://host.docker.internal:11434
    max_inflight: 4
#  - name: claude-sonnet
#    engine: bifrost
#    provider: anthropic
#    upstream_model: claude-sonnet-5
#    api_key_env: ANTHROPIC_API_KEY
#    max_inflight: 8
```

Each model maps to one engine instance: `openai_http` talks to any
OpenAI-compatible HTTP server; `bifrost` routes through the embedded Bifrost
library to a named provider (OpenAI, Anthropic, Ollama, vLLM, ...), with the
provider's own API key read from the named environment variable. `url`
defaults to `host.docker.internal`, which reaches a model server running on
the Docker host from inside the worker's container (requires the compose
file's `extra_hosts: host-gateway` mapping); point it at an in-network
service name instead if your engine runs alongside the worker.

A worker's model list is read once at startup — there is no live reload.
Adding, removing, or reconfiguring a model requires restarting the worker
process.

`max_inflight` is not a per-worker concurrency knob: it maps directly to that
model's JetStream consumer `MaxAckPending`, which is a **fleet-wide** cap on
un-acked in-flight messages shared by every worker serving that model. If
multiple workers configure different `max_inflight` values for the same
model, whichever worker most recently (re)created the consumer wins for the
whole fleet — keep the value consistent across workers serving the same
model.

Harvester (`deploy/harvester.example.yaml`) — no HTTP config beyond its own
healthz/readyz probes; consumes `METERING`, writes ClickHouse, and projects
budgets:

```yaml
nats_url: nats://localhost:4222
clickhouse_dsn: "clickhouse://inferbus:inferbus@localhost:9000/inferbus"
batch_max_events: 500        # flush trigger: accumulated events (default 500)
batch_max_interval: 2s       # flush trigger: time since last flush (default 2s)
budget_refresh_interval: 10s # how often dirty BUDGETS KV entries are republished
                             # and month rollover is checked (default 10s).
                             # Budgets themselves arrive on a live KEYS watch.
```

The compose file already runs this as the `harvester` service, gated on both
`nats` and `clickhouse` being healthy; it points the same file at the
compose network with `INFERBUS_NATS_URL` and `INFERBUS_CLICKHOUSE_DSN`
(either environment variable overrides its config counterpart). The
endpoints in the file are the ports compose publishes to the host, so
running it standalone against that stack works as written:
`inferbus harvester -config deploy/harvester.example.yaml`.

## Wire contract

`internal/wire` is the single contract shared by gateway and worker.

| Subject | Direction | Purpose |
|---|---|---|
| `inference.req.<model>` | gateway → worker | `INFERENCE` work-queue stream, one consumer per concrete model |
| `inference.resp.<req_id>` | worker → gateway | core NATS, per-request response frames |
| `inference.cancel.<req_id>` | gateway → worker | core NATS, published on client disconnect |
| `metering.usage.<org>.<project>.<model>` | worker → harvester | `METERING` stream, usage accounting |

Response frames (`wire.Message`) carry a `kind`: `chunk` (an OpenAI SSE chunk,
verbatim), `done` (closes a streamed response, carries final usage), `result`
(a complete non-streamed response), or `error` (`{code, message,
http_status}`).

Workers ack their JetStream message on every terminal outcome — `done`,
`result`, `error`, or a cancellation — never on redelivery-eligible failures.
The consumer is configured with `MaxDeliver: 2`, so a worker crash mid-request
is retried exactly once; a canceled request is acked immediately and must
never redeliver. Cancellation has two phases: a still-queued message is
deleted from the stream before any worker picks it up; a mid-generation
request is stopped via the `inference.cancel.<req_id>` subject the worker
subscribes to while handling it. A `deadline` header on every request is the
backstop for both phases and for a gateway that disappears without sending a
cancel.

## Development

```sh
task test    # go test ./...
task build   # go build -o inferbus ./cmd/inferbus
task check   # go vet, go test -race, and a branding grep
```

The test suite runs against an embedded, in-process JetStream server
(`internal/testutil.RunNATS`) — no Docker or external services required.
CI runs vet, the race-enabled suite, a branding check, and a container build
on every push and pull request.

## Roadmap

| Milestone | Scope |
|---|---|
| **M3** | ✅ shipped — Control plane: event-sourced on [es-lite](https://github.com/laenenai/es-lite) with orgs/projects/keys/aliases, admin API, NATS KV projections watched live by gateways, OIDC — see [docs/design-controlplane.md](docs/design-controlplane.md) |
| **M4** | ✅ shipped — Usage pipeline: `harvester` consuming `METERING` into ClickHouse; budget ledger + gateway `402` enforcement via `BUDGETS` KV — see [docs/design-usage.md](docs/design-usage.md) |
| **M5** | Hardening: admission control from queue depth, request logging/metrics, docs |
| **v1.5** | Priority tiers, claim-check for large payloads |
| **v2** | Management console (see [the design mock](docs/design/console-mock)) |

## Contributing

Issues and pull requests are welcome. Before submitting a PR, run
`task check` locally — CI enforces the same gates. The design spec in
[docs/design.md](docs/design.md) is the authority for wire-contract and
architectural changes; propose spec changes in an issue first.

Licensed under [Apache-2.0](LICENSE).
