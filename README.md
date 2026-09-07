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

**Status:** pre-alpha (M2 — data path). See [docs/design.md](docs/design.md)
for the full design spec and milestone plan, and
[docs/design/console-mock](docs/design/console-mock) for the management
console design (Design Component artboards for the planned v2 console).

## Architecture

```
                      control plane (planned, M3)
   console (v2) ──OIDC──▶ admin API ──▶ Postgres (orgs, projects, keys, aliases)
                          │                        │ write-through projection
                          │                        ▼
                          │              NATS KV: ALIASES ◀── watch ── gateways
                          └──────────────────────────────────────────────────

   client (OpenAI SDK)
        │ HTTP/SSE
        ▼
   ┌───────────┐  publish  ┌──────────────────────┐  pull  ┌────────┐  Chat/ChatStream ┌───────────────────────┐
   │ gateway   │──────────▶│ JetStream INFERENCE  │───────▶│ worker │─────────────────▶│ engine                │
   │  auth     │           │ (work-queue,         │        │        │                  │ openai_http: Ollama,  │
   │  alias    │           │  consumer per model) │        │        │                  │  vLLM, llama.cpp      │
   │  resolve  │           └──────────────────────┘        │        │                  │ bifrost: embedded     │
   └───────────┘                                           │        │                  │  multi-provider router│
        │  ▲                                               │        │                  └───────────────────────┘
        │  └──────────────── core NATS ────────────────────┘        │
        │      inference.resp.<req_id> {chunk|done|result|error}    │ publish usage
        ▼                                                           ▼
      SSE to client                    JetStream METERING ──pull──▶ harvester (planned) ──▶ ClickHouse (planned)
```

**Data plane (built).** The gateway authenticates the caller's API key against
a static YAML key list, resolves the request's model alias to a concrete
model via a static YAML alias map, and publishes the request onto the
`INFERENCE` JetStream stream (work-queue retention, one durable consumer per
concrete model). A worker pulls the message, runs it through its configured
engine, and streams response frames back over a per-request core-NATS
subject; the gateway relays those frames to the client, either as SSE
(`stream: true`) or as a single JSON body. Client disconnects and per-request
deadlines are handled as cancellation (see Wire contract below).

**Usage plane (partially built).** Every worker request — success, error, or
cancellation — produces a `wire.UsageEvent` published to the `METERING`
stream, keyed by org/project/model. This side is implemented in the worker
today. The **harvester** that consumes `METERING` and writes it into
ClickHouse, and the budget/dashboard reads on top of it, are planned (M4).

**Control plane (planned).** The design calls for a Postgres-backed store of
orgs, projects, API keys, and aliases, an OIDC-gated admin HTTP API, and a
projection of the alias table into a NATS KV bucket that gateways watch for
live updates. None of this exists yet: keys and aliases are loaded once from
gateway YAML at startup. Workers never talk to the control plane or to
Postgres — they need only a NATS URL and their engine endpoints.

## Status

| Area | Status |
|---|---|
| Chat completions (`/v1/chat/completions`, streaming + non-streaming) | done |
| Embeddings (`/v1/embeddings`) | planned |
| Cancel / deadline semantics (queued-delete, mid-stream cancel, deadline backstop) | done |
| Bifrost engine (multi-provider: OpenAI, Anthropic, Ollama, ...) | done |
| Static YAML keys/aliases | done |
| Postgres control plane + KV alias projection | done — [see design-controlplane.md](docs/design-controlplane.md) |
| Harvester + ClickHouse usage pipeline | planned (M4) |
| Admission control / priority tiers | planned (M5 / v1.5) |
| OIDC admin API + console | planned (v2) |

## Quickstart

```sh
docker compose -f deploy/docker-compose.yaml up -d --build
curl http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer ib_dev_change_me" \
  -H "Content-Type: application/json" \
  -d '{"model":"fast","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

The compose file brings up NATS (JetStream), the gateway (port 8080), and one
worker. The worker's example config (`deploy/worker.example.yaml`) points at
`http://host.docker.internal:11434` so the containerized worker can reach a
model server (e.g. Ollama running `llama3.2`) on the Docker host; if your
engine runs in-network instead (its own compose service, or another
container), change that URL to the service's name. The example key
`ib_dev_change_me` is allowed to use the alias `fast`, which resolves to the
concrete model `llama3.2`.

**Change the example key before exposing the gateway to anything but your own
machine** — `ib_dev_change_me` is a public, checked-in credential.

Postgres and ClickHouse containers are also defined in the compose file for
forward compatibility with the control plane and usage pipeline.

## Control plane (optional)

The quickstart above uses static YAML keys and aliases. To enable the
Postgres-backed control plane instead:

1. Start the full stack (control plane service starts automatically):
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

3. Set an alias:
```sh
curl -X PUT http://localhost:8081/admin/v1/aliases/acme/fast \
  -H "Authorization: Bearer dev_admin_change_me" \
  -H "Content-Type: application/json" \
  -d '{"target":"llama3.2"}'
```

4. Create an API key:
```sh
curl -X POST http://localhost:8081/admin/v1/keys \
  -H "Authorization: Bearer dev_admin_change_me" \
  -H "Content-Type: application/json" \
  -d '{"org":"acme","project":"default","name":"dev","allow":["fast"]}'
```
The response includes the plaintext key (shown once); use it in the data-plane
chat curl instead of `ib_dev_change_me`.

5. Switch the gateway to KV mode by editing `deploy/gateway.example.yaml`:
Replace the entire `keys:` and `aliases:` blocks with:
```yaml
iam:
  mode: kv
```

Then restart the gateway and try the same chat curl with your created key.

**Note:** KV mode forbids a static `keys:` list in the config and will error
on startup if found. The gateway `/readyz` endpoint gates on IAM sync. To
re-enable static mode, revert the gateway config and restart.

## Configuration

Gateway (`deploy/gateway.example.yaml`) — static IAM and alias table:

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

`max_inflight` is not a per-worker concurrency knob: it maps directly to that
model's JetStream consumer `MaxAckPending`, which is a **fleet-wide** cap on
un-acked in-flight messages shared by every worker serving that model. If
multiple workers configure different `max_inflight` values for the same
model, whichever worker most recently (re)created the consumer wins for the
whole fleet — keep the value consistent across workers serving the same
model.

## Wire contract

`internal/wire` is the single contract shared by gateway and worker.

| Subject | Direction | Purpose |
|---|---|---|
| `inference.req.<model>` | gateway → worker | `INFERENCE` work-queue stream, one consumer per concrete model |
| `inference.resp.<req_id>` | worker → gateway | core NATS, per-request response frames |
| `inference.cancel.<req_id>` | gateway → worker | core NATS, published on client disconnect |
| `metering.usage.<org>.<project>.<model>` | worker → (future) harvester | `METERING` stream, usage accounting |

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
| **M4** | Usage pipeline: `harvester` consuming `METERING` into ClickHouse; budget enforcement reads |
| **M5** | Hardening: admission control from queue depth, request logging/metrics, docs |
| **v1.5** | Priority tiers, claim-check for large payloads |
| **v2** | Management console (see [the design mock](docs/design/console-mock)) |

## Contributing

Issues and pull requests are welcome. Before submitting a PR, run
`task check` locally — CI enforces the same gates. The design spec in
[docs/design.md](docs/design.md) is the authority for wire-contract and
architectural changes; propose spec changes in an issue first.

Licensed under [Apache-2.0](LICENSE).
