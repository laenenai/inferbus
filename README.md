# infbus

infbus is an open-source, OpenAI-compatible inference gateway built on
[NATS](https://nats.io) JetStream. Clients speak the standard OpenAI HTTP API
against a **gateway**, which resolves a caller's API key to an org/project and
translates a caller-facing **alias** (e.g. `fast`) to a concrete backend model,
then queues the request as a durable work item. A fleet of **workers** pulls
requests off that queue, runs them through a pluggable **engine** — a plain
OpenAI-compatible HTTP endpoint (Ollama, vLLM, llama.cpp) or the embedded
[Bifrost](https://github.com/maximhq/bifrost) multi-provider router — and
streams the response back to the gateway over core NATS, which relays it to
the client as SSE. Workers also emit per-request usage events onto a
`METERING` stream, destined for a ClickHouse-backed accounting pipeline. A
Postgres-backed control plane for keys/aliases/orgs is planned; today the
gateway's IAM and alias table are static YAML.

**Status:** pre-alpha (M2 — data path). See [docs/design.md](docs/design.md)
for the full design spec and milestone plan.

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
   ┌─────────┐  publish   ┌──────────────────────┐  pull   ┌────────┐  Chat/ChatStream  ┌───────────────────────┐
   │ gateway │──────────▶ │ JetStream INFERENCE  │────────▶│ worker │──────────────────▶│ engine                │
   │ auth    │            │ (work-queue,         │         │        │                   │ openai_http: Ollama,  │
   │ alias   │            │  consumer per model) │         │        │                   │ vLLM, llama.cpp       │
   │ resolve │            └──────────────────────┘         │        │                   │ bifrost: embedded     │
   │ admission (planned) │                                 │        │                   │ multi-provider router │
   └─────────┘ ◀──────────────── core NATS ─────────────────┘       └───────────────────────┘
        │        inference.resp.<req_id>  {chunk|done|result|error}      │
        ▼                                                                │ publish usage
      SSE to client                                                      ▼
                                                        JetStream METERING ──pull──▶ harvester (planned) ──▶ ClickHouse (planned)
                                                                                             ▲
                                                              NATS KV: MODELS (planned worker advertise)
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
| Postgres control plane + KV alias projection | planned (M3) |
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
an OpenAI-compatible engine on `localhost:11434` (e.g. Ollama running
`llama3.2`) — point it at any vLLM / llama.cpp / Ollama endpoint you have. The
example key `ib_dev_change_me` is allowed to use the alias `fast`, which
resolves to the concrete model `llama3.2`.

Postgres and ClickHouse containers are also defined in the compose file for
forward compatibility with the control plane and usage pipeline; nothing in
the code currently talks to them.

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
    url: http://localhost:11434
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
provider's own API key read from the named environment variable.

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
task build   # go build -o infbus ./cmd/infbus
task check   # go vet, go test -race, and a branding grep
```

The test suite runs against an embedded, in-process JetStream server
(`internal/testutil.RunNATS`) — no Docker or external services required.

Licensed under [Apache-2.0](LICENSE).
