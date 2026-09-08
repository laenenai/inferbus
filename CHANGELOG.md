# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-09-08

First public release.

### Data path (M2)
- Gateway: OpenAI-compatible `/v1/chat/completions` (streaming + non-streaming), static YAML key/alias auth.
- Worker: pulls `INFERENCE` work-queue items, runs them through `openai_http` (any OpenAI-compatible server) or the embedded Bifrost multi-provider engine.
- Three-phase cancellation: queued-delete, mid-stream cancel subject, deadline backstop; `MaxDeliver: 2` for exactly-once-retry redelivery.

### Control plane (M3)
- Event-sourced admin API on [es-lite](https://github.com/laenenai/es-lite) with Postgres as the durable log — orgs, projects, keys, aliases.
- Live `ALIASES`/`KEYS` NATS KV projections watched by gateways in `iam.mode: kv`; OIDC on admin routes, bootstrap token for dev/CI.

### Usage & budgets (M4)
- Harvester: consumes `METERING`, batches inserts into ClickHouse (`usage_events`, `usage_hourly` rollup).
- Budget ledger folds usage into the `BUDGETS` KV bucket; a kv-mode gateway rejects an exhausted key with `402`.

### Hardening (M5)
- Admission control: gateway `admission: {max_backlog, retry_after_seconds, overrides}`, per-model JetStream backlog with a 1s cache; 429 + `Retry-After`, `error.type "overloaded"`. Disabled by default; `overrides` without `max_backlog` logs a startup warning.
- `MODELS` KV: workers best-effort advertise `{worker_id, models, started_at, last_seen}` (15s heartbeat / 45s TTL); model changes still require a worker restart.
- Zero-config worker: `inferbus worker -engine <url> [-nats <url>] [-max-inflight N]` discovers models via `/v1/models` at startup.
- `GET /admin/v1/workers` — platform-admin fleet listing sourced from the `MODELS` KV bucket.
- Prometheus `/metrics` on the gateway (`inferbus_requests_total{route,code}`, `inferbus_admission_rejected_total{model}`, `inferbus_inflight_requests`) and the harvester (`inferbus_usage_rows_inserted_total`, `inferbus_insert_failures_total`, `inferbus_budget_entries{state}`). The worker and control plane expose no `/metrics`.
- E2E coverage: worker-crash redelivery, admission-control 429 under synthetic backlog, zero-config worker discovery.
- Grafana starter dashboard (`deploy/grafana-usage.json`) over `usage_events`: tokens/hour by model, requests/hour by alias, error rate.

[0.1.0]: https://github.com/laenenai/inferbus/releases/tag/v0.1.0
