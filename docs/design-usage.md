# inferbus usage pipeline — M4 design

**Status:** Approved design (2026-09-08). Refines [design.md](design.md) §10/§11
for the M4 milestone. One deliberate amendment to §11: **gateways do not read
ClickHouse** — budget state reaches them as a NATS KV projection, preserving
the M3 invariant that the data plane is NATS-only.

## 1. Shape

```
 workers ──publish──▶ JetStream METERING (metering.usage.>)
                            │ durable pull (harvester-main)
                     ┌── inferbus harvester ─────────────────────────────┐
                     │  batch insert ─▶ ClickHouse usage_events + MVs    │
                     │  budget ledger (limits ◀── NATS KV KEYS (watch)   │
                     │                 usage  ◀── METERING)              │
                     │       └─▶ NATS KV BUDGETS {used, budget, exceeded}│
                     └───────────────────────────────────────────────────┘
                                              ▲ watch
 gateways (kv mode) ──────────────────────────┘   → 402 when exceeded

 controlplane /admin/v1/usage ──SQL──▶ ClickHouse usage_events FINAL (optional dsn)
```

- **`inferbus harvester`** (fourth role becomes real): one durable pull
  consumer on `METERING`; size- and time-bounded batches inserted into
  ClickHouse; ack after successful insert. At-least-once delivery +
  `ReplacingMergeTree` = `usage_events` collapses a redelivered event back
  to one row on merge (and any read that needs exactness says `FINAL`).
  That guarantee is the *events table's*, not the rollup's — see §2.
- **Budget ledger:** the harvester watches the **`KEYS` NATS KV bucket**
  (the same bucket, and the same watch machinery, gateways use for auth) to
  learn each key's id and `monthly_token_budget`, and folds METERING usage
  into per-key month-to-date token counts. One watch, no second durable
  consumer; a KEYS entry with no `id` carries nothing to attribute a budget
  to and is skipped. For every key with a non-zero budget it writes
  `BUDGETS` KV entries keyed by **key id**:
  `{"used": N, "budget": M, "exceeded": bool, "month": "2026-09"}`.
  Entries reset at month rollover. There is no persisted ledger checkpoint,
  and none is needed: month-to-date usage is recomputed from ClickHouse
  (`sum(total tokens) WHERE key_id AND month`) at startup, at every month
  rollover, and hourly as a re-baseline, and maintained incrementally in
  between — so a restart cannot double-count. ClickHouse is the ledger's
  source of truth, KV is its projection (rebuildable, never
  authoritative).
- **Gateway enforcement (kv mode only):** the gateway KV-watches `BUDGETS`
  (same watcher machinery as ALIASES/KEYS, same probe-based resilience).
  A key whose entry says `exceeded: true` gets
  `429`-family rejection: HTTP 402, OpenAI-style body
  `{"error":{"type":"budget_exhausted",...}}`, before publish. Absent
  entry = no budget = allow. Static mode has no budgets (unchanged).
- **Admin reads:** `GET /admin/v1/usage?org=&window=` on the controlplane,
  served by a direct ClickHouse query against `usage_events` (`FINAL`, so
  the read inherits the events table's deduplication — §2 explains why it
  is not the hourly rollup), bounded by a server-side timeout, a
  `max_execution_time` and a row limit. Enabled only when the controlplane
  config sets `clickhouse_dsn`; 501 otherwise, including when a configured
  ClickHouse is unreachable at startup — an optional analytics dependency
  never blocks the control plane from serving. The controlplane already
  owns databases; the data plane still touches none.

## 2. ClickHouse schema (owned by the harvester, DDL at startup)

- `usage_events` — `ReplacingMergeTree` ORDER BY `(org, project, key_id,
  ts, req_id)`, partition `toYYYYMM(ts)`, TTL 13 months. Columns mirror
  `wire.UsageEvent` (snake_case; enums as LowCardinality(String)).
- `usage_hourly_mv` → `usage_hourly` (SummingMergeTree): org, project,
  key_id, alias, model, provider, status, hour; sums of prompt/completion/
  cached tokens, request count, error count; TTL 13 months. **This rollup
  is approximate under redelivery.** A materialized view is an INSERT
  trigger: it fires on every physical insert block, before and independent
  of any merge, and `SummingMergeTree` has no notion of deduplication — so
  a redelivered event that `usage_events` collapses is counted twice here,
  permanently. It exists for dashboards, where speed beats exactness; the
  admin endpoint deliberately reads `usage_events` instead.
- Month-to-date budget reads use `usage_events` directly
  (`sum(prompt_tokens+completion_tokens) ... FINAL`-free: the harvester
  dedups by req_id before insert within a batch, and ReplacingMergeTree
  handles cross-batch replays; budget sums tolerate the rare pre-merge
  duplicate — budgets are advisory throttles, not invoices).

## 3. Attribution fix (prerequisite)

`cpkv.KeyEntry` gains `Id`; the KEYS projector writes it; the gateway sends
the key **id** (not name) in `Ib-Key-Id`, so `UsageEvent.KeyID` becomes a
stable aggregate id. Key NAMES stay display-only. This is additive to KV
values (old entries without id are tolerated during rollout).

## 4. Config & deployment

`inferbus harvester -config deploy/harvester.example.yaml`:
`nats_url`, `clickhouse_dsn` (required), `batch_max_events` (default 500),
`batch_max_interval` (default 2s), `budget_refresh_interval` (default 10s,
how often dirty BUDGETS entries are flushed to KV and month rollover is
checked — budgets themselves arrive on the KEYS watch, not on this tick).
`INFERBUS_NATS_URL` and `INFERBUS_CLICKHOUSE_DSN` override the two
endpoints, which is how the compose service points the host-reachable
example config at compose hostnames. Compose gains the harvester service
(depends on nats+clickhouse healthy). Controlplane config gains optional
`clickhouse_dsn`.

## 5. Testing

ClickHouse-independent core: the CH writer sits behind a small `Sink`
interface (`InsertBatch(ctx, []Row) error`, `MonthToDate(ctx, keyID,
month) (int64, error)`); unit + embedded-NATS tests use an in-memory fake.
Env-gated integration (`CP_TEST_CH_DSN`) covers real DDL, inserts, MV sums,
and month-to-date reads. The budget path gets a full in-process e2e without
ClickHouse (fake Sink): create key with tiny budget → chat until exceeded →
gateway 402 → raise budget via admin → chat succeeds.

## 6. Out of scope (unchanged)

Currency budgets, per-request price tables, admission control (M5),
Grafana dashboards (planned, not built), console (v2).
