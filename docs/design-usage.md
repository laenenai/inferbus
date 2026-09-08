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
                     │  budget ledger (limits ◀── CONTROL_EVENTS apikey  │
                     │                 usage  ◀── METERING)              │
                     │       └─▶ NATS KV BUDGETS {used, budget, exceeded}│
                     └───────────────────────────────────────────────────┘
                                              ▲ watch
 gateways (kv mode) ──────────────────────────┘   → 402 when exceeded

 controlplane /admin/v1/usage ──SQL──▶ ClickHouse MVs (optional clickhouse dsn)
```

- **`inferbus harvester`** (fourth role becomes real): one durable pull
  consumer on `METERING`; size- and time-bounded batches inserted into
  ClickHouse; ack after successful insert. At-least-once delivery +
  `ReplacingMergeTree` keyed on `req_id` = effectively-exactly-once rows.
- **Budget ledger:** the harvester also consumes `CONTROL_EVENTS`
  (`evt.*.apikey.>`) to learn each key's id and `monthly_token_budget`, and
  folds METERING usage into per-key month-to-date token counts. For every
  key with a non-zero budget it writes `BUDGETS` KV entries keyed by **key
  id**: `{"used": N, "budget": M, "exceeded": bool, "month": "2026-09"}`.
  Entries reset at month rollover. The ledger checkpoint (last applied
  positions/sequences) lives beside the data so restarts do not double-count:
  month-to-date usage is recomputed from ClickHouse at startup
  (`sum(total tokens) WHERE key_id AND month`), then maintained
  incrementally — ClickHouse is the ledger's source of truth, KV is its
  projection (rebuildable, never authoritative).
- **Gateway enforcement (kv mode only):** the gateway KV-watches `BUDGETS`
  (same watcher machinery as ALIASES/KEYS, same probe-based resilience).
  A key whose entry says `exceeded: true` gets
  `429`-family rejection: HTTP 402, OpenAI-style body
  `{"error":{"type":"budget_exhausted",...}}`, before publish. Absent
  entry = no budget = allow. Static mode has no budgets (unchanged).
- **Admin reads:** `GET /admin/v1/usage?org=&window=` on the controlplane,
  served by direct ClickHouse queries against the MVs — enabled only when
  the controlplane config sets `clickhouse_dsn`; 501 otherwise. The
  controlplane already owns databases; the data plane still touches none.

## 2. ClickHouse schema (owned by the harvester, DDL at startup)

- `usage_events` — `ReplacingMergeTree` ORDER BY `(org, project, key_id,
  ts, req_id)`, partition `toYYYYMM(ts)`, TTL 13 months. Columns mirror
  `wire.UsageEvent` (snake_case; enums as LowCardinality(String)).
- `usage_hourly_mv` → `usage_hourly` (SummingMergeTree): org, project,
  key_id, alias, model, provider, status, hour; sums of prompt/completion/
  cached tokens, request count, error count; TTL 13 months.
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
how often dirty BUDGETS entries are flushed to KV). Compose gains the
harvester service (depends on nats+clickhouse healthy). Controlplane config
gains optional `clickhouse_dsn`.

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
Grafana dashboards beyond a starter JSON, console (v2).
