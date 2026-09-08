# inferbus hardening + launch — M5 design

**Status:** Approved design (2026-09-08). Implements [design.md](design.md) §16
M5 and folds in the worker-registration improvements (zero-config worker,
MODELS KV advertisement, workers listing). Refines §6.1 step 4 (admission)
and §6.2/§5's MODELS KV sketch to as-built shape.

## 1. Shape

```
 client ──▶ gateway ── admission: backlog(model) > max? ──▶ 429 Retry-After
                │ (consumer info, cached ~1s)
                ▼
        JetStream INFERENCE ──pull──▶ workers
                                        │ heartbeat every 15s
                                        ▼
                              NATS KV MODELS (bucket TTL 45s)
                                        ▲
        controlplane GET /admin/v1/workers (platform admins)

 inferbus worker -engine http://localhost:11434     # zero-config mode
   └── GET <url>/v1/models at startup → serve all discovered models
```

## 2. Admission control (gateway)

- New gateway config block: `admission: {max_backlog: 256,
  retry_after_seconds: 2, overrides: {<model>: N}}`. `max_backlog: 0`
  disables admission entirely (default when the block is absent —
  behavior-preserving for existing configs).
- Check order: authenticate → allowlist → budget → **admission** → publish.
- Backlog source: `jetstream.Consumer` info for durable
  `model-<slug>` on `INFERENCE` (`NumPending + NumAckPending`), cached
  per-model for 1s (spec §6.1 step 4). Info fetch errors and
  missing-consumer (no worker yet) **allow** the request — admission
  protects against backlog, not absence; the existing deadline path
  handles no-worker.
- Rejection: HTTP 429, OpenAI-style body
  `{"error":{"type":"overloaded","message":"model queue is full, retry later"}}`,
  `Retry-After: <retry_after_seconds>`.

## 3. Worker advertisement (MODELS KV)

- Contract lives in `internal/wire` (data-plane; workers stay cpkv-free):
  bucket `MODELS`, key `<worker_id>`, value JSON
  `{"worker_id":…, "models":[{"name":…, "engine":…, "max_inflight":…}],
  "started_at":RFC3339, "last_seen":RFC3339}`.
- Bucket created idempotently with **TTL 45s**; each worker re-puts its
  entry every **15s** (3 missed beats ⇒ entry expires ⇒ worker gone).
  Advertisement is best-effort: KV errors are logged, never fatal, and
  the data path does not depend on the bucket.
- `worker_id` defaults to `<hostname>-<4 random hex>` when unset (config
  or zero-config mode).
- Restart-required for model-list changes is unchanged and documented;
  the KV entry reflects the running config.

## 4. Zero-config worker

`inferbus worker -engine <url> [-nats <url>] [-max-inflight N]`
(mutually exclusive with `-config`):

- Startup discovery: `GET <url>/v1/models` (OpenAI-compatible — Ollama,
  vLLM, llama.cpp all serve it); every returned model id becomes
  `ModelConfig{Name: id, Engine: "openai_http", URL: <url>,
  MaxInflight: N (default 4)}`. Model ids that fail `wire.Slug`
  validation are skipped with a warning (subject safety).
- Discovery failure or zero usable models: fatal at startup with a clear
  error. Discovery runs once; restart to pick up engine changes.
- Aliases still gate what clients may call — a zero-config worker
  serving `llama3-2` is unreachable until an alias targets it.

## 5. Workers listing (controlplane)

`GET /admin/v1/workers` — reads the MODELS bucket, returns
`{"workers":[<entries>]}` sorted by worker_id. **Platform admins only**
(fleet topology is infra-wide, not org-scoped; relax later if the v2
console needs it). NATS is always present, so no 501 path.

## 6. Metrics floor (gateway + harvester)

Minimal Prometheus text endpoints for launch — `prometheus/client_golang`,
`GET /metrics` on the existing HTTP listeners:

- gateway: `inferbus_requests_total{route,code}`,
  `inferbus_admission_rejected_total{model}`,
  `inferbus_inflight_requests`.
- harvester: `inferbus_usage_rows_inserted_total`,
  `inferbus_insert_failures_total`, `inferbus_budget_entries{state}`.

Worker and controlplane metrics are deferred (documented). No OTEL.

## 7. E2E hardening

- **Redelivery:** a fetcher grabs a request and never acks (short
  `AckWait`); `MaxDeliver: 2` redelivers to a healthy FakeEngine worker;
  the client's request completes. Asserts the at-least-once contract
  end to end.
- **429 backlog:** admission `max_backlog: 1`, no worker (consumer
  pre-created so info exists), two queued requests → third gets 429 with
  `Retry-After`; draining the queue restores 200s.
- **Zero-config:** httptest engine serving `/v1/models` + chat; worker
  started via the flag path serves a request; MODELS entry appears and
  expires after the worker stops.

## 8. Release (v0.1.0)

- `CHANGELOG.md` (Keep-a-Changelog style, M1–M5 summarized).
- Tag `v0.1.0` on the merge commit; GitHub Actions `release.yaml` on
  `v*` tags: plain `go build` matrix (linux/darwin × amd64/arm64),
  artifacts attached to the GitHub release. No goreleaser (zero new
  tooling).
- `deploy/grafana-usage.json`: starter dashboard (tokens by
  model/alias over time, requests, error rate — ClickHouse datasource),
  README pointer. Fulfils the "planned, not built" note from M4.

## 9. Out of scope (unchanged)

Priority tiers, claim-check blobs, shared rate counters (V1.5);
functional console (V2); worker live-reload; OTEL; currency budgets.
