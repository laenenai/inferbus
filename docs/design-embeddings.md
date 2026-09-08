# inferbus embeddings + alias params — M6 design

**Status:** Approved design (2026-09-08). Delivers [design.md](design.md) §2's
`/v1/embeddings` goal (declared in v1's scope, never built through M5) and
activates the alias `params` map that
[design-controlplane.md](design-controlplane.md) §2 defined and projected but
that nothing has ever applied. Ships as **v0.2.0**.

## 1. Shape

```
 client ──POST /v1/embeddings──▶ gateway
                                  │ auth → allowlist → budget → admission
                                  │ resolve alias → {target, params}
                                  │ rewrite body.model, merge params
                                  ▼
                    JetStream INFERENCE  (Ib-Kind: embed)
                                  │
                              worker ──▶ Engine.Embed ──▶ vLLM / Ollama / Bifrost
                                  │
                     core NATS: ONE `result` frame (no streaming)
                                  │ publish usage {kind: "embed"}
                                  ▼
                          METERING ──▶ harvester ──▶ ClickHouse
```

Everything except the three new pieces (an `Embed` engine method, a worker
`embed` branch, a gateway handler) is reused verbatim: IAM, allowlists,
budgets/402, admission/429, the `result` response frame, cancellation,
metering, and the ClickHouse schema — `wire.UsageEvent.Kind` has documented
`chat|embed` since M1 and only ever carried `chat`.

## 2. Wire contract (no breaking change)

- Requests carry the existing `Ib-Kind` header (`wire.HdrKind`): `chat`
  (default when absent — old gateways/workers keep interoperating) or
  `embed`.
- Embedding responses use exactly one `wire.KindResult` frame, then the
  reply subject is done. No `chunk`, no `done`. `stream: true` in an
  embeddings body is rejected by the gateway with 400 (`invalid_request`);
  the OpenAI embeddings API has no streaming form.
- A worker that receives `Ib-Kind: embed` for a model whose engine cannot
  embed returns a `KindError` frame with code `unsupported_kind`, mapped by
  the gateway to 400.

## 3. Engine seam

`engine.Engine` gains one method:

```go
// Embed returns an OpenAI-shaped embeddings response body. Engines that
// cannot embed return engine.ErrUnsupported.
Embed(ctx context.Context, model string, body json.RawMessage) (json.RawMessage, wire.Usage, error)
```

- **openai_http:** POSTs the body verbatim to `<base>/v1/embeddings`,
  reusing the existing model-rewrite and upstream-error mapping (401/403/404
  → 502, the rest passed through). Works against vLLM, Ollama, llama.cpp,
  MLX, TEI.
- **bifrost:** maps to Bifrost's `EmbeddingRequest` (`core` v1.8.4), with
  the same provider/upstream-model config the chat path uses.
- Usage: embeddings report `prompt_tokens` only; `completion_tokens` is
  always 0. Engines that report no usage fall back to 0 (same
  `estimated` semantics chat already has).

## 4. Alias params (the "resolutions" feature)

`cpkv.AliasEntry.Params` (M3, stored and projected, never read) becomes
live. The `iamProvider` seam changes shape once:

```go
// was: ResolveAlias(org, alias string) (target string, ok bool)
ResolveAlias(org, alias string) (Resolution, bool)

type Resolution struct {
	Target string
	Params map[string]string // nil in static mode
}
```

- **Merge rule:** after `model` is rewritten to `Resolution.Target`, each
  `Params` entry is set on the request body, **overriding** any client
  value. The operator owns the alias contract; a client cannot escape it.
  Applied identically to chat and embeddings — `params` was always a
  general alias feature (temperature/top_p for chat, `dimensions`/
  `encoding_format` for embeddings).
- **Typing:** KV values are strings. Each value is parsed as JSON; if it
  parses to a JSON scalar/array/object it is inserted as that type
  (`"512"` → `512`, `"true"` → `true`, `"[1,2]"` → `[1,2]`), otherwise as
  a JSON string (`float` → `"float"`). One rule, no per-key schema.
- **Reserved:** `model` and `stream` are refused as param keys — at the
  admin API (400) and defensively at merge time (skipped + warned) — since
  they would break alias resolution and the response protocol.
- **Named resolutions, the motivating case:** one worker serving
  `qwen3-embedding`, exposed as two aliases — `embed-hd`
  (`params: {dimensions: "1024"}`) and `embed-compact`
  (`params: {dimensions: "256"}`). Clients pick a vector size by alias and
  never send the parameter.

**Engine support is not uniform, and inferbus does not paper over it:**
vLLM honors `dimensions` for Matryoshka-trained models and 400s otherwise;
Bifrost forwards it to remote providers; **Ollama silently ignores it and
returns the model's native size**. Documented in README with the advice to
validate returned vector length when targeting Ollama. inferbus never
truncates or renormalizes vectors itself — that would silently change
embedding semantics.

## 5. Gateway endpoint

`POST /v1/embeddings`, OpenAI-compatible: `{model, input, dimensions?,
encoding_format?, user?}` where `input` is a string or array of strings.

- Same pipeline order as chat: ready-gate → authenticate → allowlist →
  budget (402) → admission (429) → publish → await one `result`.
- Body limit, panic recovery, request-id, deadline header, cancel-on-
  disconnect: all inherited from the shared request path.
- Metrics: `route="embeddings"` on `inferbus_requests_total`; admission
  rejections carry the concrete model as today.
- `GET /v1/models` still lists aliases (chat and embedding aliases alike —
  the OpenAI API has no per-kind listing).

## 6. Testing

Unit: `Embed` on both engines (httptest upstream + error mapping), param
merge table (typing, override-client, reserved keys), worker embed branch,
gateway handler (400 on `stream: true`, 402/429 ordering unchanged).
E2E: full-stack embeddings through gateway → NATS → worker → fake engine,
asserting the vector body reaches the client and a `kind="embed"` usage
event lands; a second e2e proves two aliases over one model produce two
different `dimensions` on the wire. Live validation against a real
embedding engine is opportunistic (an Ollama `nomic-embed-text` on a
Spark), never gating.

## 7. Out of scope

Vector truncation/renormalization in inferbus; per-kind key permissions
(alias allowlists already gate access); embedding-specific budgets
(tokens are tokens); `/v1/rerank`; batch/async embedding jobs.
