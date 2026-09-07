# Management console — design mock

Static design artboards for the planned inferbus management console
(roadmap: v2). Each `*.dc.html` file is a self-contained Design Component
artboard; `canvas.json` lays them out on one canvas.

| Artboard | Screen |
|---|---|
| `Login.dc.html` | OIDC sign-in (no API-key sign-in — keys are data-plane only) |
| `Main.dc.html` | Usage dashboard (tokens by model/alias, queue latency, top keys) |
| `Keys.dc.html` | API-key management with alias allowlist editor |
| `Aliases.dc.html` | Global + per-org alias editor with NATS KV projection status |
| `Fleet.dc.html` | Worker fleet view from MODELS KV heartbeats |

Every screen maps 1:1 to the `/admin/v1/*` API defined in
[`docs/design.md`](../../design.md) §6.1/§12. The mock is design intent, not
implementation: the admin API and console land in milestones M3 and v2.
