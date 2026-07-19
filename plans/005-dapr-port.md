# 005 — Dapr Sibling Runtime (shared core extraction + polyglot processing)

Status: **APPROVED** — restructure waves (W0–W3) touch `orleans/` under the hard gate below; Dapr
waves (W4–W9) are additive. Supersedes any notion of "fork the Host and edit" — the two runtimes
share one semantic core and one API surface, or the port is not worth doing.

**Hard gate (every commit):** `~/.dotnet/dotnet test orleans/StreamForge.sln` — all 511 tests green
with test `.cs` files **unmodified** (`git diff --stat orleans/tests -- '*.cs'` empty). Test
*csproj* files may change ProjectReference paths only.

## Problem

StreamForge exists in one flavor: Orleans grains, one process, .NET-only participants. Client
conversations keep hitting two walls:

1. **Polyglot processing.** Teams want to attach Java/Kotlin/Python/TypeScript processors to the
   platform's streams. Orleans streams are a .NET-internal transport; the only polyglot door today
   is the typed-gRPC client path (consume-only, and .NET-toolchain-generated).
2. **Runtime comparison.** "Why Orleans and not Dapr?" deserves a measured answer, not a shrug —
   the same platform on both runtimes, same SPA, same seeded pipeline, published latency numbers,
   and an honest decision matrix for which building blocks fit which constraints.

Dapr is the natural counterpart: actors mirror grains conceptually, and pub/sub is a
language-neutral stream transport — any process with a sidecar can publish into or subscribe out
of the platform. The port is only credible if it is the *same platform*: same SQL engine, same
Z-set table semantics, same REST/SignalR contract, the existing console SPA running against it
unchanged.

## Current state (what already works, don't rebuild)

The repo was layered for exactly this (DESIGN.md D1 — "the single most load-bearing decision"):

| Asset | Orleans coupling | Reuse verdict |
|---|---|---|
| `StreamForge.Engine` (SQL compiler, both executors, Z-set ops, Dataflow primitives; ~393 tests) | **none** | move to `shared/` wholesale |
| `Abstractions/Models.cs` + `StreamConstants.cs` (all DTOs) | serialization attrs only | move to `shared/StreamForge.Contracts` (attrs kept — see D-A below) |
| `Abstractions/GrainInterfaces.cs` | `IGrainWithStringKey` | stays Orleans-side; interfaces re-based onto shared facades |
| Host `Grpc/Dynamic/*` (descriptor factory, wire encoder, proto builder, field-number map) — all except the two `using Orleans` services | none (grep-verified) | move to `shared/StreamForge.AppCore` |
| Host `Search/TableSearchIndex`, `Generators/MarketDataProfiles`, `Grains/TableRowHistory.cs` (retention math), `Auth/PasswordHasher` | none | move to `shared/StreamForge.AppCore` |
| Host `Api/*Endpoints.cs`, `Hubs/StreamHub`, `Auth/JwtTokenService`, JWT/policy/CORS wiring in `Program.cs` | reach grains via `IClusterClient` only | move to `shared/StreamForge.Api` behind runtime-neutral facades |
| `web/` SPA (relative URLs + vite proxy; frozen contract `types.ts` ⇔ `Dtos.cs`) | none | move to repo root; both hosts serve the same `web/dist` |
| Registry/User seed catalogs | embedded in grains | extract data to `AppCore/SeedCatalog`; both runtimes seed the same demo world |

Verified enablers: no test touches endpoints or `Program.cs`;
`Orleans.GenerateCodeForDeclaringAssemblyAttribute` exists in
`Microsoft.Orleans.Serialization.Abstractions 10.2.1` (the version in use);
`DynamicDescriptorSet(IRegistryGrain)` is the one test-visible ctor, handled by facade inheritance.

## Decisions

**D-A — Contracts keep Orleans attributes; codegen bridges from the Orleans side.**
`shared/StreamForge.Contracts` references `Microsoft.Orleans.Serialization.Abstractions` (attribute
types only — no runtime, no analyzers) plus `<Using Include="Orleans"/>` so `Models.cs` moves
byte-identical. The Orleans-side `StreamForge.Abstractions` keeps `Microsoft.Orleans.Sdk` and adds
`[assembly: Orleans.GenerateCodeForDeclaringAssembly(typeof(SourceDefinition))]`, so serializers
for the shared DTOs are generated into the Orleans assembly. *Tradeoff*: the Dapr flavor carries a
benign attribute-package dependency; the alternative (≈22 hand-maintained surrogate types with
parallel `[Id]` maps under a field-numbers-are-forever rule) is exactly the drift this repo guards
against. Rejected.

**D-B — One API surface, two hosts, facade seam.** Runtime-neutral facade interfaces live in
Contracts (`ICatalogFacade`, `IUserStoreFacade`, `IPipelineReadFacade`, `ITableReadFacade`,
`ITableHistoryFacade`, `IArrangementMetaFacade`); grain interfaces **inherit** them (so existing
test fakes still compile — zero test edits); endpoints, `StreamHub`, JWT service and the
auth/policy/CORS/OpenAPI wiring move to `shared/StreamForge.Api`
(`AddStreamForgeApi`/`MapStreamForgeApi`). Orleans registers facades as grain-proxy singletons +
a thin keyed adapter; Dapr registers actor-proxy adapters. The frozen contract
(`Dtos.cs` ⇔ `web/src/api/types.ts`) is now enforced by construction: both hosts run the same
endpoint code.

**D-C — Namespaces frozen in place.** Moved files keep their namespaces verbatim — including
`StreamForge.Host.Grpc.Dynamic` and `StreamForge.Host.Search` living inside shared assemblies.
Ugly, deliberate: 20+ test files import these namespaces and the hard gate forbids touching them.
A cosmetic rename is deferred work requiring explicit sign-off.

**D-D — Dapr streams = four fixed envelope topics + dynamic egress.** Dapr subscriptions are
declared at app start; per-entity topics for dynamically-created entities can't be subscribed
without restart. So the internal transport is fixed topics carrying envelopes (additive records in
Contracts): `sf-sources` `{source, events[]}`, `sf-pipeline-out` `{pipelineId, rows[]}`,
`sf-table-delta` `{table, seq, deltas[]}`, `sf-lifecycle`, `sf-metrics` — mirroring Orleans'
`(namespace, key)` streams. Generators *additionally* publish per-source egress topics
`sf-source-{name}` (publish-only, so dynamism is safe). **Polyglot door:** any sidecar'd process
may subscribe egress topics and publish enveloped events into `sf-sources`; the router treats them
identically to generator output. `JsonElement` values are normalized at every topic ingress
(shared `JsonValueNormalizer`) before rows reach the Engine.

**D-E — Actors mirror grains; timers, not reminders; Redis state.** Registry/UserStore/Generator/
Pipeline/Table/TableHistory actors, turn-based like grains. Timers + a ported
`GeneratorSupervisorService` (reminders would add persistent-state churn for nothing in a
single-process dev topology). Generator ticks batch events at ≤20 Hz — a per-event sidecar
round-trip would be dishonest. State store = Redis from `dapr init` with a scoped key prefix;
"delete `data/` to reseed" becomes `dapr/tools/reset.sh`. The `RegistryGrain` `[MayInterleave]`
lesson maps to Dapr actor reentrancy — resolve in W4 (enable reentrancy config or keep
orchestration acyclic) before pipeline/table waves build on it.

**D-F — Honest descopes over half-correct ports.** Partitioned execution (Parallelism 2–16,
frontier-consistent reads, shared arrangements) is **Orleans-only**: the stage grid exists to
tighten p99 via µs-scale in-process grain calls at a 250 ms epoch cadence; on Dapr every
`(edge, epoch, partition)` batch is two sidecar hops (~0.5–2 ms), which inverts the economics on a
single node. The Dapr registry rejects `Parallelism > 1` with a clear error. gRPC *serving* is
phase 2 (`/proto` downloads work day one — the descriptor machinery is shared); `/api/meta/grpc`
keeps its response shape with an empty static-service list. `/docs` stays Orleans-served.

## Target architecture (Dapr flavor)

```
GeneratorActor ──batch──▶ sf-sources ─────────────┐            sf-source-{name} (egress)
   (timer)                    │                   │                  │
                              ▼                   ▼                  ▼
                        TopicRouter ──▶ PipelineActor        python enricher / bun consumer /
                        (host app       TableActor            any sidecar'd process
                         subscribes)      │    │                     │
                              ▲           │    └─▶ sf-table-delta ◀──┘ (publish into sf-sources)
                              │           └──────▶ sf-pipeline-out
   external publishers ───────┘                        │
                                                       ▼
                          DaprStreamBridge ──▶ SignalR /hubs/stream ──▶ same SPA (:5399)
```

One process (`StreamForge.Dapr.Host` + sidecar): REST/SignalR/SPA on **:5399**, gRPC reserved
**:5499**, sidecar HTTP/gRPC on 3599/4599. Ports 5199/5299 never touched.

## Phases

Ownership is exclusive per concurrent agent; csproj/sln files are owned by W0 (orchestrator) so no
two agents ever edit them. Commits `005-Wn: …`; push after each stable wave.

### W0 — Solution restructure skeleton (orchestrator, serial)
`git mv` Engine → `shared/StreamForge.Engine`; create `shared/{Contracts,AppCore,Api}` csproj
skeletons; re-point `orleans/StreamForge.sln` + test csproj ProjectReference paths.
**Acceptance:** build + 511 green; working tree clean.

### W1 — Contracts split + facades (~1 day, exclusive Contracts/Abstractions ownership)
Commit (a): move `Models.cs`/`StreamConstants.cs`, codegen bridge attribute. Commit (b):
`Facades.cs`, grain interfaces inherit facades, `DynamicDescriptorSet` ctor →
`ICatalogFacade`, streaming envelope records (additive).
**Acceptance:** 511 green after each commit — the TestCluster suites prove cross-assembly
serializer + inherited-proxy codegen. Fallback if codegen balks: duplicate facade methods on grain
interfaces + tiny Host adapters (still zero test edits).

### W2 — AppCore extraction ∥ web move (2 agents)
**A (AppCore):** move the Orleans-free Host files (table above), extract `SeedCatalog` from
`RegistryGrain`/`UserStoreGrain`, add `JsonValueNormalizer`.
**B (web):** `orleans/web` → `web/`; vite proxy target from `SF_PROXY_TARGET` (default
`http://localhost:5199`); Host serves SPA from configurable `Web:Dist`.
**Acceptance:** 511 green; `bun run build` green; live smoke on a 6xxx-port instance (SPA + login);
seeds byte-identical (LifecycleSeed cluster tests).

### W3 — Shared Api + Orleans adapters (~1–2 days, serial, owns Host Api/Hubs/Auth/Program)
Endpoints/hub/JWT move to `shared/StreamForge.Api` with bodies verbatim modulo
`IClusterClient` → facade; `StreamForgeApiOptions` carries host-specific facts (protos dir, gRPC
port + service list, docs file, SPA dist). Orleans facade adapters + slimmed `Program.cs`.
**Acceptance:** 511 green; scripted live parity smoke on an isolated port — login ×3 roles, CRUD +
validate for sources/pipelines/tables, rows/search/history, proto download, SignalR
`pipelineResult`/`tableDelta`/`sourceEvent`, `/scalar`, SPA served.

### W4 — Dapr host skeleton (~1–2 days, serial, owns `dapr/**`)
`dapr init` (docker present; check 6379 first). Components: `pubsub.redis`, `state.redis`
(`actorStateStore`, scoped key prefix), config. `StreamForge.Dapr.Host` on 5399 using shared
`AddStreamForgeApi`/`MapStreamForgeApi`; `RegistryActor` + `UserStoreActor` (shared `SeedCatalog`,
`PasswordHasher`, field-number map via shared `FieldNumberMap` logic — numbers are forever on this
flavor too); Dapr facade adapters; `run.sh`/`reset.sh`. Decide the reentrancy policy here, in a
short design note.
**Acceptance:** live on 5399 — login (3 seeded users), source/pipeline/table CRUD + validate,
`/proto` downloads; orleans suite untouched-green.

### W5 — Generators ∥ streaming spine (2 agents)
**A:** `GeneratorActor` (batched timer ticks, envelope + egress publishing) + supervisor service.
**B:** `TopicRouter` (subscription endpoints → actor routing, `JsonValueNormalizer` at ingress) +
`DaprStreamBridge` (same SignalR event names + ~20 msg/s source sampling as Orleans).
Envelope shapes pinned verbatim in both briefs (frozen in Contracts since W1).
**Acceptance:** `sourceEvent` live over SignalR on 5399 at seeded rates.

### W6 — PipelineActor (serial)
Compile via shared Engine, execute batches routed by W5, 500 ms watermark timer, publish
`sf-pipeline-out` + metrics + lifecycle.
**Acceptance:** seeded pipeline start → `pipelineResult` over SignalR; `/results` + `/metrics`
populated on 5399.

### W7 — TableActor ∥ TableHistoryActor (2 agents)
**A:** classic-path `TableExecutor` in-actor (P=1), snapshot write-behind to actor state, in-memory
search index, delta publishing, `Parallelism > 1` rejected with a clear error.
**B:** history actor over shared retention math, fed by the delta topic.
**Acceptance:** rows/search/deltas/history live on 5399; SPA fully functional against the Dapr
host (every console page).

### W8 — Polyglot processors (2 agents, parallel with W7)
**A:** `dapr/processors/python-enricher` — subscribes `sf-source-trades` via its own sidecar,
derives/enriches, registers + publishes a new source into `sf-sources`; the derived source appears
in the console.
**B:** `dapr/processors/ts-consumer` — bun subscriber consuming `sf-table-delta`/`sf-pipeline-out`.
Java/Kotlin: buildable sample only if gradle/maven already installed, else README scaffold.
**Acceptance:** both run LIVE in verification (not just files on disk).

### W9 — Benchmark + comparison deliverable (serial + orchestrator)
`tools/bench`: end-to-end latency of the SAME seeded table pipeline (event publish → delta
visible) against both runtimes, p50/p99, published numbers. `orleans/docs/comparison.html` —
the client-styled like `docs/index.html` (text wordmark only, never the logo graphic), linked from the
docs sidebar: in-process grains vs sidecar hops (with the measured table), typed single-runtime
performance vs polyglot reach, Orleans streams vs at-least-once pub/sub (redelivery/dedup
consequences), operational story (silo clustering vs k8s-native building blocks), decision matrix.
Update ARCHITECTURE.md/AGENTS.md; parity matrix finalized; plan status → DONE.
**Acceptance:** numbers real and reproducible; docs match reality; full sweep green.

## Parity matrix (v1)

| Capability | Orleans | Dapr v1 |
|---|---|---|
| REST CRUD + validate + rows/search/metrics + proto download | full | **full (same shared endpoints)** |
| JWT auth + RBAC + user admin | full | full (shared) |
| SignalR live events (all 6) | full | full (DaprStreamBridge) |
| Console SPA (same `web/dist`) | :5199 | :5399 |
| Generators (6 profiles) | full | full (shared profiles; batched ticks) |
| Pipelines (windowed SQL, EMIT, nested queries) | full | full (shared Engine) |
| Tables classic P=1 (Z-sets, search, snapshots) | full | full |
| Row history | full | full |
| Parallelism 2–16 / frontiers / arrangements | full | **descoped** — rejected with clear error; `frontierEpoch` null; `/api/meta/arrangements` `[]` |
| gRPC static + dynamic reflection + typed streams | full (:5299) | **phase 2** (:5499 reserved; `/api/meta/grpc` shape kept, empty service list) |
| `/docs` | full | descoped (links to Orleans flavor) |
| Persistence / reseed | JSON files / delete `data/` | Redis / `reset.sh` |
| Polyglot pub/sub participation | — | **the point**: `sf-sources` ingress + `sf-source-{name}` egress |

## Risks & mitigations

- **Cross-assembly codegen (W1)** → serial wave, two small commits, TestCluster gate, documented
  fallback that still keeps tests unmodified.
- **Endpoint drift (W3)** → bodies verbatim modulo the facade substitution; scripted curl diff
  against a pre-refactor instance.
- **`JsonElement` leakage** → single normalizer at every ingress, round-trip tests in Dapr.Tests.
- **Actor reentrancy deadlock** (Registry↔worker cycles) → explicit W4 decision before dependent
  waves; the Orleans allowlist is the cautionary tale.
- **Throughput honesty** → don't imitate the Orleans topology where sidecar hops dominate;
  descope loudly (matrix) and measure (W9) instead of shipping a slow copy.
- **Local Redis conflict (6379)** → check before `dapr init`; document any port override.

## Non-goals

Partitioned Dapr execution; exactly-once delivery semantics on pub/sub (at-least-once is the
transport's nature — documented, compared, not papered over); Kubernetes deployment manifests;
replacing Orleans as the primary flavor; namespace cosmetics inside shared assemblies.

## Sequencing / effort

P(plan) → W0 → W1 → {W2A ∥ W2B} → W3 → W4 → {W5A ∥ W5B} → W6 → {W7A ∥ W7B ∥ W8A ∥ W8B} → W9.
Roughly 2–3 agent-weeks. Every wave lands as its own commit(s) with the orleans suite green, so the
effort can pause after any wave with both platforms working. Implementation agents: Sonnet 5, high
reasoning effort, strictly disjoint file ownership, live verification on isolated ports (6xxx–9xxx,
temp data dirs, instances killed after).
