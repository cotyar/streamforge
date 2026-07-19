# StreamForge — Architecture (Dapr implementation, W4 snapshot)

Plan [`../plans/005-dapr-port.md`](../plans/005-dapr-port.md), wave W4 ("Dapr host skeleton"). This
document describes what exists **after W4** — the Dapr flavor's registry/user-catalog skeleton, serving
the same shared REST/SignalR/SPA surface as the Orleans flavor (`orleans/`) on a different port, backed
by Dapr actors instead of Orleans grains. Generators (W5), pipelines (W6), and tables/history (W7) are
not built yet — see "What's NOT here yet" below for exactly what that means in practice today.

## What exists after W4

```
StreamForge.Dapr.Host (:5399)                    Dapr sidecar (3599 HTTP / 4599 gRPC)
├─ shared/StreamForge.Api                         ├─ statestore (Redis, actorStateStore, keyPrefix=appid)
│  (AddStreamForgeApi/MapStreamForgeApi —          └─ pubsub (Redis) — unused until W5
│   REST/SignalR/SPA, JWT, RBAC, byte-identical
│   to the Orleans flavor)
├─ Actors/
│  ├─ RegistryActor  (id "catalog")  ── CatalogStore (pure logic, unit-tested)
│  └─ UserStoreActor (id "users")
├─ Facades/  (Dapr-side ICatalogFacade/IUserStoreFacade adapters + stubs for
│             IPipelineReadFacade/ITableReadFacade/ITableHistoryFacade/IArrangementMetaFacade)
├─ Lifecycle/ILifecycleOrchestrator (seam — see Reentrancy decision below)
└─ Services/CatalogInitializationService (boot-time seed, mirrors Orleans' InitializeGrainsAsync)
```

Live today: login (admin/editor/viewer), full source/pipeline/table CRUD + validate, table
`Parallelism > 1` rejection (409), user admin CRUD + self-delete rejection, `/api/meta/grpc` +
`/api/meta/protos/static` + `/api/meta/arrangements` (shape-correct, empty), pipeline/table/source
`.proto` downloads (real proto text — the shared descriptor machinery in `shared/StreamForge.AppCore`
doesn't care which runtime called it), the console SPA served at `/`, `/scalar` (OpenAPI/Scalar UI).

## Actor mapping (Orleans grain → Dapr actor)

| Orleans grain | Dapr actor (W4) | Notes |
|---|---|---|
| `RegistryGrain` (`"catalog"`) | `RegistryActor` (`"catalog"`) | Catalog CRUD/validation logic factored into `Catalog/CatalogStore.cs` — a plain, actor-framework-free class the actor delegates to (unit-tested directly, no sidecar needed: `dapr/tests/StreamForge.Dapr.Tests/CatalogStoreTests.cs`). |
| `UserStoreGrain` (`"users"`) | `UserStoreActor` (`"users"`) | Same PBKDF2 credential store (shared `PasswordHasher`), same seed data (shared `SeedCatalog.Users`). |
| `GeneratorGrain` | `GeneratorActor` (key = source name) | Batched-tick synthetic event publisher — see "Generators (W5-A)" below. |
| `PipelineGrain` | `PipelineActor` (key = pipeline id) | Compiles + runs the pipeline's streaming SQL via the shared Engine, publishes `sf-pipeline-out`/`sf-metrics` — see "Pipelines (W6)" below. |
| `TableGrain` / `TableIngestGrain` / `TableStageGrain` / `TableOutputGrain` | *(not yet — W7; partitioned variants are Orleans-only, decision D-F)* | `ITableReadFacade` is stubbed; `ILifecycleOrchestrator.StartTableAsync` currently just logs and reports success. |
| `TableHistoryGrain` | *(not yet — W7)* | `ITableHistoryFacade` is stubbed. |
| `ArrangementGrain` | *(never — Orleans-only, decision D-F)* | `IArrangementMetaFacade` always returns `[]`. |

## Decisions made this wave

### Actor state persistence and granularity

One Redis-backed state entry per singleton actor: `RegistryActor` persists a single `CatalogState` blob
(sources + pipelines + tables + field-number maps — the same shape as Orleans' `RegistryState`) under
state name `"catalog"`; `UserStoreActor` persists a single `UserStoreState` (list of `UserRecord`) under
`"users"`. Mirrors the Orleans flavor's one-JSON-file-per-grain granularity. Fine at this scale (a
handful of entities, a few KB of JSON); revisit only if profiling ever shows the whole-blob-rewrite-per-
mutation cost matters (it won't at demo scale).

The Redis `statestore` component (`components/statestore.yaml`) sets `actorStateStore: "true"` (required
for Dapr actors to use it at all) and `keyPrefix: appid`, which scopes every key under this app's id
(`streamforge-dapr||...`) — confirmed live: after a run, `redis-cli --scan --pattern 'streamforge-dapr*'`
lists exactly `streamforge-dapr||RegistryActor||catalog||catalog` and
`streamforge-dapr||UserStoreActor||users||users`. This is what makes `tools/reset.sh`'s scoped SCAN safe
on a Redis instance potentially shared with other apps.

### Serialization

**Actor-invocation wire (client proxy ↔ actor method) and actor state are both plain System.Text.Json**,
explicitly opted into on both sides:

- Server side: `builder.Services.AddActors(options => { ...; options.UseJsonSerialization = true; })`
  (Program.cs).
- Client side: every `ActorProxy.Create<T>(...)` call passes `Actors/ActorProxyDefaults.Options`
  (`new ActorProxyOptions { UseJsonSerialization = true }`).

This was **not** the default and had to be discovered live: the Dapr .NET SDK's out-of-the-box
`ActorProxy`/`ActorRuntimeOptions` default to the legacy `DataContractSerializer`, which throws
`InvalidDataContractException` on plain records without a parameterless constructor and
`[DataContract]`/`[DataMember]` attributes — exactly the shape of every request/response type here
(`SetStatusRequest`, `ValidateCredentialsRequest`, the shared Contracts DTOs, etc., all meant for
System.Text.Json). Enums (`PipelineStatus`, `FieldType`, ...) serialize as plain ints on this internal
wire — independent of the public REST/SignalR JSON contract, which
`StreamForgeApiExtensions.AddStreamForgeApi` configures separately with `JsonStringEnumConverter`. Since
the actor wire is internal-only (never observed by the SPA or any REST/gRPC client), this divergence is
harmless.

### Field numbers (dynamic protobuf)

`RegistryActor.EnsureFieldNumbersAsync` delegates to `CatalogStore.EnsureFieldNumbers`, which calls the
same shared `FieldNumberMap.Assign` algorithm Orleans' `RegistryGrain` uses, persisted in the same
`CatalogState.FieldNumberMaps` dictionary shape. Numbers are forever on this flavor too, verified live:
`.proto` downloads for a seeded source/table/pipeline show real field numbers from a real
`DynamicDescriptorSet`/`DescriptorFactory` round trip (see the live-check log in the wave's report for
sample output).

### Actor-boundary error handling: result types, not thrown exceptions

`CatalogStore`'s table-mutation methods (`CreateTableAsync`/`UpdateTableAsync`/`DeleteTableAsync`/
`SetTableStatusAsync`) throw `InvalidOperationException` on validation failures (name collisions,
`Parallelism != 1`, running-dependent guards) — ported verbatim from `RegistryGrain`. Rather than letting
that exception cross the Dapr actor-invocation boundary (where the SDK wraps it in its own exception type
with no guarantee the original CLR type survives), `RegistryActor` catches it and returns
`ActorResult<T>.Failure(message)`; `Facades/DaprCatalogFacade` unwraps that result and re-throws
`InvalidOperationException` **client-side**, so the shared `TablesEndpoints`' existing
`catch (InvalidOperationException) → 409 Conflict` pathway fires identically on both runtimes. Verified
live: `POST /api/tables` with `parallelism: 4` → `409` with body
`{"error":"Parallelism must be 1 on the Dapr flavor (got 4) — partitioned execution is Orleans-only in
the Dapr flavor."}`.

### Parallelism honesty (decision D-F)

`CatalogStore.ValidateParallelism` rejects **any** value other than `1` (stricter than Orleans' own
`1..16` range check) — partitioned execution (frontier-consistent reads, shared arrangements, the whole
stage-grid dataflow) is Orleans-only. `ITableReadFacade.GetSnapshotFrontierEpochAsync` always returns
`null` and `IArrangementMetaFacade.GetArrangementsAsync` always returns `[]`, independent of whether
`TableActor` has landed (W7) — these are permanent, not W-something stubs.

### Seed status: seeded TABLES are forced to `Stopped` — sources and pipelines are the exceptions (W5-A, W6)

`shared/StreamForge.AppCore/SeedCatalog` marks several demo pipelines/tables `Running` (the Orleans flavor
resumes them for real on boot, per `RegistryGrain.EnsureInitializedAsync`). On the Dapr flavor through W4,
**no runtime at all** existed behind a Running status — no generator publishing events, no pipeline/table
actually computing anything, so `CatalogStore.EnsureInitialized` overrode every seeded pipeline/table to
`Stopped` regardless of what `SeedCatalog` said. **This still applies to TABLES** (W7 hasn't landed) —
`GET /api/tables` on a fresh seed still shows every table `Stopped`.

**Sources (W5-A) and pipelines (W6) are no longer force-stopped**, because a real runtime now exists
behind each: `SeedCatalog.Sources()` marks every seeded source `Enabled = true` and `CatalogStore.
EnsureInitialized` does **not** override that (a seeded, enabled source starts generating within one
`GeneratorSupervisorService` sweep of boot — see "Generators" below); `SeedCatalog.Pipelines()` marks
several pipelines `Running` and, as of W6, `CatalogStore.EnsureInitialized` no longer overrides *that*
either — a seeded Running pipeline starts producing real windowed rows within one `PipelineSupervisorService`
sweep of boot (see "Pipelines" below), mirroring Orleans' own `RegistryGrain.EnsureInitializedAsync`
resume-on-boot loop. Verified live (W6): a fresh seed's four `Running`-seeded pipelines (VWAP, trade/quote
spread join, the nested-CTE hot-symbol VWAP, and `fill-rate-5s`) show nonzero `totalEventsIn`/`totalRowsOut`
in `GET /api/pipelines/{id}/metrics` within seconds of boot, with **no** `POST .../start` call ever issued
— and killing + restarting the whole host (Redis-backed catalog/actor state survives) reproduces the same
resume with no REST call either time. The only observable difference from Orleans' instant start is Dapr's
few-seconds-to-15s startup lag, a direct consequence of the periodic-sweep design (see "Generators"/
"Pipelines").

A user can still manually `POST .../start` on a seeded/created pipeline or table today — see the next
decision for what that does (tables: still bookkeeping-only, W7 hasn't landed; pipelines: a real start,
same as boot-resume).

### The `ILifecycleOrchestrator` seam — and the reentrancy decision

Every place `RegistryGrain` reaches into another grain to actually start/stop a runtime process
(`GeneratorGrain.StartAsync/StopAsync`, `PipelineGrain.StartAsync/StopAsync`, `TableGrain.StartAsync/
StopAsync`, `ITableHistoryGrain.ResetAsync/DisableAsync`, plus the lifecycle-stream publish) is routed,
in `CatalogStore`, through `Lifecycle/ILifecycleOrchestrator` instead of a direct actor-to-actor call.
W4's implementation, `NoopLifecycleOrchestrator`, logs a warning ("no runtime yet") and reports
**success** for every start call. As of W6, `DaprLifecycleOrchestrator`'s `StartPipelineAsync`/
`StopPipelineAsync` are real (see "Pipelines" below); `StartTableAsync`/`StopTableAsync`/
`ResetTableHistoryAsync`/`DisableTableHistoryAsync` are still W4's warn-and-succeed no-op (W7 replaces
them) — so `POST /api/tables/{id}/start` still only flips the catalog status to `Running` and persists it
without starting anything real ("Running-but-inert"), while `POST /api/pipelines/{id}/start` now does the
real thing end-to-end.

**Why a seam instead of direct actor-to-actor calls at all — the reentrancy decision itself:** none of
`GeneratorActor`/`PipelineActor`/`TableActor`/`TableHistoryActor` exist yet, but more importantly, this
plan's W4 acceptance criterion is to resolve the reentrancy question *before* any dependent wave builds on
it (see plan's D-E and Risks). The Orleans cautionary tale: `RegistryGrain` needed a `[MayInterleave]`
allowlist (AGENTS.md hard rule #3) because a worker grain's own `StartAsync` reads back from
`RegistryGrain` while `RegistryGrain`'s own turn that triggered the start is still in-flight — without
allowlisting, that's a deadlock (a grain/actor turn can't be re-entered by a call that's waiting on that
same turn to finish). Dapr actors are non-reentrant by default too, and enabling reentrancy is itself extra
per-actor-type configuration surface (`ActorRuntimeOptions.ReentrancyConfig`) plus a `Configuration`
resource-level opt-in.

**Decision: keep orchestration ACYCLIC — do not enable Dapr actor reentrancy.**
`RegistryActor` never calls into another actor (or itself) from inside one of its own turns; every
"start the runtime for X" side effect goes through `ILifecycleOrchestrator` (ordinary constructor-injected
DI, not an actor proxy) — in W4 that means "just log", and starting with W5 it means "publish a pub/sub
message and/or invoke a worker actor via a call path that a `RegistryActor` turn is never itself blocked
on" (fire-and-forget publish, not an inline synchronous actor-to-actor RPC that could call back in).
`dapr/components/config.yaml` deliberately does **not** set a `reentrancy` block, documenting this as a
choice, not an oversight. **Rule for W5-W7:** any new actor type must not call back into `RegistryActor`
synchronously from inside a method `RegistryActor` itself invoked; route data the callee needs as
parameters on the original call, or via pub/sub, instead.

## Generators (W5-A)

One `GeneratorActor` per source (actor type `"GeneratorActor"`, key = the source's name) — a Dapr
timer-driven synthetic event publisher built on the same `MarketDataProfiles` used by Orleans'
`GeneratorGrain`. `Actors/GeneratorActor.cs`; the batching math is a separately unit-tested pure function,
`Actors/GeneratorBatching.NextBatchCount` (`dapr/tests/StreamForge.Dapr.Tests/GeneratorBatchingTests.cs`).

**Batched ticks, not per-event (decision D-E).** Every actor timer fire and every pub/sub publish is a
sidecar round-trip, unlike Orleans' in-silo grain timer + in-process stream push. `GeneratorActor` ticks on
a fixed 200ms period (well inside D-E's "≤20Hz" ceiling) and each tick publishes
`round(EventsPerSecond × elapsed)` events — elapsed measured from actual wall-clock time since the previous
tick, not the nominal period, with the fractional remainder carried forward tick-to-tick so a low-EPS
source still converges on its configured rate rather than rounding down to zero forever — as ONE
`SourceEventsEnvelope` to both `sf-sources` (the in-host router) and `sf-source-{name}` (the publish-only
polyglot egress copy; nothing in this process subscribes to it).

**State is persisted, unlike Orleans' in-memory `GeneratorGrain`.** `GeneratorActor` persists its last
`SourceDefinition` and running flag (state name `"generator"`) specifically because Dapr actor timers do
NOT survive deactivation/reactivation — on any reactivation, `OnActivateAsync` re-arms the timer
immediately from persisted state if it was last `Running`, instead of waiting for the next
`GeneratorSupervisorService` sweep. The supervisor (`Services/GeneratorSupervisorService.cs`, hosted
service) remains the safety net for the one case self-healing can't cover: a source whose actor has never
been activated at all — every ~15s it lists sources via `ICatalogFacade` and idempotently calls
`StartAsync` on each enabled one's `GeneratorActor`, mirroring Orleans' own `GeneratorSupervisorService`
(which pings to keep grains alive/reactivated).

**Acyclic by construction.** `GeneratorActor` never resolves `ICatalogFacade`, an `IRegistryActor` proxy, or
any other actor — everything it needs arrives once as the `StartAsync` parameter. `Lifecycle/
DaprLifecycleOrchestrator` (replacing `NoopLifecycleOrchestrator` — registered by
`Actors/GeneratorRuntimeSetup.cs`, which now also registers `GeneratorActor` and a `DaprClient`) calls
straight into `GeneratorActor.StartAsync`/`StopAsync` **synchronously, inline**, from
`RegistryActor`'s own turn (via `CatalogStore`). This refines this document's earlier, more conservative
W4 reentrancy note ("fire-and-forget publish, not an inline synchronous actor-to-actor RPC that could call
back in"): the actual hazard that note guards against is a call-graph CYCLE, not synchrony per se, and an
inline call to a genuine leaf actor cannot deadlock. This is also the byte-identical-behavior choice —
Orleans' `RegistryGrain.UpsertSourceAsync` awaits `GeneratorGrain.StartAsync`/`StopAsync` inline too, and
`CatalogStore`'s contract (shared endpoint code, decision D-B) is unchanged either way. **The rule for
W6/W7 stands as originally written**: this only holds because `GeneratorActor` is a leaf with no path back
to `RegistryActor`; a worker actor that reads back from the registry (directly or via a facade) would
reintroduce the exact cycle this design avoids and must use fire-and-forget pub/sub or an equivalent
non-blocking path instead.

**`ILifecycleOrchestrator.NotifySourceChangedAsync` signature changed** from `(string name, bool enabled)`
to `(SourceDefinition def)` — the real implementation needs the source's generator profile/EventsPerSecond/
field schema to start `GeneratorActor`, and fetching that from inside the call (e.g. via `ICatalogFacade`,
which resolves to an actor-proxy call back into `RegistryActor`) would be exactly the self-call-while-
mid-turn deadlock the reentrancy decision exists to prevent. `CatalogStore` already has the full definition
in hand at every call site, so no lookup is needed with the new signature. `NoopLifecycleOrchestrator` and
the test double `TestLifecycleOrchestrator` were updated to match; no test assertions changed (the log/call
string format is unchanged: `"NotifySourceChanged:{name}:{enabled}"`, now built from `def.Name`/`def.Enabled`).

`sf-lifecycle` is now published for real too: `DaprLifecycleOrchestrator.PublishLifecycleAsync` calls
`DaprClient.PublishEventAsync` where `NoopLifecycleOrchestrator` only logged.

## Pipelines (W6)

One `PipelineActor` per running pipeline (actor type `"PipelineActor"`, key = the pipeline's `Id`) —
compiles the pipeline's streaming SQL via the shared `StreamForge.Engine` (same compile path
`PipelineGrain.StartAsync` uses: build a schema dictionary from every known source, `SqlCompiler.Compile`),
executes it against batches of routed events, and publishes emitted rows + periodic metrics to Dapr
pub/sub — a byte-for-byte mirror of `PipelineGrain`'s watermark-tick/publish cadence
(`orleans/src/StreamForge.Host/Grains/PipelineGrain.cs`), translated from Orleans streams to the fixed-topic
transport (decision D-D). `Actors/PipelineActor.cs`; `Actors/IPipelineActor.cs`.

**Acyclic by construction (same discipline as `GeneratorActor`).** `PipelineActor` never resolves
`ICatalogFacade`, an `IRegistryActor` proxy, or any other actor — everything it needs arrives once, either
as `StartAsync`'s `PipelineStartRequest` (the `PipelineDefinition` plus **every** known `SourceDefinition`,
not just the ones the SQL references — exactly what `PipelineGrain.StartAsync` builds schemas from after
its own `GetSourcesAsync()` call) or per-batch via `ProcessEventsAsync`. It only ever talks outward, to Dapr
pub/sub (`sf-pipeline-out`, `sf-metrics`).

**Routing: `Streaming/PipelineEventRouter.cs`.** Dapr's fixed `sf-sources` topic (decision D-D) means
`PipelineActor` can't subscribe per-source itself the way `PipelineGrain` subscribes Orleans stream handles
— instead `PipelineEventRouter` registers as a second `ISourceEventsSink` alongside `DaprStreamBridge`
(`Streaming/StreamingRuntimeSetup.AddServices`), maintains an in-memory `{sourceName → {pipelineId}}` table,
and forwards every `sf-sources` envelope to `IPipelineActor.ProcessEventsAsync` on every pipeline subscribed
to that source. The table is deliberately **not** persisted (a pure routing cache, rebuilt from actor state
on demand) and is maintained from two places:
- `Lifecycle/DaprLifecycleOrchestrator.StartPipelineAsync`/`StopPipelineAsync` — every explicit start/stop
  registers/unregisters it, using the source names `PipelineActor.StartAsync` itself resolved (returned in
  its `ActorResult<List<string>>`, so there is no second compile in the orchestrator).
- `Services/PipelineSupervisorService`'s boot-resume sweep repairs it for a `PipelineActor` that self-healed
  on reactivation without going through either of the calls above (see next paragraph).

**Boot resume (`Services/PipelineSupervisorService.cs`) — seeded `Running` pipelines now boot running.**
This *changes* the W4/W5 decision: `CatalogStore.EnsureInitialized` no longer force-stops seeded pipelines
(see the updated "Seed status" decision above) — mirroring Orleans' own one-shot
`RegistryGrain.EnsureInitializedAsync` boot-resume loop, but as a periodic (~15s) sweep for the same
sidecar-readiness reason `GeneratorSupervisorService` is periodic rather than one-shot. Unlike that
generator sweep (which unconditionally calls `GeneratorActor.StartAsync` every tick — harmless, since
Generator has no accumulated user-visible state to lose), this sweep checks `IPipelineActor.IsRunningAsync()`
first: an already-running actor (self-healed via `PipelineActor.OnActivateAsync` recompiling from persisted
state on any reactivation, exactly like `GeneratorActor`'s own self-heal) is left alone except for a
router-table repair (`GetSourceNamesAsync()`, a cheap in-memory read, no recompile) — restarting it would
discard in-flight window/join state and reset nothing else (Orleans doesn't reset pipeline counters on a
mere restart either — see `PipelineActor`'s class doc), a needless disruption to a pipeline that's simply
been running fine. A NOT-yet-running one goes through `ICatalogFacade.SetPipelineStatusAsync(id, Running)`
— the exact same code path `POST /api/pipelines/{id}/start` uses.

**Read surface: `Facades/DaprPipelineReadFacade.cs`** replaces the W4/W5 `StubPipelineReadFacade` —
`GET /api/pipelines/{id}/results|metrics` now forward to the pipeline's own `PipelineActor`
(`GetRecentResultsAsync`/`GetMetricsAsync`). Results are a bounded in-memory ring, capacity 100 (same as
`PipelineGrain`), extracted as the pure, unit-tested `PipelineResultRing` (append/evict + "last N" read);
compile-to-executor is likewise extracted as `PipelineCompilation.TryCompile` for the same reason —
testable without any actor/timer/Dapr-sidecar machinery (mirrors `GeneratorBatching`'s own extraction).

**JsonElement crosses the actor wire too — an important, non-obvious finding.** Decision D-D's
normalization requirement ("`JsonElement` values are normalized at every topic ingress") is satisfied once,
at the `sf-sources` pub/sub endpoint, before `PipelineEventRouter` ever sees an envelope — so by the time
the router calls `ProcessEventsAsync`, every event dictionary already holds plain CLR values. It would be
reasonable to assume `PipelineActor.ProcessEventsAsync` therefore never needs to normalize again — **that
assumption is wrong**. The Dapr actor-invocation call (`ActorProxy.Create<IPipelineActor>(...)
.ProcessEventsAsync(envelope)`) is not an in-process method call; it round-trips through System.Text.Json
via `ActorProxyOptions.UseJsonSerialization`, and System.Text.Json has no static type information for a
`Dictionary<string, object?>` value at deserialization time — so every value, even ones that started as
plain CLR on the publish side, comes back out as a `JsonElement` again once it lands inside the actor's
method body. `PipelineActor.ProcessEventsAsync` therefore re-normalizes (`JsonValueNormalizer.NormalizeInPlace`)
before constructing an `EventRecord`; skipping this would silently break every pipeline, since
`PipelineEventRouter` is the only path events ever reach a running `PipelineActor`. Proven explicitly by
round-tripping an already-normalized envelope through the actor wire's own serializer configuration:
`dapr/tests/StreamForge.Dapr.Tests/PipelineActorWireNormalizationTests.cs`.

**Live-verified (see the wave's report for the full transcript):** a fresh seed's four `Running` pipelines
(a single-source tumbling-window VWAP, a two-source `WITHIN`-join spread, a nested-CTE hot-symbol VWAP, and
a `WHERE`-filtered fill-rate aggregate) all produce real windowed rows and growing `totalEventsIn`/
`totalRowsOut` within seconds of boot — **no `POST .../start` call issued** — visible over both REST
(`GET /api/pipelines/{id}/results|metrics`) and SignalR (`pipelineResult`, `pipelineStatus` on
start/stop). Explicit `POST .../start` and `.../stop` both work identically (stop makes `totalEventsIn`
stop growing, confirmed by re-polling metrics after the stop response). Killing and restarting the whole
host reproduces the same boot-resume with no REST call either time, proving the Redis-backed
`PipelineActorState` self-heal path, not just the initial-boot path.

## What's NOT here yet (by design — later waves)

- **Generators** (W5) and **Pipelines** (W6) have both landed — see their own sections above/below.
- **Tables/history** (W7): no Z-set execution, no rows, no search, no history
  (`Facades/StubFacades.cs`, each marked with a `W7 replaces this` comment).
- **gRPC serving** (phase 2, decision D-F): `:5499` is reserved but nothing listens on it yet;
  `GET /api/meta/grpc` reports it with an empty static-service list (shape preserved).
- **`/docs`**: not mapped on this flavor (`StreamForgeApiOptions.DocsFilePath` is `null`) — stays
  Orleans-served, per decision D-F. (Note: an unmapped `/docs` still falls through to the SPA's
  client-side-routing fallback and returns the console app shell with HTTP 200, not a REST-style 404 —
  same as any other unrecognized non-`/api` path; this is normal SPA-hosting behavior, not a bug.)
- **Pub/sub topics**: `components/pubsub.yaml` exists but nothing subscribes yet — `MapSubscribeHandler()`
  in Program.cs currently reports an empty subscription list to the sidecar.

## How to run

```bash
# Prereqs: dapr init already run (containers dapr_redis on 6379, dapr_placement, dapr_scheduler);
# ~/.dotnet/dotnet is the SDK (NOT on PATH).
cd dapr
./tools/run.sh                       # dapr run --app-id streamforge-dapr --app-port 5399 ...
# Ctrl-C, or from another shell:
dapr stop --app-id streamforge-dapr

./tools/reset.sh                     # wipes this app's Redis keys (scoped SCAN, see above)
./tools/run.sh                       # next boot reseeds from empty state

~/.dotnet/dotnet test dapr/StreamForge.Dapr.sln     # 102 tests as of W6 (JsonValueNormalizer, CatalogStore,
                                                     # streaming dispatch/normalization, generator batching,
                                                     # pipeline compilation/result-ring/router/actor-wire)
```

Logins: `admin/admin123!`, `editor/editor123!`, `viewer/viewer123!` — same as the Orleans flavor (shared
`SeedCatalog.Users`). Ports: app `5399`, gRPC-reserved `5499`, sidecar HTTP `3599` / gRPC `4599`. Never
bind or kill Orleans' `5199`/`5299`.
