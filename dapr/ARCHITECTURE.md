# StreamForge — Architecture (Dapr implementation, W9 / final snapshot)

Plan [`../plans/005-dapr-port.md`](../plans/005-dapr-port.md) — this document describes the Dapr
flavor as it exists at the plan's final wave (W9): a full sibling runtime of the Orleans flavor
(`orleans/`), serving the same shared REST/SignalR/SPA surface on a different port, backed by Dapr
actors instead of Orleans grains. Generators (W5), pipelines (W6), tables (W7-A), and row history
(W7-B) have all landed and are live; see "What's NOT here yet" below for the (short) list of what
remains genuinely out of scope for this flavor, by design.

## What exists

```
StreamForge.Dapr.Host (:5399)                    Dapr sidecar (3599 HTTP / 4599 gRPC)
├─ shared/StreamForge.Api                         ├─ statestore (Redis, actorStateStore, keyPrefix=appid)
│  (AddStreamForgeApi/MapStreamForgeApi —          └─ pubsub (Redis, topics: sf-sources, sf-pipeline-out,
│   REST/SignalR/SPA, JWT, RBAC, byte-identical       sf-table-delta, sf-lifecycle, sf-metrics)
│   to the Orleans flavor)
├─ Actors/
│  ├─ RegistryActor       (id "catalog")  ── CatalogStore (pure logic, unit-tested)
│  ├─ UserStoreActor      (id "users")
│  ├─ GeneratorActor      (key = source name)     — see "Generators (W5-A)"
│  ├─ PipelineActor       (key = pipeline id)     — see "Pipelines (W6)"
│  ├─ TableActor          (key = table name)      — see "Tables (W7-A)"
│  └─ TableHistoryActor   (key = table name)      — see "Row history (W7-B)"
├─ Facades/  (Dapr-side ICatalogFacade/IUserStoreFacade/IPipelineReadFacade/DaprTableReadFacade/
│             DaprTableHistoryFacade adapters; IArrangementMetaFacade stays a permanent stub — D-F)
├─ Streaming/  (PipelineEventRouter, TableEventRouter, TableHistoryDeltaSink, DaprStreamBridge,
│               SourceRateSampler — sf-sources/sf-table-delta ingress routing + SignalR relay)
├─ Lifecycle/ILifecycleOrchestrator + DaprLifecycleOrchestrator (seam — see Reentrancy decision below)
└─ Services/ (CatalogInitializationService boot-time seed; GeneratorSupervisorService,
              PipelineSupervisorService, TableSupervisorService, TableHistorySupervisorService —
              periodic ~15s self-healing sweeps, one per actor kind, mirroring Orleans'
              resume-on-boot loop for a sidecar-readiness-tolerant topology)
```

Live today: login (admin/editor/viewer), full source/pipeline/table CRUD + validate, table
`Parallelism > 1` rejection (409), user admin CRUD + self-delete rejection, `/api/meta/grpc` +
`/api/meta/protos/static` + `/api/meta/arrangements` (shape-correct, empty), pipeline/table/source
`.proto` downloads (real proto text — the shared descriptor machinery in `shared/StreamForge.AppCore`
doesn't care which runtime called it), the console SPA served at `/`, `/scalar` (OpenAPI/Scalar UI),
seeded generators/pipelines/tables running for real (not just catalog entries) with live SignalR
events, and row history. See each numbered section below for the wave that landed it.

## Actor mapping (Orleans grain → Dapr actor)

| Orleans grain | Dapr actor | Notes |
|---|---|---|
| `RegistryGrain` (`"catalog"`) | `RegistryActor` (`"catalog"`) | Catalog CRUD/validation logic factored into `Catalog/CatalogStore.cs` — a plain, actor-framework-free class the actor delegates to (unit-tested directly, no sidecar needed: `dapr/tests/StreamForge.Dapr.Tests/CatalogStoreTests.cs`). |
| `UserStoreGrain` (`"users"`) | `UserStoreActor` (`"users"`) | Same PBKDF2 credential store (shared `PasswordHasher`), same seed data (shared `SeedCatalog.Users`). |
| `GeneratorGrain` | `GeneratorActor` (key = source name) | Batched-tick synthetic event publisher, real and live — see "Generators (W5-A)" below. |
| `PipelineGrain` | `PipelineActor` (key = pipeline id) | Compiles + runs the pipeline's streaming SQL via the shared Engine, publishes `sf-pipeline-out`/`sf-metrics`, real and live — see "Pipelines (W6)" below. |
| `TableGrain` classic path (`Parallelism == 1`) | `TableActor` (key = table name) | Same shared Engine Z-set execution, real and live — see "Tables (W7-A)" below. |
| `TableGrain` / `TableIngestGrain` / `TableStageGrain` / `TableOutputGrain` partitioned path (`Parallelism 2–16`) | *(never — Orleans-only, decision D-F)* | `CatalogStore.ValidateParallelism` rejects anything but `1`; `ITableReadFacade.GetSnapshotFrontierEpochAsync` always returns `null`. |
| `TableHistoryGrain` | `TableHistoryActor` (key = table name) | Same shared retention math (`TableGroupKeyExtractor`/`RowKeyCodec`/`TableRowHistoryRetention`), real and live — see "Row history (W7-B)" below. |
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

### Seed status: sources, pipelines, AND tables are all honestly `Running` now (W5-A, W6, W7-A)

`shared/StreamForge.AppCore/SeedCatalog` marks several demo pipelines/tables `Running` (the Orleans flavor
resumes them for real on boot, per `RegistryGrain.EnsureInitializedAsync`). On the Dapr flavor through W4,
**no runtime at all** existed behind a Running status — no generator publishing events, no pipeline/table
actually computing anything, so `CatalogStore.EnsureInitialized` overrode every seeded pipeline/table to
`Stopped` regardless of what `SeedCatalog` said.

**None of the three are force-stopped anymore**, because a real runtime now exists behind each:
`SeedCatalog.Sources()` marks every seeded source `Enabled = true` and `CatalogStore.EnsureInitialized` does
**not** override that (a seeded, enabled source starts generating within one `GeneratorSupervisorService`
sweep of boot — see "Generators" below); `SeedCatalog.Pipelines()` marks several pipelines `Running` and, as
of W6, `CatalogStore.EnsureInitialized` no longer overrides *that* either (see "Pipelines" below); and as of
W7-A, `SeedCatalog.Tables()` marks "positions"/"leg_exposure"/"order_states" `Running` and
`CatalogStore.EnsureInitialized` no longer overrides table statuses either — a seeded Running table starts
compiling/consuming/publishing real Z-set deltas within one `TableSupervisorService` sweep of boot (see
"Tables" below). The ONE remaining override, for all three seed kinds, is defensive: an entity whose SQL
fails to compile against the seeded sources/tables is forced `Stopped` regardless of what `SeedCatalog`
requested — a table/pipeline that doesn't compile can never validly be Running. Every seeded resume
mirrors Orleans' own `RegistryGrain.EnsureInitializedAsync` resume-on-boot loop, just as a periodic sweep
(~15s) instead of a one-shot boot gate, for the same "wait for the sidecar to be ready" reason each
`*SupervisorService` doc comment gives.

Verified live (W6, and again for tables in W7-A): a fresh seed's four `Running`-seeded pipelines (VWAP,
trade/quote spread join, the nested-CTE hot-symbol VWAP, and `fill-rate-5s`) show nonzero
`totalEventsIn`/`totalRowsOut` in `GET /api/pipelines/{id}/metrics` within seconds of boot, and the three
`Running`-seeded tables ("positions", "leg_exposure", "order_states") show growing `RowCount`/`DeltasIn` in
`GET /api/tables/{id}/metrics` on the same timescale — **no** `POST .../start` call ever issued for any of
them — and killing + restarting the whole host (Redis-backed catalog/actor state survives) reproduces the
same resume with no REST call either time. The only observable difference from Orleans' instant start is
Dapr's few-seconds-to-15s startup lag, a direct consequence of the periodic-sweep design (see
"Generators"/"Pipelines"/"Tables").

`SeedCatalog.Tables()` seeds two tables `Stopped` on purpose, not as a force-override: "gold_tier_orders"
and "hot_symbols" — the latter is the table-over-table chaining demo (`FROM "positions"`), deliberately left
`Stopped` so starting a dependency chain is a deliberate user action, not an implicit boot race (see
"Tables" below for how `TableSupervisorService` would handle a Running table-over-table chain if one were
ever seeded that way).

A user can still manually `POST .../start` on a seeded/created pipeline or table today — see the next
decision for what that does (both now do a real start, same as boot-resume).

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

## Tables (W7-A)

One `TableActor` per running table (actor type `"TableActor"`, key = the table's `Name` — same key
Orleans' `ITableGrain` uses), CLASSIC (Parallelism==1) PATH ONLY — the Dapr counterpart of
`TableGrain`'s `StartClassicAsync`/read-side machinery (`orleans/src/StreamForge.Host/Grains/TableGrain.cs`).
Partitioned execution (Parallelism 2-16, frontier-consistent reads, shared arrangements) is Orleans-only
(decision D-F); `CatalogStore.ValidateParallelism` already rejects anything but `1` at CRUD time, and
`TableActor.StartAsync` asserts it again defensively. `Actors/ITableActor.cs`, `Actors/TableActor.cs`.

**Compilation and Z-set execution are the identical shared code the Orleans grain uses.** `TableCompilation.TryCompile`
(extracted the same way `PipelineCompilation.TryCompile` was in W6 — testable without any actor/timer/
sidecar machinery, see `dapr/tests/StreamForge.Dapr.Tests/TableCompilationTests.cs`) builds stream/table
schema dictionaries from the full `TableStartRequest.Sources`/`Tables` lists and calls
`StreamForge.Engine.SqlCompiler.CompileTable` — the exact same entry point `TableGrain.StartClassicAsync`
calls. The resulting `TableExecutor` is the same Z-set (DBSP-style) incremental-view-maintenance engine
either runtime uses; nothing about the SQL semantics differs between flavors.

**Routing: `Streaming/TableEventRouter.cs`.** A table's SQL can read stream sources directly
(`TableCompileResult.StreamInputs`) and/or other tables' output directly (`TableInputs` — table-over-table
chaining, e.g. the seeded "hot_symbols" FROM "positions" demo). Since Dapr's fixed topics (decision D-D)
mean `TableActor` can't subscribe per-source/per-upstream-table itself, `TableEventRouter` registers as
BOTH a second `ISourceEventsSink` (alongside `PipelineEventRouter`) and a second `ITableDeltaSink`
(alongside W7-B's `TableHistoryDeltaSink`) — exactly what `Streaming/Sinks.cs`'s class doc anticipated for
`sf-sources`, and the natural generalization of that same "additive sink" pattern to `sf-table-delta`.
Two independent in-memory routing tables, split by input kind (`_byStreamSource`/`_byUpstreamTable`), so a
table subscribed to a stream source never shows up as a consumer of an upstream TABLE of the same name and
vice versa — proven directly in `dapr/tests/StreamForge.Dapr.Tests/TableEventRouterTests.cs`
(`Register_SplitsSubscriptionsByKind_StreamVsTable`). A table must never receive its own output deltas
back (defensive — the SQL compiler would never legitimately produce a self-referential `TableInputs`
entry): `OnTableDeltaAsync` filters through a pure, separately-tested `TableEventRouter.ExcludeSelf` static
method rather than an inline check, specifically so this safety rule is unit-testable without a live actor.
Registered inside `Actors/TableRuntimeSetup.cs`'s `AddServices` (not `Streaming/StreamingRuntimeSetup.cs`) —
`IEnumerable<T>` DI resolution fans out across every setup method that registers one, regardless of which
file calls `AddSingleton`, and Program.cs already calls `TableRuntimeSetup.AddServices` after
`StreamingRuntimeSetup.AddServices`.

**Orchestrator signature evolution (mirrors W6's identical `StartPipelineAsync` change):**
`ILifecycleOrchestrator.StartTableAsync` grew from `(TableDefinition def)` to
`(TableDefinition def, IReadOnlyList<SourceDefinition> sources, IReadOnlyList<TableDefinition> tables)` —
`DaprLifecycleOrchestrator.StartTableAsync` needs every known source's AND table's schema to compile
(same schema-building `TableGrain.StartClassicAsync` does via `GetSourcesAsync()`/`GetTablesAsync()`);
fetching that inside the call via `ICatalogFacade` would be a self-call back into `RegistryActor`'s
still-in-flight turn — the exact reentrancy hazard this document's reentrancy decision exists to prevent.
`Catalog/CatalogStore.cs`'s two call sites (`UpdateTableAsync`, `SetTableStatusAsync`) already hold
`state.Sources`/`state.Tables` in full, so passing them through needs no extra lookup.
`DaprLifecycleOrchestrator.StartTableAsync` starts `TableActor`; `StopTableAsync` stops it and
unregisters `TableEventRouter`. `TableActor` is still a pure leaf with respect to `RegistryActor`/
`ICatalogFacade` (never resolves either — everything arrives as a method parameter), so this inline,
synchronously-awaited actor call from inside `RegistryActor`'s own turn cannot deadlock, exactly the
same "acyclic by construction" argument `GeneratorActor`/`PipelineActor` already make.

**PARITY.md D2 (table-over-table warm attach) changed WHO registers `TableEventRouter`, and WHEN.**
Unlike every other worker actor here, `TableActor` is no longer a total leaf: `StartAsync`/
`OnActivateAsync`'s self-heal branch now call `TableEventRouter.Register` themselves — with the
stream/table input names the actor's own compile just resolved — BEFORE reading any upstream table's
snapshot, and (for a table input) call a DIFFERENT `TableActor` instance's `ITableActor.
AttachSnapshotAsync` via `ActorProxy`. Neither is the reentrancy cycle this document's decision above
guards against, but the reason is worth stating accurately rather than plausibly. It is **not** that "the
SQL compiler has no recursive-table feature" — a cycle needs no such feature, only two ordinary tables
each selecting from the other, and `ImportPlanner`'s topo sort merely *diagnoses* a table dependency cycle
and proceeds. What was actually measured (Orleans, isolated instance, 2026-08-20): creating `cyc_a` over a
source, `cyc_b` over `cyc_a`, then repointing `cyc_a` at `cyc_b` is **accepted at PUT** and then **refused
at start** — the table goes `Failed` with `1:15 Unknown source 'cyc_b'` and its `tableInputs` stays empty,
so no attach is ever issued and the A→B→A call chain never forms. `TableEventRouter` is a plain injected
singleton, not an actor, so it adds no cycle of its own.

*The residual, stated rather than hidden:* that one construction failing is not a proof that the compiler
systematically refuses every table-input cycle. If one ever does form, the exposure is specific to THIS
flavour and to the branch below: `OnActivateAsync` makes an outbound `ActorProxy` call, and Dapr activates
an actor on demand, so ordinary traffic — not just an explicit start sequence — could drive
A.activate → B.attach → B.activate → A.attach, which queues behind A's in-flight activation turn and
blocks until Dapr's actor call timeout. Orleans has no counterpart: its `TableGrain` attaches only from
`StartAsync` and overrides no activation hook. The failure mode is a loud timeout, not a silent wrong
answer, which is why this is documented rather than guarded against. The orchestrator no longer registers the router on a successful start — only `TableActor` itself
does, which is what makes the register-then-attach ordering hold: while this actor's own `StartAsync`
turn is still executing, any `TableEventRouter`-issued call the fresh registration provokes against this
SAME actor id queues behind that turn (Dapr actors process one invocation at a time per actor id) rather
than being dropped (unregistered) or interleaved with it. See `Actors/TableActor.cs`'s
`RegisterRouterAndAttachToTableInputsAsync` doc comment for the full protocol and
`dapr/tests/StreamForge.Dapr.Tests/TableAttachPolicyTests.cs` for the epoch-cutoff filter this relies on
to make a delta admitted during that queued window neither lost nor double-counted.

**Snapshot/search/seq design — mirrors `TableGrain`'s classic path field-for-field:**
- *Write-behind snapshot, not live-per-delta.* `GetRowsAsync`/`GetRowCountAsync`/`GetSeqAsync` are served
  from a flushed copy (`_flushed`, up to ~2s stale — `TableActorState.Snapshot`/`.Seq`), updated only by a
  2s flush timer or on stop/deactivate — the identical staleness `TableGrain`'s own classic path already
  has (only its Parallelism≥2 coordinator mode reads live). `SearchAsync` instead reads the LIVE
  `TableExecutor.Snapshot()` for weight lookup (the search index itself is also kept live, updated
  incrementally per delta) — mirroring `TableGrain.SearchAsync`'s identical live-vs-flushed split.
- *Two distinct, independently-owned sequence counters — do not conflate them.* `GetSeqAsync` is a
  flush-generation counter (`TableActorState.Seq`, incremented once per `FlushAsync` call, resets to 0 on
  a detected resume) — a REST read-cursor concept identical to `TableGrain.state.State.Seq`.
  `TableDeltaEnvelope.Seq` (published on `sf-table-delta`) is a SEPARATE, unpersisted, per-published-BATCH
  counter `TableActor` owns and increments once per non-empty delta batch — unlike Orleans, where
  `StreamBridgeService` invents its own `_tableSeq` locally per SignalR subscription, `DaprStreamBridge`
  here only relays the value `TableActor` already stamped (see that class's own doc comment). Verified
  against real compiled-Engine output (not synthetic data) in
  `dapr/tests/StreamForge.Dapr.Tests/TableDeltaSequencingTests.cs`.
- *Restart-resume limitation — identical to `TableGrain`'s, not a new one.* The persisted snapshot only
  ever captures OUTPUT rows, never operator internal state (join indexes, GROUP BY accumulators), so
  `ActivateExecutor` (shared by `StartAsync` and `OnActivateAsync`'s self-heal branch) detects a non-empty
  persisted snapshot as "this is a resume" and wipes it (`Rebuilding=true`), rebuilding purely from live
  traffic — exactly `TableGrain.StartClassicAsync`'s own rule. One honest, DOCUMENTED deviation from
  Orleans' incidental behavior: because Dapr actors activate on-demand (any RPC, including a read,
  triggers `OnActivateAsync` first), there is no window where a stale pre-restart snapshot is briefly
  served before the resume-reset runs the way there incidentally is on Orleans (a REST read racing
  `RegistryGrain`'s boot resume loop) — the very first read after a Dapr restart already reflects
  `Rebuilding`, which is earlier/more honest disclosure, not later.
- Search index rebuilt from the (post-reset, therefore empty either way) snapshot on activation, per the
  wave brief; fills back in incrementally as deltas land.

**Topo-order for table-over-table chains — the simpler choice, taken deliberately (`Services/
TableSupervisorService.cs`).** Unlike `PipelineSupervisorService`'s straight `IsRunningAsync` check (a
pipeline never depends on another pipeline), a Running table CAN depend on another Running table. Rather
than computing a topological order, the ~15s sweep simply lets `CatalogStore.SetTableStatusAsync`'s
existing dependency guard (`"table input(s) not running: ..."` → `Status=Failed`) do the work: a table
whose upstream hasn't started yet fails this sweep's attempt and is retried on the NEXT sweep, by which
point an earlier iteration has very likely already started its upstream. This converges over a few sweep
periods with no graph analysis. The shipped seed has no Running table that depends on another Running
table (`hot_symbols` — the only table-over-table demo — is seeded `Stopped` on purpose, so starting the
chain is a deliberate user action); this codepath exists for user-created chains, not the shipped seed —
confirmed live (see below): starting `hot_symbols` manually after `positions` was already `Running`
filled it within one sweep, and a FULL HOST RESTART with both `Running` resumed the entire chain with zero
REST calls.

**Un-force-stopping seeded tables (mirrors W6's identical pipeline change).** `CatalogStore.EnsureInitialized`
no longer overrides every seeded table to `Stopped` — see the updated "Seed status" decision above. The one
remaining override is defensive: a table whose SQL fails to compile is forced `Stopped` regardless of what
`SeedCatalog` requested.

**JsonElement re-normalization — the same actor-wire finding as `PipelineActor.ProcessEventsAsync`, proven
for BOTH of `TableActor`'s ingress methods.** `ProcessSourceEventsAsync(SourceEventsEnvelope)` needs the
same re-normalization `PipelineActor` already does (proven again here for symmetry/documentation).
`ProcessTableDeltasAsync(TableDeltaEnvelope)` is a genuinely NEW wire-crossing scenario `PipelineActor`
never has to handle (a pipeline never consumes another table's deltas) — `TableDeltaEnvelope.Deltas[].Row`
crosses the identical Dapr actor-invocation wire and needs the identical treatment. Both proven by
round-tripping an already-normalized envelope through the actor wire's own serializer configuration in
`dapr/tests/StreamForge.Dapr.Tests/TableActorWireNormalizationTests.cs`.

**Concurrency bug found and fixed live, upstream of this wave's own code:** `Streaming/SourceRateSampler.cs`
(W5-B) kept its per-source "last relayed" timestamps in a plain, unsynchronized `Dictionary` shared as a
singleton field of `DaprStreamBridge`. With W7-A's tables (and W6's pipelines) all consuming `sf-sources`
concurrently, concurrent `ShouldRelay` calls corrupted the dictionary's internal bucket array under load —
observed live as an intermittent `IndexOutOfRangeException` inside `Dictionary.set_Item` that took the
WHOLE `sf-sources` dispatch down for that request (`StreamingRuntimeSetup.DispatchSourceEventsAsync`'s
un-guarded `foreach` means one sink throwing stops every later-registered sink — including
`TableEventRouter`/`PipelineEventRouter` — from ever seeing that batch). Fixed with a single `lock` around
the read-then-write in `ShouldRelay`; the fix is out-of-wave-scope by file ownership but directly blocked
this wave's live verification, so it was applied as a minimal, targeted thread-safety fix rather than
worked around.

**Live-verified end-to-end (fresh seed, `./dapr/tools/reset.sh` + `./dapr/tools/run.sh`):**
- `GET /api/tables` shows `positions`/`leg_exposure`/`order_states` `Running` on boot with **no**
  `POST .../start` call issued; `gold_tier_orders`/`hot_symbols` stay `Stopped` as seeded.
- `positions` (`SearchMode: Fuzzy`) accumulates real rows: `GET .../rows` returns 8 GROUP-BY-symbol
  aggregate rows with growing `trades`/`total_qty`/`avg_price`; `GET .../metrics` shows `deltasIn`/
  `deltasOut` growing tick over tick, `rowCount` stable once every symbol has been seen.
  `GET .../search?q=AAPL` AND `GET .../search?q=APPL` (typo) both return the same row — fuzzy
  trigram+Levenshtein matching confirmed live, not just in the `TableSearchIndex` unit tests.
- `leg_exposure` (UNNEST + JSON `->`) and `order_states` (`LATEST BY`) both accumulate rows/metrics too —
  not just the simplest seeded table.
- Table-over-table: starting `hot_symbols` (`FROM positions WHERE trades > 50`) via `POST .../start`
  filled it with exactly the symbols whose `positions` trade count had crossed 50, within one host tick —
  proving `TableEventRouter`'s table-input routing end-to-end, not just its unit-tested routing table.
- SignalR: a bun script using `@microsoft/signalr` against `/hubs/stream`, subscribed to
  `table:positions`/`table:hot_symbols`/`source:trades`, observed 128 `tableDelta` events (both the
  direct table AND the table-over-table one, with correct retract(-1)/assert(+1) weight pairs cascading
  from `positions`' delta into `hot_symbols`') and 40 `sourceEvent` events in an 8s window.
- Stop/start via REST: `POST .../stop` on a table with a Running dependent correctly 409s
  (`ThrowIfRunningDependents`); stopping the dependent first then the table succeeds, `deltasIn` stops
  growing while stopped, and `POST .../start` resumes accumulation immediately.
- Full host restart (kill + `./dapr/tools/run.sh`, Redis-backed state survives, no `reset.sh`): both
  `positions` (a direct table) and `hot_symbols` (its table-over-table dependent) came back `Running` with
  **no REST call**, and the table-over-table chain fully reconstituted itself (rowCount back to 8 for both)
  within about 30s of the restart — the "one honest deviation" documented above (a brief `Rebuilding`
  window, not a stale-snapshot window) observed directly: the very first `/rows` read (~5s post-restart)
  already showed a smaller, freshly-rebuilding row/seq count, not the pre-restart snapshot.
- Ports verified clear (`5399`/`3599`/`4599`) after `dapr stop` + explicit process kill (the Dapr CLI's
  `stop` reliably tears down the sidecar but, observed live, sometimes leaves the plain `dotnet run` app
  process itself alive holding the port — kill it explicitly and re-check before the next run).

## Row history (W7-B)

One `TableHistoryActor` per table that has ever had row history configured (actor type
"TableHistoryActor", key = the table's Name) — the Dapr counterpart of Orleans' `TableHistoryGrain`
(`orleans/src/StreamForge.Host/Grains/TableHistoryGrain.cs`), fed by the `sf-table-delta` topic — decision
D-D's fixed-topic transport, and decision D7's framing that **the delta stream IS the event log** (no
separate history-specific stream/subscription; row history is opt-in downstream consumption of the exact
same envelope `TableActor` already publishes for every table). `Actors/ITableHistoryActor.cs`,
`Actors/TableHistoryActor.cs`.

**State and retention math are the identical shared code the Orleans grain uses.** `TableHistoryActorState`
mirrors `TableHistoryGrainState` field-for-field (HistoryEnabled/Mode/Limit/ByField/WindowMs,
`IdentityColumns`, the `Entries` version-history dictionary, the monotonic `Seq` counter); all
identity-key derivation and retention application goes through the exact same pure, Orleans-free classes
the grain calls — `StreamForge.Host.Grains.TableGroupKeyExtractor` / `RowKeyCodec` /
`TableRowHistoryRetention` (`shared/StreamForge.AppCore/History/TableRowHistory.cs`, moved there from Host
by plan 005's W2 AppCore extraction, namespace frozen per decision D-C). The actor shell's own
state-transition/query logic is further extracted to a pure `TableHistoryApplication` class (mirroring
`PipelineCompilation`/`GeneratorBatching`'s own extraction rationale — testable without any actor/timer/
sidecar machinery; see `dapr/tests/StreamForge.Dapr.Tests/TableHistoryApplicationTests.cs`), so
`TableHistoryActor` itself is a thin shell: activation/state load-save, timer arm/disarm, and the
`ITableHistoryActor` method signatures.

**`ResetAsync` config-change semantics mirror the grain exactly.** `TableHistoryApplication.Reset` ALWAYS
returns a brand-new `TableHistoryActorState` — never a mutated existing one — clearing every previously
accumulated entry regardless of whether the call was triggered by a genuine config/SQL change or just a
table re-create, exactly like `TableHistoryGrain.ResetAsync`'s own doc comment ("Always clears previously
accumulated history"). `IdentityColumns` is re-derived from `def.Sql` on every Reset (a SQL change can
change the GROUP BY identity, so a stale mapping is never carried forward). `DisableAsync` goes further —
a fully zeroed `TableHistoryActorState`, matching `TableHistoryGrain.DisableAsync`. Both call sites
(`Catalog/CatalogStore.cs`'s `CreateTableAsync`/`UpdateTableAsync`/`DeleteTableAsync`, via
`ILifecycleOrchestrator.ResetTableHistoryAsync`/`DisableTableHistoryAsync`) were already wired in an
earlier wave; W7-B only fills in the two methods themselves
(`Lifecycle/DaprLifecycleOrchestrator.History.cs`) — an inline, synchronously-awaited actor call, safe from
the reentrancy hazard this document's own decision describes because `TableHistoryActor` is a pure LEAF
(it never resolves `ICatalogFacade`, an `IRegistryActor`/`ITableActor` proxy, or any other actor — everything
it needs arrives as a method parameter, and unlike `GeneratorActor`/`PipelineActor` it never even talks
outward to Dapr pub/sub).

**Write-behind cadence mirrors the grain's 2-second flush timer — not per-delta.**
`TableHistoryApplication.ApplyDeltas` mutates the actor's in-memory state and returns a dirty flag; the
actor only calls `StateManager.SetStateAsync` from its own periodic flush timer tick (armed/disarmed
alongside `HistoryEnabled`, self-healing on reactivation exactly like `GeneratorActor`/`PipelineActor`'s own
timers — Dapr actor timers do not survive deactivation) or from a best-effort flush on deactivation.
`ResetAsync`/`DisableAsync` are the two exceptions, persisting immediately (rare, config-triggered calls,
not the hot per-delta path) — same asymmetry as the grain's own `ResetAsync`/`DisableAsync` vs.
`OnDeltaBatchAsync`.

**Sink enable-map: skip, don't forward-and-let-the-actor-no-op.** `Streaming/TableHistoryDeltaSink.cs`
registers as one more `ITableDeltaSink` (see `Streaming/Sinks.cs`'s class doc) alongside `DaprStreamBridge`
— but unlike that bridge (which relays every table's deltas to SignalR unconditionally), the sink first
checks a `TableHistoryEnabledMap` and skips forwarding entirely for a disabled/unknown table, rather than
constructing an `ITableHistoryActor` proxy and letting the actor's own `HistoryEnabled` check no-op it.
Chosen because EVERY table's delta batch crosses `sf-table-delta` regardless of whether that table has
history enabled — forwarding all of them to an actor-proxy call would be a sidecar round trip (activation +
JSON (de)serialize) for nothing, on every batch, of every table that never turned history on, forever; the
in-memory map lookup is free by comparison. The map is updated synchronously, inline, by
`DaprLifecycleOrchestrator.ResetTableHistoryAsync`/`DisableTableHistoryAsync` as part of the very same
`CatalogStore` call that creates/updates/deletes the table — and a newly created table starts `Stopped`
(no deltas at all) until a later, separate start call, so there is no observable race window where a delta
could arrive before the map reflects the table's current `HistoryEnabled` value. `TableHistoryEnabledMap`
is a static singleton instance (`TableHistoryEnabledMap.Instance`), also registered as its own DI
singleton — deliberately NOT a constructor-injected dependency of `DaprLifecycleOrchestrator` the way
`Streaming.PipelineEventRouter`/`TableEventRouter` are, because `DaprLifecycleOrchestrator.History.cs`
(W7-B's file) cannot add a parameter to `DaprLifecycleOrchestrator`'s primary constructor, which lives in
`DaprLifecycleOrchestrator.cs` (W7-A's file this wave) — mirrors `Actors/ActorProxyDefaults.cs`'s own
static-shared-instance precedent instead.

**Key-codec parity with the REST endpoint — verified, not assumed.** The shared endpoint
(`shared/StreamForge.Api/Endpoints/TablesEndpoints.cs`'s `POST /{id}/history/lookup` handler) derives the
row-identity lookup key ITSELF, from the request's raw row, via the same
`TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql)` + `RowKeyCodec.EncodeIdentity` calls
`TableHistoryActor.ResetAsync` makes for live deltas — **the endpoint, not the grain/actor, owns key
derivation**, identically on both runtimes (decision D-B: one shared endpoint body). `Facades/
DaprTableHistoryFacade.GetHistoryAsync(tableName, key, limit)` therefore receives an ALREADY-ENCODED key
and does no derivation of its own — it just forwards to `ITableHistoryActor.GetHistoryAsync`. Because both
derivations run the identical pure function against the identical `TableDefinition.Sql`, the two keys are
guaranteed to match; `dapr/tests/StreamForge.Dapr.Tests/TableHistoryKeyCodecParityTests.cs` proves this
end-to-end (endpoint-style key lookup finds the entry the actor accumulated from live deltas), including the
no-GROUP-BY whole-row-fallback case.

**JsonElement re-normalization — the same actor-wire finding as `PipelineActor.ProcessEventsAsync`.**
`TableHistoryDeltaSink` forwards an already-normalized `TableDeltaEnvelope` (normalized once at the
`sf-table-delta` pub/sub ingress, `StreamingRuntimeSetup.NormalizeTableDeltaRows`), but the Dapr
actor-invocation call re-boxes every `Row` value as a `JsonElement` again once it crosses into
`TableHistoryActor.ApplyDeltasAsync`'s method body (System.Text.Json has no static type for a
`Dictionary<string, object?>` value). `TableHistoryApplication.ApplyDeltas` re-normalizes
(`JsonValueNormalizer.NormalizeInPlace`) before `RowKeyCodec` or `TableRowHistoryRetention` ever see a
delta's row — proven by round-tripping an already-normalized envelope through the actor wire's own
serializer configuration in `TableHistoryApplicationTests.cs`, the same technique
`PipelineActorWireNormalizationTests.cs` uses for `sf-sources`.

## Ingestion connectors (plan 006)

Plan [`../plans/006-connectors-and-config.md`](../plans/006-connectors-and-config.md) adds real
ingestion (`url`/`file`/`folder`/`grpc`-subscribe source kinds) and config import/export on top of
this flavor, sharing the exact same core the Orleans flavor uses: `AppCore/Connectors/**` (schedule/
backoff, JSONPath-lite mapping, format parsers, OpenAPI derivation, `GrpcSubscriberCore`) and
`ConnectorPollCycle` — the pure step function neither flavor duplicates. `RegistryActor`'s
`CatalogStore` no longer decides start/stop directly for connector-kind sources;
`DaprLifecycleOrchestrator` dispatches on `SourceDefinition.Kind` via `SourceKindDispatch.Classify`
(null/empty/`"generator"` → `ActorKind.Generator`, the pre-006 `GeneratorActor` path; everything
else → `ActorKind.Connector`) and **always stops the other kind's actor too, idempotently** — a
source's `Kind` can change on an update (e.g. `generator` → `url`), so both paths get an
unconditional stop before the correct one starts, rather than tracking the previous `Kind`
separately.

`ConnectorActor` (`dapr/src/StreamForge.Dapr.Host/Actors/ConnectorActor.cs`, actor type
`"ConnectorActor"`, key = source name) is the Dapr counterpart of `ConnectorGrain`: url/file/folder
kinds are a **one-shot actor timer**, re-armed after every fire at the freshly computed next-due
time (Dapr's actor timer treats `period == Timeout.InfiniteTimeSpan` as a genuine one-shot); `grpc`
is a persistent background `Task` running `GrpcSubscriberCore.RunAsync` for the actor's lifetime,
with no timer at all. State (`ConnectorActorState` — `Def`, `Running`, dedup/ledger snapshots, every
`ConnectorRuntimeStatus` field) is fully persisted because, exactly like `GeneratorActor`, Dapr actor
timers do **not** survive deactivation/reactivation; `OnActivateAsync` self-resumes — re-arms the
poll timer from the persisted `NextRunMs` (clamped to "now" if already past), or relaunches the gRPC
subscriber task. `GeneratorSupervisorService`'s periodic sweep remains the safety net for a
connector-kind source whose actor was never activated at all.

**Acyclic by construction, with one carefully-scoped exception**: `ConnectorActor` never resolves
`ICatalogFacade`/an `IRegistryActor` proxy/any other actor from inside its own turn — same discipline
as `GeneratorActor`/`PipelineActor` (see the reentrancy decision above). The gRPC kind's background
subscriber task calls back into **this same actor type** via a fresh `IConnectorActor` proxy
(`RecordSubscriberBatchAsync`) to record each batch/status — that happens on an independent
background thread, not from inside an in-flight turn, so from the Dapr runtime's perspective it is an
ordinary new inbound invocation, not a reentrant self-call.

**Config export/import** (`shared/StreamForge.Api/Endpoints/ConfigEndpoints.cs` — see the Orleans
architecture doc for the full compose/merge/apply-order contract) is byte-identical code on this
flavor too, reached through the same `ICatalogFacade`; connector *definitions* travel via export/
import like any other source, but runtime state — dedup ledgers, connector counters/status — is
never exported and stays entirely per-flavor (Redis actor state here, a JSON grain-state file on
Orleans), verified W6.

**Statestore scoping caveat**: the shared `dapr/components/statestore.yaml` pins `keyPrefix: appid`
and `scopes: [streamforge-dapr]` (see "Decisions made this wave" above) — every actor's state,
`ConnectorActor` included, is reachable only under app-id `streamforge-dapr`. An isolated-app-id test
instance (a different `--app-id` for test isolation, the pattern AGENTS.md documents for Orleans'
arbitrary-port instances) has no statestore component in scope for actors at all, which panics the
1.18 sidecar rather than degrading gracefully — a known environmental limitation, not a StreamForge
bug: connector/actor-level tests on this flavor run against the shared fixed-port `streamforge-dapr`
instance (reset via `tools/reset.sh` first for a clean slate), never an isolated app-id.

**Federation**: this flavor can run a `grpc`-kind source subscribing to *any* StreamForge instance's
`DynamicStreamService` — including the Orleans flavor's, proven live in W6 (Dapr `:5399` subscribed
the Orleans flavor's `positions` table by id, over both gRPC reflection and proto-text schema paths;
`eventsEmittedTotal` climbed 418→498 in 10 s of real cross-runtime traffic). Being a federation
*server* stays Orleans-only — see "gRPC serving" below; this flavor's gRPC port (`:5499`) still only
serves proto downloads, never `SubscribeEntity`.

## Environment isolation (plan 021)

**One singleton actor per environment**, exactly like the Orleans flavor's grain: `RegistryActor`'s actor
id is `EnvKeys.Qualify(environment, StreamConstants.RegistryKey)` (`"catalog"` for the default environment
— `EnvKeys.Qualify("", k) == k` — byte-identical to every pre-plan-021 Redis key), and every name-keyed
actor kind (`GeneratorActor`, `ConnectorActor`, `TableActor`, `TableHistoryActor`) is created the same
way. `PipelineActor` stays keyed by its GUID id, needing no qualification. `EnvironmentRegistryActor`
(id = `StreamConstants.EnvironmentsKey`, `"environments"`, backed by `EnvironmentRegistryStore` — a plain,
actor-framework-free class the actor delegates to, same pattern `CatalogStore` set for `RegistryActor`) is
the one singleton that stays unqualified, for the identical reason the Orleans grain does.

**An actor learns its own environment from its own actor id, not from an ambient**, the same discipline
`RegistryGrain` uses: `RegistryActor.OnActivateAsync` reads `EnvKeys.EnvOf(Id.GetId())` — `EnvKeys.EnvOf`
on an id with no `.` returns the empty string, so `RegistryActor`'s id `"catalog"` yields the default
environment and `"staging.catalog"` yields `"staging"`. `CatalogStore` is constructed with that string
(`environment` parameter, default `EnvKeys.Default`) and never reads an ambient itself. Because every
`ICatalogFacade` consumer in this project is one of two shapes — a request-scoped one that wants
`EnvironmentAmbient.Current`, or a background sweep that must visit every environment in turn —
`ICatalogFacadeFactory.For(environment)` (`Facades/CatalogFacadeFactory.cs`) is what lets the background
kind exist without a facade-per-environment DI registration: it hands back a fresh
`ICatalogFacade`/`RegistryActor` proxy pair for an explicit environment name, on demand, as many times as
asked. The four periodic supervisors (`GeneratorSupervisorService`, `PipelineSupervisorService`,
`TableSupervisorService`, `TableHistorySupervisorService`) and `IngestDrainPumpService` all use it to loop
`IEnvironmentRegistryGrain`-equivalent's `ListAsync()` result, exactly like Orleans' boot-time sweeps do.

**Seeding is default-environment-only here too.** `CatalogInitializationService` runs UNQUALIFIED — i.e.
always against the default environment's catalog — the same guard Orleans' `RegistryGrain.EnsureInitializedAsync`
applies per-environment; a named environment starts with an empty catalog and stays that way until
something writes to it.

**No per-environment stream namespace — the entity key inside a fixed topic is qualified instead**, and
that is the one place this flavor's shape genuinely differs from Orleans'. The five app-wide pub/sub
topics (`StreamingRuntimeSetup.cs`) do not multiply per environment; every publisher (`GeneratorActor`,
`ConnectorActor`, `PipelineActor`, `TableActor`) stamps its own **qualified** actor id into the envelope's
entity-key field (`envelope.Source` / `envelope.Table` / `envelope.PipelineId`) before publishing, and
dispatch reads that key back out of the envelope after receipt — so routing to the right actor/subscriber
falls out of the qualified key for free, the same way plan 016's federation reused id-or-name resolution
instead of adding a new addressing scheme. `DaprStreamBridge` follows one rule per envelope, stated in its
own comments: **group qualified, payload bare** — the SignalR group name is the envelope's entity key
as-is (already qualified, matching `StreamHub`'s subscribe-side `$"table:{EnvKeys.Qualify(env, name)}"`),
while the argument value handed to the browser is `EnvKeys.Split(key).Key` — stripped back to bare —
because the SPA keys its own state on the plain name and never sees a qualified one anywhere else. The
`"metrics"` group is the same deliberately-unqualified exception both flavors share.

**"Strip the qualification at the Engine boundary" is the rule this flavor has to state explicitly that
Orleans gets for free.** `TableExecutor`/`PipelineExecutor` — the shared, environment-unaware Engine (hard
rule 2: no runtime types inside it, which includes no notion of "environment") — are built once per actor
against that actor's own **bare**, unqualified `StreamInputs`/`SourceNames`. But the envelope arriving
from a fixed, cross-environment topic carries the **qualified** key, because that is what let dispatch
find the right actor in the first place. `TableActor.ProcessSourceEventsAsync` and
`ProcessTableDeltasAsync`, and `PipelineActor`'s event handler, all call
`EnvKeys.Split(envelope.Source /* or .Table */).Key` — discarding the environment half — immediately
before handing the bare name to `_executor.OnEvent`/`OnStreamEvent`/`OnTableDeltaBatch`. Skipping this
strip would make every compiled plan's own key lookups miss (the Engine was built with bare keys) the
moment more than one environment existed. Orleans never needs an equivalent line: its streams are already
per-entity (`StreamId.Create(namespace, qualifiedKey)`), so a subscriber only ever receives its own
already-unambiguous stream and the Engine is called with the same bare name it was compiled against by
construction.

## What's NOT here yet (by design — permanent descopes, not later waves)

Every wave through W8 has landed (Generators W5, Pipelines W6, Tables W7-A, Row history W7-B,
polyglot processors W8 — see `../dapr/POLYGLOT.md` and `../plans/005-dapr-port.md`'s parity matrix).
What remains unbuilt on this flavor is, as of W9, a short, deliberate, permanent list — not a backlog.
(The *other* two lists — what is genuinely owed to this flavor, and which of its "verified live" claims
were never actually run — live in [`PARITY.md`](PARITY.md), added 2026-08-20.)

- **Partitioned table execution** (`Parallelism 2–16`, frontier-consistent reads, shared
  arrangements): **Orleans-only forever**, decision D-F — sidecar hops invert the economics of the
  stage-grid design at this scale. `CatalogStore.ValidateParallelism` rejects anything but `1` with a
  clear 409; `/api/meta/arrangements` always returns `[]`; `frontierEpoch` is always `null`.
- **gRPC serving** (phase 2, decision D-F): `:5499` is reserved but nothing listens on it yet;
  `GET /api/meta/grpc` reports it with an empty static-service list (shape preserved). `/proto`
  downloads work today regardless (the shared descriptor machinery in `shared/StreamForge.AppCore`
  doesn't care which runtime called it) — only the live gRPC *serving* endpoint is phase 2.
- **`/docs`**: served — the same flavor-aware `orleans/docs/index.html` (+ sibling pages like `comparison.html`) the Orleans host serves; the original W4 null-`DocsFilePath` descope was removed once the docs covered both runtimes (post-plan-005 addendum).

## Known live bug found during W9 benchmarking — FIXED

Running plan 005 W9's latency benchmark (`tools/bench/`) against a freshly booted Orleans-flavor
instance (not the Dapr flavor covered by the rest of this document — flagged here because it was
discovered while proving out the SAME benchmark harness against both flavors, and it materially
affects how to read the comparison numbers) surfaced a live defect: the Orleans flavor's SignalR
`tableDelta` relay (and, observed the same session, `pipelineResult`) never delivered to a subscribed
client on a freshly booted instance — zero events over repeated 15–90s windows across three different
seeded tables (`order_states`, `positions`, `leg_exposure`), while the SAME tables' REST
`/api/tables/{id}/metrics` showed `deltasIn`/`deltasOut` growing continuously throughout. `sourceEvent`
and `pipelineMetrics` worked correctly in the same session.

**Root cause (confirmed by direct repro + git-bisect against `ad7652d`, the pre-005 commit): a
startup race, present on both sides of the 005 restructure — not a 005 regression.**
`orleans/src/StreamForge.Host/Program.cs` kicks off registry seeding
(`RegistryGrain.EnsureInitializedAsync`, which both seeds the catalog *and* starts every
already-`Running` pipeline/table grain) from a fire-and-forget `ApplicationStarted` callback. That
callback races `StreamBridgeService.ExecuteAsync`'s own `ApplicationStarted`-gated enumeration of
`GetPipelinesAsync()`/`GetTablesAsync()`. On a *fresh* boot (no persisted catalog yet — exactly what
`tools/bench/run-bench.sh` and a first-ever install both do), `StreamBridgeService` can easily query
the registry before seeding has populated it, see an empty catalog, and subscribe to nothing.
Source subscriptions self-heal (a 30s timer re-scans `GetSourcesAsync()`), but pipeline/table
subscriptions are enumerated exactly once at boot with no retry — so a lost race meant `tableDelta`/
`pipelineResult` were subscribed to *never*, for the lifetime of the process, even though the
underlying grains kept producing deltas the whole time (matching the observed REST-metrics-vs-SignalR
mismatch exactly). Empirically: a **warm** boot (persisted catalog already on disk) never loses this
race — `RegistryGrain.OnActivateAsync` loads persisted state synchronously as part of activation,
before `EnsureInitializedAsync` is ever called — which is why this had gone unnoticed in ordinary
manual testing (SPA load + a few seconds' delay before subscribing) and only showed up under the
benchmark's fresh-instance-then-immediately-measure methodology. Confirmed identical on `ad7652d`
(pre-005) with the exact same fresh-boot repro (0 samples for `tableDelta`/`pipelineResult`,
warm-boot fully working) — so the originally-suspected cross-assembly Orleans serializer codegen
(W1, `List<TableDeltaDto>`/`List<ResultEnvelope>` payloads) was **not** the cause; a warm-boot test at
master delivering both signals correctly independently rules that theory out too.

**Fix**: `StreamBridgeService.ExecuteAsync` now awaits `registry.EnsureInitializedAsync()` itself
before enumerating pipelines/tables/sources, instead of trusting that Program.cs's fire-and-forget
call got there first. `EnsureInitializedAsync` is idempotent and Orleans serializes both callers'
calls to it (not `[MayInterleave]`), so whichever caller's turn runs first does the real seed+start
and the other is a fast no-op — the race becomes harmless. This bug is specific to the Orleans
flavor's `StreamBridgeService` (`orleans/src/StreamForge.Host/Services/StreamBridgeService.cs`); the
Dapr flavor's equivalent (`DaprStreamBridge`) was never affected (no equivalent race — see
`dapr/src/StreamForge.Dapr.Host` startup sequencing) and delivered `tableDelta` correctly throughout.
Regression coverage: `orleans/tests/StreamForge.Host.Tests/StreamBridgeServiceStartupRaceTests.cs`
(a `TestCluster`-based test that starts `StreamBridgeService` before anything has seeded the
registry — the losing side of the race — and asserts `tableDelta`/`pipelineResult` still relay).
See `orleans/docs/comparison.html`'s "Measured latency" section for the re-measured, now-real Orleans
`tableDelta` numbers — and for the post-plan root cause of the original 17× gap: Orleans' stock
memory streams are pull-based (100ms polling agents ×2 stream hops), not slower actors. With the
Orleans flavor's switchable push transport (`--Streams:Transport push`) the same benchmark measures
p50 1ms vs this flavor's 7ms — i.e. each runtime's latency is dominated by its transport (Redis
pub/sub round-trips here), which is the honest framing for client conversations.

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
