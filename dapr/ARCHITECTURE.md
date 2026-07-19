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
| `GeneratorGrain` | *(not yet — W5)* | `ILifecycleOrchestrator.NotifySourceChangedAsync` currently just logs. |
| `PipelineGrain` | *(not yet — W6)* | `IPipelineReadFacade` is stubbed (empty results, zeroed metrics); `ILifecycleOrchestrator.StartPipelineAsync` currently just logs and reports success. |
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

### Seed status: seeded pipelines/tables are forced to `Stopped`

`shared/StreamForge.AppCore/SeedCatalog` marks several demo pipelines/tables `Running` (the Orleans flavor
resumes them for real on boot, per `RegistryGrain.EnsureInitializedAsync`). On the Dapr flavor, W4 has
**no runtime at all** behind a Running status — no generator publishing events, no pipeline/table actually
computing anything. Serving a seeded "Running" badge with zero rows ever arriving would be a UI lie, so
`CatalogStore.EnsureInitialized` explicitly overrides every seeded pipeline/table to `Stopped` regardless
of what `SeedCatalog` says, before persisting. Verified live: a fresh seed shows `GET /api/tables` /
`GET /api/pipelines` with every entity `Stopped`.

A user can still manually `POST .../start` on a seeded/created pipeline or table today — see the next
decision for what that does (and does not) do.

### The `ILifecycleOrchestrator` seam — and the reentrancy decision

Every place `RegistryGrain` reaches into another grain to actually start/stop a runtime process
(`GeneratorGrain.StartAsync/StopAsync`, `PipelineGrain.StartAsync/StopAsync`, `TableGrain.StartAsync/
StopAsync`, `ITableHistoryGrain.ResetAsync/DisableAsync`, plus the lifecycle-stream publish) is routed,
in `CatalogStore`, through `Lifecycle/ILifecycleOrchestrator` instead of a direct actor-to-actor call.
W4's implementation, `NoopLifecycleOrchestrator`, logs a warning ("no runtime yet (W5/W6/W7)") and
reports **success** for every start call — so `POST /api/pipelines/{id}/start` /
`POST /api/tables/{id}/start` do flip the catalog status to `Running` and persist it (verified live), they
just don't yet start anything real. This is "Running-but-inert" for anything a user starts by hand after
boot, alongside the "seeded-Stopped" default above — two different, both-documented answers to the same
honesty question for two different triggers (seed vs. explicit user action), chosen because an explicit
`/start` call is the user asking for exactly this bookkeeping-only behavior today, whereas a seed silently
claiming "Running" for something nobody asked to start would not be.

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

## What's NOT here yet (by design — later waves)

- **Generators** (W5): sources have no synthetic event publisher; `Enabled` is stored but inert.
- **Pipelines** (W6): no SQL execution; results/metrics endpoints return empty/zeroed shapes
  (`Facades/StubFacades.cs`, each marked with a `W6 replaces this` comment).
- **Tables/history** (W7): no Z-set execution, no rows, no search, no history
  (`Facades/StubFacades.cs`, each marked with a `W6/W7 replaces this` comment).
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

~/.dotnet/dotnet test dapr/StreamForge.Dapr.sln     # 36 tests (JsonValueNormalizer + CatalogStore)
```

Logins: `admin/admin123!`, `editor/editor123!`, `viewer/viewer123!` — same as the Orleans flavor (shared
`SeedCatalog.Users`). Ports: app `5399`, gRPC-reserved `5499`, sidecar HTTP `3599` / gRPC `4599`. Never
bind or kill Orleans' `5199`/`5299`.
