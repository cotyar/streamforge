# Plan 021 — Environment isolation: non-overlapping catalogs inside one server

**Status: DONE** — waves 0–2 landed (the plan's 021-A through 021-F collapsed into three commits: wave 0
built the shared vocabulary, wave 1 wired both runtimes' registries plus the REST middleware, wave 2
qualified every remaining name-keyed grain/actor key, the two sink kinds that name a catalog entity, and
the SPA/CLI/MCP surfaces). What actually landed, and what was found and deliberately left, are recorded
at the end of this document.

**Depends on**: nothing hard. Cheaper after 016-A (see "Where this belongs in the queue"); 015 turns it
from a namespace into a security boundary (see D9, which is the sentence not to skip).

## Why

Deploying a new set of entities into a running server today lands them in the one and only catalog, next to
everything already there, sharing one name space. There is no way to say "put this pipeline and its three
tables somewhere nobody else's queries can see, and where a table called `orders` does not collide with the
`orders` that already exists". The only isolation the platform offers is *another whole server* — which is
what plan 016 means by "environment" today: `@name` endpoint aliases resolved from process configuration,
one deployed instance per environment (`plans/016-identity-versioning-discovery.md:140-150`).

This plan makes an environment a **partition inside one running server**: named, created deliberately,
non-overlapping in catalog, in names, in the SQL namespace and in the runtime's grain/actor/stream keys.

## What an environment is, and what it is not

**Is**: a disjoint catalog. Its own sources, pipelines, tables and sinks; its own name uniqueness; its own
SQL namespace, so `FROM orders` in `staging` can never resolve to `prod`'s `orders`; its own runtime
entities, so two same-named tables in two environments are two grains with two states.

**Is not**, in this plan:

- **A security boundary.** See D9. Until 015 lands, any authenticated user can address any environment.
- **A resource boundary.** One process, one heap, one thread pool. A runaway pipeline in `staging` starves
  `prod` exactly as much as it does today. Environments partition *naming and state*, not capacity.
- **A separate cluster, silo or sidecar.** No second Orleans cluster, no second Dapr app-id — and the
  latter is not an oversight: an isolated `--app-id` is documented as *broken* for actors, panicking the
  1.18 sidecar rather than degrading (`dapr/ARCHITECTURE.md:623-628`).

## Decisions, and what they cost

### D1. The environment is the registry key, not a field on every entity

The catalog is one singleton per flavour holding one blob — Orleans `RegistryGrain` at the fixed key
`StreamConstants.RegistryKey` = `"catalog"` (`shared/StreamForge.Contracts/StreamConstants.cs:14`,
state shape `orleans/src/StreamForge.Host/Grains/RegistryGrain.cs:13-23`), Dapr `RegistryActor` at the same
actor id wrapping `CatalogStore` (`dapr/src/StreamForge.Dapr.Host/Actors/RegistryActor.cs:8-19`,
`dapr/src/StreamForge.Dapr.Host/Catalog/CatalogStore.cs`).

Activate that grain at a **different key per environment** and three things isolate themselves, with no
per-call filtering anywhere:

- **Name uniqueness**, which is `ValidateUniqueTableName` scanning the in-memory lists of *that* state
  (`RegistryGrain.cs:720-729`, `CatalogStore.cs:486-495`).
- **The SQL namespace**, which is `BuildStreamSchemas()` / `BuildTableSchemas()` over the same lists
  (`RegistryGrain.cs:994-999`, `CatalogStore.cs:635,638`) merged in `CompileTableSql`
  (`RegistryGrain.cs:931-936`).
- **Persistence**, because Orleans files are named per (stateName, grainId)
  (`orleans/src/StreamForge.Host/Storage/JsonFileGrainStorage.cs:34-39`) and Dapr keys are
  `{appId}||{ActorType}||{ActorId}||{stateName}` (`dapr/components/statestore.yaml:1-18`).

The alternative — one catalog with an `Environment` column filtered on every read — was rejected: it makes
every uniqueness check, every schema build and every list endpoint responsible for remembering the filter,
and the failure mode of forgetting one is a cross-environment leak that no type checks.

**Cost**: the number of live registry grains is now the number of environments, and anything that assumed
one catalog (seeding, config export of "everything", lifecycle broadcast) must state which one it means.
17 call sites take `GetGrain<IRegistryGrain>(StreamConstants.RegistryKey)`; they become one helper call.

### D2. The default environment is the empty name and produces byte-identical keys

`default` is spelled as the empty string internally and renders as `default` in the API and UI. Every key
composition is `env == "" ? key : $"{env}:{key}"` — so with no environment specified, **every grain key,
actor id, stream id, storage filename and Redis key is exactly the byte string it is today**.

This is not a convenience, it is the migration strategy: there is no migration. An existing `data/`
directory and an existing Redis state store come up unchanged, an existing config document imports
unchanged, and every URL a client holds keeps working.

**Acceptance criterion, stated here so a wave cannot quietly trade it away**: with the feature merged and no
environment ever mentioned, the on-disk state file names and Redis keys are identical to the pre-merge run,
and the full existing suite passes **unmodified**.

### D3. Entity keys are qualified through exactly one helper

Twelve grain kinds key on the entity **name**, not its id — generator, connector, table, table history,
table ingest, table stage, table output, arrangement, shard router, shard directory, shard, ingress stats
(`orleans/src/StreamForge.Abstractions/GrainInterfaces.cs:23,65,129,173,186,210,236-244,302`,
`ShardGrainInterfaces.cs:7,11,14`, `IngressStatsGrainInterfaces.cs:14`). Two environments with a table named
`orders` would address one grain and one state file. `IPipelineGrain` is the exception, keyed by GUID
(`GrainInterfaces.cs:14`).

One helper — `EnvKeys.Qualify(env, key)` in AppCore — is applied at the **50** `GetGrain<ITable*>` sites in
`orleans/src` and the **10** `ActorProxy.Create<ITable*Actor>` sites in `dapr/src`, plus the generator,
connector and ingress-stats sites. Two details that make it safe rather than clever:

- The separator must not collide with the shard tier's own composite key, which already uses `|`
  (`shared/StreamForge.AppCore/History/TableShardKeys.cs:57-80`). Use `:`.
- `JsonFileGrainStorage` sanitizes grain-id characters outside `[A-Za-z0-9_.-]` to `_`
  (`JsonFileGrainStorage.cs:31-32,37`), so an environment name must be constrained at creation
  (`[a-z0-9-]{1,32}`, reserved: `default`, `catalog`, `users`) or two environments could sanitize to the
  same filename. That constraint is D7's job.

**Cost**: 60+ call sites in one mechanical wave per flavour, and a permanent rule that a raw
`GetGrain<ITableGrain>(name)` is a bug. Enforced by a test that greps the source for unqualified
constructions — cheap, and the only thing that stops the rule rotting.

### D4. The selector rides a header, resolved once into an ambient, read only where keys are composed

There is **no per-request context object** in this codebase: endpoints take `ClaimsPrincipal` or
`HttpContext` as minimal-API parameters directly, and the facade interfaces are declared frozen in their own
doc comments because test fakes implement them (`shared/StreamForge.Contracts/Facades.cs:163,199`). Threading
an environment parameter explicitly means touching ~66 endpoint handlers and breaking eight frozen facade
interfaces and every fake behind them.

Instead: middleware reads `X-StreamForge-Environment` (query `?env=` overrides, for a browser and a curl
that cannot set headers), validates it against the environment registry, and stores it in an `AsyncLocal`
that **only the facade implementations** read — the exact places that compose a grain key or an actor id
(`orleans/src/StreamForge.Host/Facades/OrleansFacades.cs`, `dapr/src/StreamForge.Dapr.Host/Facades/DaprFacades.cs`).
Zero endpoint signatures change, zero contracts change, zero fakes change.

**Cost, stated plainly because ambient state earns its reputation**: an `AsyncLocal` is invisible at the call
site, so it must be set in exactly one middleware and nowhere else, and the wave owes three tests — no
header resolves to default; an unknown environment is a 404 *before* any facade call; and the ambient does
not leak across requests on a reused thread.

### D5. The runtime never reads the ambient — it reads the definition

Supervisors, the lifecycle orchestrator, connector drivers and stream bridges run on timers and
subscriptions, outside any request. They must not consult an ambient that is empty there.

So each definition carries its own environment: an additive `[Id(n)]` string on `SourceDefinition`,
`PipelineDefinition` and `TableDefinition`, written at creation from the request's environment, read by
everything that runs later. The ambient answers "which catalog is this request talking to"; the definition
answers "which environment does this entity belong to". Conflating them is how background work silently
operates on `default`.

### D6. Stream and topic identities carry the environment in the key, not in a new namespace

Orleans stream ids are `StreamId.Create(namespaceConst, entityName)` over four fixed namespaces
(`StreamConstants.cs:5-10`), e.g. `ConnectorGrain.cs:203-204`, `TableOutputGrain.cs:76`,
`TableHistoryGrain.cs:309`, `ArrangementGrain.cs:166`. The entity key becomes the D3-qualified key; the four
namespace constants do not change. The one that needs thought is the lifecycle stream, keyed by the fixed
`LifecycleEventsKey="events"` with no entity key at all (`RegistryGrain.cs:1076-1077`) — it becomes one
stream per environment, or a `staging` deploy wakes every `prod` subscriber.

Dapr keeps its five fixed app-wide topics (`StreamingRuntimeSetup.cs:23-29`), because dispatch reads the
entity name out of the envelope after receipt (`StreamingRuntimeSetup.cs:59-92`) — so the envelope's entity
key becomes the qualified key and routing follows for free. The per-source egress prefix
`sf-source-{name}` (`dapr/src/StreamForge.Dapr.Host/Ingest/DaprIngressFacade.cs:57`) takes the qualified
name too.

### D7. Environments are created deliberately; a typo is a 404

Implicit creation on first use would make `X-StreamForge-Environment: stagng` a successful deploy into a new
empty environment nobody meant to make. A small `IEnvironmentRegistryGrain` / `EnvironmentRegistryActor`
singleton holds the list with `Name`, `Description`, `CreatedAtMs`, `CreatedBy`. `default` always exists,
cannot be created, cannot be deleted, cannot be renamed. Names are validated at creation per D3.

Deletion refuses a non-empty environment unless `?force=true`, and force deletes catalog **and** the runtime
state of everything in it — the one genuinely destructive operation this plan adds, so it is `Admin`-only
and audit-logged the moment 015 gives it somewhere to log.

Renaming an environment is **refused outright**, for the same reason plan 011 D2 refuses renaming a sharded
table (`ShardGrainInterfaces.cs:17-21`): the name is in every key.

### D8. The config document stays environment-free; the import request targets an environment

`ConfigDocument` (`shared/StreamForge.AppCore/Config/ConfigDocument.cs:22-29`) gains **no** environment
field. Putting one in would make a document deployable to exactly one place, which is the opposite of the
requested use case — the whole point is to take the same document and deploy it into `staging`, then into
`prod`. The environment is a property of the *import call*, exactly like the endpoint aliases plan 016
keeps out of the document for the same reason (`plans/016-identity-versioning-discovery.md:144-148`).

`ImportPlanner` needs no change at all (`ImportPlanner.cs:26-55`): it plans against a catalog snapshot, and
the snapshot now comes from a different registry.

Export mirrors it: `GET /api/config/export` exports the environment the request selected. Exporting *all*
environments into one document is deliberately not offered — the result would not be importable anywhere.

### D9. This is a namespace, not a security boundary — until 015

An authenticated `Editor` can point a header at any environment and edit it. Nothing in the current auth
model can express otherwise: the JWT carries one `ClaimTypes.Role` and no scope
(`shared/StreamForge.Api/Auth/JwtTokenService.cs:22-31`), `UserRecord` has no per-resource field
(`shared/StreamForge.Contracts/Models.cs:951-960`), and the three policies are role strings
(`shared/StreamForge.Api/StreamForgeApiExtensions.cs:85-88`).

So the docs say, in the same words, that environments prevent *collision and confusion*, not *access*.
Plan 015's permission model gets the named hook: an entitlement scoped to an environment
(`Editor` in `staging`, `Viewer` in `prod`) is the first thing per-resource scoping should express, and it
is a strictly additive use of the resolver 015-A builds.

## Where this belongs in the queue

**Cheaper after 016-A than before it.** 016-A re-keys the name-keyed grains onto the immutable `Id`
(`RegistryGrain.cs:860-896` already argues for it), and a GUID is globally unique across environments — so
every grain kind that 016-A converts needs no environment qualification at all, and D3's 60-site wave
shrinks to the kinds that remain name-keyed plus the streams of D6. Landing 021 first means composing
`{env}:{name}` keys that 016-A then rewrites into `{id}`, and paying for two state migrations where one
would do.

Recommended order: **019 → 015 → 016 → 021 → 020**. If environments are wanted sooner than that, the plan
still works standalone — D3 simply costs its full 60 sites, and 016-A inherits the qualification helper
instead of deleting most of its call sites.

## Waves

| Wave | What | Model |
|---|---|---|
| 021-A | Environment registry (grain + actor + contracts), name validation, `default` invariants, `EnvKeys` helper — pure + unit-testable, no call sites yet | Sonnet 5 high |
| 021-B | Orleans: per-environment registry activation, the 50 qualified grain-key sites, stream ids and the lifecycle stream per D6 | Sonnet 5 high |
| 021-C | Dapr: per-environment `RegistryActor`, the 10 qualified actor ids, envelope keys and the egress prefix per D6 | Sonnet 5 high |
| 021-D | REST: the middleware, the ambient, `/api/environments` CRUD, unknown-environment 404, the three ambient tests of D4 | Sonnet 5 high |
| 021-E | Definition-carried environment per D5; config import/export targeting an environment; `sf` CLI + MCP `--env` | Sonnet 5 high |
| 021-F | SPA: environment picker beside auth, header injection in `web/src/api/client.ts:58-83`, and the five raw-`fetch` bypasses (`csv.ts:16`, `config.ts:56,103,121`, `explorerTypes.ts:50`, `ingest.ts:26`) | Sonnet 5 high |
| 021-G | Docs (`AGENTS.md`, `TRANSPORTS.md` where kinds are configured, `orleans/ARCHITECTURE.md`, `dapr/ARCHITECTURE.md`) + the live isolation check below | Sonnet 5 high |

B and C are disjoint by flavour and run concurrently after A. D depends on A. E depends on D. F depends on D.
G last.

## Verification

- **The no-migration gate (D2)**: capture the state file names of a seeded instance before the merge; after
  the merge, with no environment mentioned, they are byte-identical. Both flavours.
- **The full existing suite passes unmodified** — this plan declares no behaviour change for a catalog that
  never names an environment, so any test that needs editing is a bug in the wave, not in the test.
- **The live isolation check**, on one isolated instance (6xxx–9xxx ports, temp data dir, killed after):
  create environment `staging`; import the *same* config document into `default` and into `staging`; assert
  a table named `orders` exists in both with different ids; write rows into `staging`'s `orders` and assert
  `default`'s `orders` row count is unchanged; run `SELECT … FROM orders` in each and assert each reads its
  own; delete `staging` with `?force=true` and assert `default` is untouched and its grains still resolve.
- **The unknown-environment gate**: `X-StreamForge-Environment: nope` returns 404 on a read and on a write,
  and creates nothing.
- **Cross-flavour**: the same script run against Orleans (`:5199`-shaped isolated instance) and Dapr.

## Cut, explicitly

- **Per-environment quotas, priorities or isolation of compute.** One process; see "Is not".
- **Moving or copying an entity between environments.** Export from one, import into the other — that is
  what D8 buys, and a first-class "promote" needs 016's revisions to mean anything.
- **Renaming an environment** (D7).
- **Cross-environment queries.** `FROM staging:orders` inside a `prod` pipeline is precisely what this plan
  exists to prevent; a deliberate federation across environments is plan 016's endpoint aliases.
- **Per-environment auth** — deferred to 015 with the hook named in D9, not invented here.

---

## What actually landed (waves 0–2)

Recorded as the waves went in, following plan 016's convention.

| Wave | Commit | What it actually did |
|---|---|---|
| 0 | `1ea31cf` | The shared vocabulary, pre-built so none of the three concurrent tracks would own it: `EnvKeys` (qualify/split/validate), `EnvironmentAmbient` (D4's `AsyncLocal`), `EnvironmentRecord`/`IEnvironmentFacade`, and the additive `Environment` field on all three definitions at the next free `[Id(n)]`. **Overruled the plan's own D3 on the separator** — `.` instead of `:` — because `JsonFileGrainStorage` sanitizes every state-filename character outside `[A-Za-z0-9_.-]` to `_`, and `:` would have collided with an ordinary `_`-containing name at the filesystem. `SourceNode` drops `environment` from the canonical exported config node (D8: an export must not carry its origin environment into the document). |
| 1 | `32ea87e` | Three disjoint tracks: the Orleans environment registry (`IEnvironmentRegistryGrain`, `RegistryGrainKeys.RegistryFor`, one lifecycle stream per environment, the four boot-time sweeps iterating every environment), the Dapr equivalent (`EnvironmentRegistryActor`/`EnvironmentRegistryStore`, `ICatalogFacadeFactory` fixing eight singletons that had captured a default-bound facade at container-build time), and the REST middleware (`EnvironmentSelectionMiddleware`, placed after authn/authz so its 404 can't be used as an unauthenticated environment-enumeration oracle). **The find that predates this plan**: the Dapr host had been unbootable since plan 009 — `IConnectorActor.RecordSubscriberBatchAsync` grew two C# default-parameter values, and Dapr's actor-interface validation refuses any optional parameter outright, throwing at startup rather than degrading one call. Fixed by making both parameters required. Live-checked on isolated ports: unknown env 404s on read and write, create/400/409 behave, force-delete refuses without `?force` and leaves `default` intact. |
| 2 | `f3e38db` | Orleans entity grains, stream ids and SignalR groups qualified by environment (the 50+ name-keyed `GetGrain<...>` sites); Dapr's equivalent landed in wave 1 already. **The leak found chasing an unrelated loopback inconsistency**: a `loopback`/`duplex` sink names a catalog entity by its bare name, but both registries (`LoopbackHub`, `DuplexSessions`) are keyed by the environment-qualified runtime key wave 2 introduced — so a `staging` table's loopback sink to `feed` published into `default`'s `feed` generator and reported success. Fixed by `SinkEnvironmentScoping`, resolved at client construction and never written back. `GeneratorGrain` had the same bug in miniature (draining under `_def.Name`, attached under its own qualified key) — byte-identical in `default`, silently broken everywhere else. The SPA track separately found and fixed `DashboardPage` summing the whole cluster-wide metrics dictionary (an empty `staging` showed rows/sec borrowed from `default`), and `/api/environments` being itself environment-gated (a client pointed at a force-deleted environment 404'd on the one route that would tell it so). `StreamHub` was checking entitlements against the wrong catalog — a constructor-injected `ICatalogFacade` resolves its environment at DI-resolution time, and a hub is activated outside any request — fixed to resolve per call against the connection's own selected environment. Proven on disk, not merely asserted: two environments each with a source and a table named the same produced `connector.connector_envsrc.json` **and** `connector.connector_staging.envsrc.json`, and two identically-named tables given different SQL each read back their own `rows.csv` columns. |

## Found and deliberately not fixed

Each of these was seen, argued about, and left. Written down so the next person inherits the argument
rather than rediscovering the symptom.

- **The `metrics` SignalR group is deliberately still cluster-wide.** Qualifying it correctly means the
  group name, both flavours' bridge publishers, *and* the payload's own `PipelineId` all have to move
  together — the payload id must stay bare, since every client keys its own metrics state on it — and a
  half-qualified metrics stream is silent, not loud, if any one of those three is missed. Left unqualified
  on purpose: any authenticated caller may already address any environment through the REST API (D9), so
  this leaks strictly less than the API surface does by design. The console filters metrics client-side to
  its own environment's pipeline list; any other client sees every environment's live throughput on this
  one channel until this is done properly.
- **The Dapr host was unbootable since plan 009, and was fixed here, not there.** `IConnectorActor`'s two
  C# default-parameter values (added in `db01ab1`, an unrelated commit) made Dapr's actor-interface
  validation throw inside `MapActorsHandlers` at startup — the process died before serving a single
  request. Every plan between 009 and this one that claimed to have live-verified the Dapr flavor could
  not actually have booted it in that state; whatever verification those plans recorded either predates
  `db01ab1`, used a build without the offending default parameters, or is itself now suspect. Not
  re-litigated here — flagged so the next person auditing an older plan's "verified live on Dapr" claim
  knows to check which side of `db01ab1` it falls on.
- **`EnvironmentRegistryGrain.ListAsync` is not cheap — it counts every environment's own catalog** (asks
  each environment's `IRegistryGrain` for its sources/pipelines/tables and sums them), which is why
  `IngestDrainPumpService` — whose own sweep cadence is 100ms, the hot path for buffered ingress — caches
  the environment list on a 5-second TTL (`EnvironmentListTtl`) rather than calling `ListAsync` every tick.
  A newly created environment's ingest sources start draining within one TTL window, not necessarily the
  very next tick. The same reasoning applies to every other per-environment sweep in this codebase; nothing
  currently calls `ListAsync` on a hotter path than this one.
- **Force-delete leaves the deleted environment's now-empty registry state behind** — an Orleans grain
  storage JSON file (`catalog.registry_<env>.catalog.json`) or a Dapr Redis entry, holding an empty
  `Sources`/`Pipelines`/`Tables` but a non-empty `FieldNumberMaps` dictionary for whatever the deleted
  environment used to contain. Neither storage layer exposes "delete this grain/actor's own persisted
  file/key" as an operation this code can reach, only "the object living in it is now empty". Verified
  live: after force-deleting `staging`, `catalog.registry_staging.catalog.json` remained on disk with
  `"Sources":[],"Pipelines":[],"Tables":[]` and a surviving `FieldNumberMaps` entry for the deleted table.
  Harmless (field numbers are meant to persist forever regardless — hard rule 5), but a same-named
  environment created later inherits that dead weight rather than starting clean, and nothing today
  garbage-collects it.
- **`TableFrontierClusterTests` failed once under whole-solution load during this plan's verification and
  passed 3/3 alone** — the exact shape of every test already on `AGENTS.md`'s documented time-bounded-flake
  list. It is explicitly **not** added to that list here: one sighting under load is not the paragraph of
  justification that list requires for a new entry, and promoting it on a single data point would be
  exactly the failure mode that list's own preamble warns against. Recorded so a second sighting is
  recognized as a pattern rather than rediscovered from nothing.
