# Plan 021 — Environment isolation: non-overlapping catalogs inside one server

**Status: PLANNED.**

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
