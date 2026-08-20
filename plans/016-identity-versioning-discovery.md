# Plan 016 — Name resolution, versioning &amp; dependencies, service discovery

**Status: PLANNED.**

## Why

Entities are addressed by GUID on most routes and by name on others; four sites hand-roll id-or-name
resolution differently. Nothing in the catalog records *which version* of a schema a query was authored
against. Federation requires a hardcoded address and a remote GUID. `ImportPlanner` extracts dependencies with
a `FROM`-only regex that misses JOINs and subqueries.

## Decisions, and what they cost

**The shared resolver is a pure AppCore helper, not a facade method.** `Facades.cs` states in two places that
existing facade members are frozen because test fakes implement them; a new facade interface would force two
runtimes to implement identical pure code — the duplication `SourceKindDispatch` exists to eliminate. The
logic is pure over lists every call site already fetches.

**Resolution rule, pinned verbatim in every agent brief:** exact ordinal `Id` wins outright; else exact
ordinal `Name` — 1 → Found, 0 → NotFound, ≥2 → **Ambiguous** carrying candidate ids; sources are name-only;
**no** case-insensitive, prefix or fuzzy matching, because `RegistryGrain` builds the SQL namespace with
ordinal dictionaries and a looser resolver would let `GET /api/tables/Trades` and `FROM Trades` disagree.
Ambiguity is **409** with the candidates named — 404 would be a lie and 400 blames the caller for the
catalog's state. gRPC analog: `FailedPrecondition`, not `NotFound`.

**Pipeline-name uniqueness is enforced at the write path, not migrated** — against pipelines only, not
sources+tables, because pipelines are not in the SQL namespace and a pipeline named `trades` reading
`FROM trades` is legal today. Existing violators keep working until someone edits one; they surface as
`catalogWarnings` on `/api/meta/instance`, not as a boot refusal.

**Live bug fixed in the same wave:** `ImportPlanner` does `pipelines.ToDictionary(p => p.Name, …)`, which
throws on a duplicate-name catalog — so `POST /api/config/import` **500s today** on a state the catalog
permits. `ConfigImportService.FirstByName` already documents and defends against exactly this; the planner
does not. First-wins plus a document-level diagnostic.

**Renaming: conditional for tables, impossible for sources — not flat re-keying, not flat prohibition.**

A table rename is *already* semantically broken beyond grain keys: `TableInputs` holds names, dependents' SQL
says `FROM oldname`, and nothing recompiles them. So allow rename **iff** `Status == Stopped` **and**
`ShardBy` is empty **and** no other table lists it in `TableInputs` — then explicitly reset the old-name-keyed
tier before persisting (the plumbing already exists: `ITableHistoryGrain.DisableAsync`/`ResetAsync`,
`ITableShardRouterGrain.ResetAsync`, plus the snapshot clear). That closes both holes while preserving the 95%
case: fixing a typo on a table you just created. ~40 lines per flavour, no contract change. Dapr's
`CatalogStore` has **no guard at all** today.

Re-keying the tier on `Id` — the fix `RegistryGrain.cs:860-896` itself proposes — stays the right eventual
answer, but its blocker is unchanged: `ShardedTableClusterTests` and `ShardedTableD2ClusterTests` address the
tier by name. **Deferred with the unblocking condition written down**: unblocked the moment someone is willing
to edit those two files.

**Sources do not get an id.** A source's name is simultaneously its REST route, grain/actor key, Orleans
stream key, SQL namespace entry, `EntitySchemas.SourceKey` field-number key, `PipelineDefinition.SourceNames`
and every federated peer's `EntityKey`. An id means dual truth or a migration of all of it — the largest blast
radius here for zero gain, because sources **cannot be renamed** (`SourcesEndpoints` force-overwrites the
name). Instead: a test pinning that invariant, so the thing that makes name-keying safe cannot be quietly
removed.

Net: sources unrenameable, tables conditionally renameable, names unique across sources+tables ⇒ **a name is a
stable key for exactly the entities whose runtime is name-keyed**, and pipelines — the only freely renameable
entity — are id-keyed everywhere. The asymmetry becomes principled rather than accidental.

**Two revision counters, both registry-assigned, never client-settable:** `Revision` on all three types, and
`SchemaRevision` on sources and tables bumped **only** when field shape changes. That split is what makes a
pin useful — an `eventsPerSecond` edit must not invalidate a downstream pin. "Definition changed" reuses the
predicate that already exists: `ConfigJsonMapper.ToCanonicalJsonText(...)` inequality, the exact test
`ImportPlanner` uses for "skipped" vs "updated", so a round-trip that reports "skipped" provably does not bump.

**Known interaction with 014-K:** a config document written as `INSERT INTO <sink> SELECT …` stores the
stripped `SELECT`, so its document text and its stored text legitimately differ and it plans as `updated` on
every import — which under this predicate bumps its `Revision` every time. Either desugar inside
`ImportPlanner` before comparing, or exclude sugared documents from the bump. Decide it in wave 2 rather than
discovering it as revision churn.

**Pinning lives in config documents only. Not in SQL.** `FROM trades@3` would touch tokenizer, parser, AST,
validator, planner, editor autocomplete, formatter and highlighter — the most expensive change in scope, in
the one project work serializes on — and it has no coherent runtime meaning, because the engine executes
against live streams with no versioned store to read revision 3 from. So `dependsOn: [{kind, name,
schemaRevision}]` on config entities, checked at exactly two moments: **import** (against the post-import
world `ConfigImportService` already builds, so `mode=validate` catches it before anything is applied) and
**start**. Not re-checked continuously: a pin violated by an upstream change sets `StaleReason` and badges it;
the table keeps running on its compiled plan, which is what it does today, only now visibly.

**Schema compatibility reuses `FieldNumberMap`, it does not duplicate it.** `Assign(newFields, existing)`
already grows `Reserved` iff a field was removed — the removal half, free, from the machinery that already
guarantees wire compatibility, so "compatible" means the same thing to the catalog gate and to the `.proto`
surface. The only gap is type changes. New pure `SchemaCompatibility.cs` next to it (do **not** extend
`FieldNumberMap` — its JSON shape is persisted), with a test asserting its `Removed` and `Assign`'s `Reserved`
growth agree.

Permissiveness split: **interactive editing stays permissive** (`PUT /api/sources/{name}` allows breaking by
default, `?allowBreaking=false` to opt in), **promotion is gated** (config import defaults to `compatible`
unless the document declares `schemaPolicy: any`). Costs zero pre-existing test modifications, because import
is a distinct code path.

**The highest-value line in that wave:** on a source schema change, recompile dependents (pure Engine work, no
grain calls, so no reentrancy hazard), keep them running either way, and **refresh persisted `OutputFields` +
`EnsureFieldNumbersAsync`** — so `/proto` and `/api/meta/grpc` stop serving a stale schema to generated
clients. Cascading auto-restart of dependents is explicitly **not** done: the restart-on-change machinery is
for *self* edits.

**Dependency extraction moves to the compiler.** New `SqlCompiler.ExtractReferences(sql)` (additive on
`PublicApi.cs`) walking the existing internal AST for every FROM/JOIN/subquery/set-operation relation,
returning `[]` on parse error. The AST is `internal`, which is why this must live in the Engine — and the
Engine is exclusive, so that agent owns `StreamForge.Engine/**` alone that wave. `ImportPlanner` uses it for
**new** entities; **existing** ones keep using persisted `TableInputs`/`StreamInputs`/`SourceNames`, which are
exact post-compile facts and strictly better than any scan.

**Cycles become fatal in `ConfigImportService`, not in `ImportPlanner`** — verified: `ImportPlannerTests`
asserts `Plan()` diagnoses-not-crashes and still returns actions, and that file may not be modified. So the
planner keeps its behaviour and `RunImportAsync` aborts before the apply loop, naming the full cycle. This
matches `ConfigComposer`, where include cycles are already fatal, at zero pre-existing-test risk.

**Missing-dependency detection changes no outcome** — the downstream compile already errors — it just surfaces
the real reason at plan time, which is what lets `mode=validate` say it.

**Discovery is two layers, and layer 1 is most of the value.** `GET /api/meta/instance` (anonymous, like
`/healthz`): `instanceId` (a GUID persisted at `{DataDir}/instance.json` so restarts keep identity), name,
flavor, version, endpoints, capabilities, plugins, catalog counts, catalog warnings. Layer 2 is
`IPeerDirectory` with **`StaticPeerDirectory` shipped first** (a configured list, each probed via
`/api/meta/instance`) — it already unblocks the `grpc` source and the admin app, which is the actual need. The
self-hosted heartbeat variant is in-memory, no persistence, no leader election, no consensus, and the docs say
plainly that it is **not an HA service registry**.

Not Consul/etcd/DNS-SD (a new infra dependency across two flavours, two compose stacks, two Cloud Run
manifests and the admin app, in a repo whose house rule is zero dependencies where avoidable). Not Redis (the
Orleans flavour has none, and adding one breaks the flavour parity enforced on every feature). Not Orleans
membership (`UseLocalhostClustering`, and a *peer* here is a different deployment, not another silo — wrong
scope even with a real clustering provider).

**Where it pays off:** `GrpcSubConfig.Peer` resolves at each (re)connect — exactly the cadence
`GrpcSubscriberCore` already uses for schema snapshots and login, so unresolvable → the existing status-error
path, retried at existing backoff, fixable without restart, zero new failure machinery. A peer record carries
`restEndpoint`, removing the mandatory-`RestAddress` friction; and because the consumer already does a REST GET
to translate id→name, it can run the same round trip in reverse — so `EntityKey` can be authored as
`table:daily_pnl` and canonicalized to `table:{id}`. **That is where name resolution and discovery meet, and
it is the most user-visible payoff in the plan: federation with no hardcoded address and no GUID.**

**Prerequisite fix:** `admin/sfclient.ts` keys `token.json` by URL as a single object, so it holds one instance
at a time. Multi-instance CLI is impossible without changing it to `{ [url]: StoredToken }`.

**Named external endpoints: `@name` as the whole value of an endpoint-shaped string.** A sigil inside an
existing field means **zero contract change**, and no URL can begin with `@`. The map lives in
**configuration, never the catalog** (`--Endpoints:primary-oltp=…`, `Endpoints__PRIMARY_OLTP`, optional
`{DataDir}/endpoints.json`) — per-environment by construction, which is the requirement; a document carrying
environment-specific endpoints defeats the indirection. Resolution happens at connect time and is **never
written back**, so an export still reads `@primary-oltp`. **That is the whole sales pitch: a catalog exported
from prod imports byte-identical into dev and connects to a different database.** Unresolvable at import is a
*warning*, not an error — importing a document destined for another environment must remain possible, because
that is promotion.

## Waves

| Wave | What | Model |
|---|---|---|
| **0** | Contract pre-build, orchestrator alone: every additive `[Id(n)]` later waves need, plus `Discovery.cs` and optional `types.ts` fields | — |
| **1** | 3 parallel: `EntityRef` + `EntityRefResults` (**Opus** — the rule every later wave depends on) ∥ read/write routes + gRPC ∥ rename policy + uniqueness + the `ToDictionary` fix | mixed |
| **2** | 2 parallel (registry file is the bottleneck): `SchemaCompatibility` + both registries (**Opus**) ∥ `?allowBreaking` + `staleReason` + SPA badges | mixed |
| **3** | 3 parallel: `ExtractReferences` (**Engine-exclusive**) ∥ `ImportPlanner` + config document ∥ `ConfigImportService` + CLI report | Sonnet 5 high |
| **4** | Plugin version declaration + `SemVerRange` (~80 lines, no NuGet semver dependency) + import check. Degrades to "built-in kinds are the catalog" if 014 has not landed | Sonnet 5 high |
| **5** | 3 parallel: instance identity + directory ∥ peers/meta endpoints ∥ `GrpcSubscriberCore` peer resolution + `admin/` multi-instance. Gate: **two live instances**, one federating a table from the other by peer name and entity NAME, no address anywhere | mixed |
| **6** | 2 parallel: `EndpointResolver` + the three connect sites ∥ `/api/meta/endpoints` + import warning + SPA | Sonnet 5 high |
| **7** | Docs | Sonnet 5 high |

**If only three waves run: 0 → 1 → 3.** Name resolution plus a real dry-run import report is the bulk of the
user-visible value at the lowest risk. Wave 5 is the discovery headline and stands alone. Wave 2 is the only
wave that can regress a live catalog and must never share a wave with anything else touching `RegistryGrain`.

## Cut, explicitly

`FROM trades@3` SQL pinning · giving sources an id · Consul/etcd/DNS-SD/Redis · plugin download/resolution ·
cascading auto-restart on upstream schema change · case-insensitive/fuzzy name matching · namespaced or
multi-tenant names · endpoint failover, health checking and weighted routing · push-updates to running
connectors (a change takes effect on next reconnect, the rule schema snapshots already follow).

---

## What actually landed (waves 0–4)

Recorded as the waves went in, because a plan document that only says what was *intended* is the
half nobody can act on later.

| Wave | Commit | What it actually did |
|---|---|---|
| 0 | `7c0a459` | Contract pre-build: the additive `[Id(n)]` fields, `Discovery.cs`, optional `types.ts` fields. Also uncovered and fixed a live `[Id(26)]` collision on `TableDefinition` (`8d1a4d6`) — Orleans codegen silently drops a duplicated id, so `UpdatedBy` was being written over `RetentionMaxRows`. A permanent `ContractFieldNumberTests` guard now fails the build instead. |
| 1 | `296b6d1` | `EntityRef` + the two transport adapters; one resolver replacing four hand-rolled sites; rename policy; pipeline-name uniqueness (which first shipped as a 500 and was fixed to 409 in the same wave). |
| 2-A | `80650d2` | `SchemaCompatibility` (reuses `FieldNumberMap`, does not extend it); `Revision`/`SchemaRevision` real in both flavours; **dependent tables' persisted `OutputFields` and field numbers refresh on an upstream schema change**, so `/proto` stops serving a schema the table no longer produces; `StaleReason` set and cleared. |
| 2-B | `312fe82` | `dependsOn` reaches the registry over REST (it reached nothing before, so pinning was dead on the API surface); `?allowBreaking` on the source PUT; source create/update stops echoing `revision: 0`; SPA badges. |
| 3-A | `2eafe61` | `SqlCompiler.ExtractReferences` — Engine-exclusive, additive on `PublicApi.cs`, 947 → 994 tests. |
| 3-B | `e778d39` | Planner uses the compiler for NEW entities and persisted inputs for existing ones; missing-dependency diagnostics; `dependsOn` through export/import mapping. |
| 3-C | `113d444` | Fatal cycles naming the full chain; the `schemaPolicy` gate; the `dependsOn` mapping the import service was missing; and `ConfigComposer.MergeDocs` carrying document-level scalars at all. |
| — | `7b83410` | Follow-up: the FROM-regex fallback deleted and the unparseable `"TABLE AS SELECT …"` fixtures fixed. A declared behaviour change — those expectations were not testing the behaviour they claimed to. |

## Found and deliberately not fixed

Each of these was seen, argued about, and left. They are written down so the next person inherits the
argument rather than rediscovering the symptom.

- **`PUT`/`DELETE`/`start`/`stop` resolve by exact id, not id-or-name.** A name-based `PUT` 404s where a
  name-based `GET` works. Wave 1 scoped id-or-name to READ routes on purpose; extending it to writes
  means deciding what an ambiguous name does on a mutation, which deserves its own decision.
- **A refused import returns HTTP 200 with `ok:false`.** Consistent with this file's per-entity
  convention, inconsistent with the document-error path, which serves the very same report shape via
  `BadRequest`. So `curl -f` reads a refused import as applied. Changing it is an API-convention
  decision, not a bug fix; the CLI already honours `ok`.
- **`ConfigTable`/`ConfigPipeline` do not carry `Persistence`/`FlushMs`/`JournalMaxEntries`**, so an edit
  to only those does not move `Revision`. Follows directly from reusing the config-canonical predicate;
  widening the projection is an export-contract change.
- **`RefreshTableSchemas` sweeps every table, not the changed closure.** Cheap at catalog scale and only
  runs when a schema actually moved, but it is a write to entities the caller did not name.
- **A `Json` parent that loses its children leaks field numbers.** `FieldNumberMap.Assign` never walks a
  vanished scope, so it reserves nothing for the child paths and re-adding the parent can reuse a retired
  number. Pre-existing, and the compatibility gate is strict about it even though the wire is not.
- **The missing-dependency diagnostic is masked for freshly-broken entities** — `ErrorEntry` replaces
  diagnostics with compile output, so the diagnostic's real value is quietly-stale entities, not
  obviously-broken ones.
- **The missing-dependency check is kind-blind**: a pipeline naming a table is not flagged even though it
  can never resolve. A false negative, mirroring the regex it replaced.
- **Exports carry `revision`/`schemaRevision` on sources** — harmless (comparison strips them) but visible
  to a document reader.

## Environment note, corrected

An agent reported that the documented `--Http:Port` isolated-instance recipe "reliably crashes Kestrel
after the first connection" on this machine and switched to `--urls`. **Not reproducible** — 12/12
sequential requests succeeded against an instance started exactly the documented way, and every other
live check in this plan used it without incident. The documented recipe stands.
