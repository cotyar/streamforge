# 006 — Ingestion Connectors + Config Import/Export

Status: **DONE** (W0–W7 all landed). Results summary: four real connector kinds (`url`/`file`/
`folder`/`grpc`-subscribe) and shared config import/export are live on both flavors, driven by a
pure, runtime-free connector core (`shared/StreamForge.AppCore/Connectors/**`) with thin Orleans
(`ConnectorGrain`) and Dapr (`ConnectorActor`) drivers. W6 end-to-end evidence: URL polling with
exact dedup (**50 items emitted, 50 served** across repeated polls of the same payload); an
OpenAPI-derived schema exercised end to end through the pass-through mapping path; failure backoff
observed doubling **30s → 60s** on repeated failures, then recovering on the next success; folder
polling showing at-least-once re-emission semantics as documented; the file/folder name+mtime
ledger surviving a process restart intact (**9 known files → 9 known files**, Orleans
`[PersistentState]`). **Headline — cross-runtime federation**: the Dapr flavor instance (`:5399`)
subscribed the Orleans flavor's `positions` table **by id** over gRPC, successfully via both the
reflection and proto-text schema paths, with real rows flowing cross-runtime
(`eventsEmittedTotal` climbed **418 → 498** in 10 s of live traffic, `SELECT * FROM fed` showing
real position rows in a Dapr-flavor table). One genuine defect was found and fixed live during W6:
`GrpcSubscriberCore` built the v1alpha `FileContainingSymbol` request from a `table:{id}`/
`pipeline:{id}` entity key's opaque id PascalCased — correct only for `source:{name}` keys, always
wrong for id-keyed ones, since `DynamicDescriptorSet` registers messages under entity display
**names**, not ids. Fix: `ResolveMessageIdentAsync` resolves id → name via a REST GET against the
remote's `RestAddress` before asking reflection for the symbol (commit `6182d90`). Config
import/export: a cross-flavor merge (Orleans export → Dapr import) landed **18 entities skipped
(already identical), 4 created**, and a reverse `validate` pass confirmed **zero mutation** — the
byte-equal `export → reset → import → re-export` round trip (D-I) held throughout. Final suites:
**orleans 877 / dapr 181**, `bun run build` green — roughly **350 new tests** added across W1–W6
against the pre-006 baseline (511 / ~153). The parity matrix below did not materially diverge from
plan: Dapr gRPC *serving* stays a permanent descope (plan 005 D-F), everything else landed on both
flavors.

**Hard gate (every commit):** `~/.dotnet/dotnet test orleans/StreamForge.sln` (511 pre-existing tests)
AND `~/.dotnet/dotnet test dapr/StreamForge.Dapr.sln` (~153) — all green with pre-existing test `.cs`
files **unmodified** (`git diff --stat orleans/tests dapr/tests -- '*.cs'` shows only *new* files).
`cd web && bun run build` green whenever `web/` is touched.

## Problem

StreamForge sources are synthetic generators only. Real deployments ingest from the outside world:
HTTP APIs polled on a schedule, files and folders dropped by upstream systems, and *other
StreamForge instances* (federation). Separately, the catalog (sources/tables/pipelines) lives only
inside a running instance — there is no way to version it, review it, promote it between
environments, or move it between the two runtime flavors. Plan 006 adds four real connector kinds
and a composable import/export config format, with the connector core shared (pure, unit-testable,
runtime-free) and each flavor contributing only a thin driver.

## Current state (verified by reading; load-bearing facts)

- `SourceDefinition` (`shared/StreamForge.Contracts/Models.cs:47`) — Ids 0–7 used, **next free
  `[Id]` = 8**. No kind concept; `GeneratorProfile` string + `EventsPerSecond` drive
  `MarketDataProfiles.GenerateEvent`. Sources are upsert-only (`ICatalogFacade.UpsertSourceAsync`);
  REST binds the raw `SourceDefinition` (no request DTOs); **PUT replaces the whole object**.
- `FieldDef` (Ids 0–3, next 4) with `IsArray`/`Children`; Contracts enum is `FieldType` (the
  Engine's `FieldKind` is a parallel enum — map at the boundary).
- Serialization law for anything crossing grain/actor transport: lives in Contracts,
  `[GenerateSerializer]` + `[Id(n)]`, **`set` accessors only** (ORLEANS0101 forbids `init` under
  cross-assembly codegen); codegen is automatic via `GenerateCodeForDeclaringAssembly`.
- **`FakeRegistryGrain.cs` (test fixture) implements `IRegistryGrain : ICatalogFacade`** ⇒ adding
  members to `ICatalogFacade`/`IRegistryGrain` would force a test-file edit. Therefore all new
  runtime surface goes on **new** interfaces/grains/actors.
- Source start/stop dispatch: Orleans `RegistryGrain.UpsertSourceAsync` →
  `IGeneratorGrain.StartAsync/StopAsync`; Dapr `CatalogStore` →
  `ILifecycleOrchestrator.NotifySourceChangedAsync` → `GeneratorActor`. Emission contract
  downstream of a source is already runtime-neutral: Orleans publishes `EventRecord` onto the
  memory stream `(sources, {name})` (surrogate exists); Dapr publishes `SourceEventsEnvelope` to
  `sf-sources` + egress `sf-source-{name}`. **A connector that emits through the same doors gets
  pipelines, tables, SignalR, and the SPA for free.**
- Wire format: `ProtoWireEncoder` (AppCore/Protocol) encodes rows against `FieldDef` +
  `FieldNumberMap` (dot-path scoped numbering; Reserved never reused). Envelopes:
  `{Entity}Event` = row 1 / seq 2 (int64) / ts_ms 3 (int64); `{Entity}Delta` = row 1 / weight 2
  (**sint64 zigzag**) / seq 3. Repeated = unpacked; `Json`+Children = nested message; `Json`
  schemaless = `google.protobuf.Struct`; null/missing omitted. Hand-decode patterns pinned in
  `WireRoundTripTests`/`RepeatedFieldWireRoundTripTests`. **There is no decoder** — encoder only.
- gRPC: `streamforge.dynamic.v1.DynamicStreamService/SubscribeEntity(EntitySubscribeRequest{entity_key})
  → stream DynamicFrame{entity_key, payload, seq}`, `[Authorize(Policy="Viewer")]` (JWT in
  `Authorization: Bearer` metadata; obtainable via `POST /api/auth/login`, 12 h expiry). Served by
  the **Orleans flavor only** (:5299 + hand-rolled v1alpha reflection); Dapr gRPC serving is a
  plan-005 descope (:5499 reserved). `/api/{kind}/{key}/proto` (both flavors) returns a
  StreamForge-generated self-contained proto with the *persisted* field numbers.
- Source liveness metrics: none via REST today (only pipeline/table metrics endpoints); SPA
  convention for non-pushed data is 2 s polling (`useTableMetrics`).
- Known live bug (plan 005): Orleans SignalR `tableDelta`/`pipelineResult` relay delivered 0
  events. Root cause found: `StreamBridgeService` enumerated Running pipelines/tables once at
  startup, racing `Program.cs`'s fire-and-forget seeding — a lost race meant those subscriptions
  never happened (sources self-healed via a 30 s loop; pipelines/tables never did). **A fix
  (`await registry.EnsureInitializedAsync()` in the bridge) + regression test
  (`StreamBridgeServiceStartupRaceTests.cs`) already sit uncommitted in the working tree** — W0
  verifies and lands them; this also unblocks SignalR-based live verification for everything below.
- Existing deps: no cron, no YAML, no JSONPath, no gRPC *client* packages anywhere.

## Decisions

**D-A — Connector core lives in `shared/StreamForge.AppCore/Connectors/` + `Config/`; no new
project.** AppCore is already the shared, runtime-free home (Protocol/Generators/Json/Search) that
both flavors link and both test suites cover. "No runtime deps" = no Orleans/Dapr/ASP.NET; pure
NuGet libs are fine. New packages (AppCore): **Cronos** (cron correctness beats a hand parser —
DST/day-of-week/step edge cases; 5- and 6-field), **YamlDotNet** (YAML is a genuine format
requirement; used only at the document boundary, YAML → one canonical JSON model),
**Grpc.Net.Client + Grpc.Reflection** (subscriber client + v1alpha reflection client;
Google.Protobuf already referenced). Everything else is hand-rolled and small: a **JSONPath-lite**
subset (`$`, `.name`, `['name']`, `[n]`, `[*]` — documented, closed), an **RFC-4180 CSV** parser,
an **OpenAPI v3 walker** with internal-`$ref` resolution (no Microsoft.OpenApi dependency).

**D-B — `SourceDefinition` evolves additively: `[Id(8)] Kind` (string, default `"generator"`) +
`[Id(9)] ConnectorConfig? Connector`.** Kinds: `generator | url | file | folder | grpc`. Seeds and
every existing source deserialize with `Kind == "generator"`, `Connector == null` — untouched
behavior. `ConnectorConfig` is one container with optional per-kind sub-configs
(`Schedule`, `Url`, `File`, `Folder`, `Grpc`, `Mapping`) — all new `[GenerateSerializer]` types in
Contracts (Ids from 0, `set` accessors). The mapping model is **structured**
(`MappingSpec { ItemsPath, DedupKeyField?, TimestampField?, Fields: List<FieldMapEntry
{ SourcePath, Field: FieldDef }> }`) and the "user-supplied response-structure file" (JSON or YAML)
deserializes into it — one model, two encodings, one UI editor.

**D-C — New runtime surface = new interfaces only (frozen-fake constraint).** A new
`IConnectorStatusFacade { GetStatusAsync(sourceName) → ConnectorRuntimeStatus? }` in Contracts
(null for generator-kind); Orleans adds `IConnectorGrain` to `GrainInterfaces.cs` (additive) +
`ConnectorGrain` in Host; Dapr adds `IConnectorActor`/`ConnectorActor`. `RegistryGrain`/
`DaprLifecycleOrchestrator` dispatch on `Kind` (generator → existing path, else connector).
`ConnectorRuntimeStatus` carries `NextRunMs/LastRunMs/LastStatus/LastError/ConsecutiveFailures/
EventsEmittedTotal/LastBatchCount`. Surfaced via `GET /api/sources/{name}/status`; SPA polls it
(2 s, `useTableMetrics` pattern).

**D-D — Drivers are thin; the poll cycle is a pure step function.** Shared core exposes
`ConnectorCycle.Execute(config, ConnectorState, fetched-content) → CycleResult{ events[],
newState, status }` — parsing, mapping, dedup, ledger, schema handling all pure and unit-tested;
I/O (HTTP fetch, file read, gRPC channel) sits behind tiny injectable fetch delegates. The Orleans
grain persists `ConnectorState` via `[PersistentState]`; the Dapr actor via actor state (Redis) —
"ledger persists via each flavor's storage" falls out for free. Emission: Orleans →
`(sources, {name})` stream; Dapr → `SourceEventsEnvelope` on `sf-sources` + `sf-source-{name}`.
Dedup key set is **bounded** (last 10 000 keys FIFO per source — documented ceiling; re-emit
possible beyond it). Ledger = filename+mtime map, same bound.

**D-E — Scheduling: Cronos cron (5/6-field, UTC) or fixed interval (`"every 30s"` / `intervalMs`),
min 1 s floor. Failure backoff:** after k consecutive failures the next attempt is delayed
`min(30s × 2^(k−1), 15 min)`; cron sources skip to the first occurrence ≥ that delay. No
hot-looping a dead endpoint; success resets k. All documented in docs + status surface.

**D-F — OpenAPI derivation, honest ceiling.** OpenAPI v3 (JSON or YAML), doc by URL or inline;
select operation by `operationId` (+ optional response status, default 200, first `application/json`
media type) or an explicit JSON-pointer to a schema. Type map: string→String
(`date-time` format→Timestamp), number→Double, integer→Long, boolean→Bool, object with
properties→Json+Children, array→IsArray (of item mapping), object without properties→schemaless
Json. `allOf` → best-effort shallow property merge (diagnostic emitted); `oneOf`/`anyOf`/`not` →
schemaless Json + diagnostic; **external `$ref` → error** (internal `#/…` refs resolved, cycle-safe).
Derivation returns `(fields, diagnostics[])` — the UI shows both and the user confirms.

**D-G — gRPC subscription source = the federation story.** Config: target address, JWT credentials
(username/password re-login on expiry/UNAUTHENTICATED, or a static token — secrets-lite), entity
key, schema source. Schema paths, both landing on `(List<FieldDef>, FieldNumberMap)`:
(a) **reflection** — v1alpha client walk of `FileDescriptorProto` (numbers included; robust);
(b) **proto text** — from `/api/{kind}/{key}/proto` (URL or pasted), parsed by a parser scoped to
**StreamForge-generated files only** (header-checked; arbitrary protos rejected with a clear
error). Decoding needs the **new `ProtoWireDecoder`** (AppCore/Protocol): exact counterpart of the
encoder (scalar kinds, nested Children, unpacked repeated, Struct, event/delta envelopes, unknown
fields skipped by wire type) with full encode→decode identity round-trip tests. Semantics
documented: schema is a **snapshot at (re)subscribe**; reconnect with the D-E backoff re-fetches
schema; delta frames with weight ≤ 0 are dropped (a *source* has insert-only semantics —
retraction-aware federation is a non-goal). Since Dapr serves no gRPC, federation is
**Dapr-subscribes-Orleans** (or Orleans↔Orleans); being a federation *server* stays Orleans-only
(parity matrix).

**D-H — Secrets-lite, stated ceiling.** Secret fields (URL header values, gRPC password/token) are
stored **plaintext at rest** (Orleans JSON file / Dapr Redis — same ceiling as JWT signing config
today) and **masked as `***` in every read path** (GET/list/export). Writes merge: an incoming
`***` value means "keep the stored value" (protects the SPA's GET→edit→PUT-whole-object cycle).
Export omits real values unless Admin calls `?includeSecrets=true`. No KMS, no encryption — 
documented honestly in docs.

**D-I — Config document: one canonical JSON model, YAML accepted; entity-level name-keyed merge
with field-level override.** Document: `{ version: 1, include: [relative paths], sources: [],
pipelines: [], tables: [] }` — **definitions only** (no ids, no runtime state, no timestamps/
createdBy, no users/credentials). Pipelines/tables carry `running: true|false` as *desired* state
(Failed exports as `running: true` — it was asked to run). Composition: resolve `include` list
depth-first in order, then the including document — later documents win **per entity (name-keyed)
with shallow field merge** (a present field overrides; lists/maps replace whole — never element-
merged; explicit `null` clears an optional field). The composer is pure
(`Compose(root, resolver)`); include cycles and missing includes → precise errors. The import
endpoint equally accepts an ordered multi-document array (same merge, no filesystem), and a
multipart file-set upload where includes resolve **within the uploaded set by relative name**
(the server never reads its own filesystem for includes — no path traversal by construction).
Canonical export: JSON, camelCase, entities sorted by name, fixed property order, defaults/empties
omitted — so export→reset→import→re-export is **byte-equal**.

**D-J — Import modes + rules.** `validate` (report only), `merge` (default; upsert by name),
`replace` (Admin; entities absent from the doc are deleted — running ones stopped first:
pipelines → tables reverse-topo → sources; documented stop-then-replace). Apply order: sources →
tables (topo by `TableInputs`, reusing the seeding order logic) → pipelines. Every imported
SQL is compiled through the real Engine compiler (same path as the validate endpoints) with the
composed catalog in scope; failures mark that entity `error` in the report without aborting the
rest (`validate` mode reports everything; apply modes skip failed entities). Report:
`{ mode, entries: [{kind, name, action: created|updated|deleted|skipped|error, diagnostics[]}] }`.
Auth: export Viewer, import Editor, replace Admin. One implementation in `shared/StreamForge.Api`
through `ICatalogFacade` only ⇒ both flavors, and cross-flavor moves, for free.

## Phases

Ownership is exclusive per concurrent agent. All csproj/sln edits, `StreamForgeApiExtensions.cs`,
and `web/src/api/types.ts` belong to the **orchestrator** (done between waves). Implementation
agents: **Sonnet, maximum reasoning effort**, live verification on isolated ports (6xxx–9xxx, temp
data dirs, instances killed after). Commits `006-Wn: …`; push after each stable wave.

### W0 — Land the SignalR relay fix (orchestrator, serial)
The fix (`StreamBridgeService` awaits `EnsureInitializedAsync`), its regression test
(`StreamBridgeServiceStartupRaceTests.cs`), a re-run bench (Orleans `tableDelta` now measurable),
and the doc updates (`dapr/ARCHITECTURE.md`, `orleans/docs/comparison.html`, plan 005 status) were
produced by a separate concurrent verification session working in this same checkout. W0 =
**do not duplicate or clobber it**: wait for that work to be committed, then independently verify —
full orleans suite green (with the new test), live `tableDelta`/`pipelineResult` delivery on an
isolated port — before building waves on top. If that session stalls without committing, this
orchestrator reviews and lands its working-tree changes as W0 instead.
**Acceptance:** fix + regression test in `master`; 511(+1) green; live SignalR delivery observed;
docs no longer claim an open bug. **Done** (commit `5f3d1e5`).

### W1 — Contracts + deps pinned (orchestrator, serial)
AppCore csproj: add Cronos, YamlDotNet, Grpc.Net.Client, Grpc.Reflection. Contracts additions
(exact shapes above): `SourceDefinition` Ids 8–9, `ConnectorConfig` family (`ScheduleSpec`,
`UrlPollConfig`, `FilePollConfig`, `FolderPollConfig`, `GrpcSubConfig`, `MappingSpec`,
`FieldMapEntry`), `ConnectorRuntimeStatus`, `IConnectorStatusFacade`, API DTOs
(`MappingValidateRequest/Result`, `SchemaDeriveRequest/Result`, `RemoteSchemaRequest/Result`,
`ConfigImportReport` + entry). `web/src/api/types.ts` mirrored additively (same shapes, doc
comments per house style). **Acceptance:** both suites green (types dormant), bun build green.
**Done** (commit `f4869ae`).

### W2 — Pure connector core (5 parallel agents, disjoint AppCore subfolders + own new test files)
- **2A Scheduling + poll policy** — `Connectors/Scheduling/` (ScheduleSpec parse: cron via Cronos
  5/6-field UTC + `"every Ns/m/h"` interval; next-occurrence; D-E backoff calculator) +
  `Connectors/Polling/` (`ConnectorState` — dedup ring, file ledger, failure count; change
  detection for file content (hash+mtime) and folder listings (name+mtime, glob)). Tests: cron
  edge cases, interval floor, backoff curve, dedup bound eviction, ledger semantics.
- **2B Mapping + formats** — `Connectors/Mapping/` (JsonPathLite; MappingSpec from JSON/YAML doc;
  `RecordExtractor`: response → items via ItemsPath → `Dictionary<string,object?>` rows via
  per-field SourcePath + `JsonValueNormalizer`, `_ts` from TimestampField else arrival) +
  `Connectors/Formats/` (NDJSON, JSON-array, RFC-4180 CSV-with-header; `SchemaInference` — rules:
  sample ≤ 100 items; long if all integral, double if numeric, bool, ISO-8601/epoch-ms → Timestamp,
  object → Json+Children, array → IsArray, mixed → String; documented). Tests: path subset, mapping
  round-trips, CSV quoting/newlines, inference rules.
- **2C OpenAPI** — `Connectors/OpenApi/` per D-F (loader JSON/YAML, internal-$ref resolver with
  cycle guard, operation/pointer selection, type mapping, allOf merge, diagnostics). Tests: petstore
  -style fixture + every unsupported-construct diagnostic.
- **2D Wire decode + remote schema + subscribe core** — `Protocol/ProtoWireDecoder.cs`
  (decode row/Event/Delta against FieldDef + FieldNumberMap; skip unknown fields by wire type) with
  **encode→decode identity tests for every kind incl. nested/repeated/Struct/envelopes** (new test
  files beside the existing wire tests); `Connectors/Grpc/` (`ProtoTextSchemaParser` — StreamForge-
  generated files only, header-checked, extracts fields+numbers+reserved; `ReflectionSchemaWalker` —
  FileDescriptorProto → FieldDef+numbers; `GrpcSubscriberCore` — channel, login/re-login, subscribe,
  frame→rows via decoder, reconnect hooks; I/O behind delegates so logic is testable pure).
- **2E Config engine** — `Config/` (document model; YAML↔canonical-JSON bridge; `Composer` — include
  resolution via callback, D-I merge rules, cycle/missing errors; `ImportPlanner` — pure diff of
  composed doc vs current catalog → planned actions per entity; canonical writer for byte-equal
  export). Tests: merge-rule matrix, include chains + cycle rejection, plan diffs, canonical
  stability.
**Acceptance per agent:** both suites green; new tests green; no csproj edits; no file outside the
assigned subfolders. **Done** (commit `e21bb79` — all 5 sub-agents landed).

### W3 — Runtime drivers ∥ config endpoints (3 parallel agents)
- **3A Orleans** (owns `orleans/src/**` touched files) — `IConnectorGrain` (additive in
  `GrainInterfaces.cs`), `ConnectorGrain` (schedule timer → fetch (HTTP/file/folder/gRPC via 2A–2D
  cores) → emit `EventRecord`s onto `(sources, {name})`; `[PersistentState]` ConnectorState;
  status), `RegistryGrain` Kind dispatch (start/stop/delete/boot-resume), supervisor ping coverage,
  `OrleansConnectorStatusFacade`, Program.cs DI. New TestCluster tests (file-poll connector against
  a temp dir — deterministic).
- **3B Dapr** (owns `dapr/src/**` touched files) — `IConnectorActor`/`ConnectorActor` (same cores;
  timer re-armed in `OnActivateAsync`; actor-state ConnectorState; emits envelopes to `sf-sources`
  + egress), orchestrator/supervisor dispatch, `DaprConnectorStatusFacade`, Program.cs. New unit
  tests for the pure orchestration/dispatch parts (house style: no sidecar in tests).
- **3C Config endpoints** (owns new `shared/StreamForge.Api/Endpoints/ConfigEndpoints.cs`; mount
  line pre-added by orchestrator) — `GET /api/config/export?format=json|yaml[&includeSecrets]`,
  `POST /api/config/import?mode=validate|merge|replace` accepting single doc / ordered array /
  multipart set; D-I/D-J semantics via `ICatalogFacade` + Engine compile; secrets masked per D-H.
**Acceptance:** both suites green. 3A/3B: live smoke — isolated instance, create a `url` source
against a scratch local HTTP endpoint, rows visible via REST. 3C: live export→import round-trip on
an isolated Orleans instance. **Done** (commits `6cb6507` pre-stub + `b4d3e59`, all 3 sub-agents).

### W4 — Sources API surface (1 agent, serial; owns SourcesEndpoints.cs + new source-schema endpoint file)
Masking + `***`-merge on sources GET/list/POST/PUT (D-H); kind-aware validation on create/update
(schedule parses, config-for-kind present, mapping/schema coherent); `GET /api/sources/{name}/status`
(IConnectorStatusFacade); helper endpoints for the UI: `POST /api/sources/schema/mapping-validate`,
`POST /api/sources/schema/derive-openapi`, `POST /api/sources/schema/from-remote` (gRPC target →
fields, Editor-only since it dials out). **Acceptance:** suites green; live curl checks of every
endpoint on isolated instances of **both** flavors. **Done** (commit `24c15d1`).

### W5 — SPA (2 parallel agents)
- **5A Sources page** (owns `SourcesPage.tsx` + new `components/sources/*`, `api/sources.ts`) —
  kind selector; per-kind config panels (URL+headers+schedule editor with cron/interval toggle,
  mapping editor with **Validate** button, OpenAPI derive flow, file/folder config, gRPC target +
  credentials + **Fetch schema from remote** button); list gains kind badges +
  schedule/next-run/last-status (2 s status poll hook); existing generator editing untouched.
- **5B Import/Export** (owns new page/dialog files + `App.tsx`/`Layout.tsx` nav + `api/config.ts`) —
  export download (JSON/YAML choice, `downloadText` pattern), import via file upload (multi-file
  for include sets) or paste, mode selection with replace-mode confirm, dry-run report table
  (created/updated/deleted/skipped/error + per-entity diagnostics). Editor/Viewer gating via
  `RoleGate`; tokens only; both themes.
**Acceptance:** `bun run build` green; live click-through against both flavors (create a URL source
end-to-end from the UI; export/import round trip from the UI). **Done** (commit `1504343`).

### W6 — End-to-end verification (orchestrator + 1 agent, serial)
Scripted, all on isolated ports, everything killed after:
1. **URL polling** — scratch bun HTTP server (scratchpad) serving a JSON payload with nested items
   + an OpenAPI doc; mapping path AND OpenAPI path; dedup verified across polls; failure backoff
   observed when the server is killed.
2. **File + folder** — temp dirs; NDJSON/JSON-array/CSV; append/replace file; new-file detection;
   ledger survives an instance restart (persisted state).
3. **HEADLINE — cross-runtime federation:** Dapr flavor instance subscribes the Orleans flavor's
   `positions` table over gRPC (`table:{id}` entity key, reflection schema) — rows flow Dapr←Orleans
   and appear in the Dapr SPA. Fallback if the sidecar is unavailable: two isolated Orleans
   instances (documented in results).
4. **Config** — export seeded Orleans → reset → import → **byte-equal** re-export; Orleans export →
   Dapr import → seeds run; include-composition (base + overlay) via multipart; replace-mode with a
   running entity (stop-then-replace observed); cycle rejection.
Both full suites; fixes as needed land here.
**Acceptance:** every connector kind exercised live; the four numbered checks pass and are recorded
in the plan's results summary. **Done** (commit `6182d90` — includes the live-found
`ResolveMessageIdentAsync` federation-by-id fix; see the Status line above for the full evidence:
URL dedup 50/50, backoff 30s→60s, folder ledger restart-survival 9→9, headline federation
`eventsEmittedTotal` 418→498 in 10s over both reflection AND proto-text schema paths, config merge
18 skipped/4 created + reverse validate no-mutation, suites orleans 877 / dapr 181 / bun build
green).

### W7 — Docs + skill + close (1 agent + orchestrator)
`orleans/docs/index.html`: Connectors section (kinds, schedule/backoff, mapping doc format,
OpenAPI ceiling, secrets ceiling, federation) + Config section (document format, **precise merge/
compose rules**, modes, round-trip guarantee); both `ARCHITECTURE.md`s; parity matrix finalized
here; `plans/README.md` row; new root skill **`/sf-config`** (export/import workflow: curl
examples, compose patterns, mode semantics). Final full sweep both flavors + `bun run build`.
Plan status → DONE with results summary. **Done** — this wave: two new sidebar-linked sections in
`orleans/docs/index.html` (`#connectors`, `#config`); an "Ingestion connectors (plan 006)" section
in both `orleans/ARCHITECTURE.md` and `dapr/ARCHITECTURE.md`; the `/sf-config` skill; this plan's
Status line and phase list; `plans/README.md`'s 006 row; `AGENTS.md`'s skills line. The parity
matrix below did not need to change — reality matched the target throughout (Dapr gRPC serving was
always the one deliberate, permanent descope).

## Parity matrix (achieved — matches the original target; verified W6)

| Capability | Orleans | Dapr |
|---|---|---|
| URL polling source (mapping + OpenAPI paths) | full | full (same core) |
| Cron/interval scheduling + backoff + status surface | full | full |
| File / folder polling (ledger persisted) | full (JSON file state) | full (Redis actor state) |
| gRPC subscription source (subscribe a remote StreamForge) | full | full |
| Being a federation *server* (gRPC serving) | full (:5299) | **descoped** (no gRPC serving — plan 005 D-F stands) |
| Config export/import + compose (shared endpoints) | full | full |
| Cross-flavor config portability | export ⇄ import both directions | same |
| Secrets-lite masking | masked reads/exports; plaintext at rest | same ceiling |
| Connector UI (kind editors, status, import/export) | same SPA | same SPA |

## Non-goals / honest descopes

Exactly-once ingestion (at-least-once with bounded dedup; documented); partial-file tailing;
HTTP pagination-following / non-GET polling / OAuth flows (static headers only); retraction-aware
federation (negative-weight deltas dropped at a source); parsing arbitrary third-party `.proto`
files (StreamForge-generated only — use reflection otherwise); external OpenAPI `$ref`s; config
coverage of users/credentials or runtime state; encryption at rest for secrets; Engine changes of
any kind (006 never touches `StreamForge.Engine`).

## Risks & mitigations

- **NuGet restore offline** → W1 fails fast; fallback documented: hand cron parser (drop 6-field),
  YAML descoped to JSON-only. Decide loudly, don't limp.
- **PUT-replaces-whole-object × masking** → `***`-merge server-side (D-H) + an explicit W4 test:
  GET→PUT round-trip must not clobber stored secrets.
- **JWT expiry on long gRPC subscriptions** → re-login on UNAUTHENTICATED inside the reconnect
  path (2D); verified in W6-3 by forcing a short-lived token.
- **Remote schema drift** → snapshot-at-subscribe semantics documented; reconnect re-fetches.
- **Actor/grain timer churn for slow crons** → next-run computed once per fire (single timer per
  source, re-armed); Dapr timer re-armed in `OnActivateAsync` (existing GeneratorActor pattern).
- **Import against a live catalog racing entity mutations** → import applies through the same
  serialized registry (grain/actor turn-based) — per-entity atomicity only; whole-import atomicity
  is explicitly NOT promised (documented in report semantics).
- **Frozen fakes** → no new members on existing facades/grain interfaces (D-C); enforced by the
  hard gate (pre-existing test files unmodified).

## Sequencing / effort

P → W0 → W1 → {2A ∥ 2B ∥ 2C ∥ 2D ∥ 2E} → {3A ∥ 3B ∥ 3C} → W4 → {5A ∥ 5B} → W6 → W7.
Roughly 2–3 agent-weeks. Every wave lands as its own commit(s) with both suites green, so the
effort can pause after any wave with both platforms working.
