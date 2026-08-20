# crates-foundation — Agent Instructions

Streaming-SQL platform ("StreamForge") in two runtime flavors: `orleans/` (**primary**, complete —
Microsoft Orleans 10) and `dapr/` (Dapr, for polyglot processing and runtime comparison — feature-complete
in code, but behind on live verification: read [`dapr/PARITY.md`](dapr/PARITY.md) before claiming anything
about it, and land new work on Orleans first). Both
flavors share one runtime-agnostic core (`shared/`): Engine, Contracts, AppCore, Api, and the `web/`
SPA. Execution plans with acceptance criteria: [`plans/`](plans/README.md). Adding an ingress/egress transport
(NATS + a `file` sink today; the recipe is one class + one registry line): [`TRANSPORTS.md`](TRANSPORTS.md). Architecture:
[`orleans/ARCHITECTURE.md`](orleans/ARCHITECTURE.md) · [`dapr/ARCHITECTURE.md`](dapr/ARCHITECTURE.md)
· Dapr's descoped/owed/unverified list: [`dapr/PARITY.md`](dapr/PARITY.md)
· rationale: [`orleans/DESIGN.md`](orleans/DESIGN.md) · runtime comparison + measured latency:
[`orleans/docs/comparison.html`](orleans/docs/comparison.html) (opened directly from the repo — see
its own note on why `/docs` doesn't serve it automatically).


**CSV I/O** (plan 012): `format: "csv"` on a url/file/folder/nats source sniffs its delimiter from the
header line, so TSV / semicolon / pipe exports read through the same format; the `file` sink kind
appends CSV or NDJSON to a path on the host (append-only, header fixed for the life of the file); and
`GET /api/tables/{id}/rows.csv` + `GET /api/pipelines/{id}/results.csv` (plus a **CSV** button on both
detail pages) download the same rows. One writer, `CsvFormatter`, behind all of it.

**Native CDC** (plan 017): `postgres-cdc` and `mssql-cdc` are two more `IPolledTransport` source kinds,
living in `shared/StreamForge.Connectors.Database` alongside the plain `postgres`/`mssql` kinds — a
source reads the database's own change log (Postgres logical replication, SQL Server capture tables)
instead of polling a cursor column. Their operational hazards (an undrained Postgres slot pinning WAL,
SQL Server's 3-day CDC retention default, `REPLICA IDENTITY FULL`) are written down in
[`TRANSPORTS.md`](TRANSPORTS.md)'s "Change data capture" section — read it before enabling either kind.

**FIX** (plan 018): `format: "fix"` is a fifth payload format — tag=value, delimiter sniffed, no FIX
dictionary (a static table of the common 4.2/4.4/5.0 tags, unknown tags fall back to `tag<N>` strings),
repeating groups parsed into nested JSON arrays — usable by any `url`/`file`/`folder`/`nats` source; and
`fix` is also a live, receive-only session source kind, `shared/StreamForge.Connectors.Fix` on
`QuickFIXn.Core`, an `IInboundTransport` out of the core like the database connectors. No FIX dictionary
ships with the platform (`UseDataDictionary=N`); order entry is deliberately a separate plan
([`019`](plans/019-fix-order-entry.md)). The session's operational hazards (the drop-oldest bridge
queue, `storePath`'s in-memory-vs-file-backed choice, at-most-once delivery) are written down in
[`TRANSPORTS.md`](TRANSPORTS.md)'s "FIX" section — read it before enabling the kind.

**FIX order entry** (plan 019, DONE): `fix-duplex` is a third transport seam, `IDuplexTransport` —
one live FIX session with both halves, declared as two entities that meet in the middle (a `fix-duplex`
source owning the session, a `duplex` sink on any pipeline/table naming it by `sourceName`). The session
lives in the connector driver (Orleans `ConnectorGrain` / Dapr `ConnectorActor`), not the sink, which is
why an unrelated field edit on the sink — which does tear down and rebuild that sink's client every 30s
— costs nothing: the sink owns no connection, only a `DuplexSessions.Find` lookup by name. A row's
`MsgType` column picks the outbound message; `FixRequiredFields` gates `NewOrderSingle`/
`OrderCancelRequest`/`OrderCancelReplaceRequest` against a curated table (no real dictionary); `ClOrdID`
generation is opt-in and unchecked for uniqueness when caller-supplied. `storePath` stops being optional
on this kind — an order session's sequence store is the record of what was sent, not a resend-avoidance
nicety. Execution reports correlate to the orders that caused them through ordinary platform SQL, joined
on `ClOrdID` — plan 019 D7's cheapest large win. Full wiring, the row→FIX mapping rules, and a gotcha
found live (every row a duplex sink actually forwards carries platform-reserved columns — `_ts`/
`_source`/`_weight` — that the outbound mapper must skip rather than refuse) are in
[`TRANSPORTS.md`](TRANSPORTS.md)'s `fix-duplex` sections.

**Entitlements, approvals, audit** (plan 015): authorization is per-resource grants, not the three
role strings. A grant is `action` (`pipeline.update`, `*`) × `scope` (`*` | exact entity **NAME** |
prefix `prod-*` | `tag:finance`) × `Allow`/`Deny`, deny-overrides, attached to a user, a group or a
role; `AccessGuard` asks it in every handler while the coarse `Viewer/Editor/Admin` policies stay on
every route. **Permissions resolve server-side per request, never from the JWT** — a revocation lands
within `Auth:PolicyCacheSeconds` (default 10). `Auth:Mode=legacy` is the one-flag rollback and the
default is `entitlements`, so a typo in that value leaves enforcement **on**. `access.write` (not
`user.write`) satisfies the coarse `Admin` policy — whatever satisfies it is the key to `/api/access`
itself. Scope is the NAME because ids are `Guid("n")` and match no `prod-*` grant. Optional approvals
(`Approvals:Enabled`, off by default; you cannot vote on your own request; approving records, it does
not execute) and a day-sharded audit log whose `truncated` counter exists so silence is never read as
absence. Masked `beforeJson`/`afterJson` are opt-in (`?includeChanges=true` + `access.read`) — a
credential rotation reads `"***" → "***"`, the key's presence is the signal. Routes:
`shared/StreamForge.Api/Endpoints/{AccessEndpoints,ApprovalsEndpoints,AuditEndpoints}.cs`; the pure
decision: `shared/StreamForge.AppCore/Access/PermissionEvaluator.cs`. Operator recipes and the full
gotcha list: the `sf-access` skill and `orleans/docs/index.html` § Roles, entitlements & approvals.

**Name resolution, versioning & discovery** (plan 016): every `GET /api/{sources|pipelines|tables}/{key}`
resolves the segment as an ordinal **id, else name** — 1 match, 0 → 404, ≥2 → **409** naming every candidate
id, never a silent first match. **`PUT`/`DELETE`/`start`/`stop` stay id-only** — a name a `GET` resolves
happily still 404s on a write to the identical URL. Sources have no id and can never be renamed; a table
renames only while `Stopped`, unsharded and unreferenced; pipeline names are enforced unique going forward.
Two registry-assigned counters — `Revision` (any change) and `SchemaRevision` (sources/tables, shape only)
— back `dependsOn: [{kind, name, schemaRevision}]` pins on pipeline/table config documents: a violated pin
sets `staleReason` and rolls into `catalogWarnings` as a count, and it never stops the entity.
`mode=validate` reports it as a `dependsOn:` diagnostic before anything is applied — **except** for a pin
naming an entity the same document declares, whose post-import revision is registry-assigned and so
unknowable at plan time; that one only surfaces as `staleReason` after the write. Config import is schema-gated by
default (`schemaPolicy: "any"` is the one string that turns it off) and can declare
`requires: [{kind, version}]` connector versions that refuse the **whole** import when unsatisfied — like
every per-entity refusal here, that comes back as **HTTP 200** with `ok:false`, so `curl -f` reads a
refusal as success. `GET /api/meta/instance` (anonymous) plus a configured `Discovery:Peers` directory let
a `grpc` source federate by **peer name + entity name** — no address, no GUID (`GrpcSubConfig.Peer`,
resolved fresh at each reconnect, wins over a literal `Address`/`RestAddress` when set); the peer directory
has no heartbeat or expiry, so a peer that went away is not noticed until re-probed. `@name` resolves any
endpoint-shaped config field (host/URL/connection string) from `Endpoints:<name>` at connect time, never
from the catalog and never written back on export — values read back unmasked at `GET /api/meta/endpoints`.
Full routes and verified live recipes: `orleans/docs/index.html` §§ REST API / Instance discovery &
federation / Configuration import-export; contributor-facing `@name` wiring: `TRANSPORTS.md`'s Named
external endpoints section.

**Environment isolation** (plan 021): an environment is the **registry KEY**, not a column — Orleans
activates a whole separate `RegistryGrain` per environment (Dapr: `RegistryActor`), so two same-named
tables in two environments are two grains with two states, not one filtered read. `default` is the
**empty string** internally and renders as `default` only in the API and console; `EnvKeys.Qualify("", k)
== k`, which is why an existing `data/` directory and an existing Redis store come up byte-identical —
there is no migration. The separator is `.`, **not** the `:` the plan's own D3 named — overruled in wave
0, because `JsonFileGrainStorage` sanitizes every state-filename character outside `[A-Za-z0-9_.-]` to
`_`, and `_` is legal in an entity name: `staging:orders` and a default-environment table named
`staging_orders` would sanitize to the SAME file. A source/table name may not contain `.` going forward
(refused at create/rename), which costs nothing real — the SQL tokenizer already can't reference a dotted
identifier. The ambient (`EnvironmentAmbient`, an `AsyncLocal`) is **request-only**, set by exactly one
middleware and read only where a runtime key is composed; background work — supervisors, the lifecycle
orchestrator, connector drivers — never reads it and instead reads `Environment` off the definition it is
acting on, because the ambient is empty outside a request and empty silently means default. **Seeding is
default-environment-only**: `RegistryGrain.EnsureInitializedAsync` runs on every environment's registry at
boot (so already-Running entities resume everywhere), but its three seed blocks are gated on
`_env == EnvKeys.Default` — otherwise creating an empty `staging` and restarting would fill it with the
demo catalog, and force-deleting an environment's contents would silently re-seed them on the next boot.
Environments are a **namespace, not a security boundary** until 015's grants are scoped to one (the
plan's D9) — any authenticated Editor can point `X-StreamForge-Environment` at any environment today.
Full routes, the console picker, force-delete semantics and the gotcha list: `orleans/docs/index.html`
§ Environments; contributor-facing key composition: `orleans/ARCHITECTURE.md` / `dapr/ARCHITECTURE.md`;
the `/sf-env` skill; per-wave outcomes and the found-and-not-fixed list at the end of
`plans/021-environment-isolation.md`.

## Environment — non-negotiables

- **dotnet**: `~/.dotnet/dotnet` (SDK 10.0.3xx). It is **NOT on PATH** — always use the full path.
- **JS tooling**: **bun only, never npm** (build: `bun run build` in the web folder).
- **Ports**: Orleans dev server owns `5199` (REST/SignalR/SPA) + `5299` (gRPC h2c) and is often
  running — never bind or kill it. Dapr flavor: app `5399` (REST/SignalR/SPA), gRPC reserved `5499`
  (not yet served — phase 2), sidecar HTTP `3599` / gRPC `4599`; run via `dapr/tools/run.sh` (dapr
  runtime 1.18.x, containers `dapr_redis`/`dapr_placement`/`dapr_scheduler` from `dapr init`), reseed
  via `dapr/tools/reset.sh` + restart, stop via `dapr stop --app-id streamforge-dapr`. Test instances:
  pick 6xxx–9xxx via `--Http:Port … --Grpc:Port … --DataDir <temp>` and kill them when done.
  Containerized stacks (plan 007): orleans compose `6199`, dapr compose `6399`, admin app `5599` —
  these are the *container* ports; never confuse them with (or bind over) the dev servers above.
- Seeds apply only to an **empty data dir** (`orleans/src/StreamForge.Host/data/`; delete to
  reseed). Logins: `admin/admin123!`, `editor/editor123!`, `viewer/viewer123!`.
- Git: remote `origin` = public `github.com/cotyar/streamforge`, branch `master`. Commit
  messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`. Push after stable
  committed waves.

## Build / test / run

```bash
~/.dotnet/dotnet build orleans/StreamForge.sln
~/.dotnet/dotnet test  orleans/StreamForge.sln     # 2424 tests — the whole suite must be green
~/.dotnet/dotnet build dapr/StreamForge.Dapr.sln
~/.dotnet/dotnet test  dapr/StreamForge.Dapr.sln   # 695 tests — the whole suite must be green
cd web && bun run build
~/.dotnet/dotnet run --project orleans/src/StreamForge.Host   # :5199 + :5299
cd dapr && ./tools/run.sh                                      # :5399 (needs `dapr init` done once)
```
Both counts EXCLUDE the 52 live-database tests (`StreamForge.Connectors.Database.Tests/Integration/**`,
shared by both solutions), which `DockerGate` skips with a stated reason unless a Docker daemon answers
and the backend's image (`postgres:17`, `mcr.microsoft.com/mssql/server:2022-latest`) is already local —
"0 integration tests ran" must never read as "integration passed".

**Known time-bounded flakes** (they assert progress within a deadline, so they lose races under
whole-solution parallel load — never under `--filter` on their own): `LoopbackCycleTests`,
`TableRetentionClusterTests`, `TablePersistenceModeClusterTests`, `TableSnapshotMirrorClusterTests`,
`BackfillOnAttachClusterTests`, `ShardedTableClusterTests.RetentionEviction_*`, `ApprovalSweeperTests`,
`ConnectorGrainClusterTests.GetStatusAsync_reflects_a_successful_run` (waits for a connector run's
status to propagate before a deadline),
`WarmUpstreamDiagnosticClusterTests.TableStartedAfterItsTableInputAlreadyHoldsRows_*` (waits for a
warning log line to be emitted before a deadline).
Re-run a failure in isolation before calling it a regression — and report BOTH results, never just the
green one. Nothing else on this list is allowed to grow without a paragraph saying why the test is
time-bounded; a genuinely broken test hiding among "known flakes" is the failure mode this list can cause.
Also: do not pipe a test run through `tail` — it truncates the per-project `Passed!`/`Failed!` lines and
you will not know whether a project ran at all. Grep `^(Passed!|Failed!)` on the full log instead.
Orleans stream-transport knobs (post-005 latency work): `--Streams:Transport push` swaps the
pull-based memory streams for the in-process push bus (`Host/Streaming/PushStream*`, p50 1ms vs
stock 115ms on tableDelta); default `pull` is byte-identical stock Orleans, tunable via
`--Streams:PullPeriodMs` (default 100). `TABLES__FLUSHMS` tunes the epoch flush (P≥2 tables only).
Sharded tables (plan 011 D1, `TableDefinition.ShardBy`, empty = off): `--Shards:IdleSeconds` (default
120) is the shard grain class's activation-collection age — how long an idle key stays resident before
its state is flushed to disk and the grain collected, which is the whole feature; `--Shards:QuantumSeconds`
lowers the silo-wide collection scan interval (Orleans requires it to be strictly smaller than any
collection age, so shortening the idle below ~90s needs it). Orleans-only; Dapr stores `ShardBy` but
refuses to start such a table. Plan 011 D2: a sharded table keeps NO persisted snapshot mirror (the shards
are the durable per-key copy), refuses `MemoryOnly` and refuses being RENAMED (the tier is keyed by name);
`GET /{id}/shards/scan?fenced=true` is an opt-in consistent cut that pauses the tier's ingest for the scan.
Soak shapes: `tools/soak/run-soak.sh --shape orders|instruments`.

Local skills (root `.claude/skills/`, `sf-` prefix) wrap the common workflows: `/sf-run` (both
flavors), `/sf-verify` (both flavors), `/sf-sql`, `/sf-client-gen`, `/sf-config` (catalog
export/import), `/sf-access` (entitlements, approvals, audit), `/sf-federate` (instance discovery,
peer federation, named external endpoints), `/sf-env` (environment isolation — create/select/force-delete).

**Containers, Cloud Run, admin, AI chat** (plan 007): `deploy/orleans/` and `deploy/dapr/` hold
each flavor's Dockerfile(s), `compose.yaml` (host ports 6199/6399), Cloud Run `service.yaml`, and
parameterized `deploy.sh` (`--dry-run` supported; images pinned linux/amd64 — grpc.tools protoc
segfaults under native arm64 Docker). The Dapr stack is self-contained: app+daprd+placement+redis
in one network namespace, no scheduler (timers only). `admin/` (`bun main.ts`, :5599) starts/stops
either containerized stack (or Cloud Run services with `MODE=cloudrun`) and polls `/healthz`.
AI control chat: `POST /api/chat` + SPA "AI Control" page on both flavors, Google Gemini function
calling over the catalog facades — needs `GEMINI_API_KEY` (or `Gemini:ApiKey`), returns a clear 503
without it; capped per login session by `ChatRateLimiter` (`Chat:MaxRequestsPerSession`, default 10,
≤0 disables), 429 past that. Chat logic lives in `shared/StreamForge.Api/Chat/`.

**Admin CLI + MCP server** (plan 013, `admin/`, zero npm deps like the rest of that folder):
`bun admin/sf.ts <health|login|ls|get|start|stop|create|delete|rows|results|validate|config|api>`
administers a running instance over REST (`SF_URL`, default :5199 — point it at :5399 for Dapr);
`bun admin/mcp.ts` serves the same operations as MCP tools over stdio (hand-written to the spec, no
SDK). Both share `admin/sfclient.ts`. Tests: `bun test admin/` (21).

**Dapr flavor extras**: `dapr/tools/run.sh` starts the sidecar'd host on 5399 (sidecar 3599/4599);
`dapr/tools/reset.sh` SCANs and deletes this app's Redis keys to reseed (the Dapr-flavor equivalent
of deleting Orleans' `data/`); `dapr stop --app-id streamforge-dapr` stops it. Polyglot processors
(`dapr/processors/{python-enricher,ts-consumer,java-consumer}/`, each with its own `README.md` and
own sidecar/ports) prove the pub/sub contract (`dapr/POLYGLOT.md`) works from outside .NET:
`dapr run --app-id sf-enricher --app-port 8399 --dapr-http-port 3899 --dapr-grpc-port 4899
--resources-path dapr/components -- python3 main.py` (python-enricher), analogous `dapr run`
invocations with their own ports for `ts-consumer` (`bun run main.ts`) and `java-consumer`
(`gradle --no-daemon run`) — see each processor's README for exact ports/env vars.

## Hard rules (learned the expensive way)

1. **Frozen contracts, additive evolution.** `orleans/src/StreamForge.Engine/PublicApi.cs`,
   existing `StreamForge.Abstractions` members, and `web/src/api/types.ts` change additively only
   (next free `[Id(n)]`, optional fields). Never edit existing test expectations to make a
   refactor pass — behavior-preserving refactors keep the old tests green *unmodified*.
2. **The Engine stays pure.** No Orleans/Dapr/ASP.NET types inside `StreamForge.Engine` — it is
   the shared semantic core both runtimes depend on.
3. **Grain reentrancy**: `RegistryGrain` is non-reentrant with a `[MayInterleave]` allowlist.
   Any orchestrator↔worker call cycle deadlocks without allowlisting — check before adding cycles.
4. **Orleans streams serialize payloads**: dictionary subclasses need surrogates (see
   `EventRecordSurrogate`). Memory streams are not a free pass.
5. **Field numbers are forever**: dynamic-protobuf numbering persists in the registry (Active +
   Reserved); both reflection and proto downloads must obtain numbers there, keyed canonically by
   entity **id** even when resolved by name.
6. **SQL fine print**: aggregate JSON with `->` not `->>` (text sums to zero); pipeline-mode
   subqueries must be windowed; `LATEST BY` is table-mode only. Full list: DESIGN.md §D11.
7. **Branding is neutral**: plain "StreamForge" wordmark only — every trace of the original
   client's name was removed from the pages, the docs and the plans (2026-08-04/09, pre-open-source);
   do not reintroduce any client name, and never reproduce a client logo graphic. Theme via tokens (`sf.theme`, light default); no raw hex outside the
   sanctioned `--sql-*`/`--chart-*` vars.

## Multi-agent wave discipline (how this repo was built)

- Orchestrator sequences **waves of parallel subagents** (Sonnet, maximum effort) with **strictly
  disjoint file ownership** per concurrent agent; anything shared (csproj, Program.cs, types.ts)
  is pre-assigned to exactly one owner or edited by the orchestrator between waves.
- Engine-exclusive work serializes (one agent owns `StreamForge.Engine/**` at a time); host/web
  tracks run in parallel with it.
- Every agent brief includes verification gates: build + **full** test suite (including the other
  flavor's regression suite once `shared/` exists) + live checks against a self-started instance
  on isolated ports, killed afterward. Commit (and push) between waves, one logical change per
  commit.
- Contracts that two concurrent agents must meet in the middle (stream envelopes, grain
  interfaces, DTO shapes) are pinned verbatim in both prompts, or pre-built by the orchestrator.
