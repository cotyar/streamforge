# Plan 014 — Pluggable connectors + database ingress/egress

**Status: DONE.** All waves A–M plus L·Docs landed: `IPolledTransport`/`PolledTransports`/`PolledSourceCore`,
`CdcEnvelope`, the polled branch in both `ConnectorGrain` and `ConnectorActor`, `SinkSugar`,
`shared/StreamForge.Connectors.Database/**`, the console editors, and the live-DB suite under
`Tests/Integration/**`. Suites green at `cbbe5b0`: Orleans 2424, Dapr 695, excluding the 52 `DockerGate`
tests, which skip without a local `postgres:17` / `mcr.microsoft.com/mssql/server:2022-latest` image.

## Why

Plan 010 made a *message* transport (subject/topic + opaque payload) one class plus one registry line, in
both directions. Everything else — `url`/`file`/`folder`/`generator`/`grpc`/`ingest` — is still a hardcoded
branch in two runtime drivers. A database source does not fit the message seam at all: it is **pull**-shaped,
and it needs a durable **cursor**, which no abstraction in this repo provides.

So this plan adds the second seam (`IPolledTransport`), the primitive it needs (a persisted cursor), the
egress primitive a database sink needs (a batch that survives to the transaction boundary), and then one
out-of-core project implementing Postgres + MS SQL in both directions.

## Decisions, and what they cost

**`IPolledTransport` is a sibling of `IInboundTransport`, not a generalization of it.** `Open()` returns an
async enumerable that yields until it throws — the polling loop and its cursor would live *inside the
subscription instance*, in memory, lost on every silo recycle. That is the one thing that must not happen.
`PollAsync(def, cursor, ct) -> PolledBatch(Rows, Cursor, HasMore)` puts the loop in the driver, which already
persists state once per cycle, so the cursor rides along for free.

- No `FormatOf` — a result set is already structured; JSON-round-tripping a `numeric`/`timestamptz` to
  re-parse it loses fidelity for nothing.
- No ledger — `FileLedger`'s mtime map has no meaning here; the cursor replaces it.
- `Cursor` is an **opaque `string?`** the transport mints and the driver persists, so an LSN, a composite
  `(ts,id)`, and a plain bigint all fit without the platform learning what they are. `null` = leave unchanged.
- `HasMore` = re-arm immediately. This is what makes snapshot-then-tail resumable: an initial snapshot pages
  through in successive *driver cycles*, each persisting its cursor, so a restart resumes mid-snapshot.
- **On a failed cycle the driver keeps the old cursor.** A transport bug must not skip data.

**`url`/`file`/`folder`/`generator`/`grpc`/`ingest` are NOT migrated onto the new SPI.** Migrating them
touches `ConnectorPollCycle`, `FileLedger`, both drivers, `SourceValidation` and four SPA kind arrays, and
puts six pre-existing test suites at risk, for zero user-visible gain — nobody is adding a second `folder`
kind. The extensibility claim is proved by the acceptance test (a kind the repo has never heard of, driven
end to end), not by how many built-ins were rewritten.

**Plugin isolation is a separate project with a compile-time reference and an explicit `Register()` call,
not `AssemblyLoadContext`.** `Microsoft.Data.SqlClient` drags `Azure.Identity` and `Microsoft.IdentityModel.*`;
a diamond conflict against a plugin's own copies is exactly what ALC isolation is famous for, and this repo
has no diagnostic surface for it. It also means four deploy artefacts to update. The `Register()` seam keeps
runtime loading open; this plan finally gives it a real second call site (the hosts), which it never had.

**Connector config classes stay in `StreamForge.Contracts` with `[Secret]`.** `SecretWalk.IsContractClass`
recurses only into types in the Contracts assembly, so an out-of-tree config class would export its password
**in plaintext, silently** — precisely the failure plan 010 introduced `[Secret]` to eliminate. Keeping the
POCO in Contracts (it is strings and ints; it has no Npgsql dependency) means masking, export and the console
form all work with zero new machinery. Stated cost: a genuinely third-party plugin still cannot add its own
`[Id(n)]` — it sends a PR or rides an existing slot, as `TransportRegistryTests`' `FizzTransport` already does.

**`orleans/tests/StreamForge.Host.Tests/SecretWalkTests.cs` is edited — by design.** It reflects over every
class-typed property of `ConnectorConfig` and fails when a new one is not populated in `FullConnector()`. Its
own comment says this is deliberate: *"if a future transport adds a config property to ConnectorConfig and
nobody populates it here, this fails rather than letting the mask tests pass over a container they never
visit."* The guard was written for this moment; satisfying it with a one-line addition is the correct
response. The alternatives — a non-null default that passes the guard *vacuously*, or a string bag that needs
a whole second masking path — both defeat the guard rather than answer it. This is the **only** pre-existing
test file this plan touches.

**Debezium is consumed, not embedded.** A real CDC reader means Postgres logical replication (a slot that
stops being drained pins WAL until the *source database's* disk fills), plus MSSQL's entirely different
capture-table model, plus LSN durability, plus single-writer coordination across replicas. Debezium Server
already emits to NATS, which this platform already ingests, so CDC arrives as a `MappingSpec.Envelope`
unwrapper (`op: c|u|r` → row, `d` → `payload.before` + `_weight = -1`) that **every** transport can wear.

Honest limit: `_weight` on an *inbound* row is today just a column. Sources are append-only `EventRecord`
streams; weights become real only inside the Engine. So a Debezium delete gives `_op = "d"` / `_weight = -1`
flowing as an ordinary event, and the documented pattern is `LATEST BY <key>` + `WHERE _op <> 'd'`, which
hides the key but does not free it. Threading a real ingress retraction into the Engine's Z-sets is a
separate Engine project against 815 frozen-API tests. **Cut, named as the follow-up.** The DB *egress* upsert
path gives real delete semantics in the direction that actually asked for it.

**One delivered batch = one transaction. No time-based buffering.** `SinkSelection.Signature` tears a client
down on any `SinkSpec` edit; a linger-buffered sink loses its buffer unless `DisposeAsync` flushes, and
`DisposeAsync` racing the periodic refresh is a genuinely nasty bug. Binding the buffer to the batch that just
arrived makes that class impossible. `LingerMs` is the named follow-up.

**`INSERT INTO <sink-name> SELECT …` is pre-parse sugar in AppCore.** `INSERT` is a *statement* form: a new
top-level production, AST node, validator path, and a decision about what `CompileResult` returns for a
statement that is not a query — spreading into `Planner`, `TablePlanner` and both flavours' compile sites, all
to express a **destination**, which the platform already models as `SinkSpec`. The sugar strips the target,
stores the untouched `SELECT`, and enables the named sink. It is lossy on round-trip (re-opening shows the
`SELECT` plus a sink row); the console says so after save. Not chosen: `INSERT INTO postgres.public.trades`,
which implies a catalog-level named-connection entity — a much bigger plan.

**Second face of that lossiness, found during implementation:** a sugared config document now always plans as
`updated`, never `skipped`, because `ImportPlanner` compares document text against stored text and those
legitimately differ. Harmless here, but plan 016 bumps entity revisions off the same canonical-text
predicate — see 016's note, or it becomes revision churn on every import.

## Waves

Every wave gates on both solutions building and testing green (Orleans ≥1676, Dapr ≥313), `cd web && bun run
build` when `web/` is touched, and a live check on isolated ports (`--Http:Port 71xx --Grpc:Port 72xx
--DataDir <temp>`, Dapr `73xx`) with the instance killed and its temp dir removed. Never 5199/5299/5399.

### Round 1 — three fully parallel waves, zero shared files

| Wave | Owns | Delivers | Model |
|---|---|---|---|
| **A · Contracts** | `shared/StreamForge.Contracts/ConnectorModels.cs`, `orleans/tests/.../SecretWalkTests.cs` | `SourceKinds.Postgres/MsSql`, `SinkKinds.Postgres/MsSql`, `DbSourceConfig`, `DbSinkConfig`, `ConnectorConfig.[Id(7)] Db`, `SinkSpec.[Id(4)] Db` + `[Id(5)] Name`, `ConnectorRuntimeStatus.[Id(9)] Cursor`, `MappingSpec.[Id(4)] Envelope` | Opus 5 high |
| **B · Polled SPI** | `shared/StreamForge.AppCore/Transports/{IPolledTransport,PolledTransports,PolledSourceCore}.cs` (new), `TransportDescriptor.cs`, `Connectors/ConnectorPollCycle.cs`; new `PolledTransportRegistryTests.cs` | the SPI, registry, shared cycle core, `ExecuteRows`, descriptor flags (`Text`, `Polled`, `Mapping`, `CanProbe`) | Opus 5 high |
| **C · Batch sink seam** | `shared/StreamForge.AppCore/Sinks/{IBatchSinkClient,SinkFanout}.cs` (new), `ISinkTransport.cs`; new `SinkFanoutTests.cs` | optional batch interface, the fan-out in ONE place, `ISinkTransport.Validate` as a default method | Sonnet 5 high |

### Round 2 — five parallel waves

| Wave | Owns | Depends | Model |
|---|---|---|---|
| **D · CDC unwrapper** | `AppCore/Connectors/Mapping/CdcEnvelope.cs` (new) + tests | A, B | Sonnet 5 high |
| **E · Orleans driver** | `orleans/src/.../Grains/ConnectorGrain.cs`, `Services/NatsPublisherService.cs` + cluster test | B, C | Opus 5 high |
| **F · Dapr driver** | `dapr/src/.../Actors/ConnectorActor.cs`, `Streaming/NatsSinkPublisherService.cs` + test | B, C | Opus 5 high |
| **G · API + validation** | `shared/StreamForge.Api/Endpoints/{SourceSchemaService,TransportsEndpoints}.cs` + tests | B | Sonnet 5 high |
| **K · `INSERT INTO` sugar** | `AppCore/Sql/SinkSugar.cs` (new), `Api/Endpoints/{Pipelines,Tables}Endpoints.cs`, `ConfigImportService.cs` + tests | A | Opus 5 high |

### Round 3

| Wave | Owns | Depends | Model |
|---|---|---|---|
| **H · Database connectors** | `shared/StreamForge.Connectors.Database/**` + unit tests (no live DB) | A, B, C | Opus 5 high |
| **I · Host wiring** | both `Program.cs` + `.csproj`, both `.sln` | E, F, H | Sonnet 5 high |

### Round 4

| Wave | Owns | Depends | Model |
|---|---|---|---|
| **J · Console** | `web/src/api/types.ts`, `components/sources/TransportConfigEditor.tsx`, `pages/SourcesPage.tsx`, `components/SinksEditor.tsx` | G, I | Sonnet 5 high |
| **M · Live DB integration** | `shared/StreamForge.Connectors.Database.Tests/Integration/**` | H, I | Opus 5 high |

### Round 5

**L · Docs + end-to-end** — `TRANSPORTS.md`, `AGENTS.md`, `plans/README.md`, live sweep on both flavours.

## Design detail pinned across agents

- `PolledBatch(IReadOnlyList<Dictionary<string,object?>> Rows, string? Cursor, bool HasMore)`.
- Cursor storage: `ConnectorGrainState.Cursor` / `ConnectorActorState.Cursor` — both are **plain POCOs**, not
  `[GenerateSerializer]` contracts, so this is free on both flavours and breaks no contract test.
- `DbSourceConfig`: `Host, Port, Database, Username, [Secret] Password, Schema, Table, Where, Query,
  CursorColumn, CursorKind, InitialCursor, DedupKeyColumn, BatchSize=1000, Snapshot, CommandTimeoutSeconds,
  Tls, [Secret] ConnectionString` (the last wins when set). Structured rather than raw, because a raw string
  with an embedded password masks to `***` wholesale and the operator can then no longer see the host.
- **`InitialCursor` is the TRANSPORT's job, not the driver's** (found in wave F). Neither driver seeds its
  persisted cursor from it: the transport is handed the whole `SourceDefinition` alongside a `null` cursor and
  decides what "start here" means in its own dialect. Wave H must implement that, or `InitialCursor` is
  silently inert.
- **A config edit must not reset the cursor.** Both drivers re-run their start path on every catalog upsert, so
  a cursor reset there would re-read the entire table on any edit to the source. Documented on the field.
- `@cursor` is **always a bound parameter**, never interpolated — injection, and type fidelity across DST.
- The `updated_at` hazard is stated in the descriptor Help: `>` loses rows written in the same millisecond as
  the watermark, `>=` re-reads them; recommend `>=` plus `DedupKeyColumn`. Neither sees a transaction that
  commits after a later-timestamped one — that is the honest argument for CDC.
- `numeric`/`decimal` → `Double` loses precision; the probe reports it rather than silently rounding.
- Schema discovery is a generic `ISchemaProbe` capability + `POST /api/transports/{kind}/probe`, so
  `StreamForge.Api` learns nothing about databases.
- Upsert: `KeyColumns` explicit and required; the console prefills from
  `TableGroupKeyExtractor.Describe(sql).DeclaredKeys`. **"Deletes last" is not sufficient on its own** (wave H):
  a table UPDATE arrives as two deltas on the *same* key — `-1` old row, `+1` new row — so a sink honouring
  "deletes last" would delete the row the update just wrote. Both servers also refuse a batch naming one key
  twice (SQL Server 8672, Postgres "cannot affect row a second time"). Each key therefore resolves to its
  **last delta in the batch** first; the upsert and delete sets are then disjoint and ordering is a convention
  rather than a load-bearing rule. Assumes the batch is in causal order, which a delta batch is.
  **`InitialCursor` is required in query mode** (wave H): there is no `MAX` to seed from in arbitrary SQL and
  no safe sentinel. `Snapshot` is table-mode only for the same reason. Upsert is
  rejected on a *pipeline* sink — a pipeline emits results, not deltas, so "mirror current state" is meaningless.
- Failed commit: rollback, count, throttled failure callback, **drop the batch**. One retry on a classified
  transient connection fault. The at-most-once ceiling is in the descriptor Help in those words.

## Cut, ranked

1. Embedded CDC reader (logical replication / capture tables) — Debezium-via-NATS covers it.
2. Ingress retraction into the Engine's Z-sets — deep Engine change; egress upsert delivers the use case.
3. `AssemblyLoadContext` runtime plugin loading — no consumer, real diamond risk, four deploy artefacts.
4. Migrating the six built-in kinds onto the new SPI — all the regression risk, none of the benefit.
5. Time-based sink buffering (`LingerMs`).
6. Console schema/table picker — typed name + Discover covers it.
7. `CREATE TABLE IF NOT EXISTS` from a sink — DDL from a streaming sink is a trust escalation.
8. `COPY` / `SqlBulkCopy` fast paths — parameterized chunked INSERT first; optimize on a measurement.
