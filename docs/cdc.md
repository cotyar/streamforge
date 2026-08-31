# Change data capture

Plan 017 replaced the JVM Debezium dependency, for two databases, with a native .NET reader: **`postgres-cdc`**
(Postgres logical replication over a slot + publication, decoded via `Npgsql.Replication`'s pgoutput plugin)
and **`mssql-cdc`** (SQL Server's own CDC capture tables, read via `cdc.fn_cdc_get_all_changes_*` on a
`binary(10)` LSN). Both live in `shared/StreamsForge.Connectors.Database`
([`PgCdcSource.cs`](../shared/StreamsForge.Connectors.Database/PgCdcSource.cs),
[`MsSqlCdcSource.cs`](../shared/StreamsForge.Connectors.Database/MsSqlCdcSource.cs)), share one stamping
vocabulary ([`CdcStamp.cs`](../shared/StreamsForge.Connectors.Database/CdcStamp.cs)) and one LSN codec
([`CdcLsn.cs`](../shared/StreamsForge.Connectors.Database/CdcLsn.cs)), and are both `IPolledTransport`
implementations — CDC is still pull-shaped from this platform's point of view, one cycle at a time, even
though the *source* database is doing the pushing internally.

[`TRANSPORTS.md`](../TRANSPORTS.md)'s "Change data capture" section is the recipe for *adding* a transport
like this one, and [`plans/017-native-cdc.md`](../plans/017-native-cdc.md) is the *argument* for why it was
built the way it was. Neither tells you how to *use* what already exists — that's this document.

## Two ways to use this, and they are not the same price

| | You write | The platform gives you | Cost |
|---|---|---|---|
| **Standalone** (below) | The poll loop, cursor persistence, retry/backoff | The reader, the stamping, the preflight probe | Drags in `StreamsForge.AppCore`'s dependency tail — see [the box below](#the-standalone-dependency-tax) |
| **Inside StreamsForge** ([below](#inside-streamsforge)) | A source config (form or JSON/YAML) | The loop, the cursor, the schedule, the console, retries | None beyond running StreamsForge itself |

Read the standalone section even if you plan to run this inside StreamsForge — the contract it documents
(`PollAsync`, the cursor, the failure rule) is exactly what `PolledSourceCore` is doing on your behalf when
the platform drives it, and understanding it makes the operational hazards further down make sense.

---

## Standalone: a CDC reader with no StreamsForge server

The entire contract is four calls: construct a reader, build a `SourceDefinition` with a `DbSourceConfig`,
call `PollAsync(def, cursor, ct)` in your own loop, and **persist the returned cursor yourself.** Nothing
else — no server process, no Orleans, no Dapr, no NATS.

### The standalone dependency tax

State this before anything else, because it's real and a `dotnet add reference` finds it in about 30
seconds: **`StreamsForge.Connectors.Database` is not a lightweight CDC library.** Its two direct package
references are exactly what you'd expect — `Npgsql` and `Microsoft.Data.SqlClient` — but its two *project*
references are not:

- `StreamsForge.AppCore` (needed for `IPolledTransport`, `PolledBatch`, `TransportDescriptor`) pulls in
  `Google.Protobuf`, `Cronos`, `NATS.Net`, `YamlDotNet`, `Grpc.Net.Client`, `Grpc.Reflection`, and a project
  reference to `StreamsForge.Engine` — the streaming SQL engine itself.
- `StreamsForge.Contracts` (needed for `SourceDefinition`, `DbSourceConfig`, `SourceKinds`) pulls in
  `Microsoft.Orleans.Serialization.Abstractions`.

None of that is optional today — `IPolledTransport` and `SourceDefinition` are declared where they are
because a polled *and* a CDC connector are both citizens of the same registry the rest of StreamsForge uses,
not because a CDC-only consumer needs a streaming engine, a gRPC reflection client, or Orleans serialization
attributes. Measured against the scratch console project used to verify this document:
`dotnet list package --include-transitive` reports **2 direct and 46 transitive NuGet packages**, and a
`dotnet build` output folder for a program that does nothing but poll one Postgres table comes to **~18 MB
across 52 assemblies**. Restore time and cold-start time both pay for the whole tail, not just the half you
use.

**The honest state of this:** it works, today, exactly as documented below — this is not a "coming soon."
But if you only want a CDC reader and nothing else, you are paying for a streaming SQL platform's dependency
graph to get it. The obvious follow-up — extracting `PgCdcSource`/`MsSqlCdcSource` plus their pure helpers
(`CdcStamp`, `CdcLsn`, `PgRelationCache`, `PgTupleDecoder`, `MsSqlCdcPlanner`) behind a dependency-free
surface, with `IPolledTransport`/`SourceDefinition` reduced to the minimal shape a standalone reader needs
— is not attempted here. It is a real, scoped piece of future work, not a promise this document is making on
the codebase's behalf.

### Postgres (`postgres-cdc`)

**Server-side setup**, once, as a superuser:

```sql
-- postgresql.conf, or a container's command line — NOT reloadable, needs a restart:
-- wal_level = logical

CREATE ROLE cdc_reader WITH LOGIN PASSWORD '...' REPLICATION;
GRANT ALL ON SCHEMA public TO cdc_reader;    -- or narrower: USAGE + SELECT on the tables you're capturing

CREATE PUBLICATION orders_pub FOR TABLE public.orders;   -- FOR ALL TABLES also works
```

The replication **slot** is the one piece you get to choose about: either create it yourself —

```sql
SELECT pg_create_logical_replication_slot('orders_slot', 'pgoutput');
```

— or set `DbSourceConfig.CreateSlotIfMissing = true` and let the reader create it on its first cycle.
`CreateSlotIfMissing` **defaults to `false`, deliberately**: creating a slot is what starts pinning WAL on a
database this connector doesn't own, so it is not something this reader will do without being told to. If
neither happens, the first `PollAsync` throws a message naming the exact `SELECT
pg_create_logical_replication_slot(...)` and `CREATE PUBLICATION ...` statements to run.

**The config object, fully populated**, and which fields are inert:

```csharp
new DbSourceConfig
{
    // Connection — shared with the plain "postgres" polled kind and the "mssql"/"mssql-cdc" pair.
    Host = "db.internal",
    Port = 0,                 // 0 = dialect default (5432)
    Database = "app",
    Username = "cdc_reader",
    Password = "...",         // [Secret] — masked on every read path if this ever crosses an API boundary
    Tls = false,
    ConnectionString = null,  // escape hatch: overrides every field above it when set

    // CDC-only fields (Id 18-23 on DbSourceConfig) — meaningful for postgres-cdc, ignored by "postgres"/"mssql".
    SlotName = "orders_slot",
    PublicationName = "orders_pub",
    Tables = "public.orders",        // optional CSV allowlist; the PUBLICATION is the real filter, this only narrows
    CreateSlotIfMissing = false,     // see above
    MaxPollMs = 1000,                // how long one PollAsync drains before returning what it has
    InitialCursor = "",              // an LSN ("0/16B3748") to seed from; empty = tail (new changes only)
    BatchSize = 1000,                // caps rows per cycle at a TRANSACTION boundary — see "Delivery" below

    // Inert for postgres-cdc, and ACTIVELY REJECTED by Validate() if set:
    // CursorColumn, CursorKind (if changed from "long"), Query, Where, CaptureInstance — those belong to
    // the plain "postgres" polled kind or to mssql-cdc; Validate() names which, in the error message.
    // Snapshot is also rejected outright — see "Backfill" below. DedupKeyColumn is simply unused: the CDC
    // cursor is an exact LSN, not a watermark that can re-read the same row twice.
}
```

One thing to know before it surprises you: `BatchSize` **is honored** by `PgCdcSource` (it caps rows per
cycle) but is **not exposed** in the console's config form for `postgres-cdc` — only `maxPollMs` is, because
time is the primary knob for a streaming read. If you're driving this standalone (or through the raw
`SourceDefinition`/config-document API) you can still set it; the default is `1000`.

**The complete program.** This is the exact file that was built and run against a real `postgres:17`
container (`wal_level=logical`) to verify every claim in this section:

```csharp
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Transports;
using StreamsForge.Connectors.Database;

var source = new PgCdcSource(new PostgresDialect());

var def = new SourceDefinition
{
    Name = "orders-cdc",
    Kind = SourceKinds.PostgresCdc,
    Connector = new ConnectorConfig
    {
        Db = new DbSourceConfig
        {
            Host = "localhost",
            Port = 15532,
            Database = "demo",
            Username = "cdc_reader",
            Password = "cdc_pw",
            Tls = false,

            SlotName = "orders_slot",
            PublicationName = "orders_pub",
            CreateSlotIfMissing = true, // off by default: creating a slot starts pinning WAL
            MaxPollMs = 2000,
            BatchSize = 500,
        },
    },
};

const string CursorFile = "cursor.txt";

// Load whatever was persisted last run. null on this source's very first cycle ever.
string? cursor = File.Exists(CursorFile) ? await File.ReadAllTextAsync(CursorFile) : null;

Console.WriteLine($"starting cursor = {cursor ?? "(none -- first run)"}");

while (true)
{
    PolledBatch batch;
    try
    {
        batch = await source.PollAsync(def, cursor, CancellationToken.None);
    }
    catch (Exception ex)
    {
        // PollAsync throwing is normal and expected -- a database is down far more often than a
        // config is wrong. `cursor` is left exactly as it was: the next call retries from the same
        // point. A caller that resets or advances the cursor in a catch/finally block here has
        // reintroduced the exact data loss this contract exists to prevent.
        Console.WriteLine($"poll failed, cursor unchanged at {cursor ?? "(none)"}: {ex.GetType().Name}: {ex.Message}");
        await Task.Delay(TimeSpan.FromSeconds(5));
        continue;
    }

    foreach (var row in batch.Rows)
    {
        Console.WriteLine(string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}")));
    }

    // batch.Cursor is null exactly when nothing changed -- "leave the persisted value alone",
    // never "start over". Only persist when it is non-null.
    if (batch.Cursor is not null)
    {
        cursor = batch.Cursor;
        await File.WriteAllTextAsync(CursorFile, cursor);
        Console.WriteLine($"cursor -> {cursor} (rows this cycle: {batch.Rows.Count})");
    }

    // HasMore means "there is more waiting right now" -- re-poll immediately instead of sleeping.
    if (!batch.HasMore)
    {
        await Task.Delay(TimeSpan.FromSeconds(1));
    }
}
```

Notes on the loop, since every line above is load-bearing:

- **`HasMore`** is "re-arm immediately" — not a hint, a correctness requirement. A cycle that filled
  `BatchSize` mid-backlog re-polls without delay; only an empty or under-cap cycle waits. Ignore it and a
  large backlog drains at one cycle per `Task.Delay`, which still works but throttles a catch-up read to the
  idle polling interval for no reason.
- **A `null` batch cursor means "unchanged," never "start over."** The code above only overwrites the
  persisted value when `batch.Cursor is not null` — the seed cycle *does* return a cursor (there's a new LSN
  to remember), an idle cycle with nothing committed does not.
- **The failure contract is in the `catch`, not a comment above it.** `PollAsync` throwing is the *expected*
  shape of "the database is down" or "the slot fell over," not an exceptional path to special-case. The one
  thing that must never happen in that `catch` block is touching `cursor` — do that, and a transient outage
  becomes a silent gap the next successful cycle can never detect.

**What was actually run**, against the container described above, with the table freshly truncated and no
prior cursor file:

```
starting cursor = (none -- first run)
cursor -> 0/19A15E0 (rows this cycle: 0)
```

That's the seed cycle — no rows, but a real LSN minted and persisted (Postgres has no history to hand back
before the slot's own creation point). Then, with the program still running:

```sql
INSERT INTO public.orders (id, symbol, qty) VALUES (1, 'AAA', 10);
```
```
id=1, symbol=AAA, qty=10, _op=c, _weight=1, _table=public.orders, _ts=1786726813716
cursor -> 0/19A1700 (rows this cycle: 1)
```
```sql
UPDATE public.orders SET qty = 20 WHERE id = 1;
```
```
id=1, symbol=AAA, qty=20, _op=u, _weight=1, _table=public.orders, _ts=1786726816822
cursor -> 0/19A1790 (rows this cycle: 1)
```
```sql
DELETE FROM public.orders WHERE id = 1;
```
```
id=1, _weight=-1, _op=d, _table=public.orders, _ts=1786726819934
cursor -> 0/19A1808 (rows this cycle: 1)
```

Note what the delete row does **not** carry: `symbol` and `qty` are absent, not null — this table has the
default `REPLICA IDENTITY`, so a delete carries key columns only (see "Operational hazards" below). Setting
`ALTER TABLE public.orders REPLICA IDENTITY FULL;` and repeating the insert/delete on a second row produced,
verified against the same container:

```
id=2, symbol=BBB, qty=99, _op=c, _weight=1, _table=public.orders, _ts=1786726838648
id=2, symbol=BBB, qty=99, _op=d, _weight=-1, _table=public.orders, _ts=1786726849339
```

— the full row, on the delete, this time. Same reader, same program, only the table's replica identity
changed.

**The failure contract, proven, not just claimed:** stopping the container mid-run and leaving the consumer
running produced:

```
poll failed, cursor unchanged at 0/19A1B78: NpgsqlException: Failed to connect to 127.0.0.1:15532
poll failed, cursor unchanged at 0/19A1B78: NpgsqlException: Failed to connect to 127.0.0.1:15532
```

— `cursor.txt` on disk stayed at `0/19A1B78` through both failed cycles. Restarting the container, inserting
one more row, and running the consumer again resumed cleanly from that exact cursor with no gap and no
duplicate:

```
starting cursor = 0/19A1B78
id=3, symbol=CCC, qty=5, _op=c, _weight=1, _table=public.orders, _ts=1786726879568
cursor -> 0/19A1E00 (rows this cycle: 1)
```

**Run the preflight probe before pointing this at a production database.** `CdcPreflight.ProbePostgresAsync`
needs only a `SourceDefinition` and a `CancellationToken` — no replication connection, an ordinary one is
enough, because it reads catalog views:

```csharp
var result = await CdcPreflight.ProbePostgresAsync(def, CancellationToken.None);
```

Real output, same slot and table as above, captured after the slot had gone idle for a few minutes:

```
Fields:
  id: Long
  symbol: String
  qty: Long
  _op: String
  _weight: Long
  _ts: Timestamp
  _table: String

Diagnostics:
  - replication slot 'orders_slot' exists but is not currently active (nothing is reading from it). It is still pinning WAL while idle.
  - replication slot 'orders_slot' is 896 B behind the current WAL position (restart_lsn 0/19A1AB8, current WAL position 0/19A1E38). max_slot_wal_keep_size is '-1'/'0' (unbounded) — the SOURCE DATABASE'S DISK is the only limit on how far this slot can fall behind; it pins WAL until the disk fills if nothing drains it.
  - one or more inferred columns can be TOASTed: if a row is updated without changing that column, pgoutput sends the sentinel string '__debezium_unavailable_value' instead of its real content, unless REPLICA IDENTITY FULL is set on the table — treat that literal string as "unknown", never as real data.
```

Every line there is something you want to know *before* traffic starts, not after: the slot is idle (so
nothing is draining it yet — expected before the first run, alarming a week later), the WAL-lag line names
the exact safety valve, and the TOAST warning tells you `symbol`/`qty` can produce the sentinel string on an
update that leaves them unchanged, without `REPLICA IDENTITY FULL`.

If the connecting role lacks permission to read `pg_replication_slots`/`pg_current_wal_lsn` (denied on some
managed instances), the probe does not throw — it reports a diagnostic naming the fix: `grant the pg_monitor
role`, or connect as a role that already has it.

### SQL Server (`mssql-cdc`)

**Server-side setup**, once, with `sysadmin` or `db_owner`:

```sql
USE app;
EXEC sys.sp_cdc_enable_db;

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'orders',
    @role_name     = NULL,
    @capture_instance = N'dbo_orders';   -- conventionally <schema>_<table>; this exact string is
                                          -- interpolated into cdc.fn_cdc_get_all_changes_<capture>
                                          -- later, so it must match ^[A-Za-z_][A-Za-z0-9_]*$
```

Outside Azure SQL Database, CDC needs the **SQL Server Agent** running — `sp_cdc_enable_table` schedules a
capture job and a cleanup job through it, and nothing shows up in the capture tables no matter how healthy
this reader looks until that job actually runs. **Azure SQL Database is the one exception**: CDC runs on an
internal scheduler there, there is no Agent at all, and a missing capture/cleanup job is normal, not broken
— `CdcPreflight.ProbeMsSqlAsync` checks `SERVERPROPERTY('EngineEdition')` and reports it as information,
specifically so it doesn't tell every Azure user their working setup is broken.

**The config object, fully populated:**

```csharp
new DbSourceConfig
{
    Host = "sql.internal",
    Port = 0,                 // 0 = dialect default (1433)
    Database = "app",
    Username = "cdc_reader",
    Password = "...",
    Tls = false,
    ConnectionString = null,

    // CDC-only / CDC-meaningful fields:
    CaptureInstance = "dbo_orders",  // must match ^[A-Za-z_][A-Za-z0-9_]*$ -- validated, never bound as a param
    Schema = "dbo",                  // informational: stamps _table, and is what the Tables filter compares against
    Table = "orders",                // same
    Tables = "",                     // optional CSV filter; this reader already reads exactly one capture instance
    Snapshot = false,                // true = start from sys.fn_cdc_get_min_lsn -- see "Backfill" below
    InitialCursor = "",              // a 20-char lowercase-hex LSN to start from; empty (no Snapshot) = tail only
    BatchSize = 1000,                // TOP (@batch) cap per cycle, at a transaction boundary
    CommandTimeoutSeconds = 30,

    // Inert for mssql-cdc, and ACTIVELY REJECTED by Validate() if set:
    // CursorColumn, CursorKind (if changed from "long"), Query, Where, SlotName, PublicationName --
    // those belong to the plain "mssql" polled kind or to postgres-cdc.
}
```

**The complete program** — same shape as the Postgres one, same failure contract. **This one is
compile-verified only**: it was built successfully against `StreamsForge.Connectors.Database` (`dotnet
build` succeeded, 0 warnings, 0 errors), but no SQL Server instance was run against it for this document —
pulling `mcr.microsoft.com/mssql/server:2022-latest` (~1.5 GB) was judged not worth the time for this pass.
Nothing below is claimed to have executed against a live server.

```csharp
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Transports;
using StreamsForge.Connectors.Database;

var source = new MsSqlCdcSource(new SqlServerDialect());

var def = new SourceDefinition
{
    Name = "orders-cdc",
    Kind = SourceKinds.MsSqlCdc,
    Connector = new ConnectorConfig
    {
        Db = new DbSourceConfig
        {
            Host = "localhost",
            Port = 1433,
            Database = "demo",
            Username = "cdc_reader",
            Password = "cdc_pw",
            Tls = false,

            CaptureInstance = "dbo_orders",
            Schema = "dbo",
            Table = "orders",
            BatchSize = 500,
        },
    },
};

const string CursorFile = "cursor.txt";
string? cursor = File.Exists(CursorFile) ? await File.ReadAllTextAsync(CursorFile) : null;

Console.WriteLine($"starting cursor = {cursor ?? "(none -- first run)"}");

while (true)
{
    PolledBatch batch;
    try
    {
        batch = await source.PollAsync(def, cursor, CancellationToken.None);
    }
    catch (Exception ex)
    {
        // Same failure contract as Postgres: cursor untouched, retry from the same point. On SQL
        // Server this is also how a retention breach surfaces -- CheckRetention throws rather than
        // silently skipping the gap, so this catch is where an operator finds out about it.
        Console.WriteLine($"poll failed, cursor unchanged at {cursor ?? "(none)"}: {ex.GetType().Name}: {ex.Message}");
        await Task.Delay(TimeSpan.FromSeconds(5));
        continue;
    }

    foreach (var row in batch.Rows)
    {
        Console.WriteLine(string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}")));
    }

    if (batch.Cursor is not null)
    {
        cursor = batch.Cursor;
        await File.WriteAllTextAsync(CursorFile, cursor);
        Console.WriteLine($"cursor -> {cursor} (rows this cycle: {batch.Rows.Count})");
    }

    if (!batch.HasMore)
    {
        await Task.Delay(TimeSpan.FromSeconds(1));
    }
}
```

One shape difference from Postgres worth calling out explicitly: SQL Server has no equivalent of the
Postgres seed cycle minting an LSN from nothing to remember. With no persisted cursor, no `InitialCursor`,
and `Snapshot = false` (the default), the first cycle resolves `from = sys.fn_cdc_get_max_lsn()` — the
current tail — and, like Postgres, emits zero rows and persists that as the cursor rather than reading
anything. `Snapshot = true` instead starts at `sys.fn_cdc_get_min_lsn`, i.e. whatever the capture table's
retention window still has — see "Backfill is asymmetric" below before reaching for it.

## Inside StreamsForge

Inside the platform, `IPolledTransport`/`PolledSourceCore` own everything the standalone loop above hand-rolls
— the cursor is a driver-persisted field on the source, the schedule replaces `Task.Delay`, and a failed
cycle already keeps the old cursor without you writing a `catch` block for it. You interact with a CDC source
the same way you interact with any other source kind:

- **Console:** create a source, pick kind `postgres-cdc` or `mssql-cdc`, and fill the form
  `PgCdcSource.Describe()`/`MsSqlCdcSource.Describe()` renders — the same field lists shown above, minus the
  ones each kind's descriptor doesn't expose (`postgres-cdc`'s form has no `batchSize`; see the note above).
  A "Discover" button runs the preflight probe and shows its diagnostics inline, because `CanProbe = true` on
  both descriptors.
- **API:** `POST /api/sources` / `PUT /api/sources/{name}` with a `SourceDefinition` body shaped like the
  config-document example below; `GET /api/sources/{name}/status` to watch the cursor, error, and
  `EnvelopeSkippedTotal` (Postgres only — see "Operational hazards").
- **Config document (JSON/YAML):** `GET /api/config/export` / `POST /api/config/import`, or `bun
  admin/sf.ts config` (the admin CLI's own wrapper over the same endpoints). `ConfigDocument.Sources` reuses
  `SourceDefinition` directly — nothing is stripped —
  so the exported shape is exactly `SourceDefinition` serialized, camelCase, via `ConfigSerializer`. A real
  export of two CDC sources (secrets pre-masked here to `"***"`) looks like this:

```json
{
  "version": 1,
  "sources": [
    {
      "name": "orders-cdc",
      "description": "Postgres logical replication over the orders table",
      "generatorProfile": "generic",
      "eventsPerSecond": 5,
      "enabled": true,
      "kind": "postgres-cdc",
      "connector": {
        "db": {
          "host": "db.internal",
          "port": 0,
          "database": "app",
          "username": "cdc_reader",
          "password": "***",
          "schema": "",
          "table": "",
          "where": "",
          "query": "",
          "cursorColumn": "",
          "cursorKind": "long",
          "initialCursor": "",
          "dedupKeyColumn": "",
          "batchSize": 1000,
          "snapshot": false,
          "commandTimeoutSeconds": 30,
          "tls": false,
          "slotName": "orders_slot",
          "publicationName": "orders_pub",
          "captureInstance": "",
          "tables": "public.orders",
          "maxPollMs": 1000,
          "createSlotIfMissing": false
        }
      },
      "onCoercionFailure": "Null"
    },
    {
      "name": "customers-cdc",
      "description": "SQL Server CDC capture table for customers",
      "generatorProfile": "generic",
      "eventsPerSecond": 5,
      "enabled": true,
      "kind": "mssql-cdc",
      "connector": {
        "db": {
          "host": "sql.internal",
          "port": 0,
          "database": "app",
          "username": "cdc_reader",
          "password": "***",
          "schema": "dbo",
          "table": "customers",
          "where": "",
          "query": "",
          "cursorColumn": "",
          "cursorKind": "long",
          "initialCursor": "",
          "dedupKeyColumn": "",
          "batchSize": 1000,
          "snapshot": false,
          "commandTimeoutSeconds": 30,
          "tls": false,
          "slotName": "",
          "publicationName": "",
          "captureInstance": "dbo_customers",
          "tables": "",
          "maxPollMs": 1000,
          "createSlotIfMissing": false
        }
      },
      "onCoercionFailure": "Null"
    }
  ],
  "tables": [
    {
      "name": "orders_latest",
      "description": "",
      "sql": "SELECT * FROM \"orders-cdc\" WHERE _op <> 'd' LATEST BY (id)",
      "running": true,
      "searchEnabled": false,
      "searchMode": "Exact",
      "historyEnabled": false,
      "historyMode": "All",
      "historyLimit": 10,
      "historyWindowMs": 0,
      "parallelism": 1,
      "retentionMaxRows": 0,
      "retentionTtlMs": 0
    }
  ]
}
```

This is a real, unedited `ConfigSerializer.ToCanonicalJson` render (values obviously fictional) — note that
`DbSourceConfig`'s polled-kind fields (`where`, `query`, `cursorColumn`, ...) and the *other* CDC kind's
fields (`slotName`/`publicationName` on the `mssql-cdc` entry, `captureInstance` on the `postgres-cdc` one)
are present but empty, exactly plan 017's "eight inert fields" tradeoff described in
[`plans/017-native-cdc.md`](../plans/017-native-cdc.md) — the config document doesn't hide them, only the
console form does.

**The end-to-end worked example** the `orders_latest` table above shows is the whole pattern this connector
is built around: `LATEST BY (id)` keeps one row per key (the most recent event wins), and `WHERE _op <> 'd'`
hides a row whose most recent event was a delete. Together they turn an append-only stream of
inserts/updates/deletes into what looks like a live mirror of the source table — with the caveat spelled out
under "the honest limit" below.

## Operational hazards (read this before production traffic)

These apply whether you're running standalone or inside StreamsForge — the reader behaves identically either
way; only who is driving the poll loop differs.

**An undrained Postgres slot pins WAL until the *source* database's disk fills.** Not StreamsForge's disk —
the database being read from. A replication slot is a promise to the server: "don't discard WAL past this
point, someone is coming back for it." If nothing ever comes back — a deleted source, a crashed consumer
nobody restarted, a standalone program someone forgot about — the server keeps every byte of WAL since the
slot's `restart_lsn` indefinitely. `max_slot_wal_keep_size` is the server-side safety valve: set to a byte
size, PostgreSQL drops WAL past that bound and **invalidates the slot** (a hard failure the next read will
see, not silent data loss) rather than filling the disk. Set to `-1` or `0` (both mean "unbounded" — the
probe calls this out explicitly rather than making you look up what `0` means), the source database's disk
is the *only* limit, and nothing will stop it filling. **A deleted StreamsForge source does NOT drop its
slot** — `IPolledTransport` has no "this source was deleted" notification (see plan 017's "Decisions" for
why), so the slot StreamsForge created keeps pinning WAL after the source that used it is gone. Drop it
yourself:

```sql
SELECT pg_drop_replication_slot('orders_slot');
```

— this fails if the slot is currently `active` (something is still reading from it); stop the reader first.

**SQL Server CDC retention defaults to 3 days.** `sys.sp_cdc_cleanup_change_table` runs on that schedule by
default and permanently deletes anything older. A consumer — standalone or a StreamsForge source — that
stays stopped longer than the retention window has **lost that data**, and there is no recovering it from
CDC; `MsSqlCdcPlanner.CheckRetention` compares the resolved starting LSN against `sys.fn_cdc_get_min_lsn` and
**throws** rather than silently resuming from wherever retention still reaches — the exact same "loud
failure over silent skip" discipline `PolledSourceCore`'s cursor rule enforces everywhere else. Recovering
deliberately means accepting the gap and moving the cursor forward on purpose: set `InitialCursor` to a
current LSN (or `Snapshot = true` to replay whatever retention still has, see below), never leave the reader
retrying the same throw forever.

**`REPLICA IDENTITY` decides what a Postgres `DELETE` carries.** The default (`REPLICA IDENTITY DEFAULT`)
sends only the primary key's columns on a delete — this document's own proof run showed exactly that: an
insert/update carrying `symbol`/`qty`, a delete carrying only `id`. `REPLICA IDENTITY FULL` sends the entire
old row on every delete (and every update's *before* image, which this reader doesn't currently use but
still pays the WAL cost for) — proven above with the same table, same reader, only the identity setting
changed. The cost is real and ongoing: `FULL` means Postgres writes the whole row into WAL on every `UPDATE`
to that table, not just the changed columns, for as long as the setting stays on — a table with wide rows
and heavy update traffic will measurably grow its WAL volume. Turn it on because a downstream consumer
genuinely needs the deleted row's other columns, not by default.

**An unchanged TOASTed column arrives as the sentinel `__debezium_unavailable_value`.** Postgres's TOAST
mechanism stores large column values (mostly `text`/`varchar`/`json`/`jsonb`/`bytea`/`numeric`, and arrays)
out-of-line, and logical replication omits an *unchanged* TOASTed value from the WAL entirely when the
table's replica identity doesn't force it in — there's genuinely nothing in the WAL record to decode. Rather
than invent a StreamsForge-specific placeholder, `CdcStamp.UnavailableValue` reuses **Debezium's own literal
string**, on purpose: an operator's existing SQL written against a Debezium-fed table — anything that
filters or special-cases this sentinel — keeps working unmodified against the native reader. Treat the
literal string `__debezium_unavailable_value` as "unknown, not sent," never as the column's real content;
`REPLICA IDENTITY FULL` is what makes it stop appearing.

**Delivery is at-least-once**, same ceiling as every polled kind in this platform: a cycle that fails after
reading but before its cursor is durably persisted is re-read in full on the next attempt, which can
re-deliver rows already emitted but never skip one. **A batch always ends on a transaction boundary** in
both dialects — Postgres buffers rows from `BeginMessage` to `CommitMessage` and only emits once the COMMIT
is actually seen; SQL Server's `TOP`-capped read re-reads, bounded to exactly one transaction's own
`__$start_lsn` with no `TOP`, whenever a capped read's only group can't be proven complete. The practical
consequence: **`BatchSize` is a target for a cycle's read, never a hard ceiling on what one batch can emit**
— a transaction bigger than the configured batch size is always delivered whole, over budget, once resolved,
at the cost of one extra round trip on the cycle that hits it. Emitting a truncated transaction and advancing
the cursor past it would be silent, permanent data loss, which is precisely the failure this rule exists to
make structurally impossible.

On Postgres specifically, an unrecognized pgoutput message (`TRUNCATE`, a type message, an origin message, a
future message this reader doesn't know) never fails the cycle, but is **counted**, not silently absorbed —
`PolledBatch.EnvelopeSkipped` rides into `ConnectorRuntimeStatus.EnvelopeSkippedTotal` inside StreamsForge, and
a standalone consumer reading `PolledBatch.EnvelopeSkipped` directly gets the same number. SQL Server's
reader is always `0` here — its `'all'` CDC row filter can only ever produce `__$operation` 1/2/4, and the one
way that contract could break already fails the cycle loudly (a thrown exception) rather than needing a
counter.

**Backfill is asymmetric between the two dialects:**

- `postgres-cdc` **refuses `Snapshot` outright** (`Validate()` rejects it with a message naming the fix) — a
  replication slot carries no history from before its own creation, full stop. Backfill with the plain
  `postgres` polled kind first, then switch the source to `postgres-cdc` to tail from where the backfill left
  off.
- `mssql-cdc`'s `Snapshot = true` means **"replay whatever the capture table still retains,"** not a
  full-table snapshot — CDC retention is finite (3 days by default, see above), so this can only ever replay
  what hasn't aged out. For a true backfill, run the plain `mssql` polled kind first.

**The honest limit, restated because it's the one thing that's easy to assume away:** a StreamsForge source is
an append-only `EventRecord` stream. `_weight` on an inbound row — `-1` from `CdcStamp.WeightOf` on a
delete — is just a column value, not a retraction the Engine's Z-sets act on; that machinery is computed
*from* table SQL, not carried in from ingress. Neither CDC path, nor the Debezium-envelope path it sits next
to, frees the key a delete removed. `WHERE _op <> 'd' LATEST BY (<key>)` (the pattern shown above) **hides** a
deleted key from query results — it does not free it from the source's history. The tombstone event, and
every insert/update before it for that key, is still sitting in the stream.

**This is no longer the whole story — there is now a way to actually free a key**, just not one either CDC
path drives automatically. `POST /api/sources/{name}/events` accepts a `"_retract": true` row: it emits a
real weight `-1` for the last asserted row of that key, and a `LATEST BY (<key>)` table consuming that
source drops the key entirely — the row leaves `Snapshot()`, not just the query results a `WHERE` clause
filters. It is deliberately narrow: only a `LATEST BY` consumer knows what "the current row for a key" is,
so a source with any OTHER shape of running consumer (a `GROUP BY`, a plain projection) rejects the
retraction outright rather than admitting a delta that operator has no correct way to interpret — see
`RetractConsumerValidation` in `shared/StreamsForge.AppCore/Ingest/`. Nothing about the CDC readers above
calls this automatically: a Postgres or SQL Server delete still arrives as one more `_op = "d"` event, not a
push to this endpoint. An operator who wants a CDC delete to actually free the key, not just hide it, has to
bridge the two explicitly — e.g. a small consumer of the `_op = 'd'` events that turns each one into its own
`_retract: true` push, keyed the same way the `LATEST BY` table is. That bridge is not built here; this
paragraph exists so the gap between "CDC delivers deletes" and "the table forgets the key" is a documented
seam, not a surprise.

## Comparison to Debezium

Factual, not a sales pitch — decide from this, don't take it on faith.

| | This connector | Debezium |
|---|---|---|
| **Databases** | Postgres, SQL Server only | Postgres, SQL Server, MySQL, Oracle, MongoDB, and more |
| **Deployment** | In-process .NET library, or a StreamsForge `IPolledTransport` source | A separate JVM process (Kafka Connect or Debezium Server) |
| **Row shape** | Deliberately Debezium-compatible — same `_op` letters, same weight sign, same TOAST sentinel | The reference shape this connector copies |
| **Schema history** | None | Tracks DDL history, so a consumer can reason about a column's type at the time a given event was produced |
| **Connector catalog** | Two connectors | Dozens, actively maintained by a large community |
| **Single-message transforms** | None | A configurable transform pipeline (SMTs) between source and sink |
| **Ecosystem maturity** | New (plan 017, 2026) | Years of production use across many organizations |

**What this does not cover**, and does not intend to: MySQL, Oracle, MongoDB, and everything else Debezium
speaks that this project has no client library for. `MappingSpec.Envelope = "debezium"` still exists inside
StreamsForge for exactly that case — Debezium Server emitting into a NATS source remains the right route for
any database this connector doesn't speak natively; see
[`CdcEnvelope`](../shared/StreamsForge.AppCore/Connectors/Mapping/CdcEnvelope.cs) and
[`TRANSPORTS.md`](../TRANSPORTS.md#change-data-capture). The native pair is an *addition* for the two
databases this connector already speaks, not a replacement for the Debezium path in general — it exists
because a slot/capture-table reader was cheap to add for these two, not because Debezium was found wanting
for the rest.

## See also

- [`TRANSPORTS.md`](../TRANSPORTS.md) — the recipe for adding a transport, and the CDC section this document
  restates rather than duplicates.
- [`plans/017-native-cdc.md`](../plans/017-native-cdc.md) — why this was built the way it was, what was cut,
  and what it cost.
- [`shared/StreamsForge.Connectors.Database/CdcPreflight.cs`](../shared/StreamsForge.Connectors.Database/CdcPreflight.cs) —
  the preflight probe's full source; every diagnostic sentence it can produce is written there.
