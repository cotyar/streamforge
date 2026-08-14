namespace StreamForge.Abstractions;

// Plan 006 — connector configuration + runtime-status contracts. Frozen like everything else in
// this assembly: additive evolution only, next free [Id], set-accessors (ORLEANS0101 forbids init
// under cross-assembly codegen). Secret fields (URL header values, gRPC password/token) follow
// D-H secrets-lite: masked as SecretMask in every read path; a written SecretMask value means
// "keep the stored value".

/// <summary>Well-known source kinds (string constants, not an enum — additive like GeneratorProfile).</summary>
public static class SourceKinds
{
    public const string Generator = "generator";
    public const string Url = "url";
    public const string File = "file";
    public const string Folder = "folder";
    public const string Grpc = "grpc";
    /// <summary>Plan 008 W4: client-push ingress. The only kind that is not pull-based — there is no
    /// connector and no timer; rows arrive through IIngressFacade. See <see cref="IngestConfig"/>.</summary>
    public const string Ingest = "ingest";
    /// <summary>Plan 009 B1: a NATS subject subscription. A persistent subscriber like
    /// <see cref="Grpc"/>, not a polled kind — its Schedule is ignored. See <see cref="NatsSubConfig"/>.</summary>
    public const string Nats = "nats";

    /// <summary>Plan 014: a PostgreSQL table or query, polled with a durable cursor. Unlike every kind
    /// above it is driven by <c>IPolledTransport</c> — the pull-shaped seam — and is implemented in
    /// <c>StreamForge.Connectors.Database</c>, out of the core. See <see cref="DbSourceConfig"/>.</summary>
    public const string Postgres = "postgres";
    /// <summary>Plan 014: the Microsoft SQL Server twin of <see cref="Postgres"/>. Same config, same
    /// transport, a different dialect.</summary>
    public const string MsSql = "mssql";

    /// <summary>The masked placeholder for secrets-lite values (D-H).</summary>
    public const string SecretMask = "***";
}

/// <summary>Per-kind connector config container. Exactly one of Url/File/Folder/Grpc/Nats is set,
/// matching <see cref="SourceDefinition.Kind"/>. Schedule applies to url/file/folder kinds (grpc
/// is a persistent subscription — its Schedule is ignored). Mapping applies to url/file/folder/nats.
///
/// <para>Plan 010: a new message-transport kind adds its config property here (next free
/// <c>[Id(n)]</c>, <c>[Secret]</c> on its credential fields) and one <c>IInboundTransport</c>
/// implementation. Nothing in SecretsMasker, the connector drivers, or the validator needs editing —
/// see <c>InboundTransports</c>.</para></summary>
[GenerateSerializer]
public sealed class ConnectorConfig
{
    [Id(0)] public ScheduleSpec? Schedule { get; set; }
    [Id(1)] public UrlPollConfig? Url { get; set; }
    [Id(2)] public FilePollConfig? File { get; set; }
    [Id(3)] public FolderPollConfig? Folder { get; set; }
    [Id(4)] public GrpcSubConfig? Grpc { get; set; }
    [Id(5)] public MappingSpec? Mapping { get; set; }
    /// <summary>Plan 009 B1; set only for <see cref="SourceKinds.Nats"/>.</summary>
    [Id(6)] public NatsSubConfig? Nats { get; set; }

    /// <summary>Plan 014; set for <see cref="SourceKinds.Postgres"/> and <see cref="SourceKinds.MsSql"/>.
    /// One container for both, because the two differ only in dialect — the config surface is identical.
    /// It lives HERE rather than in the connector project because <c>SecretWalk</c> only recurses into
    /// types declared in this assembly, so a config class outside it would export its password in
    /// plaintext, silently — the exact failure <c>[Secret]</c> was introduced to eliminate.</summary>
    [Id(7)] public DbSourceConfig? Db { get; set; }
}

/// <summary>Plan 009 B1: NATS subject subscription. Credentials follow the secrets-lite convention
/// plan 006 established — read back as <see cref="SourceKinds.SecretMask"/>, and sending the mask on
/// write means "keep the stored value".</summary>
[GenerateSerializer]
public sealed class NatsSubConfig
{
    /// <summary>Server URL(s), comma-separated, e.g. "nats://localhost:4222".</summary>
    [Id(0)] public string Url { get; set; } = "";
    /// <summary>Subject to subscribe to; NATS wildcards (<c>*</c>, <c>&gt;</c>) are the server's to
    /// interpret, not ours.</summary>
    [Id(1)] public string Subject { get; set; } = "";
    /// <summary>Queue group. Two replicas sharing one group split the subject between them instead of
    /// both ingesting every message — which is this path's answer to the per-replica problem that
    /// IngestStatus.Aggregated documents on the push-ingress side. Empty = every replica gets
    /// everything, which is rarely what you want with more than one instance.</summary>
    [Id(2)] public string QueueGroup { get; set; } = "";
    /// <summary>Payload format, same vocabulary as the file/folder connectors ("ndjson" | "json" | "csv").</summary>
    [Id(3)] public string Format { get; set; } = "json";
    [Id(4)] [Secret] public string? Token { get; set; }
    [Id(5)] public string? Username { get; set; }
    [Id(6)] [Secret] public string? Password { get; set; }
    /// <summary>Contents of a .creds file, not a path — the catalog has to be portable across hosts.</summary>
    [Id(7)] [Secret] public string? Credentials { get; set; }
    /// <summary>Null (default) = core NATS subscribe: at-most-once, no cursor, nothing left behind on
    /// the server. Non-null opts into a JetStream durable consumer, which gets redelivery and acks at
    /// the price of server-side state this platform then owns.</summary>
    [Id(8)] public NatsJetStreamConfig? JetStream { get; set; }
}

/// <summary>Plan 009 B1: opt-in JetStream durable consumer. Deliberately not the default — a durable
/// consumer nobody drains is a server-side resource we would create and never clean up.</summary>
[GenerateSerializer]
public sealed class NatsJetStreamConfig
{
    [Id(0)] public string Stream { get; set; } = "";
    /// <summary>Durable consumer name. Two replicas sharing it share the work; distinct names each get
    /// their own cursor over the whole stream.</summary>
    [Id(1)] public string Durable { get; set; } = "";
    /// <summary>Max in-flight unacked messages, the JetStream-side analogue of an ingress buffer bound.</summary>
    [Id(2)] public int MaxAckPending { get; set; } = 1000;
}

/// <summary>Plan 009 B2: where a pipeline's rows or a table's deltas are republished. The platform's
/// first outbound concept — everything before this was inbound or read-on-demand. Kind is currently
/// only "nats"; the container shape exists so a second sink kind is additive.</summary>
[GenerateSerializer]
public sealed class SinkSpec
{
    [Id(0)] public string Kind { get; set; } = SinkKinds.Nats;
    [Id(1)] public bool Enabled { get; set; } = true;
    [Id(2)] public NatsPubConfig? Nats { get; set; }
    /// <summary>Plan 012; set only for <see cref="SinkKinds.File"/>.</summary>
    [Id(3)] public FileSinkConfig? File { get; set; }

    /// <summary>Plan 014; set for <see cref="SinkKinds.Postgres"/> and <see cref="SinkKinds.MsSql"/>.</summary>
    [Id(4)] public DbSinkConfig? Db { get; set; }

    /// <summary>Plan 014: an optional name, so <c>INSERT INTO &lt;name&gt; SELECT …</c> has something to
    /// address. Empty for every sink authored before that syntax existed, which is why the sugar reports
    /// an unknown target rather than guessing at a positional one.</summary>
    [Id(5)] public string Name { get; set; } = "";
}

public static class SinkKinds
{
    public const string Nats = "nats";

    /// <summary>Plan 012: append rows to a local file as CSV or NDJSON — the egress twin of the
    /// <see cref="SourceKinds.File"/> source kind. See <see cref="FileSinkConfig"/>.</summary>
    public const string File = "file";

    /// <summary>Plan 014: write rows into a PostgreSQL table, appending or mirroring. The first sink whose
    /// natural unit is a TRANSACTION rather than a message, which is why <c>IBatchSinkClient</c> exists.
    /// See <see cref="DbSinkConfig"/>.</summary>
    public const string Postgres = "postgres";
    /// <summary>Plan 014: the Microsoft SQL Server twin of <see cref="Postgres"/>.</summary>
    public const string MsSql = "mssql";
}

/// <summary>Plan 009 B2. DELIVERY IS FIRE-AND-FORGET and there is no backpressure from the sink into
/// the pipeline — the same honest limit plan 008's ingress documents, restated rather than pretended
/// away: a slow or absent broker drops, it does not slow the platform down.</summary>
[GenerateSerializer]
public sealed class NatsPubConfig
{
    [Id(0)] public string Url { get; set; } = "";
    /// <summary>Subject to publish to. May contain <c>{name}</c>, replaced with the pipeline/table
    /// name, so one spec can serve a whole catalog.</summary>
    [Id(1)] public string Subject { get; set; } = "";
    [Id(2)] [Secret] public string? Token { get; set; }
    [Id(3)] public string? Username { get; set; }
    [Id(4)] [Secret] public string? Password { get; set; }
    [Id(5)] [Secret] public string? Credentials { get; set; }
}

/// <summary>Plan 012: the file egress sink. Rows are APPENDED, never truncated — the file is a log,
/// and a sink that could truncate would silently discard whatever an operator pointed it at by mistake.
/// The writing process is the StreamForge host, so the path is resolved on the HOST's filesystem with
/// the host process's permissions: this is exactly the same trust the <see cref="SourceKinds.File"/> /
/// <see cref="SourceKinds.Folder"/> source kinds already place in an Editor, in the write direction.
/// In a container or on Cloud Run the target must be a mounted volume, or the output dies with the
/// instance.</summary>
[GenerateSerializer]
public sealed class FileSinkConfig
{
    /// <summary>Destination path. May contain <c>{name}</c>, replaced with the pipeline id / table name,
    /// so one spec can serve a whole catalog (same substitution <see cref="NatsPubConfig.Subject"/> uses).
    /// Missing parent directories are created.</summary>
    [Id(0)] public string Path { get; set; } = "";
    /// <summary><see cref="FileFormats.Csv"/> (default) or <see cref="FileFormats.Ndjson"/>. NOT
    /// <see cref="FileFormats.JsonArray"/>: a JSON array has a closing bracket, and an append-only writer
    /// with no idea when the stream ends can never write one — a half-written array file that no parser
    /// accepts would be worse than refusing the format.</summary>
    [Id(1)] public string Format { get; set; } = FileFormats.Csv;
    /// <summary>CSV only: explicit column order, comma-separated (e.g. "symbol,qty,_weight"). Empty =
    /// the first written row's key order. Either way the header is fixed for the life of the file, so a
    /// column that only appears in a later row is dropped rather than silently shifting every subsequent
    /// row one cell to the left — set this when the rows are not uniform.</summary>
    [Id(2)] public string Columns { get; set; } = "";
}

/// <summary>Cron (5/6-field, UTC, Cronos) XOR fixed interval; IntervalMs floor is 1000 (D-E).</summary>
[GenerateSerializer]
public sealed class ScheduleSpec
{
    [Id(0)] public string? Cron { get; set; }
    [Id(1)] public int? IntervalMs { get; set; }
}

/// <summary>HTTP(S) GET polling. Header VALUES are secrets-lite (D-H).</summary>
[GenerateSerializer]
public sealed class UrlPollConfig
{
    [Id(0)] public string Url { get; set; } = "";
    [Id(1)] public Dictionary<string, string> Headers { get; set; } = [];
    /// <summary>Optional OpenAPI derivation reference (schema was derived from it; kept for re-derive).</summary>
    [Id(2)] public OpenApiRef? OpenApi { get; set; }
    /// <summary>Plan 012: response body format, same vocabulary as the file/folder connectors
    /// ("ndjson" | "json" | "csv"). Defaults to "json", which is what this kind did before the field
    /// existed — an endpoint serving text/csv or NDJSON no longer needs a file in between.</summary>
    [Id(3)] public string Format { get; set; } = FileFormats.JsonArray;
}

/// <summary>Where an OpenAPI-derived schema came from (D-F).</summary>
[GenerateSerializer]
public sealed class OpenApiRef
{
    /// <summary>URL of the OpenAPI v3 document (JSON or YAML). Mutually exclusive with DocInline.</summary>
    [Id(0)] public string? DocUrl { get; set; }
    /// <summary>The document text itself, when supplied inline instead of by URL.</summary>
    [Id(1)] public string? DocInline { get; set; }
    /// <summary>operationId in the doc; response defaults to 200 / first application/json media type.</summary>
    [Id(2)] public string? OperationId { get; set; }
    /// <summary>Explicit JSON pointer to a schema (e.g. "#/components/schemas/Trade"); overrides OperationId.</summary>
    [Id(3)] public string? SchemaPointer { get; set; }
}

// ---- Plan 014: database ingress / egress ----

/// <summary>Plan 014: a database source, Postgres or MS SQL. Structured rather than a single connection
/// string BECAUSE a raw string with an embedded password masks to "***" wholesale, and the operator can
/// then no longer see which host it points at — the escape hatch below exists for the cases the structured
/// fields cannot express, and it pays that price knowingly.</summary>
[GenerateSerializer]
public sealed class DbSourceConfig
{
    [Id(0)] public string Host { get; set; } = "";
    /// <summary>0 = the dialect default (5432 / 1433).</summary>
    [Id(1)] public int Port { get; set; }
    [Id(2)] public string Database { get; set; } = "";
    [Id(3)] public string Username { get; set; } = "";
    [Id(4)] [Secret] public string? Password { get; set; }

    /// <summary>Schema holding <see cref="Table"/>. Empty = the dialect default (public / dbo).</summary>
    [Id(5)] public string Schema { get; set; } = "";
    /// <summary>Table to poll. Ignored when <see cref="Query"/> is set.</summary>
    [Id(6)] public string Table { get; set; } = "";
    /// <summary>Extra predicate ANDed onto the generated WHERE. Table mode only.</summary>
    [Id(7)] public string Where { get; set; } = "";
    /// <summary>Escape hatch: your own SQL, which MUST contain the <c>@cursor</c> placeholder. Bound as a
    /// PARAMETER, never interpolated — injection, and just as importantly type fidelity, since a
    /// timestamptz spliced in as text compares wrong across a DST boundary.</summary>
    [Id(8)] public string Query { get; set; } = "";

    /// <summary>The monotonic column the high-water mark is taken from.
    ///
    /// <para><b>Read this before choosing a timestamp.</b> A surrogate key compared with <c>&gt;</c> is
    /// safe. An <c>updated_at</c> compared with <c>&gt;</c> LOSES every row written in the same millisecond
    /// as the watermark; compared with <c>&gt;=</c> it re-reads them, which is why the recommended shape is
    /// <c>&gt;=</c> plus a <see cref="DedupKeyColumn"/>. Neither variant ever sees a row whose transaction
    /// commits after a later-timestamped one — a polled source is eventually-consistent-with-holes on a
    /// timestamp column, and that is the honest argument for CDC when it matters.</para></summary>
    [Id(9)] public string CursorColumn { get; set; } = "";
    /// <summary>"long" | "timestamp" | "string" (<see cref="CursorKinds"/>) — how the persisted opaque
    /// cursor string is parsed back into a bound parameter.</summary>
    [Id(10)] public string CursorKind { get; set; } = CursorKinds.Long;
    /// <summary>Where to start when no cursor is persisted yet. Empty + <see cref="Snapshot"/> = from the
    /// beginning; empty without it = from MAX(cursorColumn), i.e. new rows only.</summary>
    [Id(11)] public string InitialCursor { get; set; } = "";
    /// <summary>Column whose value dedups re-read rows. The companion to a <c>&gt;=</c> cursor.</summary>
    [Id(12)] public string DedupKeyColumn { get; set; } = "";

    /// <summary>Rows per poll. Also the page size for the initial snapshot, which pages across successive
    /// driver cycles — each persisting its cursor — so a restart mid-snapshot resumes instead of starting
    /// over.</summary>
    [Id(13)] public int BatchSize { get; set; } = 1000;
    /// <summary>Read the whole table first, then tail. See <see cref="InitialCursor"/>.</summary>
    [Id(14)] public bool Snapshot { get; set; }
    [Id(15)] public int CommandTimeoutSeconds { get; set; } = 30;
    /// <summary>Require TLS on the connection.</summary>
    [Id(16)] public bool Tls { get; set; }

    /// <summary>Full connection string. When set it WINS over every structured field above — for the
    /// options this shape does not model. Masked wholesale, with the visibility cost named at the top.</summary>
    [Id(17)] [Secret] public string? ConnectionString { get; set; }
}

/// <summary>Plan 014: a database sink. Connection fields mirror <see cref="DbSourceConfig"/> deliberately,
/// so an operator configuring both directions types the same form twice rather than two different ones.</summary>
[GenerateSerializer]
public sealed class DbSinkConfig
{
    [Id(0)] public string Host { get; set; } = "";
    [Id(1)] public int Port { get; set; }
    [Id(2)] public string Database { get; set; } = "";
    [Id(3)] public string Username { get; set; } = "";
    [Id(4)] [Secret] public string? Password { get; set; }
    [Id(5)] public string Schema { get; set; } = "";
    /// <summary>Destination table. May contain <c>{name}</c>, replaced with the pipeline id / table name,
    /// the same substitution the NATS and file sinks use. It must ALREADY EXIST — this sink issues no DDL,
    /// because a streaming sink that can CREATE is a trust escalation over one that can only INSERT.</summary>
    [Id(6)] public string Table { get; set; } = "";

    /// <summary>"append" | "upsert" (<see cref="DbSinkModes"/>).</summary>
    [Id(7)] public string Mode { get; set; } = DbSinkModes.Append;
    /// <summary>Comma-separated key columns. REQUIRED for <see cref="DbSinkModes.Upsert"/> and unused by
    /// append. Explicit rather than derived, because a sink client only ever sees the entity NAME — reaching
    /// back for its SQL to guess the identity would couple egress to the catalog. The console prefills it
    /// from the table's declared keys, which is a visible suggestion rather than a silent derivation.</summary>
    [Id(8)] public string KeyColumns { get; set; } = "";
    /// <summary>Write the Z-set weight as a <c>_weight</c> column. Append mode only — in upsert mode the
    /// weight IS the operation (negative = DELETE), so persisting it would store a number already spent.</summary>
    [Id(9)] public bool IncludeWeight { get; set; }

    /// <summary>Explicit column order, comma-separated. Empty = the union of the batch's keys.</summary>
    [Id(10)] public string Columns { get; set; } = "";
    [Id(11)] public int CommandTimeoutSeconds { get; set; } = 30;
    [Id(12)] public bool Tls { get; set; }
    [Id(13)] [Secret] public string? ConnectionString { get; set; }
}

/// <summary>How <see cref="DbSourceConfig.CursorKind"/> parses the persisted opaque cursor.</summary>
public static class CursorKinds
{
    public const string Long = "long";
    public const string Timestamp = "timestamp";
    public const string String = "string";
}

/// <summary>How a database sink applies a batch.</summary>
public static class DbSinkModes
{
    /// <summary>Every delivered row becomes an INSERT. Never deletes; the destination is a log.</summary>
    public const string Append = "append";
    /// <summary>Positive weights UPSERT on <see cref="DbSinkConfig.KeyColumns"/>, negative weights DELETE,
    /// deletes applied last within the one transaction so a delete-then-reinsert of the same key inside a
    /// batch lands the way the caller meant it. Rejected on a PIPELINE sink: a pipeline emits results, not
    /// deltas, so there is no identity and no weight and "mirror current state" means nothing there.</summary>
    public const string Upsert = "upsert";
}

/// <summary>Formats for file/folder sources.</summary>
public static class FileFormats
{
    public const string Ndjson = "ndjson";
    public const string JsonArray = "json";
    public const string Csv = "csv";
}

/// <summary>Poll one file; re-parse on content change (hash+mtime). No tailing guarantees.</summary>
[GenerateSerializer]
public sealed class FilePollConfig
{
    [Id(0)] public string Path { get; set; } = "";
    /// <summary>"ndjson" | "json" | "csv" (<see cref="FileFormats"/>).</summary>
    [Id(1)] public string Format { get; set; } = FileFormats.Ndjson;
}

/// <summary>Poll a directory; each NEW file (name+mtime ledger) is parsed once and remembered.</summary>
[GenerateSerializer]
public sealed class FolderPollConfig
{
    [Id(0)] public string Path { get; set; } = "";
    [Id(1)] public string Format { get; set; } = FileFormats.Ndjson;
    /// <summary>Optional glob over file NAMES within the folder (no recursion), e.g. "*.json".</summary>
    [Id(2)] public string? Glob { get; set; }
}

/// <summary>Subscription to a remote StreamForge DynamicStreamService (D-G — federation).
/// Password/Token are secrets-lite (D-H).</summary>
[GenerateSerializer]
public sealed class GrpcSubConfig
{
    /// <summary>Target gRPC address, e.g. "http://localhost:5299" (h2c).</summary>
    [Id(0)] public string Address { get; set; } = "";
    /// <summary>"source:{name}" | "pipeline:{id}" | "table:{id}" on the REMOTE instance.</summary>
    [Id(1)] public string EntityKey { get; set; } = "";
    /// <summary>Login for the remote's /api/auth/login (re-login on expiry). XOR Token.</summary>
    [Id(2)] public string? Username { get; set; }
    [Id(3)] [Secret] public string? Password { get; set; }
    /// <summary>Static bearer token alternative (no re-login possible — documented).</summary>
    [Id(4)] [Secret] public string? Token { get; set; }
    /// <summary>"reflection" (default) | "proto". Reflection walks the remote's v1alpha service;
    /// "proto" parses <see cref="ProtoText"/> (StreamForge-generated files only).</summary>
    [Id(5)] public string SchemaSource { get; set; } = "reflection";
    /// <summary>Pasted/downloaded proto text when SchemaSource == "proto".</summary>
    [Id(6)] public string? ProtoText { get; set; }
    /// <summary>Remote REST base for login when it differs from the gRPC address,
    /// e.g. "http://localhost:5199".</summary>
    [Id(7)] public string? RestAddress { get; set; }
}

/// <summary>Response-structure mapping (the "mapping document" deserializes into this; JSON or
/// YAML accepted at the API boundary). Paths use the JSONPath-lite subset:
/// $ .name ['name'] [n] [*] — nothing else (documented, closed).</summary>
[GenerateSerializer]
public sealed class MappingSpec
{
    /// <summary>Where the items live, e.g. "$.data.trades[*]". "$" = the root (single item, or
    /// each element when the root is an array).</summary>
    [Id(0)] public string ItemsPath { get; set; } = "$";
    /// <summary>Emitted FIELD name whose value dedups re-polled items. Null = no dedup.</summary>
    [Id(1)] public string? DedupKeyField { get; set; }
    /// <summary>Emitted FIELD name holding the event timestamp (epoch-ms or ISO-8601) → _ts.
    /// Null = arrival time.</summary>
    [Id(2)] public string? TimestampField { get; set; }
    [Id(3)] public List<FieldMapEntry> Fields { get; set; } = [];

    /// <summary>Plan 014: an envelope the payload is wrapped in and should be unwrapped from BEFORE
    /// <see cref="ItemsPath"/> applies. <see cref="CdcEnvelopes.None"/> (the default) is byte-identical
    /// to every mapping authored before this existed. It lives on the MAPPING rather than on a transport
    /// so that one unwrapper serves every transport at once — NATS today, a queue tomorrow, and a
    /// file of captured envelopes in a test.</summary>
    [Id(4)] public string Envelope { get; set; } = CdcEnvelopes.None;
}

/// <summary>Plan 014: recognized payload envelopes for <see cref="MappingSpec.Envelope"/>.</summary>
public static class CdcEnvelopes
{
    public const string None = "none";

    /// <summary>Debezium's change envelope: <c>op</c> c/u/r take the row from <c>after</c>, <c>d</c> takes
    /// it from <c>before</c> and stamps <c>_weight = -1</c>; <c>ts_ms</c> becomes <c>_ts</c>. Accepts both
    /// the raw <c>{schema,payload}</c> form and the already-unwrapped form the <c>ExtractNewRecordState</c>
    /// SMT emits, and passes a message with no <c>op</c> through untouched.
    ///
    /// <para><b>The honest limit:</b> a source is an append-only event stream, so <c>_weight</c> on an
    /// INBOUND row is just a column — the Engine's Z-set weights are computed by table SQL, not carried in
    /// from ingress. A delete therefore arrives as a tombstone row; the working pattern is
    /// <c>LATEST BY key</c> + <c>WHERE _op &lt;&gt; 'd'</c>, which hides the key but does not free it.</para></summary>
    public const string Debezium = "debezium";
}

/// <summary>One output field: where it comes from (path relative to the item) and its FieldDef
/// (name/type/children/isArray — the existing schema model, reused).</summary>
[GenerateSerializer]
public sealed class FieldMapEntry
{
    /// <summary>JSONPath-lite relative to the item, e.g. "price" or "user.tier". Null = same as Field.Name.</summary>
    [Id(0)] public string? SourcePath { get; set; }
    [Id(1)] public FieldDef Field { get; set; } = new("", FieldType.String);
}

/// <summary>Connector runtime status (D-C). Returned by IConnectorStatusFacade; null for
/// generator-kind sources. LastStatus: "never" | "ok" | "error".</summary>
[GenerateSerializer]
public sealed class ConnectorRuntimeStatus
{
    [Id(0)] public string SourceName { get; set; } = "";
    [Id(1)] public long? NextRunMs { get; set; }
    [Id(2)] public long? LastRunMs { get; set; }
    [Id(3)] public string LastStatus { get; set; } = "never";
    [Id(4)] public string? LastError { get; set; }
    [Id(5)] public int ConsecutiveFailures { get; set; }
    [Id(6)] public long EventsEmittedTotal { get; set; }
    [Id(7)] public int LastBatchCount { get; set; }

    /// <summary>Plan 009 C2: cumulative field-level coercion failures on this source's inbound rows —
    /// a value that would not convert to its declared type. A queryable counter rather than only a note
    /// in <see cref="LastError"/>, because under
    /// <see cref="CoercionFailurePolicy.DropRow"/>/<see cref="CoercionFailurePolicy.RejectBatch"/> these
    /// are rows that did not land, and "counted and surfaced" was the whole condition for letting a
    /// policy discard anything. Cumulative since activation, like
    /// <see cref="EventsEmittedTotal"/>.</summary>
    [Id(8)] public long CoercionFailuresTotal { get; set; }

    /// <summary>Plan 014: the polled transport's persisted high-water mark, verbatim and opaque — an
    /// LSN, a composite "(ts,id)", or a plain bigint, whatever the transport minted. Surfaced because an
    /// operator who cannot see the cursor cannot tell a stuck source from an idle one. Null for every
    /// kind that has no cursor.</summary>
    [Id(9)] public string? Cursor { get; set; }
}

// ---- REST helper DTOs (cross HTTP only, but follow house serialization style anyway) ----

/// <summary>POST /api/sources/schema/mapping-validate request.</summary>
[GenerateSerializer]
public sealed class MappingValidateRequest
{
    /// <summary>Mapping document text (JSON or YAML).</summary>
    [Id(0)] public string Document { get; set; } = "";
    /// <summary>Optional sample response body to dry-run the mapping against.</summary>
    [Id(1)] public string? Sample { get; set; }
}

[GenerateSerializer]
public sealed class MappingValidateResult
{
    [Id(0)] public bool Ok { get; set; }
    [Id(1)] public MappingSpec? Mapping { get; set; }
    [Id(2)] public List<string> Diagnostics { get; set; } = [];
    /// <summary>Rows extracted from Sample (first 10), for UI preview.</summary>
    [Id(3)] public List<Dictionary<string, object?>> PreviewRows { get; set; } = [];
}

/// <summary>POST /api/sources/schema/derive-openapi request.</summary>
[GenerateSerializer]
public sealed class SchemaDeriveRequest
{
    [Id(0)] public OpenApiRef OpenApi { get; set; } = new();
}

[GenerateSerializer]
public sealed class SchemaDeriveResult
{
    [Id(0)] public List<FieldDef> Fields { get; set; } = [];
    [Id(1)] public List<string> Diagnostics { get; set; } = [];
}

/// <summary>POST /api/sources/schema/from-remote request.</summary>
[GenerateSerializer]
public sealed class RemoteSchemaRequest
{
    [Id(0)] public GrpcSubConfig Grpc { get; set; } = new();
}

[GenerateSerializer]
public sealed class RemoteSchemaResult
{
    [Id(0)] public List<FieldDef> Fields { get; set; } = [];
    /// <summary>FieldNumberMap JSON (EntitySchemas.ParseMap format) captured from the remote.</summary>
    [Id(1)] public string FieldNumbersJson { get; set; } = "";
    [Id(2)] public List<string> Diagnostics { get; set; } = [];
}

/// <summary>One entity's outcome in a config import (D-J).</summary>
[GenerateSerializer]
public sealed class ConfigImportReportEntry
{
    /// <summary>"source" | "pipeline" | "table".</summary>
    [Id(0)] public string Kind { get; set; } = "";
    [Id(1)] public string Name { get; set; } = "";
    /// <summary>"created" | "updated" | "deleted" | "skipped" | "error".</summary>
    [Id(2)] public string Action { get; set; } = "";
    [Id(3)] public List<string> Diagnostics { get; set; } = [];
}

/// <summary>POST /api/config/import response (D-J).</summary>
[GenerateSerializer]
public sealed class ConfigImportReport
{
    /// <summary>"validate" | "merge" | "replace".</summary>
    [Id(0)] public string Mode { get; set; } = "";
    [Id(1)] public List<ConfigImportReportEntry> Entries { get; set; } = [];
    [Id(2)] public bool Ok { get; set; }
}
