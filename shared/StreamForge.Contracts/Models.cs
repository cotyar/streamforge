namespace StreamForge.Abstractions;

[GenerateSerializer]
public enum FieldType { String, Double, Long, Bool, Timestamp, Json }

/// <summary>A source field. <see cref="Children"/> is the declared nested shape of a
/// <see cref="FieldType.Json"/> field (drill-down schema) — metadata that documents the payload,
/// drives synthetic generation for the "generic" profile, and feeds editor autocomplete. Null/empty
/// for scalar fields.
///
/// <para><see cref="IsArray"/> (additive, default false): the field holds a JSON array rather than a
/// single value. Combined with the other two: IsArray + <see cref="Children"/> declared = a typed list
/// of records (each element shaped like <see cref="Children"/>) — DescriptorFactory emits a repeated
/// nested message. IsArray + no Children (and Type != Json) = a repeated scalar of <see cref="Type"/>.
/// IsArray + Type == Json + no Children = a repeated schemaless value — DescriptorFactory emits
/// repeated google.protobuf.Struct. Orthogonal to Type/Children, so every existing combination keeps
/// its current (non-array) meaning.</para></summary>
// NOTE (005-W1): written as a record with body-declared properties (plain `set`) rather than the
// original positional-record shorthand (which synthesizes `init`-only properties). Orleans'
// cross-assembly codegen path (GenerateCodeForDeclaringAssembly, used because this type now lives in
// shared/StreamForge.Contracts while the generator runs in StreamForge.Abstractions) resolves
// property accessors purely from metadata and doesn't recognize `init` as settable there (ORLEANS0101:
// "does not have an accessible setter") — the same constructor-matching heuristic that lets same-
// assembly codegen use positional records apparently isn't available across that boundary. Equality/
// ToString/deconstruction/`with` all still work identically (records synthesize those from every
// public instance property regardless of positional-vs-body declaration); only init-vs-set changed,
// which Orleans serialization never observed either way. No caller depends on init-only-ness (verified:
// every construction site uses `new FieldDef(...)`, none use object-initializer-only patterns that
// require init).
[GenerateSerializer]
public sealed record FieldDef
{
    [Id(0)] public string Name { get; set; }
    [Id(1)] public FieldType Type { get; set; }
    [Id(2)] public List<FieldDef>? Children { get; set; }
    [Id(3)] public bool IsArray { get; set; }

    public FieldDef(string Name, FieldType Type, List<FieldDef>? Children = null, bool IsArray = false)
    {
        this.Name = Name;
        this.Type = Type;
        this.Children = Children;
        this.IsArray = IsArray;
    }
}

/// <summary>A stream source: schema + synthetic generator settings.</summary>
[GenerateSerializer]
public sealed class SourceDefinition
{
    [Id(0)] public string Name { get; set; } = "";
    [Id(1)] public string Description { get; set; } = "";
    [Id(2)] public List<FieldDef> Fields { get; set; } = [];
    /// <summary>Generator profile: "trades" | "quotes" | "orders" | "generic".</summary>
    [Id(3)] public string GeneratorProfile { get; set; } = "generic";
    [Id(4)] public double EventsPerSecond { get; set; } = 5;
    [Id(5)] public bool Enabled { get; set; } = true;
    /// <summary>User-editable free-form labels — see Feature A (metadata) in TableDefinition's doc comment.</summary>
    [Id(6)] public List<string> Tags { get; set; } = [];
    /// <summary>User-editable free-form key-value annotations.</summary>
    [Id(7)] public Dictionary<string, string> Metadata { get; set; } = [];
    /// <summary>Source kind (plan 006, additive): "generator" (default — the pre-existing
    /// behavior) | "url" | "file" | "folder" | "grpc". See <see cref="SourceKinds"/>.</summary>
    [Id(8)] public string Kind { get; set; } = SourceKinds.Generator;
    /// <summary>Connector configuration; null for generator-kind sources (plan 006).</summary>
    [Id(9)] public ConnectorConfig? Connector { get; set; }
    /// <summary>Client-push ingress configuration (plan 008 W4); non-null only for
    /// <see cref="SourceKinds.Ingest"/> sources.</summary>
    [Id(10)] public IngestConfig? Ingest { get; set; }
}

[GenerateSerializer]
public enum PipelineStatus { Stopped, Running, Failed }

[GenerateSerializer]
public sealed class PipelineDefinition
{
    [Id(0)] public string Id { get; set; } = "";
    [Id(1)] public string Name { get; set; } = "";
    [Id(2)] public string Description { get; set; } = "";
    [Id(3)] public string Sql { get; set; } = "";
    [Id(4)] public PipelineStatus Status { get; set; } = PipelineStatus.Stopped;
    [Id(5)] public string? Error { get; set; }
    [Id(6)] public string CreatedBy { get; set; } = "";
    [Id(7)] public long CreatedAtMs { get; set; }
    [Id(8)] public long UpdatedAtMs { get; set; }
    /// <summary>User-editable free-form labels — see Feature A (metadata) in TableDefinition's doc comment.</summary>
    [Id(9)] public List<string> Tags { get; set; } = [];
    /// <summary>User-editable free-form key-value annotations.</summary>
    [Id(10)] public Dictionary<string, string> Metadata { get; set; } = [];

    /// <summary>Plan 008: real leaf source names this pipeline reads, from the last successful compile —
    /// the pipeline-side counterpart of TableDefinition.StreamInputs/TableInputs, and what makes lineage
    /// readable without a compile round-trip (POST /api/pipelines/validate is Editor-gated, a lineage view
    /// is not). Derived, never user-editable; empty until the SQL compiles.</summary>
    [Id(11)] public List<string> SourceNames { get; set; } = [];
}

/// <summary>One emitted result row. Values are primitives only (string/double/long/bool/null).</summary>
[GenerateSerializer]
public sealed class ResultEnvelope
{
    [Id(0)] public string PipelineId { get; set; } = "";
    [Id(1)] public long Seq { get; set; }
    [Id(2)] public long TimestampMs { get; set; }
    [Id(3)] public Dictionary<string, object?> Row { get; set; } = [];
}

[GenerateSerializer]
public sealed class PipelineMetrics
{
    [Id(0)] public string PipelineId { get; set; } = "";
    [Id(1)] public PipelineStatus Status { get; set; }
    [Id(2)] public double EventsInPerSec { get; set; }
    [Id(3)] public double RowsOutPerSec { get; set; }
    [Id(4)] public long TotalEventsIn { get; set; }
    [Id(5)] public long TotalRowsOut { get; set; }
    [Id(6)] public long WindowsClosed { get; set; }
    [Id(7)] public long LastEventTsMs { get; set; }
}

/// <summary>Published on the lifecycle stream when a pipeline changes state.</summary>
[GenerateSerializer]
public sealed class LifecycleEvent
{
    [Id(0)] public string PipelineId { get; set; } = "";
    /// <summary>"created" | "updated" | "deleted" | "started" | "stopped" | "failed".</summary>
    [Id(1)] public string Kind { get; set; } = "";
    [Id(2)] public PipelineStatus Status { get; set; }
    [Id(3)] public long TimestampMs { get; set; }
}

/// <summary>Per-table reverse-index search strategy — see StreamForge.Host.Search.TableSearchIndex.</summary>
[GenerateSerializer]
public enum TableSearchMode { Exact, Fuzzy }

/// <summary>Per-key retention policy for opt-in ROW HISTORY (see TableDefinition.HistoryEnabled and
/// StreamForge.Host.Grains.TableHistoryGrain). All: keep every version up to an internal safety cap.
/// LastN/FirstN: keep the most-recent/earliest N versions (ring buffer / stop-appending respectively).
/// MinBy/MaxBy: keep only the version with the min/max value of HistoryByField, plus the always-current
/// latest version (2 entries max).</summary>
[GenerateSerializer]
public enum TableHistoryMode { All, LastN, FirstN, MinBy, MaxBy }

/// <summary>A persistent materialized TABLE: a SELECT over streams and/or other tables, without windows
/// (running aggregates instead of windowed ones). Its name is unique across sources+tables and enters the
/// SQL namespace, so other tables can FROM/JOIN it directly.</summary>
[GenerateSerializer]
public sealed class TableDefinition
{
    [Id(0)] public string Id { get; set; } = "";
    [Id(1)] public string Name { get; set; } = "";
    [Id(2)] public string Description { get; set; } = "";
    [Id(3)] public string Sql { get; set; } = "";
    [Id(4)] public PipelineStatus Status { get; set; } = PipelineStatus.Stopped;
    [Id(5)] public string? Error { get; set; }
    [Id(6)] public string CreatedBy { get; set; } = "";
    [Id(7)] public long CreatedAtMs { get; set; }
    [Id(8)] public long UpdatedAtMs { get; set; }
    /// <summary>Output row schema (name + kind) from the last successful compile — used to validate
    /// downstream tables that FROM/JOIN this one, independent of whether this table is currently Running.</summary>
    [Id(9)] public List<FieldDef> OutputFields { get; set; } = [];
    /// <summary>Stream source names this table's SQL reads from directly (from the last successful compile).</summary>
    [Id(10)] public List<string> StreamInputs { get; set; } = [];
    /// <summary>Other table names this table's SQL reads from directly (from the last successful compile).</summary>
    [Id(11)] public List<string> TableInputs { get; set; } = [];
    /// <summary>Whether a reverse (inverted) search index over this table's rows is maintained.</summary>
    [Id(12)] public bool SearchEnabled { get; set; }
    /// <summary>Exact (token/prefix/substring) or Fuzzy (trigram-similarity, typo-tolerant) search.</summary>
    [Id(13)] public TableSearchMode SearchMode { get; set; } = TableSearchMode.Exact;

    // ------------------------------------------------------------------
    // Feature B: opt-in per-row-identity version history. See TableHistoryGrain.
    // ------------------------------------------------------------------

    /// <summary>Whether a TableHistoryGrain records per-row-identity version history for this table.</summary>
    [Id(14)] public bool HistoryEnabled { get; set; }
    [Id(15)] public TableHistoryMode HistoryMode { get; set; } = TableHistoryMode.All;
    /// <summary>Version cap for LastN/FirstN modes.</summary>
    [Id(16)] public int HistoryLimit { get; set; } = 10;
    /// <summary>Output field (numeric or timestamp) MinBy/MaxBy ranks on. Required (and validated against
    /// OutputFields) when HistoryMode is MinBy or MaxBy.</summary>
    [Id(17)] public string? HistoryByField { get; set; }
    /// <summary>Retention time window in ms; versions older than (now - window) are pruned on append and
    /// on read. 0 = unbounded.</summary>
    [Id(18)] public long HistoryWindowMs { get; set; }

    // ------------------------------------------------------------------
    // Feature A: user-editable metadata. See SourceDefinition's doc comment for the same fields there.
    // ------------------------------------------------------------------

    [Id(19)] public List<string> Tags { get; set; } = [];
    [Id(20)] public Dictionary<string, string> Metadata { get; set; } = [];

    /// <summary>Plan 003 M2: opt-in partitioned execution. 1 (default) = the original single-grain
    /// TableGrain path, byte-for-byte unchanged (zero-risk default — see TableGrain's class comment on the
    /// Parallelism==1 fast path). 2..16 deploys the partitioned dataflow graph (TableIngestGrain +
    /// TableStageGrain × stages × partitions + TableOutputGrain) — see StreamForge.Engine.Dataflow.TableDataflowPlan
    /// and TableGrain's Parallelism&gt;=2 coordinator-mode doc comment. Validated 1..16 by RegistryGrain;
    /// changing it restarts the table (same restart condition as a SQL/search-config change).</summary>
    [Id(21)] public int Parallelism { get; set; } = 1;

    /// <summary>Plan 008: how this table's materialized snapshot reaches durable storage.
    /// <see cref="TablePersistenceMode.Batched"/> (default) is the pre-008 behavior — a dirty flag plus a
    /// periodic flush that awaits the write inside the grain turn, so a flush stalls the table for as long
    /// as serializing the whole snapshot takes. The other two trade durability for that stall; see the enum.</summary>
    [Id(22)] public TablePersistenceMode Persistence { get; set; } = TablePersistenceMode.Batched;

    /// <summary>Flush interval in ms for <see cref="TablePersistenceMode.Batched"/> and
    /// <see cref="TablePersistenceMode.FireAndForget"/>. 0 = the 2000 ms default. Ignored for
    /// <see cref="TablePersistenceMode.MemoryOnly"/>. Changing it restarts the table.</summary>
    [Id(23)] public int FlushMs { get; set; }

    /// <summary>Plan 009 A2: journal length that triggers a compaction, for
    /// <see cref="TablePersistenceMode.Journaled"/> only. 0 = a sensible default. Too small and every
    /// flush degenerates into a full snapshot write (i.e. Batched with extra steps); too large and
    /// activation spends its time replaying.</summary>
    [Id(24)] public int JournalMaxEntries { get; set; }
}

/// <summary>Plan 008: per-table durability policy. State is the materialized snapshot; the question is only
/// how it gets to storage, never how it is computed.
///
/// The cost being traded away is real and measurable: a flush serializes the ENTIRE snapshot into DTOs and
/// awaits the write **inside the grain turn**, so the stall grows with the row count and lands on the same
/// turn queue as incoming deltas.</summary>
[GenerateSerializer]
public enum TablePersistenceMode
{
    /// <summary>Dirty flag + periodic flush, awaited in the grain turn. Survives a restart, resuming from the
    /// last flush — up to one interval of deltas is lost. The pre-008 behavior and still the default.</summary>
    Batched,

    /// <summary>Same periodic flush, but the write is not awaited by the grain turn: the turn returns as soon
    /// as the snapshot is captured, and the write completes in the background (single-flight — a flush already
    /// in progress is not overlapped, the next tick is skipped instead). A crash loses whatever had not yet
    /// reached the disk, with no signal that it was lost.</summary>
    FireAndForget,

    /// <summary>Never written. The table lives entirely in the activation, so nothing touches storage on any
    /// path. A restart brings the table back **empty**, re-accumulating only from deltas that arrive after it
    /// — it does not replay history, so this suits tables that are naturally re-derivable or short-lived, and
    /// nothing else.</summary>
    MemoryOnly,

    /// <summary>Plan 009 A2. Same durability as <see cref="Batched"/>, but a flush writes only the rows that
    /// CHANGED since the last compaction (a separate, small journal state) instead of rewriting the whole
    /// snapshot — so write cost becomes O(changed) rather than O(|table|), which is what makes the flush
    /// interval stop being a latency knob on large tables. When the journal outgrows
    /// <see cref="TableDefinition.JournalMaxEntries"/> it is compacted: the full snapshot is written once
    /// and the journal truncated. Activation loads the snapshot and replays the journal over it, so the
    /// resumed state is identical to Batched's — the restart-resume limitation in TableGrain's class doc
    /// (output rows only, no operator internals) is unchanged and applies here too.</summary>
    Journaled,
}

/// <summary>Plan 003 M2: one partition's contribution to a partitioned table's aggregate
/// <see cref="TableMetrics"/> — additive detail, present only when Parallelism &gt;= 2 (see
/// TableMetrics.Partitions). StageId/Partition identify which TableStageGrain this is; the rest mirrors
/// TableMetrics' own per-activation counters at that grain.</summary>
[GenerateSerializer]
public sealed class TablePartitionMetrics
{
    [Id(0)] public int StageId { get; set; }
    [Id(1)] public int Partition { get; set; }
    [Id(2)] public long DeltasIn { get; set; }
    [Id(3)] public long DeltasOut { get; set; }
    [Id(4)] public long FrontierEpoch { get; set; } = -1;
    [Id(5)] public long LastUpdateMs { get; set; }
    /// <summary>Plan 003 M4: this stage's operator name (StreamForge.Engine.Dataflow.TableStageKind, e.g.
    /// "Join"/"Reduce"/"FilterProject" — see StreamForge.Engine.Dataflow.TableStageKindLabel), for the M5
    /// dataflow panel to render real operator names instead of bare stage ids. Additive; "" only if the
    /// producing grain somehow never learned its own stage descriptor (never happens in practice — see
    /// TableStageGrain.GetMetricsAsync).</summary>
    [Id(6)] public string Kind { get; set; } = "";
}

/// <summary>Serializable mirror of StreamForge.Engine's TableDelta, for Orleans/SignalR transport: one Z-set
/// delta — a row entering (+1) or leaving (-1) a table's output.</summary>
[GenerateSerializer]
public sealed class TableDeltaDto
{
    [Id(0)] public Dictionary<string, object?> Row { get; set; } = [];
    [Id(1)] public long Weight { get; set; }
}

/// <summary>One row of a table's current consolidated Z-set snapshot (weight is always &gt; 0 in a
/// consolidated snapshot, but the DTO carries it through as-is for transport symmetry with TableDeltaDto).</summary>
[GenerateSerializer]
public sealed class TableRowDto
{
    [Id(0)] public Dictionary<string, object?> Row { get; set; } = [];
    [Id(1)] public long Weight { get; set; }
}

[GenerateSerializer]
public sealed class TableMetrics
{
    [Id(0)] public string TableId { get; set; } = "";
    [Id(1)] public PipelineStatus Status { get; set; }
    [Id(2)] public long RowCount { get; set; }
    [Id(3)] public long DeltasIn { get; set; }
    [Id(4)] public long DeltasOut { get; set; }
    [Id(5)] public long LastUpdateMs { get; set; }
    /// <summary>True immediately after a restart-resume, until this table has rebuilt its state from live
    /// traffic — see TableGrain's rehydration-limitation comment.</summary>
    [Id(6)] public bool Rebuilding { get; set; }

    /// <summary>Plan 003 M2: per-partition detail, present (non-null) only for a Parallelism &gt;= 2 table —
    /// null/absent for every Parallelism==1 table, so this is additive-safe for existing consumers (REST
    /// JSON, gRPC, any client that ignores unknown fields).</summary>
    [Id(7)] public List<TablePartitionMetrics>? Partitions { get; set; }

    /// <summary>Plan 003 M3: distinct raw input names (stream sources / upstream tables) this table reads
    /// via a SHARED ArrangementGrain instead of a private per-table ingest — null/absent unless the table is
    /// Parallelism &gt;= 2 AND at least one of its join edges qualified as arrangeable (see
    /// StreamForge.Engine.Dataflow.TableDataflowBuilder's arrangeability rule). Purely informational; does
    /// not affect Rebuilding (see that flag's own doc — an attached-but-still-rebuilding-from-checkpoint
    /// arrangement is folded into THIS table's own Rebuilding instead).</summary>
    [Id(8)] public List<string>? ArrangedInputs { get; set; }

    /// <summary>Plan 003 M4: the epoch this table's consolidated read-side snapshot (Snapshot/RowCount/
    /// search index — everything GetRowsAsync/SearchAsync serve) reflects, for a Parallelism &gt;= 2 table
    /// only — null/absent for every Parallelism==1 table (which has no partitioned frontier at all) AND
    /// for a Parallelism &gt;= 2 table that hasn't yet observed a full epoch from every terminal-stage
    /// partition (see TableGrain's OnOutputBatchAsync doc comment for exactly what this number means and
    /// the consistency guarantee it carries: the snapshot reflects ALL deltas whose epoch is &lt;= this
    /// value and NONE beyond it). Mirrors <see cref="Host.Api.TableRowsResponse.FrontierEpoch"/> (same
    /// value, exposed on the read path that actually needs it) — kept on both DTOs since GetRowsAsync
    /// callers (REST /rows, StreamForge.Host.Api.TableRowsResponse.FrontierEpoch) shouldn't have to pay
    /// for a full GetMetricsAsync fan-out just to read it.</summary>
    [Id(9)] public long? SnapshotFrontierEpoch { get; set; }
}

/// <summary>
/// Plan 003 M3: request payload for <see cref="IArrangementGrain.AttachAsync"/> — everything a fresh
/// (refcount 0-&gt;1) arrangement activation needs to bootstrap itself (which raw input to subscribe to, the
/// raw field(s) forming its key, its partition identity) PLUS the routing info the arrangement needs to push
/// the atomic seed-then-live-deltas handshake directly to the attaching consumer's own ITableStageGrain (see
/// IArrangementGrain's class doc for why the arrangement — not the coordinator — must be the one to deliver
/// the snapshot, to avoid a snapshot/live-delta ordering race). Every field except <see cref="ConsumerId"/>/
/// <see cref="TargetGrainKey"/>/<see cref="TargetEdgeId"/> is redundant across repeated attaches to the SAME
/// arrangement (same keySpecHash ⇒ same InputName/KeyFields/KeySpec/PartitionCount/Partition by
/// construction) — only the FIRST attach (refcount 0-&gt;1) actually consumes them to activate; later attaches
/// trust the caller sent the same values (recompile-per-grain determinism — see GrainInterfaces.cs's M2
/// design note, which applies identically here) rather than re-validating.
/// </summary>
[GenerateSerializer]
public sealed class ArrangementAttachRequest
{
    /// <summary>Unique per (table, edge, partition) — e.g. "{tableName}:{edgeId}:{partition}". Used as the
    /// key DetachAsync later removes.</summary>
    [Id(0)] public string ConsumerId { get; set; } = "";
    /// <summary>The ITableStageGrain key ("{tableName}:{stageId}:{partition}") this arrangement partition
    /// pushes PushBatchAsync calls to.</summary>
    [Id(1)] public string TargetGrainKey { get; set; } = "";
    /// <summary>The EdgeId.Value the target's PushBatchAsync expects for this arrangement's contribution
    /// (the Left or Right join edge id on the CONSUMER's own dataflow plan).</summary>
    [Id(2)] public int TargetEdgeId { get; set; }
    /// <summary>Real stream source or upstream table name this arrangement indexes.</summary>
    [Id(3)] public string InputName { get; set; } = "";
    /// <summary>True if InputName is an upstream TABLE (subscribes to TableDeltaNamespace) rather than a raw
    /// stream SOURCE (SourcesNamespace).</summary>
    [Id(4)] public bool IsTableInput { get; set; }
    /// <summary>Raw field name(s), in order, forming this arrangement's key (see
    /// StreamForge.Engine.Dataflow.ArrangementKeySpec).</summary>
    [Id(5)] public List<string> KeyFields { get; set; } = [];
    /// <summary>Human-readable canonical form of (KeyFields, PartitionCount) — carried for diagnostics/
    /// GetInfoAsync; the grain KEY itself uses ArrangementKeySpec.HashOf(KeySpec) instead.</summary>
    [Id(6)] public string KeySpec { get; set; } = "";
    [Id(7)] public int PartitionCount { get; set; }
    [Id(8)] public int Partition { get; set; }
}

/// <summary>Plan 003 M3: point-in-time view of one ArrangementGrain partition — backs GetInfoAsync and the
/// GET /api/meta/arrangements endpoint.</summary>
[GenerateSerializer]
public sealed class ArrangementInfo
{
    [Id(0)] public string InputName { get; set; } = "";
    [Id(1)] public string KeySpec { get; set; } = "";
    [Id(2)] public int Partition { get; set; }
    [Id(3)] public int PartitionCount { get; set; }
    [Id(4)] public long RowCount { get; set; }
    [Id(5)] public int ConsumerCount { get; set; }
    /// <summary>True from (re)activation off a persisted checkpoint until this partition has processed at
    /// least one live batch since — mirrors TableGrain's own restart-resume Rebuilding contract (see
    /// ArrangementGrain's class doc).</summary>
    [Id(6)] public bool Rebuilding { get; set; }
    /// <summary>Last epoch this partition stamped (-1 if it has never flushed).</summary>
    [Id(7)] public long Epoch { get; set; } = -1;
}

// ============================================================================
// Row history (Feature B) — see StreamForge.Host.Grains.TableHistoryGrain.
// ============================================================================

/// <summary>One recorded ASSERTION version of a row-history entry: the row's content at a point in time,
/// plus a per-table monotonic sequence number (assigned from every delta the history grain observes,
/// assertion or retraction, so gaps between consecutive Seq values indicate retractions happened
/// in-between) for stable ordering.</summary>
// NOTE (005-W1): body-declared properties (plain `set`) instead of positional-record shorthand —
// see FieldDef's identical note above (ORLEANS0101 under cross-assembly codegen).
[GenerateSerializer]
public sealed record HistoryVersion
{
    [Id(0)] public Dictionary<string, object?> Row { get; set; }
    [Id(1)] public long TsMs { get; set; }
    [Id(2)] public long Seq { get; set; }

    public HistoryVersion(Dictionary<string, object?> Row, long TsMs, long Seq)
    {
        this.Row = Row;
        this.TsMs = TsMs;
        this.Seq = Seq;
    }
}

/// <summary>Retention state for one row identity (see TableHistoryGrain / TableGroupKeyExtractor for how
/// the identity key is derived). Versions holds the retained ASSERTION history per the table's configured
/// HistoryMode; RetractionCount counts every retraction (weight &lt;= 0 delta) ever observed for this key —
/// retractions are not themselves stored as versions.</summary>
[GenerateSerializer]
public sealed class RowHistoryEntry
{
    [Id(0)] public List<HistoryVersion> Versions { get; set; } = [];
    [Id(1)] public long RetractionCount { get; set; }
}

/// <summary>Result of ITableHistoryGrain.GetHistoryAsync for one row identity.</summary>
[GenerateSerializer]
public sealed class TableHistoryQueryResult
{
    [Id(0)] public List<HistoryVersion> Versions { get; set; } = [];
    [Id(1)] public long RetractionCount { get; set; }
    [Id(2)] public TableHistoryMode Mode { get; set; }
    [Id(3)] public int TotalVersions { get; set; }
    /// <summary>False when the key has never been observed (as opposed to observed-but-empty).</summary>
    [Id(4)] public bool KeyFound { get; set; }
}

/// <summary>Result of ITableHistoryGrain.GetStatsAsync.</summary>
[GenerateSerializer]
public sealed class TableHistoryStats
{
    [Id(0)] public bool Enabled { get; set; }
    [Id(1)] public TableHistoryMode Mode { get; set; }
    [Id(2)] public int KeyCount { get; set; }
    [Id(3)] public long TotalVersions { get; set; }
}

[GenerateSerializer]
public sealed class UserRecord
{
    [Id(0)] public string Username { get; set; } = "";
    [Id(1)] public string DisplayName { get; set; } = "";
    /// <summary>"Admin" | "Editor" | "Viewer".</summary>
    [Id(2)] public string Role { get; set; } = "Viewer";
    [Id(3)] public string PasswordHash { get; set; } = "";
    [Id(4)] public string PasswordSalt { get; set; } = "";
    [Id(5)] public long CreatedAtMs { get; set; }
}
