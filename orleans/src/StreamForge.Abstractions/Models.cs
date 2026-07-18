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
[GenerateSerializer]
public sealed record FieldDef(
    [property: Id(0)] string Name,
    [property: Id(1)] FieldType Type,
    [property: Id(2)] List<FieldDef>? Children = null,
    [property: Id(3)] bool IsArray = false);

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
}

// ============================================================================
// Row history (Feature B) — see StreamForge.Host.Grains.TableHistoryGrain.
// ============================================================================

/// <summary>One recorded ASSERTION version of a row-history entry: the row's content at a point in time,
/// plus a per-table monotonic sequence number (assigned from every delta the history grain observes,
/// assertion or retraction, so gaps between consecutive Seq values indicate retractions happened
/// in-between) for stable ordering.</summary>
[GenerateSerializer]
public sealed record HistoryVersion(
    [property: Id(0)] Dictionary<string, object?> Row,
    [property: Id(1)] long TsMs,
    [property: Id(2)] long Seq);

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
