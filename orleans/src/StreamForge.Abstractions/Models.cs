namespace StreamForge.Abstractions;

[GenerateSerializer]
public enum FieldType { String, Double, Long, Bool, Timestamp, Json }

/// <summary>A source field. <see cref="Children"/> is the declared nested shape of a
/// <see cref="FieldType.Json"/> field (drill-down schema) — metadata that documents the payload,
/// drives synthetic generation for the "generic" profile, and feeds editor autocomplete. Null/empty
/// for scalar fields.</summary>
[GenerateSerializer]
public sealed record FieldDef(
    [property: Id(0)] string Name,
    [property: Id(1)] FieldType Type,
    [property: Id(2)] List<FieldDef>? Children = null);

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
