namespace StreamForge.Engine;

// ============================================================================
// FROZEN PUBLIC CONTRACT — the Host and tests compile against exactly this.
// Implementations live in Sql/, Planning/, Runtime/. Do not change signatures.
// ============================================================================

/// <summary>A stream event / result row. Reserved keys: "_ts" (epoch ms long), "_source" (string).</summary>
public sealed class EventRecord : Dictionary<string, object?>
{
    public const string TimestampField = "_ts";
    public const string SourceField = "_source";

    public EventRecord() { }
    public EventRecord(IDictionary<string, object?> other) : base(other) { }

    public long Timestamp => this.TryGetValue(TimestampField, out var v) && v is long l ? l : 0L;
    public string Source => this.TryGetValue(SourceField, out var v) && v is string s ? s : "";
}

public enum DiagnosticSeverity { Error, Warning }

public sealed record SqlDiagnostic(string Message, int Line, int Column, DiagnosticSeverity Severity = DiagnosticSeverity.Error);

/// <summary>Json: the field may hold a nested JSON-like structure — Dictionary&lt;string, object?&gt; and
/// List&lt;object?&gt; nodes with primitive (string/double/long/bool/null) leaves. Accessed via the SQL
/// dialect's Postgres-style '->'/'->>' operators (see Sql/Ast.cs's JsonAccessExpr).</summary>
public enum FieldKind { String, Double, Long, Bool, Timestamp, Json }

/// <summary>Schema of a source stream, as the validator sees it.</summary>
public sealed record SourceSchema(string Name, IReadOnlyDictionary<string, FieldKind> Fields);

/// <summary>Outcome of compiling a streaming-SQL query.</summary>
public sealed class CompileResult
{
    public bool Ok { get; init; }
    public IReadOnlyList<SqlDiagnostic> Diagnostics { get; init; } = [];
    /// <summary>Human-readable one-line plan description, e.g. "trades ⋈ quotes WITHIN 5s → WHERE → TUMBLING(5s) GROUP BY symbol → SELECT 3 cols".</summary>
    public string? PlanSummary { get; init; }
    /// <summary>Source stream names the query subscribes to. Empty when !Ok.</summary>
    public IReadOnlyList<string> SourceNames { get; init; } = [];
    /// <summary>Column names + kinds of the pipeline's output row, derived from the projection —
    /// mirrors TableCompileResult.OutputSchema. Null when !Ok.</summary>
    public SourceSchema? OutputSchema { get; init; }
    /// <summary>Executable plan; null when !Ok. Call CreateExecutor() per running pipeline.</summary>
    public PipelinePlan? Plan { get; init; }
}

/// <summary>Compiled, immutable query plan. Thread-safe to share; executors hold the mutable state.</summary>
public sealed partial class PipelinePlan
{
    public PipelineExecutor CreateExecutor() => new(this);
}

/// <summary>
/// Per-pipeline runtime. Single-threaded (called from one grain).
/// Feed events via OnEvent; advance the watermark periodically via AdvanceWatermark
/// (both may emit rows: matches, EMIT CHANGES updates, or closed windows).
/// </summary>
public sealed partial class PipelineExecutor
{
    internal PipelineExecutor(PipelinePlan plan) => _plan = plan;
    private readonly PipelinePlan _plan;

    public long Watermark { get; private set; }

    public IReadOnlyList<EventRecord> OnEvent(string sourceName, EventRecord evt) => OnEventCore(sourceName, evt);

    /// <summary>Advance watermark to max(current, nowMs - allowed lateness); closes due windows, evicts join buffers.</summary>
    public IReadOnlyList<EventRecord> AdvanceWatermark(long nowMs) => AdvanceWatermarkCore(nowMs);
}

// ============================================================================
// TABLES — additive: persistent materialized tables (Z-set / DBSP-style incremental
// view maintenance) layered on the same SQL dialect used for stream pipelines.
// A table's SQL is a SELECT over streams AND/OR other tables, WITHOUT windows —
// GROUP BY + aggregates are running (unwindowed) aggregates that emit
// retraction/assertion pairs as groups change.
// ============================================================================

/// <summary>Outcome of compiling table-mode SQL.</summary>
public sealed class TableCompileResult
{
    public bool Ok { get; init; }
    public IReadOnlyList<SqlDiagnostic> Diagnostics { get; init; } = [];
    /// <summary>Human-readable one-line plan description, analogous to CompileResult.PlanSummary.</summary>
    public string? PlanSummary { get; init; }
    /// <summary>Stream source names this table subscribes to directly. Empty when !Ok.</summary>
    public IReadOnlyList<string> StreamInputs { get; init; } = [];
    /// <summary>Other table names this table subscribes to (table-over-table chaining). Empty when !Ok.</summary>
    public IReadOnlyList<string> TableInputs { get; init; } = [];
    /// <summary>Column names + kinds of the table's output row, derived from the projection — used to
    /// validate downstream tables that reference this one. Null when !Ok.</summary>
    public SourceSchema? OutputSchema { get; init; }
    /// <summary>Executable plan; null when !Ok. Call CreateExecutor() per running table.</summary>
    public TablePlan? Plan { get; init; }
}

/// <summary>One Z-set delta: a row entering (+1) or leaving (-1) a table's output. Weights other than
/// ±1 arise from join fan-out (weight multiplication) or upstream retraction cascades.</summary>
public sealed record TableDelta(EventRecord Row, long Weight);

public static class SqlCompiler
{
    /// <summary>Tokenize → parse → validate → plan. Never throws on bad SQL; returns diagnostics.</summary>
    public static CompileResult Compile(string sql, IReadOnlyDictionary<string, SourceSchema> schemas)
        => Planning.Planner.Compile(sql, schemas);

    /// <summary>Tokenize → parse → validate (table-mode grammar) → plan a materialized TABLE's SELECT.
    /// streamSchemas and tableSchemas together form the SQL namespace a FROM/JOIN identifier resolves
    /// against; a name present in both is an "ambiguous name" diagnostic. Never throws.</summary>
    public static TableCompileResult CompileTable(
        string sql,
        IReadOnlyDictionary<string, SourceSchema> streamSchemas,
        IReadOnlyDictionary<string, SourceSchema> tableSchemas)
        => Planning.TablePlanner.Compile(sql, streamSchemas, tableSchemas);
}

/// <summary>Compiled, immutable table plan. Thread-safe to share; executors hold the mutable state.</summary>
public sealed partial class TablePlan
{
    public TableExecutor CreateExecutor() => new(this);
}

/// <summary>
/// Per-table runtime: Z-set (DBSP-style) incremental view maintenance. Single-threaded (called from one
/// grain); no frontier/timestamp machinery — tables are order-insensitive for the commutative operators
/// this dialect supports. Feed stream events via OnStreamEvent and upstream table deltas via
/// OnTableDelta; both return the deltas this table itself emits downstream (retraction/assertion pairs
/// from grouped aggregation, or straight weight-passthrough for filter/project/join). Snapshot() exposes
/// the current consolidated output (weight &gt; 0 rows only) for rehydration-free reads.
/// </summary>
public sealed partial class TableExecutor
{
    internal TableExecutor(TablePlan plan) => _plan = plan;
    private readonly TablePlan _plan;

    public IReadOnlyList<TableDelta> OnStreamEvent(string source, EventRecord evt) => OnStreamEventCore(source, evt);

    public IReadOnlyList<TableDelta> OnTableDelta(string table, TableDelta delta) => OnTableDeltaCore(table, delta);

    public IReadOnlyDictionary<string, (EventRecord Row, long Weight)> Snapshot() => SnapshotCore();

    /// <summary>The canonical identity Snapshot() keys rows by (a deterministic serialization of the row's
    /// fields) — exposed so callers that react to individual OnStreamEvent/OnTableDelta deltas (e.g. a
    /// reverse search index kept incrementally in sync) can look a delta's row up in Snapshot() without
    /// re-deriving the same key by hand.</summary>
    public string CanonicalRowKey(EventRecord row) => Runtime.JsonText.SerializeCanonicalRow(row);
}
