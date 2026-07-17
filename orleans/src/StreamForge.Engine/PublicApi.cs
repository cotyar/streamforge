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

public enum FieldKind { String, Double, Long, Bool, Timestamp }

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
    /// <summary>Executable plan; null when !Ok. Call CreateExecutor() per running pipeline.</summary>
    public PipelinePlan? Plan { get; init; }
}

public static class SqlCompiler
{
    /// <summary>Tokenize → parse → validate → plan. Never throws on bad SQL; returns diagnostics.</summary>
    public static CompileResult Compile(string sql, IReadOnlyDictionary<string, SourceSchema> schemas)
        => Planning.Planner.Compile(sql, schemas);
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
