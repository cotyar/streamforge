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
/// <param name="Row">The output row.</param>
/// <param name="Weight">Signed Z-set weight.</param>
public sealed record TableDelta(EventRecord Row, long Weight)
{
    /// <summary>Plan 011 C2 — ADDITIVE (default false, so every existing producer and consumer is
    /// unchanged): true only for a retraction emitted because the table's ROW RETENTION policy evicted
    /// this row, as opposed to one emitted because an upstream input retracted it. The distinction is not
    /// cosmetic: an ordinary retraction means "this row is no longer true", and per-row-identity history
    /// records it as one more retraction against a key that may well come back; an eviction means "this
    /// table is a bounded view and has stopped carrying this row", and the honest thing for history to do
    /// is drop the key's version list with it (otherwise retention bounds the table while history keeps
    /// every key it ever saw — see StreamForge.Host.Grains.TableHistoryGrain). Consumers that do not care
    /// simply ignore it and treat the delta as the plain retraction it also is.</summary>
    public bool Retention { get; init; }
}

/// <summary>
/// Plan 011 C2 — an opt-in, per-table ROW RETENTION policy. DEFAULT OFF (<see cref="None"/>), and that
/// default is load-bearing: a table with retention is NOT the full relation its SQL describes, it is a
/// BOUNDED VIEW of that relation. Turning it on changes the table's results — rows that the SQL says
/// belong in the table are deliberately dropped, with a real retraction, once the bound is exceeded.
/// Enable it when an unbounded key space (a per-request GUID, an order id, a session id) would otherwise
/// grow the table forever; do not enable it on a table whose consumers assume completeness.
///
/// The two bounds compose (both are applied, TTL first): <paramref name="MaxRows"/> caps how many rows
/// the table retains at once; <paramref name="TtlMs"/> caps how far back in EVENT TIME a retained row may
/// be. Both are evaluated against the row's event timestamp, never the wall clock — see IRetentionScope's
/// doc comment for the determinism and the "if the input stops, event time stops" consequence.
/// </summary>
/// <param name="MaxRows">Maximum retained rows; 0 = unbounded (no row-count bound).</param>
/// <param name="TtlMs">Maximum age in event-time milliseconds, measured back from the highest event
/// timestamp the table has admitted; 0 = unbounded (no age bound).</param>
public sealed record TableRetentionPolicy(int MaxRows, long TtlMs)
{
    /// <summary>Retention off — the default for every table, and byte-for-byte the pre-011 behavior.</summary>
    public static readonly TableRetentionPolicy None = new(0, 0);

    /// <summary>True when at least one bound is set. A policy that is not enabled costs the executor
    /// nothing: no ordering index is built and the hot path keeps its original shape.</summary>
    public bool IsEnabled => MaxRows > 0 || TtlMs > 0;
}

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

    /// <summary>Plan 011 C2 — whether a <see cref="TableRetentionPolicy"/> can be honestly applied to a
    /// table with THIS plan shape. True for a plan that reads one leaf source with no joins, no set
    /// operation, no derived/CTE source and no GROUP BY/aggregate, i.e. exactly the shapes whose entire
    /// per-row state is reachable from the terminal stage (LATEST BY's per-key map, or the consolidated
    /// ledger for a plain projection).
    ///
    /// WHY THE OTHERS ARE EXCLUDED RATHER THAN "SUPPORTED, PARTIALLY". A join's ZSet indexes hold the
    /// INPUT rows of both sides; evicting an output row leaves both indexes untouched and growing, so a
    /// policy there would bound the number the console shows while bounding nothing that occupies memory
    /// — the exact theatre this feature exists to avoid. A GROUP BY is excluded for a different and
    /// stronger reason: evicting a group would drop its running aggregate state, so the next contributing
    /// delta would restart that group's SUM/COUNT from zero and silently emit a WRONG value — a bounded
    /// view is a defensible thing to offer, a wrong aggregate is not. Callers (RegistryGrain /
    /// CatalogStore) reject the combination up front with that message rather than accepting it and
    /// under-delivering.</summary>
    public bool SupportsRetention => Runtime.TableRetentionSupport.IsSupported(Compiled);
}

/// <summary>
/// Per-table runtime: Z-set (DBSP-style) incremental view maintenance. Single-threaded (called from one
/// grain); no frontier/timestamp machinery — tables are order-insensitive for the commutative operators
/// this dialect supports. Feed stream events via OnStreamEvent and upstream table deltas via
/// OnTableDelta (or OnTableDeltaBatch — see its own doc comment); all three return the deltas this table
/// itself emits downstream (retraction/assertion pairs from grouped aggregation, or straight weight-
/// passthrough for filter/project/join). Snapshot() exposes the current consolidated output (weight &gt; 0
/// rows only) for rehydration-free reads.
/// </summary>
public sealed partial class TableExecutor
{
    internal TableExecutor(TablePlan plan) => _plan = plan;
    private readonly TablePlan _plan;

    public IReadOnlyList<TableDelta> OnStreamEvent(string source, EventRecord evt) => OnStreamEventCore(source, evt);

    public IReadOnlyList<TableDelta> OnTableDelta(string table, TableDelta delta) => OnTableDeltaCore(table, delta);

    /// <summary>
    /// Wishlist #15 — the batch sibling of <see cref="OnTableDelta"/>: every element of
    /// <paramref name="deltas"/> is admitted, processed and emitted under ONE epoch, instead of a caller
    /// looping <see cref="OnTableDelta"/> once per element and getting one epoch (and one downstream
    /// publish) per element. Use this whenever the deltas being fed in are already known to have come from
    /// a SINGLE upstream epoch/batch — e.g. one upstream table's own published delta batch — so that an
    /// upstream change expressed as [retract(old), assert(new)] (a changed GROUP BY row, a changed
    /// LATEST BY key) is applied and republished atomically here too, rather than splitting into as many
    /// downstream epochs as it had elements with a wrong intermediate state observable in between (see
    /// TableGrain.OnTableDeltaBatchAsync / TableActor.ProcessTableDeltasAsync, the two Host-side callers
    /// this exists for).
    ///
    /// The returned batch is already consolidated (see the Engine-internal ConsolidateEpochOutput's doc
    /// comment) — a row this call's own admission asserted and then retracted (or the reverse) before ever
    /// leaving this table is not in it. <c>OnTableDelta(table, delta)</c> is exactly
    /// <c>OnTableDeltaBatch(table, [delta])</c>; every existing single-delta caller is unaffected.
    /// </summary>
    public IReadOnlyList<TableDelta> OnTableDeltaBatch(string table, IReadOnlyList<TableDelta> deltas) => OnTableDeltaBatchCore(table, deltas);

    public IReadOnlyDictionary<string, (EventRecord Row, long Weight)> Snapshot() => SnapshotCore();

    /// <summary>The canonical identity Snapshot() keys rows by (a deterministic serialization of the row's
    /// fields) — exposed so callers that react to individual OnStreamEvent/OnTableDelta deltas (e.g. a
    /// reverse search index kept incrementally in sync) can look a delta's row up in Snapshot() without
    /// re-deriving the same key by hand.</summary>
    public string CanonicalRowKey(EventRecord row) => Runtime.JsonText.SerializeCanonicalRow(row);

    /// <summary>Wishlist item 16's netting half — the static twin of <see cref="CanonicalRowKey"/>, added
    /// for a caller shape the instance method does not fit: something that wants to net a BATCH of
    /// already-emitted rows by the exact same row-identity rule TableExecutorImpl's own epoch consolidation
    /// uses (its private ConsolidateEpochOutput / <see cref="Runtime.ConsolidationLedger"/>, both keyed by
    /// <see cref="Runtime.JsonText.SerializeCanonicalRow"/>) but has no live <see cref="TableExecutor"/> to
    /// call <see cref="CanonicalRowKey"/> on and, more importantly, should not have to build one just to
    /// reach a function of one row's fields — constructing a TableExecutor needs a compiled
    /// <see cref="TablePlan"/>, which in turn needs the table's catalog definition and a SQL recompile, all
    /// of it dead weight for a caller that only ever sees the wire shape (StreamForge.Host's
    /// StreamBridgeService, coalescing a table's SignalR deltas per flush window, is the concrete caller —
    /// see its own doc comment on why it must not invent its own row-identity rule: this is the
    /// <see cref="Runtime.ConsolidationLedger"/> class doc's own "exposed to Host only via the public …
    /// wrapper" story, extended with the one wrapper shape that was still missing). ADDITIVE — a new static
    /// member beside the existing instance one; nothing existing changes shape. Byte-identical output to
    /// <see cref="CanonicalRowKey"/> for the same fields.</summary>
    public static string CanonicalRowKeyOf(IReadOnlyDictionary<string, object?> row) => Runtime.JsonText.SerializeCanonicalRow(row);

    /// <summary>
    /// Wishlist #14 option (a) — REAL backfill on attach. The epoch (see TableExecutorImpl.cs's own EPOCH
    /// doc paragraph) most recently allocated by <see cref="OnStreamEvent"/>/<see cref="OnTableDelta"/>/
    /// <see cref="OnTableDeltaBatch"/> — i.e. the epoch that produced whatever <see cref="Snapshot"/>
    /// currently reflects. -1 before this executor has ever admitted anything.
    ///
    /// THE CONTRACT THIS EXISTS TO SERVE: a caller (TableGrain.AttachSnapshotAsync / TableActor's Dapr
    /// mirror) reads <c>Snapshot()</c> and <c>LastEpoch</c> together, with NO <c>await</c> in between, so
    /// nothing else can have run on this single-threaded executor between the two reads — the pair is
    /// therefore an atomically-consistent (rows, epoch-as-of-those-rows) fact. A brand-new downstream table
    /// attaching to this one as a table input can then: (1) subscribe to this table's own published delta
    /// stream FIRST; (2) call this attach-snapshot pair; (3) admit the returned rows as its own initial
    /// batch (exactly like any other <see cref="OnTableDeltaBatch"/> admission — through the SAME plan, so
    /// GROUP BY/JOIN/LATEST BY state is correctly built up, not bypassed); (4) for every batch it goes on to
    /// receive from step 1's subscription (whether "received" before or after step 2's read — Orleans/Dapr
    /// both queue delivery to a single-threaded consumer, so anything from step 1 that arrives before this
    /// table finishes starting is simply processed after, not lost), apply it only if its OWN stamped epoch
    /// (see TableDeltaDto.Epoch) is STRICTLY GREATER than the LastEpoch this attach returned — anything
    /// &lt;= that epoch is, by construction, already reflected in the snapshot step 3 admitted, and
    /// re-admitting it would double the row's Z-set weight; anything strictly greater is, by the same
    /// single-threaded-monotonic-epoch argument, guaranteed not to already be in the snapshot. No gap, no
    /// double-count, regardless of exactly when between steps 1 and 2 the upstream happens to publish.
    ///
    /// Every producer this repo owns (TableGrain/TableActor's own ordinary publish path, and TableOutputGrain
    /// for a coordinator-mode/Parallelism&gt;=2 upstream — see that grain's own doc comment) stamps
    /// TableDeltaDto.Epoch from exactly this property at the moment of publishing, so the cutoff a consumer
    /// reads from one AttachSnapshotAsync call is always comparable against what it later receives on that
    /// same table's delta stream.
    /// </summary>
    public long LastEpoch => _lastEpoch;

    /// <summary>
    /// Plan 011 C2 — the eviction seam. ADDITIVE: not calling it (or passing
    /// <see cref="TableRetentionPolicy.None"/>) leaves this executor byte-for-byte the pre-011 one.
    ///
    /// WHY CONFIGURATION AND NOT AN "EvictNow()" CALL, which is the shape a host would find easier to
    /// schedule: eviction must be indistinguishable from an ordinary retraction to every consumer —
    /// downstream tables, the delta stream, SignalR, sinks, the search index, the history grain — and the
    /// only way to guarantee that for ALL of them at once is to emit the eviction retractions through the
    /// exact same return value ordinary deltas already travel through. So the policy is installed once and
    /// the evictions are appended to whatever <see cref="OnStreamEvent"/>/<see cref="OnTableDelta"/>
    /// return, after that call's own deltas. A host that already publishes what those methods return
    /// therefore stays consistent with no new plumbing, and no consumer can be forgotten by omission. It
    /// also keeps the Engine free of any clock or scheduler — the exact thing a periodic EvictNow() would
    /// have needed a host type for.
    ///
    /// Idempotent and re-callable (a table restart re-applies its definition's current policy). Throws
    /// <see cref="InvalidOperationException"/> for an ENABLED policy on a plan shape where retention
    /// cannot reclaim the real state — see <see cref="TablePlan.SupportsRetention"/> for which shapes and
    /// why refusing beats silently trimming the copy.
    /// </summary>
    public void ConfigureRetention(TableRetentionPolicy policy) => ConfigureRetentionCore(policy);
}
