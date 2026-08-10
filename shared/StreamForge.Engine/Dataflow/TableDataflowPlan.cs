using StreamForge.Engine.Planning;
using StreamForge.Engine.Runtime;
using StreamForge.Engine.Runtime.Ops;
using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Dataflow;

// ============================================================================
// Plan 003 M2 — ADDITIVE public seam. Everything in this file/region is new
// surface added on top of the frozen PublicApi.cs contract (see that file's
// header comment); nothing here changes an existing signature. This is the
// "minimal explicit seam" the M2 task calls for instead of InternalsVisibleTo:
// TablePlan.CreateDataflow(partitionCount) hands the Host a description of the
// compiled plan's stage/edge graph plus a factory for per-stage per-partition
// executors that speak only the existing public wire types (Epoch, EdgeId,
// DeltaBatch, TableDelta/EventRecord) — the Host drives grains with it without
// ever touching Expr/WorkingRow/CompiledTablePlan or parsing SQL itself.
// ============================================================================

/// <summary>One kind of node in a table plan's partitioned dataflow graph. Mirrors the M1 op inventory
/// 1:1: Ingest wraps <see cref="TableIngestOp"/>, Join wraps <see cref="TableJoinOp"/> (plain INNER/CROSS
/// equi-joins AND scalar-subquery joins — see <see cref="TableEdgeMode.Broadcast"/>) OR, for JoinKind
/// Left/Right/Full, <see cref="TableOuterJoinOp"/> (plan 008 — see JoinChainStageExecutor's op-selection
/// switch), SemiAnti wraps <see cref="TableSemiAntiOp"/>, Unnest wraps <see cref="TableUnnestOp"/>,
/// FilterProject wraps <see cref="TableFilterProjectOp"/> (WHERE, +terminal projection when ungrouped),
/// Reduce wraps <see cref="TableReduceOp"/> (GROUP BY/aggregates), LatestBy wraps
/// <see cref="TableLatestByOp"/>. No dedicated TableStageKind for outer joins — deliberately: they still
/// share every structural property (two in-edges, Left/Right roles, hash-partitioned on the join key) a
/// plain Join stage has; only the op instantiated inside JoinChainStageExecutor differs by JoinKind.</summary>
public enum TableStageKind { Ingest, Join, SemiAnti, Unnest, FilterProject, Reduce, LatestBy }

/// <summary>How a <see cref="TableEdgeDescriptor"/>'s deltas are routed from producer to consumer
/// partitions.</summary>
public enum TableEdgeMode
{
    /// <summary>Partition-preserving: producer partition p feeds consumer partition p directly, no
    /// exchange (stateless row-local operators — Unnest, FilterProject, and a Scalar/SemiAnti join's Left
    /// side, which deliberately does NOT get re-hashed by the subquery's key — see Broadcast).</summary>
    Local,
    /// <summary>Hash-partitioned by the consuming stage's key expression(s) (join key / group key / latest
    /// key), via <see cref="TableDataflowPlan.PartitionOf"/> — the DBSP "exchange" edge; both sides of a
    /// join/semi-anti feed this mode so matching keys land on the same partition ("co-partitioned").</summary>
    HashPartition,
    /// <summary>Replicated to every partition of the consuming stage, unpartitioned. Used for scalar
    /// subquery / semi-anti (IN/EXISTS) singleton sides: the residual subquery is small (often one row),
    /// so instead of re-partitioning the table's main row chain by an unrelated correlation key, every
    /// partition runs its own private nested single-partition execution of the subquery (fed by broadcast)
    /// and joins locally against it.</summary>
    Broadcast,
    /// <summary>The terminal edge: a stage's final output (ToStageId == -1) is gathered to the table's one
    /// output publisher (see plan 003 M2's terminal-publisher choice, documented on the Host side —
    /// TableOutputGrain) rather than routed to another stage.</summary>
    Gather,
}

/// <summary>One directed edge in a table's partitioned dataflow graph. <paramref name="FromStageId"/> ==
/// -1 means the edge originates OUTSIDE the graph — a real external input (a stream source or an upstream
/// table's delta stream), named in <see cref="ExternalInputNames"/>, that a Host-side ingest grain
/// subscribes to and routes in. <paramref name="ToStageId"/> == -1 means the edge is the table's terminal
/// output (see <see cref="TableEdgeMode.Gather"/>).</summary>
/// <param name="ArrangeKeyFields">Plan 003 M3 — non-null ONLY for an external-input (FromStageId == -1),
/// HashPartition-mode edge whose key is a bare reference to the raw input's OWN field(s) with no pre-join
/// transform (see TableDataflowBuilder's arrangeability check) — the raw field name(s), in order, that
/// TableGrain's coordinator uses to attach a shared ArrangementGrain instead of deploying a private
/// TableIngestGrain for this edge. Null for every other edge (including a non-arrangeable external-input
/// edge, which still gets a private TableIngestGrain exactly as before M3 — see TableDataflowBuilder's class
/// doc for the exact arrangeable-vs-not rule). Additive/optional so every pre-M3 positional construction of
/// this record keeps compiling unchanged.</param>
public sealed record TableEdgeDescriptor(
    EdgeId EdgeId,
    int FromStageId,
    int ToStageId,
    string Role,
    TableEdgeMode Mode,
    IReadOnlyList<string> ExternalInputNames,
    IReadOnlyList<string>? ArrangeKeyFields = null);

/// <summary>One node in a table's partitioned dataflow graph. <see cref="Alias"/> is the join/ingest
/// alias this stage plays (empty for the single-per-plan FilterProject/Reduce/LatestBy stages, which are
/// not per-alias). <see cref="InEdges"/> lists every inbound edge (1 for Ingest/FilterProject/Unnest/
/// Reduce/LatestBy, 2 — "Left"+"Right" — for Join/SemiAnti). An Ingest stage always has partition count 1
/// (one instance per external input — see <see cref="TableDataflowPlan.PartitionCountOf"/>); every other
/// stage runs at the plan's full partition count.</summary>
public sealed record TableStageDescriptor(
    int StageId,
    TableStageKind Kind,
    string Alias,
    IReadOnlyList<TableEdgeDescriptor> InEdges);

/// <summary>A batch of deltas a stage executor emitted on one outbound edge, still needing the Host to
/// route it (hash/broadcast/local/gather) to the target partition(s) — see
/// <see cref="TableDataflowPlan.PartitionOf"/>.</summary>
public readonly record struct TableStageOutput(TableEdgeDescriptor OutEdge, IReadOnlyList<TableDelta> Deltas);

/// <summary>Per-stage, per-partition operator instance — the M2 "factory for per-stage per-partition
/// executor instances operating on DeltaBatch/Epoch" the task calls for. A fresh instance (with its own
/// private op state — ZSetIndex, group table, etc.) is created per (stage, partition) via
/// <see cref="TableDataflowPlan.CreateStageExecutor"/>; nothing is shared across partitions.
///
/// Table-mode ops are order-insensitive per-delta (see TableExecutorImpl's class doc: single-partition
/// composition reproduces the monolith bit-for-bit regardless of internal call order) — a Host grain is
/// expected to buffer inbound batches per epoch (EpochBuffer) and feed them to <see cref="OnBatch"/> in
/// the deterministic order <see cref="EpochBuffer.OnFrontier"/> returns, then call
/// <see cref="OnFrontier"/> once its own frontier (FrontierTracker) advances — this is what makes the
/// M2 determinism guarantee (same batches, any arrival order ⇒ same consolidated output) hold across
/// partitions, not just within one.</summary>
public interface ITableStageExecutor
{
    int StageId { get; }
    int Partition { get; }

    /// <summary>Feed one inbound batch (already known to belong to edge <paramref name="inEdge"/>) at
    /// <paramref name="epoch"/>. <paramref name="originName"/> is the real external input name the batch
    /// ultimately came from — only meaningful for a Broadcast-mode edge feeding a scalar-subquery/semi-anti
    /// join's nested residual execution (mirrors TableExecutor.OnTableDelta's own "which name" dispatch);
    /// ignored otherwise. Returns zero or more (outbound edge, deltas) pairs still needing routing.</summary>
    IReadOnlyList<TableStageOutput> OnBatch(EdgeId inEdge, string originName, Epoch epoch, IReadOnlyList<TableDelta> deltas);

    /// <summary>Frontier-advance hook — see M1's op doc comments: every table-mode op's OnFrontier is a
    /// pass-through today (no epoch-driven eviction in table mode), proven live via per-op unit tests
    /// rather than the hot path. Wired here so M4 (frontier-consistent reads / EMIT FINAL table variants)
    /// has somewhere to hang real behavior without another public-surface change.</summary>
    IReadOnlyList<TableStageOutput> OnFrontier(Epoch epoch);
}

/// <summary>
/// The compiled table plan's partitioned dataflow graph (plan 003 M2's engine seam): stages + edges +
/// routing, and a factory for per-stage per-partition executors. Obtain via
/// <see cref="TablePlan.CreateDataflow"/>. Immutable and re-derivable from the same TablePlan any number
/// of times (Registry restarts a table on a Parallelism change exactly by calling this again).
///
/// SCOPE (M2, extended by plan 008): every table plan built from Sources[0] (FROM) directly and JOIN
/// aliases that are either (a) a plain real stream/table alias — INNER, CROSS, LEFT, RIGHT, or FULL
/// equi-join (plan 008 lifted the table-mode outer-join validator gate; see TableJoinOp's and
/// TableOuterJoinOp's class docs for which op a non-derived source's JoinKind maps to) or (b) a
/// derived/residual subquery join (Scalar, Semi, Anti — IN/EXISTS/scalar-subquery predicates, always
/// compiled with a DerivedPlan — see TablePlanner.BuildScalarJoin/BuildSemiAntiJoin) is supported. A
/// derived table/CTE named directly in FROM or JOIN position (plan 004 N1, CompiledTableSource.DerivedPlan
/// / a non-Scalar/SemiAnti CompiledTableJoin.DerivedPlan) is NOT supported for Parallelism &gt; 1 in M2 —
/// CreateDataflow throws <see cref="NotSupportedException"/> for those plans; Parallelism stays pinned to
/// 1 (the existing single-grain TableGrain path, unaffected) for such tables. LEFT/RIGHT/FULL need no
/// extra restriction beyond that: a non-derived, non-CROSS join's Left AND Right edges are already
/// unconditionally HashPartition-mode (see TableDataflowBuilder) — the composite-key-aware routing
/// (RoutingKeySpec.KeyExprs, now the join's FULL key list rather than a single expression) is what keeps
/// matching rows co-partitioned, and hence TableOuterJoinOp's per-partition flip state correct, at
/// Parallelism &gt;= 2. See the M2 report's descope list.
/// </summary>
public sealed class TableDataflowPlan
{
    private readonly CompiledTablePlan _compiled;
    private readonly List<TableStageDescriptor> _stages;
    private readonly List<TableEdgeDescriptor> _edges;
    private readonly Dictionary<int, StageBuild> _stageBuilds;
    private readonly Dictionary<int, RoutingKeySpec> _routingSpecs; // by EdgeId.Value, HashPartition edges only

    internal TableDataflowPlan(CompiledTablePlan compiled, int partitionCount)
    {
        if (partitionCount < 1) throw new ArgumentOutOfRangeException(nameof(partitionCount), "Partition count must be >= 1.");
        _compiled = compiled;
        PartitionCount = partitionCount;
        (_stages, _edges, _stageBuilds, _routingSpecs) = TableDataflowBuilder.Build(compiled, partitionCount);
        TerminalEdge = _edges.Single(e => e.ToStageId == -1);
    }

    public int PartitionCount { get; }

    public IReadOnlyList<TableStageDescriptor> Stages => _stages;

    /// <summary>Every edge in the graph, flattened — includes each stage's inbound edges (also reachable
    /// via <see cref="TableStageDescriptor.InEdges"/>) plus the one terminal edge (<see cref="TerminalEdge"/>).</summary>
    public IReadOnlyList<TableEdgeDescriptor> Edges => _edges;

    /// <summary>The single edge whose ToStageId == -1 — this table's output, gathered to the table's own
    /// delta-stream publisher (see the M2 report's terminal-publisher choice).</summary>
    public TableEdgeDescriptor TerminalEdge { get; }

    public IReadOnlyList<string> StreamInputs => _compiled.StreamInputs;
    public IReadOnlyList<string> TableInputs => _compiled.TableInputs;

    /// <summary>Every edge a real external input (stream source or upstream table) feeds — an input can
    /// feed more than one edge (e.g. a self-join, or a name used both by the main FROM/JOIN chain and by a
    /// scalar-subquery/semi-anti join elsewhere in the same plan). A Host-side ingest grain for
    /// <paramref name="inputName"/> routes its admitted batch to every edge this returns.</summary>
    public IReadOnlyList<TableEdgeDescriptor> EdgesForExternalInput(string inputName) =>
        _edges.Where(e => e.FromStageId == -1 && e.ExternalInputNames.Contains(inputName)).ToList();

    /// <summary>Every outbound edge of a stage (FromStageId == stageId) — exactly one per stage in M2's
    /// graph shape (each op has one forward path; fan-out to multiple destinations for the same real input
    /// happens at the Ingest/external level via <see cref="EdgesForExternalInput"/>, not mid-graph).</summary>
    public TableEdgeDescriptor OutEdgeOf(int stageId) => _edges.Single(e => e.FromStageId == stageId);

    /// <summary>Plan 003 M3: every edge TableDataflowBuilder marked arrangeable (see
    /// <see cref="TableEdgeDescriptor.ArrangeKeyFields"/> and TableDataflowBuilder's class doc for the exact
    /// rule) — the set of edges TableGrain's coordinator attaches a shared ArrangementGrain to instead of
    /// deploying/routing through a private TableIngestGrain. By construction every such edge originates from
    /// an Ingest-kind stage (its FromStageId is that stage's id, NOT -1 — mirrors TableIngestGrain's own "the
    /// REAL routing decision is the Ingest stage's own outbound edge" pattern) — use
    /// <see cref="ExternalInputNameOf"/> to recover the real source/table name it ultimately reads from.</summary>
    public IReadOnlyList<TableEdgeDescriptor> ArrangeableExternalEdges =>
        _edges.Where(e => e.ArrangeKeyFields is not null).ToList();

    /// <summary>The real external input name (stream source or upstream table) an arrangeable edge
    /// ultimately reads from — recovered from its producing Ingest-kind stage's own single inbound edge
    /// (FromStageId == -1, ExternalInputNames singleton — see TableDataflowBuilder's class doc on Ingest
    /// stage shape). Throws if <paramref name="edge"/> isn't arrangeable.</summary>
    public string ExternalInputNameOf(TableEdgeDescriptor edge)
    {
        if (edge.ArrangeKeyFields is null)
            throw new InvalidOperationException($"Edge {edge.EdgeId} is not arrangeable.");
        var ingestStage = _stages.First(s => s.StageId == edge.FromStageId);
        return ingestStage.InEdges[0].ExternalInputNames[0];
    }

    /// <summary>Canonical key-spec string for an arrangeable edge (see <see cref="ArrangementKeySpec"/>) —
    /// throws if <paramref name="edge"/> isn't arrangeable. Bakes in the CONSUMING stage's partition count
    /// (<see cref="PartitionCountOf"/> of the edge's target stage) so two tables only ever share an
    /// arrangement when both the raw key field(s) AND the partition count match (an arrangement's partition
    /// p is exchanged 1:1, unre-hashed, into its consuming join stage's own partition p — see this class's
    /// doc — so a different partition count is a genuinely different physical index).</summary>
    public string KeySpecOf(TableEdgeDescriptor edge)
    {
        if (edge.ArrangeKeyFields is null)
            throw new InvalidOperationException($"Edge {edge.EdgeId} is not arrangeable; it has no ArrangeKeyFields.");
        return ArrangementKeySpec.Canonicalize(edge.ArrangeKeyFields, PartitionCountOf(edge.ToStageId));
    }

    /// <summary>Partition count a given stage runs at: 1 for Ingest (one instance per external input —
    /// routing/fan-out only, no per-partition state), <see cref="PartitionCount"/> for everything else.</summary>
    public int PartitionCountOf(int stageId) => _stageBuilds[stageId].Kind == TableStageKind.Ingest ? 1 : PartitionCount;

    /// <summary>Computes which partition of the edge's TARGET stage <paramref name="row"/> routes to, for a
    /// HashPartition-mode edge — the row-key-extraction seam the M2 task requires ("the engine knows the
    /// exprs; Host must not parse SQL"). Throws <see cref="InvalidOperationException"/> for a non-
    /// HashPartition edge (Local/Broadcast/Gather routing needs no key).</summary>
    public int PartitionOf(EdgeId edgeId, EventRecord row)
    {
        if (!_routingSpecs.TryGetValue(edgeId.Value, out var spec))
            throw new InvalidOperationException($"Edge {edgeId} is not hash-partitioned; only HashPartition-mode edges have a routing key.");

        string canonical;
        if (spec.UseRowContentHash)
        {
            // No natural join/group key applies at this hop (first fan-out out of a 1-partition Ingest
            // stage into a P-partition stateless stage, e.g. no JOIN at all, or Unnest/Scalar/SemiAnti at
            // chain position 0) — spread load across partitions by the row's own content instead. Safe
            // because the receiving stage is either purely row-local (no cross-row state to co-locate) or
            // itself feeds a properly key-hashed edge further downstream (Reduce/LatestBy), so which
            // partition a given row lands on here doesn't affect correctness, only balance.
            canonical = JsonText.SerializeCanonicalRow(row);
        }
        else
        {
            var wr = spec.IsWireEncoded ? WorkingRowWireCodec.FromWire(row) : WorkingRow.FromEvent(spec.Alias!, row);
            var ctx = new EvalContext(wr, spec.Bindings!);
            var values = spec.KeyExprs!.Select(e => ExpressionEvaluator.Eval(e, ctx)).ToArray();
            canonical = TableKeyEncoding.EncodeGroupKey(values);
        }
        return ExchangeRouter.PartitionOf(canonical, PartitionCountOf(spec.ToStageId));
    }

    /// <summary>Builds a fresh per-(stage, partition) executor — its own private op state, shared with
    /// nothing else. Call once per (table, stage, partition) grain activation.</summary>
    public ITableStageExecutor CreateStageExecutor(int stageId, int partition)
    {
        var build = _stageBuilds[stageId];
        int pcount = PartitionCountOf(stageId);
        if (partition < 0 || partition >= pcount)
            throw new ArgumentOutOfRangeException(nameof(partition), $"Stage {stageId} runs at partition count {pcount}.");
        return TableDataflowBuilder.CreateExecutor(build, partition);
    }

    internal sealed class StageBuild
    {
        public required TableStageKind Kind;
        public required TableStageDescriptor Stage;
        public required TableEdgeDescriptor OutEdge;

        // Join / SemiAnti / Unnest
        public CompiledTableJoin? Join;
        public EdgeId? LeftEdge;
        public bool LeftIsWire;
        public string? LeftAlias;
        public EdgeId? RightEdge;
        public string? RightAlias;
        public CompiledTablePlan? RightDerivedPlan; // set => broadcast + nested TableExecutor on the right

        // Join only, JoinKind Left/Right/Full (plan 008): every (alias, schema) accumulated on the left so
        // far, and this join's own (alias, schema) on the right — TableOuterJoinOp's constructor needs both
        // to build its all-NULL left/right pad rows, exactly like TableExecutorImpl.EnsureInit's single-
        // partition `accumulated` list. Null for every other stage (populated only alongside Join/SemiAnti's
        // non-Unnest branch in TableDataflowBuilder.Build).
        public IReadOnlyList<(string Alias, SourceSchema Schema)>? LeftAliasesSoFar;
        public (string Alias, SourceSchema Schema)? RightSide;

        // Ingest / FilterProject / Reduce / LatestBy (single input)
        public EdgeId? InEdge;
        public bool InIsWire;
        public string? InAlias;
        public bool Terminal; // FilterProject only: true => OnBatchTerminal path

        public required CompiledTablePlan Compiled;
        public List<Expr>? ReduceOrLatestKeys; // Reduce: null (GroupBy read straight off Compiled); LatestBy: the LATEST BY key list
    }

    internal sealed class RoutingKeySpec
    {
        public required bool IsWireEncoded;
        public required bool UseRowContentHash;
        public string? Alias;
        public List<Expr>? KeyExprs;
        public Dictionary<Expr, (string Alias, string Field)>? Bindings;
        public required int ToStageId;
    }
}
