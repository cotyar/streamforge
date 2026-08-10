using StreamForge.Engine.Runtime;
using StreamForge.Engine.Runtime.Ops;
using StreamForge.Engine.Sql;
using static StreamForge.Engine.Dataflow.TableDataflowPlan;

namespace StreamForge.Engine.Dataflow;

/// <summary>Shared plumbing for every M2 per-(stage,partition) executor: knows its stage id/partition and
/// its one outbound edge (see TableDataflowPlan's class doc: every stage in M2's graph shape has exactly
/// one forward edge).</summary>
internal abstract class TableStageExecutorBase : ITableStageExecutor
{
    protected TableStageExecutorBase(StageBuild build, int partition)
    {
        Build = build;
        Partition = partition;
    }

    protected readonly StageBuild Build;
    public int StageId => Build.Stage.StageId;
    public int Partition { get; }

    public abstract IReadOnlyList<TableStageOutput> OnBatch(EdgeId inEdge, string originName, Epoch epoch, IReadOnlyList<TableDelta> deltas);

    /// <summary>Every table-mode op's OnFrontier is a documented pass-through today (see M1's op doc
    /// comments) — table mode has no epoch-driven eviction. Wired here, not on the hot path, exactly
    /// mirroring TableExecutorImpl's own façade choice.</summary>
    public virtual IReadOnlyList<TableStageOutput> OnFrontier(Epoch epoch) => [];

    protected IReadOnlyList<TableStageOutput> Single(IReadOnlyList<TableDelta> deltas) =>
        deltas.Count == 0 ? [] : [new TableStageOutput(Build.OutEdge, deltas)];
}

/// <summary>Ingest stage: routing-only, no state (mirrors TableIngestOp's own "STATE: none" — the actual
/// alias-tagging WorkingRow.FromEvent conversion happens on the RECEIVING join/filter-project stage, which
/// is the one that knows which role this edge plays; TableDelta/EventRecord is already the correct public
/// wire shape, so an ingest stage's own job is purely "forward on my one outbound edge").</summary>
internal sealed class IngestStageExecutor(StageBuild build, int partition) : TableStageExecutorBase(build, partition)
{
    public override IReadOnlyList<TableStageOutput> OnBatch(EdgeId inEdge, string originName, Epoch epoch, IReadOnlyList<TableDelta> deltas) => Single(deltas);
}

/// <summary>Join / SemiAnti / Unnest stage: wraps the corresponding ITableJoinStage op (TableJoinOp /
/// TableSemiAntiOp / TableUnnestOp — all three share the OnLeftBatch/OnRightBatch shape). For a plain
/// (non-derived) join, Left/Right both admit a raw external EventRecord tagged with the appropriate alias;
/// for a derived (Scalar/Semi/Anti) join, the Right edge is broadcast and its raw external deltas are first
/// run through a private nested TableExecutor (one per partition — see TableDataflowPlan's class doc on
/// why broadcast+redundant-nested-execution is correct for a singleton/small residual subquery) before
/// admission, exactly mirroring TableExecutorImpl.AddRole's "role.Derived.OnTableDelta(name, ...)" path.
/// Unnest has no Right edge at all (see TableUnnestOp's class doc: OnRightBatch is dead code there).</summary>
internal sealed class JoinChainStageExecutor : TableStageExecutorBase
{
    private readonly ITableJoinStage _op;
    private readonly TableExecutor? _rightNested;

    public JoinChainStageExecutor(StageBuild build, int partition) : base(build, partition)
    {
        var j = build.Join!;
        // Plan 008: a Join-kind stage build (TableStageKind.Join) covers Inner/Cross/Scalar AND, now,
        // Left/Right/Full — the coarse stage kind doesn't distinguish them (see TableDataflowPlan's
        // TableStageKind doc), so the outer-kind check is on j.Kind (the actual JoinKind) directly.
        // TableOuterJoinOp reads the full LeftKeys/RightKeys composite key lists plus build's accumulated
        // alias/schema bookkeeping (populated in TableDataflowBuilder.Build); every other kind is
        // unchanged from pre-008 — single-key LeftKey/RightKey (component [0], residual already folded).
        _op = build.Kind switch
        {
            TableStageKind.SemiAnti => new TableSemiAntiOp(j.Kind, j.LeftKey!, j.RightKey!, build.Compiled.Bindings),
            TableStageKind.Unnest => new TableUnnestOp(j.UnnestExpr!, j.Alias, build.Compiled.Bindings),
            TableStageKind.Join when j.Kind is JoinKind.Left or JoinKind.Right or JoinKind.Full =>
                new TableOuterJoinOp(j.Kind, j.LeftKeys!, j.RightKeys!, j.Residual, build.Compiled.Bindings, build.LeftAliasesSoFar!, build.RightSide!.Value),
            _ => new TableJoinOp(j.LeftKey!, j.RightKey!, j.Residual, build.Compiled.Bindings),
        };
        _rightNested = build.RightDerivedPlan is not null ? new TablePlan(build.RightDerivedPlan).CreateExecutor() : null;
    }

    public override IReadOnlyList<TableStageOutput> OnBatch(EdgeId inEdge, string originName, Epoch epoch, IReadOnlyList<TableDelta> deltas)
    {
        IReadOnlyList<TableRowDelta> outRows;

        if (Build.LeftEdge is { } leftEdge && inEdge == leftEdge)
        {
            var rows = ToRowDeltas(deltas, Build.LeftIsWire, Build.LeftAlias);
            outRows = _op.OnLeftBatch(epoch, rows);
        }
        else if (Build.RightEdge is { } rightEdge && inEdge == rightEdge)
        {
            IReadOnlyList<TableRowDelta> rightRows;
            if (_rightNested is not null)
            {
                var nestedOut = new List<TableDelta>();
                foreach (var d in deltas) nestedOut.AddRange(_rightNested.OnTableDelta(originName, d));
                rightRows = ToRowDeltas(nestedOut, isWire: false, Build.RightAlias);
            }
            else
            {
                rightRows = ToRowDeltas(deltas, isWire: false, Build.RightAlias);
            }
            outRows = _op.OnRightBatch(epoch, rightRows);
        }
        else
        {
            throw new InvalidOperationException($"Stage {StageId} (partition {Partition}) received a batch on unrecognized edge {inEdge}.");
        }

        if (outRows.Count == 0) return [];
        var wire = new List<TableDelta>(outRows.Count);
        foreach (var r in outRows) wire.Add(new TableDelta(WorkingRowWireCodec.ToWire(r.Row), r.Weight));
        return Single(wire);
    }

    private static List<TableRowDelta> ToRowDeltas(IReadOnlyList<TableDelta> deltas, bool isWire, string? alias)
    {
        var results = new List<TableRowDelta>(deltas.Count);
        foreach (var d in deltas)
        {
            var wr = isWire ? WorkingRowWireCodec.FromWire(d.Row) : WorkingRow.FromEvent(alias!, d.Row);
            results.Add(new TableRowDelta(wr, d.Weight));
        }
        return results;
    }
}

/// <summary>FilterProject stage: WHERE (+terminal projection when ungrouped). Stateless — see
/// TableFilterProjectOp's class doc.</summary>
internal sealed class FilterProjectStageExecutor : TableStageExecutorBase
{
    private readonly TableFilterProjectOp _op;

    public FilterProjectStageExecutor(StageBuild build, int partition) : base(build, partition)
    {
        _op = new TableFilterProjectOp(build.Compiled);
    }

    public override IReadOnlyList<TableStageOutput> OnBatch(EdgeId inEdge, string originName, Epoch epoch, IReadOnlyList<TableDelta> deltas)
    {
        var rows = new List<TableRowDelta>(deltas.Count);
        foreach (var d in deltas)
        {
            var wr = Build.InIsWire ? WorkingRowWireCodec.FromWire(d.Row) : WorkingRow.FromEvent(Build.InAlias!, d.Row);
            rows.Add(new TableRowDelta(wr, d.Weight));
        }

        if (Build.Terminal)
        {
            var outDeltas = _op.OnBatchTerminal(epoch, rows);
            return Single(outDeltas);
        }

        var outRows = _op.OnBatch(epoch, rows);
        if (outRows.Count == 0) return [];
        var wire = new List<TableDelta>(outRows.Count);
        foreach (var r in outRows) wire.Add(new TableDelta(WorkingRowWireCodec.ToWire(r.Row), r.Weight));
        return Single(wire);
    }
}

/// <summary>Reduce stage: running GROUP BY aggregates, partitioned on the group key (see
/// TableReduceOp's class doc). Terminal — emits the table's final output TableDeltas directly.</summary>
internal sealed class ReduceStageExecutor : TableStageExecutorBase
{
    private readonly TableReduceOp _op;

    public ReduceStageExecutor(StageBuild build, int partition) : base(build, partition)
    {
        _op = new TableReduceOp(build.Compiled);
    }

    public override IReadOnlyList<TableStageOutput> OnBatch(EdgeId inEdge, string originName, Epoch epoch, IReadOnlyList<TableDelta> deltas)
    {
        var rows = new List<TableRowDelta>(deltas.Count);
        foreach (var d in deltas) rows.Add(new TableRowDelta(WorkingRowWireCodec.FromWire(d.Row), d.Weight));
        return Single(_op.OnBatch(epoch, rows));
    }
}

/// <summary>LatestBy stage: argmax-by-timestamp per key, partitioned on the latest-by key (see
/// TableLatestByOp's class doc). Terminal — mutually exclusive with Reduce by construction.</summary>
internal sealed class LatestByStageExecutor : TableStageExecutorBase
{
    private readonly TableLatestByOp _op;

    public LatestByStageExecutor(StageBuild build, int partition) : base(build, partition)
    {
        _op = new TableLatestByOp(build.Compiled, build.ReduceOrLatestKeys!);
    }

    public override IReadOnlyList<TableStageOutput> OnBatch(EdgeId inEdge, string originName, Epoch epoch, IReadOnlyList<TableDelta> deltas)
    {
        var rows = new List<TableRowDelta>(deltas.Count);
        foreach (var d in deltas) rows.Add(new TableRowDelta(WorkingRowWireCodec.FromWire(d.Row), d.Weight));
        return Single(_op.OnBatch(epoch, rows));
    }
}
