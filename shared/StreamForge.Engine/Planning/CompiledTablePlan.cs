using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Planning;

internal sealed class CompiledTableSource
{
    public required string Alias { get; init; }
    public required string SourceName { get; init; }
    public required SourceSchema Schema { get; init; }
    public required bool IsTable { get; init; }
    /// <summary>Plan 004 N1: set when this FROM source is a derived table/CTE — the nested, fully compiled
    /// child table plan this source wraps. Null for a plain named stream/table. See TableExecutorImpl's
    /// derived-node wiring: the child plan gets its own nested TableExecutor (the same "table-over-table
    /// chaining" mechanism this codebase already has — TableExecutor.OnTableDelta), fed by the same real
    /// leaf stream/table inputs, whose emitted TableDeltas become this alias's input deltas one level up.</summary>
    public CompiledTablePlan? DerivedPlan { get; init; }
}

internal sealed class CompiledTableJoin
{
    public required JoinKind Kind { get; init; }
    public required string Alias { get; init; }
    public required string SourceName { get; init; }
    public required SourceSchema Schema { get; init; }
    public required bool IsTable { get; init; }
    /// <summary>Plan 008: for Inner/Cross/Semi/Anti/Scalar, the first equi-key component only, with every
    /// OTHER component folded back into <see cref="Residual"/> — the pre-008 shape TableJoinOp/
    /// TableSemiAntiOp still read (see Sql/Validator.cs's JoinKeyFolding). For Left/Right/Full, this is
    /// simply <see cref="LeftKeys"/>[0] — TableOuterJoinOp reads <see cref="LeftKeys"/>/
    /// <see cref="RightKeys"/> directly instead, and <see cref="Residual"/> here is then the PURE
    /// residual (no fold-back — see TablePlanner's join builder).</summary>
    public Expr? LeftKey { get; init; }
    public Expr? RightKey { get; init; }
    public Expr? Residual { get; init; }
    /// <summary>Plan 008: every equi-conjunct's left/right operand, in ON-clause order — the full
    /// composite key. TableOuterJoinOp (Left/Right/Full) consumes this directly; every other op still
    /// reads the single-key <see cref="LeftKey"/>/<see cref="RightKey"/> above.</summary>
    public IReadOnlyList<Expr>? LeftKeys { get; init; }
    public IReadOnlyList<Expr>? RightKeys { get; init; }
    /// <summary>Plan 004 N1: set when this JOIN's source is a derived table/CTE. See CompiledTableSource.DerivedPlan.</summary>
    public CompiledTablePlan? DerivedPlan { get; init; }
    /// <summary>Plan 002 L2: set only when Kind == Unnest — the expression TableUnnestOp evaluates against
    /// the accumulated left row. Null for every other join kind.</summary>
    public Expr? UnnestExpr { get; init; }
}

/// <summary>The fully resolved, executable form of a compiled TABLE query — the table-mode analogue of
/// <see cref="CompiledPlan"/> (no Window/Emit: tables are unwindowed by construction).</summary>
internal sealed class CompiledTablePlan
{
    public required List<CompiledTableSource> Sources { get; init; } // [0] = FROM; [1..] mirror Joins order
    public required List<CompiledTableJoin> Joins { get; init; }
    public required Expr? Where { get; init; }
    public required List<Expr>? GroupBy { get; init; }
    public required List<OutputItem> Output { get; init; }
    public required List<AggregateCallExpr> AggregateNodes { get; init; }
    public required Dictionary<AggregateCallExpr, int> AggregateIndex { get; init; }
    public required Dictionary<Expr, (string Alias, string Field)> Bindings { get; init; }
    public required bool HasAggregates { get; init; }
    public required string PlanSummary { get; init; }
    public required List<string> StreamInputs { get; init; }
    public required List<string> TableInputs { get; init; }
    public required string SourceLabel { get; init; } // comma-joined source names, used as output _source
    public required SourceSchema OutputSchema { get; init; }
    /// <summary>Plan 002 L3: LATEST BY key expressions — null unless the query has a LATEST BY clause
    /// (mutually exclusive with GroupBy/HasAggregates by construction; see Validator's exclusivity
    /// diagnostics). See Runtime/Ops/TableLatestByOp.cs.</summary>
    public List<Expr>? LatestBy { get; init; }
}
