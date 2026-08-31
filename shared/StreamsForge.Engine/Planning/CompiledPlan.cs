using StreamsForge.Engine.Sql;

namespace StreamsForge.Engine.Planning;

/// <summary>One SELECT output column: either a direct substitution of a stored GROUP BY value
/// (GroupByIndex set, used for windowed/grouped queries) or a general expression to evaluate.</summary>
internal sealed class OutputItem
{
    public required string Name { get; init; }
    public required Expr Expression { get; init; }
    public int? GroupByIndex { get; set; }
}

internal sealed class CompiledSource
{
    public required string Alias { get; init; }
    public required string SourceName { get; init; }
    public required SourceSchema Schema { get; init; }
    /// <summary>Plan 004 N1: set when this FROM source is a derived table/CTE — the nested, fully compiled
    /// child plan this source wraps. Null for a plain named stream. See ExecutorImpl's derived-node
    /// wiring: the child plan gets its own nested PipelineExecutor, fed by the same real leaf source names
    /// (<see cref="CompiledPlan.SourceNames"/>, already flattened through it), whose emissions become this
    /// alias's input events one level up — exactly plan 003 M1's IPipelineOpChain composability seam.</summary>
    public CompiledPlan? DerivedPlan { get; init; }
}

internal sealed class CompiledJoin
{
    public required JoinKind Kind { get; init; }
    public required string Alias { get; init; }
    public required string SourceName { get; init; }
    public required SourceSchema Schema { get; init; }
    public required TimeSpan Within { get; init; }
    /// <summary>Plan 008: first equi-key component only, with every OTHER component folded back into
    /// <see cref="Residual"/> — the pre-008 shape every existing single-key consumer (PipelineJoinOp,
    /// PipelineSubqueryOp) still reads. See <see cref="LeftKeys"/> for the full composite key.</summary>
    public Expr? LeftKey { get; init; }
    public Expr? RightKey { get; init; }
    public Expr? Residual { get; init; }
    /// <summary>Plan 008: every equi-conjunct's left/right operand, in ON-clause order (composite keys —
    /// see Sql/Validator.cs's ExtractEquiKey doc comment). Not consumed by any pipeline-mode op yet
    /// (PipelineJoinOp has no composite-key-aware path in this wave — see JoinKeyFolding), but propagated
    /// here for parity with CompiledTableJoin and future use.</summary>
    public IReadOnlyList<Expr>? LeftKeys { get; init; }
    public IReadOnlyList<Expr>? RightKeys { get; init; }
    /// <summary>Plan 004 N1: set when this JOIN's source is a derived table/CTE. See CompiledSource.DerivedPlan.</summary>
    public CompiledPlan? DerivedPlan { get; init; }
    /// <summary>Plan 002 L2: set only when Kind == Unnest — the expression PipelineUnnestOp evaluates
    /// against the accumulated left row. Null for every other join kind.</summary>
    public Expr? UnnestExpr { get; init; }
}

/// <summary>The fully resolved, executable form of a compiled query — everything the runtime needs,
/// independent of the frozen public DTOs. Immutable; shared across all executors of a plan.</summary>
internal sealed class CompiledPlan
{
    public required List<CompiledSource> Sources { get; init; } // [0] = FROM; [1..] mirror Joins order
    public required List<CompiledJoin> Joins { get; init; }
    public required Expr? Where { get; init; }
    public required List<Expr>? GroupBy { get; init; }
    public required WindowSpec? Window { get; init; }
    public required EmitMode Emit { get; init; } // meaningful only when Window != null
    public required List<OutputItem> Output { get; init; }
    public required List<AggregateCallExpr> AggregateNodes { get; init; }
    public required Dictionary<AggregateCallExpr, int> AggregateIndex { get; init; }
    public required Dictionary<Expr, (string Alias, string Field)> Bindings { get; init; }
    public required bool HasAggregates { get; init; }
    public required string PlanSummary { get; init; }
    public required SourceSchema OutputSchema { get; init; }
    public required List<string> SourceNames { get; init; }
    public required string SourceLabel { get; init; } // comma-joined source names, used as output _source

    /// <summary>Plan 008 W3: non-null only for a set-operation root (top-level `SELECT ... UNION ALL
    /// SELECT ...`, or the same in derived-table position — see Sql/Validator.cs's UnionDerivedInfo). When
    /// set, EVERY other field above is a benign placeholder (Sources/Joins empty, Where/GroupBy/Window
    /// null, Output empty, ...) — the union-root plan has no FROM/JOIN chain of its own; ExecutorImpl's
    /// union-aware EnsureInit/OnEventCore/AdvanceWatermarkCore branch on this field FIRST and never touch
    /// those placeholder fields. Each branch is itself a complete, independently-compiled CompiledPlan
    /// (built by the SAME BuildCompiledPlan every ordinary query goes through) — "reuse the nesting seam"
    /// per plan 008: this slot sits exactly where CompiledSource.DerivedPlan/CompiledJoin.DerivedPlan
    /// already nest one compiled plan inside another for a derived table, so a set operation appearing
    /// there (FROM ( ... UNION ... ) alias) needs zero extra wiring at the executor layer — see
    /// Planning/Planner.cs's BuildCompiledUnionPlan.</summary>
    public List<CompiledPlan>? UnionBranches { get; init; }
}
