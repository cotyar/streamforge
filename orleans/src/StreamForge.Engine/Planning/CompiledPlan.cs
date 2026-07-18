using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Planning;

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
    public Expr? LeftKey { get; init; }
    public Expr? RightKey { get; init; }
    public Expr? Residual { get; init; }
    /// <summary>Plan 004 N1: set when this JOIN's source is a derived table/CTE. See CompiledSource.DerivedPlan.</summary>
    public CompiledPlan? DerivedPlan { get; init; }
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
}
