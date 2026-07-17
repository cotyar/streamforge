using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Planning;

internal sealed class CompiledTableSource
{
    public required string Alias { get; init; }
    public required string SourceName { get; init; }
    public required SourceSchema Schema { get; init; }
    public required bool IsTable { get; init; }
}

internal sealed class CompiledTableJoin
{
    public required JoinKind Kind { get; init; }
    public required string Alias { get; init; }
    public required string SourceName { get; init; }
    public required SourceSchema Schema { get; init; }
    public required bool IsTable { get; init; }
    public Expr? LeftKey { get; init; }
    public Expr? RightKey { get; init; }
    public Expr? Residual { get; init; }
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
}
