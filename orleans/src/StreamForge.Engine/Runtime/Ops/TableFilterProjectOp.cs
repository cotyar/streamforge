using StreamForge.Engine.Dataflow;
using StreamForge.Engine.Planning;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// WHERE filtering, with terminal projection to the table's output row when no GROUP BY/aggregate sits
/// downstream (plan 003 M1's suggested op set: "TableFilterProjectOp").
///
/// A table plan has exactly one WHERE stage, applied to every row surviving the join chain, regardless of
/// whether the plan is grouped. What differs is what happens to a row that passes: an UNGROUPED plan
/// projects it straight to the table's final output <see cref="TableDelta"/> right here (weight
/// passthrough — see <see cref="OnBatchTerminal"/>); a GROUPED plan instead hands the filtered
/// (still-WorkingRow) delta to <see cref="TableReduceOp"/>, which performs its OWN projection using
/// per-group aggregate state that this op has no access to (see <see cref="OnBatch"/>). This mirrors the
/// pre-M1 `TableExecutorImpl.HandleIncoming`'s `if (_group is not null) group.OnDelta(...) else
/// output.Add(ProjectRow(...))` branch exactly — same two outcomes, now named as two methods on one op
/// instead of an inline branch, selected by TableExecutor's façade based on the compiled plan's shape
/// (matches the plan's own graph: whether a TableReduceOp exists downstream is a plan-time decision, not
/// a per-row one).
///
/// STATE: none. WHERE and the projection expressions are pure functions of one input row plus the
/// compiled plan's static expression trees — nothing carries between calls.
/// </summary>
internal sealed class TableFilterProjectOp : ITableOp
{
    private readonly CompiledTablePlan _plan;

    public TableFilterProjectOp(CompiledTablePlan plan) => _plan = plan;

    /// <summary>WHERE only (weight passthrough on pass, drop on fail) — used when a TableReduceOp is
    /// downstream and will perform its own projection.</summary>
    public IReadOnlyList<TableRowDelta> OnBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableRowDelta>();
        foreach (var d in input)
        {
            if (!PassesWhere(d.Row)) continue;
            results.Add(d);
        }
        return results;
    }

    /// <summary>WHERE + projection — used when this op is the terminal stage (no GROUP BY): produces the
    /// table's final output TableDelta directly, weight unchanged from the input delta.</summary>
    public IReadOnlyList<TableDelta> OnBatchTerminal(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableDelta>();
        foreach (var d in input)
        {
            if (!PassesWhere(d.Row)) continue;
            results.Add(new TableDelta(ProjectRow(d.Row, _plan), d.Weight));
        }
        return results;
    }

    private bool PassesWhere(WorkingRow row)
    {
        if (_plan.Where is null) return true;
        return ExpressionEvaluator.IsTrue(ExpressionEvaluator.Eval(_plan.Where, new EvalContext(row, _plan.Bindings)));
    }

    private static EventRecord ProjectRow(WorkingRow row, CompiledTablePlan plan)
    {
        var evt = new EventRecord();
        var ctx = new EvalContext(row, plan.Bindings);
        foreach (var item in plan.Output)
        {
            evt[item.Name] = ExpressionEvaluator.Eval(item.Expression, ctx);
        }
        evt[EventRecord.TimestampField] = row.Ts;
        evt[EventRecord.SourceField] = plan.SourceLabel;
        return evt;
    }

    /// <summary>Pass-through — see class doc: WHERE/projection are pure per-row functions, no time/
    /// epoch-driven state to flush.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
