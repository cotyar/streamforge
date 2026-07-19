using StreamForge.Engine.Planning;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// WHERE filtering, with terminal projection to the pipeline's output row when no WINDOW sits downstream
/// (plan 003 M1 Part B — the pipeline-mode mirror of table mode's <see cref="TableFilterProjectOp"/>,
/// same split for the same reason: a windowed plan hands filtered rows to <see cref="PipelineWindowOp"/>,
/// which performs its OWN projection from per-window aggregate state this op has no access to; an
/// unwindowed plan projects immediately, right here). Mechanical extraction of the WHERE-check + "else
/// project" branch that pre-M1 `ExecutorImpl.ProcessRows` inlined — logic unchanged, now named.
///
/// STATE: none — WHERE and the projection expressions are pure functions of one input row plus the
/// compiled plan's static expression trees.
/// </summary>
internal sealed class PipelineFilterProjectOp
{
    private readonly CompiledPlan _plan;

    public PipelineFilterProjectOp(CompiledPlan plan) => _plan = plan;

    /// <summary>WHERE only — used when a PipelineWindowOp is downstream and will perform its own
    /// projection.</summary>
    public List<WorkingRow> OnBatch(List<WorkingRow> rows)
    {
        var results = new List<WorkingRow>();
        foreach (var row in rows)
        {
            if (PassesWhere(row)) results.Add(row);
        }
        return results;
    }

    /// <summary>WHERE + projection — used when this op is the terminal stage (no WINDOW): produces the
    /// pipeline's output row directly.</summary>
    public List<EventRecord> OnBatchTerminal(List<WorkingRow> rows)
    {
        var results = new List<EventRecord>();
        foreach (var row in rows)
        {
            if (PassesWhere(row)) results.Add(ProjectRow(row, _plan));
        }
        return results;
    }

    private bool PassesWhere(WorkingRow row)
    {
        if (_plan.Where is null) return true;
        var whereResult = ExpressionEvaluator.Eval(_plan.Where, new EvalContext(row, _plan.Bindings));
        return ExpressionEvaluator.IsTrue(whereResult);
    }

    private static EventRecord ProjectRow(WorkingRow row, CompiledPlan compiled)
    {
        var evt = new EventRecord();
        var ctx = new EvalContext(row, compiled.Bindings);
        foreach (var item in compiled.Output)
        {
            evt[item.Name] = ExpressionEvaluator.Eval(item.Expression, ctx);
        }
        evt[EventRecord.TimestampField] = row.Ts;
        evt[EventRecord.SourceField] = compiled.SourceLabel;
        return evt;
    }
}
