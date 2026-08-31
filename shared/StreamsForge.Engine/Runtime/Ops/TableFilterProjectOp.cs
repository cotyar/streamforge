using StreamsForge.Engine.Dataflow;
using StreamsForge.Engine.Planning;

namespace StreamsForge.Engine.Runtime.Ops;

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
///
/// <para>Wishlist "explicit key retraction through ingest", gap 2 — a client-issued KEY RETRACTION
/// (<see cref="TableIngestOp.RetractField"/>, alias-qualified onto the <see cref="WorkingRow"/> by
/// <see cref="TableIngestOp"/>; see <see cref="TableLatestByOp"/>'s identical class-doc section) carries
/// ONLY the retracted row's key columns — "a client retraction only ever carries key columns, never the
/// whole row" (RetractConsumerValidation's own doc). A WHERE predicate over any OTHER column therefore
/// sees a missing/null field and, per SQL's usual three-valued logic, evaluates false or null — so
/// <see cref="PassesWhere"/> would drop the retraction right here, before <see cref="TableLatestByOp"/>
/// ever sees it, and it would vanish with no trace: not counted, not logged, nothing downstream ever
/// finds out the key was supposed to be freed.
///
/// Two honest fixes were on the table (wishlist gap 2): (a) let a retraction bypass WHERE outright,
/// since it targets a KEY, not a row that has to qualify on content it doesn't carry; or (b) reject at
/// validate time when the table's WHERE references a non-key column. (b) was rejected: it would need
/// the WHERE's referenced-column set cross-checked against the LATEST BY key list at validate time (the
/// same place RetractConsumerValidation already runs), which means either widening that check to inspect
/// a compiled plan's WHERE expression tree (real added complexity for what is fundamentally a runtime
/// data-shape question, not a schema-shape one) or accepting a table that could NEVER validly retract
/// through this column set, which is a strange thing to reject at CREATE time when the same table might
/// serve ordinary (non-retracting) traffic just fine. (a) also matches what <see cref="TableIngestOp"/>
/// already does one stage upstream: it does not inspect or require the rest of the row either, it just
/// honors the flag. Consistency won: a key retraction is a key-level operation end to end, not a row
/// that has to earn its way past a content filter meant for rows that DO carry their content.
///
/// <see cref="IsKeyRetraction"/> below duplicates <see cref="TableLatestByOp"/>'s private detector of
/// the same name rather than sharing it — that method is <c>private</c> on a class this file may not
/// modify (repo file-ownership rule for this change), and the flag is a literal already duplicated once
/// on purpose between <see cref="TableIngestOp"/> and AppCore's <c>IngressRowAcceptance.RetractField</c>
/// (see that op's own doc on why two copies of a stable literal beats one cross-assembly dependency the
/// Engine does not otherwise need) — same reasoning applies to this third copy, one assembly over.
/// </para>
/// </summary>
internal sealed class TableFilterProjectOp : ITableOp
{
    private readonly CompiledTablePlan _plan;

    public TableFilterProjectOp(CompiledTablePlan plan) => _plan = plan;

    /// <summary>WHERE only (weight passthrough on pass, drop on fail) — used when a TableReduceOp is
    /// downstream and will perform its own projection. A key retraction (see class doc) bypasses WHERE
    /// unconditionally rather than being evaluated against it.</summary>
    public IReadOnlyList<TableRowDelta> OnBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableRowDelta>();
        foreach (var d in input)
        {
            if (!IsKeyRetraction(d.Row) && !PassesWhere(d.Row)) continue;
            results.Add(d);
        }
        return results;
    }

    /// <summary>WHERE + projection — used when this op is the terminal stage (no GROUP BY): produces the
    /// table's final output TableDelta directly, weight unchanged from the input delta. A key retraction
    /// (see class doc) bypasses WHERE the same way OnBatch does — it still gets projected (only the key
    /// columns it carries will have real values; that is the same "safe, at worst incomplete" shape
    /// TableIngestOp's own doc already accepts for a shape other than LATEST BY).</summary>
    public IReadOnlyList<TableDelta> OnBatchTerminal(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableDelta>();
        foreach (var d in input)
        {
            if (!IsKeyRetraction(d.Row) && !PassesWhere(d.Row)) continue;
            results.Add(new TableDelta(ProjectRow(d.Row, _plan), d.Weight));
        }
        return results;
    }

    private bool PassesWhere(WorkingRow row)
    {
        if (_plan.Where is null) return true;
        return ExpressionEvaluator.IsTrue(ExpressionEvaluator.Eval(_plan.Where, new EvalContext(row, _plan.Bindings)));
    }

    /// <summary>See class doc. Mirrors <see cref="TableLatestByOp"/>'s identically-named private method
    /// field-for-field: true when ANY alias this row carries has its "{alias}__retract" field set to
    /// true (checked across all aliases, not just this plan's FROM alias, so a retraction upstream of a
    /// join still bypasses WHERE downstream of it — WorkingRow.Combine unions both sides' Fields).</summary>
    private static bool IsKeyRetraction(WorkingRow row)
    {
        foreach (var alias in row.Aliases)
        {
            if (row.Fields.TryGetValue(alias + "_" + TableIngestOp.RetractField, out var v) && v is true)
            {
                return true;
            }
        }
        return false;
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
