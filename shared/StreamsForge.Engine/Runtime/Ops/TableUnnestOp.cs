using StreamsForge.Engine.Dataflow;
using StreamsForge.Engine.Sql;

namespace StreamsForge.Engine.Runtime.Ops;

/// <summary>
/// Plan 002 L2, table mode: the ITableJoinStage counterpart of <see cref="PipelineUnnestOp"/> — same
/// expr-over-accumulated-left-row 1-to-N expansion, same "NULL/non-array/empty -> zero rows" rule, same
/// synthetic one-field element schema (see Validator.ResolveUnnestJoin).
///
/// Z-SET LINEARITY: UNNEST is a linear operator — each element inherits the INPUT delta's weight UNCHANGED
/// (no bilinear weight multiplication the way TableJoinOp's two-sided equi-join has). Concretely: a
/// retraction (weight -1) of a row whose array has 3 elements produces 3 output deltas, each weight -1 —
/// retracting exactly the 3 element-rows that row's earlier assertion produced. This requires no additional
/// state beyond re-evaluating expr on each incoming delta; the weight arithmetic is "multiply by 1" per
/// element, which is just "copy the weight" (see OnLeftBatch below).
///
/// STATE: none. Like TableJoinOp/TableSemiAntiOp, OnRightBatch is part of the shared ITableJoinStage
/// interface but is dead code here in practice — see PipelineUnnestOp's class doc for why an UNNEST alias
/// never has its own driving role (TableExecutorImpl's EnsureInit skips role-registration for
/// JoinKind.Unnest the same way ExecutorImpl does).
/// </summary>
internal sealed class TableUnnestOp(Expr expr, string alias, IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings) : ITableJoinStage
{
    public IReadOnlyList<TableRowDelta> OnLeftBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableRowDelta>();
        string key = $"{alias}_{alias}";

        foreach (var d in input)
        {
            var value = ExpressionEvaluator.Eval(expr, new EvalContext(d.Row, bindings));
            if (value is not List<object?> list) continue; // NULL / non-array: zero rows

            foreach (var element in list)
            {
                var fields = new Dictionary<string, object?>(d.Row.Fields) { [key] = element };
                var aliases = new List<string>(d.Row.Aliases) { alias };
                var newRow = new WorkingRow { Ts = d.Row.Ts, Aliases = aliases, Fields = fields };
                // Linear pass-through: the element-row inherits the INPUT delta's weight unchanged — see
                // class doc's Z-set linearity note.
                results.Add(new TableRowDelta(newRow, d.Weight));
            }
        }
        return results;
    }

    /// <summary>Dead in practice — see class doc.</summary>
    public IReadOnlyList<TableRowDelta> OnRightBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input) => [];

    /// <summary>Pass-through — no epoch-driven state (same reasoning as every other table-mode op's
    /// OnFrontier — see TableJoinOp's identical doc comment).</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
