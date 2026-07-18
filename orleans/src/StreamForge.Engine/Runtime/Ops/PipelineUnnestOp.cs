using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// Plan 002 L2: `[CROSS] JOIN UNNEST(expr) AS alias` (or its comma-form sugar, desugared to the same
/// JoinClause at parse time — see Parser.ParseSelectQuery/UnnestSource). Evaluates <c>expr</c> against the
/// accumulated LEFT row (everything folded so far — the same "accumulatedLeft" PipelineJoinOp folds
/// against) and emits ONE output row per array element, in array order, with <c>alias</c> bound to that
/// element as a single Json-kind value (see Validator.ResolveUnnestJoin's synthetic one-field element
/// schema). A NULL value, a non-list value, or an empty list all emit ZERO rows — there is no NULL-padded
/// "LEFT UNNEST" variant in this dialect (see UnnestSource's doc comment).
///
/// LINEAR AND STATELESS: unlike PipelineJoinOp (which buffers both sides for a WITHIN-bounded interval
/// match), UNNEST has no buffering, no WITHIN, and no "other side" — it is a pure 1-to-N row expansion of
/// whatever arrives on the left. It still implements <see cref="IPipelineJoinStage"/> (so it can occupy an
/// ordinary slot in PipelineExecutor's `_joins` chain and be reached via the same left-to-right
/// PropagateForward walk every other join stage uses) but OnRight/Evict are dead code paths in practice:
/// ExecutorImpl never registers a role for an UNNEST alias (it has no external driving source of its own —
/// see EnsureInit's role-registration skip for JoinKind.Unnest), so OnRight is never called and there is no
/// buffered state for Evict to flush.
///
/// STATE: none.
/// </summary>
internal sealed class PipelineUnnestOp(Expr expr, string alias, IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings) : IPipelineJoinStage
{
    public List<WorkingRow> OnLeft(WorkingRow row)
    {
        var value = ExpressionEvaluator.Eval(expr, new EvalContext(row, bindings));
        var results = new List<WorkingRow>();
        if (value is not List<object?> list) return results; // NULL / non-array: zero rows (documented; no LEFT UNNEST)

        string key = $"{alias}_{alias}";
        foreach (var element in list)
        {
            var fields = new Dictionary<string, object?>(row.Fields) { [key] = element };
            var aliases = new List<string>(row.Aliases) { alias };
            results.Add(new WorkingRow { Ts = row.Ts, Aliases = aliases, Fields = fields });
        }
        return results;
    }

    /// <summary>Dead in practice — see class doc: an UNNEST alias never has its own driving role, so nothing
    /// ever calls OnRight on this stage.</summary>
    public List<WorkingRow> OnRight(WorkingRow row) => [];

    /// <summary>Dead in practice — see class doc: stateless, nothing to evict.</summary>
    public List<WorkingRow> Evict(long watermark) => [];
}
