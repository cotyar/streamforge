using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Runtime;

/// <summary>One stage of a left-to-right folded relational equi-join chain in table mode:
/// `(accumulatedLeft) JOIN rightAlias ON leftKey = rightKey [AND residual]`. Unlike the stream dialect's
/// interval join, table joins keep BOTH sides' full state (no WITHIN eviction — state is unbounded and
/// consolidated). On a delta (row, weight) from either side: look up matches in the *other* side's index,
/// emit combine(row, other) with weight = weight * otherWeight for each match (Z-set bilinear join), then
/// update this side's own index. Only INNER equi-joins are supported in table mode (validator-enforced).</summary>
internal sealed class TableJoinStage(Expr leftKey, Expr rightKey, Expr? residual, IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings)
{
    private readonly ZSetIndex _left = new();
    private readonly ZSetIndex _right = new();

    public List<(WorkingRow Row, long Weight)> OnLeft(WorkingRow row, long weight) => OnArrival(row, weight, isLeft: true);

    public List<(WorkingRow Row, long Weight)> OnRight(WorkingRow row, long weight) => OnArrival(row, weight, isLeft: false);

    private List<(WorkingRow, long)> OnArrival(WorkingRow row, long weight, bool isLeft)
    {
        var results = new List<(WorkingRow, long)>();

        var keyExpr = isLeft ? leftKey : rightKey;
        var keyVal = ExpressionEvaluator.Eval(keyExpr, new EvalContext(row, bindings));
        if (keyVal is null)
        {
            // SQL equi-join semantics: NULL never matches NULL (or anything) — the row contributes
            // nothing to the join output and isn't indexed for future matches either.
            return results;
        }

        var keyStr = TableKeyEncoding.EncodeScalar(keyVal);
        var otherIndex = isLeft ? _right : _left;

        foreach (var (otherRow, otherWeight) in otherIndex.Lookup(keyStr))
        {
            var combined = isLeft ? WorkingRow.Combine(row, otherRow) : WorkingRow.Combine(otherRow, row);
            if (residual is not null)
            {
                var ok = ExpressionEvaluator.IsTrue(ExpressionEvaluator.Eval(residual, new EvalContext(combined, bindings)));
                if (!ok) continue;
            }
            results.Add((combined, weight * otherWeight));
        }

        var ownIndex = isLeft ? _left : _right;
        var rowCanonical = JsonText.SerializeCanonicalRow(row.Fields);
        ownIndex.Apply(keyStr, rowCanonical, row, weight);

        return results;
    }
}
