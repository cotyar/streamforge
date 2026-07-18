using StreamForge.Engine.Dataflow;
using StreamForge.Engine.Sql;

namespace StreamForge.Engine.Runtime.Ops;

/// <summary>
/// One stage of a left-to-right folded relational equi-join chain in table mode: `(accumulatedLeft) JOIN
/// rightAlias ON leftKey = rightKey [AND residual]`. Unlike the stream dialect's interval join, table
/// joins keep BOTH sides' full state (no WITHIN eviction — state is unbounded and consolidated). On a
/// delta (row, weight) from either side: look up matches in the *other* side's index, emit
/// combine(row, other) with weight = weight * otherWeight for each match (Z-set bilinear join), then
/// update this side's own index. Only INNER equi-joins are supported in table mode (validator-enforced).
///
/// This is a mechanical relocation of the pre-M1 `TableJoinStage` into the explicit-op shape (plan 003
/// M1: "TableJoinOp (bilinear equi-join, one per join)") — per-row join algorithm unchanged; OnLeft/
/// OnRight became OnLeftBatch/OnRightBatch (batch-shaped per the op contract, looping the same per-row
/// logic) and it now carries an Epoch parameter (unused today — see OnFrontier) and implements
/// <see cref="ITableOp"/>.
///
/// TWO-INPUT SEAM: a join is the one table op with two distinct input edges (left = accumulated rows
/// from everything folded so far, right = this join's own alias) — hence OnLeftBatch/OnRightBatch
/// instead of a single OnBatch, per plan 003 M1: "or per-input-edge variant where an op has 2 inputs —
/// design the seam; joins have two input edges".
///
/// STATE: both sides' full Z-set index — <see cref="Left"/> and <see cref="Right"/>, each a
/// <see cref="ZSetIndex"/> (join-key string -> canonical-row-text string -> (WorkingRow, weight)), which
/// is already plain nested-dictionary data (no behavior) and IS this op's state object, exposed live
/// (not copied) rather than duplicated into a separate snapshot type. A POCO snapshot for M2 checkpointing
/// is exactly `{ Left: ZSetIndex, Right: ZSetIndex }`.
/// </summary>
internal sealed class TableJoinOp(Expr leftKey, Expr rightKey, Expr? residual, IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings) : ITableOp
{
    /// <summary>This join's state, left side (rows accumulated from everything folded so far).</summary>
    public ZSetIndex Left { get; } = new();

    /// <summary>This join's state, right side (rows from this join's own alias).</summary>
    public ZSetIndex Right { get; } = new();

    public IReadOnlyList<TableRowDelta> OnLeftBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input) => OnBatch(input, isLeft: true);

    public IReadOnlyList<TableRowDelta> OnRightBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input) => OnBatch(input, isLeft: false);

    private List<TableRowDelta> OnBatch(IReadOnlyList<TableRowDelta> input, bool isLeft)
    {
        var results = new List<TableRowDelta>();
        foreach (var d in input)
        {
            foreach (var (row, weight) in OnArrival(d.Row, d.Weight, isLeft))
            {
                results.Add(new TableRowDelta(row, weight));
            }
        }
        return results;
    }

    private List<(WorkingRow Row, long Weight)> OnArrival(WorkingRow row, long weight, bool isLeft)
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
        var otherIndex = isLeft ? Right : Left;

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

        var ownIndex = isLeft ? Left : Right;
        var rowCanonical = JsonText.SerializeCanonicalRow(row.Fields);
        ownIndex.Apply(keyStr, rowCanonical, row, weight);

        return results;
    }

    /// <summary>Pass-through — see class doc: table joins have no epoch-driven eviction (unbounded,
    /// consolidated state by design). Hook exists for M2 partition-frontier propagation.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
