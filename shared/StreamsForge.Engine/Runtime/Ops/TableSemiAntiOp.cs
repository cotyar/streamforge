using StreamsForge.Engine.Dataflow;
using StreamsForge.Engine.Sql;

namespace StreamsForge.Engine.Runtime.Ops;

/// <summary>
/// Plan 004 N2's table-mode semi/anti join: `expr [NOT] IN (SELECT ...)` / `[NOT] EXISTS (SELECT ...)`,
/// rewritten by Planner into a Semi (IN/EXISTS) or Anti (NOT IN/NOT EXISTS) <see cref="JoinKind"/> stage.
/// Unlike <see cref="TableJoinOp"/>'s bilinear combine-and-multiply-weight join, this is PRESENCE-gated
/// pass-through of the LEFT (outer) row: a B-side (subquery) row never appears in the output, and B-side
/// row MULTIPLICITY never fans out A-side rows — "duplicates in B must not duplicate A's rows: presence,
/// not join fan-out" (plan 004 N2). What matters is only whether a key's accumulated weight on the B side
/// is currently &gt; 0 ("present") — and, when a key's presence FLIPS (0 → positive or positive → 0), every
/// currently-indexed A-row under that key must be retracted/re-asserted to reflect the flip, even though no
/// NEW A-row arrived (plan 004 N2: "when B's last row for key k retracts, all matching A rows must
/// retract; when first row for k asserts, they assert").
///
/// EXISTS/NOT EXISTS reuses this SAME machinery with a trick at the Planner level, not here: both
/// <see cref="_leftKey"/> and <see cref="_rightKey"/> are compiled as the SAME constant expression (see
/// Planner.BuildSemiAntiJoin), collapsing every row on both sides into one global bucket — "is there any B
/// row at all" becomes "is the constant key present", the exact same presence test IN uses per-key.
///
/// NOT IN NULL rule (plan 004, deliberate deviation from strict three-valued SQL): a B-side row whose key
/// expression evaluates to NULL is never indexed at all — see <see cref="OnRightBatch"/> — so it can never
/// make a key "present" and never poisons NOT IN into "always false" the way strict SQL's NULL-in-subquery
/// trap would. An A-side row whose OWN key is NULL is dropped outright (never indexed, never emitted) —
/// ordinary WHERE null-handling (IsTrue(null) == false) applies there, unrelated to the NOT IN NULL rule.
///
/// STATE: <see cref="Left"/> (A-side rows, keyed by their evaluated LeftKey — used to find "which A rows
/// need re-emitting" when a B-side key's presence flips) and <see cref="Right"/> (B-side rows, keyed by
/// their evaluated RightKey — used purely to track each key's accumulated weight/presence; the actual ROW
/// contents on this side are never read, only whether the bucket is non-empty). Both are
/// <see cref="ZSetIndex"/>, exactly TableJoinOp's state shape — this op differs in what it DOES with that
/// state (presence tracking + flip-triggered re-emission), not in the state's shape.
/// </summary>
internal sealed class TableSemiAntiOp(JoinKind kind, Expr leftKey, Expr rightKey, IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings) : ITableJoinStage
{
    private readonly bool _isAnti = kind == JoinKind.Anti;

    /// <summary>A-side (outer row) state: join-key → (row, weight) — indexed so a B-side presence flip can
    /// find every currently-live A-row under that key to re-emit.</summary>
    public ZSetIndex Left { get; } = new();

    /// <summary>B-side (subquery row) state: join-key → (row, weight) — only ever consulted via
    /// <c>.Lookup(key).Any()</c> (is this key currently present with positive weight); the row payload
    /// itself is never read back out.</summary>
    public ZSetIndex Right { get; } = new();

    public IReadOnlyList<TableRowDelta> OnLeftBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableRowDelta>();
        foreach (var d in input)
        {
            var keyVal = ExpressionEvaluator.Eval(leftKey, new EvalContext(d.Row, bindings));
            if (keyVal is null) continue; // outer key NULL: unknown membership, WHERE-null semantics (dropped)

            var keyStr = TableKeyEncoding.EncodeScalar(keyVal);
            var rowCanonical = JsonText.SerializeCanonicalRow(d.Row.Fields);
            Left.Apply(keyStr, rowCanonical, d.Row, d.Weight);

            bool present = Right.Lookup(keyStr).Any();
            bool passes = present != _isAnti; // Semi: passes when present; Anti: passes when absent
            if (passes) results.Add(new TableRowDelta(d.Row, d.Weight));
        }
        return results;
    }

    public IReadOnlyList<TableRowDelta> OnRightBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableRowDelta>();
        foreach (var d in input)
        {
            var keyVal = ExpressionEvaluator.Eval(rightKey, new EvalContext(d.Row, bindings));
            if (keyVal is null) continue; // NOT IN NULL rule: NULLs in the subquery result are ignored

            var keyStr = TableKeyEncoding.EncodeScalar(keyVal);
            bool wasPresent = Right.Lookup(keyStr).Any();

            var rowCanonical = JsonText.SerializeCanonicalRow(d.Row.Fields);
            Right.Apply(keyStr, rowCanonical, d.Row, d.Weight);

            bool isPresentNow = Right.Lookup(keyStr).Any();
            if (wasPresent == isPresentNow) continue; // presence didn't flip: B changed, but no A row is affected

            // Presence flipped: re-emit every currently-indexed A-row under this key. Semi: newly-present
            // means assert (their weight), newly-absent means retract (negate their weight). Anti mirrors.
            bool nowPasses = isPresentNow != _isAnti;
            int sign = nowPasses ? 1 : -1;
            foreach (var (aRow, aWeight) in Left.Lookup(keyStr))
            {
                results.Add(new TableRowDelta(aRow, sign * aWeight));
            }
        }
        return results;
    }

    /// <summary>Pass-through — see TableJoinOp's identical OnFrontier doc: table mode has no epoch-driven
    /// eviction; this op's state (like TableJoinOp's) is unbounded and consolidated by design.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
