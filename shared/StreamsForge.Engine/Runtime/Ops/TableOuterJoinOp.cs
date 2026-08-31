using StreamsForge.Engine.Dataflow;
using StreamsForge.Engine.Sql;

namespace StreamsForge.Engine.Runtime.Ops;

/// <summary>
/// Plan 008 table-mode LEFT/RIGHT/FULL OUTER join over Z-sets: `(accumulatedLeft) {LEFT|RIGHT|FULL}
/// JOIN rightAlias ON leftKeys = rightKeys [AND residual]`. Same unbounded, consolidated-state shape
/// as <see cref="TableJoinOp"/> (no WITHIN eviction) but with one extra behavior: a row on the
/// "outer" (padded) side that currently has no matching row on the other side is emitted joined
/// against an all-NULL row for that side, and re-emitted (retracted/re-asserted) whenever a later
/// delta on the OTHER side flips that presence from empty to non-empty or back. WIRED as of plan 008
/// wave 2b: <see cref="TableExecutorImpl"/>.EnsureInit (single-partition) and
/// <see cref="TableDataflowBuilder"/>/<see cref="TableStageExecutors.JoinChainStageExecutor"/>
/// (partitioned) both select this op for JoinKind Left/Right/Full — TableJoinOp otherwise — see those
/// files' own doc comments for the op-selection switch and the accumulated-alias threading this op's
/// constructor needs for null-padding.
///
/// THE INVARIANT (must hold after every batch, i.e. the fully consolidated view — this is what the
/// incremental delta rules below exist to maintain without ever recomputing it from scratch). For
/// LEFT (RIGHT is the exact mirror, swapping which side is "outer"; FULL is literally both):
///   T1 (products)       for each key k, every pair (l ∈ L_k, r ∈ R_k) with weight w_l · w_r.
///   T2 (pads)           for each key k where R_k is empty, every l ∈ L_k as Combine(l, nullRight)
///                       with weight w_l.
///   T3 (null-key pads)  every left row whose key evaluates to NULL, padded, with its own weight —
///                       a NULL key can never equi-match anything, in or out of any bucket, so it is
///                       never indexed (kept out of every flip re-emission below) and its pad is final
///                       the instant it arrives.
/// FULL runs T1/T2/T3 for the left-as-outer role AND their mirror (T1 is shared — the SAME product
/// set either way, computed once) for the right-as-outer role, keyed to two independent booleans
/// (<see cref="_padLeftUnmatched"/>, <see cref="_padRightUnmatched"/>) rather than three separate
/// code paths. They cannot double-count: a left pad requires "no residual-passing right row in R_k
/// for THIS l", its mirror requires "no residual-passing left row in L_k for THIS r" — disjoint rows,
/// disjoint emissions. FULL never emits a (nullLeft, nullRight) row: a pad is always {real row} +
/// {synthetic null row for the OTHER side}, never two synthetic rows combined.
///
/// PRESENCE, EXACTLY: every "is key k present" check below is <c>ZSetIndex.Lookup(k).Any()</c>, i.e.
/// accumulated weight != 0 — NOT weight &gt; 0 (ZSetIndex.Lookup already only yields non-zero entries,
/// so ".Any()" on it *is* the != 0 rule for free; no extra code makes this choice, which is exactly
/// why it's safe to get right by construction here). This matters under out-of-order delivery: retract
/// before assert. Trace (LEFT join, same key k throughout, starting empty) — literally required by
/// this file's test suite (OutOfOrder_RetractThenAssert_SelfHeals):
///   1. R(r, -1):  Right bucket for k has no entry yet -> "was present" = false. Apply weight -1 ->
///                 bucket now holds {r: -1}, and -1 != 0 so Lookup(k).Any() = true -> "is present now".
///                 Flips false->true, but Left has no rows under k yet, so nothing is re-emitted.
///                 No products either (Left bucket empty). Net so far: nothing.
///   2. L(l, +1):  Left arrival. Right bucket still holds {r: -1}, which Lookup(k) yields (non-zero) —
///                 so this counts as a match: emit product (Combine(l,r), 1 * -1 = -1). Matched, so no
///                 pad for l. Index l into Left (weight +1).
///   3. R(r, +1):  This is the SAME r canonical row re-asserting. "Was present" (before apply) =
///                 Right.Lookup(k).Any() = true (the -1 entry is non-zero). Emit product against the
///                 now-indexed l: (Combine(l,r), 1 * 1 = +1). Apply weight +1 to the existing -1 entry
///                 -> nets to 0 -> ZSetIndex prunes it. "Is present now" = Right.Lookup(k).Any() =
///                 false. Flipped true->false -> re-assert the pad for every indexed left row under k:
///                 emit (Combine(l, nullRight), +1 * 1 = +1).
///   Net over the three deltas: products = -1 + 1 = 0 (correct — r's weight is actually 0, so r never
///   truly existed). Pad = +1 (correct — l is genuinely unmatched, since the only right row that ever
///   claimed k retracted before it asserted). Using "weight &gt; 0" instead of "!= 0" would make step 1's
///   "is present now" false (a -1 isn't &gt; 0), which would desync step 3's "was present" (also &gt; 0,
///   also false) from the true bucket contents and either drop the flip or double up on it.
///
/// NULL-KEY RULES — asymmetric on purpose, and the asymmetry tracks which side is "outer" for THIS
/// delta's own side, not the physical Left/Right of the wire:
///   - If the arriving side pads its own unmatched rows (Left for LEFT/FULL, Right for RIGHT/FULL),
///     a NULL key on that side still gets its pad (T3) — it just never enters that side's index, so
///     it can never be found by, or itself trigger, a flip.
///   - If the arriving side does NOT pad (Right for a pure LEFT join, Left for a pure RIGHT join), a
///     NULL key on that side contributes nothing at all and is not indexed either. Indexing it would
///     collide every NULL-keyed row into one bucket and make that bucket look "present", silently
///     suppressing every T2 pad for that key on the other side — the single worst bug available here,
///     guarded by RightNullKey_NotIndexed_DoesNotMakeKeyPresent below.
///
/// COMPOSITE KEYS: the constructor takes LISTS of key expressions per side (a later wave changes the
/// validator/planner to produce multi-expression ON clauses; today's SQL compiler still folds extra
/// ANDed equalities into the residual — see Validator's ON-extraction). Encoded with
/// <see cref="TableKeyEncoding.EncodeGroupKey"/> — the SAME multi-value encoding GROUP BY already uses
/// (and which degenerates to plain <see cref="TableKeyEncoding.EncodeScalar"/> for a 1-element list,
/// so single-key behavior is byte-identical to <see cref="TableJoinOp"/>'s encoding) — not a second,
/// bespoke encoding. A composite key is NULL (T3 rules apply) the instant ANY one component is NULL;
/// evaluation short-circuits there without encoding the other components.
///
/// RESIDUAL COST: with no residual, "does k have a match" is a single key-level test (ZSetIndex is
/// already bucketed by key, so this is O(1) plus an O(bucket size) re-emission on an actual flip) —
/// this is the cheap path and is used whenever <see cref="_residual"/> is null. With a residual, "the
/// bucket is non-empty" no longer implies "THIS specific row has a surviving match" — the residual can
/// reject every candidate for one bucket-mate while passing for another. So presence becomes PER-ROW:
/// on every delta that could flip the other side's pads, this op rescans the arriving side's own
/// bucket once per already-indexed other-side row (before AND after applying the delta) to find that
/// row's own residual-passing match count crossing zero. This is O(|L_k| · |R_k|) per delta — paid
/// only when a residual is present; the key-level O(1) path is untouched otherwise.
///
/// STATE: <see cref="Left"/>/<see cref="Right"/> — both sides' full <see cref="ZSetIndex"/>, exactly
/// TableJoinOp's state shape (join-key string -> canonical-row-text string -> (row, weight)).
/// </summary>
internal sealed class TableOuterJoinOp : ITableJoinStage
{
    private readonly JoinKind _kind;
    private readonly IReadOnlyList<Expr> _leftKeys;
    private readonly IReadOnlyList<Expr> _rightKeys;
    private readonly Expr? _residual;
    private readonly IReadOnlyDictionary<Expr, (string Alias, string Field)> _bindings;
    private readonly bool _padLeftUnmatched;
    private readonly bool _padRightUnmatched;
    private readonly WorkingRow _nullRight;
    private readonly WorkingRow _nullLeft;

    /// <summary>This join's state, left side (rows accumulated from everything folded so far).</summary>
    public ZSetIndex Left { get; } = new();

    /// <summary>This join's state, right side (rows from this join's own alias).</summary>
    public ZSetIndex Right { get; } = new();

    public TableOuterJoinOp(
        JoinKind kind,
        IReadOnlyList<Expr> leftKeys,
        IReadOnlyList<Expr> rightKeys,
        Expr? residual,
        IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings,
        IReadOnlyList<(string Alias, SourceSchema Schema)> leftAliasesSoFar,
        (string Alias, SourceSchema Schema) rightSide)
    {
        if (kind is not (JoinKind.Left or JoinKind.Right or JoinKind.Full))
            throw new ArgumentOutOfRangeException(nameof(kind), kind, "TableOuterJoinOp only supports Left/Right/Full — Inner uses TableJoinOp, Semi/Anti use TableSemiAntiOp.");
        if (leftKeys.Count == 0 || leftKeys.Count != rightKeys.Count)
            throw new ArgumentException($"Left/right key lists must be the same non-zero length (got {leftKeys.Count} vs {rightKeys.Count}).", nameof(rightKeys));

        _kind = kind;
        _leftKeys = leftKeys;
        _rightKeys = rightKeys;
        _residual = residual;
        _bindings = bindings;
        _padLeftUnmatched = kind is JoinKind.Left or JoinKind.Full;
        _padRightUnmatched = kind is JoinKind.Right or JoinKind.Full;
        _nullRight = WorkingRow.NullSide([rightSide]);
        _nullLeft = WorkingRow.NullSide(leftAliasesSoFar);
    }

    public IReadOnlyList<TableRowDelta> OnLeftBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableRowDelta>();
        foreach (var d in input) OnArrival(d.Row, d.Weight, isLeftSide: true, results);
        return results;
    }

    public IReadOnlyList<TableRowDelta> OnRightBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableRowDelta>();
        foreach (var d in input) OnArrival(d.Row, d.Weight, isLeftSide: false, results);
        return results;
    }

    /// <summary>
    /// One delta's worth of work, generalized over which physical side it arrived on. "Own" = the side
    /// this delta arrived on; "other" = the opposite side. <paramref name="isLeftSide"/> only ever
    /// controls (a) which literal ZSetIndex is "own" vs "other", (b) Combine() argument order (always
    /// canonical Combine(leftRow, rightRow), matching TableJoinOp), and (c) which null-pad
    /// (<see cref="_nullLeft"/>/<see cref="_nullRight"/>) plugs which hole. Everything else — whether
    /// THIS row gets its own pad, and whether the OTHER side's already-indexed rows need a flip — is
    /// driven purely by <see cref="_padLeftUnmatched"/>/<see cref="_padRightUnmatched"/>, which is what
    /// makes FULL "run both halves" for free: a LEFT arrival under FULL both pads itself (own) AND can
    /// flip the RIGHT side's pads (other), in the same call, with no kind-specific branch beyond those
    /// two booleans.
    /// </summary>
    private void OnArrival(WorkingRow row, long weight, bool isLeftSide, List<TableRowDelta> results)
    {
        var ownKeys = isLeftSide ? _leftKeys : _rightKeys;
        var ownIndex = isLeftSide ? Left : Right;
        var otherIndex = isLeftSide ? Right : Left;
        bool ownPads = isLeftSide ? _padLeftUnmatched : _padRightUnmatched;
        bool otherPads = isLeftSide ? _padRightUnmatched : _padLeftUnmatched;

        var keyValues = EvalKeyValues(ownKeys, row);
        if (keyValues is null)
        {
            // T3 / its mirror: NULL key is final and immediate — pad iff this side pads its own
            // unmatched rows; never indexed either way (see class doc's NULL-KEY RULES).
            if (ownPads) results.Add(new TableRowDelta(PadOwn(row, isLeftSide), weight));
            return;
        }
        var keyStr = TableKeyEncoding.EncodeGroupKey(keyValues);

        // --- T1 products against the OTHER side's current bucket, plus this row's own match count
        //     (drives this row's own pad below — same loop, no extra cost either way). ---
        int ownMatchCount = 0;
        foreach (var (otherRow, otherWeight) in otherIndex.Lookup(keyStr))
        {
            var combined = isLeftSide ? WorkingRow.Combine(row, otherRow) : WorkingRow.Combine(otherRow, row);
            if (!PassesResidual(combined)) continue;
            ownMatchCount++;
            results.Add(new TableRowDelta(combined, weight * otherWeight));
        }

        // --- OTHER side's pad-flip: snapshot "before" state (own bucket, as seen by each already-
        //     indexed other-side row) BEFORE this row is applied to the own index. Cheap key-level
        //     path when there's no residual (mirrors TableSemiAntiOp exactly); per-other-row path when
        //     there is one (see class doc's RESIDUAL COST). ---
        bool hasResidual = _residual is not null;
        bool wasPresentCheap = false;
        List<(WorkingRow Row, long Weight, bool WasMatched)>? perRowBefore = null;
        if (otherPads)
        {
            if (!hasResidual)
            {
                wasPresentCheap = ownIndex.Lookup(keyStr).Any();
            }
            else
            {
                perRowBefore = otherIndex.Lookup(keyStr)
                    .Select(o => (o.Row, o.Weight, WasMatched: OwnBucketHasMatch(ownIndex, keyStr, o.Row, isLeftSide)))
                    .ToList();
            }
        }

        // --- Index self (unconditional — future opposite-side arrivals must find this row regardless
        //     of whether either pad flag is set). ---
        var rowCanonical = JsonText.SerializeCanonicalRow(row.Fields);
        ownIndex.Apply(keyStr, rowCanonical, row, weight);

        // --- OTHER side's pad-flip: "after" state + re-emission. ---
        if (otherPads)
        {
            if (!hasResidual)
            {
                bool isPresentNow = ownIndex.Lookup(keyStr).Any();
                if (wasPresentCheap != isPresentNow)
                {
                    int sign = isPresentNow ? -1 : 1;
                    foreach (var (otherRow, otherWeight) in otherIndex.Lookup(keyStr))
                    {
                        results.Add(new TableRowDelta(PadOther(otherRow, isLeftSide), sign * otherWeight));
                    }
                }
            }
            else
            {
                foreach (var (otherRow, otherWeight, wasMatched) in perRowBefore!)
                {
                    bool isMatched = OwnBucketHasMatch(ownIndex, keyStr, otherRow, isLeftSide);
                    if (wasMatched == isMatched) continue;
                    int sign = isMatched ? -1 : 1;
                    results.Add(new TableRowDelta(PadOther(otherRow, isLeftSide), sign * otherWeight));
                }
            }
        }

        // --- This row's own pad (T2 / its mirror): only when unmatched and this side pads. ---
        if (ownPads && ownMatchCount == 0)
        {
            results.Add(new TableRowDelta(PadOwn(row, isLeftSide), weight));
        }
    }

    /// <summary>Evaluates a (possibly composite) key against a row; null the instant any component is
    /// null (T3's NULL rule — a composite key is NULL if ANY part is, per the class doc).</summary>
    private object?[]? EvalKeyValues(IReadOnlyList<Expr> keys, WorkingRow row)
    {
        var values = new object?[keys.Count];
        for (int i = 0; i < keys.Count; i++)
        {
            var v = ExpressionEvaluator.Eval(keys[i], new EvalContext(row, _bindings));
            if (v is null) return null;
            values[i] = v;
        }
        return values;
    }

    /// <summary>Does ANY row currently in <paramref name="ownIndex"/> under <paramref name="keyStr"/>
    /// residual-pass when combined with <paramref name="otherRow"/>? Used only on the residual path,
    /// once per already-indexed other-side row, both before and after the arriving delta is applied —
    /// the O(|L_k| · |R_k|) cost documented on the class.</summary>
    private bool OwnBucketHasMatch(ZSetIndex ownIndex, string keyStr, WorkingRow otherRow, bool isLeftSide)
    {
        foreach (var (ownRow, _) in ownIndex.Lookup(keyStr))
        {
            var combined = isLeftSide ? WorkingRow.Combine(ownRow, otherRow) : WorkingRow.Combine(otherRow, ownRow);
            if (PassesResidual(combined)) return true;
        }
        return false;
    }

    private bool PassesResidual(WorkingRow combined) =>
        _residual is null || ExpressionEvaluator.IsTrue(ExpressionEvaluator.Eval(_residual, new EvalContext(combined, _bindings)));

    /// <summary>Pads the ARRIVING row itself (T2/T3 and their mirror) — Combine(l, nullRight) for a
    /// left row, Combine(nullLeft, r) for a right row.</summary>
    private WorkingRow PadOwn(WorkingRow row, bool isLeftSide) =>
        isLeftSide ? WorkingRow.Combine(row, _nullRight) : WorkingRow.Combine(_nullLeft, row);

    /// <summary>Pads an ALREADY-INDEXED row on the OTHER side during a flip — the other side's rows are
    /// right rows when this delta arrived on the left (Combine(nullLeft, r)), and left rows when this
    /// delta arrived on the right (Combine(l, nullRight)).</summary>
    private WorkingRow PadOther(WorkingRow otherRow, bool isLeftSide) =>
        isLeftSide ? WorkingRow.Combine(_nullLeft, otherRow) : WorkingRow.Combine(otherRow, _nullRight);

    /// <summary>Pass-through — see TableJoinOp's identical OnFrontier doc: table mode has no epoch-
    /// driven eviction; this op's state (like TableJoinOp's) is unbounded and consolidated by design.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
