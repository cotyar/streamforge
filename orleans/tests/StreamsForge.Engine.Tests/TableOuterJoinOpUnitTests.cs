using StreamsForge.Engine.Dataflow;
using StreamsForge.Engine.Runtime;
using StreamsForge.Engine.Runtime.Ops;
using StreamsForge.Engine.Sql;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Op-level unit tests for <see cref="TableOuterJoinOp"/> (plan 008 wave 2a-B). Modeled on
/// TableOpsUnitTests' TableJoinOp section: the op is constructed directly (never through
/// TableExecutor — nothing wires this op yet) and driven with explicit deltas-in/deltas-out
/// assertions. Since the validator still rejects LEFT/RIGHT/FULL in table mode, every SQL-derived
/// test compiles the equivalent INNER join and hands its keys/bindings to TableOuterJoinOp's
/// constructor with an explicit JoinKind — the compiled ON clause (an equi-key + optional residual)
/// is identical regardless of which JoinKind ultimately consumes it. Composite-key tests build their
/// Expr/bindings by hand instead (today's ON-clause extractor only ever yields a single equi-key
/// pair; see Validator's JOIN resolution) — see CreateCompositeOp.
/// </summary>
public class TableOuterJoinOpUnitTests
{
    private static readonly Epoch E0 = new(0);
    private static readonly Epoch E1 = new(1);
    private static readonly Epoch E2 = new(2);

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// <summary>Compiles "trades t JOIN ref r ON t.symbol = r.symbol [AND &lt;extraOn&gt;]" as INNER
    /// (table mode has no outer-join grammar path yet) and builds a TableOuterJoinOp of the requested
    /// kind from the compiled keys/residual/bindings — exactly the hand-off a later wiring wave will
    /// perform once the validator grows LEFT/RIGHT/FULL support.</summary>
    private static TableOuterJoinOp CreateJoinOp(JoinKind kind, string? extraOn = null)
    {
        var on = extraOn is null ? "t.symbol = r.symbol" : $"t.symbol = r.symbol AND {extraOn}";
        var compiled = CompileTable($"SELECT t.symbol, r.tag FROM trades t JOIN ref r ON {on}", [Trades], [Ref]).Plan!.Compiled;
        var j = compiled.Joins[0];
        var leftSide = (compiled.Sources[0].Alias, compiled.Sources[0].Schema);
        var rightSide = (j.Alias, j.Schema);
        return new TableOuterJoinOp(kind, [j.LeftKey!], [j.RightKey!], j.Residual, compiled.Bindings, [leftSide], rightSide);
    }

    private static TableRowDelta[] Left(TableOuterJoinOp op, Epoch epoch, string symbol, long weight) =>
        [.. op.OnLeftBatch(epoch, new TableIngestOp("t").OnBatch(epoch, [new TableDelta(Evt(1000, "trades", ("symbol", symbol)), weight)]))];

    private static TableRowDelta[] Right(TableOuterJoinOp op, Epoch epoch, string? symbol, string tag, long weight) =>
        [.. op.OnRightBatch(epoch, new TableIngestOp("r").OnBatch(epoch, [new TableDelta(Evt(0, "ref", ("symbol", symbol), ("tag", tag)), weight)]))];

    private static bool IsPad(TableRowDelta d) => d.Row.Fields["t_symbol"] is null || d.Row.Fields["r_symbol"] is null;

    // ------------------------------------------------------------------
    // LEFT join — core T1/T2/T3 rules and the presence-flip machinery.
    // ------------------------------------------------------------------

    [Fact]
    public void LeftAssert_EmptyRightBucket_Pads_AndLeftIsIndexed()
    {
        var op = CreateJoinOp(JoinKind.Left);

        var outp = Left(op, E0, "AAPL", 1);

        var d = Assert.Single(outp);
        Assert.Equal(1, d.Weight);
        Assert.Equal("AAPL", d.Row.Fields["t_symbol"]);
        Assert.Null(d.Row.Fields["r_symbol"]);
        Assert.Null(d.Row.Fields["r_tag"]);

        Assert.Single(op.Left.Lookup(TableKeyEncoding.EncodeScalar("AAPL")));
    }

    [Fact]
    public void LeftAssert_NonEmptyRightBucket_ProductsOnly_NoPad()
    {
        var op = CreateJoinOp(JoinKind.Left);
        Right(op, E0, "AAPL", "watchlist", 2);

        var outp = Left(op, E1, "AAPL", 1);

        var d = Assert.Single(outp);
        Assert.Equal(2, d.Weight); // 1 * 2
        Assert.Equal("AAPL", d.Row.Fields["t_symbol"]);
        Assert.Equal("watchlist", d.Row.Fields["r_tag"]);
        Assert.False(IsPad(d));
    }

    [Fact]
    public void RightAssert_FirstForKey_ProductsAndPadRetraction_TheFlip()
    {
        var op = CreateJoinOp(JoinKind.Left);
        Left(op, E0, "AAPL", 1); // pads: right bucket is empty

        var outp = Right(op, E1, "AAPL", "watchlist", 2);

        Assert.Equal(2, outp.Length);
        var product = Assert.Single(outp, d => !IsPad(d));
        Assert.Equal(2, product.Weight); // 1 * 2
        var padRetraction = Assert.Single(outp, IsPad);
        Assert.Equal(-1, padRetraction.Weight); // retracts the earlier left pad (weight 1)
        Assert.Equal("AAPL", padRetraction.Row.Fields["t_symbol"]);
        Assert.Null(padRetraction.Row.Fields["r_symbol"]);
    }

    [Fact]
    public void RightAssert_SecondRowSameKey_ProductsOnly_NoPadTraffic()
    {
        // Load-bearing negative test: the flip is per-KEY presence (Right.Lookup(k).Any()), not
        // per-delta — a second right row under an already-present key must not re-trigger it.
        var op = CreateJoinOp(JoinKind.Left);
        Left(op, E0, "AAPL", 1);
        Right(op, E1, "AAPL", "watchlist", 1); // first: flips, pad retracts

        var outp = Right(op, E2, "AAPL", "core", 3);

        var d = Assert.Single(outp);
        Assert.False(IsPad(d));
        Assert.Equal(3, d.Weight); // 1 (left) * 3 (right)
        Assert.Equal("core", d.Row.Fields["r_tag"]);
    }

    [Fact]
    public void RightRetract_NotLastRow_NoPadTraffic()
    {
        var op = CreateJoinOp(JoinKind.Left);
        Left(op, E0, "AAPL", 1);
        Right(op, E1, "AAPL", "watchlist", 1);
        Right(op, E1, "AAPL", "core", 1); // two distinct right rows now present under AAPL

        var outp = Right(op, E2, "AAPL", "watchlist", -1); // retract one; the other keeps the key present

        var d = Assert.Single(outp);
        Assert.False(IsPad(d));
        Assert.Equal(-1, d.Weight);
        Assert.Equal("watchlist", d.Row.Fields["r_tag"]);
    }

    [Fact]
    public void RightRetract_LastRow_PadReassertion_ForEveryLeftRow()
    {
        var op = CreateJoinOp(JoinKind.Left);
        Left(op, E0, "AAPL", 1);
        Right(op, E1, "AAPL", "watchlist", 1);

        var outp = Right(op, E2, "AAPL", "watchlist", -1); // the only right row retracts

        Assert.Equal(2, outp.Length);
        var productRetraction = Assert.Single(outp, d => !IsPad(d));
        Assert.Equal(-1, productRetraction.Weight);
        var padReassertion = Assert.Single(outp, IsPad);
        Assert.Equal(1, padReassertion.Weight);
        Assert.Equal("AAPL", padReassertion.Row.Fields["t_symbol"]);
    }

    [Fact]
    public void LeftRetract_WhileUnmatched_PadRetraction_AndIndexPruning()
    {
        var op = CreateJoinOp(JoinKind.Left);
        Left(op, E0, "AAPL", 1); // pads (right empty)

        var outp = Left(op, E1, "AAPL", -1);

        var d = Assert.Single(outp);
        Assert.True(IsPad(d));
        Assert.Equal(-1, d.Weight);
        Assert.Empty(op.Left.Lookup(TableKeyEncoding.EncodeScalar("AAPL")));
    }

    [Fact]
    public void LeftRetract_WhileMatched_ProductRetractionOnly()
    {
        var op = CreateJoinOp(JoinKind.Left);
        Right(op, E0, "AAPL", "watchlist", 1);
        Left(op, E1, "AAPL", 1); // matched: product only, no pad

        var outp = Left(op, E2, "AAPL", -1);

        var d = Assert.Single(outp);
        Assert.False(IsPad(d));
        Assert.Equal(-1, d.Weight);
        Assert.Empty(op.Left.Lookup(TableKeyEncoding.EncodeScalar("AAPL")));
    }

    [Fact]
    public void LeftNullKey_PadEmitted_NotIndexed()
    {
        var op = CreateJoinOp(JoinKind.Left);

        var outp = op.OnLeftBatch(E0, new TableIngestOp("t").OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", null)), 1)]));

        var d = Assert.Single(outp);
        Assert.True(IsPad(d));
        Assert.Equal(1, d.Weight);
        Assert.Empty(op.Left.Lookup(TableKeyEncoding.EncodeScalar(null)));
    }

    [Fact]
    public void RightNullKey_NotIndexed_DoesNotMakeKeyPresent()
    {
        // Guards the worst bug in the spec: if a NULL-keyed right row were indexed under a shared
        // placeholder key, it would fake "presence" and silently suppress every left pad under that
        // key. Prove it has zero effect: the left pad survives, and a REAL right row for the same key
        // still triggers the normal flip afterwards.
        var op = CreateJoinOp(JoinKind.Left);
        var leftPad = Left(op, E0, "AAPL", 1);
        Assert.Single(leftPad); // sanity: padded, right bucket empty

        var nullOutp = Right(op, E1, null, "whatever", 1);

        Assert.Empty(nullOutp);
        Assert.Empty(op.Right.Lookup(TableKeyEncoding.EncodeScalar(null)));

        // A genuine right row for AAPL afterwards must still see the normal flip — proving the NULL
        // row above never made ANY key (least of all "AAPL") look present.
        var realOutp = Right(op, E2, "AAPL", "watchlist", 1);
        Assert.Equal(2, realOutp.Length);
        Assert.Single(realOutp, d => !IsPad(d));
        var flip = Assert.Single(realOutp, IsPad);
        Assert.Equal(-1, flip.Weight);
    }

    [Fact]
    public void OutOfOrder_RetractThenAssert_SelfHeals()
    {
        // The exact trace from the op's doc comment: R(r,-1), L(l,+1), R(r,+1) must net to
        // products 0 and pad +1 — proof that presence is "!= 0", not "> 0".
        var op = CreateJoinOp(JoinKind.Left);

        var step1 = Right(op, E0, "AAPL", "watchlist", -1);
        var step2 = Left(op, E0, "AAPL", 1);
        var step3 = Right(op, E0, "AAPL", "watchlist", 1);

        long productNet = step1.Concat(step2).Concat(step3).Where(d => !IsPad(d)).Sum(d => d.Weight);
        long padNet = step1.Concat(step2).Concat(step3).Where(IsPad).Sum(d => d.Weight);

        Assert.Equal(0, productNet);
        Assert.Equal(1, padNet);
    }

    // ------------------------------------------------------------------
    // RIGHT mirrors LEFT; FULL runs both halves and never emits (null, null).
    // ------------------------------------------------------------------

    [Fact]
    public void RightJoin_MirrorsLeft_OwnPadThenFlipOnOppositeArrival()
    {
        var op = CreateJoinOp(JoinKind.Right);

        // A lone right row with no left match: pads itself (mirror of T2), and IS indexed.
        var rightAlone = Right(op, E0, "AAPL", "watchlist", 1);
        var pad = Assert.Single(rightAlone);
        Assert.True(IsPad(pad));
        Assert.Null(pad.Row.Fields["t_symbol"]);
        Assert.Equal("watchlist", pad.Row.Fields["r_tag"]);

        // A matching left row arrives: product fires, and the right row's earlier pad retracts (the
        // mirrored flip, detected on the Left index per the class doc).
        var leftArrival = Left(op, E1, "AAPL", 1);
        Assert.Equal(2, leftArrival.Length);
        var product = Assert.Single(leftArrival, d => !IsPad(d));
        Assert.Equal(1, product.Weight);
        var padRetraction = Assert.Single(leftArrival, IsPad);
        Assert.Equal(-1, padRetraction.Weight);
    }

    [Fact]
    public void FullJoin_PadsBothSides_NeverEmitsNullNullRow()
    {
        var op = CreateJoinOp(JoinKind.Full);
        var all = new List<TableRowDelta>();

        all.AddRange(Left(op, E0, "AAPL", 1));   // unmatched left -> pads itself
        all.AddRange(Right(op, E1, "MSFT", "other", 1)); // unmatched right (different key) -> pads itself
        all.AddRange(Right(op, E2, "AAPL", "watchlist", 1)); // matches the left row: product + left's pad retracts

        Assert.Equal(4, all.Count); // left pad, right pad, product, left-pad retraction
        foreach (var d in all)
        {
            bool leftNull = d.Row.Fields["t_symbol"] is null;
            bool rightNull = d.Row.Fields["r_symbol"] is null;
            Assert.False(leftNull && rightNull);
        }

        var leftPad = Assert.Single(all, d => d.Row.Fields["r_symbol"] is null && d.Weight > 0);
        Assert.Equal(1, leftPad.Weight);
        var rightPad = Assert.Single(all, d => d.Row.Fields["t_symbol"] is null);
        Assert.Equal(1, rightPad.Weight);
        var product = Assert.Single(all, d => d.Row.Fields["t_symbol"] is not null && d.Row.Fields["r_symbol"] is not null && d.Weight > 0);
        Assert.Equal(1, product.Weight);
        var retraction = Assert.Single(all, d => d.Row.Fields["r_symbol"] is null && d.Weight < 0);
        Assert.Equal(-1, retraction.Weight);
    }

    // ------------------------------------------------------------------
    // Composite keys — hand-built Expr/bindings (validator doesn't produce key lists yet).
    // ------------------------------------------------------------------

    private static WorkingRow Row(string alias, params (string Field, object? Value)[] fields)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var (f, v) in fields) dict[$"{alias}_{f}"] = v;
        return new WorkingRow { Ts = 0, Aliases = [alias], Fields = dict };
    }

    private static (TableOuterJoinOp Op, Identifier La, Identifier Lb, Identifier Ra, Identifier Rb) CreateCompositeOp(JoinKind kind, Expr? residual = null, Dictionary<Expr, (string, string)>? extraBindings = null)
    {
        var la = new Identifier("l.a", 0, 0);
        var lb = new Identifier("l.b", 0, 0);
        var ra = new Identifier("r.a", 0, 0);
        var rb = new Identifier("r.b", 0, 0);
        var bindings = new Dictionary<Expr, (string, string)>
        {
            [la] = ("l", "a"),
            [lb] = ("l", "b"),
            [ra] = ("r", "a"),
            [rb] = ("r", "b"),
        };
        if (extraBindings is not null) foreach (var kv in extraBindings) bindings[kv.Key] = kv.Value;

        var leftSchema = Schema("lsrc", ("a", FieldKind.String), ("b", FieldKind.String));
        var rightSchema = Schema("rsrc", ("a", FieldKind.String), ("b", FieldKind.String));
        var op = new TableOuterJoinOp(kind, [la, lb], [ra, rb], residual, bindings, [("l", leftSchema)], ("r", rightSchema));
        return (op, la, lb, ra, rb);
    }

    [Fact]
    public void CompositeKey_Matching_ProductsAcrossBothComponents()
    {
        var (op, _, _, _, _) = CreateCompositeOp(JoinKind.Left);
        op.OnRightBatch(E0, [new TableRowDelta(Row("r", ("a", "X"), ("b", "Y")), 1)]);

        var outp = op.OnLeftBatch(E1, [new TableRowDelta(Row("l", ("a", "X"), ("b", "Y")), 2)]);

        var d = Assert.Single(outp);
        Assert.Equal(2, d.Weight);
        Assert.Equal("X", d.Row.Fields["l_a"]);
        Assert.Equal("Y", d.Row.Fields["r_b"]);
    }

    [Fact]
    public void CompositeKey_NonMatching_OneComponentDiffers_Pads()
    {
        var (op, _, _, _, _) = CreateCompositeOp(JoinKind.Left);
        op.OnRightBatch(E0, [new TableRowDelta(Row("r", ("a", "X"), ("b", "Z")), 1)]); // b differs

        var outp = op.OnLeftBatch(E1, [new TableRowDelta(Row("l", ("a", "X"), ("b", "Y")), 1)]);

        var d = Assert.Single(outp);
        Assert.Null(d.Row.Fields["r_a"]);
        Assert.Equal(1, d.Weight);
    }

    [Fact]
    public void CompositeKey_NullInOneComponent_PadsAndIsNotIndexed()
    {
        var (op, _, _, _, _) = CreateCompositeOp(JoinKind.Left);

        var outp = op.OnLeftBatch(E0, [new TableRowDelta(Row("l", ("a", "X"), ("b", null)), 1)]);

        var d = Assert.Single(outp);
        Assert.Equal("X", d.Row.Fields["l_a"]);
        Assert.Null(d.Row.Fields["r_a"]);
        Assert.Equal(1, d.Weight);

        // Not indexed under any encoding that includes this row's non-null component.
        Assert.Empty(op.Left.Lookup(TableKeyEncoding.EncodeGroupKey(["X", null!])));
    }

    // ------------------------------------------------------------------
    // Residual predicate — breaks the cheap key-level presence test; must be evaluated per left row.
    // ------------------------------------------------------------------

    [Fact]
    public void Residual_NonEmptyBucketButNoPassingCandidate_StillPads_ThenUnpadsWhenOnePasses()
    {
        // "t.symbol = r.tag" as the residual: passes only when the left row's own symbol equals the
        // right row's tag — a plain "is the bucket non-empty" test would wrongly report present.
        var op = CreateJoinOp(JoinKind.Left, extraOn: "t.symbol = r.tag");

        Right(op, E0, "AAPL", "watchlist", 1); // bucket becomes non-empty, but "AAPL" != "watchlist"

        var leftOutp = Left(op, E1, "AAPL", 1);
        var pad = Assert.Single(leftOutp);
        Assert.True(IsPad(pad));
        Assert.Equal(1, pad.Weight);

        var rightOutp = Right(op, E2, "AAPL", "AAPL", 1); // this candidate DOES pass ("AAPL" == "AAPL")

        Assert.Equal(2, rightOutp.Length);
        var product = Assert.Single(rightOutp, d => !IsPad(d));
        Assert.Equal(1, product.Weight);
        Assert.Equal("AAPL", product.Row.Fields["r_tag"]);
        var unpad = Assert.Single(rightOutp, IsPad);
        Assert.Equal(-1, unpad.Weight); // retracts the earlier pad
    }

    // ------------------------------------------------------------------
    // OnFrontier
    // ------------------------------------------------------------------

    [Fact]
    public void OnFrontierIsADocumentedPassThrough()
    {
        var op = CreateJoinOp(JoinKind.Full);
        Assert.Empty(op.OnFrontier(E0));
        Assert.Empty(op.OnFrontier(new Epoch(999)));
    }
}
