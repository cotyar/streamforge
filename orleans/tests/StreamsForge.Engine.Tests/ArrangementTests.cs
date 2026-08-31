using StreamsForge.Engine.Dataflow;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 003 M3 acceptance: keySpec canonicalization (same spec for semantically-equal keys, different for
/// different keys/partition counts), TableDataflowBuilder's arrangeability rule (marks a plain equi-join's
/// bare-column key arrangeable; does NOT mark a transformed key or a chained (i&gt;0) join's Left side
/// arrangeable), and a harness-level proof that two DIFFERENT tables sharing the same raw input each
/// converge to their own classic-executor baseline when fed that input's event stream — the property that
/// makes sharing the underlying indexed state across tables safe (Z-set/DBSP consolidation only cares about
/// the multiset of deltas observed, not which private-ingest-vs-shared-arrangement plumbing delivered them).
/// </summary>
public class ArrangementTests
{
    private readonly record struct Ev(string Origin, EventRecord Row, long Weight, bool IsTable);

    private static Ev S(string source, EventRecord row) => new(source, row, 1, false);
    private static Ev T(string table, EventRecord row, long weight) => new(table, row, weight, true);

    private static string Canon(TableExecutor exec) =>
        string.Join("\n", exec.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    private static string Canon(PartitionedTableHarness harness) =>
        string.Join("\n", harness.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    // ------------------------------------------------------------------------------------------------
    // ArrangementKeySpec: canonicalization + hashing, tested directly (no SQL involved).
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void KeySpec_SameFieldsAndPartitionCount_SameCanonicalFormAndHash()
    {
        var a = ArrangementKeySpec.Canonicalize(["symbol"], 4);
        var b = ArrangementKeySpec.Canonicalize(["symbol"], 4);
        Assert.Equal(a, b);
        Assert.Equal(ArrangementKeySpec.HashOf(a), ArrangementKeySpec.HashOf(b));
    }

    [Fact]
    public void KeySpec_DifferentField_DifferentCanonicalFormAndHash()
    {
        var a = ArrangementKeySpec.Canonicalize(["symbol"], 4);
        var b = ArrangementKeySpec.Canonicalize(["qty"], 4);
        Assert.NotEqual(a, b);
        Assert.NotEqual(ArrangementKeySpec.HashOf(a), ArrangementKeySpec.HashOf(b));
    }

    [Fact]
    public void KeySpec_DifferentPartitionCount_DifferentCanonicalFormAndHash()
    {
        // Different partition count is a genuinely different physical index (an arrangement partition is
        // exchanged 1:1, unre-hashed, into a consuming join stage's own partition — see TableDataflowPlan's
        // class doc) — must NOT collide even though the key fields are identical.
        var a = ArrangementKeySpec.Canonicalize(["symbol"], 2);
        var b = ArrangementKeySpec.Canonicalize(["symbol"], 4);
        Assert.NotEqual(a, b);
        Assert.NotEqual(ArrangementKeySpec.HashOf(a), ArrangementKeySpec.HashOf(b));
    }

    [Fact]
    public void KeySpec_FieldNameLengthPrefixed_NoDelimiterCollision()
    {
        // Two different (field-list) shapes that would collide under a naive "join with ';'" scheme don't
        // collide here because each field is length-prefixed.
        var a = ArrangementKeySpec.Canonicalize(["ab", "c"], 4);
        var b = ArrangementKeySpec.Canonicalize(["a", "bc"], 4);
        Assert.NotEqual(a, b);
    }

    // ------------------------------------------------------------------------------------------------
    // KeySpec canonicalization is ALIAS-independent: two tables joining "trades" on "symbol" via different
    // aliases must derive the identical keySpec for the "trades" edge (that's the whole sharing mechanism —
    // see GrainInterfaces.cs's M3 section doc).
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void KeySpec_SameForSemanticallyEqualJoin_DifferentAliasesAndSql()
    {
        var planA = CompileTable(
            "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol",
            [Trades], [Ref]).Plan!;
        var planB = CompileTable(
            "SELECT x.symbol, COUNT(*) AS cnt FROM trades x JOIN ref y ON x.symbol = y.symbol GROUP BY x.symbol",
            [Trades], [Ref]).Plan!;

        var dfA = planA.CreateDataflow(4);
        var dfB = planB.CreateDataflow(4);

        var tradesEdgeA = dfA.ArrangeableExternalEdges.Single(e => dfA.ExternalInputNameOf(e) == "trades");
        var tradesEdgeB = dfB.ArrangeableExternalEdges.Single(e => dfB.ExternalInputNameOf(e) == "trades");

        Assert.Equal(dfA.KeySpecOf(tradesEdgeA), dfB.KeySpecOf(tradesEdgeB));
        Assert.Equal(
            ArrangementKeySpec.HashOf(dfA.KeySpecOf(tradesEdgeA)),
            ArrangementKeySpec.HashOf(dfB.KeySpecOf(tradesEdgeB)));

        // "ref" is arrangeable too (the join's own dedicated Right ingest, bare-field-keyed) — but on a
        // DIFFERENT field-spec identity than "trades" (different input name), obviously not colliding.
        var refEdgeA = dfA.ArrangeableExternalEdges.Single(e => dfA.ExternalInputNameOf(e) == "ref");
        Assert.Equal(["symbol"], refEdgeA.ArrangeKeyFields);
    }

    // ------------------------------------------------------------------------------------------------
    // Builder arrangeability rule.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void Builder_PlainEquiJoin_BothSidesArrangeable_BareColumnKeys()
    {
        var plan = CompileTable(
            "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol",
            [Trades], [Ref]).Plan!;
        var dataflow = plan.CreateDataflow(4);

        Assert.Equal(2, dataflow.ArrangeableExternalEdges.Count);

        var tradesEdge = dataflow.ArrangeableExternalEdges.Single(e => dataflow.ExternalInputNameOf(e) == "trades");
        Assert.Equal(["symbol"], tradesEdge.ArrangeKeyFields);
        Assert.Equal("Left", tradesEdge.Role);

        var refEdge = dataflow.ArrangeableExternalEdges.Single(e => dataflow.ExternalInputNameOf(e) == "ref");
        Assert.Equal(["symbol"], refEdge.ArrangeKeyFields);
        Assert.Equal("Right", refEdge.Role);
    }

    [Fact]
    public void Builder_NoJoin_NothingArrangeable()
    {
        var plan = CompileTable("SELECT symbol, SUM(qty) AS total FROM trades GROUP BY symbol", Trades).Plan!;
        Assert.Empty(plan.CreateDataflow(4).ArrangeableExternalEdges);
    }

    [Fact]
    public void Builder_TransformedJoinKey_ThatSideNotArrangeable_OtherSideStillIs()
    {
        // Left key is a JSON-path extraction (JsonAccessExpr, a transform) -> NOT a bare own-field reference
        // -> not arrangeable. Right key (r.symbol) is still a bare reference -> still arrangeable. Proves the
        // check is per-side, not per-edge-pair.
        var plan = CompileTable(
            "SELECT e.eventType, r.tag FROM events e JOIN ref r ON e.payload ->> 'symbol' = r.symbol",
            [Events], [Ref]).Plan!;
        var dataflow = plan.CreateDataflow(4);

        Assert.DoesNotContain(dataflow.ArrangeableExternalEdges, e => dataflow.ExternalInputNameOf(e) == "events");
        var refEdge = Assert.Single(dataflow.ArrangeableExternalEdges);
        Assert.Equal("ref", dataflow.ExternalInputNameOf(refEdge));
        Assert.Equal(["symbol"], refEdge.ArrangeKeyFields);
    }

    [Fact]
    public void Builder_ChainedSecondJoin_LeftSideNotArrangeable_RightSideStillIs()
    {
        // Two joins: trades JOIN ref (hop 0) JOIN quotes (hop 1), both on symbol. The SECOND join's Left
        // side is fed by the first join's already-computed multi-alias WorkingRow (a "pre-join transform"
        // in the M3 SCOPE sense, even though t.symbol itself resolves via Bindings) — not the raw FROM
        // ingest — so it must NOT be arrangeable, even though its own RightKey (quotes' dedicated ingest,
        // bare column) still is.
        var plan = CompileTable(
            "SELECT t.symbol, r.tag, q.bid FROM trades t JOIN ref r ON t.symbol = r.symbol JOIN quotes q ON t.symbol = q.symbol",
            [Trades, Quotes], [Ref]).Plan!;
        var dataflow = plan.CreateDataflow(4);

        var joinStages = dataflow.Stages.Where(s => s.Kind == TableStageKind.Join).OrderBy(s => s.StageId).ToList();
        Assert.Equal(2, joinStages.Count);
        var secondJoin = joinStages[1];
        var secondLeftEdge = secondJoin.InEdges.Single(e => e.Role == "Left");
        Assert.Null(secondLeftEdge.ArrangeKeyFields); // chained hop -> not arrangeable

        // The first join's Left (trades, hop 0) and Right (ref) ARE arrangeable, and quotes (second join's
        // Right, its own dedicated ingest regardless of chain position) is too.
        Assert.Equal(3, dataflow.ArrangeableExternalEdges.Count);
        Assert.Contains(dataflow.ArrangeableExternalEdges, e => dataflow.ExternalInputNameOf(e) == "trades");
        Assert.Contains(dataflow.ArrangeableExternalEdges, e => dataflow.ExternalInputNameOf(e) == "ref");
        Assert.Contains(dataflow.ArrangeableExternalEdges, e => dataflow.ExternalInputNameOf(e) == "quotes");
    }

    [Fact]
    public void Builder_ScalarBroadcastEdge_NeverArrangeable()
    {
        // Broadcast (scalar-subquery residual) edges are never real hash-partitioned join-input indexes —
        // they run a private nested single-partition execution per partition (see TableDataflowPlan's class
        // doc) — so they must never appear in ArrangeableExternalEdges regardless of key shape.
        var plan = CompileTable("SELECT symbol, price - (SELECT AVG(price) FROM trades) AS rel FROM trades", Trades).Plan!;
        var dataflow = plan.CreateDataflow(4);
        Assert.Empty(dataflow.ArrangeableExternalEdges);
    }

    // ------------------------------------------------------------------------------------------------
    // Harness-level acceptance: two DIFFERENT tables sharing the same raw input ("trades", keyed on
    // "symbol") each converge to their OWN classic single-partition baseline when independently fed that
    // input's event stream — proving that sharing the underlying indexed state (an arrangement) across
    // differently-shaped consuming tables cannot corrupt either one, since DBSP/Z-set consolidation only
    // depends on the multiset of deltas observed, never on which plumbing (private ingest vs. shared
    // arrangement snapshot+deltas) delivered them.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void SharedArrangement_TwoDifferentTablesOverSameRawInput_BothConvergeToOwnClassicBaseline()
    {
        var sqlA = "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";
        var sqlB = "SELECT t.symbol, COUNT(*) AS legs, MAX(t.price) AS hi FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";

        var compileA = CompileTable(sqlA, [Trades], [Ref]);
        var compileB = CompileTable(sqlB, [Trades], [Ref]);
        Assert.True(compileA.Ok);
        Assert.True(compileB.Ok);

        var dataflowA = compileA.Plan!.CreateDataflow(4);
        var dataflowB = compileB.Plan!.CreateDataflow(4);

        // Both tables' "trades" edge resolves to the identical keySpec (same arrangement identity) — the
        // precondition for TableGrain's coordinator to attach BOTH to the SAME ArrangementGrain set.
        var tradesEdgeA = dataflowA.ArrangeableExternalEdges.Single(e => dataflowA.ExternalInputNameOf(e) == "trades");
        var tradesEdgeB = dataflowB.ArrangeableExternalEdges.Single(e => dataflowB.ExternalInputNameOf(e) == "trades");
        Assert.Equal(dataflowA.KeySpecOf(tradesEdgeA), dataflowB.KeySpecOf(tradesEdgeB));

        // The SAME raw event stream (as either a private ingest or a shared arrangement would deliver it)
        // feeds BOTH tables' harnesses.
        var events = new List<Ev>
        {
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1),
            T("ref", Evt(0, "ref", ("symbol", "MSFT"), ("tag", "watch")), 1),
            S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            S("trades", Evt(2, "trades", ("symbol", "AAPL"), ("price", 12.0), ("qty", 3L), ("active", true))),
            S("trades", Evt(3, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 7L), ("active", true))),
            S("trades", Evt(4, "trades", ("symbol", "GOOG"), ("price", 30.0), ("qty", 2L), ("active", true))), // no matching ref row
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1), // retraction cascades through both
        };

        var classicA = compileA.Plan!.CreateExecutor();
        var classicB = compileB.Plan!.CreateExecutor();
        var harnessA = new PartitionedTableHarness(dataflowA);
        var harnessB = new PartitionedTableHarness(dataflowB);

        foreach (var e in events)
        {
            if (e.IsTable)
            {
                classicA.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
                classicB.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
            }
            else
            {
                classicA.OnStreamEvent(e.Origin, e.Row);
                classicB.OnStreamEvent(e.Origin, e.Row);
            }
            harnessA.Admit(e.Origin, e.Row, e.Weight);
            harnessB.Admit(e.Origin, e.Row, e.Weight);
        }

        Assert.Equal(Canon(classicA), Canon(harnessA));
        Assert.Equal(Canon(classicB), Canon(harnessB));
        // And the two tables' own outputs genuinely differ (different SQL) — sanity check that this isn't
        // vacuously true because both happened to compute the same thing.
        Assert.NotEqual(Canon(harnessA), Canon(harnessB));
    }
}
