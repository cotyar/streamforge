using StreamForge.Engine.Dataflow;
using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 008 wave 2c-A: arrangement-sharing coverage for table-mode LEFT/RIGHT/FULL joins, modeled on
/// ArrangementTests.cs -- specifically SharedArrangement_TwoDifferentTablesOverSameRawInput_..., generalized
/// to a table PAIR where one does plain INNER and the other does LEFT over the identical raw input and key.
/// That pairing is what would catch a divergence between ArrangementKeySpec.PartitionOfRow and
/// TableDataflowPlan.PartitionOf, or between "consolidated snapshot seed then live deltas" and "raw delta
/// replay" for TableOuterJoinOp's presence-flip logic -- invariants of code in a DIFFERENT project
/// (StreamForge.Host) that nothing in this project currently forces to stay true, since ArrangementTests
/// itself only ever pairs two INNER-shaped tables.
/// </summary>
public class TableOuterJoinArrangementTests
{
    private readonly record struct Ev(string Origin, EventRecord Row, long Weight, bool IsTable);

    private static Ev S(string source, EventRecord row) => new(source, row, 1, false);
    private static Ev T(string table, EventRecord row, long weight) => new(table, row, weight, true);

    private static string Canon(TableExecutor exec) =>
        string.Join("\n", exec.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    private static string Canon(PartitionedTableHarness harness) =>
        string.Join("\n", harness.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    private const string LeftSql = "SELECT t.symbol, r.tag FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol";
    private const string FullSql = "SELECT t.symbol, r.tag FROM trades t FULL JOIN ref r ON t.symbol = r.symbol";

    // ------------------------------------------------------------------------------------------------
    // The important one: two DIFFERENT tables over the SAME raw input+key, one INNER and one LEFT, at the
    // SAME partition count -- both must converge to their own classic single-partition baseline.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void SharedArrangement_InnerAndLeftOverSameRawInputAndKey_BothConvergeToOwnClassicBaseline()
    {
        const string sqlInner = "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";
        const string sqlLeft = LeftSql;

        var compileInner = CompileTable(sqlInner, [Trades], [Ref]);
        var compileLeft = CompileTable(sqlLeft, [Trades], [Ref]);
        Assert.True(compileInner.Ok);
        Assert.True(compileLeft.Ok);

        var dataflowInner = compileInner.Plan!.CreateDataflow(4);
        var dataflowLeft = compileLeft.Plan!.CreateDataflow(4);

        // Precondition for sharing: both tables' "trades" edge resolves to the identical keySpec (the
        // TableGrain coordinator's attach-to-the-same-ArrangementGrain-set test).
        var tradesEdgeInner = dataflowInner.ArrangeableExternalEdges.Single(e => dataflowInner.ExternalInputNameOf(e) == "trades");
        var tradesEdgeLeft = dataflowLeft.ArrangeableExternalEdges.Single(e => dataflowLeft.ExternalInputNameOf(e) == "trades");
        Assert.Equal(dataflowInner.KeySpecOf(tradesEdgeInner), dataflowLeft.KeySpecOf(tradesEdgeLeft));

        // The SAME raw event stream feeds BOTH tables' classic executors and BOTH tables' harnesses.
        var events = new List<Ev>
        {
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1),
            S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            S("trades", Evt(2, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 7L), ("active", true))), // no ref row: INNER drops it, LEFT null-pads it
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1), // retraction cascades through both
        };

        var classicInner = compileInner.Plan!.CreateExecutor();
        var classicLeft = compileLeft.Plan!.CreateExecutor();
        var harnessInner = new PartitionedTableHarness(dataflowInner);
        var harnessLeft = new PartitionedTableHarness(dataflowLeft);

        foreach (var e in events)
        {
            if (e.IsTable)
            {
                classicInner.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
                classicLeft.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
            }
            else
            {
                classicInner.OnStreamEvent(e.Origin, e.Row);
                classicLeft.OnStreamEvent(e.Origin, e.Row);
            }
            harnessInner.Admit(e.Origin, e.Row, e.Weight);
            harnessLeft.Admit(e.Origin, e.Row, e.Weight);
        }

        Assert.Equal(Canon(classicInner), Canon(harnessInner));
        Assert.Equal(Canon(classicLeft), Canon(harnessLeft));

        // Sanity: genuinely different outputs, not vacuously equal -- the retraction leaves INNER's only
        // surviving group empty (AAPL's own match unwound too), while LEFT still carries both rows,
        // NULL-padded (MSFT never matched; AAPL re-padded once its match retracted).
        Assert.NotEqual(Canon(harnessInner), Canon(harnessLeft));
        Assert.Empty(harnessInner.Snapshot());
        Assert.Equal(2, harnessLeft.Snapshot().Count);
        Assert.Contains(harnessLeft.Snapshot().Values, v => (string?)v.Row["symbol"] == "MSFT" && v.Row["tag"] is null);
        Assert.Contains(harnessLeft.Snapshot().Values, v => (string?)v.Row["symbol"] == "AAPL" && v.Row["tag"] is null);
    }

    // ------------------------------------------------------------------------------------------------
    // A LEFT join's right edge is still arrangeable for a bare-column key -- same rule non-outer joins get
    // (ArrangementTests.Builder_PlainEquiJoin_BothSidesArrangeable_BareColumnKeys), unaffected by JoinKind.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void LeftJoin_BothEdgesArrangeable_BareColumnKeys()
    {
        var plan = CompileTable(LeftSql, [Trades], [Ref]).Plan!;
        var dataflow = plan.CreateDataflow(4);

        Assert.Equal(2, dataflow.ArrangeableExternalEdges.Count);

        var tradesEdge = dataflow.ArrangeableExternalEdges.Single(e => dataflow.ExternalInputNameOf(e) == "trades");
        Assert.Equal(["symbol"], tradesEdge.ArrangeKeyFields);
        Assert.Equal("Left", tradesEdge.Role);

        var refEdge = dataflow.ArrangeableExternalEdges.Single(e => dataflow.ExternalInputNameOf(e) == "ref");
        Assert.Equal(["symbol"], refEdge.ArrangeKeyFields);
        Assert.Equal("Right", refEdge.Role);
    }

    // ------------------------------------------------------------------------------------------------
    // FULL join: both sides still arrangeable too -- FULL pads both sides, but arrangeability only cares
    // about "is this side's key a bare own-field reference", unaffected by which sides pad.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void FullJoin_BothEdgesArrangeable_BareColumnKeys()
    {
        var plan = CompileTable(FullSql, [Trades], [Ref]).Plan!;
        var dataflow = plan.CreateDataflow(4);

        Assert.Equal(2, dataflow.ArrangeableExternalEdges.Count);

        var tradesEdge = dataflow.ArrangeableExternalEdges.Single(e => dataflow.ExternalInputNameOf(e) == "trades");
        Assert.Equal(["symbol"], tradesEdge.ArrangeKeyFields);
        Assert.Equal("Left", tradesEdge.Role);

        var refEdge = dataflow.ArrangeableExternalEdges.Single(e => dataflow.ExternalInputNameOf(e) == "ref");
        Assert.Equal(["symbol"], refEdge.ArrangeKeyFields);
        Assert.Equal("Right", refEdge.Role);
    }
}
