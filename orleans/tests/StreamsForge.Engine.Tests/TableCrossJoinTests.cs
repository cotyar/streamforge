using StreamsForge.Engine.Dataflow;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 008 W2-A: CROSS JOIN in table mode. Unlike TableJoinTests' equi-join, there is no join key at all —
/// every left row must match every right row (the cartesian product), which TablePlanner realizes by
/// synthesizing a constant key (NumberLiteral(0)) on both sides of the same TableJoinOp equi-join used for
/// INNER — see TablePlanner.BuildCompiledTablePlan and TableJoinOp's class doc. No new op, no new runtime
/// code path; this file exercises that the trick actually produces cartesian-product-with-weight-
/// multiplication semantics end to end, plus the M2 partitioned-execution guardrail (a constant key would
/// silently serialize a multi-partition table, so CreateDataflow rejects Parallelism &gt; 1 for CROSS —
/// mirrors PartitionedDataflowTests.DerivedTableInFromPosition_CreateDataflowThrows's shape).
/// </summary>
public class TableCrossJoinTests
{
    private const string Sql = "SELECT t.symbol, r.tag FROM trades t CROSS JOIN ref r";

    private static string Canon(TableExecutor exec) =>
        string.Join("\n", exec.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    private static string Canon(PartitionedTableHarness harness) =>
        string.Join("\n", harness.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    [Fact]
    public void CrossJoinCompilesInTableMode()
    {
        var r = CompileTable(Sql, [Trades], [Ref]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void CartesianProductMultipliesWeights()
    {
        var exec = CompileTableAndCreate(Sql, [Trades], [Ref]);

        // ref (a table input) arrives first with nothing on the trades side yet -> no match, no output —
        // same "one side arrives first" shape as TableJoinTests.LeftArrivalMatchesAlreadyIndexedRightSide,
        // just with no ON clause to satisfy (there is none; every row matches unconditionally).
        var noMatch = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 2));
        Assert.Empty(noMatch);

        var matched = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));
        var delta = Assert.Single(matched);
        Assert.Equal(2, delta.Weight); // weight(trades)=1 * weight(ref)=2 — the whole point of a cross join
        Assert.Equal("AAPL", delta.Row["symbol"]);
        Assert.Equal("watchlist", delta.Row["tag"]);
    }

    [Fact]
    public void RetractionCascadesANegativeWeightDeltaAndPrunesTheSnapshot()
    {
        var exec = CompileTableAndCreate(Sql, [Trades], [Ref]);

        exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));
        var matched = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 2));
        Assert.Single(matched);
        Assert.Single(exec.Snapshot());

        var retracted = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), -2));
        var delta = Assert.Single(retracted);
        Assert.Equal(-2, delta.Weight);
        Assert.Equal("AAPL", delta.Row["symbol"]);
        Assert.Equal("watchlist", delta.Row["tag"]);

        // Consolidated: the earlier +2 and this -2 net to zero -> the row is pruned from the snapshot.
        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void SnapshotMatchesTheFullCartesianSet()
    {
        var exec = CompileTableAndCreate(Sql, [Trades], [Ref]);

        exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));
        exec.OnStreamEvent("trades", Evt(1001, "trades", ("symbol", "MSFT"), ("price", 50.0), ("qty", 5L), ("active", true)));
        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "core-tag"), ("tag", "core")), 1));
        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "watch-tag"), ("tag", "watch")), 1));

        // No ON clause -> every trades row combines with every ref row: 2 x 2 = 4 rows, each weight 1,
        // regardless of the (deliberately) mismatched "symbol" values -- a cross join ignores key equality
        // entirely, which is exactly what distinguishes it from the equi-join TableJoinTests covers.
        var snapshot = exec.Snapshot();
        Assert.Equal(4, snapshot.Count);
        Assert.All(snapshot.Values, v => Assert.Equal(1, v.Weight));

        var pairs = snapshot.Values.Select(v => ((string)v.Row["symbol"]!, (string)v.Row["tag"]!)).OrderBy(p => p).ToList();
        var expected = new[] { ("AAPL", "core"), ("AAPL", "watch"), ("MSFT", "core"), ("MSFT", "watch") }.OrderBy(p => p).ToList();
        Assert.Equal(expected, pairs);
    }

    [Fact]
    public void PlanSummaryRendersTheCrossSymbol()
    {
        var r = CompileTable(Sql, [Trades], [Ref]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("×[CROSS] ref AS r", r.PlanSummary);
    }

    [Fact]
    public void CrossJoinEdgesAreNeverArrangeable()
    {
        var plan = CompileTable(Sql, [Trades], [Ref]).Plan!;
        // Parallelism = 1 -> CreateDataflow succeeds (see CreateDataflowThrowsAboveParallelism1 below for the
        // >1 guardrail); TablePlanner's synthesized NumberLiteral(0) key is not an Identifier/QualifiedIdentifier,
        // so TableDataflowBuilder.IsBareOwnFieldRef rejects it on both sides -> no arrangeable external edge
        // at all for this plan, unlike a real equi-join (see ArrangementTests.Builder_PlainEquiJoin_...).
        var dataflow = plan.CreateDataflow(1);
        Assert.Empty(dataflow.ArrangeableExternalEdges);
    }

    [Fact]
    public void CreateDataflowThrowsAboveParallelism1()
    {
        var result = CompileTable(Sql, [Trades], [Ref]);
        Assert.True(result.Ok);
        var ex = Assert.Throws<NotSupportedException>(() => result.Plan!.CreateDataflow(4));
        Assert.Contains("Use Parallelism = 1", ex.Message);
        Assert.Contains("CROSS JOIN", ex.Message);
    }

    [Fact]
    public void AtParallelism1ResultsMatchTheClassicExecutor()
    {
        var result = CompileTable(Sql, [Trades], [Ref]);
        Assert.True(result.Ok);
        var plan = result.Plan!;

        var classic = plan.CreateExecutor();
        var harness = new PartitionedTableHarness(plan.CreateDataflow(1));

        var events = new (string Origin, EventRecord Row, long Weight, bool IsTable)[]
        {
            ("ref", Evt(0, "ref", ("symbol", "core-tag"), ("tag", "core")), 1, true),
            ("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)), 1, false),
            ("trades", Evt(1001, "trades", ("symbol", "MSFT"), ("price", 50.0), ("qty", 5L), ("active", true)), 1, false),
            ("ref", Evt(0, "ref", ("symbol", "watch-tag"), ("tag", "watch")), 1, true),
            ("ref", Evt(0, "ref", ("symbol", "core-tag"), ("tag", "core")), -1, true), // retraction cascades too
        };

        foreach (var e in events)
        {
            if (e.IsTable) classic.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
            else classic.OnStreamEvent(e.Origin, e.Row);
            harness.Admit(e.Origin, e.Row, e.Weight);
        }

        Assert.Equal(Canon(classic), Canon(harness));
        Assert.NotEmpty(classic.Snapshot()); // sanity: the comparison isn't vacuously true over an empty set
    }
}
