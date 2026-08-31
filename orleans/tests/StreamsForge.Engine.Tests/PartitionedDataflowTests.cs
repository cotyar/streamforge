using StreamsForge.Engine.Dataflow;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 003 M2 acceptance §5: stage-graph construction (5a), determinism (5b), and the equivalence oracle
/// (5c) — classic single-partition <see cref="TableExecutor"/> vs the partitioned <see cref="PartitionedTableHarness"/>
/// at P=1 and P=4, for six representative plan shapes: aggregate-only, join+aggregate, unnest+aggregate,
/// latest-by, semi-join, and scalar broadcast.
/// </summary>
public class PartitionedDataflowTests
{
    private readonly record struct Ev(string Origin, EventRecord Row, long Weight, bool IsTable);

    private static Ev S(string source, EventRecord row) => new(source, row, 1, false);
    private static Ev T(string table, EventRecord row, long weight) => new(table, row, weight, true);

    private static string Canon(TableExecutor exec) =>
        string.Join("\n", exec.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    private static string Canon(PartitionedTableHarness harness) =>
        string.Join("\n", harness.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    private static void AssertEquivalence(TableCompileResult compileResult, IReadOnlyList<Ev> events, int[] partitionCounts)
    {
        Assert.True(compileResult.Ok, string.Join("; ", compileResult.Diagnostics.Select(d => d.Message)));
        var plan = compileResult.Plan!;

        var classic = plan.CreateExecutor();
        foreach (var e in events)
        {
            if (e.IsTable) classic.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
            else classic.OnStreamEvent(e.Origin, e.Row);
        }
        var classicCanon = Canon(classic);

        foreach (var p in partitionCounts)
        {
            var dataflow = plan.CreateDataflow(p);
            var harness = new PartitionedTableHarness(dataflow);
            foreach (var e in events) harness.Admit(e.Origin, e.Row, e.Weight);
            Assert.Equal(classicCanon, Canon(harness));
        }
    }

    /// <summary>Same event sequence fed to fresh P=4 harnesses twice — once draining FIFO, once LIFO —
    /// asserts identical consolidated output (plan 003 M2 acceptance 5b: "fed identical batch sets in
    /// different arrival orders ⇒ identical consolidated output").</summary>
    private static void AssertDeterministic(TableCompileResult compileResult, IReadOnlyList<Ev> events, int partitionCount = 4)
    {
        Assert.True(compileResult.Ok);
        var plan = compileResult.Plan!;

        var fifoHarness = new PartitionedTableHarness(plan.CreateDataflow(partitionCount));
        foreach (var e in events) fifoHarness.Admit(e.Origin, e.Row, e.Weight, lifo: false);

        var lifoHarness = new PartitionedTableHarness(plan.CreateDataflow(partitionCount));
        foreach (var e in events) lifoHarness.Admit(e.Origin, e.Row, e.Weight, lifo: true);

        var reversedHarness = new PartitionedTableHarness(plan.CreateDataflow(partitionCount));
        foreach (var e in events.Reverse()) reversedHarness.Admit(e.Origin, e.Row, e.Weight, lifo: false);

        var fifo = Canon(fifoHarness);
        Assert.Equal(fifo, Canon(lifoHarness));
        // A fully reversed ADMISSION order isn't guaranteed to match for plans with order-sensitive
        // semantics (LATEST BY: "last write wins" depends on timestamp, not arrival order, so it DOES
        // still hold — see the LatestBy-specific test below using this same helper). For plans where it
        // holds (every representative shape here), assert it too — the stronger claim.
        Assert.Equal(fifo, Canon(reversedHarness));
    }

    // ------------------------------------------------------------------------------------------------
    // 1) Aggregate-only: no JOIN, straight into Reduce.
    // ------------------------------------------------------------------------------------------------

    private const string AggSql = "SELECT symbol, SUM(qty) AS total FROM trades GROUP BY symbol";

    [Fact]
    public void AggregateOnly_StageGraph()
    {
        var plan = CompileTable(AggSql, Trades).Plan!;
        var dataflow = plan.CreateDataflow(4);

        Assert.Equal([TableStageKind.Ingest, TableStageKind.FilterProject, TableStageKind.Reduce], dataflow.Stages.Select(s => s.Kind));
        var ingestToFp = dataflow.Edges.Single(e => e.FromStageId == dataflow.Stages[0].StageId);
        Assert.Equal(TableEdgeMode.HashPartition, ingestToFp.Mode); // no join -> content-hash fan-out out of the 1-partition ingest
        var fpToReduce = dataflow.Edges.Single(e => e.FromStageId == dataflow.Stages[1].StageId);
        Assert.Equal(TableEdgeMode.HashPartition, fpToReduce.Mode); // partitioned on GROUP BY key
        Assert.Equal(TableEdgeMode.Gather, dataflow.TerminalEdge.Mode);
        Assert.Equal(dataflow.Stages[2].StageId, dataflow.TerminalEdge.FromStageId);
    }

    [Fact]
    public void AggregateOnly_Equivalence()
    {
        var events = new List<Ev>
        {
            S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            S("trades", Evt(2, "trades", ("symbol", "AAPL"), ("price", 11.0), ("qty", 3L), ("active", true))),
            S("trades", Evt(3, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 7L), ("active", true))),
            S("trades", Evt(4, "trades", ("symbol", "GOOG"), ("price", 30.0), ("qty", 2L), ("active", true))),
        };
        AssertEquivalence(CompileTable(AggSql, Trades), events, [1, 4]);
    }

    [Fact]
    public void AggregateOnly_Deterministic() =>
        AssertDeterministic(CompileTable(AggSql, Trades), [
            S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            S("trades", Evt(2, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 7L), ("active", true))),
            S("trades", Evt(3, "trades", ("symbol", "AAPL"), ("price", 11.0), ("qty", 1L), ("active", true))),
            S("trades", Evt(4, "trades", ("symbol", "GOOG"), ("price", 30.0), ("qty", 2L), ("active", true))),
        ]);

    // ------------------------------------------------------------------------------------------------
    // 2) Join + aggregate: trades (stream) JOIN ref (table) ON symbol, GROUP BY.
    // ------------------------------------------------------------------------------------------------

    private const string JoinAggSql = "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";

    [Fact]
    public void JoinAggregate_StageGraph()
    {
        var plan = CompileTable(JoinAggSql, [Trades], [Ref]).Plan!;
        var dataflow = plan.CreateDataflow(4);

        Assert.Equal([TableStageKind.Ingest, TableStageKind.Ingest, TableStageKind.Join, TableStageKind.FilterProject, TableStageKind.Reduce],
            dataflow.Stages.Select(s => s.Kind));

        var joinStage = dataflow.Stages.Single(s => s.Kind == TableStageKind.Join);
        Assert.Equal(2, joinStage.InEdges.Count);
        Assert.All(joinStage.InEdges, e => Assert.Equal(TableEdgeMode.HashPartition, e.Mode)); // co-partitioned on the join key
        Assert.Equal(["Left", "Right"], joinStage.InEdges.Select(e => e.Role));
    }

    [Fact]
    public void JoinAggregate_Equivalence()
    {
        var events = new List<Ev>
        {
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1),
            T("ref", Evt(0, "ref", ("symbol", "MSFT"), ("tag", "watch")), 1),
            S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            S("trades", Evt(2, "trades", ("symbol", "AAPL"), ("price", 11.0), ("qty", 3L), ("active", true))),
            S("trades", Evt(3, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 7L), ("active", true))),
            S("trades", Evt(4, "trades", ("symbol", "GOOG"), ("price", 30.0), ("qty", 2L), ("active", true))), // no matching ref row
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1), // retraction cascades through the join
        };
        AssertEquivalence(CompileTable(JoinAggSql, [Trades], [Ref]), events, [1, 4]);
    }

    [Fact]
    public void JoinAggregate_Deterministic() =>
        AssertDeterministic(CompileTable(JoinAggSql, [Trades], [Ref]), [
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1),
            S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            T("ref", Evt(0, "ref", ("symbol", "MSFT"), ("tag", "watch")), 1),
            S("trades", Evt(2, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 7L), ("active", true))),
            S("trades", Evt(3, "trades", ("symbol", "AAPL"), ("price", 11.0), ("qty", 3L), ("active", true))),
        ]);

    // ------------------------------------------------------------------------------------------------
    // 3) Unnest + aggregate: structures CROSS JOIN UNNEST(legs), GROUP BY trade_id.
    // ------------------------------------------------------------------------------------------------

    private const string UnnestAggSql = "SELECT s.trade_id, COUNT(*) AS legcount FROM structures s CROSS JOIN UNNEST(s.legs) AS l GROUP BY s.trade_id";

    [Fact]
    public void UnnestAggregate_StageGraph()
    {
        var plan = CompileTable(UnnestAggSql, Structures).Plan!;
        var dataflow = plan.CreateDataflow(4);

        Assert.Equal([TableStageKind.Ingest, TableStageKind.Unnest, TableStageKind.FilterProject, TableStageKind.Reduce],
            dataflow.Stages.Select(s => s.Kind));

        var unnestStage = dataflow.Stages.Single(s => s.Kind == TableStageKind.Unnest);
        Assert.Single(unnestStage.InEdges);
        // Unnest at chain position 0, fed by the 1-partition ingest -> content-hash fan-out (stateless, no key).
        Assert.Equal(TableEdgeMode.HashPartition, unnestStage.InEdges[0].Mode);
    }

    [Fact]
    public void UnnestAggregate_Equivalence()
    {
        object? Legs(params string[] ccys) => ccys.Select(c => (object?)new Dictionary<string, object?> { ["ccy"] = c }).ToList();
        var events = new List<Ev>
        {
            S("structures", Evt(1, "structures", ("trade_id", "T1"), ("legs", Legs("USD", "EUR")), ("payload", null))),
            S("structures", Evt(2, "structures", ("trade_id", "T2"), ("legs", Legs("GBP")), ("payload", null))),
            S("structures", Evt(3, "structures", ("trade_id", "T1"), ("legs", Legs("USD", "EUR", "JPY")), ("payload", null))),
        };
        AssertEquivalence(CompileTable(UnnestAggSql, Structures), events, [1, 4]);
    }

    // ------------------------------------------------------------------------------------------------
    // 4) LATEST BY: argmax-by-timestamp, no join, no aggregation.
    // ------------------------------------------------------------------------------------------------

    private const string LatestBySql = "SELECT order_id, stage, filled_qty FROM order_events LATEST BY (order_id)";

    [Fact]
    public void LatestBy_StageGraph()
    {
        var plan = CompileTable(LatestBySql, OrderEvents).Plan!;
        var dataflow = plan.CreateDataflow(4);

        Assert.Equal([TableStageKind.Ingest, TableStageKind.FilterProject, TableStageKind.LatestBy], dataflow.Stages.Select(s => s.Kind));
        var latestByStage = dataflow.Stages.Single(s => s.Kind == TableStageKind.LatestBy);
        Assert.Equal(TableEdgeMode.HashPartition, latestByStage.InEdges[0].Mode); // partitioned on the LATEST BY key
        Assert.Equal(latestByStage.StageId, dataflow.TerminalEdge.FromStageId);
    }

    [Fact]
    public void LatestBy_Equivalence()
    {
        var events = new List<Ev>
        {
            S("order_events", Evt(100, "order_events", ("order_id", "O1"), ("stage", "new"), ("filled_qty", 0L))),
            S("order_events", Evt(200, "order_events", ("order_id", "O2"), ("stage", "new"), ("filled_qty", 0L))),
            S("order_events", Evt(300, "order_events", ("order_id", "O1"), ("stage", "partial"), ("filled_qty", 5L))),
            S("order_events", Evt(250, "order_events", ("order_id", "O1"), ("stage", "stale"), ("filled_qty", 1L))), // late arrival, older ts
            S("order_events", Evt(400, "order_events", ("order_id", "O2"), ("stage", "filled"), ("filled_qty", 10L))),
        };
        AssertEquivalence(CompileTable(LatestBySql, OrderEvents), events, [1, 4]);
    }

    // ------------------------------------------------------------------------------------------------
    // 5) Semi-join: trades WHERE symbol IN (SELECT symbol FROM ref) — always a derived/broadcast join.
    // ------------------------------------------------------------------------------------------------

    private const string SemiSql = "SELECT symbol, price FROM trades WHERE symbol IN (SELECT symbol FROM ref)";

    [Fact]
    public void SemiJoin_StageGraph()
    {
        var plan = CompileTable(SemiSql, [Trades], [Ref]).Plan!;
        var dataflow = plan.CreateDataflow(4);

        Assert.Equal([TableStageKind.Ingest, TableStageKind.SemiAnti, TableStageKind.FilterProject], dataflow.Stages.Select(s => s.Kind));
        var semiStage = dataflow.Stages.Single(s => s.Kind == TableStageKind.SemiAnti);
        var rightEdge = semiStage.InEdges.Single(e => e.Role == "Right");
        Assert.Equal(TableEdgeMode.Broadcast, rightEdge.Mode);
        Assert.Equal(-1, rightEdge.FromStageId); // external: fed by ref directly, not another stage
        Assert.Contains("ref", rightEdge.ExternalInputNames);
        var leftEdge = semiStage.InEdges.Single(e => e.Role == "Left");
        Assert.Equal(TableEdgeMode.HashPartition, leftEdge.Mode); // chain position 0, fed by 1-partition ingest -> content-hash fan-out
    }

    [Fact]
    public void SemiJoin_Equivalence()
    {
        var events = new List<Ev>
        {
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1),
            S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            S("trades", Evt(2, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 7L), ("active", true))), // no ref row: filtered
            T("ref", Evt(0, "ref", ("symbol", "MSFT"), ("tag", "watch")), 1),
            S("trades", Evt(3, "trades", ("symbol", "MSFT"), ("price", 21.0), ("qty", 1L), ("active", true))), // now passes
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1), // presence flips off -> AAPL rows retract
        };
        AssertEquivalence(CompileTable(SemiSql, [Trades], [Ref]), events, [1, 4]);
    }

    [Fact]
    public void SemiJoin_Deterministic() =>
        AssertDeterministic(CompileTable(SemiSql, [Trades], [Ref]), [
            T("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1),
            S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            S("trades", Evt(2, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 7L), ("active", true))),
            T("ref", Evt(0, "ref", ("symbol", "MSFT"), ("tag", "watch")), 1),
        ]);

    // ------------------------------------------------------------------------------------------------
    // 6) Scalar broadcast: uncorrelated scalar subquery over the SAME stream ("trades" feeds two edges).
    // ------------------------------------------------------------------------------------------------

    private const string ScalarSql = "SELECT symbol, price - (SELECT AVG(price) FROM trades) AS rel FROM trades";

    [Fact]
    public void ScalarBroadcast_StageGraph()
    {
        var plan = CompileTable(ScalarSql, Trades).Plan!;
        var dataflow = plan.CreateDataflow(4);

        Assert.Equal([TableStageKind.Ingest, TableStageKind.Join, TableStageKind.FilterProject], dataflow.Stages.Select(s => s.Kind));
        var joinStage = dataflow.Stages.Single(s => s.Kind == TableStageKind.Join);
        var rightEdge = joinStage.InEdges.Single(e => e.Role == "Right");
        Assert.Equal(TableEdgeMode.Broadcast, rightEdge.Mode);
        Assert.Equal(-1, rightEdge.FromStageId);
        Assert.Contains("trades", rightEdge.ExternalInputNames);

        // "trades" feeds TWO edges: the main FROM ingest AND the scalar subquery's broadcast edge.
        var trainEdges = dataflow.EdgesForExternalInput("trades");
        Assert.Equal(2, trainEdges.Count);
        Assert.Contains(trainEdges, e => e.Mode == TableEdgeMode.Broadcast);
        Assert.Contains(trainEdges, e => e.Mode != TableEdgeMode.Broadcast);
    }

    [Fact]
    public void ScalarBroadcast_Equivalence()
    {
        var events = new List<Ev>
        {
            S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            S("trades", Evt(2, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 7L), ("active", true))),
            S("trades", Evt(3, "trades", ("symbol", "GOOG"), ("price", 30.0), ("qty", 2L), ("active", true))),
        };
        AssertEquivalence(CompileTable(ScalarSql, Trades), events, [1, 4]);
    }

    [Fact]
    public void ScalarBroadcast_Deterministic() =>
        AssertDeterministic(CompileTable(ScalarSql, Trades), [
            S("trades", Evt(1, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true))),
            S("trades", Evt(2, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 7L), ("active", true))),
            S("trades", Evt(3, "trades", ("symbol", "GOOG"), ("price", 30.0), ("qty", 2L), ("active", true))),
        ]);

    // ------------------------------------------------------------------------------------------------
    // Unsupported shape: derived table/CTE in FROM position -> M2 explicitly out of scope (Parallelism
    // pinned to 1 for such tables; see TableDataflowPlan's class doc).
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void DerivedTableInFromPosition_CreateDataflowThrows()
    {
        var result = CompileTable("SELECT x.symbol FROM (SELECT symbol FROM trades) x", Trades);
        Assert.True(result.Ok);
        Assert.Throws<NotSupportedException>(() => result.Plan!.CreateDataflow(4));
    }
}
