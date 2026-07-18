using StreamForge.Engine.Dataflow;
using StreamForge.Engine.Runtime;
using StreamForge.Engine.Runtime.Ops;
using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Per-op unit tests for the table-mode operator chain (plan 003 M1 acceptance: "new per-op unit tests").
/// Each op is instantiated directly — NOT through TableExecutor's façade — and driven with explicit
/// state-in/deltas-in/deltas-out assertions, per the M1 task's requirement to cover "join retraction
/// cascades and reduce retract+assert pairs" at the op level specifically (TableJoinTests/TableZSetTests
/// already cover the same scenarios end-to-end through the public API; these tests instead assert on each
/// op's own exposed state — ZSetIndex contents, TableReduceOp.Groups — which the façade-level tests
/// can't reach).
/// </summary>
public class TableOpsUnitTests
{
    private static readonly Epoch E0 = new(0);
    private static readonly Epoch E1 = new(1);

    // ------------------------------------------------------------------
    // TableIngestOp — stateless: alias-tag + weight passthrough.
    // ------------------------------------------------------------------

    [Fact]
    public void IngestOp_TagsRowWithAliasAndPassesWeightThrough()
    {
        var op = new TableIngestOp("t");
        var evt = Evt(1000, "trades", ("symbol", "AAPL"));

        var outp = op.OnBatch(E0, [new TableDelta(evt, 3)]);

        var d = Assert.Single(outp);
        Assert.Equal(3, d.Weight);
        Assert.Equal("AAPL", d.Row.Fields["t_symbol"]);
        Assert.Equal(1000L, d.Row.Ts);
    }

    [Fact]
    public void IngestOp_BatchOfMultipleDeltasPreservesOrderAndEachWeight()
    {
        var op = new TableIngestOp("r");
        var batch = new List<TableDelta>
        {
            new(Evt(0, "ref", ("symbol", "AAPL")), 2),
            new(Evt(0, "ref", ("symbol", "MSFT")), -1),
        };

        var outp = op.OnBatch(E0, batch);

        Assert.Equal(2, outp.Count);
        Assert.Equal("AAPL", outp[0].Row.Fields["r_symbol"]);
        Assert.Equal(2, outp[0].Weight);
        Assert.Equal("MSFT", outp[1].Row.Fields["r_symbol"]);
        Assert.Equal(-1, outp[1].Weight);
    }

    [Fact]
    public void IngestOp_OnFrontierIsADocumentedPassThrough()
    {
        var op = new TableIngestOp("t");
        Assert.Empty(op.OnFrontier(E0));
        Assert.Empty(op.OnFrontier(new Epoch(999)));
    }

    // ------------------------------------------------------------------
    // TableJoinOp — bilinear equi-join; state = both sides' ZSetIndex.
    // ------------------------------------------------------------------

    private static TableJoinOp CreateJoinOp()
    {
        var compiled = CompileTable("SELECT t.symbol, r.tag FROM trades t JOIN ref r ON t.symbol = r.symbol", [Trades], [Ref]).Plan!.Compiled;
        var j = compiled.Joins[0];
        return new TableJoinOp(j.LeftKey!, j.RightKey!, j.Residual, compiled.Bindings);
    }

    [Fact]
    public void JoinOp_RightArrivalMatchesAlreadyIndexedLeftSide_StateHoldsBothRows()
    {
        var join = CreateJoinOp();
        var left = new TableIngestOp("t");
        var right = new TableIngestOp("r");

        var leftDelta = left.OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", "AAPL")), 1)]);
        Assert.Empty(join.OnLeftBatch(E0, leftDelta)); // nothing on the right side to match yet

        var rightDelta = right.OnBatch(E0, [new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 2)]);
        var matched = join.OnRightBatch(E0, rightDelta);

        var d = Assert.Single(matched);
        Assert.Equal(2, d.Weight); // weight(left)=1 * weight(right)=2
        Assert.Equal("AAPL", d.Row.Fields["t_symbol"]);
        Assert.Equal("watchlist", d.Row.Fields["r_tag"]);

        // State: both sides now hold exactly the one row that was indexed on each.
        var key = TableKeyEncoding.EncodeScalar("AAPL");
        Assert.Single(join.Left.Lookup(key));
        Assert.Single(join.Right.Lookup(key));
    }

    [Fact]
    public void JoinOp_RetractionCascadesANegativeWeightDelta_AndPrunesTheIndexEntry()
    {
        var join = CreateJoinOp();
        var left = new TableIngestOp("t");
        var right = new TableIngestOp("r");

        join.OnLeftBatch(E0, left.OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", "AAPL")), 1)]));

        var rightAssert = right.OnBatch(E0, [new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 2)]);
        Assert.Single(join.OnRightBatch(E0, rightAssert));

        var rightRetract = right.OnBatch(E1, [new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), -2)]);
        var cascaded = join.OnRightBatch(E1, rightRetract);

        var d = Assert.Single(cascaded);
        Assert.Equal(-2, d.Weight);
        Assert.Equal("AAPL", d.Row.Fields["t_symbol"]);
        Assert.Equal("watchlist", d.Row.Fields["r_tag"]);

        // State: the retraction netted the right index's entry to zero weight — ZSetIndex prunes it.
        var key = TableKeyEncoding.EncodeScalar("AAPL");
        Assert.Empty(join.Right.Lookup(key));
        // Left side is untouched by a right-side retraction.
        Assert.Single(join.Left.Lookup(key));
    }

    [Fact]
    public void JoinOp_NullKeyContributesNothingAndIsNotIndexed()
    {
        var join = CreateJoinOp();
        var left = new TableIngestOp("t");

        var leftDelta = left.OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", null)), 1)]);
        Assert.Empty(join.OnLeftBatch(E0, leftDelta));
        Assert.Empty(join.Left.Lookup(TableKeyEncoding.EncodeScalar(null)));
    }

    [Fact]
    public void JoinOp_OnFrontierIsADocumentedPassThrough()
    {
        Assert.Empty(CreateJoinOp().OnFrontier(E0));
    }

    // ------------------------------------------------------------------
    // TableFilterProjectOp — stateless WHERE (+terminal projection).
    // ------------------------------------------------------------------

    [Fact]
    public void FilterProjectOp_OnBatchTerminal_ProjectsPassingRowsAndDropsFailing()
    {
        var compiled = CompileTable("SELECT symbol, price FROM trades WHERE price > 50", [Trades]).Plan!.Compiled;
        var alias = compiled.Sources[0].Alias;
        var op = new TableFilterProjectOp(compiled);
        var ingest = new TableIngestOp(alias);

        var passing = ingest.OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0)), 1)]);
        var outPassing = op.OnBatchTerminal(E0, passing);
        var d = Assert.Single(outPassing);
        Assert.Equal(1, d.Weight);
        Assert.Equal("AAPL", d.Row["symbol"]);
        Assert.Equal(100.0, d.Row["price"]);

        var failing = ingest.OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", "MSFT"), ("price", 10.0)), 1)]);
        Assert.Empty(op.OnBatchTerminal(E0, failing));
    }

    [Fact]
    public void FilterProjectOp_OnBatch_FiltersButLeavesWorkingRowShapeForDownstreamReduce()
    {
        var compiled = CompileTable("SELECT symbol, COUNT(*) AS cnt FROM trades WHERE price > 50 GROUP BY symbol", [Trades]).Plan!.Compiled;
        var alias = compiled.Sources[0].Alias;
        var op = new TableFilterProjectOp(compiled);
        var ingest = new TableIngestOp(alias);

        var passing = ingest.OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0)), 1)]);
        var filtered = op.OnBatch(E0, passing);

        var rd = Assert.Single(filtered);
        Assert.Equal(1, rd.Weight);
        Assert.Equal("AAPL", rd.Row.Fields[$"{alias}_symbol"]);

        var failing = ingest.OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", "MSFT"), ("price", 1.0)), 1)]);
        Assert.Empty(op.OnBatch(E0, failing));
    }

    [Fact]
    public void FilterProjectOp_OnFrontierIsADocumentedPassThrough()
    {
        var compiled = CompileTable("SELECT symbol FROM trades", [Trades]).Plan!.Compiled;
        Assert.Empty(new TableFilterProjectOp(compiled).OnFrontier(E0));
    }

    // ------------------------------------------------------------------
    // TableReduceOp — running GROUP BY aggregates; state = per-group aggregator dictionary.
    // ------------------------------------------------------------------

    [Fact]
    public void ReduceOp_FirstContributionEmitsOnlyAnAssertion_AndCreatesGroupState()
    {
        var compiled = CompileTable("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", [Trades]).Plan!.Compiled;
        var op = new TableReduceOp(compiled);
        var ingest = new TableIngestOp(compiled.Sources[0].Alias);

        var admitted = ingest.OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", "AAPL")), 1)]);
        var outp = op.OnBatch(E0, admitted);

        var d = Assert.Single(outp);
        Assert.Equal(1, d.Weight);
        Assert.Equal(1L, d.Row["cnt"]);

        Assert.Single(op.Groups);
        var key = TableKeyEncoding.EncodeGroupKey(["AAPL"]);
        Assert.Equal(1, op.Groups[key].TotalWeight);
    }

    [Fact]
    public void ReduceOp_SecondContributionEmitsRetractThenAssertPair_AndUpdatesGroupState()
    {
        var compiled = CompileTable("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", [Trades]).Plan!.Compiled;
        var op = new TableReduceOp(compiled);
        var ingest = new TableIngestOp(compiled.Sources[0].Alias);

        op.OnBatch(E0, ingest.OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", "AAPL")), 1)]));
        var second = op.OnBatch(E1, ingest.OnBatch(E1, [new TableDelta(Evt(1000, "trades", ("symbol", "AAPL")), 1)]));

        Assert.Equal(2, second.Count);
        Assert.Equal(-1, second[0].Weight);
        Assert.Equal(1L, second[0].Row["cnt"]);
        Assert.Equal(1, second[1].Weight);
        Assert.Equal(2L, second[1].Row["cnt"]);

        var key = TableKeyEncoding.EncodeGroupKey(["AAPL"]);
        Assert.Equal(2, op.Groups[key].TotalWeight);
    }

    [Fact]
    public void ReduceOp_GroupVanishesWhenWeightReachesZero_AndIsRemovedFromState()
    {
        var compiled = CompileTable("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", [Trades]).Plan!.Compiled;
        var op = new TableReduceOp(compiled);
        var ingest = new TableIngestOp(compiled.Sources[0].Alias);

        op.OnBatch(E0, ingest.OnBatch(E0, [new TableDelta(Evt(1000, "trades", ("symbol", "AAPL")), 1)]));
        var retracted = op.OnBatch(E1, ingest.OnBatch(E1, [new TableDelta(Evt(1000, "trades", ("symbol", "AAPL")), -1)]));

        var d = Assert.Single(retracted);
        Assert.Equal(-1, d.Weight);
        Assert.Empty(op.Groups); // group dropped from state entirely
    }

    [Fact]
    public void ReduceOp_OnFrontierIsADocumentedPassThrough()
    {
        var compiled = CompileTable("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", [Trades]).Plan!.Compiled;
        Assert.Empty(new TableReduceOp(compiled).OnFrontier(E0));
    }
}
