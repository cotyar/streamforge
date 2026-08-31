using StreamsForge.Engine.Runtime;
using StreamsForge.Engine.Runtime.Ops;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Per-op unit tests for the pipeline-mode (streaming) operator chain (plan 003 M1 Part B / acceptance:
/// "new per-op unit tests"). Each op is instantiated directly — not through PipelineExecutor's façade —
/// with explicit state-in/rows-in/rows-out assertions, mirroring TableOpsUnitTests' structure for table
/// mode.
/// </summary>
public class PipelineOpsUnitTests
{
    // ------------------------------------------------------------------
    // PipelineJoinOp — buffered interval join; state = per-side buffered entries.
    // ------------------------------------------------------------------

    private static PipelineJoinOp CreateInnerJoinOp()
    {
        var compiled = Compile("SELECT t.symbol, q.bid FROM trades t JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", Trades, Quotes).Plan!.Compiled;
        var j = compiled.Joins[0];
        return new PipelineJoinOp(j.Kind, j.Within, j.LeftKey, j.RightKey, j.Residual, compiled.Bindings,
            [(compiled.Sources[0].Alias, compiled.Sources[0].Schema)], (j.Alias, j.Schema));
    }

    [Fact]
    public void JoinOp_MatchWithinWindowEmitsCombinedRow_AndBuffersBothSides()
    {
        var join = CreateInnerJoinOp();

        var leftRow = WorkingRow.FromEvent("t", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        Assert.Empty(join.OnLeft(leftRow));

        var rightRow = WorkingRow.FromEvent("q", Evt(1200, "quotes", ("symbol", "AAPL"), ("bid", 99.5), ("ask", 100.5)));
        var matched = join.OnRight(rightRow);

        var combined = Assert.Single(matched);
        Assert.Equal("AAPL", combined.Fields["t_symbol"]);
        Assert.Equal(99.5, combined.Fields["q_bid"]);

        Assert.Single(join.Left);
        Assert.Single(join.Right);
        Assert.True(join.Left[0].Matched);
        Assert.True(join.Right[0].Matched);
    }

    [Fact]
    public void JoinOp_EvictBeyondWithinRemovesUnmatchedEntriesFromState()
    {
        var join = CreateInnerJoinOp();
        join.OnLeft(WorkingRow.FromEvent("t", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true))));

        var evicted = join.Evict(watermark: 7000); // 1000 + 5000(within) = 6000 < 7000
        Assert.Empty(evicted); // INNER join: no null-padded row on an unmatched eviction
        Assert.Empty(join.Left); // but the buffered entry IS gone (state pruned)
    }

    // ------------------------------------------------------------------
    // PipelineFilterProjectOp — stateless WHERE (+terminal projection).
    // ------------------------------------------------------------------

    [Fact]
    public void FilterProjectOp_OnBatchTerminal_ProjectsPassingRowsAndDropsFailing()
    {
        var compiled = Compile("SELECT symbol, price FROM trades WHERE price > 50", Trades).Plan!.Compiled;
        var alias = compiled.Sources[0].Alias;
        var op = new PipelineFilterProjectOp(compiled);

        var passing = WorkingRow.FromEvent(alias, Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        var outp = op.OnBatchTerminal([passing]);
        var row = Assert.Single(outp);
        Assert.Equal("AAPL", row["symbol"]);
        Assert.Equal(100.0, row["price"]);

        var failing = WorkingRow.FromEvent(alias, Evt(1000, "trades", ("symbol", "MSFT"), ("price", 1.0), ("qty", 1L), ("active", true)));
        Assert.Empty(op.OnBatchTerminal([failing]));
    }

    [Fact]
    public void FilterProjectOp_OnBatch_FiltersButLeavesWorkingRowForDownstreamWindow()
    {
        var compiled = Compile(
            "SELECT symbol, COUNT(*) AS cnt FROM trades WHERE price > 50 GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)",
            Trades).Plan!.Compiled;
        var alias = compiled.Sources[0].Alias;
        var op = new PipelineFilterProjectOp(compiled);

        var passing = WorkingRow.FromEvent(alias, Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        var filtered = op.OnBatch([passing]);
        var row = Assert.Single(filtered);
        Assert.Equal("AAPL", row.Fields[$"{alias}_symbol"]);
    }

    // ------------------------------------------------------------------
    // PipelineWindowOp — tumbling/hopping/session aggregate state; covered end-to-end by
    // ExecutorWindowAndAggregateTests via the façade. This adds a direct-instantiation check that the
    // op's own State dictionaries reflect what's documented (state in/rows in/rows out).
    // ------------------------------------------------------------------

    [Fact]
    public void WindowOp_TumblingWindow_TracksOneStateEntryPerWindowGroup()
    {
        var compiled = Compile(
            "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL",
            Trades).Plan!.Compiled;
        var window = new PipelineWindowOp(compiled);
        var alias = compiled.Sources[0].Alias;

        window.OnRow(WorkingRow.FromEvent(alias, Evt(1000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true))));
        window.OnRow(WorkingRow.FromEvent(alias, Evt(1500, "trades", ("symbol", "MSFT"), ("price", 1.0), ("qty", 1L), ("active", true))));

        Assert.Equal(2, window.States.Count); // one window-state entry per distinct group in [0,10000)

        var closed = window.Evict(watermark: 12000);
        Assert.Equal(2, closed.Count);
        Assert.Empty(window.States); // closed windows are removed from state
    }
}
