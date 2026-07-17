using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

public class ExecutorWindowAndAggregateTests
{
    [Fact]
    public void TumblingWindowClosesOnlyWhenWatermarkPassesEnd()
    {
        var sql = "SELECT symbol, COUNT(*) AS cnt, SUM(qty) AS total FROM trades " +
                  "GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = CompileAndCreate(sql, Trades);

        var r1 = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 10L), ("active", true)));
        Assert.Empty(r1); // EMIT FINAL: no update rows

        var r2 = exec.OnEvent("trades", Evt(2000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 20L), ("active", true)));
        Assert.Empty(r2);

        var notYet = exec.AdvanceWatermark(10500); // watermark = 9500, window end = 10000, not yet passed
        Assert.Empty(notYet);

        var closed = exec.AdvanceWatermark(12000); // watermark = 11000 >= 10000
        var row = Assert.Single(closed);
        Assert.Equal("AAPL", row["symbol"]);
        Assert.Equal(2L, row["cnt"]);
        Assert.Equal(30L, row["total"]);
        Assert.Equal(0L, row["window_start"]);
        Assert.Equal(10000L, row["window_end"]);
        Assert.Equal(10000L, row.Timestamp);
    }

    [Fact]
    public void HoppingWindowAssignsRowToEveryOverlappingWindow()
    {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades " +
                  "GROUP BY symbol WINDOW HOPPING(SIZE 10 SECONDS, ADVANCE BY 5 SECONDS) EMIT FINAL";
        var exec = CompileAndCreate(sql, Trades);

        exec.OnEvent("trades", Evt(7000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));

        // ts=7000 belongs to windows [0,10000) and [5000,15000)
        var closedFirst = exec.AdvanceWatermark(11500); // watermark = 10500 >= 10000, closes [0,10000)
        var firstRow = Assert.Single(closedFirst);
        Assert.Equal(0L, firstRow["window_start"]);
        Assert.Equal(10000L, firstRow["window_end"]);
        Assert.Equal(1L, firstRow["cnt"]);

        var closedSecond = exec.AdvanceWatermark(16500); // watermark = 15500 >= 15000
        var secondRow = Assert.Single(closedSecond);
        Assert.Equal(5000L, secondRow["window_start"]);
        Assert.Equal(15000L, secondRow["window_end"]);
        Assert.Equal(1L, secondRow["cnt"]);
    }

    [Fact]
    public void SessionWindowExtendsWithinGapAndSplitsAcrossGap()
    {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW SESSION(GAP 5 SECONDS) EMIT FINAL";
        var exec = CompileAndCreate(sql, Trades);

        exec.OnEvent("trades", Evt(0, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));
        exec.OnEvent("trades", Evt(3000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true))); // within gap of 0+5000, extends
        // ts=20000 is beyond 3000+5000=8000, so it starts a new session; it also jumps the watermark to
        // 20000-1000=19000 via OnEvent's own max(seen event ts)-lateness advance.
        exec.OnEvent("trades", Evt(20000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));

        var closedFirst = exec.AdvanceWatermark(10000); // watermark stays max(19000, 9000) = 19000 > 3000 + 5000
        var firstRow = Assert.Single(closedFirst);
        Assert.Equal(0L, firstRow["window_start"]);
        Assert.Equal(3000L, firstRow["window_end"]);
        Assert.Equal(2L, firstRow["cnt"]);

        var closedSecond = exec.AdvanceWatermark(27000); // watermark = max(19000, 26000) = 26000 > 20000 + 5000 = 25000
        var secondRow = Assert.Single(closedSecond);
        Assert.Equal(20000L, secondRow["window_start"]);
        Assert.Equal(1L, secondRow["cnt"]);
    }

    [Fact]
    public void EmitChangesProducesUpdateRowsThenFinalRow()
    {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT CHANGES";
        var exec = CompileAndCreate(sql, Trades);

        var r1 = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));
        var u1 = Assert.Single(r1);
        Assert.Equal(false, u1["_final"]);
        Assert.Equal(1L, u1["cnt"]);

        var r2 = exec.OnEvent("trades", Evt(2000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));
        var u2 = Assert.Single(r2);
        Assert.Equal(false, u2["_final"]);
        Assert.Equal(2L, u2["cnt"]);

        var closed = exec.AdvanceWatermark(12000);
        var final = Assert.Single(closed);
        Assert.Equal(true, final["_final"]);
        Assert.Equal(2L, final["cnt"]);
    }

    [Fact]
    public void DistinctGroupsProduceSeparateWindowRows()
    {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = CompileAndCreate(sql, Trades);

        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));
        exec.OnEvent("trades", Evt(1500, "trades", ("symbol", "MSFT"), ("price", 1.0), ("qty", 1L), ("active", true)));
        exec.OnEvent("trades", Evt(1600, "trades", ("symbol", "MSFT"), ("price", 1.0), ("qty", 1L), ("active", true)));

        var closed = exec.AdvanceWatermark(12000);
        Assert.Equal(2, closed.Count);
        Assert.Contains(closed, r => Equals(r["symbol"], "AAPL") && Equals(r["cnt"], 1L));
        Assert.Contains(closed, r => Equals(r["symbol"], "MSFT") && Equals(r["cnt"], 2L));
    }

    [Fact]
    public void CountStarCountsRowsCountColumnSkipsNulls()
    {
        var sql = "SELECT symbol, COUNT(*) AS all_rows, COUNT(qty) AS non_null_qty FROM trades " +
                  "GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = CompileAndCreate(sql, Trades);

        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 10L), ("active", true)));
        exec.OnEvent("trades", Evt(1100, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", null), ("active", true)));
        exec.OnEvent("trades", Evt(1200, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 5L), ("active", true)));

        var row = Assert.Single(exec.AdvanceWatermark(12000));
        Assert.Equal(3L, row["all_rows"]);
        Assert.Equal(2L, row["non_null_qty"]);
    }

    [Fact]
    public void AvgMinMaxAggregates()
    {
        var sql = "SELECT symbol, AVG(price) AS avgp, MIN(price) AS minp, MAX(price) AS maxp, MIN(symbol) AS mins " +
                  "FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = CompileAndCreate(sql, Trades);

        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 1L), ("active", true)));
        exec.OnEvent("trades", Evt(1100, "trades", ("symbol", "AAPL"), ("price", 20.0), ("qty", 1L), ("active", true)));
        exec.OnEvent("trades", Evt(1200, "trades", ("symbol", "AAPL"), ("price", 30.0), ("qty", 1L), ("active", true)));

        var row = Assert.Single(exec.AdvanceWatermark(12000));
        Assert.Equal(20.0, row["avgp"]);
        Assert.Equal(10.0, row["minp"]);
        Assert.Equal(30.0, row["maxp"]);
        Assert.Equal("AAPL", row["mins"]);
    }

    [Fact]
    public void LateEventIsDroppedAndCounted()
    {
        var exec = CompileAndCreate("SELECT symbol FROM trades", Trades);

        exec.OnEvent("trades", Evt(10000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));
        Assert.Equal(9000L, exec.Watermark); // 10000 - 1000 allowed lateness

        var results = exec.OnEvent("trades", Evt(5000, "trades", ("symbol", "MSFT"), ("price", 1.0), ("qty", 1L), ("active", true)));
        Assert.Empty(results);
        Assert.Equal(1L, exec.LateEvents);
    }

    [Fact]
    public void LateEventDropViaExplicitAdvanceWatermark()
    {
        var exec = CompileAndCreate("SELECT symbol FROM trades", Trades);

        exec.AdvanceWatermark(11000); // watermark = 10000
        var results = exec.OnEvent("trades", Evt(9000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));
        Assert.Empty(results);
        Assert.Equal(1L, exec.LateEvents);
    }

    [Fact]
    public void GroupByJsonArrowArrowExtractedValueAggregatesPerDistinctKey()
    {
        var sql = "SELECT payload -> 'user' ->> 'tier' AS tier, COUNT(*) AS cnt " +
                  "FROM events GROUP BY payload -> 'user' ->> 'tier' WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = CompileAndCreate(sql, Events);

        object? PayloadWithTier(string tier) => new Dictionary<string, object?>
        {
            ["user"] = new Dictionary<string, object?> { ["tier"] = tier },
        };

        exec.OnEvent("events", Evt(1000, "events", ("eventType", "e"), ("payload", PayloadWithTier("gold"))));
        exec.OnEvent("events", Evt(1100, "events", ("eventType", "e"), ("payload", PayloadWithTier("silver"))));
        exec.OnEvent("events", Evt(1200, "events", ("eventType", "e"), ("payload", PayloadWithTier("gold"))));

        var closed = exec.AdvanceWatermark(12000);
        Assert.Equal(2, closed.Count);
        Assert.Contains(closed, r => Equals(r["tier"], "gold") && Equals(r["cnt"], 2L));
        Assert.Contains(closed, r => Equals(r["tier"], "silver") && Equals(r["cnt"], 1L));
    }
}
