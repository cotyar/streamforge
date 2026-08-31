using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

public class ExecutorJoinTests
{
    private const string JoinSelect = "SELECT t.symbol AS symbol, t.price AS price, q.bid AS bid FROM trades t";

    [Fact]
    public void InnerJoinEmitsOnMatchImmediately()
    {
        var exec = CompileAndCreate($"{JoinSelect} INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", Trades, Quotes);

        var afterTrade = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        Assert.Empty(afterTrade);

        var afterQuote = exec.OnEvent("quotes", Evt(1200, "quotes", ("symbol", "AAPL"), ("bid", 99.5), ("ask", 100.5)));
        Assert.Single(afterQuote);
        Assert.Equal("AAPL", afterQuote[0]["symbol"]);
        Assert.Equal(100.0, afterQuote[0]["price"]);
        Assert.Equal(99.5, afterQuote[0]["bid"]);
        Assert.Equal(1200L, afterQuote[0].Timestamp); // combined ts = max(1000,1200)
    }

    [Fact]
    public void InnerJoinNoMatchNoNullPadOnEviction()
    {
        var exec = CompileAndCreate($"{JoinSelect} INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", Trades, Quotes);
        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));

        var evicted = exec.AdvanceWatermark(8000); // watermark = 7000 > 1000 + 5000
        Assert.Empty(evicted);
    }

    [Fact]
    public void LeftJoinNullPadsUnmatchedOnEviction()
    {
        var exec = CompileAndCreate($"{JoinSelect} LEFT JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", Trades, Quotes);
        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));

        var immediate = exec.AdvanceWatermark(6500); // watermark = 5500, not yet past 1000+5000=6000
        Assert.Empty(immediate);

        var evicted = exec.AdvanceWatermark(8000); // watermark = 7000 > 6000
        Assert.Single(evicted);
        Assert.Equal("AAPL", evicted[0]["symbol"]);
        Assert.Null(evicted[0]["bid"]);
    }

    [Fact]
    public void RightJoinNullPadsUnmatchedLeftColumnsOnEviction()
    {
        var exec = CompileAndCreate($"{JoinSelect} RIGHT JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", Trades, Quotes);
        exec.OnEvent("quotes", Evt(1000, "quotes", ("symbol", "AAPL"), ("bid", 99.0), ("ask", 101.0)));

        var evicted = exec.AdvanceWatermark(8000);
        Assert.Single(evicted);
        Assert.Null(evicted[0]["symbol"]);
        Assert.Null(evicted[0]["price"]);
        Assert.Equal(99.0, evicted[0]["bid"]);
    }

    [Fact]
    public void FullJoinNullPadsBothSidesIndependently()
    {
        var exec = CompileAndCreate($"{JoinSelect} FULL JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", Trades, Quotes);
        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        exec.OnEvent("quotes", Evt(1100, "quotes", ("symbol", "MSFT"), ("bid", 50.0), ("ask", 51.0)));

        var evicted = exec.AdvanceWatermark(8000);
        Assert.Equal(2, evicted.Count);
        Assert.Contains(evicted, r => Equals(r["symbol"], "AAPL") && r["bid"] is null);
        Assert.Contains(evicted, r => r["symbol"] is null && Equals(r["bid"], 50.0));
    }

    [Fact]
    public void CrossJoinMatchesAllBufferedRowsRegardlessOfKey()
    {
        var exec = CompileAndCreate($"{JoinSelect} CROSS JOIN quotes q WITHIN 5 SECONDS", Trades, Quotes);
        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        exec.OnEvent("trades", Evt(1100, "trades", ("symbol", "MSFT"), ("price", 200.0), ("qty", 1L), ("active", true)));

        var results = exec.OnEvent("quotes", Evt(1200, "quotes", ("symbol", "GOOG"), ("bid", 1.0), ("ask", 1.1)));
        Assert.Equal(2, results.Count); // matches both buffered trades, ignoring symbol key
    }

    [Fact]
    public void MultiWayJoinFoldsLeftToRightAndCombinedTimestampIsMax()
    {
        var sql = "SELECT t.symbol AS symbol, q.bid AS bid, r.tag AS tag FROM trades t " +
                  "INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol " +
                  "INNER JOIN ref r WITHIN 5 SECONDS ON t.symbol = r.symbol";
        var exec = CompileAndCreate(sql, Trades, Quotes, Ref);

        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        var afterQuote = exec.OnEvent("quotes", Evt(1100, "quotes", ("symbol", "AAPL"), ("bid", 99.0), ("ask", 101.0)));
        Assert.Empty(afterQuote); // waiting on ref

        var afterRef = exec.OnEvent("ref", Evt(1200, "ref", ("symbol", "AAPL"), ("tag", "blue-chip")));
        Assert.Single(afterRef);
        Assert.Equal("AAPL", afterRef[0]["symbol"]);
        Assert.Equal(99.0, afterRef[0]["bid"]);
        Assert.Equal("blue-chip", afterRef[0]["tag"]);
        Assert.Equal(1200L, afterRef[0].Timestamp);
    }

    [Fact]
    public void SelfJoinDeliversEventToBothAliases()
    {
        var sql = "SELECT a.symbol AS asym, b.symbol AS bsym FROM trades a INNER JOIN trades b WITHIN 5 SECONDS ON a.symbol = b.symbol";
        var exec = CompileAndCreate(sql, Trades);

        // The very first trade event is delivered to both alias 'a' (FROM role) and alias 'b' (JOIN role),
        // so it can match against itself.
        var results = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));
        Assert.Single(results);
        Assert.Equal("AAPL", results[0]["asym"]);
        Assert.Equal("AAPL", results[0]["bsym"]);
    }

    [Fact]
    public void JoinOnJsonArrowArrowExtractedEquiKeyMatches()
    {
        // ON e.payload ->> 'order' ->> 'symbol'... — wait, the equi-key itself is 'payload -> 'order' ->> 'symbol''
        // (an object step then a text-extracting step). The equi-key extractor must walk through the
        // JsonAccessExpr nodes on the JSON side to recognize this as a valid hash equi-key against t.symbol.
        var sql = "SELECT e.eventType AS eventType, e.payload -> 'user' ->> 'tier' AS tier, t.price AS price FROM events e " +
                  "JOIN trades t WITHIN 5 SECONDS ON e.payload -> 'order' ->> 'symbol' = t.symbol";
        var exec = CompileAndCreate(sql, Events, Trades);

        var payload = new Dictionary<string, object?>
        {
            ["user"] = new Dictionary<string, object?> { ["tier"] = "gold" },
            ["order"] = new Dictionary<string, object?> { ["symbol"] = "AAPL" },
        };

        var afterEvent = exec.OnEvent("events", Evt(1000, "events", ("eventType", "order.placed"), ("payload", payload)));
        Assert.Empty(afterEvent);

        var afterTrade = exec.OnEvent("trades", Evt(1100, "trades", ("symbol", "AAPL"), ("price", 150.0), ("qty", 10L), ("active", true)));
        Assert.Single(afterTrade);
        Assert.Equal("order.placed", afterTrade[0]["eventType"]);
        Assert.Equal("gold", afterTrade[0]["tier"]);
        Assert.Equal(150.0, afterTrade[0]["price"]);

        // A trade for a different symbol must not match.
        var noMatch = exec.OnEvent("trades", Evt(1200, "trades", ("symbol", "MSFT"), ("price", 250.0), ("qty", 5L), ("active", true)));
        Assert.Empty(noMatch);
    }
}
