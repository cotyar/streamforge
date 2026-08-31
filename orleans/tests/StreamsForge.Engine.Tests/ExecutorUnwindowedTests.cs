using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

public class ExecutorUnwindowedTests
{
    [Fact]
    public void FilterAndProjectEmitsImmediatelyPerEvent()
    {
        var exec = CompileAndCreate("SELECT symbol AS sym, price * qty AS notional FROM trades WHERE price > 10", Trades);

        var passing = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 3L), ("active", true)));
        Assert.Single(passing);
        Assert.Equal("AAPL", passing[0]["sym"]);
        Assert.Equal(300.0, passing[0]["notional"]);

        var filtered = exec.OnEvent("trades", Evt(2000, "trades", ("symbol", "MSFT"), ("price", 5.0), ("qty", 3L), ("active", true)));
        Assert.Empty(filtered);
    }

    [Fact]
    public void OutputTimestampIsRowTimestamp()
    {
        var exec = CompileAndCreate("SELECT symbol FROM trades", Trades);
        var results = exec.OnEvent("trades", Evt(4242, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));
        Assert.Single(results);
        Assert.Equal(4242L, results[0].Timestamp);
    }

    [Fact]
    public void UnknownEventSourceProducesNoOutput()
    {
        var exec = CompileAndCreate("SELECT symbol FROM trades", Trades, Quotes);
        var results = exec.OnEvent("quotes", Evt(1000, "quotes", ("symbol", "AAPL"), ("bid", 1.0), ("ask", 1.1)));
        Assert.Empty(results);
    }

    [Fact]
    public void DefaultColumnNamesUseIdentifierOrFunctionName()
    {
        var exec = CompileAndCreate("SELECT symbol, ABS(price) FROM trades", Trades);
        var results = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", -5.0), ("qty", 1L), ("active", true)));
        Assert.Single(results);
        Assert.Equal("AAPL", results[0]["symbol"]);
        Assert.Equal(5.0, results[0]["abs"]);
    }

    [Fact]
    public void WhereFiltersOnJsonArrowArrowExtractedValue()
    {
        var exec = CompileAndCreate(
            "SELECT eventType, payload -> 'order' ->> 'symbol' AS symbol FROM events WHERE payload -> 'user' ->> 'tier' = 'gold'",
            Events);

        var gold = new Dictionary<string, object?>
        {
            ["user"] = new Dictionary<string, object?> { ["tier"] = "gold" },
            ["order"] = new Dictionary<string, object?> { ["symbol"] = "AAPL" },
        };
        var silver = new Dictionary<string, object?>
        {
            ["user"] = new Dictionary<string, object?> { ["tier"] = "silver" },
            ["order"] = new Dictionary<string, object?> { ["symbol"] = "MSFT" },
        };

        var passing = exec.OnEvent("events", Evt(1000, "events", ("eventType", "order.placed"), ("payload", gold)));
        Assert.Single(passing);
        Assert.Equal("AAPL", passing[0]["symbol"]);

        var filtered = exec.OnEvent("events", Evt(2000, "events", ("eventType", "order.placed"), ("payload", silver)));
        Assert.Empty(filtered);
    }
}
