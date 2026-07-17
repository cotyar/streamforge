using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

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
}
