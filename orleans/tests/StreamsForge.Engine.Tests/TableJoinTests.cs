using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>Relational delta-join (table mode): both sides fully indexed, weight multiplication on match,
/// and retraction propagation through the join when an upstream (table-sourced) side is retracted.
/// Output columns are unqualified (SELECT t.symbol -> "symbol"), unlike the alias-prefixed WorkingRow
/// field keys used internally during the join itself.</summary>
public class TableJoinTests
{
    [Fact]
    public void LeftArrivalMatchesAlreadyIndexedRightSide()
    {
        // ref (a table input) arrives first with nothing on the trades side yet — no match, no output.
        // trades (the FROM/left side, a stream) then arrives and matches against ref's populated index.
        var exec = CompileTableAndCreate("SELECT t.symbol, r.tag FROM trades t JOIN ref r ON t.symbol = r.symbol", [Trades], [Ref]);

        var noMatch = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 2));
        Assert.Empty(noMatch);

        var matched = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));
        var delta = Assert.Single(matched);
        Assert.Equal(2, delta.Weight); // weight(trades)=1 * weight(ref)=2
        Assert.Equal("AAPL", delta.Row["symbol"]);
        Assert.Equal("watchlist", delta.Row["tag"]);
    }

    [Fact]
    public void RightArrivalMatchesAlreadyIndexedLeftSide()
    {
        var exec = CompileTableAndCreate("SELECT t.symbol, r.tag FROM trades t JOIN ref r ON t.symbol = r.symbol", [Trades], [Ref]);

        var noMatch = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "MSFT"), ("price", 50.0), ("qty", 5L), ("active", true)));
        Assert.Empty(noMatch);

        var matched = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "MSFT"), ("tag", "core")), 5));
        var delta = Assert.Single(matched);
        Assert.Equal(5, delta.Weight); // weight(ref)=5 * weight(trades)=1
        Assert.Equal("MSFT", delta.Row["symbol"]);
        Assert.Equal("core", delta.Row["tag"]);
    }

    [Fact]
    public void RetractingAMatchedRowPropagatesANegativeWeightDelta()
    {
        var exec = CompileTableAndCreate("SELECT t.symbol, r.tag FROM trades t JOIN ref r ON t.symbol = r.symbol", [Trades], [Ref]);

        exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));
        var matched = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 2));
        Assert.Single(matched);

        var retracted = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), -2));
        var delta = Assert.Single(retracted);
        Assert.Equal(-2, delta.Weight);
        Assert.Equal("AAPL", delta.Row["symbol"]);
        Assert.Equal("watchlist", delta.Row["tag"]);

        // Consolidated: the earlier +2 and this -2 net to zero — the row is gone from the snapshot.
        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void FanOutMultipleMatchesEachEmitTheirOwnWeightedDelta()
    {
        var exec = CompileTableAndCreate("SELECT t.symbol, r.tag FROM trades t JOIN ref r ON t.symbol = r.symbol", [Trades], [Ref]);

        exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));

        var first = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "a")), 1));
        var second = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "b")), 1));

        Assert.Single(first);
        Assert.Single(second);
        Assert.Equal(2, exec.Snapshot().Count); // two distinct combined rows (different tag), both weight 1
    }

    [Fact]
    public void NonMatchingKeyProducesNoOutput()
    {
        var exec = CompileTableAndCreate("SELECT t.symbol, r.tag FROM trades t JOIN ref r ON t.symbol = r.symbol", [Trades], [Ref]);

        exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));
        var noMatch = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "MSFT"), ("tag", "core")), 1));

        Assert.Empty(noMatch);
    }
}
