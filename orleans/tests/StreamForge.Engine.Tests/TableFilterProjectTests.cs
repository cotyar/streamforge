using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>Filter/project (no GROUP BY, no JOIN): weight passes through unchanged; WHERE simply drops
/// rows that don't match (no retraction machinery needed — nothing was asserted for a filtered-out row).</summary>
public class TableFilterProjectTests
{
    [Fact]
    public void RowsPassingWhereAreEmittedWithWeightUnchanged()
    {
        var exec = CompileTableAndCreate("SELECT symbol, price FROM trades WHERE price > 50", Trades);

        var deltas = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));

        var delta = Assert.Single(deltas);
        Assert.Equal(1, delta.Weight);
        Assert.Equal("AAPL", delta.Row["symbol"]);
        Assert.Equal(100.0, delta.Row["price"]);
    }

    [Fact]
    public void RowsFailingWhereAreDropped()
    {
        var exec = CompileTableAndCreate("SELECT symbol, price FROM trades WHERE price > 50", Trades);

        var deltas = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 10L), ("active", true)));

        Assert.Empty(deltas);
    }

    [Fact]
    public void ProjectionPassesUpstreamTableWeightThroughUnchanged()
    {
        var exec = CompileTableAndCreate("SELECT symbol, tag FROM ref", [], [Ref]);

        var deltas = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 3));

        var delta = Assert.Single(deltas);
        Assert.Equal(3, delta.Weight); // filter/project: weight passthrough, no multiplication
        Assert.Equal("AAPL", delta.Row["symbol"]);
        Assert.Equal("core", delta.Row["tag"]);
    }

    [Fact]
    public void RetractionThroughProjectionAlsoPassesThroughUnchanged()
    {
        var exec = CompileTableAndCreate("SELECT symbol, tag FROM ref", [], [Ref]);

        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 3));
        var retracted = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -3));

        var delta = Assert.Single(retracted);
        Assert.Equal(-3, delta.Weight);
        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void QualifiedStarExpandsAllColumnsOfThatAliasUnprefixedWithSingleSource()
    {
        // Single-source table (no joins): mirrors bare SELECT *'s "no prefix" rule.
        var exec = CompileTableAndCreate("SELECT t.* FROM trades t", Trades);

        var deltas = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));

        var delta = Assert.Single(deltas);
        Assert.Equal("AAPL", delta.Row["symbol"]);
        Assert.Equal(100.0, delta.Row["price"]);
        Assert.Equal(10L, delta.Row["qty"]);
        Assert.Equal(true, delta.Row["active"]);
    }

    [Fact]
    public void QualifiedStarWithJoinPrefixesAndCombinesWithExtraColumn()
    {
        var sql = "SELECT t.*, r.tag FROM trades t JOIN ref r ON t.symbol = r.symbol";
        var exec = CompileTableAndCreate(sql, [Trades], [Ref]);

        exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));
        var deltas = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1));

        var delta = Assert.Single(deltas);
        Assert.Equal("AAPL", delta.Row["t_symbol"]);
        Assert.Equal(100.0, delta.Row["t_price"]);
        // r.tag keeps its ordinary (unqualified) default column name, same as ValidatorTests'
        // QualifiedStarWithJoinExpandsAllTradeColumnsPrefixedPlusExtraColumn — only the star-expanded t.*
        // columns are alias-prefixed.
        Assert.Equal("core", delta.Row["tag"]);
    }
}
