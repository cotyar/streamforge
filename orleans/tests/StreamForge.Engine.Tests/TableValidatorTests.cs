using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

public class TableValidatorTests
{
    [Fact]
    public void WindowClauseIsForbiddenInTableMode()
    {
        var r = CompileTable("SELECT symbol FROM trades WINDOW TUMBLING(SIZE 5 SECONDS)", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("WINDOW clause not allowed in table mode"));
    }

    [Fact]
    public void EmitIsForbiddenInTableMode()
    {
        var r = CompileTable("SELECT symbol FROM trades EMIT CHANGES", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("EMIT not allowed in table mode"));
    }

    [Fact]
    public void AggregatesAndGroupByAreAllowedWithoutWindowInTableMode()
    {
        var r = CompileTable("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol", Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void GlobalAggregateWithoutGroupByIsAllowedInTableMode()
    {
        var r = CompileTable("SELECT COUNT(*) AS cnt, AVG(price) AS avgp FROM trades", Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void AmbiguousNamePresentInBothStreamsAndTablesIsError()
    {
        var tradesAsTable = Schema("trades", ("symbol", FieldKind.String));
        var r = CompileTable("SELECT symbol FROM trades", [Trades], [tradesAsTable]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Ambiguous name 'trades'"));
    }

    [Fact]
    public void UnknownSourceListsBothStreamsAndTables()
    {
        var positions = Schema("positions", ("symbol", FieldKind.String));
        var r = CompileTable("SELECT symbol FROM bogus", [Trades], [positions]);
        Assert.False(r.Ok);
        var d = Assert.Single(r.Diagnostics, d => d.Message.Contains("Unknown source 'bogus'"));
        Assert.Contains("trades", d.Message);
        Assert.Contains("positions", d.Message);
    }

    [Fact]
    public void WithinClauseOnJoinIsForbiddenInTableMode()
    {
        var sql = "SELECT t.symbol FROM trades t JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("WITHIN clause in table mode"));
    }

    [Fact]
    public void CrossJoinIsForbiddenInTableMode()
    {
        var sql = "SELECT t.symbol FROM trades t CROSS JOIN quotes q";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("CROSS JOIN is not allowed in table mode"));
    }

    [Fact]
    public void LeftJoinIsNotSupportedInTableMode()
    {
        var sql = "SELECT t.symbol FROM trades t LEFT JOIN quotes q ON t.symbol = q.symbol";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("LEFT JOIN is not allowed in table mode"));
    }

    [Fact]
    public void InnerJoinRequiresEquiComparisonInTableMode()
    {
        var sql = "SELECT t.symbol FROM trades t JOIN quotes q ON t.price > q.bid";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("equi-comparison"));
    }

    [Fact]
    public void InnerJoinWithoutWithinCompilesInTableMode()
    {
        var sql = "SELECT t.symbol, q.bid FROM trades t JOIN quotes q ON t.symbol = q.symbol";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("trades", r.StreamInputs);
        Assert.Contains("quotes", r.StreamInputs);
    }

    [Fact]
    public void NonAggregateSelectItemMustAppearInGroupByWithoutWindow()
    {
        var sql = "SELECT symbol, price, COUNT(*) AS cnt FROM trades GROUP BY symbol";
        var r = CompileTable(sql, Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Non-aggregate select item must appear in GROUP BY"));
    }

    [Fact]
    public void QualifiedStarResolvesInTableMode()
    {
        var positions = Schema("positions", ("symbol", FieldKind.String), ("trades", FieldKind.Long));
        var sql = "SELECT p.* FROM positions p";
        var r = CompileTable(sql, [Trades], [positions]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("symbol", r.OutputSchema!.Fields.Keys);
        Assert.Contains("trades", r.OutputSchema.Fields.Keys);
    }

    [Fact]
    public void QualifiedStarUnknownAliasIsErrorInTableMode()
    {
        var r = CompileTable("SELECT z.* FROM trades t", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unknown source/alias 'z'"));
    }

    [Fact]
    public void TableInputResolvesSeparatelyFromStreamInput()
    {
        var positions = Schema("positions", ("symbol", FieldKind.String), ("trades", FieldKind.Long));
        var sql = "SELECT p.symbol, p.trades FROM positions p WHERE p.trades > 50";
        var r = CompileTable(sql, [Trades], [positions]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Empty(r.StreamInputs);
        Assert.Contains("positions", r.TableInputs);
    }
}
