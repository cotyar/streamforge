using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

public class ValidatorTests
{
    [Fact]
    public void UnknownSourceListsAvailableSources()
    {
        var r = Compile("SELECT symbol FROM ticks", Trades, Quotes);
        Assert.False(r.Ok);
        var d = Assert.Single(r.Diagnostics, d => d.Message.Contains("Unknown source 'ticks'"));
        Assert.Contains("quotes", d.Message);
        Assert.Contains("trades", d.Message);
    }

    [Fact]
    public void UnknownColumnHasPosition()
    {
        var r = Compile("SELECT bogus FROM trades", Trades);
        Assert.False(r.Ok);
        var d = Assert.Single(r.Diagnostics);
        Assert.Contains("Unknown column 'bogus'", d.Message);
        Assert.Equal(1, d.Line);
        Assert.Equal(8, d.Column);
    }

    [Fact]
    public void AmbiguousUnqualifiedColumnAcrossJoinedSourcesIsError()
    {
        // both trades and quotes declare "symbol"
        var sql = "SELECT symbol FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = Compile(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Ambiguous column 'symbol'"));
    }

    [Fact]
    public void QualifiedColumnResolvesAliasUnambiguously()
    {
        var sql = "SELECT t.symbol FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = Compile(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void UnknownAliasQualifierIsError()
    {
        var r = Compile("SELECT z.symbol FROM trades t", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unknown source 'z'"));
    }

    [Fact]
    public void ReservedColumnsAreAccessiblePerAlias()
    {
        var sql = "SELECT t._ts AS tts, t._source AS tsrc FROM trades t";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void AggregateWithoutWindowIsError()
    {
        var r = Compile("SELECT COUNT(*) AS cnt FROM trades", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Aggregate functions require a WINDOW clause"));
    }

    [Fact]
    public void GroupByWithoutWindowIsError()
    {
        var r = Compile("SELECT symbol FROM trades GROUP BY symbol", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("GROUP BY requires a WINDOW clause"));
    }

    [Fact]
    public void EmitWithoutWindowIsError()
    {
        var r = Compile("SELECT symbol FROM trades EMIT CHANGES", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("EMIT requires a WINDOW clause"));
    }

    [Fact]
    public void NonAggregateSelectItemMustAppearInGroupBy()
    {
        var sql = "SELECT symbol, price, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = Compile(sql, Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Non-aggregate select item must appear in GROUP BY"));
    }

    [Fact]
    public void NestedAggregateIsError()
    {
        var sql = "SELECT SUM(COUNT(*)) AS x FROM trades WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = Compile(sql, Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("cannot be nested"));
    }

    [Fact]
    public void JoinMissingWithinIsError()
    {
        var sql = "SELECT t.symbol FROM trades t INNER JOIN quotes q ON t.symbol = q.symbol";
        var r = Compile(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("WITHIN"));
    }

    [Fact]
    public void JoinWithoutEquiComparisonIsError()
    {
        var sql = "SELECT t.symbol FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.price > q.bid";
        var r = Compile(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("equi-comparison"));
    }

    [Fact]
    public void JoinEquiComparisonWithResidualCompiles()
    {
        var sql = "SELECT t.symbol FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol AND t.price > q.bid";
        var r = Compile(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void DuplicateAliasIsError()
    {
        var sql = "SELECT t.symbol FROM trades t INNER JOIN quotes t WITHIN 5 SECONDS ON t.symbol = t.symbol";
        var r = Compile(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Duplicate alias"));
    }

    [Fact]
    public void UnknownFunctionIsError()
    {
        var r = Compile("SELECT NOPE(price) AS x FROM trades", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unknown function"));
    }

    [Fact]
    public void FunctionArityMismatchIsError()
    {
        var r = Compile("SELECT ABS(price, qty) AS x FROM trades", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("wrong number of arguments"));
    }

    [Fact]
    public void SelfJoinUnderDifferentAliasesIsValid()
    {
        var sql = "SELECT a.symbol, b.symbol FROM trades a INNER JOIN trades b WITHIN 5 SECONDS ON a.symbol = b.symbol";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(["trades"], r.SourceNames);
    }

    [Fact]
    public void SelectStarWithJoinPrefixesColumnsWithAlias()
    {
        var sql = "SELECT * FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var exec = CompileAndCreate(sql, Trades, Quotes);
        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));
        var results = exec.OnEvent("quotes", Evt(1100, "quotes", ("symbol", "AAPL"), ("bid", 99.0), ("ask", 101.0)));
        Assert.Single(results);
        Assert.True(results[0].ContainsKey("t_symbol"));
        Assert.True(results[0].ContainsKey("q_bid"));
        Assert.False(results[0].ContainsKey("t__ts")); // reserved fields not included in *
    }

    [Fact]
    public void SelectStarWithoutJoinUsesPlainNames()
    {
        var exec = CompileAndCreate("SELECT * FROM trades", Trades);
        var results = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));
        Assert.Single(results);
        Assert.True(results[0].ContainsKey("symbol"));
        Assert.True(results[0].ContainsKey("price"));
    }
}
