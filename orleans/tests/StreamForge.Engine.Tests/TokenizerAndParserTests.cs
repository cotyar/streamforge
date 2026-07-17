using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

public class TokenizerAndParserTests
{
    [Fact]
    public void SimpleSelectCompiles()
    {
        var r = Compile("SELECT symbol, price FROM trades", Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(["trades"], r.SourceNames);
    }

    [Fact]
    public void SelectStarCompiles()
    {
        var r = Compile("SELECT * FROM trades", Trades);
        Assert.True(r.Ok);
    }

    [Fact]
    public void LineCommentIsSkipped()
    {
        var sql = "-- this is a header comment\nSELECT symbol -- trailing comment\nFROM trades";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void StringLiteralWithEscapedQuoteEvaluates()
    {
        var exec = CompileAndCreate("SELECT 'it''s' AS x FROM trades", Trades);
        var results = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 1.0)));
        Assert.Single(results);
        Assert.Equal("it's", results[0]["x"]);
    }

    [Fact]
    public void UnexpectedCharacterProducesDiagnosticWithPosition()
    {
        var r = Compile("SELECT symbol FROM trades WHERE price # 1", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Line == 1 && d.Column == 39);
    }

    [Fact]
    public void UnterminatedStringProducesDiagnostic()
    {
        var r = Compile("SELECT 'oops FROM trades", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unterminated"));
    }

    [Fact]
    public void MultiLinePositionsAreTracked()
    {
        var sql = "SELECT symbol\nFROM trades\nWHERE @@\n";
        var r = Compile(sql, Trades);
        Assert.False(r.Ok);
        var d = r.Diagnostics[0];
        Assert.Equal(3, d.Line);
        Assert.Equal(7, d.Column);
    }

    [Fact]
    public void MissingFromKeywordIsParseError()
    {
        var r = Compile("SELECT symbol trades", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("FROM"));
    }

    [Fact]
    public void TrailingGarbageIsParseError()
    {
        // "trades EXTRA" alone is a valid implicit alias, so use punctuation that can never start a clause.
        var r = Compile("SELECT symbol FROM trades )", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unexpected token"));
    }

    [Fact]
    public void BadDurationUnitIsParseError()
    {
        var r = Compile("SELECT t.symbol FROM trades t INNER JOIN quotes q WITHIN 5 FORTNIGHTS ON t.symbol = q.symbol", Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("duration unit"));
    }

    [Theory]
    [InlineData("INNER")]
    [InlineData("LEFT")]
    [InlineData("LEFT OUTER")]
    [InlineData("RIGHT")]
    [InlineData("RIGHT OUTER")]
    [InlineData("FULL")]
    [InlineData("FULL OUTER")]
    public void AllJoinKindsParse(string kind)
    {
        var sql = $"SELECT t.symbol FROM trades t {kind} JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = Compile(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void BareJoinKeywordDefaultsToInner()
    {
        var r = Compile("SELECT t.symbol FROM trades t JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", Trades, Quotes);
        Assert.True(r.Ok);
        Assert.Contains("INNER", r.PlanSummary);
    }

    [Fact]
    public void CrossJoinParsesAndForbidsOn()
    {
        var r = Compile("SELECT t.symbol FROM trades t CROSS JOIN quotes q WITHIN 5 SECONDS", Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));

        var withOn = Compile("SELECT t.symbol FROM trades t CROSS JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol", Trades, Quotes);
        Assert.False(withOn.Ok);
    }

    [Theory]
    [InlineData("WINDOW TUMBLING(SIZE 10 SECONDS)")]
    [InlineData("WINDOW HOPPING(SIZE 10 SECONDS, ADVANCE BY 5 SECONDS)")]
    [InlineData("WINDOW SESSION(GAP 5 SECONDS)")]
    public void AllWindowKindsParse(string windowClause)
    {
        var sql = $"SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol {windowClause} EMIT FINAL";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Theory]
    [InlineData("EMIT CHANGES")]
    [InlineData("EMIT FINAL")]
    [InlineData("")]
    public void EmitVariantsParse(string emit)
    {
        var sql = $"SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) {emit}";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void SingularDurationUnitsAreAccepted()
    {
        var r = Compile("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 1 SECOND)", Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void OperatorPrecedenceParsesWithoutError()
    {
        // OR < AND < NOT < comparisons < + - < * / % < unary minus < primary
        var sql = "SELECT symbol FROM trades WHERE price + 1 * 2 > 3 AND NOT active OR qty - -1 = 5";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void GroupByMultipleColumnsParses()
    {
        var sql = "SELECT t.symbol, q.bid, COUNT(*) AS cnt FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol " +
                  "GROUP BY t.symbol, q.bid WINDOW TUMBLING(SIZE 10 SECONDS)";
        var r = Compile(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void PlanSummaryDescribesPipeline()
    {
        var sql = "SELECT t.symbol AS symbol, COUNT(*) AS cnt FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol " +
                  "WHERE t.price > 0 GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS) EMIT FINAL";
        var r = Compile(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("trades AS t", r.PlanSummary);
        Assert.Contains("⋈[INNER,5s] quotes AS q", r.PlanSummary);
        Assert.Contains("WHERE", r.PlanSummary);
        Assert.Contains("TUMBLING(5s)", r.PlanSummary);
        Assert.Contains("GROUP BY t_symbol", r.PlanSummary);
        Assert.Contains("SELECT 2", r.PlanSummary);
    }

    [Fact]
    public void CompileNeverThrowsOnGarbageInput()
    {
        foreach (var sql in new[] { "", "   ", "SELECT", "))))((((", "SELECT FROM FROM FROM", "🚀🚀🚀" })
        {
            var r = Compile(sql, Trades);
            Assert.False(r.Ok);
            Assert.NotEmpty(r.Diagnostics);
        }
    }
}
