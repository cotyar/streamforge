using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

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

    // ------------------------------------------------------------------
    // JSON access ('->' / '->>') — tokenizer disambiguation and parser precedence.
    // Internal Tokenizer/Parser types aren't exposed to tests, so — like every other tokenizer/parser
    // test in this file — correctness is observed through Compile()/CompileAndCreate(): a mis-tokenized
    // '->'/'->>' would either fail to parse or evaluate to the wrong value.
    // ------------------------------------------------------------------

    [Fact]
    public void MinusAndGreaterThanTokenizeCorrectlyWithNoWhitespace()
    {
        // "qty-price" must lex as (qty)(-)(price), not be swallowed by the new '->'/'->>' matching;
        // "qty>0" must stay a plain '>' comparison.
        var exec = CompileAndCreate("SELECT qty-price AS diff FROM trades WHERE qty>0", Trades);
        var results = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 3.0), ("qty", 10L), ("active", true)));
        Assert.Single(results);
        Assert.Equal(7.0, results[0]["diff"]);
    }

    [Fact]
    public void ArrowAndArrowArrowLexCorrectlyWithAndWithoutWhitespace()
    {
        // "payload->'k'" (no space) and "payload ->> 'k'" (spaced) must both lex as single JSON
        // operator tokens, not decompose into '-'/'>' pairs (which would be a parse error here).
        var r = Compile("SELECT payload->'k' AS viaArrow, payload ->> 'k' AS viaArrowArrow FROM events", Events);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void ArrowArrowIsMatchedBeforeArrowSoItIsNotSplit()
    {
        // If '->>' were mis-lexed as '->' followed by a stray '>', this would be a parse error
        // (an operand can't start with '>'). Compiling successfully proves the longest match won.
        var r = Compile("SELECT payload ->> 'k' AS x FROM events", Events);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void ChainedJsonAccessParsesLeftAssociativeAndBindsTighterThanComparison()
    {
        // a -> 'k' ->> 'x' = 'y' must parse as ((a -> 'k') ->> 'x') = 'y'. If '->'/'->>' bound looser
        // than '=' (or the chain were right-associative), 'x' = 'y' would need to itself be a valid
        // JSON key (it isn't — keys are literals only), so this would fail to compile.
        var exec = CompileAndCreate("SELECT eventType AS x FROM events WHERE payload -> 'k' ->> 'x' = 'y'", Events);

        var matches = new Dictionary<string, object?> { ["k"] = new Dictionary<string, object?> { ["x"] = "y" } };
        var matchResult = exec.OnEvent("events", Evt(1000, "events", ("eventType", "e1"), ("payload", matches)));
        Assert.Single(matchResult);

        var mismatches = new Dictionary<string, object?> { ["k"] = new Dictionary<string, object?> { ["x"] = "z" } };
        var mismatchResult = exec.OnEvent("events", Evt(2000, "events", ("eventType", "e2"), ("payload", mismatches)));
        Assert.Empty(mismatchResult);
    }

    [Fact]
    public void JsonAccessBindsTighterThanUnaryMinus()
    {
        // '-payload -> 'k'' must parse as '-(payload -> 'k')', not '(-payload) -> 'k''. The two
        // groupings are behaviorally distinguishable: with the intended (tighter-than-unary) grouping,
        // 'payload -> 'k'' evaluates first (5L), then negation gives -5L. Under the wrong grouping,
        // unary '-' would apply to the raw Dictionary payload first (evaluates to NULL, since unary
        // minus only negates long/double), and NULL -> 'k' is NULL — so a NULL result would mean the
        // parser bound '-' the wrong way around.
        var exec = CompileAndCreate("SELECT -payload -> 'k' AS x FROM events", Events);
        var payload = new Dictionary<string, object?> { ["k"] = 5L };
        var results = exec.OnEvent("events", Evt(1000, "events", ("eventType", "e"), ("payload", payload)));
        Assert.Single(results);
        Assert.Equal(-5L, results[0]["x"]);
    }

    // ------------------------------------------------------------------
    // Qualified star `alias.*` in the select list
    // ------------------------------------------------------------------

    [Fact]
    public void QualifiedStarParses()
    {
        var r = Compile("SELECT t.* FROM trades t", Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void QualifiedStarAlongsideOtherColumnParses()
    {
        var sql = "SELECT t.*, q.bid FROM trades t INNER JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = Compile(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void QualifiedStarWithAsAliasIsParseError()
    {
        var r = Compile("SELECT t.* AS x FROM trades t", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("alias.*"));
    }

    [Fact]
    public void QualifiedStarWithImplicitAliasIsParseError()
    {
        var r = Compile("SELECT t.* x FROM trades t", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("alias.*"));
    }

    [Fact]
    public void OriginalBugQueryCompilesOk()
    {
        // The exact query reported as a false validation error before this feature.
        var r = Compile("SELECT t.* FROM trades t", Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void JsonAccessKeyMustBeStringOrIntegerLiteral()
    {
        var parenExpr = Compile("SELECT payload -> (1 + 1) AS x FROM events", Events);
        Assert.False(parenExpr.Ok);
        Assert.Contains(parenExpr.Diagnostics, d => d.Message.Contains("string literal") && d.Message.Contains("integer literal"));

        var doubleKey = Compile("SELECT payload -> 1.5 AS x FROM events", Events);
        Assert.False(doubleKey.Ok);
        Assert.Contains(doubleKey.Diagnostics, d => d.Message.Contains("string literal") && d.Message.Contains("integer literal"));
    }
}
