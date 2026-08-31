using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 009 Round C wave C1 — runtime (ExpressionEvaluator) behavior of TO_LONG/TO_DOUBLE/TO_BOOL/
/// TO_TIMESTAMP/TO_STRING and CAST(expr AS type) sugar, in both pipeline and table mode. Result-kind/
/// arity/diagnostics live in <see cref="TypeConversionValidatorTests"/>; canonical-conversion-rule unit
/// coverage lives in <see cref="FieldValueConversionTests"/>. Every function here is total: an
/// unconvertible or NULL argument yields NULL, never an exception (DESIGN.md §D11).
/// </summary>
public class TypeConversionFunctionsTests
{
    private static readonly SourceSchema Mixed = Schema(
        "mixed",
        ("s", FieldKind.String), ("l", FieldKind.Long), ("d", FieldKind.Double),
        ("b", FieldKind.Bool), ("j", FieldKind.Json));

    private static readonly SourceSchema PriceFeed = Schema(
        "pricefeed", ("symbol", FieldKind.String), ("price_str", FieldKind.String));

    private static object? EvalSingle(string selectExpr, params (string Field, object? Value)[] eventFields)
    {
        var exec = CompileAndCreate($"SELECT {selectExpr} AS x FROM mixed", Mixed);
        var results = exec.OnEvent("mixed", Evt(1000, "mixed", eventFields));
        Assert.Single(results);
        return results[0]["x"];
    }

    private static object? EvalSingleTable(string selectExpr, params (string Field, object? Value)[] eventFields)
    {
        var exec = CompileTableAndCreate($"SELECT {selectExpr} AS x FROM mixed", Mixed);
        var deltas = exec.OnStreamEvent("mixed", Evt(1000, "mixed", eventFields));
        Assert.Single(deltas);
        return deltas[0].Row["x"];
    }

    // ------------------------------------------------------------------
    // TO_LONG — every source kind
    // ------------------------------------------------------------------

    [Fact]
    public void ToLong_FromLong_IsIdentity()
    {
        Assert.Equal(42L, EvalSingle("TO_LONG(l)", ("l", 42L), ("s", "x"), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToLong_FromDouble_Truncates()
    {
        Assert.Equal(3L, EvalSingle("TO_LONG(d)", ("d", 3.9), ("s", "x"), ("l", 1L), ("b", true)));
    }

    [Fact]
    public void ToLong_FromBool_IsOneOrZero()
    {
        Assert.Equal(1L, EvalSingle("TO_LONG(b)", ("b", true), ("s", "x"), ("l", 1L), ("d", 1.0)));
        Assert.Equal(0L, EvalSingle("TO_LONG(b)", ("b", false), ("s", "x"), ("l", 1L), ("d", 1.0)));
    }

    [Fact]
    public void ToLong_FromNumericString_Parses()
    {
        Assert.Equal(42L, EvalSingle("TO_LONG(s)", ("s", "42"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToLong_FromNonNumericString_IsNull()
    {
        Assert.Null(EvalSingle("TO_LONG(s)", ("s", "abc"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToLong_FromOverflowingNumericString_IsNull()
    {
        Assert.Null(EvalSingle("TO_LONG(s)", ("s", "99999999999999999999"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToLong_NullInputIsNull()
    {
        Assert.Null(EvalSingle("TO_LONG(s)", ("s", null), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    // ------------------------------------------------------------------
    // TO_DOUBLE — every source kind
    // ------------------------------------------------------------------

    [Fact]
    public void ToDouble_FromLong_Promotes()
    {
        Assert.Equal(5.0, EvalSingle("TO_DOUBLE(l)", ("l", 5L), ("s", "x"), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToDouble_FromDouble_IsIdentity()
    {
        Assert.Equal(3.5, EvalSingle("TO_DOUBLE(d)", ("d", 3.5), ("s", "x"), ("l", 1L), ("b", true)));
    }

    [Fact]
    public void ToDouble_FromBool_IsOneOrZero()
    {
        Assert.Equal(1.0, EvalSingle("TO_DOUBLE(b)", ("b", true), ("s", "x"), ("l", 1L), ("d", 1.0)));
    }

    [Fact]
    public void ToDouble_FromNumericString_Parses()
    {
        Assert.Equal(3.14, EvalSingle("TO_DOUBLE(s)", ("s", "3.14"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToDouble_FromEmptyString_IsNull()
    {
        Assert.Null(EvalSingle("TO_DOUBLE(s)", ("s", ""), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToDouble_FromNonNumericString_IsNull()
    {
        Assert.Null(EvalSingle("TO_DOUBLE(s)", ("s", "not-a-number"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToDouble_NullInputIsNull()
    {
        Assert.Null(EvalSingle("TO_DOUBLE(s)", ("s", null), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    // ------------------------------------------------------------------
    // TO_BOOL — every source kind, including the documented permissive-string finding
    // ------------------------------------------------------------------

    [Fact]
    public void ToBool_FromLong_IsNonzeroIsTrue()
    {
        Assert.Equal(true, EvalSingle("TO_BOOL(l)", ("l", 2L), ("s", "x"), ("d", 1.0), ("b", true)));
        Assert.Equal(false, EvalSingle("TO_BOOL(l)", ("l", 0L), ("s", "x"), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToBool_FromDouble_IsNonzeroIsTrue()
    {
        Assert.Equal(true, EvalSingle("TO_BOOL(d)", ("d", 2.5), ("s", "x"), ("l", 1L), ("b", true)));
        Assert.Equal(false, EvalSingle("TO_BOOL(d)", ("d", 0.0), ("s", "x"), ("l", 1L), ("b", true)));
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("TRUE", true)]
    [InlineData("false", false)]
    [InlineData("0", false)]
    [InlineData("", false)]
    public void ToBool_FromRecognizedStringSpellings(string input, bool expected)
    {
        Assert.Equal(expected, EvalSingle("TO_BOOL(s)", ("s", input), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToBool_FromAnyOtherStringIsTrue_DocumentedFinding()
    {
        // See FieldValueConversion's class doc: this is a pinned, documented divergence from a strict
        // "true/false/0/1 else NULL" rule — the canonical function must match
        // FieldValueCoercion.TryToBool's existing (permissive) inbound-path behavior exactly, since
        // that AppCore call site is meant to delegate to the very same code.
        Assert.Equal(true, EvalSingle("TO_BOOL(s)", ("s", "banana"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToBool_NullInputIsNull()
    {
        Assert.Null(EvalSingle("TO_BOOL(s)", ("s", null), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    // ------------------------------------------------------------------
    // TO_TIMESTAMP — epoch-ms (number or numeric string) or ISO-8601 text
    // ------------------------------------------------------------------

    [Fact]
    public void ToTimestamp_FromLong_IsIdentity()
    {
        Assert.Equal(1_700_000_000_000L, EvalSingle("TO_TIMESTAMP(l)", ("l", 1_700_000_000_000L), ("s", "x"), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToTimestamp_FromDouble_Truncates()
    {
        Assert.Equal(1_700_000_000_000L, EvalSingle("TO_TIMESTAMP(d)", ("d", 1_700_000_000_000.0), ("s", "x"), ("l", 1L), ("b", true)));
    }

    [Fact]
    public void ToTimestamp_FromNumericString_ParsesAsEpochMs()
    {
        Assert.Equal(1_700_000_000_000L, EvalSingle("TO_TIMESTAMP(s)", ("s", "1700000000000"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToTimestamp_FromIso8601Text_Parses()
    {
        Assert.Equal(1_700_000_000_000L, EvalSingle("TO_TIMESTAMP(s)", ("s", "2023-11-14T22:13:20Z"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToTimestamp_FromUnparseableStringIsNull()
    {
        Assert.Null(EvalSingle("TO_TIMESTAMP(s)", ("s", "not-a-timestamp"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToTimestamp_FromBoolIsNull()
    {
        Assert.Null(EvalSingle("TO_TIMESTAMP(b)", ("b", true), ("s", "x"), ("l", 1L), ("d", 1.0)));
    }

    [Fact]
    public void ToTimestamp_NullInputIsNull()
    {
        Assert.Null(EvalSingle("TO_TIMESTAMP(s)", ("s", null), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    // ------------------------------------------------------------------
    // TO_STRING — culture-invariant, every source kind, plus JSON/timestamp special cases
    // ------------------------------------------------------------------

    [Fact]
    public void ToString_FromString_IsIdentity()
    {
        Assert.Equal("hi", EvalSingle("TO_STRING(s)", ("s", "hi"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToString_FromLong_IsInvariantIntegerText()
    {
        Assert.Equal("42", EvalSingle("TO_STRING(l)", ("l", 42L), ("s", "x"), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToString_FromDouble_IsInvariantText()
    {
        Assert.Equal("3.5", EvalSingle("TO_STRING(d)", ("d", 3.5), ("s", "x"), ("l", 1L), ("b", true)));
    }

    [Fact]
    public void ToString_FromBool_IsLowercaseLiteral()
    {
        Assert.Equal("true", EvalSingle("TO_STRING(b)", ("b", true), ("s", "x"), ("l", 1L), ("d", 1.0)));
        Assert.Equal("false", EvalSingle("TO_STRING(b)", ("b", false), ("s", "x"), ("l", 1L), ("d", 1.0)));
    }

    [Fact]
    public void ToString_NullInputIsNull()
    {
        Assert.Null(EvalSingle("TO_STRING(s)", ("s", null), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    [Fact]
    public void ToString_OfToTimestamp_RendersIso8601()
    {
        // Syntactic detection: TO_STRING(TO_TIMESTAMP(...)) renders ISO-8601 text rather than a bare
        // integer — see ExpressionEvaluator.EvalToString's doc comment on why this is detected from the
        // AST shape (the argument being a TO_TIMESTAMP call) rather than from a runtime type tag, which
        // doesn't exist for FieldKind.Timestamp vs FieldKind.Long (both are a bare CLR `long`).
        var v = EvalSingle("TO_STRING(TO_TIMESTAMP(l))", ("l", 1_700_000_000_000L), ("s", "x"), ("d", 1.0), ("b", true));
        Assert.Equal("2023-11-14T22:13:20.000Z", v);
    }

    [Fact]
    public void ToString_OfCastAsTimestamp_RendersIso8601()
    {
        // CAST(... AS TIMESTAMP) desugars to the same TO_TIMESTAMP node, so the same detection fires.
        var v = EvalSingle("TO_STRING(CAST(l AS TIMESTAMP))", ("l", 1_700_000_000_000L), ("s", "x"), ("d", 1.0), ("b", true));
        Assert.Equal("2023-11-14T22:13:20.000Z", v);
    }

    [Fact]
    public void ToString_OfABareLongDoesNotRenderIso8601()
    {
        // Contrast with the two tests above: without a syntactic TO_TIMESTAMP wrapper, a long renders
        // as a plain integer even if it happens to be epoch-ms-shaped.
        Assert.Equal("1700000000000", EvalSingle("TO_STRING(l)", ("l", 1_700_000_000_000L), ("s", "x"), ("d", 1.0), ("b", true)));
    }

    // ------------------------------------------------------------------
    // Round trip
    // ------------------------------------------------------------------

    [Fact]
    public void RoundTrip_ToStringOfToLong()
    {
        Assert.Equal("42", EvalSingle("TO_STRING(TO_LONG(s))", ("s", "42"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    // ------------------------------------------------------------------
    // JSON leaves — the wave's motivating case: a terminal '->' access cast to a usable type
    // ------------------------------------------------------------------

    private static object? EvalJson(string selectExpr, object? payload)
    {
        var exec = CompileAndCreate($"SELECT {selectExpr} AS x FROM events", Events);
        var results = exec.OnEvent("events", Evt(1000, "events", ("eventType", "e"), ("payload", payload)));
        Assert.Single(results);
        return results[0]["x"];
    }

    [Fact]
    public void ToDouble_OverArrowAccess_ParsesAQuotedNumber()
    {
        // The repo's own documented '->'/'->>' gotcha made survivable: a producer that quoted its
        // numbers (payload -> 'qty' is a JSON string leaf "10.5") can now be summed/compared.
        var payload = new Dictionary<string, object?> { ["qty"] = "10.5" };
        Assert.Equal(10.5, EvalJson("TO_DOUBLE(payload -> 'qty')", payload));
    }

    [Fact]
    public void ToLong_OverArrowAccess_OnANumericJsonLeaf()
    {
        var payload = new Dictionary<string, object?> { ["qty"] = 10L };
        Assert.Equal(10L, EvalJson("TO_LONG(payload -> 'qty')", payload));
    }

    [Fact]
    public void ToString_OverArrowAccess_OnACompositeJsonNode_RendersCompactJsonText()
    {
        var payload = new Dictionary<string, object?>
        {
            ["order"] = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = 10L },
        };
        Assert.Equal("{\"symbol\":\"AAPL\",\"qty\":10}", EvalJson("TO_STRING(payload -> 'order')", payload));
    }

    // ------------------------------------------------------------------
    // CAST(expr AS type) sugar — identical results to the TO_* function form
    // ------------------------------------------------------------------

    [Theory]
    [InlineData("TO_LONG(s)", "CAST(s AS LONG)")]
    [InlineData("TO_DOUBLE(s)", "CAST(s AS DOUBLE)")]
    [InlineData("TO_BOOL(s)", "CAST(s AS BOOL)")]
    [InlineData("TO_TIMESTAMP(s)", "CAST(s AS TIMESTAMP)")]
    public void CastSugarProducesIdenticalResultsToFunctionForm(string fnForm, string castForm)
    {
        var fnResult = EvalSingle(fnForm, ("s", "42"), ("l", 1L), ("d", 1.0), ("b", true));
        var castResult = EvalSingle(castForm, ("s", "42"), ("l", 1L), ("d", 1.0), ("b", true));
        Assert.Equal(fnResult, castResult);
    }

    [Fact]
    public void CastSugarOnUnconvertibleInputAlsoYieldsNull()
    {
        Assert.Null(EvalSingle("CAST(s AS LONG)", ("s", "not-a-number"), ("l", 1L), ("d", 1.0), ("b", true)));
    }

    // ------------------------------------------------------------------
    // Casts inside WHERE / GROUP BY
    // ------------------------------------------------------------------

    [Fact]
    public void CastInWhereClauseFiltersOnTheConvertedValue()
    {
        var exec = CompileAndCreate("SELECT s AS x FROM mixed WHERE TO_DOUBLE(s) > 10", Mixed);

        var passing = exec.OnEvent("mixed", Evt(1000, "mixed", ("s", "15.5"), ("l", 1L), ("d", 1.0), ("b", true)));
        Assert.Single(passing);

        var failing = exec.OnEvent("mixed", Evt(1000, "mixed", ("s", "abc"), ("l", 1L), ("d", 1.0), ("b", true)));
        Assert.Empty(failing); // TO_DOUBLE('abc') > 10 is NULL > 10 = NULL -> WHERE filters it out
    }

    [Fact]
    public void CastInGroupByGroupsRowsByTheConvertedValue_TableMode()
    {
        var exec = CompileTableAndCreate(
            "SELECT TO_LONG(s) AS grp, COUNT(*) AS cnt FROM mixed GROUP BY TO_LONG(s)", Mixed);

        exec.OnStreamEvent("mixed", Evt(1000, "mixed", ("s", "1"), ("l", 1L), ("d", 1.0), ("b", true)));
        var deltas = exec.OnStreamEvent("mixed", Evt(1000, "mixed", ("s", "1"), ("l", 1L), ("d", 1.0), ("b", true)));

        // Same group both times ("1" -> 1L both rows) -> retract-then-assert with an updated count.
        Assert.Equal(2, deltas.Count);
        Assert.Equal(2L, deltas[1].Row["cnt"]);
        Assert.Equal(1L, deltas[1].Row["grp"]);
    }

    // ------------------------------------------------------------------
    // Motivating case: SUM(TO_DOUBLE(price_str)) — a String-declared price column, summed
    // ------------------------------------------------------------------

    [Fact]
    public void SumOfToDoubleOverStringPriceColumn_PipelineMode()
    {
        var sql = "SELECT symbol, SUM(TO_DOUBLE(price_str)) AS total FROM pricefeed " +
                  "GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = CompileAndCreate(sql, PriceFeed);

        exec.OnEvent("pricefeed", Evt(1000, "pricefeed", ("symbol", "AAPL"), ("price_str", "10.5")));
        exec.OnEvent("pricefeed", Evt(2000, "pricefeed", ("symbol", "AAPL"), ("price_str", "5.25")));

        var closed = exec.AdvanceWatermark(12000);
        var row = Assert.Single(closed);
        Assert.Equal("AAPL", row["symbol"]);
        Assert.Equal(15.75, row["total"]);
    }

    [Fact]
    public void SumOfToDoubleOverStringPriceColumn_TableMode()
    {
        var exec = CompileTableAndCreate(
            "SELECT symbol, SUM(TO_DOUBLE(price_str)) AS total FROM pricefeed GROUP BY symbol", PriceFeed);

        exec.OnStreamEvent("pricefeed", Evt(1000, "pricefeed", ("symbol", "AAPL"), ("price_str", "10.5")));
        exec.OnStreamEvent("pricefeed", Evt(1000, "pricefeed", ("symbol", "AAPL"), ("price_str", "5.25")));

        var snapshot = exec.Snapshot();
        var current = Assert.Single(snapshot);
        Assert.Equal("AAPL", current.Value.Row["symbol"]);
        Assert.Equal(15.75, current.Value.Row["total"]);
    }

    [Fact]
    public void SumOfToDoubleSkipsUnconvertibleRowsAsZeroContribution()
    {
        // A row whose price_str doesn't parse contributes NULL, which SUM treats as "no contribution"
        // (existing aggregate NULL-skip semantics) — not a thrown exception, not a poisoned window.
        var sql = "SELECT symbol, SUM(TO_DOUBLE(price_str)) AS total FROM pricefeed " +
                  "GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var exec = CompileAndCreate(sql, PriceFeed);

        exec.OnEvent("pricefeed", Evt(1000, "pricefeed", ("symbol", "AAPL"), ("price_str", "10.5")));
        exec.OnEvent("pricefeed", Evt(2000, "pricefeed", ("symbol", "AAPL"), ("price_str", "garbage")));

        var closed = exec.AdvanceWatermark(12000);
        var row = Assert.Single(closed);
        Assert.Equal(10.5, row["total"]);
    }
}
