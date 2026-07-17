using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

public class ExpressionSemanticsTests
{
    private static object? EvalSingle(string selectExpr, params (string Field, object? Value)[] eventFields)
    {
        var exec = CompileAndCreate($"SELECT {selectExpr} AS x FROM trades", Trades);
        var results = exec.OnEvent("trades", Evt(1000, "trades", eventFields));
        Assert.Single(results);
        return results[0]["x"];
    }

    private static IReadOnlyList<EventRecord> EvalWhere(string whereExpr, params (string Field, object? Value)[] eventFields)
    {
        var exec = CompileAndCreate($"SELECT symbol AS x FROM trades WHERE {whereExpr}", Trades);
        return exec.OnEvent("trades", Evt(1000, "trades", eventFields));
    }

    [Fact]
    public void NullPlusNumberIsNull()
    {
        Assert.Null(EvalSingle("price + 1", ("price", null), ("symbol", "A"), ("qty", 1L), ("active", true)));
    }

    [Fact]
    public void NullEqualsNullIsNullSoWhereFiltersRowOut()
    {
        var results = EvalWhere("price = price", ("price", null), ("symbol", "A"), ("qty", 1L), ("active", true));
        Assert.Empty(results);
    }

    [Fact]
    public void MixedLongAndDoublePromotesToDouble()
    {
        var v = EvalSingle("qty + price", ("qty", 2L), ("price", 1.5), ("symbol", "A"), ("active", true));
        Assert.IsType<double>(v);
        Assert.Equal(3.5, v);
    }

    [Fact]
    public void LongPlusLongStaysLong()
    {
        var v = EvalSingle("qty + 3", ("qty", 2L), ("price", 1.5), ("symbol", "A"), ("active", true));
        Assert.IsType<long>(v);
        Assert.Equal(5L, v);
    }

    [Fact]
    public void DivisionAlwaysProducesDouble()
    {
        var v = EvalSingle("qty / 2", ("qty", 4L), ("price", 1.0), ("symbol", "A"), ("active", true));
        Assert.IsType<double>(v);
        Assert.Equal(2.0, v);
    }

    [Fact]
    public void DivisionByZeroIsNull()
    {
        Assert.Null(EvalSingle("qty / 0", ("qty", 4L), ("price", 1.0), ("symbol", "A"), ("active", true)));
    }

    [Fact]
    public void StringComparisonIsOrdinal()
    {
        var v = EvalSingle("symbol > 'AAA'", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true));
        Assert.Equal(true, v);
    }

    [Fact]
    public void CoalesceReturnsFirstNonNull()
    {
        var v = EvalSingle("COALESCE(price, qty, 99)", ("price", null), ("qty", null), ("symbol", "A"), ("active", true));
        Assert.Equal(99L, v);
    }

    [Fact]
    public void AbsRoundUpperLowerFunctions()
    {
        Assert.Equal(5.0, EvalSingle("ABS(price)", ("price", -5.0), ("symbol", "a"), ("qty", 1L), ("active", true)));
        Assert.Equal(3.14, EvalSingle("ROUND(price, 2)", ("price", 3.14159), ("symbol", "a"), ("qty", 1L), ("active", true)));
        Assert.Equal("AAPL", EvalSingle("UPPER(symbol)", ("symbol", "aapl"), ("price", 1.0), ("qty", 1L), ("active", true)));
        Assert.Equal("aapl", EvalSingle("LOWER(symbol)", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true)));
    }

    [Fact]
    public void ThreeValuedAndLogic()
    {
        // NULL AND FALSE = FALSE
        var r1 = EvalWhere("active AND price > 0", ("active", null), ("price", -1.0), ("symbol", "a"), ("qty", 1L));
        Assert.Empty(r1);

        // TRUE AND NULL = NULL -> filtered out
        var r2 = EvalWhere("active AND price > 0", ("active", true), ("price", null), ("symbol", "a"), ("qty", 1L));
        Assert.Empty(r2);
    }

    [Fact]
    public void ThreeValuedOrLogic()
    {
        // NULL OR TRUE = TRUE -> row passes
        var r1 = EvalWhere("active OR qty > 0", ("active", null), ("qty", 5L), ("price", 1.0), ("symbol", "a"));
        Assert.Single(r1);

        // FALSE OR NULL = NULL -> filtered
        var r2 = EvalWhere("active OR price > 100", ("active", false), ("price", null), ("qty", 1L), ("symbol", "a"));
        Assert.Empty(r2);
    }

    [Fact]
    public void NotNullIsNull()
    {
        var results = EvalWhere("NOT active", ("active", null), ("price", 1.0), ("qty", 1L), ("symbol", "a"));
        Assert.Empty(results);
    }

    [Fact]
    public void UnaryMinusNegatesNumbers()
    {
        Assert.Equal(-5L, EvalSingle("-qty", ("qty", 5L), ("price", 1.0), ("symbol", "a"), ("active", true)));
        Assert.Equal(-1.5, EvalSingle("-price", ("price", 1.5), ("qty", 1L), ("symbol", "a"), ("active", true)));
    }

    [Fact]
    public void ModuloOperator()
    {
        Assert.Equal(1L, EvalSingle("qty % 2", ("qty", 5L), ("price", 1.0), ("symbol", "a"), ("active", true)));
    }
}
