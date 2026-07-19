using System.Text.Json;
using StreamForge.AppCore.Json;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W4: round-trip coverage for the normalizer every Dapr pub/sub ingress
/// will run payloads through starting W5 (decision D-D). Written now because the class already exists in
/// shared/StreamForge.AppCore and W4 is the first Dapr-side test project to land.
/// Integers must stay <see cref="long"/> (NormalizeNumber deliberately avoids a long/double ternary,
/// which would unify both branches to <c>double</c>).
/// </summary>
public class JsonValueNormalizerTests
{
    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public void Normalize_String_ReturnsClrString()
    {
        var element = Parse("\"hello\"");
        Assert.Equal("hello", JsonValueNormalizer.Normalize(element));
    }

    [Fact]
    public void Normalize_IntegerNumber_ReturnsLong()
    {
        var element = Parse("42");
        var result = JsonValueNormalizer.Normalize(element);
        Assert.IsType<long>(result);
        Assert.Equal(42L, result);
    }

    [Fact]
    public void Normalize_FractionalNumber_ReturnsDouble()
    {
        var element = Parse("42.5");
        var result = JsonValueNormalizer.Normalize(element);
        Assert.IsType<double>(result);
        Assert.Equal(42.5, result);
    }

    [Fact]
    public void Normalize_LargeInteger_RoundTripsExactlyAsLong()
    {
        // 9007199254740993 is one past double's 2^53 exact-integer range — a double path would lose it.
        var element = Parse("9007199254740993");
        var result = JsonValueNormalizer.Normalize(element);
        Assert.IsType<long>(result);
        Assert.Equal(9007199254740993L, result);
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void Normalize_Bool_ReturnsClrBool(string json, bool expected)
    {
        var element = Parse(json);
        Assert.Equal(expected, JsonValueNormalizer.Normalize(element));
    }

    [Fact]
    public void Normalize_Null_ReturnsNull()
    {
        var element = Parse("null");
        Assert.Null(JsonValueNormalizer.Normalize(element));
    }

    [Fact]
    public void Normalize_NestedObject_ReturnsDictionaryOfPlainValues()
    {
        var element = Parse("""{"user":{"tier":"gold","score":7},"active":true}""");
        var result = JsonValueNormalizer.Normalize(element);

        var dict = Assert.IsType<Dictionary<string, object?>>(result);
        Assert.Equal(true, dict["active"]);

        var user = Assert.IsType<Dictionary<string, object?>>(dict["user"]);
        Assert.Equal("gold", user["tier"]);
        Assert.Equal(7L, user["score"]);
    }

    [Fact]
    public void Normalize_Array_ReturnsListOfPlainValues()
    {
        var element = Parse("""[1, "two", 3.5, true, null]""");
        var result = JsonValueNormalizer.Normalize(element);

        var list = Assert.IsType<List<object?>>(result);
        Assert.Equal(5, list.Count);
        Assert.Equal(1L, list[0]);
        Assert.Equal("two", list[1]);
        Assert.Equal(3.5, list[2]);
        Assert.Equal(true, list[3]);
        Assert.Null(list[4]);
    }

    [Fact]
    public void Normalize_ArrayOfObjects_RecursivelyNormalizesEachElement()
    {
        var element = Parse("""[{"ccy":"USD","notional":100},{"ccy":"EUR","notional":50.5}]""");
        var result = JsonValueNormalizer.Normalize(element);

        var list = Assert.IsType<List<object?>>(result);
        var first = Assert.IsType<Dictionary<string, object?>>(list[0]);
        Assert.Equal("USD", first["ccy"]);
        Assert.Equal(100L, first["notional"]);
    }

    [Fact]
    public void Normalize_NonJsonElementValue_PassesThroughUnchanged()
    {
        object? already = "already-clr";
        Assert.Same(already, JsonValueNormalizer.Normalize(already));

        object? number = 5L;
        Assert.Equal(5L, JsonValueNormalizer.Normalize(number));
    }

    [Fact]
    public void NormalizeInPlace_ReplacesOnlyJsonElementValues()
    {
        var row = new Dictionary<string, object?>
        {
            ["symbol"] = "AAPL", // already CLR — untouched
            ["price"] = Parse("101.5"),
            ["qty"] = Parse("10"),
            ["payload"] = Parse("""{"tier":"gold"}"""),
        };

        JsonValueNormalizer.NormalizeInPlace(row);

        Assert.Equal("AAPL", row["symbol"]);
        Assert.Equal(101.5, row["price"]);
        Assert.Equal(10L, row["qty"]);
        var payload = Assert.IsType<Dictionary<string, object?>>(row["payload"]);
        Assert.Equal("gold", payload["tier"]);
    }

    [Fact]
    public void NormalizeInPlace_NoJsonElementValues_IsNoOp()
    {
        var row = new Dictionary<string, object?> { ["a"] = 1L, ["b"] = "x" };
        JsonValueNormalizer.NormalizeInPlace(row);
        Assert.Equal(1L, row["a"]);
        Assert.Equal("x", row["b"]);
    }
}
