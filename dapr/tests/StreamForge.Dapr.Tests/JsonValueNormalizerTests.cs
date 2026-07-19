using System.Text.Json;
using StreamForge.AppCore.Json;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W4: round-trip coverage for the normalizer every Dapr pub/sub ingress
/// will run payloads through starting W5 (decision D-D). Written now because the class already exists in
/// shared/StreamForge.AppCore and W4 is the first Dapr-side test project to land.
///
/// <para><b>Known bug found while writing these tests (NOT fixed here — out of this wave's ownership,
/// shared/** is owned by earlier waves; flagged separately for a follow-up fix):</b>
/// <c>JsonValueNormalizer.NormalizeNumber</c> is implemented as
/// <c>element.TryGetInt64(out var l) ? l : element.GetDouble()</c>. Because C#'s conditional operator
/// picks a single common type for both branches, and <c>long</c> has an implicit conversion to
/// <c>double</c> but not vice versa, the compiler unifies BOTH branches to <c>double</c> — so this
/// method always returns a boxed <see cref="double"/>, never a <see cref="long"/>, even though the XML
/// doc comment (and the class's whole reason for existing — giving JSON-ingested integers real integer
/// semantics downstream in the Engine) says integers should stay <see cref="long"/>. Verified directly:
/// <c>object o = true ? 42L : 3.5; // o.GetType() == typeof(double)</c>. The tests below assert the
/// CURRENT (buggy) behavior and are annotated at each affected assertion — update them alongside the
/// one-line fix (something like assigning each branch to an <c>object</c> local first, or an explicit
/// <c>(object)l</c> cast) when that lands.</para>
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
    public void Normalize_IntegerNumber_CurrentlyReturnsDouble_KnownBug()
    {
        // Intended (per the class's own doc comment): a boxed `long` 42. Actual: a boxed `double` 42.0 —
        // see this class's doc comment above for the root cause (conditional-operator type unification).
        var element = Parse("42");
        var result = JsonValueNormalizer.Normalize(element);
        Assert.IsType<double>(result);
        Assert.Equal(42.0, result);
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
    public void Normalize_LargeIntegerLookingNumber_CurrentlyLosesPrecisionAsDouble_KnownBug()
    {
        // Intended: exact long round-trip (9007199254740993 is one past double's 2^53 exact-integer
        // range). Actual: silently loses precision because NormalizeNumber always returns double — see
        // this class's doc comment above.
        var element = Parse("9007199254740993");
        var result = JsonValueNormalizer.Normalize(element);
        Assert.IsType<double>(result);
        // Runtime double->long conversion (not a double literal, which would fold to the same rounded
        // constant at compile time and hide the mismatch) shows the value is no longer the original
        // integer: precision was already lost at the JSON->double step.
        Assert.NotEqual(9007199254740993L, (long)(double)result!);
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
        Assert.Equal(7.0, user["score"]); // would be 7L if not for the known bug documented above
    }

    [Fact]
    public void Normalize_Array_ReturnsListOfPlainValues()
    {
        var element = Parse("""[1, "two", 3.5, true, null]""");
        var result = JsonValueNormalizer.Normalize(element);

        var list = Assert.IsType<List<object?>>(result);
        Assert.Equal(5, list.Count);
        Assert.Equal(1.0, list[0]); // would be 1L if not for the known bug documented above
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
        Assert.Equal(100.0, first["notional"]); // would be 100L if not for the known bug documented above
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
        Assert.Equal(10.0, row["qty"]); // would be 10L if not for the known bug documented above
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
