using System.Text.Json;
using StreamForge.AppCore.Connectors.Mapping;
using Xunit;

namespace StreamForge.Host.Tests;

public class JsonPathLiteTests
{
    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    // ---- supported forms ----

    [Fact]
    public void Dollar_alone_selects_the_root()
    {
        var root = Parse("""{"a":1}""");
        var result = JsonPathLite.Select(root, "$");
        Assert.Single(result);
        Assert.Equal(JsonValueKind.Object, result[0].ValueKind);
    }

    [Fact]
    public void Dotted_property_path_with_leading_dollar()
    {
        var root = Parse("""{"data":{"trades":[{"id":1}]}}""");
        var result = JsonPathLite.Select(root, "$.data.trades[0].id");
        Assert.Single(result);
        Assert.Equal(1, result[0].GetInt32());
    }

    [Fact]
    public void Relative_dotted_property_path_without_dollar()
    {
        var root = Parse("""{"user":{"tier":"gold"}}""");
        var result = JsonPathLite.Select(root, "user.tier");
        Assert.Single(result);
        Assert.Equal("gold", result[0].GetString());
    }

    [Fact]
    public void Bare_single_identifier_path()
    {
        var root = Parse("""{"price": 42.5}""");
        var result = JsonPathLite.Select(root, "price");
        Assert.Single(result);
        Assert.Equal(42.5, result[0].GetDouble());
    }

    [Fact]
    public void Quoted_bracket_property_single_quotes()
    {
        var root = Parse("""{"weird name": "x"}""");
        var result = JsonPathLite.Select(root, "$['weird name']");
        Assert.Single(result);
        Assert.Equal("x", result[0].GetString());
    }

    [Fact]
    public void Quoted_bracket_property_double_quotes()
    {
        var root = Parse("""{"weird name": "x"}""");
        var result = JsonPathLite.Select(root, "$[\"weird name\"]");
        Assert.Single(result);
        Assert.Equal("x", result[0].GetString());
    }

    [Fact]
    public void Numeric_index_selects_one_array_element()
    {
        var root = Parse("""[10,20,30]""");
        var result = JsonPathLite.Select(root, "$[1]");
        Assert.Single(result);
        Assert.Equal(20, result[0].GetInt32());
    }

    [Fact]
    public void Wildcard_selects_every_array_element()
    {
        var root = Parse("""{"data":{"trades":[{"id":1},{"id":2},{"id":3}]}}""");
        var result = JsonPathLite.Select(root, "$.data.trades[*]");
        Assert.Equal(3, result.Count);
    }

    [Fact]
    public void Wildcard_then_property_selects_property_of_each_element()
    {
        var root = Parse("""{"items":[{"id":1},{"id":2}]}""");
        var result = JsonPathLite.Select(root, "$.items[*].id");
        Assert.Equal([1, 2], result.Select(e => e.GetInt32()));
    }

    // ---- missing segments = empty result, NOT an error ----

    [Fact]
    public void Missing_property_yields_no_match()
    {
        var root = Parse("""{"a":1}""");
        var result = JsonPathLite.Select(root, "$.b");
        Assert.Empty(result);
    }

    [Fact]
    public void Index_out_of_range_yields_no_match()
    {
        var root = Parse("""[1,2]""");
        var result = JsonPathLite.Select(root, "$[5]");
        Assert.Empty(result);
    }

    [Fact]
    public void Property_access_on_non_object_yields_no_match()
    {
        var root = Parse("""{"a": 5}""");
        var result = JsonPathLite.Select(root, "$.a.b");
        Assert.Empty(result);
    }

    [Fact]
    public void Wildcard_on_non_array_yields_no_match()
    {
        var root = Parse("""{"a": 5}""");
        var result = JsonPathLite.Select(root, "$.a[*]");
        Assert.Empty(result);
    }

    // ---- rejected forms: FormatException naming the offending token ----

    [Fact]
    public void Recursive_descent_is_rejected()
    {
        var ex = Assert.Throws<FormatException>(() => JsonPathLite.Select(Parse("{}"), "$..name"));
        Assert.Contains("..", ex.Message);
    }

    [Fact]
    public void Wildcard_property_key_is_rejected()
    {
        var ex = Assert.Throws<FormatException>(() => JsonPathLite.Select(Parse("{}"), "$.*"));
        Assert.Contains(".*", ex.Message);
    }

    [Fact]
    public void Filter_expression_is_rejected()
    {
        var ex = Assert.Throws<FormatException>(() => JsonPathLite.Select(Parse("{}"), "$.items[?(@.price>10)]"));
        Assert.Contains("?(@.price>10)", ex.Message);
    }

    [Fact]
    public void Slice_expression_is_rejected()
    {
        var ex = Assert.Throws<FormatException>(() => JsonPathLite.Select(Parse("{}"), "$.items[1:3]"));
        Assert.Contains("1:3", ex.Message);
    }

    [Fact]
    public void Negative_index_is_rejected()
    {
        var ex = Assert.Throws<FormatException>(() => JsonPathLite.Select(Parse("{}"), "$.items[-1]"));
        Assert.Contains("-1", ex.Message);
    }

    [Fact]
    public void Unterminated_bracket_is_rejected()
    {
        var ex = Assert.Throws<FormatException>(() => JsonPathLite.Select(Parse("{}"), "$.items[0"));
        Assert.Contains("unterminated", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Empty_property_name_after_dot_is_rejected()
    {
        var ex = Assert.Throws<FormatException>(() => JsonPathLite.Select(Parse("{}"), "$."));
        Assert.Contains("empty property name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Unexpected_token_after_a_bracket_segment_is_rejected()
    {
        // After "a" (bare identifier) and "[0]" (index), a bare 'x' with no leading '.' or '['
        // is not a valid way to start a further segment.
        var ex = Assert.Throws<FormatException>(() => JsonPathLite.Select(Parse("{}"), "a[0]x"));
        Assert.Contains("'x'", ex.Message);
    }
}
