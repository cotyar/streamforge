using System.Text.Json;
using StreamsForge.AppCore.Connectors.Formats;
using Xunit;

namespace StreamsForge.Host.Tests;

public class FormatParsersTests
{
    // ---- NDJSON ----

    [Fact]
    public void Ndjson_parses_one_value_per_line()
    {
        var items = FormatParsers.ParseNdjson("{\"a\":1}\n{\"a\":2}\n{\"a\":3}");

        Assert.Equal(3, items.Count);
        Assert.Equal(1, items[0].GetProperty("a").GetInt32());
        Assert.Equal(2, items[1].GetProperty("a").GetInt32());
        Assert.Equal(3, items[2].GetProperty("a").GetInt32());
    }

    [Fact]
    public void Ndjson_tolerates_blank_lines()
    {
        var items = FormatParsers.ParseNdjson("{\"a\":1}\n\n   \n{\"a\":2}\n");

        Assert.Equal(2, items.Count);
    }

    [Fact]
    public void Ndjson_malformed_line_throws_with_line_number()
    {
        var ex = Assert.Throws<FormatException>(() => FormatParsers.ParseNdjson("{\"a\":1}\nnot json\n{\"a\":3}"));

        Assert.Contains("line 2", ex.Message);
    }

    // ---- JSON array ----

    [Fact]
    public void Json_array_root_yields_each_element()
    {
        var items = FormatParsers.ParseJsonArray("[{\"a\":1},{\"a\":2}]");

        Assert.Equal(2, items.Count);
    }

    [Fact]
    public void Json_object_root_yields_a_single_item()
    {
        var items = FormatParsers.ParseJsonArray("{\"a\":1}");

        Assert.Single(items);
        Assert.Equal(1, items[0].GetProperty("a").GetInt32());
    }

    [Fact]
    public void Json_array_malformed_throws_format_exception()
    {
        Assert.Throws<FormatException>(() => FormatParsers.ParseJsonArray("[1, 2,"));
    }

    // ---- CSV ----

    [Fact]
    public void Csv_basic_header_and_rows()
    {
        var items = FormatParsers.ParseCsv("a,b\n1,2\n3,4\n");

        Assert.Equal(2, items.Count);
        Assert.Equal(1L, items[0].GetProperty("a").GetInt64());
        Assert.Equal(2L, items[0].GetProperty("b").GetInt64());
        Assert.Equal(3L, items[1].GetProperty("a").GetInt64());
        Assert.Equal(4L, items[1].GetProperty("b").GetInt64());
    }

    [Fact]
    public void Csv_quoted_field_with_embedded_comma()
    {
        var items = FormatParsers.ParseCsv("name,note\nAlice,\"hello, world\"\n");

        Assert.Single(items);
        Assert.Equal("hello, world", items[0].GetProperty("note").GetString());
    }

    [Fact]
    public void Csv_quoted_field_with_embedded_newline()
    {
        var items = FormatParsers.ParseCsv("name,note\nAlice,\"line1\nline2\"\n");

        Assert.Single(items);
        Assert.Equal("line1\nline2", items[0].GetProperty("note").GetString());
    }

    [Fact]
    public void Csv_quoted_field_with_double_quote_escape()
    {
        var items = FormatParsers.ParseCsv("name,quote\nAlice,\"she said \"\"hi\"\"\"\n");

        Assert.Single(items);
        Assert.Equal("she said \"hi\"", items[0].GetProperty("quote").GetString());
    }

    [Fact]
    public void Csv_type_sniffing_long_double_bool_string()
    {
        var items = FormatParsers.ParseCsv("i,d,b,s\n42,3.14,true,hello\n");

        var row = items[0];
        Assert.Equal(JsonValueKind.Number, row.GetProperty("i").ValueKind);
        Assert.Equal(42L, row.GetProperty("i").GetInt64());
        Assert.Equal(JsonValueKind.Number, row.GetProperty("d").ValueKind);
        Assert.Equal(3.14, row.GetProperty("d").GetDouble());
        Assert.Equal(JsonValueKind.True, row.GetProperty("b").ValueKind);
        Assert.Equal(JsonValueKind.String, row.GetProperty("s").ValueKind);
        Assert.Equal("hello", row.GetProperty("s").GetString());
    }

    [Fact]
    public void Csv_scientific_notation_sniffs_as_double_not_long()
    {
        var items = FormatParsers.ParseCsv("x\n1e3\n");

        Assert.Equal(JsonValueKind.Number, items[0].GetProperty("x").ValueKind);
        Assert.Equal(1000.0, items[0].GetProperty("x").GetDouble());
    }

    [Fact]
    public void Csv_value_that_does_not_parse_exactly_stays_string()
    {
        // A quoted field keeps the embedded comma as literal content (not a column separator);
        // "1,000" isn't a number that parses exactly (thousands separators aren't accepted), so it
        // stays a string.
        var items = FormatParsers.ParseCsv("x\n\"1,000\"\n");

        Assert.Equal(JsonValueKind.String, items[0].GetProperty("x").ValueKind);
        Assert.Equal("1,000", items[0].GetProperty("x").GetString());
    }

    [Fact]
    public void Csv_short_row_pads_missing_columns_with_empty_string()
    {
        var items = FormatParsers.ParseCsv("a,b,c\n1\n");

        Assert.Equal("", items[0].GetProperty("b").GetString());
        Assert.Equal("", items[0].GetProperty("c").GetString());
    }

    [Fact]
    public void Csv_long_row_drops_extra_columns()
    {
        var items = FormatParsers.ParseCsv("a,b\n1,2,3,4\n");

        var props = items[0].EnumerateObject().Select(p => p.Name).ToList();
        Assert.Equal(["a", "b"], props);
    }

    [Fact]
    public void Csv_duplicate_header_keeps_last_occurrence_value()
    {
        var items = FormatParsers.ParseCsv("a,a\n1,2\n");

        var row = items[0];
        var props = row.EnumerateObject().ToList();
        Assert.Single(props); // only one "a" property survives
        Assert.Equal(2L, row.GetProperty("a").GetInt64()); // the LAST column's value wins
    }

    [Fact]
    public void Csv_only_header_row_yields_no_items()
    {
        var items = FormatParsers.ParseCsv("a,b\n");

        Assert.Empty(items);
    }

    [Fact]
    public void Csv_empty_text_yields_no_items()
    {
        var items = FormatParsers.ParseCsv("");

        Assert.Empty(items);
    }

    [Fact]
    public void Csv_unterminated_quote_throws_with_line_number()
    {
        var ex = Assert.Throws<FormatException>(() => FormatParsers.ParseCsv("a\n\"unterminated\n"));

        Assert.Contains("line", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Csv_stray_quote_inside_unquoted_field_throws_with_line_number()
    {
        var ex = Assert.Throws<FormatException>(() => FormatParsers.ParseCsv("a,b\n1,ab\"cd\n"));

        Assert.Contains("line 2", ex.Message);
    }

    [Fact]
    public void Csv_crlf_line_endings_are_supported()
    {
        var items = FormatParsers.ParseCsv("a,b\r\n1,2\r\n3,4\r\n");

        Assert.Equal(2, items.Count);
        Assert.Equal(1L, items[0].GetProperty("a").GetInt64());
        Assert.Equal(3L, items[1].GetProperty("a").GetInt64());
    }
}
