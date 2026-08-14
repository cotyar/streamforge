using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Connectors.Formats;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 012: CSV in both directions. Delimiter sniffing on the read side ("and similar files" — TSV,
/// semicolon, pipe), <see cref="CsvFormatter"/> on the write side, and the property that actually
/// matters for a format that has to survive a round trip through a spreadsheet: anything this platform
/// WRITES, this platform READS BACK unchanged.
/// </summary>
public class CsvIoTests
{
    // ---- delimiter sniffing ----

    [Theory]
    [InlineData("a,b\n1,2")]
    [InlineData("a\tb\n1\t2")]
    [InlineData("a;b\n1;2")]
    [InlineData("a|b\n1|2")]
    public void Sniffs_the_delimiter_from_the_header(string text)
    {
        var items = FormatParsers.ParseCsv(text);

        Assert.Single(items);
        Assert.Equal(1, items[0].GetProperty("a").GetInt64());
        Assert.Equal(2, items[0].GetProperty("b").GetInt64());
    }

    [Fact]
    public void A_semicolon_file_with_commas_inside_values_still_splits_on_semicolons()
    {
        // The shape Excel writes in a decimal-comma locale: the comma is a DECIMAL point here, not a
        // separator, which is exactly the case that made "csv means comma" wrong.
        var items = FormatParsers.ParseCsv("symbol;note\nACME;a, b, c");

        Assert.Single(items);
        Assert.Equal("ACME", items[0].GetProperty("symbol").GetString());
        Assert.Equal("a, b, c", items[0].GetProperty("note").GetString());
    }

    [Fact]
    public void Quoted_header_cells_do_not_vote_for_a_delimiter()
    {
        var items = FormatParsers.ParseCsv("\"a;b;c\",second\n1,2");

        Assert.Single(items);
        Assert.Equal(1, items[0].GetProperty("a;b;c").GetInt64());
        Assert.Equal(2, items[0].GetProperty("second").GetInt64());
    }

    [Fact]
    public void A_single_column_file_has_no_delimiter_and_is_read_as_one_column()
    {
        var items = FormatParsers.ParseCsv("symbol\nACME\nWIDGET");

        Assert.Equal(2, items.Count);
        Assert.Equal("ACME", items[0].GetProperty("symbol").GetString());
    }

    [Fact]
    public void An_explicit_delimiter_overrides_the_sniffer()
    {
        var items = FormatParsers.ParseCsv("a;b\n1;2", ',');

        Assert.Single(items);
        Assert.Equal("1;2", items[0].GetProperty("a;b").GetString());
    }

    [Fact]
    public void CsvHeader_returns_the_column_names_in_order()
    {
        Assert.Equal(["symbol", "qty", "_weight"], FormatParsers.CsvHeader("symbol,qty,_weight\nACME,1,1"));
        Assert.Empty(FormatParsers.CsvHeader(""));
    }

    // ---- writing ----

    [Fact]
    public void Row_quotes_only_what_needs_quoting()
    {
        Assert.Equal("a,b\r\n", CsvFormatter.Row(["a", "b"]));
        Assert.Equal("\"a,b\",c\r\n", CsvFormatter.Row(["a,b", "c"]));
        Assert.Equal("\"say \"\"hi\"\"\"\r\n", CsvFormatter.Row(["say \"hi\""]));
        Assert.Equal("\" padded \"\r\n", CsvFormatter.Row([" padded "]));
    }

    [Fact]
    public void Values_render_invariantly()
    {
        Assert.Equal("", CsvFormatter.Render(null));
        Assert.Equal("true", CsvFormatter.Render(true));
        Assert.Equal("1.5", CsvFormatter.Render(1.5d));
        Assert.Equal("42", CsvFormatter.Render(42L));
    }

    [Fact]
    public void What_the_formatter_writes_the_parser_reads_back()
    {
        var rows = new List<IReadOnlyDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["symbol"] = "ACME", ["note"] = "a,b \"quoted\"\nsecond line", ["qty"] = 3L, ["ok"] = true },
            new Dictionary<string, object?> { ["symbol"] = "WIDGET", ["note"] = "", ["qty"] = -1L, ["ok"] = false },
        };

        var parsed = FormatParsers.ParseCsv(CsvFormatter.Table(["symbol", "note", "qty", "ok"], rows));

        Assert.Equal(2, parsed.Count);
        Assert.Equal("ACME", parsed[0].GetProperty("symbol").GetString());
        Assert.Equal("a,b \"quoted\"\nsecond line", parsed[0].GetProperty("note").GetString());
        Assert.Equal(3, parsed[0].GetProperty("qty").GetInt64());
        Assert.True(parsed[0].GetProperty("ok").GetBoolean());
        Assert.Equal(-1, parsed[1].GetProperty("qty").GetInt64());
    }

    [Fact]
    public void A_row_missing_a_column_writes_an_empty_cell_rather_than_shifting_the_line()
    {
        var text = CsvFormatter.Table(
            ["a", "b", "c"],
            [new Dictionary<string, object?> { ["a"] = 1L, ["c"] = 3L }]);

        Assert.Equal("a,b,c\r\n1,,3\r\n", text);
    }

    // ---- the export routes' column selection ----

    [Fact]
    public void Table_export_takes_its_columns_from_the_compiled_output_fields_and_ends_with_weight()
    {
        var def = new TableDefinition
        {
            Name = "positions",
            OutputFields = [new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long)],
        };
        var rows = new List<TableRowDto>
        {
            new() { Row = new Dictionary<string, object?> { ["symbol"] = "ACME", ["qty"] = 5L }, Weight = 1 },
            new() { Row = new Dictionary<string, object?> { ["symbol"] = "WIDGET", ["qty"] = 2L }, Weight = -1 },
        };

        Assert.Equal("symbol,qty,_weight\r\nACME,5,1\r\nWIDGET,2,-1\r\n", CsvExport.Table(def, rows));
    }

    [Fact]
    public void Table_export_falls_back_to_the_rows_own_keys_before_the_table_has_compiled()
    {
        var def = new TableDefinition { Name = "fresh" };
        var rows = new List<TableRowDto>
        {
            new() { Row = new Dictionary<string, object?> { ["a"] = 1L }, Weight = 1 },
        };

        Assert.Equal("a,_weight\r\n1,1\r\n", CsvExport.Table(def, rows));
    }

    [Fact]
    public void Loose_row_export_headers_the_union_of_keys_in_first_seen_order()
    {
        var csv = CsvExport.Rows(
        [
            new Dictionary<string, object?> { ["b"] = 1L },
            new Dictionary<string, object?> { ["b"] = 2L, ["a"] = 3L },
        ]);

        Assert.Equal("b,a\r\n1,\r\n2,3\r\n", csv);
    }
}
