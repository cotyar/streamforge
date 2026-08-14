using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Connectors;
using StreamForge.AppCore.Connectors.Polling;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 012: a url-kind source may declare its response body's format, so an endpoint that serves
/// text/csv (or NDJSON) is readable without a file in between — the gap that made "sources should work
/// with CSV" only two-thirds true, since file/folder/nats already had a Format and url did not.
/// </summary>
public class UrlSourceFormatTests
{
    private static SourceDefinition UrlSource(string? format) => new()
    {
        Name = "s1",
        Kind = SourceKinds.Url,
        Fields = [new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long)],
        Connector = new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://example.invalid/data.csv", Format = format! },
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                Fields =
                [
                    new FieldMapEntry { Field = new FieldDef("symbol", FieldType.String) },
                    new FieldMapEntry { Field = new FieldDef("qty", FieldType.Long) },
                ],
            },
        },
    };

    [Fact]
    public void A_csv_body_becomes_rows()
    {
        var result = ConnectorPollCycle.ExecuteUrl(UrlSource(FileFormats.Csv), "symbol,qty\nACME,5\nWIDGET,2", new DedupTracker(), 1000);

        Assert.Null(result.Error);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("ACME", result.Rows[0]["symbol"]);
        Assert.Equal(5L, result.Rows[0]["qty"]);
        Assert.Equal("s1", result.Rows[0]["_source"]);
    }

    [Fact]
    public void A_tab_separated_body_works_through_the_same_csv_format()
    {
        var result = ConnectorPollCycle.ExecuteUrl(UrlSource(FileFormats.Csv), "symbol\tqty\nACME\t5", new DedupTracker(), 1000);

        Assert.Null(result.Error);
        Assert.Equal("ACME", Assert.Single(result.Rows)["symbol"]);
    }

    [Fact]
    public void An_ndjson_body_becomes_rows()
    {
        var body = "{\"symbol\":\"ACME\",\"qty\":5}\n{\"symbol\":\"WIDGET\",\"qty\":2}";

        var result = ConnectorPollCycle.ExecuteUrl(UrlSource(FileFormats.Ndjson), body, new DedupTracker(), 1000);

        Assert.Null(result.Error);
        Assert.Equal(2, result.Rows.Count);
    }

    [Fact]
    public void A_malformed_csv_body_is_an_error_not_an_exception()
    {
        var result = ConnectorPollCycle.ExecuteUrl(UrlSource(FileFormats.Csv), "symbol,qty\nAC\"ME,5", new DedupTracker(), 1000);

        Assert.NotNull(result.Error);
        Assert.Empty(result.Rows);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(FileFormats.JsonArray)]
    public void An_unset_or_json_format_keeps_the_pre_012_json_path(string? format)
    {
        // The whole compatibility claim: a definition stored before this field existed reads a JSON body
        // exactly as it always did.
        var result = ConnectorPollCycle.ExecuteUrl(UrlSource(format), """[{"symbol":"ACME","qty":5}]""", new DedupTracker(), 1000);

        Assert.Null(result.Error);
        Assert.Equal("ACME", Assert.Single(result.Rows)["symbol"]);
    }

    // ---- validation ----

    [Fact]
    public void Validation_rejects_an_unknown_url_format_and_accepts_the_known_ones()
    {
        Assert.Contains(
            SourceValidation.Validate(WithFormat("xlsx")),
            e => e.Contains("connector.url.format"));

        foreach (var format in new[] { FileFormats.Csv, FileFormats.Ndjson, FileFormats.JsonArray, "" })
        {
            Assert.DoesNotContain(
                SourceValidation.Validate(WithFormat(format)),
                e => e.Contains("connector.url.format"));
        }

        static SourceDefinition WithFormat(string format)
        {
            var def = UrlSource(format);
            def.Connector!.Schedule = new ScheduleSpec { IntervalMs = 5000 };
            return def;
        }
    }
}
