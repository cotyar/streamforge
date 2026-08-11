using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>Plan 008 W4: the shared per-row client-push ingest pipeline — coercion of declared
/// fields, unknown-field handling both ways, "_ts" resolution, and "_source" always being
/// overwritten (the security property IngestModels.cs calls out explicitly).</summary>
public class IngressRowAcceptanceTests
{
    private static readonly List<FieldDef> Fields =
    [
        new("symbol", FieldType.String),
        new("qty", FieldType.Long),
        new("tags", FieldType.String, IsArray: true),
    ];

    [Fact]
    public void Accepted_row_contains_declared_fields_plus_ts_and_source()
    {
        var raw = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = 10L };

        var result = IngressRowAcceptance.Accept(Fields, "my-source", rejectUnknownFields: false, raw, arrivalMs: 1000);

        Assert.True(result.Accepted);
        Assert.Equal("AAPL", result.Row!["symbol"]);
        Assert.Equal(10L, result.Row["qty"]);
        Assert.Equal(1000L, result.Row["_ts"]);
        Assert.Equal("my-source", result.Row["_source"]);
    }

    [Fact]
    public void Absent_declared_field_is_omitted_not_defaulted()
    {
        var raw = new Dictionary<string, object?> { ["symbol"] = "AAPL" };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        Assert.False(result.Row!.ContainsKey("qty"));
    }

    [Fact]
    public void Null_declared_field_is_omitted_like_an_absent_one()
    {
        var raw = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = null };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        Assert.False(result.Row!.ContainsKey("qty"));
    }

    [Fact]
    public void Unknown_field_is_dropped_and_counted_by_default()
    {
        var raw = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["bogus"] = "x" };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        Assert.Equal(1, result.UnknownFieldsDropped);
        Assert.False(result.Row!.ContainsKey("bogus"));
    }

    [Fact]
    public void Unknown_field_fails_the_whole_row_when_RejectUnknownFields()
    {
        var raw = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["bogus"] = "x" };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: true, raw, arrivalMs: 0);

        Assert.False(result.Accepted);
        Assert.NotNull(result.Error);
    }

    [Fact]
    public void Field_that_fails_coercion_fails_the_whole_row_not_just_the_field()
    {
        var raw = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = "not-a-number" };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.False(result.Accepted);
        Assert.Contains("qty", result.Error);
        Assert.Null(result.Row);
    }

    [Fact]
    public void Ts_present_as_epoch_ms_number_is_honoured()
    {
        var raw = new Dictionary<string, object?> { ["_ts"] = 555L };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: false, raw, arrivalMs: 999);

        Assert.Equal(555L, result.Row!["_ts"]);
    }

    [Fact]
    public void Ts_present_as_iso8601_string_is_honoured()
    {
        var raw = new Dictionary<string, object?> { ["_ts"] = "2020-01-01T00:00:00Z" };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: false, raw, arrivalMs: 1);

        var expected = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds();
        Assert.Equal(expected, result.Row!["_ts"]);
    }

    [Fact]
    public void Ts_absent_is_stamped_with_arrival_time()
    {
        var raw = new Dictionary<string, object?> { ["symbol"] = "AAPL" };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: false, raw, arrivalMs: 42);

        Assert.Equal(42L, result.Row!["_ts"]);
    }

    [Fact]
    public void Source_is_always_overwritten_even_when_the_client_sets_one()
    {
        var raw = new Dictionary<string, object?> { ["_source"] = "someone-elses-source" };

        var result = IngressRowAcceptance.Accept(Fields, "the-real-source", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.Equal("the-real-source", result.Row!["_source"]);
    }

    [Fact]
    public void Client_supplied_ts_and_source_keys_are_never_treated_as_unknown()
    {
        var raw = new Dictionary<string, object?> { ["_source"] = "x", ["_ts"] = 1L };

        // Would fail (unknown field) if "_source"/"_ts" were misclassified as undeclared fields.
        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: true, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        Assert.Equal(0, result.UnknownFieldsDropped);
    }

    [Fact]
    public void Array_field_coerces_every_element()
    {
        var raw = new Dictionary<string, object?> { ["tags"] = new List<object?> { "a", 1L } };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.True(result.Accepted);
        var tags = Assert.IsType<List<object?>>(result.Row!["tags"]);
        Assert.Equal(["a", "1"], tags);
    }

    [Fact]
    public void Array_field_skips_null_elements()
    {
        var raw = new Dictionary<string, object?> { ["tags"] = new List<object?> { "a", null, "b" } };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: false, raw, arrivalMs: 0);

        var tags = Assert.IsType<List<object?>>(result.Row!["tags"]);
        Assert.Equal(["a", "b"], tags);
    }

    [Fact]
    public void Array_field_given_a_non_list_value_fails_the_row()
    {
        var raw = new Dictionary<string, object?> { ["tags"] = "not-a-list" };

        var result = IngressRowAcceptance.Accept(Fields, "s", rejectUnknownFields: false, raw, arrivalMs: 0);

        Assert.False(result.Accepted);
    }

    [Fact]
    public void AcceptBatch_partitions_valid_and_invalid_rows_and_reports_row_indexed_errors()
    {
        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["symbol"] = "AAPL" },
            new() { ["qty"] = "bad" },
            new() { ["symbol"] = "MSFT" },
        };

        var result = IngressRowAcceptance.AcceptBatch(Fields, "s", rejectUnknownFields: false, rows, arrivalMs: 0);

        Assert.Equal(2, result.Accepted.Count);
        Assert.Single(result.RowErrors);
        Assert.Contains("row 1", result.RowErrors[0]);
    }

    [Fact]
    public void AcceptBatch_sums_unknown_field_drops_across_the_batch()
    {
        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["symbol"] = "AAPL", ["bogus1"] = "x" },
            new() { ["symbol"] = "MSFT", ["bogus2"] = "y", ["bogus3"] = "z" },
        };

        var result = IngressRowAcceptance.AcceptBatch(Fields, "s", rejectUnknownFields: false, rows, arrivalMs: 0);

        Assert.Equal(2, result.Accepted.Count);
        Assert.Equal(3, result.UnknownFieldsDropped);
    }
}
