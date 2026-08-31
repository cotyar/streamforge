using System.Text.Json;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors.Formats;
using Xunit;

namespace StreamsForge.Host.Tests;

public class SchemaInferenceTests
{
    private static List<JsonElement> ParseItems(string ndjson) => FormatParsers.ParseNdjson(ndjson);

    private static FieldDef Field(List<FieldDef> fields, string name) =>
        Assert.Single(fields, f => f.Name == name);

    [Fact]
    public void All_integral_numbers_infer_long()
    {
        var items = ParseItems("{\"n\":1}\n{\"n\":2}\n{\"n\":3}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.Long, Field(fields, "n").Type);
    }

    [Fact]
    public void Any_fractional_number_infers_double()
    {
        var items = ParseItems("{\"n\":1}\n{\"n\":2.5}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.Double, Field(fields, "n").Type);
    }

    [Fact]
    public void Scientific_notation_infers_double()
    {
        var items = ParseItems("{\"n\":1e3}\n{\"n\":2}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.Double, Field(fields, "n").Type);
    }

    [Fact]
    public void Bool_values_infer_bool()
    {
        var items = ParseItems("{\"b\":true}\n{\"b\":false}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.Bool, Field(fields, "b").Type);
    }

    [Fact]
    public void Strings_that_all_parse_as_iso8601_infer_timestamp()
    {
        var items = ParseItems(
            "{\"ts\":\"2020-01-01T00:00:00Z\"}\n{\"ts\":\"2021-06-15T10:30:00.123Z\"}\n{\"ts\":\"2022-12-31\"}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.Timestamp, Field(fields, "ts").Type);
    }

    [Fact]
    public void Strings_that_dont_all_parse_as_iso8601_infer_string()
    {
        var items = ParseItems("{\"ts\":\"2020-01-01T00:00:00Z\"}\n{\"ts\":\"not a date\"}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.String, Field(fields, "ts").Type);
    }

    [Fact]
    public void Plain_strings_infer_string()
    {
        var items = ParseItems("{\"s\":\"hello\"}\n{\"s\":\"world\"}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.String, Field(fields, "s").Type);
    }

    [Fact]
    public void Nested_object_infers_json_with_children()
    {
        var items = ParseItems("""{"user":{"id":1,"name":"a"}}""" + "\n" + """{"user":{"id":2,"name":"b"}}""");
        var fields = SchemaInference.Infer(items);

        var user = Field(fields, "user");
        Assert.Equal(FieldType.Json, user.Type);
        Assert.NotNull(user.Children);
        Assert.Equal(FieldType.Long, Field(user.Children!, "id").Type);
        Assert.Equal(FieldType.String, Field(user.Children!, "name").Type);
    }

    [Fact]
    public void Array_of_scalars_infers_is_array_with_element_type()
    {
        var items = ParseItems("""{"tags":[1,2,3]}""" + "\n" + """{"tags":[4,5]}""");
        var fields = SchemaInference.Infer(items);

        var tags = Field(fields, "tags");
        Assert.True(tags.IsArray);
        Assert.Equal(FieldType.Long, tags.Type);
    }

    [Fact]
    public void Array_of_objects_infers_is_array_of_json_with_children()
    {
        var items = ParseItems("""{"trades":[{"id":1,"px":1.5},{"id":2,"px":2.5}]}""");
        var fields = SchemaInference.Infer(items);

        var trades = Field(fields, "trades");
        Assert.True(trades.IsArray);
        Assert.Equal(FieldType.Json, trades.Type);
        Assert.NotNull(trades.Children);
        Assert.Equal(FieldType.Long, Field(trades.Children!, "id").Type);
        Assert.Equal(FieldType.Double, Field(trades.Children!, "px").Type);
    }

    [Fact]
    public void Mixed_kinds_infer_string()
    {
        var items = ParseItems("{\"m\":1}\n{\"m\":\"text\"}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.String, Field(fields, "m").Type);
    }

    [Fact]
    public void All_null_values_infer_string()
    {
        var items = ParseItems("{\"n\":null}\n{\"n\":null}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.String, Field(fields, "n").Type);
    }

    [Fact]
    public void Key_missing_from_some_items_is_still_typed_from_the_items_that_have_it()
    {
        var items = ParseItems("{\"a\":1}\n{\"b\":2}\n{\"a\":3}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.Long, Field(fields, "a").Type);
        Assert.Equal(FieldType.Long, Field(fields, "b").Type);
    }

    [Fact]
    public void Null_values_are_ignored_when_determining_type_alongside_real_values()
    {
        var items = ParseItems("{\"a\":1}\n{\"a\":null}\n{\"a\":2}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(FieldType.Long, Field(fields, "a").Type);
    }

    [Fact]
    public void Field_order_follows_first_seen_order_across_the_sample()
    {
        var items = ParseItems("{\"z\":1,\"a\":2}\n{\"m\":3}");
        var fields = SchemaInference.Infer(items);

        Assert.Equal(["z", "a", "m"], fields.Select(f => f.Name));
    }

    [Fact]
    public void Sample_is_capped_at_first_100_items()
    {
        var lines = Enumerable.Range(0, 150).Select(i => i < 100 ? "{\"n\":1}" : "{\"n\":1.5}");
        var items = ParseItems(string.Join('\n', lines));
        var fields = SchemaInference.Infer(items);

        // Only the first 100 items (all integral) are sampled, so despite fractional values
        // appearing later in the feed, the field infers Long.
        Assert.Equal(FieldType.Long, Field(fields, "n").Type);
    }

    [Fact]
    public void Non_object_items_contribute_no_fields()
    {
        var items = FormatParsers.ParseJsonArray("[1, 2, 3]");
        var fields = SchemaInference.Infer(items);

        Assert.Empty(fields);
    }
}
