using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Mapping;
using Xunit;

namespace StreamForge.Host.Tests;

public class MappingLoaderTests
{
    private const string JsonDoc = """
        {
          "itemsPath": "$.data.trades[*]",
          "dedupKeyField": "id",
          "timestampField": "ts",
          "fields": [
            { "field": { "name": "id", "type": "String" } },
            { "sourcePath": "px", "field": { "name": "price", "type": "Double" } },
            { "field": { "name": "ts", "type": "Timestamp" } }
          ]
        }
        """;

    private const string YamlDoc = """
        itemsPath: "$.data.trades[*]"
        dedupKeyField: id
        timestampField: ts
        fields:
          - field:
              name: id
              type: String
          - sourcePath: px
            field:
              name: price
              type: Double
          - field:
              name: ts
              type: Timestamp
        """;

    [Fact]
    public void Json_document_parses_cleanly()
    {
        var (spec, diagnostics) = MappingLoader.Parse(JsonDoc);

        Assert.NotNull(spec);
        Assert.Empty(diagnostics);
        AssertWellFormedSpec(spec!);
    }

    [Fact]
    public void Yaml_document_parses_to_an_equivalent_spec_as_the_json_document()
    {
        var (jsonSpec, jsonDiagnostics) = MappingLoader.Parse(JsonDoc);
        var (yamlSpec, yamlDiagnostics) = MappingLoader.Parse(YamlDoc);

        Assert.Empty(jsonDiagnostics);
        Assert.Empty(yamlDiagnostics);
        Assert.NotNull(jsonSpec);
        Assert.NotNull(yamlSpec);

        Assert.Equal(jsonSpec!.ItemsPath, yamlSpec!.ItemsPath);
        Assert.Equal(jsonSpec.DedupKeyField, yamlSpec.DedupKeyField);
        Assert.Equal(jsonSpec.TimestampField, yamlSpec.TimestampField);
        Assert.Equal(jsonSpec.Fields.Count, yamlSpec.Fields.Count);
        for (var i = 0; i < jsonSpec.Fields.Count; i++)
        {
            Assert.Equal(jsonSpec.Fields[i].SourcePath, yamlSpec.Fields[i].SourcePath);
            Assert.Equal(jsonSpec.Fields[i].Field.Name, yamlSpec.Fields[i].Field.Name);
            Assert.Equal(jsonSpec.Fields[i].Field.Type, yamlSpec.Fields[i].Field.Type);
        }

        AssertWellFormedSpec(yamlSpec);
    }

    private static void AssertWellFormedSpec(MappingSpec spec)
    {
        Assert.Equal("$.data.trades[*]", spec.ItemsPath);
        Assert.Equal("id", spec.DedupKeyField);
        Assert.Equal("ts", spec.TimestampField);
        Assert.Equal(3, spec.Fields.Count);

        Assert.Null(spec.Fields[0].SourcePath);
        Assert.Equal("id", spec.Fields[0].Field.Name);
        Assert.Equal(FieldType.String, spec.Fields[0].Field.Type);

        Assert.Equal("px", spec.Fields[1].SourcePath);
        Assert.Equal("price", spec.Fields[1].Field.Name);
        Assert.Equal(FieldType.Double, spec.Fields[1].Field.Type);

        Assert.Equal("ts", spec.Fields[2].Field.Name);
        Assert.Equal(FieldType.Timestamp, spec.Fields[2].Field.Type);
    }

    [Fact]
    public void Unparseable_document_yields_null_spec_and_a_diagnostic()
    {
        var (spec, diagnostics) = MappingLoader.Parse("[1, 2,");

        Assert.Null(spec);
        Assert.Contains(diagnostics, d => d.Contains("neither valid JSON nor valid YAML"));
    }

    [Fact]
    public void Non_object_root_yields_null_spec_and_a_diagnostic()
    {
        var (spec, diagnostics) = MappingLoader.Parse("[1,2,3]");

        Assert.Null(spec);
        Assert.Contains(diagnostics, d => d.Contains("root must be an object"));
    }

    [Fact]
    public void Unknown_top_level_key_is_a_non_fatal_diagnostic()
    {
        const string doc = """
            {
              "fields": [ { "field": { "name": "a", "type": "String" } } ],
              "bogus": true
            }
            """;

        var (spec, diagnostics) = MappingLoader.Parse(doc);

        Assert.NotNull(spec);
        Assert.Contains(diagnostics, d => d.Contains("unknown top-level key 'bogus'"));
    }

    [Fact]
    public void Empty_field_name_is_a_diagnostic()
    {
        const string doc = """
            { "fields": [ { "field": { "type": "String" } } ] }
            """;

        var (spec, diagnostics) = MappingLoader.Parse(doc);

        Assert.NotNull(spec);
        Assert.Contains(diagnostics, d => d.Contains("has an empty field name"));
    }

    [Fact]
    public void Duplicate_field_names_are_a_diagnostic()
    {
        const string doc = """
            {
              "fields": [
                { "field": { "name": "id", "type": "String" } },
                { "field": { "name": "id", "type": "Long" } }
              ]
            }
            """;

        var (spec, diagnostics) = MappingLoader.Parse(doc);

        Assert.NotNull(spec);
        Assert.Contains(diagnostics, d => d.Contains("duplicate field name 'id'"));
    }

    [Fact]
    public void Invalid_items_path_syntax_is_a_diagnostic()
    {
        const string doc = """
            {
              "itemsPath": "$.items[?(@.x)]",
              "fields": [ { "field": { "name": "a", "type": "String" } } ]
            }
            """;

        var (spec, diagnostics) = MappingLoader.Parse(doc);

        Assert.NotNull(spec);
        Assert.Contains(diagnostics, d => d.Contains("itemsPath") && d.Contains("invalid path"));
    }

    [Fact]
    public void Dedup_key_field_not_among_fields_is_a_diagnostic()
    {
        const string doc = """
            {
              "dedupKeyField": "zzz",
              "fields": [ { "field": { "name": "a", "type": "String" } } ]
            }
            """;

        var (spec, diagnostics) = MappingLoader.Parse(doc);

        Assert.NotNull(spec);
        Assert.Contains(diagnostics, d => d.Contains("dedupKeyField 'zzz' is not among the mapped fields"));
    }

    [Fact]
    public void Timestamp_field_not_among_fields_is_a_diagnostic()
    {
        const string doc = """
            {
              "timestampField": "zzz",
              "fields": [ { "field": { "name": "a", "type": "String" } } ]
            }
            """;

        var (spec, diagnostics) = MappingLoader.Parse(doc);

        Assert.NotNull(spec);
        Assert.Contains(diagnostics, d => d.Contains("timestampField 'zzz' is not among the mapped fields"));
    }

    [Fact]
    public void Empty_document_is_fatally_unparseable()
    {
        var (spec, diagnostics) = MappingLoader.Parse("   ");

        Assert.Null(spec);
        Assert.NotEmpty(diagnostics);
    }
}

public class RecordExtractorTests
{
    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private static MappingSpec BuildSpec(string itemsPath = "$.items[*]", string? timestampField = null) => new()
    {
        ItemsPath = itemsPath,
        TimestampField = timestampField,
        Fields =
        [
            new FieldMapEntry { Field = new FieldDef("id", FieldType.String) },
            new FieldMapEntry { SourcePath = "nested.value", Field = new FieldDef("value", FieldType.Long) },
            new FieldMapEntry { SourcePath = "tags[*]", Field = new FieldDef("tagsFromWildcard", FieldType.String, IsArray: true) },
            new FieldMapEntry { SourcePath = "tags", Field = new FieldDef("tagsFromArrayValue", FieldType.String, IsArray: true) },
            new FieldMapEntry { SourcePath = "missing", Field = new FieldDef("missingField", FieldType.String) },
        ],
    };

    private const string SampleItem =
        """{"id":"x1","nested":{"value":42},"tags":["a","b","c"],"ts":1700000000000}""";

    [Fact]
    public void Nested_path_and_array_extraction_and_missing_path_omission()
    {
        var root = Parse($$"""{"items":[{{SampleItem}}]}""");
        var spec = BuildSpec();

        var rows = RecordExtractor.Extract(root, spec, arrivalMs: 999);

        Assert.Single(rows);
        var row = rows[0];

        Assert.Equal("x1", row["id"]);
        Assert.Equal(42L, row["value"]); // nested.value
        Assert.Equal(new List<object?> { "a", "b", "c" }, (List<object?>)row["tagsFromWildcard"]!); // from [*] matches
        Assert.Equal(new List<object?> { "a", "b", "c" }, (List<object?>)row["tagsFromArrayValue"]!); // from a single array value
        Assert.False(row.ContainsKey("missingField")); // missing path -> key omitted entirely
    }

    [Fact]
    public void Items_path_dollar_on_array_root_yields_one_row_per_element()
    {
        var root = Parse("""[{"id":"a"},{"id":"b"}]""");
        var spec = new MappingSpec
        {
            ItemsPath = "$",
            Fields = [new FieldMapEntry { Field = new FieldDef("id", FieldType.String) }],
        };

        var rows = RecordExtractor.Extract(root, spec, arrivalMs: 0);

        Assert.Equal(2, rows.Count);
        Assert.Equal("a", rows[0]["id"]);
        Assert.Equal("b", rows[1]["id"]);
    }

    [Fact]
    public void Items_path_dollar_on_object_root_yields_the_single_item()
    {
        var root = Parse("""{"id":"solo"}""");
        var spec = new MappingSpec
        {
            ItemsPath = "$",
            Fields = [new FieldMapEntry { Field = new FieldDef("id", FieldType.String) }],
        };

        var rows = RecordExtractor.Extract(root, spec, arrivalMs: 0);

        Assert.Single(rows);
        Assert.Equal("solo", rows[0]["id"]);
    }

    [Fact]
    public void Items_path_pointing_at_an_array_without_trailing_wildcard_still_spreads_it()
    {
        var root = Parse("""{"data":[{"id":"a"},{"id":"b"},{"id":"c"}]}""");
        var spec = new MappingSpec
        {
            ItemsPath = "$.data",
            Fields = [new FieldMapEntry { Field = new FieldDef("id", FieldType.String) }],
        };

        var rows = RecordExtractor.Extract(root, spec, arrivalMs: 0);

        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public void Ts_from_epoch_ms_number()
    {
        var root = Parse($$"""{"items":[{{SampleItem}}]}""");
        var spec = BuildSpec(timestampField: "value"); // "value" <- nested.value = 42 (long)

        var rows = RecordExtractor.Extract(root, spec, arrivalMs: 999);

        Assert.Equal(42L, rows[0]["_ts"]);
    }

    [Fact]
    public void Ts_from_fractional_number_truncates_to_long()
    {
        var root = Parse("""{"items":[{"id":"x","ts":1700000000000.9}]}""");
        var spec = new MappingSpec
        {
            ItemsPath = "$.items[*]",
            TimestampField = "ts",
            Fields =
            [
                new FieldMapEntry { Field = new FieldDef("id", FieldType.String) },
                new FieldMapEntry { Field = new FieldDef("ts", FieldType.Timestamp) },
            ],
        };

        var rows = RecordExtractor.Extract(root, spec, arrivalMs: 1);

        Assert.Equal(1700000000000L, rows[0]["_ts"]);
    }

    [Fact]
    public void Ts_from_iso8601_string_converts_to_epoch_ms_utc()
    {
        var root = Parse("""{"items":[{"id":"x","ts":"2020-01-01T00:00:00Z"}]}""");
        var spec = new MappingSpec
        {
            ItemsPath = "$.items[*]",
            TimestampField = "ts",
            Fields =
            [
                new FieldMapEntry { Field = new FieldDef("id", FieldType.String) },
                new FieldMapEntry { Field = new FieldDef("ts", FieldType.Timestamp) },
            ],
        };

        var rows = RecordExtractor.Extract(root, spec, arrivalMs: 1);

        var expected = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds();
        Assert.Equal(expected, rows[0]["_ts"]);
    }

    [Fact]
    public void Ts_falls_back_to_arrival_when_unparseable()
    {
        var root = Parse("""{"items":[{"id":"x","ts":"not-a-date"}]}""");
        var spec = new MappingSpec
        {
            ItemsPath = "$.items[*]",
            TimestampField = "ts",
            Fields =
            [
                new FieldMapEntry { Field = new FieldDef("id", FieldType.String) },
                new FieldMapEntry { Field = new FieldDef("ts", FieldType.Timestamp) },
            ],
        };

        var rows = RecordExtractor.Extract(root, spec, arrivalMs: 12345);

        Assert.Equal(12345L, rows[0]["_ts"]);
    }

    [Fact]
    public void Ts_falls_back_to_arrival_when_no_timestamp_field_configured()
    {
        var root = Parse($$"""{"items":[{{SampleItem}}]}""");
        var spec = BuildSpec(timestampField: null);

        var rows = RecordExtractor.Extract(root, spec, arrivalMs: 55555);

        Assert.Equal(55555L, rows[0]["_ts"]);
    }

    [Fact]
    public void Row_never_contains_a_source_key()
    {
        var root = Parse($$"""{"items":[{{SampleItem}}]}""");
        var spec = BuildSpec();

        var rows = RecordExtractor.Extract(root, spec, arrivalMs: 0);

        Assert.False(rows[0].ContainsKey("_source"));
    }
}
