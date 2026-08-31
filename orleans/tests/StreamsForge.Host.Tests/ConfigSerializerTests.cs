using StreamsForge.Abstractions;
using StreamsForge.AppCore.Config;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 006 (D-I): <see cref="ConfigSerializer"/> — JSON/YAML parse, canonical byte-
/// stability, omission rules, entity sorting, and <see cref="ConfigSerializer.FromCatalog"/>
/// mapping (Running/masking). See <see cref="ConfigJsonMapper"/>'s class doc for the exact
/// canonical omission rule these tests pin.</summary>
public class ConfigSerializerTests
{
    private static SourceDefinition Source(string name, string kind = SourceKinds.Generator, ConnectorConfig? connector = null) => new()
    {
        Name = name,
        Description = "",
        Fields = [new FieldDef("price", FieldType.Double)],
        GeneratorProfile = "generic",
        EventsPerSecond = 5,
        Enabled = true,
        Kind = kind,
        Connector = connector,
    };

    private static PipelineDefinition Pipeline(string name, PipelineStatus status) => new()
    {
        Id = name,
        Name = name,
        Description = "",
        Sql = $"SELECT * FROM {name}_src",
        Status = status,
    };

    private static TableDefinition Table(string name, PipelineStatus status) => new()
    {
        Id = name,
        Name = name,
        Description = "",
        Sql = $"SELECT * FROM {name}_src",
        Status = status,
    };

    // ------------------------------------------------------------------
    // Parse: JSON/YAML equivalence + unparseable/unknown-version/structural diagnostics.
    // ------------------------------------------------------------------

    [Fact]
    public void Parse_json_and_equivalent_yaml_produce_the_same_canonical_document()
    {
        var doc = new ConfigDocument
        {
            Sources = [Source("trades")],
            Pipelines = [new ConfigPipeline { Name = "p1", Sql = "SELECT * FROM trades", Running = true }],
            Tables = [new ConfigTable { Name = "t1", Sql = "SELECT * FROM trades", SearchMode = "Fuzzy" }],
        };
        var json = ConfigSerializer.ToCanonicalJson(doc);
        var yaml = ConfigSerializer.ToYaml(doc);

        var (fromJson, jsonDiag) = ConfigSerializer.Parse(json);
        var (fromYaml, yamlDiag) = ConfigSerializer.Parse(yaml);

        Assert.Empty(jsonDiag);
        Assert.Empty(yamlDiag);
        Assert.NotNull(fromJson);
        Assert.NotNull(fromYaml);
        Assert.Equal(ConfigSerializer.ToCanonicalJson(fromJson!), ConfigSerializer.ToCanonicalJson(fromYaml!));
    }

    [Fact]
    public void Parse_rejects_unparseable_json()
    {
        var (doc, diagnostics) = ConfigSerializer.Parse("{not valid json");
        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("invalid JSON"));
    }

    [Fact]
    public void Parse_rejects_unparseable_yaml()
    {
        var (doc, diagnostics) = ConfigSerializer.Parse("sources:\n- name: a\n  bad indent:\n foo");
        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("invalid YAML"));
    }

    [Fact]
    public void Parse_rejects_unknown_version()
    {
        var (doc, diagnostics) = ConfigSerializer.Parse("""{"version": 2}""");
        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("unsupported version"));
    }

    [Fact]
    public void Parse_rejects_non_object_root()
    {
        var (doc, diagnostics) = ConfigSerializer.Parse("[1,2,3]");
        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("root must be a JSON object"));
    }

    [Fact]
    public void Parse_skips_an_entity_missing_name_but_keeps_the_rest()
    {
        var (doc, diagnostics) = ConfigSerializer.Parse(
            """{"version":1,"sources":[{"description":"no name here"},{"name":"ok"}]}""");
        Assert.NotNull(doc);
        Assert.Single(doc!.Sources);
        Assert.Equal("ok", doc.Sources[0].Name);
        Assert.Contains(diagnostics, d => d.Contains("sources[0]") && d.Contains("missing name"));
    }

    [Fact]
    public void Parse_of_empty_text_yields_empty_document_with_no_diagnostics()
    {
        var (doc, diagnostics) = ConfigSerializer.Parse("   \n  ");
        Assert.NotNull(doc);
        Assert.Empty(doc!.Sources);
        Assert.Equal(1, doc.Version);
        Assert.Empty(diagnostics);
    }

    // ------------------------------------------------------------------
    // Canonical byte-stability.
    // ------------------------------------------------------------------

    [Fact]
    public void ToCanonicalJson_is_stable_under_serialize_parse_serialize()
    {
        var doc = new ConfigDocument
        {
            Sources = [Source("z-src"), Source("a-src", SourceKinds.Url, new ConnectorConfig
            {
                Schedule = new ScheduleSpec { IntervalMs = 5000 },
                Url = new UrlPollConfig { Url = "http://example.test", Headers = { ["X-Key"] = "secret" } },
                Mapping = new MappingSpec { ItemsPath = "$.data[*]", Fields = [new FieldMapEntry { Field = new FieldDef("v", FieldType.Double) }] },
            })],
            Pipelines = [new ConfigPipeline { Name = "p", Sql = "SELECT 1", Running = true, Tags = ["x"] }],
            Tables = [new ConfigTable { Name = "t", Sql = "SELECT 1", HistoryEnabled = true, HistoryMode = "MaxBy", HistoryByField = "ts" }],
        };

        var json1 = ConfigSerializer.ToCanonicalJson(doc);
        var (parsed, diagnostics) = ConfigSerializer.Parse(json1);
        Assert.Empty(diagnostics);
        var json2 = ConfigSerializer.ToCanonicalJson(parsed!);

        Assert.Equal(json1, json2);
    }

    [Fact]
    public void ToCanonicalJson_uses_two_space_indent_and_camelCase()
    {
        var doc = new ConfigDocument { Sources = [Source("s")] };
        var json = ConfigSerializer.ToCanonicalJson(doc);

        Assert.Contains("\n  \"sources\": [", json);
        Assert.Contains("\"eventsPerSecond\"", json);
        Assert.DoesNotContain("EventsPerSecond", json);
    }

    // ------------------------------------------------------------------
    // Omission rules.
    // ------------------------------------------------------------------

    [Fact]
    public void Kind_is_omitted_for_the_default_generator_kind_but_kept_otherwise()
    {
        var doc = new ConfigDocument { Sources = [Source("gen"), Source("web", SourceKinds.Url, new ConnectorConfig { Url = new UrlPollConfig { Url = "http://x" } })] };
        var json = ConfigSerializer.ToCanonicalJson(doc);

        // The generator-kind source's object should not mention "kind" at all.
        var genStart = json.IndexOf("\"name\": \"gen\"", StringComparison.Ordinal);
        var webStart = json.IndexOf("\"name\": \"web\"", StringComparison.Ordinal);
        Assert.True(genStart >= 0 && webStart >= 0);
        var genObjectEnd = json.IndexOf('}', genStart);
        var genSlice = json[genStart..genObjectEnd];
        Assert.DoesNotContain("\"kind\"", genSlice);
        Assert.Contains("\"kind\": \"url\"", json);
    }

    [Fact]
    public void Running_false_is_never_omitted()
    {
        var doc = new ConfigDocument { Pipelines = [new ConfigPipeline { Name = "p", Sql = "SELECT 1", Running = false }] };
        var json = ConfigSerializer.ToCanonicalJson(doc);
        Assert.Contains("\"running\": false", json);
    }

    [Fact]
    public void Empty_collections_and_null_connector_are_omitted()
    {
        // The omission rule is precisely "null or an empty array/object" (see ConfigJsonMapper's
        // class doc) — NOT "the CLR/field-initializer default for every type". An empty STRING
        // (Description == "") is neither null nor an empty collection, so it is intentionally KEPT
        // (see the companion test below) — only collections/null are pruned.
        var doc = new ConfigDocument { Sources = [Source("s")] };
        var json = ConfigSerializer.ToCanonicalJson(doc);

        Assert.DoesNotContain("\"tags\"", json);
        Assert.DoesNotContain("\"metadata\"", json);
        Assert.DoesNotContain("\"connector\"", json); // Connector is null for a generator source.
    }

    [Fact]
    public void Empty_string_fields_are_not_omitted_only_null_and_empty_collections_are()
    {
        var doc = new ConfigDocument { Sources = [Source("s")] };
        var json = ConfigSerializer.ToCanonicalJson(doc);

        Assert.Contains("\"description\": \"\"", json);
    }

    [Fact]
    public void Include_and_empty_entity_arrays_are_omitted_from_the_root()
    {
        var doc = new ConfigDocument();
        var json = ConfigSerializer.ToCanonicalJson(doc);

        Assert.Equal("{\n  \"version\": 1\n}", json);
    }

    [Fact]
    public void A_scheduleSpec_with_only_null_fields_disappears_entirely()
    {
        var doc = new ConfigDocument
        {
            Sources = [Source("s", SourceKinds.Url, new ConnectorConfig
            {
                Schedule = new ScheduleSpec(), // Cron and IntervalMs both null.
                Url = new UrlPollConfig { Url = "http://x" },
            })],
        };
        var json = ConfigSerializer.ToCanonicalJson(doc);

        Assert.DoesNotContain("\"schedule\"", json);
        Assert.Contains("\"url\"", json); // The Url sub-config itself survives (has a non-empty Url).
    }

    // ------------------------------------------------------------------
    // Entity sorting.
    // ------------------------------------------------------------------

    [Fact]
    public void Entities_are_sorted_by_name_ordinal_within_each_array()
    {
        var doc = new ConfigDocument { Sources = [Source("zebra"), Source("alpha"), Source("Mango")] };
        var json = ConfigSerializer.ToCanonicalJson(doc);

        var iAlpha = json.IndexOf("\"alpha\"", StringComparison.Ordinal);
        var iMango = json.IndexOf("\"Mango\"", StringComparison.Ordinal);
        var iZebra = json.IndexOf("\"zebra\"", StringComparison.Ordinal);

        // Ordinal comparison: uppercase 'M' (77) sorts before lowercase 'a'/'z' (97+).
        Assert.True(iMango < iAlpha);
        Assert.True(iAlpha < iZebra);
    }

    // ------------------------------------------------------------------
    // FromCatalog.
    // ------------------------------------------------------------------

    [Fact]
    public void FromCatalog_maps_running_desired_state_from_status()
    {
        var doc = ConfigSerializer.FromCatalog(
            [],
            [Pipeline("running-one", PipelineStatus.Running), Pipeline("failed-one", PipelineStatus.Failed), Pipeline("stopped-one", PipelineStatus.Stopped)],
            [],
            includeSecrets: true);

        Assert.True(doc.Pipelines.Single(p => p.Name == "running-one").Running);
        Assert.True(doc.Pipelines.Single(p => p.Name == "failed-one").Running);
        Assert.False(doc.Pipelines.Single(p => p.Name == "stopped-one").Running);
    }

    [Fact]
    public void FromCatalog_maps_table_running_and_search_mode()
    {
        var table = Table("t", PipelineStatus.Running);
        table.SearchMode = TableSearchMode.Fuzzy;

        var doc = ConfigSerializer.FromCatalog([], [], [table], includeSecrets: true);

        var t = doc.Tables.Single();
        Assert.True(t.Running);
        Assert.Equal("Fuzzy", t.SearchMode);
    }

    [Fact]
    public void FromCatalog_masks_secrets_by_default_and_reveals_them_with_includeSecrets()
    {
        var source = Source("web", SourceKinds.Url, new ConnectorConfig
        {
            Url = new UrlPollConfig { Url = "http://x", Headers = { ["Authorization"] = "Bearer real-token" } },
            Grpc = null,
        });

        var masked = ConfigSerializer.FromCatalog([source], [], [], includeSecrets: false);
        Assert.Equal(SourceKinds.SecretMask, masked.Sources[0].Connector!.Url!.Headers["Authorization"]);

        var revealed = ConfigSerializer.FromCatalog([source], [], [], includeSecrets: true);
        Assert.Equal("Bearer real-token", revealed.Sources[0].Connector!.Url!.Headers["Authorization"]);

        // FromCatalog must not mutate the original catalog object either way.
        Assert.Equal("Bearer real-token", source.Connector!.Url!.Headers["Authorization"]);
    }
}
