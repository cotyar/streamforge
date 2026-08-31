using StreamsForge.AppCore.Config;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 006 (D-I): <see cref="ConfigComposer"/> — later-document-wins shallow field
/// override (scalar/whole-list/whole-object/explicit-null/absent), multi-document order, and
/// include-chain resolution (nesting, precedence, missing includes, cycles).</summary>
public class ConfigComposerTests
{
    // ------------------------------------------------------------------
    // Shallow override matrix.
    // ------------------------------------------------------------------

    [Fact]
    public void Later_document_overrides_a_scalar_field()
    {
        var (doc, diagnostics) = ConfigComposer.Compose([
            """{"version":1,"pipelines":[{"name":"p","sql":"SELECT 1","description":"base"}]}""",
            """{"version":1,"pipelines":[{"name":"p","description":"overlay"}]}""",
        ]);

        Assert.Empty(diagnostics);
        var p = Assert.Single(doc!.Pipelines);
        Assert.Equal("overlay", p.Description);
        Assert.Equal("SELECT 1", p.Sql); // absent from the later doc -> kept from the earlier one.
    }

    [Fact]
    public void Later_document_replaces_a_list_whole_never_element_merges()
    {
        var (doc, _) = ConfigComposer.Compose([
            """{"version":1,"pipelines":[{"name":"p","sql":"SELECT 1","tags":["a","b"]}]}""",
            """{"version":1,"pipelines":[{"name":"p","tags":["c"]}]}""",
        ]);

        var p = Assert.Single(doc!.Pipelines);
        Assert.Equal(["c"], p.Tags);
    }

    [Fact]
    public void Later_document_replaces_a_nested_object_whole_not_merged()
    {
        // doc1's source has BOTH url and schedule under connector; doc2 only sets schedule.
        // Whole-object replace means the composed connector is doc2's connector VERBATIM — url is
        // gone, not preserved from doc1 (never element/property-merged below the entity's own
        // top-level keys).
        var (doc, _) = ConfigComposer.Compose([
            """{"version":1,"sources":[{"name":"s","kind":"url","connector":{"url":{"url":"http://old"},"schedule":{"intervalMs":1000}}}]}""",
            """{"version":1,"sources":[{"name":"s","connector":{"schedule":{"cron":"* * * * *"}}}]}""",
        ]);

        var s = Assert.Single(doc!.Sources);
        Assert.Null(s.Connector!.Url);
        Assert.Equal("* * * * *", s.Connector.Schedule!.Cron);
        Assert.Null(s.Connector.Schedule.IntervalMs);
    }

    [Fact]
    public void Explicit_null_clears_an_optional_field()
    {
        var (doc, _) = ConfigComposer.Compose([
            """{"version":1,"tables":[{"name":"t","sql":"T","historyMode":"MaxBy","historyByField":"ts"}]}""",
            """{"version":1,"tables":[{"name":"t","historyByField":null}]}""",
        ]);

        var t = Assert.Single(doc!.Tables);
        Assert.Null(t.HistoryByField);
        Assert.Equal("MaxBy", t.HistoryMode); // untouched field is kept.
    }

    [Fact]
    public void Field_absent_from_the_later_document_keeps_the_earlier_value()
    {
        var (doc, _) = ConfigComposer.Compose([
            """{"version":1,"pipelines":[{"name":"p","sql":"SELECT 1","tags":["x"],"description":"d"}]}""",
            """{"version":1,"pipelines":[{"name":"p","description":"d2"}]}""",
        ]);

        var p = Assert.Single(doc!.Pipelines);
        Assert.Equal("SELECT 1", p.Sql);
        Assert.Equal(["x"], p.Tags);
        Assert.Equal("d2", p.Description);
    }

    [Fact]
    public void Multi_document_order_chains_left_to_right_last_wins()
    {
        var (doc, diagnostics) = ConfigComposer.Compose([
            """{"version":1,"sources":[{"name":"s","description":"v0"}]}""",
            """{"version":1,"sources":[{"name":"s","description":"v1"}]}""",
            """{"version":1,"sources":[{"name":"s","description":"v2"}]}""",
        ]);

        Assert.Empty(diagnostics);
        Assert.Equal("v2", Assert.Single(doc!.Sources).Description);
    }

    [Fact]
    public void An_entity_only_present_in_a_later_document_is_added()
    {
        var (doc, _) = ConfigComposer.Compose([
            """{"version":1,"sources":[{"name":"a"}]}""",
            """{"version":1,"sources":[{"name":"b"}]}""",
        ]);

        Assert.Equal(["a", "b"], doc!.Sources.Select(s => s.Name).OrderBy(n => n));
    }

    [Fact]
    public void Compose_of_empty_list_yields_empty_document()
    {
        var (doc, diagnostics) = ConfigComposer.Compose([]);
        Assert.NotNull(doc);
        Assert.Empty(doc!.Sources);
        Assert.Empty(diagnostics);
    }

    [Fact]
    public void Per_doc_parse_diagnostics_are_prefixed_with_the_doc_index()
    {
        var (doc, diagnostics) = ConfigComposer.Compose([
            """{"version":1,"sources":[{"name":"ok"}]}""",
            "{not json",
        ]);

        Assert.NotNull(doc); // the good doc still composes.
        Assert.Contains(diagnostics, d => d.StartsWith("doc[1]:") && d.Contains("invalid JSON"));
    }

    // ------------------------------------------------------------------
    // Include resolution.
    // ------------------------------------------------------------------

    [Fact]
    public void Include_chain_precedence_is_base_then_overlay_then_includer()
    {
        var files = new Dictionary<string, string>
        {
            ["base.json"] = """{"version":1,"sources":[{"name":"s","description":"from-base","tags":["base-tag"]}]}""",
            ["overlay.json"] = """{"version":1,"include":["base.json"],"sources":[{"name":"s","description":"from-overlay"}]}""",
            ["root.json"] = """{"version":1,"include":["overlay.json"],"sources":[{"name":"s","sql":"root-sql"}]}""",
        };

        var (doc, diagnostics) = ConfigComposer.ComposeWithIncludes("root.json", p => files.GetValueOrDefault(p));

        Assert.Empty(diagnostics);
        var s = Assert.Single(doc!.Sources);
        Assert.Equal("from-overlay", s.Description); // root didn't override description -> overlay wins over base.
        Assert.Equal(["base-tag"], s.Tags); // nobody overrode tags -> base's value survives.
    }

    [Fact]
    public void Nested_includes_resolve_recursively()
    {
        var files = new Dictionary<string, string>
        {
            ["a.json"] = """{"version":1,"sources":[{"name":"s","description":"a"}]}""",
            ["b.json"] = """{"version":1,"include":["a.json"],"sources":[{"name":"s","description":"b"}]}""",
            ["c.json"] = """{"version":1,"include":["b.json"],"sources":[{"name":"s","description":"c"}]}""",
            ["root.json"] = """{"version":1,"include":["c.json"]}""",
        };

        var (doc, diagnostics) = ConfigComposer.ComposeWithIncludes("root.json", p => files.GetValueOrDefault(p));

        Assert.Empty(diagnostics);
        Assert.Equal("c", Assert.Single(doc!.Sources).Description);
    }

    [Fact]
    public void Missing_include_is_a_fatal_diagnostic()
    {
        var files = new Dictionary<string, string>
        {
            ["root.json"] = """{"version":1,"include":["missing.json"]}""",
        };

        var (doc, diagnostics) = ConfigComposer.ComposeWithIncludes("root.json", p => files.GetValueOrDefault(p));

        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("missing include") && d.Contains("missing.json"));
    }

    [Fact]
    public void Include_cycle_is_rejected_and_names_the_cycle()
    {
        var files = new Dictionary<string, string>
        {
            ["a.json"] = """{"version":1,"include":["b.json"]}""",
            ["b.json"] = """{"version":1,"include":["a.json"]}""",
        };

        var (doc, diagnostics) = ConfigComposer.ComposeWithIncludes("a.json", p => files.GetValueOrDefault(p));

        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("cycle") && d.Contains("a.json") && d.Contains("b.json"));
    }

    [Fact]
    public void Self_include_is_rejected_as_a_cycle()
    {
        var files = new Dictionary<string, string>
        {
            ["a.json"] = """{"version":1,"include":["a.json"]}""",
        };

        var (doc, diagnostics) = ConfigComposer.ComposeWithIncludes("a.json", p => files.GetValueOrDefault(p));

        Assert.Null(doc);
        Assert.Contains(diagnostics, d => d.Contains("cycle"));
    }

    // ------------------------------------------------------------------
    // Document-level carry (schemaPolicy, requires) — MergeDocs rebuilds the merged root from scratch,
    // which is exactly what silently dropped schemaPolicy once (found live by wave 3-C, fixed by the
    // orchestrator; nothing here regression-tested THAT fix before now). Plan 016 wave 4 gives requires
    // the identical "reaches the merged root" guarantee, via the SAME key-keyed-map treatment
    // sources/pipelines/tables already get (see MergeEntitiesInto's keyProperty parameter) rather than
    // schemaPolicy's whole-scalar-wins-by-presence-or-absence rule — requires is a repeatable list keyed
    // by kind, not a single scalar, so "does an include's declaration survive when the includer doesn't
    // repeat it" has a real "yes" answer here that schemaPolicy's security reasoning deliberately rules
    // out for itself.
    // ------------------------------------------------------------------

    [Fact]
    public void SchemaPolicy_survives_multi_document_composition()
    {
        var (doc, diagnostics) = ConfigComposer.Compose([
            """{"version":1,"sources":[{"name":"s"}]}""",
            """{"version":1,"schemaPolicy":"any"}""",
        ]);

        Assert.Empty(diagnostics);
        Assert.Equal("any", doc!.SchemaPolicy);
    }

    [Fact]
    public void Requires_survives_multi_document_composition()
    {
        var (doc, diagnostics) = ConfigComposer.Compose([
            """{"version":1,"sources":[{"name":"s"}]}""",
            """{"version":1,"requires":[{"kind":"postgres-cdc","version":"^2.0.0"}]}""",
        ]);

        Assert.Empty(diagnostics);
        var req = Assert.Single(doc!.Requires);
        Assert.Equal("postgres-cdc", req.Kind);
        Assert.Equal("^2.0.0", req.Version);
    }

    [Fact]
    public void Requires_survives_an_include_chain_when_only_the_include_declares_it()
    {
        var files = new Dictionary<string, string>
        {
            ["base.json"] = """{"version":1,"requires":[{"kind":"fix","version":"^1.0.0"}]}""",
            ["root.json"] = """{"version":1,"include":["base.json"],"sources":[{"name":"s"}]}""",
        };

        var (doc, diagnostics) = ConfigComposer.ComposeWithIncludes("root.json", p => files.GetValueOrDefault(p));

        Assert.Empty(diagnostics);
        Assert.Equal("fix", Assert.Single(doc!.Requires).Kind);
    }

    [Fact]
    public void Requires_from_root_and_include_merge_by_kind_when_the_kinds_differ()
    {
        // Same "later document wins per KEY" rule sources/pipelines/tables already follow — a shared
        // base template's requirement for one kind and the includer's own requirement for a different
        // kind are both real requirements of the composed document, so both must survive.
        var files = new Dictionary<string, string>
        {
            ["base.json"] = """{"version":1,"requires":[{"kind":"fix","version":"^1.0.0"}]}""",
            ["root.json"] = """{"version":1,"include":["base.json"],"requires":[{"kind":"nats","version":"*"}],"sources":[{"name":"s"}]}""",
        };

        var (doc, diagnostics) = ConfigComposer.ComposeWithIncludes("root.json", p => files.GetValueOrDefault(p));

        Assert.Empty(diagnostics);
        Assert.Equal(["fix", "nats"], doc!.Requires.Select(r => r.Kind).OrderBy(k => k, StringComparer.Ordinal));
    }

    [Fact]
    public void Requires_root_overrides_the_SAME_kind_declared_by_an_include()
    {
        var files = new Dictionary<string, string>
        {
            ["base.json"] = """{"version":1,"requires":[{"kind":"fix","version":"^1.0.0"}]}""",
            ["root.json"] = """{"version":1,"include":["base.json"],"requires":[{"kind":"fix","version":"^2.0.0"}],"sources":[{"name":"s"}]}""",
        };

        var (doc, diagnostics) = ConfigComposer.ComposeWithIncludes("root.json", p => files.GetValueOrDefault(p));

        Assert.Empty(diagnostics);
        var req = Assert.Single(doc!.Requires); // one entry, not two -- same kind, later (root) wins whole-item.
        Assert.Equal("fix", req.Kind);
        Assert.Equal("^2.0.0", req.Version);
    }
}
