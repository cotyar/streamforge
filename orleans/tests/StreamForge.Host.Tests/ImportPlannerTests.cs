using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>Plan 006 (D-J): <see cref="ImportPlanner"/> — created/updated/skipped/deleted per
/// entity, apply ordering (sources, then topo-sorted tables, then pipelines; deletions last in
/// reverse-dependency order), masked-secret equality, and replace-mode deletion.</summary>
public class ImportPlannerTests
{
    private static SourceDefinition CatalogSource(string name) => new() { Name = name, Fields = [new FieldDef("v", FieldType.Double)] };

    private static PipelineDefinition CatalogPipeline(string name, PipelineStatus status = PipelineStatus.Running) => new()
    {
        Id = name,
        Name = name,
        Sql = "SELECT 1",
        Status = status,
    };

    private static TableDefinition CatalogTable(string name, PipelineStatus status = PipelineStatus.Running, List<string>? inputs = null) => new()
    {
        Id = name,
        Name = name,
        Sql = "SELECT 1",
        Status = status,
        TableInputs = inputs ?? [],
    };

    // ------------------------------------------------------------------
    // created / updated / skipped.
    // ------------------------------------------------------------------

    [Fact]
    public void New_entity_is_created()
    {
        var doc = new ConfigDocument { Sources = [CatalogSource("new-src")] };
        var actions = ImportPlanner.Plan(doc, [], [], [], "merge");

        var action = Assert.Single(actions);
        Assert.Equal("source", action.Kind);
        Assert.Equal("created", action.Action);
    }

    [Fact]
    public void Identical_pipeline_is_skipped()
    {
        var catalog = CatalogPipeline("p", PipelineStatus.Running);
        var doc = new ConfigDocument { Pipelines = [ConfigDocPipelineFrom(catalog)] };

        var actions = ImportPlanner.Plan(doc, [], [catalog], [], "merge");

        Assert.Equal("skipped", Assert.Single(actions).Action);
    }

    [Fact]
    public void Differing_pipeline_is_updated()
    {
        var catalog = CatalogPipeline("p", PipelineStatus.Running);
        var docPipeline = ConfigDocPipelineFrom(catalog);
        docPipeline.Sql = "SELECT 2"; // differs from catalog's "SELECT 1"

        var doc = new ConfigDocument { Pipelines = [docPipeline] };
        var actions = ImportPlanner.Plan(doc, [], [catalog], [], "merge");

        Assert.Equal("updated", Assert.Single(actions).Action);
    }

    [Fact]
    public void Stopped_status_maps_to_running_false_for_comparison()
    {
        var catalog = CatalogPipeline("p", PipelineStatus.Stopped);
        var docPipeline = ConfigDocPipelineFrom(catalog);
        docPipeline.Running = false;

        var actions = ImportPlanner.Plan(new ConfigDocument { Pipelines = [docPipeline] }, [], [catalog], [], "merge");
        Assert.Equal("skipped", Assert.Single(actions).Action);
    }

    [Fact]
    public void Failed_status_catalog_pipeline_compares_equal_to_running_true_in_doc()
    {
        var catalog = CatalogPipeline("p", PipelineStatus.Failed);
        var docPipeline = ConfigDocPipelineFrom(catalog);
        docPipeline.Running = true; // Failed exports/compares as Running == true (D-I).

        var actions = ImportPlanner.Plan(new ConfigDocument { Pipelines = [docPipeline] }, [], [catalog], [], "merge");
        Assert.Equal("skipped", Assert.Single(actions).Action);
    }

    private static ConfigPipeline ConfigDocPipelineFrom(PipelineDefinition p) => new()
    {
        Name = p.Name,
        Sql = p.Sql,
        Running = p.Status != PipelineStatus.Stopped,
    };

    // ------------------------------------------------------------------
    // Masked-secret equality.
    // ------------------------------------------------------------------

    [Fact]
    public void Masked_secret_in_doc_compares_equal_to_stored_real_value()
    {
        var stored = new SourceDefinition
        {
            Name = "web",
            Kind = SourceKinds.Url,
            Connector = new ConnectorConfig { Url = new UrlPollConfig { Url = "http://x", Headers = { ["Authorization"] = "Bearer real-secret" } } },
        };
        var docSource = new SourceDefinition
        {
            Name = "web",
            Kind = SourceKinds.Url,
            Connector = new ConnectorConfig { Url = new UrlPollConfig { Url = "http://x", Headers = { ["Authorization"] = SourceKinds.SecretMask } } },
        };

        var actions = ImportPlanner.Plan(new ConfigDocument { Sources = [docSource] }, [stored], [], [], "merge");

        var action = Assert.Single(actions);
        Assert.Equal("skipped", action.Action);
        Assert.Contains(action.Diagnostics, d => d.Contains("secrets: kept stored values"));
    }

    [Fact]
    public void Masked_secret_does_not_hide_a_genuine_change_elsewhere()
    {
        var stored = new SourceDefinition
        {
            Name = "web",
            Kind = SourceKinds.Url,
            Connector = new ConnectorConfig { Url = new UrlPollConfig { Url = "http://old", Headers = { ["Authorization"] = "Bearer real-secret" } } },
        };
        var docSource = new SourceDefinition
        {
            Name = "web",
            Kind = SourceKinds.Url,
            Connector = new ConnectorConfig { Url = new UrlPollConfig { Url = "http://new", Headers = { ["Authorization"] = SourceKinds.SecretMask } } },
        };

        var actions = ImportPlanner.Plan(new ConfigDocument { Sources = [docSource] }, [stored], [], [], "merge");

        var action = Assert.Single(actions);
        Assert.Equal("updated", action.Action);
        Assert.Contains(action.Diagnostics, d => d.Contains("secrets: kept stored values"));
    }

    // ------------------------------------------------------------------
    // Apply ordering: sources -> tables (topo) -> pipelines.
    // ------------------------------------------------------------------

    [Fact]
    public void Overall_order_is_sources_then_tables_then_pipelines()
    {
        var doc = new ConfigDocument
        {
            Sources = [CatalogSource("s")],
            Tables = [new ConfigTable { Name = "t", Sql = "SELECT 1" }],
            Pipelines = [new ConfigPipeline { Name = "p", Sql = "SELECT 1" }],
        };

        var actions = ImportPlanner.Plan(doc, [], [], [], "merge");

        Assert.Equal(["source", "table", "pipeline"], actions.Select(a => a.Kind));
    }

    [Fact]
    public void Existing_table_dependency_orders_the_dependency_first()
    {
        // Catalog already has table B depending on table A (TableInputs); the import updates both.
        var catalogA = CatalogTable("A");
        var catalogB = CatalogTable("B", inputs: ["A"]);
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "B", Sql = "SELECT 2 FROM A" }, // listed first in the doc, differs -> updated
                new ConfigTable { Name = "A", Sql = "SELECT 2" },        // differs -> updated
            ],
        };

        var actions = ImportPlanner.Plan(doc, [], [], [catalogA, catalogB], "merge");
        var order = actions.Select(a => a.Name).ToList();

        Assert.True(order.IndexOf("A") < order.IndexOf("B"), $"expected A before B, got [{string.Join(",", order)}]");
    }

    [Fact]
    public void New_table_dependency_is_inferred_from_sql_from_reference()
    {
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "downstream", Sql = "SELECT * FROM upstream" },
                new ConfigTable { Name = "upstream", Sql = "SELECT 1" },
            ],
        };

        var actions = ImportPlanner.Plan(doc, [], [], [], "merge");
        var order = actions.Where(a => a.Kind == "table").Select(a => a.Name).ToList();

        Assert.Equal(["upstream", "downstream"], order);
    }

    [Fact]
    public void Table_dependency_cycle_is_diagnosed_not_crashed()
    {
        var doc = new ConfigDocument
        {
            Tables =
            [
                new ConfigTable { Name = "x", Sql = "SELECT * FROM y" },
                new ConfigTable { Name = "y", Sql = "SELECT * FROM x" },
            ],
        };

        var actions = ImportPlanner.Plan(doc, [], [], [], "merge");
        var tableActions = actions.Where(a => a.Kind == "table").ToList();

        Assert.Equal(2, tableActions.Count);
        Assert.Contains(tableActions, a => a.Diagnostics.Any(d => d.Contains("cycle")));
    }

    // ------------------------------------------------------------------
    // Replace mode: deletions.
    // ------------------------------------------------------------------

    [Fact]
    public void Merge_mode_never_deletes()
    {
        var catalogSource = CatalogSource("gone");
        var actions = ImportPlanner.Plan(new ConfigDocument(), [catalogSource], [], [], "merge");
        Assert.Empty(actions);
    }

    [Fact]
    public void Replace_mode_deletes_entities_absent_from_the_document()
    {
        var catalogSource = CatalogSource("gone");
        var actions = ImportPlanner.Plan(new ConfigDocument(), [catalogSource], [], [], "replace");

        var action = Assert.Single(actions);
        Assert.Equal("source", action.Kind);
        Assert.Equal("gone", action.Name);
        Assert.Equal("deleted", action.Action);
    }

    [Fact]
    public void Replace_mode_deletion_order_is_pipelines_then_tables_reverse_topo_then_sources()
    {
        var catalogSource = CatalogSource("s-gone");
        var catalogPipeline = CatalogPipeline("p-gone");
        var catalogA = CatalogTable("A-gone");
        var catalogB = CatalogTable("B-gone", inputs: ["A-gone"]); // B depends on A

        var actions = ImportPlanner.Plan(new ConfigDocument(), [catalogSource], [catalogPipeline], [catalogA, catalogB], "replace");

        Assert.Equal(4, actions.Count);
        Assert.True(actions.All(a => a.Action == "deleted"));

        var kinds = actions.Select(a => a.Kind).ToList();
        Assert.Equal("pipeline", kinds[0]);
        Assert.Equal("source", kinds[^1]);

        // Tables: dependent (B) deleted before its dependency (A) — reverse of creation topo order.
        var tableNames = actions.Where(a => a.Kind == "table").Select(a => a.Name).ToList();
        Assert.Equal(["B-gone", "A-gone"], tableNames);
    }

    [Fact]
    public void Replace_mode_still_reports_created_updated_skipped_for_entities_present_in_the_doc()
    {
        var catalogSource = CatalogSource("keep-me");
        var doc = new ConfigDocument { Sources = [CatalogSource("keep-me"), CatalogSource("brand-new")] };

        var actions = ImportPlanner.Plan(doc, [catalogSource], [], [], "replace");

        Assert.Equal(2, actions.Count);
        Assert.Equal("skipped", actions.Single(a => a.Name == "keep-me").Action);
        Assert.Equal("created", actions.Single(a => a.Name == "brand-new").Action);
    }
}
