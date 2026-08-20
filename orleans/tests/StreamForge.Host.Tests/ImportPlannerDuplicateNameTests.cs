using StreamForge.Abstractions;
using StreamForge.AppCore.Config;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 016 wave 1-C — the live crash, and the rule that replaced it.
///
/// <para><b>What was broken.</b> <see cref="ImportPlanner.Plan"/> built its three catalog lookups with
/// <c>ToDictionary</c>, which THROWS on a duplicate key. Pipeline names were never enforced unique, so a
/// duplicate-name catalog was a state the platform itself produced — and every <c>POST
/// /api/config/import</c> against one answered <b>500</b>, from
/// <c>ConfigImportService.FindUnentitledChangesAsync</c>, before the entitlement pass even ran.
/// Reproduced against a live instance: two pipelines named <c>dupe</c> both created with 201, the export
/// round-tripped, and re-importing it returned 500 with "An item with the same key has already been
/// added. Key: dupe" pointing at ImportPlanner.cs line 34.</para>
///
/// <para><b>Why the planner and not just the write path.</b> The write path now refuses NEW duplicates
/// (<c>RegistryGrain.ValidateUniquePipelineName</c> / <c>CatalogStore</c>), but catalogs written before
/// that guard still exist and must stay importable — importing one is exactly how somebody would fix it.
/// A refusal that fires on the tool you would use to repair the state is not a fix.</para>
///
/// <para>New file: <c>ImportPlannerTests.cs</c> is pinned by this wave's brief and may not be touched.
/// Every expectation in it still holds unmodified — the diagnostic only appears when a name is actually
/// duplicated, so a clean catalog plans exactly as many actions as before.</para>
/// </summary>
public class ImportPlannerDuplicateNameTests
{
    private static PipelineDefinition Pipeline(string id, string name, string sql = "SELECT 1") => new()
    {
        Id = id,
        Name = name,
        Sql = sql,
        Status = PipelineStatus.Stopped,
    };

    private static ConfigPipeline DocPipeline(string name, string sql = "SELECT 1") => new()
    {
        Name = name,
        Sql = sql,
        Running = false,
    };

    // ------------------------------------------------------------------
    // The regression itself.
    // ------------------------------------------------------------------

    /// <summary>The exact shape that produced the 500. Before the fix this line threw
    /// <see cref="ArgumentException"/>; the assertion is simply that a plan comes back.</summary>
    [Fact]
    public void Plan_survives_a_duplicate_pipeline_name_catalog()
    {
        var catalog = new List<PipelineDefinition> { Pipeline("id-first", "dupe"), Pipeline("id-second", "dupe") };

        var actions = ImportPlanner.Plan(new ConfigDocument { Pipelines = [DocPipeline("dupe")] }, [], catalog, [], "merge");

        var action = Assert.Single(actions);
        Assert.Equal("pipeline", action.Kind);
        Assert.Equal("dupe", action.Name);
    }

    /// <summary>First-wins, and it is the SAME first that <c>ConfigImportService.FirstByName</c> picks —
    /// the plan and the apply loop that follows it have to be talking about the same entity or the report
    /// describes a pipeline other than the one that got written. Proved by making the two duplicates
    /// differ: the document matches the FIRST, so the answer is "skipped", not "updated".</summary>
    [Fact]
    public void Plan_compares_against_the_first_entity_with_the_duplicated_name()
    {
        var catalog = new List<PipelineDefinition>
        {
            Pipeline("id-first", "dupe", "SELECT 1"),
            Pipeline("id-second", "dupe", "SELECT 999"),
        };

        var actions = ImportPlanner.Plan(
            new ConfigDocument { Pipelines = [DocPipeline("dupe", "SELECT 1")] }, [], catalog, [], "merge");

        Assert.Equal("skipped", Assert.Single(actions).Action);
    }

    /// <summary>Silent first-wins would be the second bug. The diagnostic names the duplicated name, how
    /// many entities share it, and which id was actually used.</summary>
    [Fact]
    public void Duplicate_pipeline_name_is_diagnosed_on_the_planned_action()
    {
        var catalog = new List<PipelineDefinition> { Pipeline("id-first", "dupe"), Pipeline("id-second", "dupe") };

        var action = Assert.Single(ImportPlanner.Plan(
            new ConfigDocument { Pipelines = [DocPipeline("dupe", "SELECT 2")] }, [], catalog, [], "merge"));

        var diagnostic = Assert.Single(action.Diagnostics);
        Assert.Contains("duplicate pipeline name 'dupe'", diagnostic, StringComparison.Ordinal);
        Assert.Contains("id-first", diagnostic, StringComparison.Ordinal);
        Assert.Contains("id-second", diagnostic, StringComparison.Ordinal);
    }

    /// <summary>Replace mode plans a deletion per catalog entity, so a duplicated name produces two —
    /// both carrying the diagnostic, because "deleted" against an ambiguous name is precisely when an
    /// operator wants to be told which entity the name resolves to.</summary>
    [Fact]
    public void Replace_mode_deletion_of_a_duplicated_name_carries_the_diagnostic()
    {
        var catalog = new List<PipelineDefinition> { Pipeline("id-first", "dupe"), Pipeline("id-second", "dupe") };

        var actions = ImportPlanner.Plan(new ConfigDocument(), [], catalog, [], "replace");

        Assert.Equal(2, actions.Count);
        Assert.All(actions, a =>
        {
            Assert.Equal("deleted", a.Action);
            Assert.Contains(a.Diagnostics, d => d.Contains("duplicate pipeline name 'dupe'", StringComparison.Ordinal));
        });
    }

    /// <summary>The cost on the overwhelmingly common case: nothing. A clean catalog gets no extra
    /// action and no extra diagnostic — which is what keeps every <c>Assert.Single(actions)</c> in
    /// <c>ImportPlannerTests</c> green without editing it.</summary>
    [Fact]
    public void A_catalog_without_duplicates_gains_no_diagnostic()
    {
        var catalog = new List<PipelineDefinition> { Pipeline("id-a", "a"), Pipeline("id-b", "b") };

        var actions = ImportPlanner.Plan(
            new ConfigDocument { Pipelines = [DocPipeline("a", "SELECT 2")] }, [], catalog, [], "merge");

        Assert.Empty(Assert.Single(actions).Diagnostics);
    }

    /// <summary>Duplicate SOURCE and TABLE names are impossible (both registries enforce uniqueness
    /// across the pair), but the planner is the last thing standing between a hand-edited state file and
    /// a 500, so it survives those too rather than trusting the guard upstream of it.</summary>
    [Fact]
    public void Plan_survives_duplicate_source_and_table_names_too()
    {
        var sources = new List<SourceDefinition> { new() { Name = "s" }, new() { Name = "s" } };
        var tables = new List<TableDefinition>
        {
            new() { Id = "t1", Name = "t", Sql = "SELECT 1" },
            new() { Id = "t2", Name = "t", Sql = "SELECT 1" },
        };

        var actions = ImportPlanner.Plan(new ConfigDocument(), sources, [], tables, "merge");

        Assert.Empty(actions); // merge mode, empty document — the point is that it returned at all.
    }

    // ------------------------------------------------------------------
    // CatalogWarnings — where wave 5 reads this from.
    // ------------------------------------------------------------------

    [Fact]
    public void CatalogWarnings_are_empty_for_an_ordinary_catalog()
    {
        Assert.Empty(CatalogWarnings.Compute([Pipeline("id-a", "a"), Pipeline("id-b", "b")]));
    }

    /// <summary>One warning per duplicated name — the list
    /// <c>InstanceInfo.CatalogWarnings</c> exists to carry (wave 5 hangs it off
    /// <c>GET /api/meta/instance</c>).</summary>
    [Fact]
    public void CatalogWarnings_name_each_duplicated_pipeline_name_once()
    {
        var warnings = CatalogWarnings.Compute(
        [
            Pipeline("id-1", "dupe"),
            Pipeline("id-2", "dupe"),
            Pipeline("id-3", "dupe"),
            Pipeline("id-4", "fine"),
        ]);

        var warning = Assert.Single(warnings);
        Assert.Contains("duplicate pipeline name 'dupe'", warning, StringComparison.Ordinal);
        Assert.Contains("3 pipelines share it", warning, StringComparison.Ordinal);
        Assert.DoesNotContain("fine", warning, StringComparison.Ordinal);
    }
}
