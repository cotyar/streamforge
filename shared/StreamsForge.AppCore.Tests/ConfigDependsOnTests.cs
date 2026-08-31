using StreamsForge.Abstractions;
using StreamsForge.AppCore.Config;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>
/// Plan 016 wave 3-B — carrying <see cref="EntityPin"/> pins through
/// <see cref="ConfigSerializer.FromCatalog"/>/<c>ToConfigPipeline</c>/<c>ToConfigTable</c>, the JSON
/// round-trip, and the consequence documented on those two mapping methods: because they feed
/// <c>CatalogRevisions.PipelineCanonicalText</c>/<c>TableCanonicalText</c>, editing a pin is now a
/// DEFINITION change — it plans as "updated" and (per <c>CatalogRevisionsTests</c>) bumps
/// <c>Revision</c>. <c>ConfigDocument.DependsOn</c>/<c>ConfigPipeline.DependsOn</c>/
/// <c>ConfigTable.DependsOn</c> themselves were pre-built by the orchestrator (819db76) — this file is
/// wave 3-B's own coverage of what plugs into them.
/// </summary>
public class ConfigDependsOnTests
{
    private static EntityPin SourcePin(string name, long schemaRevision) =>
        new() { Kind = "source", Name = name, SchemaRevision = schemaRevision };

    // ------------------------------------------------------------------
    // FromCatalog / ToConfigPipeline / ToConfigTable carry DependsOn.
    // ------------------------------------------------------------------

    [Fact]
    public void FromCatalogExportsPipelineDependsOn()
    {
        var pipeline = new PipelineDefinition
        {
            Id = "p1",
            Name = "p",
            Sql = "SELECT 1",
            DependsOn = [SourcePin("trades", 3)],
        };

        var doc = ConfigSerializer.FromCatalog([], [pipeline], [], includeSecrets: true);

        var pin = Assert.Single(doc.Pipelines[0].DependsOn);
        Assert.Equal("source", pin.Kind);
        Assert.Equal("trades", pin.Name);
        Assert.Equal(3, pin.SchemaRevision);
    }

    [Fact]
    public void FromCatalogExportsTableDependsOn()
    {
        var table = new TableDefinition
        {
            Id = "t1",
            Name = "t",
            Sql = "SELECT 1",
            DependsOn = [new EntityPin { Kind = "table", Name = "positions", SchemaRevision = 2 }],
        };

        var doc = ConfigSerializer.FromCatalog([], [], [table], includeSecrets: true);

        var pin = Assert.Single(doc.Tables[0].DependsOn);
        Assert.Equal("table", pin.Kind);
        Assert.Equal("positions", pin.Name);
        Assert.Equal(2, pin.SchemaRevision);
    }

    [Fact]
    public void APipelineWithNoPinsExportsNoDependsOnField()
    {
        var pipeline = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1" };
        var doc = ConfigSerializer.FromCatalog([], [pipeline], [], includeSecrets: true);

        Assert.Empty(doc.Pipelines[0].DependsOn);
        Assert.DoesNotContain("dependsOn", ConfigSerializer.ToCanonicalJson(doc));
    }

    // ------------------------------------------------------------------
    // JSON round-trip.
    // ------------------------------------------------------------------

    [Fact]
    public void DependsOnSurvivesTheCanonicalJsonRoundTrip()
    {
        var doc = new ConfigDocument
        {
            Pipelines = [new ConfigPipeline { Name = "p", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 5)] }],
            Tables = [new ConfigTable { Name = "t", Sql = "SELECT 1", DependsOn = [new EntityPin { Kind = "table", Name = "u", SchemaRevision = 7 }] }],
        };

        var json = ConfigSerializer.ToCanonicalJson(doc);
        var (parsed, diagnostics) = ConfigSerializer.Parse(json);

        Assert.Empty(diagnostics);
        Assert.NotNull(parsed);

        var pipelinePin = Assert.Single(parsed!.Pipelines[0].DependsOn);
        Assert.Equal("source", pipelinePin.Kind);
        Assert.Equal("trades", pipelinePin.Name);
        Assert.Equal(5, pipelinePin.SchemaRevision);

        var tablePin = Assert.Single(parsed.Tables[0].DependsOn);
        Assert.Equal("table", tablePin.Kind);
        Assert.Equal("u", tablePin.Name);
        Assert.Equal(7, tablePin.SchemaRevision);
    }

    [Fact]
    public void ADocumentWithNoPinsParsesToAnEmptyDependsOnList()
    {
        // NormalizeCollections' explicit-null -> [] rule, the same one Sinks/Tags already get.
        var (parsed, _) = ConfigSerializer.Parse("""{ "version": 1, "pipelines": [ { "name": "p", "sql": "SELECT 1", "running": false, "dependsOn": null } ] }""");
        Assert.NotNull(parsed);
        Assert.NotNull(parsed!.Pipelines[0].DependsOn);
        Assert.Empty(parsed.Pipelines[0].DependsOn);
    }

    // ------------------------------------------------------------------
    // The consequence: a pin is part of the definition, so editing one bumps Revision.
    // ------------------------------------------------------------------

    [Fact]
    public void IdenticalPinListRoundTripsAsSkippedForAPipeline()
    {
        var stored = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 1)] };
        var doc = new ConfigDocument { Pipelines = [new ConfigPipeline { Name = "p", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 1)] }] };

        Assert.Equal("skipped", Assert.Single(ImportPlanner.Plan(doc, [], [stored], [], "merge")).Action);
    }

    [Fact]
    public void IdenticalPinListRoundTripsAsSkippedForATable()
    {
        var stored = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 1)] };
        var doc = new ConfigDocument { Tables = [new ConfigTable { Name = "t", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 1)] }] };

        Assert.Equal("skipped", Assert.Single(ImportPlanner.Plan(doc, [], [], [stored], "merge")).Action);
    }

    [Fact]
    public void EditingAPinsSchemaRevisionPlansAsUpdated()
    {
        var stored = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 1)] };
        var doc = new ConfigDocument { Pipelines = [new ConfigPipeline { Name = "p", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 2)] }] };

        Assert.Equal("updated", Assert.Single(ImportPlanner.Plan(doc, [], [stored], [], "merge")).Action);
    }

    [Fact]
    public void AddingAPinToAPreviouslyUnpinnedPipelinePlansAsUpdated()
    {
        var stored = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1" }; // no pins
        var doc = new ConfigDocument { Pipelines = [new ConfigPipeline { Name = "p", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 1)] }] };

        Assert.Equal("updated", Assert.Single(ImportPlanner.Plan(doc, [], [stored], [], "merge")).Action);
    }

    [Fact]
    public void RemovingAPinPlansAsUpdated()
    {
        var stored = new TableDefinition { Id = "t1", Name = "t", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 1)] };
        var doc = new ConfigDocument { Tables = [new ConfigTable { Name = "t", Sql = "SELECT 1" }] }; // no pins

        Assert.Equal("updated", Assert.Single(ImportPlanner.Plan(doc, [], [], [stored], "merge")).Action);
    }

    /// <summary>The behaviour change stated as a fact rather than a claim: the SAME predicate ImportPlanner
    /// uses to say "updated" is what the registry bumps Revision on (CatalogRevisions.BumpPipeline), so a
    /// pin edit that plans as "updated" provably also bumps — and a round-trip that plans as "skipped"
    /// provably does not, which is the property CatalogRevisionsTests pins for every other field.</summary>
    [Fact]
    public void APinEditThatPlansAsUpdatedAlsoBumpsRevision()
    {
        var stored = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", Revision = 4, DependsOn = [SourcePin("trades", 1)] };
        var incoming = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 2)] };

        CatalogRecordMerge.CarryServerOwnedFields(stored, incoming, nowMs: 1);
        CatalogRevisions.BumpPipeline(stored, incoming);

        Assert.Equal(5, incoming.Revision);
    }

    [Fact]
    public void AnUnpinnedRoundTripDoesNotBumpRevision()
    {
        var stored = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", Revision = 4, DependsOn = [SourcePin("trades", 1)] };
        var incoming = new PipelineDefinition { Id = "p1", Name = "p", Sql = "SELECT 1", DependsOn = [SourcePin("trades", 1)] };

        CatalogRecordMerge.CarryServerOwnedFields(stored, incoming, nowMs: 1);
        CatalogRevisions.BumpPipeline(stored, incoming);

        Assert.Equal(4, incoming.Revision);
    }
}
