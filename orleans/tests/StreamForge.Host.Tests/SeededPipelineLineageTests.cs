using Orleans.TestingHost;
using StreamForge.Abstractions;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 011 Wave A: the acceptance criterion plan 008 claimed
/// (plans/008-rest-joins-setops-ingress-lineage.md:87 — "graph matches the seeded catalog on both
/// flavors") but never actually tested. Root cause (see plan 011's Wave A section):
/// RegistryGrain.EnsureInitializedAsync added SeedCatalog.Pipelines() raw — unlike tables, which get
/// compiled and get StreamInputs/TableInputs populated right there — so every seeded pipeline shipped
/// with an empty SourceNames and drew zero lineage edges on the Lineage page, even though two of the
/// seven seeded pipelines (SeedCatalog.cs's "Order bursts (session)" and "Unfilled orders (LEFT JOIN)")
/// read from the "orders" source. This test proves the EnsureInitializedAsync backfill (added right after
/// the three seed blocks) actually populates SourceNames for the freshly-seeded catalog, using the SAME
/// TestCluster/silo-config pattern as PipelineLineageTests.
/// </summary>
public sealed class SeededPipelineLineageTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private IRegistryGrain _registry = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<MetadataTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<MetadataTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await _registry.EnsureInitializedAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task SeededCatalog_BothOrdersPipelines_GetNonEmptySourceNames()
    {
        var pipelines = await _registry.GetPipelinesAsync();

        var orderBursts = pipelines.Single(p => p.Name == "Order bursts (session)");
        Assert.Equal(["orders"], orderBursts.SourceNames);

        // Also reads "trades" via its LEFT JOIN — both leaf sources must be present so the lineage canvas
        // draws both incoming edges for this node.
        var unfilledOrders = pipelines.Single(p => p.Name == "Unfilled orders (LEFT JOIN)");
        Assert.Equal(["orders", "trades"], unfilledOrders.SourceNames);
    }

    [Fact]
    public async Task SeededCatalog_EveryCompilablePipeline_HasNonEmptySourceNames()
    {
        var pipelines = await _registry.GetPipelinesAsync();

        // All seven seeded pipelines (SeedCatalog.Pipelines()) compile against the seeded sources — none
        // of them should be left with an empty SourceNames now that the backfill runs at init.
        Assert.All(pipelines, p => Assert.NotEmpty(p.SourceNames));
    }
}
