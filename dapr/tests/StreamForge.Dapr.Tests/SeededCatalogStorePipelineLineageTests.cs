using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Catalog;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 011 Wave A: the Dapr-flavor counterpart of
/// orleans/tests/StreamForge.Host.Tests/SeededPipelineLineageTests.cs. Same root cause as the Orleans
/// side (see plan 011's Wave A section): CatalogStore.EnsureInitialized added SeedCatalog.Pipelines()
/// raw — unlike tables, compiled right there — so every seeded pipeline shipped with an empty
/// SourceNames and drew zero lineage edges, even though two of the seven seeded pipelines ("Order bursts
/// (session)" and "Unfilled orders (LEFT JOIN)") read from the "orders" source. This proves the
/// EnsureInitialized backfill (added right after the three seed blocks) populates SourceNames for the
/// freshly-seeded catalog.
/// </summary>
public class SeededCatalogStorePipelineLineageTests
{
    private static (CatalogState State, CatalogStore Store) NewSeededStore()
    {
        var state = new CatalogState();
        var store = new CatalogStore(state, new TestLifecycleOrchestrator());
        store.EnsureInitialized();
        return (state, store);
    }

    [Fact]
    public void SeededCatalog_BothOrdersPipelines_GetNonEmptySourceNames()
    {
        var (state, _) = NewSeededStore();

        var orderBursts = state.Pipelines.Single(p => p.Name == "Order bursts (session)");
        Assert.Equal(["orders"], orderBursts.SourceNames);

        // Also reads "trades" via its LEFT JOIN — both leaf sources must be present so the lineage canvas
        // draws both incoming edges for this node.
        var unfilledOrders = state.Pipelines.Single(p => p.Name == "Unfilled orders (LEFT JOIN)");
        Assert.Equal(["orders", "trades"], unfilledOrders.SourceNames);
    }

    [Fact]
    public void SeededCatalog_EveryCompilablePipeline_HasNonEmptySourceNames()
    {
        var (state, _) = NewSeededStore();

        // All seven seeded pipelines (SeedCatalog.Pipelines()) compile against the seeded sources — none
        // of them should be left with an empty SourceNames now that the backfill runs at init.
        Assert.All(state.Pipelines, p => Assert.NotEmpty(p.SourceNames));
    }
}
