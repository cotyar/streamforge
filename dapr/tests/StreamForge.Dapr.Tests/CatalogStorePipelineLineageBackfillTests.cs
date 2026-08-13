using StreamForge.AppCore;
using StreamForge.Dapr.Host.Catalog;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 011 Wave A: the Dapr-flavor counterpart of
/// orleans/tests/StreamForge.Host.Tests/PipelineLineageBackfillTests.cs. CatalogStore.EnsureInitialized's
/// backfill must repair pipelines that were durably persisted with an empty SourceNames BEFORE this
/// backfill existed — not just freshly-seeded ones (SeededCatalogStorePipelineLineageTests covers that
/// path). Unlike the Orleans side, CatalogStore is a plain class over an in-memory CatalogState with no
/// persistence concerns of its own (RegistryActor owns loading/saving via Dapr's StateManager — see
/// CatalogStore's class doc) — so "restored from a persisted catalog" is reproduced directly: a
/// CatalogState is hand-built in exactly the shape a pre-Wave-A install would have persisted (Sources
/// seeded, Pipelines seeded RAW — SeedCatalog.Pipelines() unmodified, so every SourceNames is empty
/// exactly like the confirmed bug), fed straight into a fresh CatalogStore, and EnsureInitialized() must
/// repair every pipeline's SourceNames without re-seeding (Pipelines.Count is already non-zero, so the
/// seed block itself is a no-op — only the new backfill loop, driven off SourceNames.Count == 0, can fix
/// it).
/// </summary>
public class CatalogStorePipelineLineageBackfillTests
{
    [Fact]
    public void EnsureInitialized_RestoredCatalogWithEmptySourceNames_RepairsWithoutReseeding()
    {
        // Hand-build a CatalogState in the exact pre-Wave-A persisted shape: sources present, pipelines
        // present but RAW (SeedCatalog.Pipelines() itself never populates SourceNames).
        var state = new CatalogState();
        state.Sources.AddRange(SeedCatalog.Sources());
        state.Pipelines.AddRange(SeedCatalog.Pipelines());
        Assert.All(state.Pipelines, p => Assert.Empty(p.SourceNames)); // sanity: reproduces the bug shape

        var orderBurstsId = state.Pipelines.Single(p => p.Name == "Order bursts (session)").Id;
        var unfilledOrdersId = state.Pipelines.Single(p => p.Name == "Unfilled orders (LEFT JOIN)").Id;

        var store = new CatalogStore(state, new TestLifecycleOrchestrator());
        var dirty = store.EnsureInitialized();

        Assert.True(dirty);
        Assert.Equal(7, state.Pipelines.Count); // no re-seed happened — still exactly the original 7

        var orderBursts = state.Pipelines.Single(p => p.Id == orderBurstsId);
        Assert.Equal(["orders"], orderBursts.SourceNames);

        var unfilledOrders = state.Pipelines.Single(p => p.Id == unfilledOrdersId);
        Assert.Equal(["orders", "trades"], unfilledOrders.SourceNames);
    }

    [Fact]
    public void EnsureInitialized_CatalogWithNoEmptySourceNames_ReportsNotDirtyForPipelines()
    {
        // A catalog that's already fully backfilled (e.g. every pipeline created/updated through the
        // normal API, which always populates SourceNames) must not be reported dirty on a second
        // EnsureInitialized call purely because of the pipeline loop — Sources/Pipelines/Tables are all
        // already non-empty, so nothing in this method should have anything left to do.
        var state = new CatalogState();
        state.Sources.AddRange(SeedCatalog.Sources());
        state.Pipelines.AddRange(SeedCatalog.Pipelines());
        state.Tables.AddRange(SeedCatalog.Tables());
        var store = new CatalogStore(state, new TestLifecycleOrchestrator());
        store.EnsureInitialized(); // first call backfills pipelines (and compiles the raw table seeds)

        var dirtyAgain = store.EnsureInitialized();

        Assert.False(dirtyAgain);
    }
}
