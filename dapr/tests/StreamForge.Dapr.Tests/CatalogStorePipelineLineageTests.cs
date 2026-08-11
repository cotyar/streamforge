using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Catalog;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 008 W5: PipelineDefinition.SourceNames round-trip against CatalogStore — the Dapr-flavor
/// counterpart of orleans/tests/StreamForge.Host.Tests/PipelineLineageTests.cs. No actor runtime needed
/// (see CatalogStoreTests' class doc) — CatalogStore is a plain class over an in-memory CatalogState.
/// Same coverage: populated on a successful compile at create time, refreshed on update, left empty for
/// SQL that doesn't currently compile (draft-friendly — never blocks create/update on its own).
/// </summary>
public class CatalogStorePipelineLineageTests
{
    private static (CatalogState State, CatalogStore Store) NewSeededStore()
    {
        var state = new CatalogState();
        var store = new CatalogStore(state, new TestLifecycleOrchestrator());
        store.EnsureInitialized(); // seeds "trades"/"quotes"/... so SQL below has real sources.
        return (state, store);
    }

    [Fact]
    public async Task CreatePipelineAsync_CompilingSql_PopulatesSourceNames()
    {
        var (_, store) = NewSeededStore();

        var created = await store.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "lineage_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT symbol FROM trades",
        });

        Assert.Equal(["trades"], created.SourceNames);
    }

    [Fact]
    public async Task CreatePipelineAsync_NonCompilingSql_LeavesSourceNamesEmpty()
    {
        var (_, store) = NewSeededStore();

        var created = await store.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "lineage_bad_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT * FROM nonexistent_stream",
        });

        Assert.Empty(created.SourceNames);
    }

    [Fact]
    public async Task UpdatePipelineAsync_SqlChangedToDifferentSource_RefreshesSourceNames()
    {
        var (_, store) = NewSeededStore();

        var created = await store.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "lineage_upd_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT symbol FROM trades",
        });
        Assert.Equal(["trades"], created.SourceNames);

        created.Sql = "SELECT symbol FROM quotes";
        var updated = await store.UpdatePipelineAsync(created);

        Assert.NotNull(updated);
        Assert.Equal(["quotes"], updated!.SourceNames);
    }

    [Fact]
    public async Task UpdatePipelineAsync_SqlChangedToNonCompiling_ClearsSourceNames()
    {
        var (_, store) = NewSeededStore();

        var created = await store.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "lineage_clear_" + Guid.NewGuid().ToString("n")[..8],
            Sql = "SELECT symbol FROM trades",
        });
        Assert.Equal(["trades"], created.SourceNames);

        created.Sql = "SELECT * FROM nonexistent_stream";
        var updated = await store.UpdatePipelineAsync(created);

        Assert.NotNull(updated);
        Assert.Empty(updated!.SourceNames);
    }
}
