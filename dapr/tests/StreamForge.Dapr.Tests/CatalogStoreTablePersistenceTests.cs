using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Catalog;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 008 (per-table durability policy): unit tests for <see cref="CatalogStore"/>'s handling of
/// <see cref="TableDefinition.Persistence"/>/<see cref="TableDefinition.FlushMs"/> — FlushMs validation
/// (mirroring the existing Parallelism validation style, see <c>CatalogStoreTests.
/// CreateTableAsync_ParallelismOtherThanOne_Throws</c>), that <c>UpdateTableAsync</c> actually copies both
/// fields onto the persisted definition, and that changing either one restarts an already-Running table —
/// same "changing it restarts the table" contract <see cref="TableDefinition.Parallelism"/> already has,
/// per <see cref="TableDefinition.FlushMs"/>'s own doc comment. New file (not an edit to the existing
/// <c>CatalogStoreTests.cs</c>) per this wave's file-ownership rule.
/// </summary>
public class CatalogStoreTablePersistenceTests
{
    private static (CatalogState State, CatalogStore Store, TestLifecycleOrchestrator Orchestrator) NewStore()
    {
        var state = new CatalogState();
        var orchestrator = new TestLifecycleOrchestrator();
        var store = new CatalogStore(state, orchestrator);
        return (state, store, orchestrator);
    }

    /// <summary>Builds an UPDATE payload as a genuinely separate object from <paramref name="src"/> (rather
    /// than mutating and re-passing the very same instance <c>CreateTableAsync</c> returned) — that
    /// returned instance is the SAME reference <c>CatalogStore</c> stores in <c>state.Tables</c>, so
    /// mutating it directly before calling <c>UpdateTableAsync</c> would make its own
    /// "existing != def"-style change-detection compare an object against itself and trivially see no
    /// diff, defeating exactly the restart-detection these tests exist to verify.</summary>
    private static TableDefinition UpdatePayload(TableDefinition src) => new()
    {
        Id = src.Id,
        Name = src.Name,
        Description = src.Description,
        Sql = src.Sql,
        SearchEnabled = src.SearchEnabled,
        SearchMode = src.SearchMode,
        Persistence = src.Persistence,
        FlushMs = src.FlushMs,
    };

    // ------------------------------------------------------------------
    // FlushMs validation.
    // ------------------------------------------------------------------

    [Fact]
    public async Task CreateTableAsync_NegativeFlushMs_Throws()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var def = new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades", FlushMs = -1 };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.CreateTableAsync(def));
        Assert.Contains("FlushMs", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(60_000)]
    public async Task CreateTableAsync_NonNegativeFlushMs_Succeeds(int flushMs)
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var def = new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades", FlushMs = flushMs };

        var created = await store.CreateTableAsync(def);

        Assert.Equal(flushMs, created.FlushMs);
    }

    [Fact]
    public async Task UpdateTableAsync_NegativeFlushMs_Throws()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var created = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });

        var payload = UpdatePayload(created);
        payload.FlushMs = -5;
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.UpdateTableAsync(payload));
    }

    // ------------------------------------------------------------------
    // UpdateTableAsync copies Persistence/FlushMs onto the stored definition.
    // ------------------------------------------------------------------

    [Fact]
    public async Task UpdateTableAsync_ChangesPersistenceMode_PersistsTheNewValue()
    {
        var (state, store, _) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var created = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });
        Assert.Equal(TablePersistenceMode.Batched, created.Persistence); // sanity: default

        var payload = UpdatePayload(created);
        payload.Persistence = TablePersistenceMode.MemoryOnly;
        payload.FlushMs = 250;
        var updated = await store.UpdateTableAsync(payload);

        Assert.NotNull(updated);
        Assert.Equal(TablePersistenceMode.MemoryOnly, updated!.Persistence);
        Assert.Equal(250, updated.FlushMs);
    }

    // ------------------------------------------------------------------
    // Persistence-mode/FlushMs change restarts an already-Running table — same contract as Parallelism.
    // ------------------------------------------------------------------

    [Fact]
    public async Task UpdateTableAsync_PersistenceModeChangedWhileRunning_StopsAndRestartsTheTable()
    {
        var (state, store, orchestrator) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var created = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });
        await store.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        orchestrator.Calls.Clear();

        var payload = UpdatePayload(created);
        payload.Persistence = TablePersistenceMode.FireAndForget;
        var updated = await store.UpdateTableAsync(payload);

        Assert.Equal(PipelineStatus.Running, updated!.Status);
        Assert.Contains($"StopTable:{created.Name}", orchestrator.Calls);
        Assert.Contains(orchestrator.Calls, c => c.StartsWith($"StartTable:{created.Name}:"));
    }

    [Fact]
    public async Task UpdateTableAsync_FlushMsChangedWhileRunning_StopsAndRestartsTheTable()
    {
        var (state, store, orchestrator) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var created = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });
        await store.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        orchestrator.Calls.Clear();

        var payload = UpdatePayload(created);
        payload.FlushMs = 5000;
        var updated = await store.UpdateTableAsync(payload);

        Assert.Equal(PipelineStatus.Running, updated!.Status);
        Assert.Contains($"StopTable:{created.Name}", orchestrator.Calls);
        Assert.Contains(orchestrator.Calls, c => c.StartsWith($"StartTable:{created.Name}:"));
    }

    [Fact]
    public async Task UpdateTableAsync_PersistenceUnchangedWhileRunning_DoesNotRestart()
    {
        var (state, store, orchestrator) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var created = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });
        await store.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        orchestrator.Calls.Clear();

        var payload = UpdatePayload(created);
        payload.Description = "unrelated metadata edit only";
        var updated = await store.UpdateTableAsync(payload);

        Assert.Equal(PipelineStatus.Running, updated!.Status);
        Assert.DoesNotContain($"StopTable:{created.Name}", orchestrator.Calls);
    }

    [Fact]
    public async Task UpdateTableAsync_PersistenceModeChangedWhileStopped_DoesNotRestart()
    {
        var (state, store, orchestrator) = NewStore();
        state.Sources.Add(new SourceDefinition { Name = "trades", Fields = [new FieldDef("qty", FieldType.Long)] });
        var created = await store.CreateTableAsync(new TableDefinition { Name = "positions", Sql = "SELECT qty FROM trades" });
        orchestrator.Calls.Clear();

        var payload = UpdatePayload(created);
        payload.Persistence = TablePersistenceMode.MemoryOnly;
        var updated = await store.UpdateTableAsync(payload);

        Assert.Equal(PipelineStatus.Stopped, updated!.Status);
        Assert.DoesNotContain(orchestrator.Calls, c => c.StartsWith("StopTable") || c.StartsWith("StartTable"));
    }
}
