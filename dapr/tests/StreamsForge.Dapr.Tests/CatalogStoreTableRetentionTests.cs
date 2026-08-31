using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Catalog;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 011 wave C2: <see cref="CatalogStore"/>'s half of the opt-in row retention policy — the 409-style
/// guards (mirroring <c>RegistryGrain.ValidateRetention</c> rule for rule), that the two fields survive an
/// update, and that changing a bound restarts a Running table (the policy is installed on the executor at
/// StartAsync and nowhere else, so it could not otherwise take effect until the next manual stop/start).
///
/// New file rather than an edit to <c>CatalogStoreTests.cs</c>, per this wave's file-ownership rule.
/// </summary>
public class CatalogStoreTableRetentionTests
{
    private static (CatalogState State, CatalogStore Store, TestLifecycleOrchestrator Orchestrator) NewStore()
    {
        var state = new CatalogState();
        var orchestrator = new TestLifecycleOrchestrator();
        state.Sources.Add(new SourceDefinition
        {
            Name = "order_events",
            Fields = [new FieldDef("order_id", FieldType.String), new FieldDef("stage", FieldType.String)],
        });
        return (state, new CatalogStore(state, orchestrator), orchestrator);
    }

    /// <summary>See CatalogStoreTablePersistenceTests' identical helper: the UPDATE payload must be a
    /// genuinely separate object, or the store's own change detection compares a reference against
    /// itself.</summary>
    private static TableDefinition UpdatePayload(TableDefinition src, int maxRows, long ttlMs) => new()
    {
        Id = src.Id,
        Name = src.Name,
        Description = src.Description,
        Sql = src.Sql,
        SearchEnabled = src.SearchEnabled,
        SearchMode = src.SearchMode,
        RetentionMaxRows = maxRows,
        RetentionTtlMs = ttlMs,
    };

    private const string LatestBySql = "SELECT order_id, stage FROM order_events LATEST BY (order_id)";

    // ------------------------------------------------------------------
    // Guards.
    // ------------------------------------------------------------------

    [Theory]
    [InlineData(-1, 0)]
    [InlineData(0, -5)]
    public async Task CreateTableAsync_NegativeBounds_Throws(int maxRows, long ttlMs)
    {
        var (_, store, _) = NewStore();
        var def = new TableDefinition { Name = "order_states", Sql = LatestBySql, RetentionMaxRows = maxRows, RetentionTtlMs = ttlMs };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.CreateTableAsync(def));
        Assert.Contains("Retention bounds", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateTableAsync_RetentionOnAnAggregateTable_Throws()
    {
        var (_, store, _) = NewStore();
        var def = new TableDefinition
        {
            Name = "per_order",
            Sql = "SELECT order_id, COUNT(*) AS n FROM order_events GROUP BY order_id",
            RetentionMaxRows = 100,
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.CreateTableAsync(def));
        Assert.Contains("not supported", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CreateTableAsync_RetentionOnUncompilableSql_IsNotBlocked()
    {
        // Draft-friendly, exactly like ValidateHistoryConfig: bad SQL is saved with diagnostics, and the
        // shape check simply has nothing to check yet.
        var (_, store, _) = NewStore();
        var def = new TableDefinition { Name = "draft", Sql = "SELECT nope FROM nowhere", RetentionMaxRows = 10 };

        var created = await store.CreateTableAsync(def);
        Assert.Equal(10, created.RetentionMaxRows);
    }

    [Fact]
    public async Task CreateTableAsync_RetentionOffIsAlwaysAccepted_WhateverTheShape()
    {
        // The default-off property that lets this land without touching a single existing test.
        var (_, store, _) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition
        {
            Name = "per_order",
            Sql = "SELECT order_id, COUNT(*) AS n FROM order_events GROUP BY order_id",
        });

        Assert.Equal(0, created.RetentionMaxRows);
        Assert.Equal(0, created.RetentionTtlMs);
    }

    // ------------------------------------------------------------------
    // Round trip + restart.
    // ------------------------------------------------------------------

    [Fact]
    public async Task UpdateTableAsync_CopiesBothBoundsOntoThePersistedDefinition()
    {
        var (state, store, _) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition { Name = "order_states", Sql = LatestBySql });

        await store.UpdateTableAsync(UpdatePayload(created, maxRows: 500, ttlMs: 30_000));

        var stored = state.Tables.Single(t => t.Id == created.Id);
        Assert.Equal(500, stored.RetentionMaxRows);
        Assert.Equal(30_000, stored.RetentionTtlMs);
    }

    [Fact]
    public async Task UpdateTableAsync_ChangingABound_RestartsARunningTable()
    {
        var (_, store, orchestrator) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition { Name = "order_states", Sql = LatestBySql });
        await store.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        orchestrator.Calls.Clear();

        await store.UpdateTableAsync(UpdatePayload(created, maxRows: 100, ttlMs: 0));

        Assert.Contains($"StopTable:{created.Name}", orchestrator.Calls);
        Assert.Contains(orchestrator.Calls, c => c.StartsWith($"StartTable:{created.Name}:", StringComparison.Ordinal));
    }

    [Fact]
    public async Task UpdateTableAsync_UnchangedBounds_DoNotRestart()
    {
        var (_, store, orchestrator) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition { Name = "order_states", Sql = LatestBySql, RetentionMaxRows = 100 });
        await store.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        orchestrator.Calls.Clear();

        await store.UpdateTableAsync(UpdatePayload(created, maxRows: 100, ttlMs: 0));

        Assert.DoesNotContain($"StopTable:{created.Name}", orchestrator.Calls);
    }
}
