using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Catalog;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 009 B2: <see cref="CatalogStore.UpdatePipelineAsync"/>/<see cref="CatalogStore.UpdateTableAsync"/>
/// previously copied a fixed field list onto the stored entity that did not include <c>Sinks</c> — a PUT
/// that changed ONLY Sinks was silently dropped. Pins the fix (both create, which was never affected
/// since it persists the whole submitted object, and update, which needed the explicit copy added).
/// </summary>
public class CatalogStoreSinksTests
{
    private static (CatalogState State, CatalogStore Store) NewStore()
    {
        var state = new CatalogState();
        var orchestrator = new TestLifecycleOrchestrator();
        var store = new CatalogStore(state, orchestrator);
        return (state, store);
    }

    private static SinkSpec NatsSink(string subject = "sf.out") => new()
    {
        Kind = SinkKinds.Nats,
        Enabled = true,
        Nats = new NatsPubConfig { Url = "nats://localhost:4222", Subject = subject },
    };

    [Fact]
    public async Task CreatePipelineAsync_PersistsSinks()
    {
        var (_, store) = NewStore();

        var created = await store.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "p1",
            Sql = "SELECT 1",
            Sinks = [NatsSink()],
        });

        Assert.Single(created.Sinks);
        Assert.Equal("sf.out", created.Sinks[0].Nats!.Subject);
    }

    [Fact]
    public async Task UpdatePipelineAsync_ChangingOnlySinks_PersistsTheNewSinksList()
    {
        var (_, store) = NewStore();
        var created = await store.CreatePipelineAsync(new PipelineDefinition { Name = "p1", Sql = "SELECT 1" });
        Assert.Empty(created.Sinks);

        created.Sinks = [NatsSink("sf.pipeline.p1")];
        var updated = await store.UpdatePipelineAsync(created);

        Assert.NotNull(updated);
        Assert.Single(updated!.Sinks);
        Assert.Equal("sf.pipeline.p1", updated.Sinks[0].Nats!.Subject);
    }

    [Fact]
    public async Task UpdatePipelineAsync_ClearingSinks_RemovesThem()
    {
        var (_, store) = NewStore();
        var created = await store.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "p1",
            Sql = "SELECT 1",
            Sinks = [NatsSink()],
        });

        created.Sinks = [];
        var updated = await store.UpdatePipelineAsync(created);

        Assert.NotNull(updated);
        Assert.Empty(updated!.Sinks);
    }

    [Fact]
    public async Task CreateTableAsync_PersistsSinks()
    {
        var (_, store) = NewStore();

        var created = await store.CreateTableAsync(new TableDefinition
        {
            Name = "t1",
            Sql = "SELECT 1",
            Sinks = [NatsSink()],
        });

        Assert.Single(created.Sinks);
    }

    [Fact]
    public async Task UpdateTableAsync_ChangingOnlySinks_PersistsTheNewSinksList()
    {
        var (_, store) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition { Name = "t1", Sql = "SELECT 1" });
        Assert.Empty(created.Sinks);

        created.Sinks = [NatsSink("sf.table.t1")];
        var updated = await store.UpdateTableAsync(created);

        Assert.NotNull(updated);
        Assert.Single(updated!.Sinks);
        Assert.Equal("sf.table.t1", updated.Sinks[0].Nats!.Subject);
    }
}
