using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Catalog;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 016 wave 2, Dapr flavour. The rules themselves are proven once, for both flavours, in
/// <c>StreamsForge.AppCore.Tests.CatalogRevisionsTests</c> (that project is listed in both solutions);
/// what this file proves is that <see cref="CatalogStore"/> WIRES them in at the same sites
/// <c>RegistryGrain</c> does — which is exactly the kind of thing the two flavours have drifted on
/// before (see <c>CatalogUpdateRoundTripTests</c>' JournalMaxEntries story).
/// </summary>
public class CatalogStoreRevisionTests
{
    private static (CatalogState State, CatalogStore Store) NewStore()
    {
        var state = new CatalogState();
        return (state, new CatalogStore(state, new TestLifecycleOrchestrator()));
    }

    private static SourceDefinition Src(string name, params FieldDef[] fields) => new()
    {
        Name = name,
        EventsPerSecond = 0,
        Enabled = false,
        Fields = [.. fields],
    };

    [Fact]
    public async Task ANewSourceStartsAtRevisionOneNotZero()
    {
        var (state, store) = NewStore();

        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String)));

        var stored = state.Sources.Single(s => s.Name == "s");
        Assert.Equal(1, stored.Revision);
        Assert.Equal(1, stored.SchemaRevision);
    }

    [Fact]
    public async Task AKnobOnlyEditDoesNotBumpSchemaRevision()
    {
        var (state, store) = NewStore();
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String)));

        var knob = Src("s", new FieldDef("symbol", FieldType.String));
        knob.EventsPerSecond = 42;
        await store.UpsertSourceAsync(knob);

        var stored = state.Sources.Single(s => s.Name == "s");
        Assert.Equal(2, stored.Revision);
        Assert.Equal(1, stored.SchemaRevision);
    }

    [Fact]
    public async Task ASchemaEditBumpsBothAndACallerCannotChooseEither()
    {
        var (state, store) = NewStore();
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String)));

        var forged = Src("s", new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long));
        forged.Revision = 5000;
        forged.SchemaRevision = 5000;
        await store.UpsertSourceAsync(forged);

        var stored = state.Sources.Single(s => s.Name == "s");
        Assert.Equal(2, stored.Revision);
        Assert.Equal(2, stored.SchemaRevision);
    }

    [Fact]
    public async Task ReUpsertingAnIdenticalSourceMovesNothing()
    {
        var (state, store) = NewStore();
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String)));
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String)));

        var stored = state.Sources.Single(s => s.Name == "s");
        Assert.Equal(1, stored.Revision);
        Assert.Equal(1, stored.SchemaRevision);
    }

    [Fact]
    public async Task ASourceSchemaChangeRefreshesADependentTablesPersistedOutputFieldsAndFieldNumbers()
    {
        var (state, store) = NewStore();
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)));

        var table = await store.CreateTableAsync(new TableDefinition { Name = "t", Sql = "SELECT symbol, price FROM s" });
        Assert.Equal(FieldType.Double, table.OutputFields.Single(f => f.Name == "price").Type);

        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Long)));

        var stored = state.Tables.Single(t => t.Id == table.Id);
        Assert.Equal(FieldType.Long, stored.OutputFields.Single(f => f.Name == "price").Type);
        Assert.Equal(2, stored.SchemaRevision);
        Assert.True(EntitySchemas.ParseMap(state.FieldNumberMaps[EntitySchemas.TableKey(stored.Id)]).Active.ContainsKey("price"));
    }

    [Fact]
    public async Task TheRefreshReachesTheSecondHopOfATableOverTableChain()
    {
        var (state, store) = NewStore();
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)));

        var up = await store.CreateTableAsync(new TableDefinition { Name = "up", Sql = "SELECT symbol, price FROM s" });
        var down = await store.CreateTableAsync(new TableDefinition { Name = "down", Sql = "SELECT symbol, price FROM up" });
        Assert.Equal(FieldType.Double, down.OutputFields.Single(f => f.Name == "price").Type);

        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Long)));

        Assert.Equal(FieldType.Long, state.Tables.Single(t => t.Id == up.Id).OutputFields.Single(f => f.Name == "price").Type);
        Assert.Equal(FieldType.Long, state.Tables.Single(t => t.Id == down.Id).OutputFields.Single(f => f.Name == "price").Type);
    }

    [Fact]
    public async Task ADependentIsNotRestartedAndATableThatStopsCompilingKeepsItsStoredSchema()
    {
        var (state, store) = NewStore();
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)));
        var table = await store.CreateTableAsync(new TableDefinition { Name = "t", Sql = "SELECT symbol, price FROM s" });
        await store.SetTableStatusAsync(table.Id, PipelineStatus.Running);
        Assert.Equal(PipelineStatus.Running, state.Tables.Single(t => t.Id == table.Id).Status);

        // `price` disappears — the table's SELECT stops resolving.
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String)));

        var stored = state.Tables.Single(t => t.Id == table.Id);
        Assert.Equal(PipelineStatus.Running, stored.Status);       // never cascaded a restart
        Assert.Equal(2, stored.OutputFields.Count);                // never emptied what it publishes
        Assert.Equal(table.SchemaRevision, stored.SchemaRevision);
    }

    [Fact]
    public async Task StaleReasonIsSetByTheUpstreamChangeAndClearedWhenThePinIsReSatisfied()
    {
        var (state, store) = NewStore();
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String)));

        var table = await store.CreateTableAsync(new TableDefinition
        {
            Name = "t",
            Sql = "SELECT symbol FROM s",
            DependsOn = [new EntityPin { Kind = "source", Name = "s", SchemaRevision = 1 }],
        });
        Assert.Null(table.StaleReason);

        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long)));

        var stale = state.Tables.Single(t => t.Id == table.Id);
        Assert.NotNull(stale.StaleReason);
        Assert.Contains("'s'", stale.StaleReason);

        await store.UpdateTableAsync(new TableDefinition
        {
            Id = table.Id,
            Name = "t",
            Sql = "SELECT symbol FROM s",
            DependsOn = [new EntityPin { Kind = "source", Name = "s", SchemaRevision = 2 }],
        });

        Assert.Null(state.Tables.Single(t => t.Id == table.Id).StaleReason);
    }

    [Fact]
    public async Task DeletingAPinnedSourceMakesTheDependantStale()
    {
        var (state, store) = NewStore();
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String)));
        var table = await store.CreateTableAsync(new TableDefinition
        {
            Name = "t",
            Sql = "SELECT symbol FROM s",
            DependsOn = [new EntityPin { Kind = "source", Name = "s", SchemaRevision = 1 }],
        });
        Assert.Null(table.StaleReason);

        await store.DeleteSourceAsync("s");

        Assert.NotNull(state.Tables.Single(t => t.Id == table.Id).StaleReason);
    }

    [Fact]
    public async Task StartingAndStoppingAPipelineIsNotADefinitionChange()
    {
        var (state, store) = NewStore();
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String)));
        var pipe = await store.CreatePipelineAsync(new PipelineDefinition { Name = "p", Sql = "SELECT symbol FROM s" });
        Assert.Equal(1, pipe.Revision);

        await store.SetPipelineStatusAsync(pipe.Id, PipelineStatus.Running);
        await store.SetPipelineStatusAsync(pipe.Id, PipelineStatus.Stopped);

        Assert.Equal(1, state.Pipelines.Single(p => p.Id == pipe.Id).Revision);
    }

    [Fact]
    public async Task AnEditToAPipelinesSqlBumpsItsRevisionExactlyOnce()
    {
        var (state, store) = NewStore();
        await store.UpsertSourceAsync(Src("s", new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long)));
        var pipe = await store.CreatePipelineAsync(new PipelineDefinition { Name = "p", Sql = "SELECT symbol FROM s" });

        await store.UpdatePipelineAsync(new PipelineDefinition { Id = pipe.Id, Name = "p", Sql = "SELECT symbol, qty FROM s" });
        Assert.Equal(2, state.Pipelines.Single(p => p.Id == pipe.Id).Revision);

        // …and saving the identical definition again does not.
        await store.UpdatePipelineAsync(new PipelineDefinition { Id = pipe.Id, Name = "p", Sql = "SELECT symbol, qty FROM s" });
        Assert.Equal(2, state.Pipelines.Single(p => p.Id == pipe.Id).Revision);
    }
}
