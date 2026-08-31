using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Catalog;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 011 wave D1: this flavor's half of key sharding, which is the half that says NO.
///
/// <see cref="TableDefinition.ShardBy"/> is Orleans-only — the shard tier is three Orleans grain kinds
/// whose entire value comes from Orleans' activation collector reclaiming an idle shard, and this flavor
/// has no actor equivalent to point at. The refusal therefore lives in <c>TableActor.StartAsync</c>, not
/// at upsert, and these tests pin BOTH halves of that decision:
///
///  * the catalog STORES the field (a catalog exported from an Orleans instance imports here intact and
///    can be promoted back without loss), because this flavor's standing contract — asserted by
///    <see cref="CatalogUpdateRoundTripTests"/>, which sets every writable property by reflection — is
///    that every client-owned field survives an update. An upsert-time refusal would make ShardBy the one
///    field that could not round-trip;
///  * and a sharded table can never RUN here, which is what keeps "stored" from meaning "half-served".
///    The start-time refusal is asserted next to the executor in <c>TableActor.StartAsync</c>; what is
///    checkable without an actor host is that nothing in the catalog path quietly clears the field, so
///    the definition that reaches the actor still carries what the actor refuses on.
///
/// New file rather than an edit to CatalogStoreTests.cs, per this wave's file-ownership rule.
/// </summary>
public class CatalogStoreShardByTests
{
    private static (CatalogState State, CatalogStore Store) NewStore()
    {
        var state = new CatalogState();
        state.Sources.Add(new SourceDefinition
        {
            Name = "instruments",
            Fields = [new FieldDef("instrument", FieldType.String), new FieldDef("stage", FieldType.String)],
        });
        return (state, new CatalogStore(state, new TestLifecycleOrchestrator()));
    }

    private const string Sql = "SELECT instrument, stage FROM instruments LATEST BY (instrument)";

    [Fact]
    public async Task CreateTable_StoresShardBy_RatherThanRejectingOrClearingIt()
    {
        var (state, store) = NewStore();

        var created = await store.CreateTableAsync(new TableDefinition
        {
            Name = "sharded_here", Sql = Sql, ShardBy = ["instrument"],
        });

        Assert.Equal(new[] { "instrument" }, created.ShardBy.ToArray());
        Assert.Equal(new[] { "instrument" }, state.Tables.Single(t => t.Id == created.Id).ShardBy.ToArray());
    }

    [Fact]
    public async Task UpdateTable_RoundTripsShardBy()
    {
        var (state, store) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition { Name = "t", Sql = Sql });

        var updated = await store.UpdateTableAsync(new TableDefinition
        {
            Id = created.Id, Name = created.Name, Sql = created.Sql, ShardBy = ["instrument"],
        });

        Assert.NotNull(updated);
        Assert.Equal(new[] { "instrument" }, updated!.ShardBy.ToArray());
        Assert.Equal(new[] { "instrument" }, state.Tables.Single(t => t.Id == created.Id).ShardBy.ToArray());
    }

    [Fact]
    public async Task DefaultIsEmpty_SoNothingAboutAnExistingTableChanges()
    {
        var (_, store) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition { Name = "t_plain", Sql = Sql });
        Assert.Empty(created.ShardBy);
    }

    [Fact]
    public async Task SearchEnabledPlusShardBy_IsAcceptedHere_BecauseTheTableCannotRunAnyway()
    {
        // The Orleans registry refuses this combination at upsert (a table-wide reverse index would keep
        // every row resident and defeat sharding). This flavor deliberately does not duplicate that guard:
        // a sharded table never starts here at all, so there is no index to keep and nothing to protect —
        // and adding a second, flavor-specific reason for a 409 would make an Orleans catalog fail to
        // import here for a rule that cannot bite. Asserted so the asymmetry is a decision on record
        // rather than an oversight someone "fixes" later.
        var (_, store) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition
        {
            Name = "t_search", Sql = Sql, ShardBy = ["instrument"], SearchEnabled = true,
        });
        Assert.True(created.SearchEnabled);
        Assert.Single(created.ShardBy);
    }
}
