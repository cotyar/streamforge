using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Catalog;
using StreamsForge.Host.Grains;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// This flavor's half of the row-identity warning: the SAME verdict, on a definition that came out of
/// THIS flavor's catalog.
///
/// <para>Both runtimes derive a table's row identity with the identical pure function
/// (<c>TableGroupKeyExtractor</c>, in shared AppCore — see <see cref="TableHistoryKeyCodecParityTests"/>
/// for the same argument applied to the key ENCODING), and both report the degraded case through the same
/// shared API layer: a log line at table create/update, and <c>TableMetrics.RowIdentityWarning</c> on
/// <c>GET /api/tables/{id}/metrics</c>. There is deliberately no second implementation to keep in step —
/// nothing about the verdict is persisted by either <c>CatalogStore</c> or <c>RegistryGrain</c>, so the
/// two cannot drift. What IS worth pinning here is that a definition surviving this flavor's own
/// create/update round-trip still produces the same answer: the warning is a function of Sql +
/// HistoryEnabled + ShardBy, and all three must come back off the catalog intact for it to hold.</para>
///
/// <para>New file, per this wave's file-ownership rule — no pre-existing test is touched.</para>
/// </summary>
public class RowIdentityWarningParityTests
{
    private const string ExpressionKeySql =
        "SELECT ts_ms - ts_ms % 43000 AS bucket, symbol, price FROM trades LATEST BY (ts_ms - ts_ms% 43000)";

    private const string CorrectedSql =
        "SELECT ts_ms - ts_ms % 43000 AS bucket, symbol, price FROM trades LATEST BY (ts_ms - ts_ms % 43000)";

    private const string NoIdentitySql = "SELECT symbol, price FROM trades";

    private static (CatalogState State, CatalogStore Store) NewStore()
    {
        var state = new CatalogState();
        state.Sources.Add(new SourceDefinition
        {
            Name = "trades",
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("ts_ms", FieldType.Long),
            ],
        });
        return (state, new CatalogStore(state, new TestLifecycleOrchestrator()));
    }

    [Fact]
    public async Task CreatedTable_WithUnmappableLatestByKeyAndHistory_ReportsTheWarning()
    {
        var (state, store) = NewStore();

        var created = await store.CreateTableAsync(new TableDefinition
        {
            Name = "degraded", Sql = ExpressionKeySql, HistoryEnabled = true,
        });

        var warning = TableRowIdentityWarning.For(state.Tables.Single(t => t.Id == created.Id));
        Assert.NotNull(warning);
        Assert.Contains("LATEST BY", warning);
        Assert.Contains("ts_ms - ts_ms% 43000", warning);
    }

    [Fact]
    public async Task CreatedTable_WithNoDeclaredIdentity_ReportsNothing()
    {
        // The must-not-flag case: this table has always been keyed by its whole row and that is correct.
        var (state, store) = NewStore();

        var created = await store.CreateTableAsync(new TableDefinition
        {
            Name = "plain", Sql = NoIdentitySql, HistoryEnabled = true,
        });

        Assert.Null(TableRowIdentityWarning.For(state.Tables.Single(t => t.Id == created.Id)));
    }

    [Fact]
    public async Task UpdatedTable_FixingTheSql_StopsReporting()
    {
        var (state, store) = NewStore();
        var created = await store.CreateTableAsync(new TableDefinition
        {
            Name = "fixme", Sql = ExpressionKeySql, HistoryEnabled = true,
        });
        Assert.NotNull(TableRowIdentityWarning.For(state.Tables.Single(t => t.Id == created.Id)));

        await store.UpdateTableAsync(new TableDefinition
        {
            Id = created.Id, Name = created.Name, Sql = CorrectedSql, HistoryEnabled = true,
        });

        // Nothing had to be recomputed, backfilled or invalidated for this to go quiet — the verdict is
        // derived on read from the stored definition.
        Assert.Null(TableRowIdentityWarning.For(state.Tables.Single(t => t.Id == created.Id)));
    }

    [Fact]
    public async Task ShardedTable_KeepsTheWarningAcrossThisFlavorsCatalogRoundTrip()
    {
        // ShardBy is Orleans-only at RUNTIME but stored here (CatalogStoreShardByTests), and a shard's
        // per-key version trail is grouped by the same derived identity — so a catalog promoted between
        // flavors must carry the same verdict with it.
        var (state, store) = NewStore();

        var created = await store.CreateTableAsync(new TableDefinition
        {
            Name = "sharded", Sql = ExpressionKeySql, ShardBy = ["symbol"],
        });

        var stored = state.Tables.Single(t => t.Id == created.Id);
        var warning = TableRowIdentityWarning.For(stored);
        Assert.NotNull(warning);
        Assert.Contains("shard", warning);
    }

    [Fact]
    public void TheVerdictLivesOnTheMetrics_NotOnTheDefinition()
    {
        // Stated as a test because it is a design commitment, not an accident. TableDefinition is a
        // client-owned record whose every writable field must round-trip an update untouched — asserted
        // by CatalogUpdateRoundTripTests, which sets every writable property by reflection — so a
        // server-computed warning field there would be a contradiction (and would break that test). The
        // verdict belongs on the status/metrics surface, beside Rebuilding, and is derived on read.
        Assert.Null(typeof(TableDefinition).GetProperty("RowIdentityWarning"));
        Assert.NotNull(typeof(TableMetrics).GetProperty("RowIdentityWarning"));
    }
}
