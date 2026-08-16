using Orleans;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Grains;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Wishlist #14 option (a) — the REAL fix (backfill on attach), end to end through real Orleans grains:
/// <c>TableGrain.AttachToTableInputAsync</c> subscribing to an upstream's delta stream, then atomically
/// reading (<see cref="TableAttachSnapshot"/>) via <c>ITableGrain.AttachSnapshotAsync</c>, then admitting the
/// snapshot as this table's own initial state, then filtering any subsequently-received delta by the
/// recorded cutoff epoch (<c>TableDeltaDto.Epoch</c>).
///
/// This is the end-to-end counterpart of <c>WarmAttachBackfillEquivalenceTests</c> (Engine-only, no grains):
/// where that test pins the ARITHMETIC ("cold-built vs warm-attached agree exactly, UnmatchedRetractions ==
/// 0"), this one pins the GRAIN WIRING that has to get the timing right for that arithmetic to apply for
/// real — subscribe-then-attach ordering, AttachSnapshotAsync's atomicity, and the epoch cutoff filter.
///
/// Reuses <c>PartitionedTableTestSiloConfigurator</c>/<c>PartitionedTableTestClientConfigurator</c> (memory
/// streams + memory grain storage) from PartitionedTableClusterTests.cs — same pattern every other cluster
/// test file in this project uses.
/// </summary>
public sealed class BackfillOnAttachClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<PartitionedTableTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<PartitionedTableTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private async Task<string> SeedSourceAsync(IRegistryGrain registry)
    {
        var sourceName = "backfill_src_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "backfill-on-attach cluster test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false, // deterministic: the test publishes events itself, no live generator
            Fields = [new FieldDef("symbol", FieldType.String), new FieldDef("price", FieldType.Double)],
        });
        return sourceName;
    }

    private async Task PublishTickAsync(string sourceName, string symbol, double price)
    {
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = sourceName,
            ["symbol"] = symbol,
            ["price"] = price,
        });
    }

    private static async Task<int> PollRowCountAsync(ITableGrain grain, int atLeast, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        int last = 0;
        while (DateTime.UtcNow < deadline)
        {
            last = await grain.GetRowCountAsync();
            if (last >= atLeast) return last;
            await Task.Delay(50);
        }
        return last;
    }

    private static async Task<List<TableRowDto>> PollRowsAsync(ITableGrain grain, int atLeast, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        List<TableRowDto> last = [];
        while (DateTime.UtcNow < deadline)
        {
            last = await grain.GetRowsAsync(1000, 0);
            if (last.Count >= atLeast) return last;
            await Task.Delay(50);
        }
        return last;
    }

    /// <summary>The wishlist's exact reported shape: a GROUP BY table created AFTER its LATEST BY input
    /// already holds rows. Before this fix (886c3dc/771ea85's option (b)) it reported NOTHING — an honest
    /// empty rather than a wrong count, but still not the right answer. With the real backfill, it must
    /// report exactly one row per symbol with n=1 (one contributing LATEST BY row per key) — the answer a
    /// table built cold from the start would also report.</summary>
    [Fact]
    public async Task GroupBy_table_created_after_its_upstream_is_warm_backfills_the_correct_counts()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedSourceAsync(registry);

        var upstreamName = "backfill_up_" + Guid.NewGuid().ToString("n")[..8];
        var upstream = await registry.CreateTableAsync(new TableDefinition
        {
            Name = upstreamName,
            Sql = $"SELECT symbol, price FROM {sourceName} LATEST BY (symbol)",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(upstream.Id, PipelineStatus.Running);
        var upstreamGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(upstreamName);

        // Warm the upstream BEFORE the consumer exists, including an UPDATE to an existing key (retract +
        // assert) — the shape that used to destroy a late-attached group's arithmetic entirely.
        await PublishTickAsync(sourceName, "AAPL", 100.0);
        await PublishTickAsync(sourceName, "MSFT", 200.0);
        await PublishTickAsync(sourceName, "AAPL", 101.0); // update: retract(100.0) + assert(101.0)
        await PublishTickAsync(sourceName, "GOOG", 50.0);
        var upstreamRows = await PollRowCountAsync(upstreamGrain, atLeast: 3, TimeSpan.FromSeconds(10));
        Assert.Equal(3, upstreamRows);

        // NOW create the consumer — StartClassicAsync's AttachToTableInputAsync backfills from the
        // upstream's current snapshot as part of starting.
        var consumerName = "backfill_agg_" + Guid.NewGuid().ToString("n")[..8];
        var consumer = await registry.CreateTableAsync(new TableDefinition
        {
            Name = consumerName,
            Sql = $"SELECT symbol, COUNT(*) AS n FROM {upstreamName} GROUP BY symbol",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(consumer.Id, PipelineStatus.Running);
        var consumerGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(consumerName);

        var rows = await PollRowsAsync(consumerGrain, atLeast: 3, TimeSpan.FromSeconds(10));
        Assert.Equal(3, rows.Count);
        var bySymbol = rows.ToDictionary(r => (string)r.Row["symbol"]!, r => r);
        Assert.Equal(["AAPL", "GOOG", "MSFT"], bySymbol.Keys.OrderBy(k => k, StringComparer.Ordinal));
        foreach (var row in rows)
        {
            Assert.Equal(1L, row.Row["n"]); // exactly one contributing LATEST BY row per symbol, not zero
            Assert.Equal(1L, row.Weight);
        }

        // Live traffic after attach must still layer correctly on top of the backfill — a NEW symbol and an
        // update to a symbol that predates the attach.
        await PublishTickAsync(sourceName, "AMZN", 300.0);
        await PublishTickAsync(sourceName, "AAPL", 102.0);
        rows = await PollRowsAsync(consumerGrain, atLeast: 4, TimeSpan.FromSeconds(10));
        Assert.Equal(4, rows.Count);
        foreach (var row in rows)
        {
            // n must stay 1 forever for this SQL shape — never 0 (unmatched retraction) and never 2
            // (double-counted backfill).
            Assert.Equal(1L, row.Row["n"]);
        }
    }

    /// <summary>Sanity companion to the warm case: a consumer created BEFORE its upstream ever holds rows
    /// must still work exactly as before — AttachSnapshotAsync returns an empty snapshot with Epoch -1, the
    /// cutoff filter admits everything (Epoch &gt; -1 for every real published batch), and rows accumulate
    /// as ordinary live traffic arrives.</summary>
    [Fact]
    public async Task GroupBy_table_created_before_its_upstream_has_rows_still_works_via_live_traffic_only()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedSourceAsync(registry);

        var upstreamName = "backfill_cold_up_" + Guid.NewGuid().ToString("n")[..8];
        var upstream = await registry.CreateTableAsync(new TableDefinition
        {
            Name = upstreamName,
            Sql = $"SELECT symbol, price FROM {sourceName} LATEST BY (symbol)",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(upstream.Id, PipelineStatus.Running);

        var consumerName = "backfill_cold_agg_" + Guid.NewGuid().ToString("n")[..8];
        var consumer = await registry.CreateTableAsync(new TableDefinition
        {
            Name = consumerName,
            Sql = $"SELECT symbol, COUNT(*) AS n FROM {upstreamName} GROUP BY symbol",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(consumer.Id, PipelineStatus.Running);
        var consumerGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(consumerName);

        await PublishTickAsync(sourceName, "TSLA", 400.0);
        await PublishTickAsync(sourceName, "NFLX", 500.0);

        var rows = await PollRowsAsync(consumerGrain, atLeast: 2, TimeSpan.FromSeconds(10));
        Assert.Equal(2, rows.Count);
        foreach (var row in rows)
        {
            Assert.Equal(1L, row.Row["n"]);
        }
    }
}
