using Orleans;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using Xunit;

namespace StreamsForge.Engine.Tests;

/// <summary>End-to-end smoke test: a real Orleans TestingHost cluster, a real ITableGrain subscribed to a
/// real memory stream, fed real events from a stream-provider client — verifying RegistryGrain →
/// TableGrain → Engine TableExecutor → GetRowsAsync wiring, and that a second contribution to an existing
/// group UPDATES the row (retract+assert) rather than appending a new one.</summary>
public sealed class TableGrainClusterSmokeTest : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<TestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<TestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task TableGrain_AggregatesFromRealStream_AndUpdatesInPlaceRatherThanAppending()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = "trades",
            Description = "test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false, // no synthetic generator noise
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("qty", FieldType.Long),
            ],
        });

        var tableName = "positions_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = tableName,
            Description = "smoke test",
            Sql = "SELECT symbol, COUNT(*) AS trades, SUM(qty) AS total_qty FROM trades GROUP BY symbol",
            Status = PipelineStatus.Stopped,
        };

        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        await table.StartAsync(def);

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, "trades"));

        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = "trades",
            ["symbol"] = "AAPL",
            ["price"] = 100.0,
            ["qty"] = 10L,
        });

        // Assert: poll GetRowsAsync until the AAPL aggregate row appears (write-behind flush is every 2s).
        var rows = await PollUntilRowAppears(table, "AAPL", deadlineSeconds: 15);
        Assert.NotNull(rows);
        var row = rows!.Single(r => Equals(r.Row["symbol"], "AAPL"));
        Assert.Equal(1L, row.Row["trades"]);
        Assert.Equal(10L, row.Row["total_qty"]);

        // Act: push a second trade for the same symbol.
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = "trades",
            ["symbol"] = "AAPL",
            ["price"] = 105.0,
            ["qty"] = 20L,
        });

        // Assert: the row is UPDATED in place (still exactly one AAPL row, count=2, qty=30) — not appended.
        var deadline = DateTime.UtcNow.AddSeconds(15);
        List<TableRowDto> updatedRows = [];
        while (DateTime.UtcNow < deadline)
        {
            updatedRows = await table.GetRowsAsync(100, 0);
            var aapl = updatedRows.FirstOrDefault(r => Equals(r.Row.GetValueOrDefault("symbol"), "AAPL"));
            if (aapl is not null && Equals(aapl.Row.GetValueOrDefault("trades"), 2L))
            {
                break;
            }
            await Task.Delay(200);
        }

        var aaplRows = updatedRows.Where(r => Equals(r.Row.GetValueOrDefault("symbol"), "AAPL")).ToList();
        var singleAapl = Assert.Single(aaplRows); // updated in place, not a second row for the same symbol
        Assert.Equal(2L, singleAapl.Row["trades"]);
        Assert.Equal(30L, singleAapl.Row["total_qty"]);

        await table.StopAsync();
    }

    [Fact]
    public async Task TableGrain_WithSearchEnabled_FindsKnownSymbolViaSearchAsync()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = "trades",
            Description = "test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false,
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("qty", FieldType.Long),
            ],
        });

        var tableName = "positions_search_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = tableName,
            Description = "search smoke test",
            Sql = "SELECT symbol, COUNT(*) AS trades, SUM(qty) AS total_qty FROM trades GROUP BY symbol",
            Status = PipelineStatus.Stopped,
            SearchEnabled = true,
            SearchMode = TableSearchMode.Fuzzy,
        };

        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        await table.StartAsync(def);

        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, "trades"));

        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = "trades",
            ["symbol"] = "AAPL",
            ["price"] = 100.0,
            ["qty"] = 10L,
        });

        // Unlike GetRowsAsync (which reads the write-behind-flushed snapshot, up to 2s stale), the search
        // index is updated synchronously as deltas land — so a short poll (just waiting for stream
        // delivery, not the flush timer) suffices.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        List<TableRowDto> hits = [];
        while (DateTime.UtcNow < deadline)
        {
            hits = await table.SearchAsync("AAPL", 10);
            if (hits.Count > 0) break;
            await Task.Delay(100);
        }

        var hit = Assert.Single(hits);
        Assert.Equal("AAPL", hit.Row["symbol"]);

        // A fuzzy typo of the symbol should also find it.
        var fuzzyHits = await table.SearchAsync("AAPLl", 10);
        Assert.Contains(fuzzyHits, r => Equals(r.Row.GetValueOrDefault("symbol"), "AAPL"));

        await table.StopAsync();
    }

    private static async Task<List<TableRowDto>?> PollUntilRowAppears(ITableGrain table, string symbol, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        while (DateTime.UtcNow < deadline)
        {
            var rows = await table.GetRowsAsync(100, 0);
            if (rows.Any(r => Equals(r.Row.GetValueOrDefault("symbol"), symbol)))
            {
                return rows;
            }
            await Task.Delay(200);
        }
        return null;
    }
}
