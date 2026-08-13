using Microsoft.Extensions.Configuration;
using Orleans.Runtime;
using Orleans.TestingHost;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Grains;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 011 wave C: <c>TableGrain.CaptureSnapshotIntoState</c> no longer rebuilds the whole persisted
/// mirror on every flush tick — it applies only the row keys that changed since the previous capture.
/// That is a pure optimization ONLY IF the mirror still ends up byte-equivalent to the live ledger, and
/// the way an incremental mirror goes wrong is not by being slow, it is by leaking a row that should have
/// been removed.
///
/// This file attacks exactly that. It uses <c>LATEST BY</c>, whose every update is a REMOVAL of the old
/// canonical row key plus an insert of a new one (TableExecutor's canonical key hashes the whole row, so
/// changing any column changes the key), and it reads back through <c>GetRowsAsync</c>/<c>GetRowCountAsync</c>
/// — which for <see cref="TablePersistenceMode.Batched"/>/<see cref="TablePersistenceMode.FireAndForget"/>
/// read the MIRROR, not the live executor (see <c>TableGrain.ClassicModeRows</c>' doc comment). So a mirror
/// that forgot to drop a superseded key shows up immediately as a row count that climbs with the update
/// count instead of staying at the key count — which is precisely the failure the old whole-rebuild capture
/// could not have, and the new incremental one could.
///
/// Reuses <c>PersistenceModeTestSiloConfigurator</c>/<c>PersistenceModeTestRegistry</c> from
/// TablePersistenceModeClusterTests (real JsonFileGrainStorage on a real temp dir), since the mirror only
/// exists for modes that actually persist.
/// </summary>
public sealed class TableSnapshotMirrorClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _testId = null!;
    private string _dataDir = null!;

    public async Task InitializeAsync()
    {
        _testId = Guid.NewGuid().ToString("n");
        _dataDir = Path.Combine(Path.GetTempPath(), "sf-snapshot-mirror-tests", _testId);
        Directory.CreateDirectory(_dataDir);

        var builder = new TestClusterBuilder(1);
        builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TestId"] = _testId,
            ["DataDir"] = _dataDir,
        }));
        builder.AddSiloBuilderConfigurator<PersistenceModeTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<PersistenceModeTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey).GetTablesAsync();
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        PersistenceModeTestRegistry.Storages.TryRemove(_testId, out _);
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private async Task<(string SourceName, TableDefinition Created)> SeedLatestByTableAsync(TablePersistenceMode persistence)
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = "mirror_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
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

        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = "mirror_tbl_" + Guid.NewGuid().ToString("n")[..8],
            Description = "plan 011 wave C incremental-mirror coverage",
            Sql = $"SELECT symbol, price, qty FROM {sourceName} LATEST BY (symbol)",
            Persistence = persistence,
            FlushMs = 250, // fast enough that a poll converges without a long deadline
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        return (sourceName, created);
    }

    private async Task PublishAsync(string sourceName, long ts, string symbol, double price, long qty)
    {
        var stream = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = ts,
            [EventRecord.SourceField] = sourceName,
            ["symbol"] = symbol,
            ["price"] = price,
            ["qty"] = qty,
        });
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds = 20)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(100);
        }
        return last;
    }

    [Theory]
    [InlineData(TablePersistenceMode.Batched)]
    [InlineData(TablePersistenceMode.FireAndForget)]
    public async Task MirrorConvergesToTheLiveRowSetAcrossManyUpdatesAndDropsSupersededKeys(TablePersistenceMode persistence)
    {
        var (sourceName, created) = await SeedLatestByTableAsync(persistence);
        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(created.Name);

        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await PublishAsync(sourceName, ts++, "AAPL", 100, 10);
        await PublishAsync(sourceName, ts++, "MSFT", 200, 20);

        var rows = await PollUntilAsync(() => table.GetRowsAsync(500, 0), r => r.Count == 2);
        Assert.Equal(2, rows.Count);

        // 20 updates across the SAME two keys. Each one supersedes a canonical row key, so a mirror that
        // only ever adds would report 22 rows here rather than 2.
        for (var i = 1; i <= 10; i++)
        {
            await PublishAsync(sourceName, ts++, "AAPL", 100 + i, 10 + i);
            await PublishAsync(sourceName, ts++, "MSFT", 200 + i, 20 + i);
        }

        var settled = await PollUntilAsync(
            () => table.GetRowsAsync(500, 0),
            r => r.Count == 2 && r.Any(x => (string?)x.Row["symbol"] == "AAPL" && Convert.ToDouble(x.Row["price"]) == 110));

        Assert.Equal(2, settled.Count);
        Assert.Equal(2, await table.GetRowCountAsync());

        var aapl = settled.Single(r => (string?)r.Row["symbol"] == "AAPL");
        Assert.Equal(110, Convert.ToDouble(aapl.Row["price"]));
        Assert.Equal(20L, Convert.ToInt64(aapl.Row["qty"]));

        var msft = settled.Single(r => (string?)r.Row["symbol"] == "MSFT");
        Assert.Equal(210, Convert.ToDouble(msft.Row["price"]));
        Assert.Equal(30L, Convert.ToInt64(msft.Row["qty"]));

        // Every mirrored row carries weight 1 — a superseded key must be REMOVED from the mirror, never
        // left behind with a zeroed/negative weight (which would still show up in a row listing).
        Assert.All(settled, r => Assert.Equal(1, r.Weight));
    }

    [Fact]
    public async Task NewKeysAppearInTheMirrorWhileExistingOnesKeepTheirValues()
    {
        var (sourceName, created) = await SeedLatestByTableAsync(TablePersistenceMode.Batched);
        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(created.Name);

        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var symbols = new[] { "A", "B", "C", "D", "E" };
        foreach (var symbol in symbols)
        {
            await PublishAsync(sourceName, ts++, symbol, 1, 1);
        }
        await PollUntilAsync(() => table.GetRowCountAsync(), c => c == symbols.Length);

        // Touch ONE key across several flush intervals; the other four must survive untouched in the
        // mirror — the incremental path never re-captures them, so a bug that dropped un-touched keys
        // would show up here and nowhere else.
        for (var i = 1; i <= 5; i++)
        {
            await PublishAsync(sourceName, ts++, "C", 1 + i, 1 + i);
            await Task.Delay(120);
        }

        var rows = await PollUntilAsync(
            () => table.GetRowsAsync(500, 0),
            r => r.Count == symbols.Length && r.Any(x => (string?)x.Row["symbol"] == "C" && Convert.ToDouble(x.Row["price"]) == 6));

        Assert.Equal(symbols.Length, rows.Count);
        Assert.Equal(symbols.OrderBy(s => s), rows.Select(r => (string)r.Row["symbol"]!).OrderBy(s => s));
        foreach (var untouched in symbols.Where(s => s != "C"))
        {
            var row = rows.Single(r => (string?)r.Row["symbol"] == untouched);
            Assert.Equal(1, Convert.ToDouble(row.Row["price"]));
            Assert.Equal(1L, Convert.ToInt64(row.Row["qty"]));
        }
    }
}
