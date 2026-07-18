using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.TestingHost;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Grains;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 003 M4 acceptance at the cluster level: frontier-consistent reads for a coordinator-mode
/// (Parallelism &gt;= 2) table. Reuses <see cref="PartitionedTableTestSiloConfigurator"/>/
/// <see cref="PartitionedTableTestClientConfigurator"/> (memory streams + memory grain storage) — same
/// pattern as <see cref="PartitionedTableClusterTests"/>, a fresh <see cref="TestCluster"/> per test class.
///
/// Two acceptance angles:
///  1. End-to-end (<see cref="Parallelism4Table_FrontierEpoch_IsNonNull_Monotonic_AndAccompaniesRowCountChanges"/>):
///     a real P=4 table driven by real published events — frontierEpoch (mirrored on GetRowsAsync's
///     TableRowsResponse.FrontierEpoch and TableMetrics.SnapshotFrontierEpoch) eventually goes non-null,
///     never regresses, and every observed RowCount change is accompanied by a frontier advance (never a
///     RowCount change with an unchanged frontier — that would mean the read surface silently disagreed
///     with what it claims to reflect).
///  2. Direct/synthetic (<see cref="OnOutputBatchAsync_NeverExposesAPartiallyAppliedEpoch"/>): calls
///     ITableGrain.OnOutputBatchAsync directly (bypassing the real dataflow graph, which would otherwise
///     race this test's controlled two-partition scenario with its own real, small-epoch-numbered empty
///     markers — see that test's own comment for why a deliberately huge epoch sidesteps that) to pin the
///     M4 batch-atomicity fix: a GetRowsAsync call between the FIRST and SECOND terminal partition's
///     contribution to the SAME epoch must see NEITHER row — not one, not a torn mix — and only after BOTH
///     partitions have reported does it see BOTH, atomically, with frontierEpoch advanced to match.
/// </summary>
public sealed class TableFrontierClusterTests : IAsyncLifetime
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

    private async Task<string> SeedSourceAsync(IRegistryGrain registry, bool enabled = false)
    {
        var sourceName = "trades_fr_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "frontier cluster test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = enabled,
            Fields =
            [
                new FieldDef("symbol", FieldType.String),
                new FieldDef("price", FieldType.Double),
                new FieldDef("qty", FieldType.Long),
            ],
        });
        return sourceName;
    }

    private async Task PublishTradeAsync(string sourceName, string symbol, double price, long qty)
    {
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = sourceName,
            ["symbol"] = symbol,
            ["price"] = price,
            ["qty"] = qty,
        });
    }

    private static readonly (string Symbol, double Price, long Qty)[] DeterministicEvents =
    [
        ("AAPL", 100.0, 10), ("MSFT", 200.0, 5), ("AAPL", 101.0, 7), ("GOOG", 50.0, 20),
        ("MSFT", 199.5, 3), ("AAPL", 99.0, 4), ("GOOG", 51.0, 6), ("MSFT", 201.0, 9),
        ("AAPL", 102.0, 2), ("GOOG", 49.5, 11),
    ];

    [Fact]
    public async Task Parallelism4Table_FrontierEpoch_IsNonNull_Monotonic_AndAccompaniesRowCountChanges()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedSourceAsync(registry);
        var sql = $"SELECT symbol, SUM(qty) AS total FROM {sourceName} GROUP BY symbol";

        var tableName = "part4fr_" + Guid.NewGuid().ToString("n")[..8];
        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = tableName,
            Sql = sql,
            Parallelism = 4,
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        var tableGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        // Classic (Parallelism==1) tables must never report a frontier at all — sanity-check the "null
        // means no partitioned frontier exists" half of the contract before exercising the P=4 path.
        var classicName = "classicfr_" + Guid.NewGuid().ToString("n")[..8];
        var classic = await registry.CreateTableAsync(new TableDefinition { Name = classicName, Sql = sql, Parallelism = 1 });
        await registry.SetTableStatusAsync(classic.Id, PipelineStatus.Running);
        var classicGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(classicName);
        Assert.Null(await classicGrain.GetSnapshotFrontierEpochAsync());
        Assert.Null((await classicGrain.GetMetricsAsync()).SnapshotFrontierEpoch);

        foreach (var (symbol, price, qty) in DeterministicEvents)
        {
            await PublishTradeAsync(sourceName, symbol, price, qty);
        }

        var expectedTotals = DeterministicEvents
            .GroupBy(e => e.Symbol)
            .ToDictionary(g => g.Key, g => g.Sum(e => e.Qty));

        var samples = new List<(int RowCount, long? FrontierEpoch)>();
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline)
        {
            var metrics = await tableGrain.GetMetricsAsync();
            samples.Add(((int)metrics.RowCount, metrics.SnapshotFrontierEpoch));
            if (metrics.RowCount == expectedTotals.Count && metrics.SnapshotFrontierEpoch is not null)
            {
                // One more sample after convergence, so the "no further rowCount change without a frontier
                // change" assertion below has something to compare the converged state against too.
                await Task.Delay(50);
                metrics = await tableGrain.GetMetricsAsync();
                samples.Add(((int)metrics.RowCount, metrics.SnapshotFrontierEpoch));
                break;
            }
            await Task.Delay(50);
        }

        Assert.True(samples.Count > 1, "expected at least two polls");
        Assert.Contains(samples, s => s.FrontierEpoch is not null); // frontier eventually goes non-null
        Assert.Equal(expectedTotals.Count, samples[^1].RowCount); // converged to the right row count

        var nonNullFrontiers = samples.Where(s => s.FrontierEpoch is not null).Select(s => s.FrontierEpoch!.Value).ToList();
        for (int i = 1; i < nonNullFrontiers.Count; i++)
        {
            Assert.True(nonNullFrontiers[i] >= nonNullFrontiers[i - 1],
                $"frontier regressed: {nonNullFrontiers[i - 1]} -> {nonNullFrontiers[i]}");
        }

        // The core M4 consistency claim, sampled: a RowCount change between two consecutive polls is never
        // observed alongside an unchanged frontier (once the frontier has gone non-null) — RowCount only
        // ever changes as a DIRECT consequence of a frontier advance (OnOutputBatchAsync applies a batch and
        // advances the frontier in the same synchronous step — see TableGrain's class doc).
        for (int i = 1; i < samples.Count; i++)
        {
            var (prevCount, prevFrontier) = samples[i - 1];
            var (count, frontier) = samples[i];
            if (count != prevCount && prevFrontier is not null && frontier is not null)
            {
                Assert.True(frontier != prevFrontier,
                    $"rowCount changed ({prevCount} -> {count}) but frontierEpoch stayed at {frontier}");
            }
        }

        // Mirrors GetSnapshotFrontierEpochAsync (the /rows endpoint's source) — same value as GetMetricsAsync
        // reported once converged.
        var finalEpoch = await tableGrain.GetSnapshotFrontierEpochAsync();
        Assert.NotNull(finalEpoch);
        Assert.Equal(samples[^1].FrontierEpoch, finalEpoch);
    }

    /// <summary>Plan 003 M4 batch-atomicity pin. Deliberately bypasses the real dataflow graph — a real P=2
    /// table's terminal-stage partitions independently advance via real, small (0, 1, 2, …) epoch numbers
    /// driven by the 250ms ingest tick even with no data (see TableIngestGrain's class doc: an epoch marker
    /// flows every tick regardless of volume), which would race this test's own injected epoch and could
    /// make FrontierTracker reject the injected batches as regressions if the real graph got there first.
    /// Using an astronomically large epoch (never reachable by real per-tick traffic within a test's
    /// lifetime) for the injected batches sidesteps that race entirely while still exercising the REAL
    /// TableGrain (started via the real StartAsync -&gt; StartCoordinatorAsync path, so _outputFrontier is a
    /// real FrontierTracker registered over the real terminal-stage partition count for a P=2 table).</summary>
    [Fact]
    public async Task OnOutputBatchAsync_NeverExposesAPartiallyAppliedEpoch()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = await SeedSourceAsync(registry);
        var sql = $"SELECT symbol, price FROM {sourceName}"; // ungrouped -> FilterProject is the terminal stage, at full Parallelism partitions

        var tableName = "part2atomic_" + Guid.NewGuid().ToString("n")[..8];
        var created = await registry.CreateTableAsync(new TableDefinition
        {
            Name = tableName,
            Sql = sql,
            Parallelism = 2,
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        var tableGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        const long bigEpoch = 1_000_000_000L;
        var rowA = new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "ATOM_A", ["price"] = 1.0 }, Weight = 1 };
        var rowB = new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "ATOM_B", ["price"] = 2.0 }, Weight = 1 };

        // First terminal partition (of 2) reports at the huge epoch — real per-tick traffic on THIS
        // partition is nowhere near bigEpoch, so this is accepted as a genuine advance of partition 0's own
        // high-water mark, but the COMBINED frontier (min over both partitions) stays wherever partition 1's
        // real (tiny) high-water mark currently is — nowhere near bigEpoch — so this batch must NOT be
        // consolidated into the read-side snapshot yet.
        await tableGrain.OnOutputBatchAsync(0, bigEpoch, [rowA]);

        var afterFirst = await tableGrain.GetRowCountAsync();
        Assert.Equal(0, afterFirst); // neither row visible — NOT a partial application of rowA alone
        var rowsAfterFirst = await tableGrain.GetRowsAsync(10, 0);
        Assert.DoesNotContain(rowsAfterFirst, r => Equals(r.Row.GetValueOrDefault("symbol"), "ATOM_A"));
        Assert.DoesNotContain(rowsAfterFirst, r => Equals(r.Row.GetValueOrDefault("symbol"), "ATOM_B"));
        var frontierAfterFirst = await tableGrain.GetSnapshotFrontierEpochAsync();
        Assert.True(frontierAfterFirst is null || frontierAfterFirst < bigEpoch);

        // Second (and last) terminal partition now also reports at bigEpoch — EVERY terminal partition has
        // now reached bigEpoch, so the combined frontier advances to bigEpoch and BOTH batches admitted for
        // it (rowA from the first call, rowB from this one) are consolidated together, atomically, in this
        // one synchronous call.
        await tableGrain.OnOutputBatchAsync(1, bigEpoch, [rowB]);

        var afterSecond = await tableGrain.GetRowCountAsync();
        Assert.Equal(2, afterSecond); // BOTH rows visible together — never observed one without the other
        var rowsAfterSecond = await tableGrain.GetRowsAsync(10, 0);
        Assert.Contains(rowsAfterSecond, r => Equals(r.Row.GetValueOrDefault("symbol"), "ATOM_A"));
        Assert.Contains(rowsAfterSecond, r => Equals(r.Row.GetValueOrDefault("symbol"), "ATOM_B"));

        var frontierAfterSecond = await tableGrain.GetSnapshotFrontierEpochAsync();
        Assert.Equal(bigEpoch, frontierAfterSecond);

        var metrics = await tableGrain.GetMetricsAsync();
        Assert.Equal(bigEpoch, metrics.SnapshotFrontierEpoch);
        Assert.Equal(2, metrics.RowCount);
    }
}
