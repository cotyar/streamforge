using Orleans.Runtime;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 011 wave C: <c>TableHistoryGrain.CaptureSnapshotIntoState</c> used to deep-clone the ENTIRE entry
/// map — a fresh <c>RowHistoryEntry</c> and a fresh <c>List&lt;HistoryVersion&gt;</c> per retained key —
/// on every flush tick. It now re-clones only the entries touched since the previous capture.
///
/// The mirror it maintains (<c>state.State.Entries</c>) is invisible to normal reads, which always go to
/// the live working set — so the only honest way to test an incremental mirror is to make something read
/// the mirror back. <c>ResumeAsync</c> is exactly that: it is what the real restart path calls, and it
/// discards the live set and rebuilds it from the persisted mirror. A capture that skipped an entry it
/// should have re-cloned therefore shows up here as history that silently loses versions across a resume —
/// which is the actual user-visible failure mode of getting this wrong, not a synthetic proxy for it.
///
/// Reuses <c>HistoryTestSiloConfigurator</c>/<c>HistoryTestClientConfigurator</c> from
/// HistoryGrainClusterTests (memory storage is fine — the mirror is an in-memory object either way; what
/// is under test is which entries get re-cloned into it, not the bytes on disk).
/// </summary>
public sealed class TableHistoryMirrorClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<HistoryTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<HistoryTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    private async Task<(TableDefinition Created, string SourceName)> SeedAsync()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = "hmirror_" + Guid.NewGuid().ToString("n")[..8];
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
            Name = "hmirror_tbl_" + Guid.NewGuid().ToString("n")[..8],
            Description = "plan 011 wave C incremental history-mirror coverage",
            Sql = $"SELECT symbol, price, qty FROM {sourceName} LATEST BY (symbol)",
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.All,
            FlushMs = 200,
        });
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        return (created, sourceName);
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

    [Fact]
    public async Task EveryTouchedEntryReachesThePersistedMirrorAndSurvivesAResume()
    {
        var (created, sourceName) = await SeedAsync();
        var history = _cluster.GrainFactory.GetGrain<ITableHistoryGrain>(created.Name);

        // Three keys, updated a different number of times each, interleaved across several flush intervals
        // so the incremental capture has to accumulate touched keys across ticks rather than see them all
        // in one batch.
        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var symbols = new[] { "AAA", "BBB", "CCC" };
        for (var round = 0; round < 4; round++)
        {
            foreach (var symbol in symbols)
            {
                await PublishAsync(sourceName, ts++, symbol, 100 + round, 10 + round);
            }
            await Task.Delay(150);
        }

        // 3 keys x 4 asserted versions each = 12 assertions, plus one retraction per superseded version
        // (LATEST BY emits retract+assert on update). Only the assertion versions are retained as versions.
        var live = await PollUntilAsync(() => history.GetStatsAsync(), s => s.KeyCount == symbols.Length && s.TotalVersions >= symbols.Length * 4);
        Assert.Equal(symbols.Length, live.KeyCount);

        // ResumeAsync is DESTRUCTIVE by design — it replaces the live set with whatever the mirror holds —
        // so it can only be called once, and only after a capture has certainly run; retrying it would
        // truncate the live set on the first attempt and then "pass" against its own damage. Publishing has
        // stopped by here, so one generous wait (well over a dozen 200ms flush ticks) is enough for the
        // timer-driven capture to have caught up. A capture that WRONGLY skipped a touched entry does not
        // catch up no matter how long the wait, which is exactly what this asserts.
        await Task.Delay(3000);
        var beforeResume = await history.GetStatsAsync();
        Assert.Equal(symbols.Length * 4, beforeResume.TotalVersions);

        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var def = (await registry.GetTablesAsync()).Single(t => t.Id == created.Id);
        await history.ResumeAsync(def);
        var afterResume = await history.GetStatsAsync();

        Assert.Equal(symbols.Length, afterResume.KeyCount);
        Assert.Equal(beforeResume.TotalVersions, afterResume.TotalVersions);

        // And the per-key trails themselves survived, not just the totals.
        foreach (var symbol in symbols)
        {
            var key = RowKeyCodec.EncodeIdentity(new Dictionary<string, object?> { ["symbol"] = symbol }, ["symbol"]);
            var trail = await history.GetHistoryAsync(key, 0);
            Assert.True(trail.KeyFound, $"history for {symbol} was lost across the resume");
            Assert.Equal(4, trail.TotalVersions);
            Assert.Equal([103d, 102d, 101d, 100d], trail.Versions.Select(v => Convert.ToDouble(v.Row["price"])));
        }
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds = 20)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(150);
        }
        return last;
    }
}
