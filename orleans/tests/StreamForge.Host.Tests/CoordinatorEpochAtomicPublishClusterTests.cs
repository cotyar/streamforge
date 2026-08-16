using Orleans;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Host.Grains;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Wishlist #15 / #14, PART 2 — coordinator mode (Parallelism &gt;= 2) gets the SAME "one upstream batch
/// applied as one epoch with its output consolidated" property the classic path already has (see
/// TableGrain.ConsolidateCoordinatorEpochOutput and TableOutputGrain.PublishAsync's own doc comments for the
/// mechanism: publish now happens once per fully-advanced frontier round, netted by canonical row key,
/// instead of TableOutputGrain's old "publish immediately, per terminal-partition arrival" design).
///
/// This is the cluster-level, real-grains counterpart of
/// orleans/tests/StreamForge.Engine.Tests/EpochAtomicConsolidationTests.cs (which pins the SAME property for
/// the classic/Engine-only path) — same reported shape from the wishlist, `LEFT JOIN` onto a `GROUP BY`
/// table, except the `GROUP BY` table here runs as a REAL Parallelism=4 coordinator-mode table (through
/// TableIngestGrain/TableStageGrain/TableOutputGrain), and the `LEFT JOIN` table is a classic (Parallelism=1)
/// table chained onto it — exercising Part 1's warm-attach backfill against a coordinator-mode upstream at
/// the same time (TableGrain.AttachSnapshotAsync's <c>_coordinatorMode</c> branch, reading
/// _coordinatorLedger/_snapshotFrontierEpoch).
/// </summary>
public sealed class CoordinatorEpochAtomicPublishClusterTests : IAsyncLifetime
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

    private async Task PublishTickAsync(string sourceName, string g, double x)
    {
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = sourceName,
            ["g"] = g,
            ["x"] = x,
        });
    }

    private async Task PublishOrderAsync(string sourceName, string tag)
    {
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName));
        await stream.OnNextAsync(new EventRecord
        {
            [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            [EventRecord.SourceField] = sourceName,
            ["tag"] = tag,
        });
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
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

    [Fact]
    public async Task LeftJoin_onto_a_coordinator_mode_aggregate_never_observes_an_intermediate_null_total()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

        var ticksName = "coord_ticks_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = ticksName,
            Description = "coordinator epoch atomicity test ticks",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false,
            Fields = [new FieldDef("g", FieldType.String), new FieldDef("x", FieldType.Double)],
        });

        var ordersName = "coord_orders_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = ordersName,
            Description = "coordinator epoch atomicity test orders",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false,
            Fields = [new FieldDef("tag", FieldType.String)],
        });

        var latestName = "coord_latest_" + Guid.NewGuid().ToString("n")[..8];
        var latest = await registry.CreateTableAsync(new TableDefinition
        {
            Name = latestName,
            Sql = $"SELECT g, x FROM {ticksName} LATEST BY (g)",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(latest.Id, PipelineStatus.Running);

        // The aggregate is COORDINATOR MODE (Parallelism >= 2) — the whole point of this test.
        var aggName = "coord_agg_" + Guid.NewGuid().ToString("n")[..8];
        var agg = await registry.CreateTableAsync(new TableDefinition
        {
            Name = aggName,
            Sql = $"SELECT g, SUM(x) AS total FROM {latestName} GROUP BY g",
            Parallelism = 4,
        });
        await registry.SetTableStatusAsync(agg.Id, PipelineStatus.Running);

        var joinedName = "coord_joined_" + Guid.NewGuid().ToString("n")[..8];
        var joined = await registry.CreateTableAsync(new TableDefinition
        {
            Name = joinedName,
            Sql = $"SELECT o.tag, b.total FROM {ordersName} o LEFT JOIN {aggName} b ON o.tag = b.g",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(joined.Id, PipelineStatus.Running);
        var joinedGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(joinedName);

        // Seed key "a" and its matching order row; wait for the whole chain to converge.
        await PublishTickAsync(ticksName, "a", 5.0);
        await PublishOrderAsync(ordersName, "a");

        var seededRows = await PollUntilAsync(
            () => joinedGrain.GetRowsAsync(10, 0),
            rows => rows.Any(r => (string?)r.Row.GetValueOrDefault("tag") == "a" && r.Row.GetValueOrDefault("total") is not null),
            deadlineSeconds: 20);
        Assert.Contains(seededRows, r => (string?)r.Row.GetValueOrDefault("tag") == "a" && Convert.ToDouble(r.Row["total"]) == 5.0);

        // NOW subscribe directly to `joined`'s own delta stream — the exact one a real client/SignalR
        // subscriber would receive — and watch EVERY batch published from here on.
        var received = new List<List<TableDeltaDto>>();
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var deltaStream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, joinedName));
        var subHandle = await deltaStream.SubscribeAsync((batch, _) => { received.Add(batch); return Task.CompletedTask; });

        // One upstream change to `ticks` for key "a" — a real update (retract 5.0 + assert 9.0 in
        // `latest_x`), cascading through the COORDINATOR-mode `agg` table's own GROUP BY (TableIngestGrain
        // -> TableStageGrain(s) -> TableOutputGrain) and finally into `joined`'s LEFT JOIN.
        await PublishTickAsync(ticksName, "a", 9.0);

        var convergedRows = await PollUntilAsync(
            () => joinedGrain.GetRowsAsync(10, 0),
            rows => rows.Any(r => (string?)r.Row.GetValueOrDefault("tag") == "a" && r.Row.GetValueOrDefault("total") is not null && Convert.ToDouble(r.Row["total"]) == 9.0),
            deadlineSeconds: 20);
        Assert.Contains(convergedRows, r => (string?)r.Row.GetValueOrDefault("tag") == "a" && Convert.ToDouble(r.Row["total"]) == 9.0);

        // Give any straggler publishes a moment to land, then inspect EVERY batch this table published
        // during the transition — none of them may ever assert a NULL total for tag "a". A pre-fix
        // TableOutputGrain (publish immediately per terminal-partition arrival, before frontier
        // consolidation) could let `joined`'s LEFT JOIN observe `agg`'s retract before its matching assert
        // as two separate deltas, producing exactly that NULL pad.
        await Task.Delay(500);
        await subHandle.UnsubscribeAsync();

        Assert.NotEmpty(received);
        foreach (var batch in received)
        {
            foreach (var dto in batch)
            {
                if ((string?)dto.Row.GetValueOrDefault("tag") == "a" && dto.Weight > 0)
                {
                    Assert.True(dto.Row.GetValueOrDefault("total") is not null,
                        "joined published an asserted row for tag 'a' with a NULL total — the coordinator-mode NULL-flap this test guards against.");
                }
            }
        }
    }

    /// <summary>Wishlist #14 option (a), coordinator-mode upstream: <c>TableGrain.AttachSnapshotAsync</c>'s
    /// <c>_coordinatorMode</c> branch (reads <c>_coordinatorLedger</c>/<c>_snapshotFrontierEpoch</c> instead
    /// of <c>TableExecutor.Snapshot()</c>/<c>LastEpoch</c>) — a classic (Parallelism==1) table created AFTER
    /// its table input, which is itself a WARM Parallelism&gt;=2 coordinator-mode table, must backfill from
    /// it exactly like the all-classic case in BackfillOnAttachClusterTests.</summary>
    [Fact]
    public async Task Classic_table_attached_to_an_already_warm_coordinator_mode_upstream_backfills_correctly()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

        var ticksName = "coord_warm_ticks_" + Guid.NewGuid().ToString("n")[..8];
        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = ticksName,
            Description = "coordinator warm-attach test ticks",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false,
            Fields = [new FieldDef("g", FieldType.String), new FieldDef("x", FieldType.Double)],
        });

        // The upstream is COORDINATOR MODE (Parallelism >= 2) and will be warmed BEFORE any consumer exists.
        var aggName = "coord_warm_agg_" + Guid.NewGuid().ToString("n")[..8];
        var agg = await registry.CreateTableAsync(new TableDefinition
        {
            Name = aggName,
            Sql = $"SELECT g, SUM(x) AS total FROM {ticksName} GROUP BY g",
            Parallelism = 4,
        });
        await registry.SetTableStatusAsync(agg.Id, PipelineStatus.Running);
        var aggGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(aggName);

        await PublishTickAsync(ticksName, "a", 1.0);
        await PublishTickAsync(ticksName, "b", 2.0);
        await PublishTickAsync(ticksName, "a", 3.0); // a SECOND contribution to group "a" (SUM, not LATEST BY)

        await PollUntilAsync(() => aggGrain.GetRowCountAsync(), c => c == 2, deadlineSeconds: 20);

        // NOW create a classic (Parallelism==1) consumer over the warm coordinator-mode upstream.
        var consumerName = "coord_warm_consumer_" + Guid.NewGuid().ToString("n")[..8];
        var consumer = await registry.CreateTableAsync(new TableDefinition
        {
            Name = consumerName,
            Sql = $"SELECT g, COUNT(*) AS n FROM {aggName} GROUP BY g",
            Parallelism = 1,
        });
        await registry.SetTableStatusAsync(consumer.Id, PipelineStatus.Running);
        var consumerGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(consumerName);

        var rows = await PollUntilAsync(() => consumerGrain.GetRowsAsync(10, 0), r => r.Count >= 2, deadlineSeconds: 20);
        Assert.Equal(2, rows.Count);
        foreach (var row in rows)
        {
            // One contributing row per group from `agg`'s own current snapshot — never 0 (an unmatched
            // retraction ping-ponging the group to empty, the pre-fix symptom).
            Assert.Equal(1L, row.Row["n"]);
        }
    }
}
