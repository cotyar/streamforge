using Orleans.TestingHost;
using StreamsForge.Abstractions;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>THE LATE CONSUMER. Every other test in this assembly is careful to create its consumer before
/// its producer, because Orleans memory streams have no replay and a table only ever received rows
/// published after it subscribed. These tests exist to prove the OPPOSITE order now works, because that
/// order is the natural one in the console: create the source (enabled), watch it start polling, THEN
/// write the table SQL. Before the attach protocol (<see cref="IConnectorGrain.BeginAttachAsync"/> +
/// <c>SourceReplayBuffer</c>) everything the source had already emitted was gone — permanently, since a
/// dedup key means the same rows are never re-emitted.
///
/// <para>The assertions are EXACT counts, both directions. "&gt;= 500" would pass on a system that
/// replayed the rows twice, which is the other half of the risk here: the gate must deliver the backlog
/// exactly once and must not double-count anything published while the consumer was attaching. The
/// append-more-rows-afterwards step is what tests that overlap — it forces live traffic through the same
/// subscription the replay just fed.</para>
///
/// <para>Fixture: own <see cref="TestCluster"/> reusing <see cref="ConnectorTestSiloConfigurator"/>/
/// <see cref="ConnectorTestClientConfigurator"/> (both `internal`, same assembly — see
/// <see cref="ConnectorGrainClusterTests"/>). <c>EnsureInitializedAsync</c> is deliberately never called:
/// these tests want an empty catalog, not the demo seed. The poll floor is 1000 ms
/// (<c>ScheduleCalc.MinIntervalMs</c>) — a smaller interval is invalid and silently becomes a 30 s
/// cadence, which is why every deadline below is generous rather than tight.</para></summary>
public sealed class SourceLateConsumerClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _scratchDir = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<ConnectorTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ConnectorTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        _scratchDir = Directory.CreateTempSubdirectory("sf-late-consumer-").FullName;
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        try { Directory.Delete(_scratchDir, recursive: true); } catch { /* best-effort */ }
    }

    private IRegistryGrain Registry => _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    private static SourceDefinition FileSource(string name, string path) => new()
    {
        Name = name,
        Kind = SourceKinds.File,
        Enabled = true,
        Fields =
        [
            new FieldDef("id", FieldType.Long),
            new FieldDef("seq", FieldType.Long),
            new FieldDef("value", FieldType.String),
        ],
        Connector = new ConnectorConfig
        {
            // The floor. Anything lower is rejected by ScheduleCalc.Validate and falls back to 30 s.
            Schedule = new ScheduleSpec { IntervalMs = 1000 },
            File = new FilePollConfig { Path = path, Format = FileFormats.Ndjson },
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                // The dedup key is what makes this test meaningful: without it, the next poll after the
                // table subscribes would re-emit the whole file and the table would fill up whether or not
                // the replay worked. With it, the first poll is the ONLY chance those rows ever have.
                DedupKeyField = "id",
                Fields =
                [
                    new FieldMapEntry { Field = new FieldDef("id", FieldType.Long) },
                    new FieldMapEntry { Field = new FieldDef("seq", FieldType.Long) },
                    new FieldMapEntry { Field = new FieldDef("value", FieldType.String) },
                ],
            },
        },
    };

    private static string Ndjson(int from, int count) =>
        string.Concat(Enumerable.Range(from, count).Select(i => $"{{\"id\":{i},\"seq\":{i},\"value\":\"v{i}\"}}\n"));

    /// <summary>Appends rows and then, after a beat, forces a fresh mtime — because the `file` kind's ledger
    /// is (name, mtime in MILLISECONDS) and a poll that lands mid-append records the mtime it saw BEFORE
    /// reading. If the append's own final mtime falls in that same millisecond, the ledger reads "unchanged"
    /// and the rest of the appended rows wait for the next real edit. That is a pre-existing property of the
    /// file kind, not of the replay protocol under test here, and it was observed derailing this test under
    /// whole-suite load (73 of 200 appended rows landing, then nothing). The touch removes it from this
    /// test's blast radius without hiding it — a partially-written file is exactly what
    /// <c>SourceExactCountClusterTests</c> covers deliberately.</summary>
    private static async Task AppendRowsAsync(string path, int from, int count)
    {
        await File.AppendAllTextAsync(path, Ndjson(from, count));
        await Task.Delay(1200);
        File.SetLastWriteTimeUtc(path, DateTime.UtcNow);
    }

    /// <summary>Creates the source ALREADY ENABLED and waits until it has actually published everything —
    /// so by the time the caller creates its consumer, the replay window this suite is about is wide open.
    ///
    /// <para>THE SETTLE DELAY IS NOT PADDING, it is what makes "exactly N" a legitimate assertion. The
    /// attach hold stops the source PUBLISHING; it has no reach into the stream provider's own pipeline, and
    /// a message the connector has already handed to <c>OnNextAsync</c> may still be sitting in the memory
    /// stream's queue, not yet pulled into the cache the pulling agent serves subscribers from (default pull
    /// period: 100 ms). A consumer that subscribes inside that window is delivered those in-flight messages
    /// AND replays them from the ring — genuine duplicates, in a window one pull period wide. Measured, not
    /// theorised: subscribing the instant the connector's counter reached 500 produced 501 rows on an idle
    /// machine and 554 under whole-suite load, scaling with how far behind the agent was. Waiting for the
    /// stream to quiesce first is also the honest model of the case this whole feature is for — someone
    /// creating a table minutes after the source started — rather than a millisecond-precise race nobody
    /// hits. The caveat itself is documented on <c>IConnectorGrain.BeginAttachAsync</c>.</para></summary>
    private async Task<(string Name, string Path)> StartedFileSourceWithAsync(string label, int rows)
    {
        var name = label + "_" + Guid.NewGuid().ToString("n")[..8];
        var path = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(path, Ndjson(0, rows));

        await Registry.UpsertSourceAsync(FileSource(name, path));

        var connector = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        var status = await PollUntilAsync(
            () => connector.GetStatusAsync(),
            s => s.EventsEmittedTotal >= rows,
            deadlineSeconds: 90);
        Assert.Equal(rows, status.EventsEmittedTotal);

        await Task.Delay(2000);

        return (name, path);
    }

    private async Task<string> StartRunningTableAsync(string sourceName, string sql, int parallelism = 1)
    {
        var tableName = "late_tbl_" + Guid.NewGuid().ToString("n")[..8];
        var created = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = tableName,
            Sql = sql,
            Parallelism = parallelism,
        });
        await Registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        return tableName;
    }

    [Fact]
    public async Task Table_created_after_a_file_source_already_polled_gets_every_row()
    {
        var (source, path) = await StartedFileSourceWithAsync("late_file", 500);

        var tableName = await StartRunningTableAsync(source, $"SELECT id, seq, value FROM {source} LATEST BY (id)");
        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        try
        {
            var count = await PollUntilAsync(() => table.GetRowCountAsync(), c => c >= 500, deadlineSeconds: 90);
            Assert.Equal(500, count);

            // Give a (wrong) second delivery of the same 500 a chance to show up before believing the count.
            await Task.Delay(1500);
            Assert.Equal(500, await table.GetRowCountAsync());

            // Live traffic through the SAME subscription the replay just fed: 200 new ids, no re-delivery
            // of the overlapping 500.
            await AppendRowsAsync(path, 500, 200);

            var grown = await PollUntilAsync(() => table.GetRowCountAsync(), c => c >= 700, deadlineSeconds: 90);
            Assert.Equal(700, grown);
            await Task.Delay(1500);
            Assert.Equal(700, await table.GetRowCountAsync());
        }
        finally
        {
            await Registry.DeleteSourceAsync(source);
        }
    }

    /// <summary>Same proof one layer down: a Parallelism &gt;= 2 table never runs StartClassicAsync at all —
    /// its stream inputs are subscribed by <c>TableIngestGrain</c>, which needs the identical attach. A pass
    /// here and a fail there (or the reverse) would mean the fix covers only half the tables in the system.</summary>
    [Fact]
    public async Task Partitioned_table_created_after_the_source_already_polled_also_gets_every_row()
    {
        var (source, path) = await StartedFileSourceWithAsync("late_file_p2", 500);

        var tableName = await StartRunningTableAsync(source, $"SELECT id, seq, value FROM {source} LATEST BY (id)", parallelism: 2);
        var table = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        try
        {
            var count = await PollUntilAsync(() => table.GetRowCountAsync(), c => c >= 500, deadlineSeconds: 90);
            Assert.Equal(500, count);

            await Task.Delay(1500);
            Assert.Equal(500, await table.GetRowCountAsync());

            await AppendRowsAsync(path, 500, 200);

            var grown = await PollUntilAsync(() => table.GetRowCountAsync(), c => c >= 700, deadlineSeconds: 90);
            Assert.Equal(700, grown);
        }
        finally
        {
            await Registry.DeleteSourceAsync(source);
        }
    }

    /// <summary>The pipeline half of the same protocol. <see cref="PipelineMetrics.TotalRowsOut"/> is the
    /// cheap result count that makes this variant possible at all (GetRecentResultsAsync caps at 100, so it
    /// could not tell 500 from 700).</summary>
    [Fact]
    public async Task Pipeline_created_after_a_file_source_already_polled_gets_every_row()
    {
        var (source, path) = await StartedFileSourceWithAsync("late_file_pipe", 500);

        var created = await Registry.CreatePipelineAsync(new PipelineDefinition
        {
            Name = "late_pipe_" + Guid.NewGuid().ToString("n")[..8],
            Sql = $"SELECT id, seq, value FROM {source}",
        });
        await Registry.SetPipelineStatusAsync(created.Id, PipelineStatus.Running);
        var pipeline = _cluster.GrainFactory.GetGrain<IPipelineGrain>(created.Id);

        try
        {
            var metrics = await PollUntilAsync(() => pipeline.GetMetricsAsync(), m => m.TotalRowsOut >= 500, deadlineSeconds: 90);
            Assert.Equal(500, metrics.TotalRowsOut);

            await Task.Delay(1500);
            Assert.Equal(500, (await pipeline.GetMetricsAsync()).TotalRowsOut);

            await AppendRowsAsync(path, 500, 200);

            var grown = await PollUntilAsync(() => pipeline.GetMetricsAsync(), m => m.TotalRowsOut >= 700, deadlineSeconds: 90);
            Assert.Equal(700, grown.TotalRowsOut);
        }
        finally
        {
            await Registry.DeletePipelineAsync(created.Id);
            await Registry.DeleteSourceAsync(source);
        }
    }

    /// <summary>The gate's own contract, at the grain level: while a hold is outstanding the source
    /// publishes nothing, and the deferred rows are released — to every subscriber — when it is dropped.
    /// This is what makes the replay exactly-once for the consumers above, and it is worth pinning
    /// separately because the three consumers all trust it silently.</summary>
    [Fact]
    public async Task An_attach_hold_defers_publishing_and_the_release_delivers_what_was_deferred()
    {
        var name = "attach_gate_" + Guid.NewGuid().ToString("n")[..8];
        var path = Path.Combine(_scratchDir, name + ".ndjson");
        await File.WriteAllTextAsync(path, Ndjson(0, 3));

        var connector = _cluster.GrainFactory.GetGrain<IConnectorGrain>(name);
        try
        {
            // Attach BEFORE the source has ever run: the snapshot is empty, and nothing it emits while the
            // hold is held may reach the stream.
            await connector.StartAsync(FileSource(name, path));
            var beforeAnyPoll = await connector.BeginAttachAsync();
            Assert.Empty(beforeAnyPoll.Rows);
            Assert.Equal(0, beforeAnyPoll.TotalSeen);

            // Let at least one poll cycle run while held. The counters advance (the cycle succeeded); the
            // rows are parked.
            await PollUntilAsync(() => connector.GetStatusAsync(), s => s.EventsEmittedTotal >= 3, deadlineSeconds: 90);

            var stillHeld = await connector.BeginAttachAsync();
            Assert.Empty(stillHeld.Rows); // nothing has been PUBLISHED yet, so nothing is in the ring
            Assert.Equal(0, stillHeld.TotalSeen);

            // Two holds taken, so two releases are needed before anything flows.
            await connector.EndAttachAsync();
            var afterOneRelease = await connector.BeginAttachAsync();
            Assert.Empty(afterOneRelease.Rows);
            await connector.EndAttachAsync();

            await connector.EndAttachAsync(); // the last one — the deferred rows go out here

            var afterRelease = await PollUntilAsync(
                async () => await connector.BeginAttachAsync(),
                s => s.TotalSeen >= 3,
                deadlineSeconds: 15);
            await connector.EndAttachAsync();

            Assert.Equal(3, afterRelease.TotalSeen);
            Assert.Equal(3, afterRelease.Rows.Count);
            Assert.Equal([0L, 1L, 2L], afterRelease.Rows.Select(r => Convert.ToInt64(r["id"])).ToArray());
        }
        finally
        {
            await connector.StopAsync();
        }
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        T last = await poll();
        if (until(last)) return last;
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(200);
        }
        return last;
    }
}
