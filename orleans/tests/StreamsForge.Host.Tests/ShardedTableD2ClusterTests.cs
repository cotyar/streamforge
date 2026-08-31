using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 011 wave D2 — the three things D2 changed about sharded tables, on a real cluster.
///
///  * <b>The duplicate copy is gone</b> (<see cref="ShardedTable_KeepsNoPersistedSnapshotMirror"/>,
///    <see cref="ShardedTable_ResumesAsRebuilding_WithoutTheMirrorToDetectItBy"/>). D1's tier was purely
///    ADDITIVE: <c>TableGrain</c> still held and still persisted a full consolidated snapshot of exactly
///    the rows the shards already hold durably, which is why D1's soak showed RSS going UP. D2 stops
///    keeping it. The subtle part is that a non-empty persisted snapshot was ALSO how a restart was
///    detected, so shedding it has to replace that signal — and both tests below exist because getting
///    the memory back while silently losing "this table is rebuilding" would be a bad trade nobody asked
///    for.
///  * <b>The fenced scan</b> (<see cref="FencedScan_TakenDuringIngest_IsARealCut"/>,
///    <see cref="FencedScan_DoesNotHangOnAShardThatWillNeverReachTheFenceSequence"/>). The second of those
///    is the one worth reading: the obvious design — every shard waits until its own AppliedSeq reaches
///    the fence — deadlocks on the most ordinary configuration there is, a key that has simply seen no
///    traffic lately.
///  * <b>Renaming a sharded table is refused</b> (<see cref="RenamingAShardedTable_IsRefused"/>), because
///    the tier is keyed by name and a rename would strand every shard silently.
///
/// The cut is asserted by an arithmetic identity rather than by eyeballing rows, which is what makes it a
/// real test: every delta the router forwards goes to exactly one shard, so summed over every shard of a
/// table, <c>DeltasApplied</c> must equal the router's own <c>RoutedDeltasAtFence</c> — not one delta from
/// at-or-before the fence missing, and not one from after it counted. Under a scan that is NOT a cut, that
/// sum drifts above the fence's count as ingest continues mid-scan.
/// </summary>
public sealed class ShardedTableD2ClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<ShardTestSiloConfigurator>();
        builder.AddClientBuilderConfigurator<ShardTestClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    // ------------------------------------------------------------------
    // Fixtures — the same instrument-with-legs shape wave D was built for.
    // ------------------------------------------------------------------

    private const string LegSql =
        "SELECT instrument, leg, stage, notional FROM __SOURCE__ LATEST BY (instrument, leg)";

    private IRegistryGrain Registry => _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);

    private async Task<string> SeedSourceAsync()
    {
        var sourceName = "d2src_" + Guid.NewGuid().ToString("n")[..8];
        await Registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "plan 011 D2 test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false,
            Fields =
            [
                new FieldDef("instrument", FieldType.String),
                new FieldDef("leg", FieldType.Long),
                new FieldDef("stage", FieldType.String),
                new FieldDef("notional", FieldType.Double),
            ],
        });
        return sourceName;
    }

    private Task PublishAsync(string sourceName, string instrument, long leg, string stage, double notional) =>
        _cluster.Client.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, sourceName))
            .OnNextAsync(new EventRecord
            {
                [EventRecord.TimestampField] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                [EventRecord.SourceField] = sourceName,
                ["instrument"] = instrument,
                ["leg"] = leg,
                ["stage"] = stage,
                ["notional"] = notional,
            });

    private async Task<TableDefinition> CreateTableAsync(
        string sourceName, string prefix, List<string> shardBy, TablePersistenceMode persistence = TablePersistenceMode.Batched)
    {
        var created = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = prefix + "_" + Guid.NewGuid().ToString("n")[..8],
            Sql = LegSql.Replace("__SOURCE__", sourceName),
            ShardBy = shardBy,
            HistoryEnabled = true,
            HistoryMode = TableHistoryMode.All,
            Persistence = persistence,
            // Short enough that a test does not spend its life waiting for the write-behind timer, long
            // enough that it is still a write-BEHIND rather than a write-through.
            FlushMs = 300,
        });
        await Registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        return created;
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> poll, Func<T, bool> until, int deadlineSeconds = 15)
    {
        var deadline = DateTime.UtcNow.AddSeconds(deadlineSeconds);
        var last = await poll();
        while (DateTime.UtcNow < deadline)
        {
            last = await poll();
            if (until(last)) return last;
            await Task.Delay(100);
        }
        return last;
    }

    // ------------------------------------------------------------------
    // Part 1 — the duplicate copy
    // ------------------------------------------------------------------

    /// <summary>
    /// The heart of D2. Two tables built from the same SQL over the same input, differing only in ShardBy,
    /// are both STOPPED — which tears the executor down and takes a final flush on the way out — and then
    /// asked for their rows.
    ///
    /// A stopped table has no executor, so whatever it answers with comes from the persisted mirror and
    /// nowhere else. The unsharded one still has one and still answers with its rows; the sharded one
    /// answers empty, because it no longer keeps a mirror at all. That asymmetry IS the memory D2
    /// reclaims: one full copy of every row, rewritten on every flush and resident for as long as the
    /// grain is.
    ///
    /// The honest consequence, asserted here rather than discovered later: a STOPPED sharded table reports
    /// zero rows where a stopped unsharded one reports its last snapshot. The rows are not lost — they are
    /// in the shards, and a per-key lookup returns them, which the second half of this test checks. What
    /// is gone is the convenience of the table grain answering a keyless listing while stopped.
    /// </summary>
    [Fact]
    public async Task ShardedTable_KeepsNoPersistedSnapshotMirror()
    {
        var sourceName = await SeedSourceAsync();
        var plain = await CreateTableAsync(sourceName, "d2_plain", []);
        var sharded = await CreateTableAsync(sourceName, "d2_sharded", ["instrument"]);

        foreach (var instrument in new[] { "XS100", "XS200" })
        {
            foreach (var leg in new long[] { 1, 2 })
            {
                await PublishAsync(sourceName, instrument, leg, "Settled", 1_000 * leg);
            }
        }

        var plainGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(plain.Name);
        var shardedGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(sharded.Name);
        await PollUntilAsync(() => plainGrain.GetRowCountAsync(), c => c == 4);
        await PollUntilAsync(() => shardedGrain.GetRowCountAsync(), c => c == 4);

        // Both agree while running — the sharded one now serves rows live from its executor rather than
        // from the (absent) mirror, which is fresher, not staler.
        Assert.Equal(4, await plainGrain.GetRowCountAsync());
        Assert.Equal(4, await shardedGrain.GetRowCountAsync());

        await Registry.SetTableStatusAsync(plain.Id, PipelineStatus.Stopped);
        await Registry.SetTableStatusAsync(sharded.Id, PipelineStatus.Stopped);

        // Stopped: no executor, so this can only be the persisted mirror.
        Assert.Equal(4, await plainGrain.GetRowCountAsync());
        Assert.Equal(0, await shardedGrain.GetRowCountAsync());

        // ...and the rows are still there, in the tier that is supposed to hold them.
        var key = TableShardKeys.EncodeShardKey(
            new Dictionary<string, object?> { ["instrument"] = "XS100" }, ["instrument"]);
        var view = await _cluster.GrainFactory
            .GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(sharded.Name, key)).GetViewAsync(0);
        Assert.True(view.Found);
        Assert.Equal(2, view.Rows.Count); // both legs of XS100
    }

    /// <summary>
    /// Shedding the mirror sheds the thing restart-resume was detected BY: a non-empty persisted snapshot
    /// is how StartClassicAsync tells a resume from a first-ever start, and marking the table Rebuilding is
    /// the whole point of that distinction (operator state — join indexes, LATEST BY's current row — cannot
    /// be rebuilt from output rows, so the table honestly says so and rebuilds from live traffic).
    ///
    /// D2 replaces the signal with an O(1) persisted boolean rather than dropping it, and this is where
    /// that is checked end to end: stop, restart, and the table must still say Rebuilding. It must also
    /// STOP saying it once real traffic arrives, or the flag would be permanently stuck on and worthless.
    ///
    /// The shard tier itself deliberately survives the restart with its trails intact — it is the durable
    /// per-key copy, and RegistryGrain resumes (never resets) a sharded table's router for exactly that
    /// reason. So a per-key lookup answers with pre-restart data while the table view is still rebuilding.
    /// That is the design, not a bug: the shard tier is more durable than the table it mirrors.
    /// </summary>
    [Fact]
    public async Task ShardedTable_ResumesAsRebuilding_WithoutTheMirrorToDetectItBy()
    {
        var sourceName = await SeedSourceAsync();
        var sharded = await CreateTableAsync(sourceName, "d2_resume", ["instrument"]);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(sharded.Name);

        await PublishAsync(sourceName, "XS900", 1, "New", 10);
        await PublishAsync(sourceName, "XS900", 1, "Settled", 10);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 1);
        Assert.False((await grain.GetMetricsAsync()).Rebuilding);

        // Give the write-behind flush a tick to persist the HadRows marker, then bounce the table.
        await Task.Delay(600);
        await Registry.SetTableStatusAsync(sharded.Id, PipelineStatus.Stopped);
        await Registry.SetTableStatusAsync(sharded.Id, PipelineStatus.Running);

        var metrics = await grain.GetMetricsAsync();
        Assert.True(metrics.Rebuilding, "a sharded table that had rows before the restart must still resume as Rebuilding");
        Assert.Equal(0, metrics.RowCount); // reset to empty and rebuilding from live traffic, exactly as before D2

        // The shard tier kept the key's rows AND its whole version trail across the restart.
        var key = TableShardKeys.EncodeShardKey(
            new Dictionary<string, object?> { ["instrument"] = "XS900" }, ["instrument"]);
        var view = await _cluster.GrainFactory
            .GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(sharded.Name, key)).GetViewAsync(0);
        Assert.True(view.Found);
        Assert.Single(view.Rows);
        Assert.Equal(2, view.History.Sum(h => h.Versions.Count));

        // Live traffic clears Rebuilding and the table comes back.
        await PublishAsync(sourceName, "XS901", 1, "New", 10);
        var after = await PollUntilAsync(() => grain.GetMetricsAsync(), m => !m.Rebuilding && m.RowCount > 0);
        Assert.False(after.Rebuilding);
        Assert.Equal(1, after.RowCount);
    }

    /// <summary>A table that has NEVER been sharded is byte-identically unaffected: it still detects its
    /// resume from the mirror it still keeps. The guard exists because D2's resume detection now checks two
    /// markers, and a change that made the unsharded path depend on the new one would be a silent
    /// regression on every existing table in every existing data dir.</summary>
    [Fact]
    public async Task UnshardedTable_StillResumesFromItsOwnMirror()
    {
        var sourceName = await SeedSourceAsync();
        var plain = await CreateTableAsync(sourceName, "d2_plain_resume", []);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(plain.Name);

        await PublishAsync(sourceName, "XS800", 1, "New", 10);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 1);
        await Task.Delay(600);

        await Registry.SetTableStatusAsync(plain.Id, PipelineStatus.Stopped);
        await Registry.SetTableStatusAsync(plain.Id, PipelineStatus.Running);

        Assert.True((await grain.GetMetricsAsync()).Rebuilding);
    }

    /// <summary>Plan 011 D2: <c>MemoryOnly</c> + <c>ShardBy</c> is refused at upsert. D1 allowed it and
    /// documented the consequence; D2 is what made it indefensible, because with the table's own mirror
    /// gone there is nothing at all behind a shard that never writes — and a shard's write on deactivation
    /// is not a durability nicety, it IS the swap-out. Refusing beats redefining a mode whose contract says
    /// "a RESTART brings the table back empty", not "an idle minute loses this key".</summary>
    [Fact]
    public async Task MemoryOnlyPersistence_CombinedWithShardBy_IsRefused()
    {
        var sourceName = await SeedSourceAsync();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.CreateTableAsync(new TableDefinition
        {
            Name = "d2_memonly_" + Guid.NewGuid().ToString("n")[..8],
            Sql = LegSql.Replace("__SOURCE__", sourceName),
            ShardBy = ["instrument"],
            Persistence = TablePersistenceMode.MemoryOnly,
        }));

        Assert.Contains("MemoryOnly", ex.Message, StringComparison.Ordinal);
        Assert.Contains("shardBy", ex.Message, StringComparison.Ordinal);

        // The same table without sharding is of course fine — the refusal is about the COMBINATION.
        var ok = await Registry.CreateTableAsync(new TableDefinition
        {
            Name = "d2_memonly_ok_" + Guid.NewGuid().ToString("n")[..8],
            Sql = LegSql.Replace("__SOURCE__", sourceName),
            Persistence = TablePersistenceMode.MemoryOnly,
        });
        Assert.Equal(TablePersistenceMode.MemoryOnly, ok.Persistence);
    }

    /// <summary>Plan 011 D2: a sharded table's <c>Journaled</c> mode is accepted and behaves — the journal
    /// simply has nothing to journal, because the state it exists to make cheap to write is no longer
    /// written at all, and each shard's own write is already O(one key). This pins that the combination
    /// works rather than throwing or silently producing an empty table.</summary>
    [Fact]
    public async Task JournaledPersistence_CombinedWithShardBy_StillProducesTheRightRows()
    {
        var sourceName = await SeedSourceAsync();
        var table = await CreateTableAsync(sourceName, "d2_journaled", ["instrument"], TablePersistenceMode.Journaled);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(table.Name);

        await PublishAsync(sourceName, "XS700", 1, "New", 10);
        await PublishAsync(sourceName, "XS700", 2, "New", 20);
        await PublishAsync(sourceName, "XS701", 1, "New", 30);

        Assert.Equal(3, await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 3));

        var directory = _cluster.GrainFactory.GetGrain<ITableShardDirectoryGrain>(table.Name);
        Assert.Equal(2, await PollUntilAsync(() => directory.GetCountAsync(), c => c == 2));
    }

    // ------------------------------------------------------------------
    // Part 2 — the fenced scan
    // ------------------------------------------------------------------

    /// <summary>
    /// A fenced scan taken WHILE events keep arriving is a genuine consistent cut, checked by identity
    /// rather than by inspection: every delta the router forwards belongs to exactly one shard, so across
    /// every shard of the table the applied-delta counts must sum to precisely the number of deltas the
    /// router had forwarded at the fence. One delta from before the fence missing, or one from after it
    /// counted, and the sum is wrong.
    ///
    /// Repeated several times during continuous ingest, because a cut that only holds when nothing is
    /// happening is not a cut.
    /// </summary>
    [Fact]
    public async Task FencedScan_TakenDuringIngest_IsARealCut()
    {
        var sourceName = await SeedSourceAsync();
        var table = await CreateTableAsync(sourceName, "d2_fence", ["instrument"]);
        var router = _cluster.GrainFactory.GetGrain<ITableShardRouterGrain>(table.Name);
        var instruments = Enumerable.Range(0, 12).Select(i => $"XS{i:000}").ToArray();

        // Prime every key so the directory is populated before the first cut.
        foreach (var instrument in instruments)
        {
            await PublishAsync(sourceName, instrument, 1, "New", 1);
        }
        await PollUntilAsync(
            () => _cluster.GrainFactory.GetGrain<ITableShardDirectoryGrain>(table.Name).GetCountAsync(),
            c => c == instruments.Length);

        using var stop = new CancellationTokenSource();
        var ingest = Task.Run(async () =>
        {
            var n = 0;
            while (!stop.IsCancellationRequested)
            {
                var instrument = instruments[n % instruments.Length];
                await PublishAsync(sourceName, instrument, 1 + n % 3, $"Stage{n}", n);
                n++;
                await Task.Delay(5);
            }
        });

        long firstRouted = -1, lastRouted = -1;
        try
        {
            for (var attempt = 0; attempt < 5; attempt++)
            {
                var cut = await router.FencedScanAsync(1000, 0);
                if (firstRouted < 0) firstRouted = cut.RoutedDeltasAtFence;
                lastRouted = cut.RoutedDeltasAtFence;

                Assert.True(cut.FenceSeq >= 0, "something must have been routed by now");
                Assert.Equal(cut.ShardCount, cut.Shards.Count); // the page covers every shard, so the sum is comparable
                Assert.Equal(cut.RoutedDeltasAtFence, cut.Shards.Sum(s => s.DeltasApplied));
                Assert.All(cut.Shards, s => Assert.True(
                    s.AppliedSeq <= cut.FenceSeq,
                    $"shard '{s.ShardKey}' reported AppliedSeq {s.AppliedSeq} beyond the fence {cut.FenceSeq}"));

                await Task.Delay(60);
            }
        }
        finally
        {
            await stop.CancelAsync();
            await ingest;
        }

        // Guards the test against measuring a quiescent system: the fence has to have been taken WHILE the
        // table was moving, or the identity above would hold for trivial reasons.
        Assert.True(lastRouted > firstRouted,
            $"ingest must have advanced across the scans (first {firstRouted}, last {lastRouted})");
    }

    /// <summary>
    /// THE CASE THAT KILLS THE OBVIOUS DESIGN. The sequence is per TABLE and stamped per forwarded batch,
    /// so a shard whose key has seen no traffic since sequence 3 sits at AppliedSeq 3 forever while the
    /// table's sequence runs away. A fence implemented as "every shard waits until its AppliedSeq reaches
    /// S" therefore hangs on the single most ordinary configuration there is — one hot key and one quiet
    /// one — and it would hang for good, not merely slowly.
    ///
    /// D2's fence waits nowhere. The router is the fence (it cannot forward while the scan runs), so every
    /// shard is already exactly at the cut and answers immediately, reporting its own honest AppliedSeq.
    /// The idle shard's state at sequence 3 IS its state at the fence — there was never anything to wait
    /// for. Asserted with a hard timeout, since "does not hang" is the entire claim.
    /// </summary>
    [Fact]
    public async Task FencedScan_DoesNotHangOnAShardThatWillNeverReachTheFenceSequence()
    {
        var sourceName = await SeedSourceAsync();
        var table = await CreateTableAsync(sourceName, "d2_idle", ["instrument"]);
        var router = _cluster.GrainFactory.GetGrain<ITableShardRouterGrain>(table.Name);

        await PublishAsync(sourceName, "IDLE", 1, "New", 1);
        await PollUntilAsync(
            () => _cluster.GrainFactory.GetGrain<ITableShardDirectoryGrain>(table.Name).GetCountAsync(),
            c => c == 1);

        // Now drive one OTHER key hard, so the table's sequence leaves IDLE far behind.
        for (var i = 0; i < 60; i++)
        {
            await PublishAsync(sourceName, "HOT", 1, $"Stage{i}", i);
        }
        await PollUntilAsync(
            () => _cluster.GrainFactory.GetGrain<ITableShardDirectoryGrain>(table.Name).GetCountAsync(),
            c => c == 2);
        await PollUntilAsync(() => router.GetInfoAsync(), i => i.RoutedDeltas >= 60);

        var scan = router.FencedScanAsync(100, 0);
        var finished = await Task.WhenAny(scan, Task.Delay(TimeSpan.FromSeconds(20)));
        Assert.Same(scan, finished); // it returned; it did not wait for IDLE to catch up

        var cut = await scan;
        var idle = cut.Shards.Single(s => s.ShardKey.Contains("IDLE", StringComparison.Ordinal));
        var hot = cut.Shards.Single(s => s.ShardKey.Contains("HOT", StringComparison.Ordinal));

        Assert.Equal(1, idle.DeltasApplied);
        Assert.True(idle.AppliedSeq < hot.AppliedSeq, "the idle shard's sequence legitimately lags — that is the point");
        Assert.True(idle.AppliedSeq < cut.FenceSeq, "and it lags the fence, without that being an error");
        Assert.Equal(cut.RoutedDeltasAtFence, cut.Shards.Sum(s => s.DeltasApplied)); // still an exact cut
    }

    /// <summary>A fenced scan of a table nothing has been routed to yet answers honestly — no shards, and
    /// a fence of -1 saying "no sequence has ever been stamped" — rather than blocking or inventing a
    /// sequence the tier has never seen.</summary>
    [Fact]
    public async Task FencedScan_OnATableWithNoTrafficYet_ReportsAnEmptyCut()
    {
        var sourceName = await SeedSourceAsync();
        var table = await CreateTableAsync(sourceName, "d2_quiet", ["instrument"]);

        var cut = await _cluster.GrainFactory.GetGrain<ITableShardRouterGrain>(table.Name).FencedScanAsync(100, 0);

        Assert.Equal(-1, cut.FenceSeq);
        Assert.Empty(cut.Shards);
        Assert.Equal(0, cut.ShardCount);
    }

    // ------------------------------------------------------------------
    // Part 4 — renaming
    // ------------------------------------------------------------------

    /// <summary>
    /// The shard tier's grain keys are derived from the table's NAME, so renaming a sharded table would
    /// leave every shard's rows and version trails filed under a key nothing ever looks up again — with
    /// nothing failing and nothing logged, which is the worst shape a data loss can take. D2 refuses the
    /// rename, and the refusal names the way around it.
    ///
    /// Two guards, not one: the refusal must not leak onto unsharded tables (they rename exactly as
    /// before), and clearing ShardBy — which deletes the shards explicitly and visibly — must genuinely
    /// unlock the rename rather than leaving the table permanently pinned to its first name.
    /// </summary>
    [Fact]
    public async Task RenamingAShardedTable_IsRefused()
    {
        var sourceName = await SeedSourceAsync();
        var sharded = await CreateTableAsync(sourceName, "d2_rename", ["instrument"]);

        await PublishAsync(sourceName, "XS600", 1, "New", 5);
        await PollUntilAsync(
            () => _cluster.GrainFactory.GetGrain<ITableShardDirectoryGrain>(sharded.Name).GetCountAsync(),
            c => c == 1);

        var renamed = CloneWithName(sharded, sharded.Name + "_renamed");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Registry.UpdateTableAsync(renamed));
        Assert.Contains("cannot be renamed", ex.Message, StringComparison.Ordinal);

        // The refusal left nothing half-applied: the stored table is untouched and its shards are intact.
        var stored = (await Registry.GetTablesAsync()).Single(t => t.Id == sharded.Id);
        Assert.Equal(sharded.Name, stored.Name);
        Assert.Equal(1, await _cluster.GrainFactory.GetGrain<ITableShardDirectoryGrain>(sharded.Name).GetCountAsync());

        // Un-shard (which deletes the shards, explicitly), then the rename goes through.
        var unsharded = CloneWithName(sharded, sharded.Name);
        unsharded.ShardBy = [];
        await Registry.UpdateTableAsync(unsharded);

        // PLAN 016 WAVE 1-C: …after stopping it. The rename policy grew two conditions beyond D2's
        // ShardBy one — Stopped, and no other table listing it in TableInputs — because a rename is
        // just as unsafe against a LIVE table grain, history grain and delta stream, all also keyed by
        // name (D2's own closing note said as much and left it out of scope). The un-shard update above
        // restarts a Running table, so it has to be stopped again here. What this test is about is
        // unchanged: clearing ShardBy genuinely unlocks the rename rather than pinning the table to its
        // first name forever.
        await Registry.SetTableStatusAsync(sharded.Id, PipelineStatus.Stopped);

        var nowRenamed = CloneWithName(unsharded, sharded.Name + "_renamed");
        var saved = await Registry.UpdateTableAsync(nowRenamed);
        Assert.NotNull(saved);
        Assert.Equal(sharded.Name + "_renamed", saved!.Name);
    }

    [Fact]
    public async Task RenamingAnUnshardedTable_IsUnaffected()
    {
        var sourceName = await SeedSourceAsync();
        var plain = await CreateTableAsync(sourceName, "d2_plain_rename", []);

        // PLAN 016 WAVE 1-C: CreateTableAsync above starts the table, and a Running table is no longer
        // renameable on ANY flavour — see the note in RenamingAShardedTable_IsRefused. The assertion this
        // test exists to make is untouched: the SHARDED refusal does not leak onto an unsharded table.
        await Registry.SetTableStatusAsync(plain.Id, PipelineStatus.Stopped);

        var saved = await Registry.UpdateTableAsync(CloneWithName(plain, plain.Name + "_renamed"));

        Assert.NotNull(saved);
        Assert.Equal(plain.Name + "_renamed", saved!.Name);
    }

    /// <summary>UpdateTableAsync replaces the whole record (plan 009 E), so an update has to resend every
    /// client-owned field — this mirrors what the REST PUT handler does.</summary>
    private static TableDefinition CloneWithName(TableDefinition def, string name) => new()
    {
        Id = def.Id,
        Name = name,
        Description = def.Description,
        Sql = def.Sql,
        HistoryEnabled = def.HistoryEnabled,
        HistoryMode = def.HistoryMode,
        HistoryLimit = def.HistoryLimit,
        HistoryByField = def.HistoryByField,
        HistoryWindowMs = def.HistoryWindowMs,
        Parallelism = def.Parallelism,
        Persistence = def.Persistence,
        FlushMs = def.FlushMs,
        JournalMaxEntries = def.JournalMaxEntries,
        RetentionMaxRows = def.RetentionMaxRows,
        RetentionTtlMs = def.RetentionTtlMs,
        ShardBy = [.. def.ShardBy],
    };
}
