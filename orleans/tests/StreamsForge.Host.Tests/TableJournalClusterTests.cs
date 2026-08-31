using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.TestingHost;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Grains;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 009 A2: <see cref="TablePersistenceMode.Journaled"/> — a second, small persisted state
/// ("table-journal") that lets a flush write only the rows that changed since the last compaction instead
/// of the whole snapshot (see TableGrain's Plan 009 A2 class-doc paragraph for the full design). Reuses the
/// real-<c>JsonFileGrainStorage</c>-backed cluster infrastructure from <see cref="TablePersistenceModeClusterTests"/>
/// (<see cref="PersistenceModeTestSiloConfigurator"/>/<see cref="PersistenceModeTestClientConfigurator"/>/
/// <see cref="PersistenceModeTestRegistry"/>, all `internal` — cross-file reusable within this assembly) so
/// these tests can inspect real on-disk journal/snapshot files exactly like that file's own tests inspect
/// "table"/"tableHistory" files — but keeps its OWN copies of the small per-test helper methods
/// (SeedTradesTableAsync/PublishTradeAsync/PollUntilAsync-equivalents), which were `private` there and are
/// duplicated here rather than requiring an edit to that existing file.
/// </summary>
public sealed class TableJournalClusterTests : IAsyncLifetime
{
    private TestCluster _cluster = null!;
    private string _testId = null!;
    private string _dataDir = null!;

    public async Task InitializeAsync()
    {
        _testId = Guid.NewGuid().ToString("n");
        _dataDir = Path.Combine(Path.GetTempPath(), "sf-journal-tests", _testId);
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

        // Force the lazy IGrainStorage factory to run (see TablePersistenceModeClusterTests.InitializeAsync's
        // identical comment) — this test doesn't need the DelayableGrainStorage handle itself, only the real
        // JsonFileGrainStorage it wraps, which every table created below writes through.
        await _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey).GetTablesAsync();
    }

    public async Task DisposeAsync()
    {
        await _cluster.DisposeAsync();
        PersistenceModeTestRegistry.Storages.TryRemove(_testId, out _);
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private async Task<(IRegistryGrain Registry, string SourceName, TableDefinition Created)> SeedTradesTableAsync(TableDefinition tableDef)
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sourceName = "trades_" + Guid.NewGuid().ToString("n")[..8];
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
        tableDef.Sql = tableDef.Sql.Replace("__SOURCE__", sourceName);
        var created = await registry.CreateTableAsync(tableDef);
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);
        return (registry, sourceName, created);
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

    /// <summary>Same JsonFileGrainStorage naming convention TablePersistenceModeClusterTests relies on
    /// ("{stateName}.{sanitized grainId}.json" under "{dataDir}/state/") — "table." doesn't collide with
    /// "table-journal." (the literal "." right after "table" in the glob only matches a state name that is
    /// exactly "table", not a prefix of a longer one).</summary>
    private bool AnyPersistedTableFileFor(string tableName)
    {
        var stateDir = Path.Combine(_dataDir, "state");
        return Directory.Exists(stateDir) && Directory.GetFiles(stateDir, $"table.*{tableName}*.json").Length > 0;
    }

    private string? FindStateFile(string statePrefix, string tableName)
    {
        var stateDir = Path.Combine(_dataDir, "state");
        if (!Directory.Exists(stateDir)) return null;
        return Directory.GetFiles(stateDir, $"{statePrefix}.*{tableName}*.json").FirstOrDefault();
    }

    /// <summary>Reads a JSON state file's given top-level array/object property's member count, tolerating a
    /// concurrent in-progress write (JsonFileGrainStorage does a plain File.Create + async serialize, not an
    /// atomic write-then-rename — see its own doc comment — so a read racing a write can transiently see the
    /// file locked or briefly truncated/invalid JSON). Returns -1 (a sentinel no real count ever is) on
    /// either failure so PollUntilAsync's retry loop simply tries again instead of the test flaking.</summary>
    private static int TryReadPropertyCount(string path, string propertyName)
    {
        try
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var doc = JsonDocument.Parse(stream);
            return doc.RootElement.TryGetProperty(propertyName, out var prop) ? prop.EnumerateObject().Count() : 0;
        }
        catch (IOException)
        {
            return -1;
        }
        catch (JsonException)
        {
            return -1;
        }
    }

    /// <summary>Reads the persisted "table-journal" state file's Entries count directly — 0 if the table has
    /// never Journal-flushed yet (no file) or the journal is currently empty (present, but an empty object).</summary>
    private int ReadPersistedJournalEntryCount(string tableName)
    {
        var path = FindStateFile("table-journal", tableName);
        return path is null ? 0 : TryReadPropertyCount(path, "Entries");
    }

    /// <summary>Reads the persisted "table" state file's Snapshot count directly.</summary>
    private int ReadPersistedSnapshotEntryCount(string tableName)
    {
        var path = FindStateFile("table", tableName);
        return path is null ? 0 : TryReadPropertyCount(path, "Snapshot");
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

    /// <summary>Row content signature for the byte-identical comparison — excludes EventRecord's own
    /// bookkeeping fields (<see cref="EventRecord.SourceField"/>/<see cref="EventRecord.TimestampField"/>,
    /// both prefixed "_"), which legitimately differ between the journaled and batched tables in
    /// <see cref="Journaled_And_Batched_RestartResume_ByteIdenticalRows"/> (two independent source streams
    /// feeding otherwise-identical content) — only the actual SELECTed columns and the weight matter for
    /// "byte-identical rows".</summary>
    private static string RowSignature(TableRowDto row) =>
        string.Join(",", row.Row.Where(kv => !kv.Key.StartsWith('_')).OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => $"{kv.Key}={kv.Value}")) + $";weight={row.Weight}";

    [Fact]
    public async Task Journaled_And_Batched_RestartResume_ByteIdenticalRows()
    {
        var journaledName = "jrnl_journaled_" + Guid.NewGuid().ToString("n")[..8];
        var batchedName = "jrnl_batched_" + Guid.NewGuid().ToString("n")[..8];

        var journaledDef = new TableDefinition
        {
            Name = journaledName,
            Sql = "SELECT symbol, COUNT(*) AS trades, SUM(qty) AS total_qty FROM __SOURCE__ GROUP BY symbol",
            Persistence = TablePersistenceMode.Journaled,
            FlushMs = 50,
        };
        var batchedDef = new TableDefinition
        {
            Name = batchedName,
            Sql = "SELECT symbol, COUNT(*) AS trades, SUM(qty) AS total_qty FROM __SOURCE__ GROUP BY symbol",
            Persistence = TablePersistenceMode.Batched,
            FlushMs = 50,
        };

        var (registry, journaledSource, journaledCreated) = await SeedTradesTableAsync(journaledDef);
        var (_, batchedSource, batchedCreated) = await SeedTradesTableAsync(batchedDef);

        var journaledGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(journaledName);
        var batchedGrain = _cluster.GrainFactory.GetGrain<ITableGrain>(batchedName);

        // Identical delta stream fed to both tables (via two independent, but identically-driven, sources —
        // the row CONTENT produced is what's compared, not the source identity).
        var preRestart = new (string Symbol, double Price, long Qty)[]
        {
            ("AAPL", 100, 10), ("MSFT", 50, 3), ("AAPL", 101, 5), ("GOOG", 200, 1),
        };
        foreach (var (symbol, price, qty) in preRestart)
        {
            await PublishTradeAsync(journaledSource, symbol, price, qty);
            await PublishTradeAsync(batchedSource, symbol, price, qty);
        }

        await PollUntilAsync(() => journaledGrain.GetRowCountAsync(), c => c == 3, deadlineSeconds: 15);
        await PollUntilAsync(() => batchedGrain.GetRowCountAsync(), c => c == 3, deadlineSeconds: 15);

        // Final flush on stop, awaited synchronously regardless of FlushMs (mirrors
        // TablePersistenceModeClusterTests.Batched_Default_RestartsAndResumesFromPersistedSnapshot...).
        await registry.SetTableStatusAsync(journaledCreated.Id, PipelineStatus.Stopped);
        await registry.SetTableStatusAsync(batchedCreated.Id, PipelineStatus.Stopped);
        Assert.True(AnyPersistedTableFileFor(batchedName));
        // The journaled table's final flush writes only the (small) journal, not the whole "table" state —
        // with only 3 distinct groups and JournalMaxEntries left at its default (200), no compaction has
        // happened yet, so "table" legitimately has no file at all yet; the journal is the persisted
        // evidence instead.
        Assert.True(ReadPersistedJournalEntryCount(journaledName) > 0);

        await registry.SetTableStatusAsync(journaledCreated.Id, PipelineStatus.Running);
        await registry.SetTableStatusAsync(batchedCreated.Id, PipelineStatus.Running);

        // Both must report the IDENTICAL documented restart-resume contract (TableGrain's class doc):
        // Rebuilding=true, RowCount==0 immediately after resume — Journaled must be byte-identical to
        // Batched here, not a different (better OR worse) contract.
        var journaledAfterRestart = await journaledGrain.GetMetricsAsync();
        var batchedAfterRestart = await batchedGrain.GetMetricsAsync();
        Assert.True(journaledAfterRestart.Rebuilding);
        Assert.True(batchedAfterRestart.Rebuilding);
        Assert.Equal(0, journaledAfterRestart.RowCount);
        Assert.Equal(0, batchedAfterRestart.RowCount);

        // Feed the SAME post-restart sequence to both.
        var postRestart = new (string Symbol, double Price, long Qty)[] { ("AAPL", 102, 2), ("TSLA", 300, 7) };
        foreach (var (symbol, price, qty) in postRestart)
        {
            await PublishTradeAsync(journaledSource, symbol, price, qty);
            await PublishTradeAsync(batchedSource, symbol, price, qty);
        }

        await PollUntilAsync(() => journaledGrain.GetRowCountAsync(), c => c == 2, deadlineSeconds: 15);
        await PollUntilAsync(() => batchedGrain.GetRowCountAsync(), c => c == 2, deadlineSeconds: 15);

        var journaledRows = await journaledGrain.GetRowsAsync(100, 0);
        var batchedRows = await batchedGrain.GetRowsAsync(100, 0);

        var journaledSet = journaledRows.Select(RowSignature).OrderBy(s => s, StringComparer.Ordinal).ToList();
        var batchedSet = batchedRows.Select(RowSignature).OrderBy(s => s, StringComparer.Ordinal).ToList();
        Assert.Equal(batchedSet, journaledSet);
    }

    [Fact]
    public async Task Journaled_CompactionTriggersAtThreshold_AndTruncatesJournal()
    {
        var tableName = "jrnl_compact_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM __SOURCE__ GROUP BY symbol",
            Persistence = TablePersistenceMode.Journaled,
            FlushMs = 50,
            JournalMaxEntries = 3,
        };
        var (_, sourceName, _) = await SeedTradesTableAsync(def);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        // 3 distinct symbols -> 3 distinct journal entries; the flush that reaches the threshold (>= 3)
        // must trigger a compaction, truncating the journal back to empty.
        foreach (var symbol in new[] { "AAPL", "MSFT", "GOOG" })
        {
            await PublishTradeAsync(sourceName, symbol, 10, 1);
        }
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 3, deadlineSeconds: 15);

        // First wait for the journal to become genuinely non-empty (proves a real pre-compaction flush
        // actually landed on disk — "0" is ALSO what a not-yet-created file reads as, so polling straight
        // for 0 could trivially "pass" before any flush ever ran at all), THEN wait for it to drop back to
        // 0 (proves a real compaction truncated it, not just an absent file).
        await PollUntilAsync(() => Task.FromResult(ReadPersistedJournalEntryCount(tableName)), c => c > 0, deadlineSeconds: 15);
        var truncatedCount = await PollUntilAsync(
            () => Task.FromResult(ReadPersistedJournalEntryCount(tableName)),
            count => count == 0,
            deadlineSeconds: 15);
        Assert.Equal(0, truncatedCount);

        // The full snapshot file must reflect all 3 rows post-compaction (CompactAsync's own full write).
        Assert.True(AnyPersistedTableFileFor(tableName));
        Assert.Equal(3, ReadPersistedSnapshotEntryCount(tableName));
    }

    [Fact]
    public async Task Journaled_FlushWriteVolume_IsSmallerThanFullSnapshot_AfterCompaction()
    {
        var tableName = "jrnl_small_" + Guid.NewGuid().ToString("n")[..8];
        var def = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM __SOURCE__ GROUP BY symbol",
            Persistence = TablePersistenceMode.Journaled,
            FlushMs = 50,
            JournalMaxEntries = 5,
        };
        var (_, sourceName, _) = await SeedTradesTableAsync(def);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        var symbols = new[] { "AAPL", "MSFT", "GOOG", "TSLA", "AMZN" };
        foreach (var s in symbols)
        {
            await PublishTradeAsync(sourceName, s, 10, 1);
        }
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == symbols.Length, deadlineSeconds: 15);

        // 5 distinct keys hits JournalMaxEntries=5 exactly -> a compaction must have run, truncating the
        // journal back to empty first (same assertion as the compaction test above — needed here as a
        // precondition so the NEXT flush's entry count isn't inflated by the initial population). Wait for
        // a genuinely non-empty journal FIRST — "0" is also what a not-yet-created file reads as, so polling
        // straight for 0 could trivially pass before any flush (let alone a compaction) ever ran.
        await PollUntilAsync(() => Task.FromResult(ReadPersistedJournalEntryCount(tableName)), c => c > 0, deadlineSeconds: 15);
        await PollUntilAsync(() => Task.FromResult(ReadPersistedJournalEntryCount(tableName)), c => c == 0, deadlineSeconds: 15);

        // One more trade for a BRAND NEW symbol — a pure insert, touching exactly one new canonical row (no
        // retraction of anything: CanonicalRowKey is content-based — StreamsForge.Engine.PublicApi's
        // TableExecutor.CanonicalRowKey serializes the whole row, so updating an EXISTING group's aggregate
        // would itself retract the old content-keyed row and assert a new one, i.e. touch two keys — a new
        // group is the clean, unambiguous "exactly one row changed" case). The journal write for THIS flush
        // must hold exactly ONE entry, not the whole 6-row table: that is the O(changed) claim under test —
        // assert on entry counts, not wall-clock, per the plan's own verify gate.
        await PublishTradeAsync(sourceName, "NFLX", 20, 1);
        var journalCountAfterOneChange = await PollUntilAsync(
            () => Task.FromResult(ReadPersistedJournalEntryCount(tableName)), c => c == 1, deadlineSeconds: 15);

        var fullRowCount = await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 6, deadlineSeconds: 15);
        Assert.Equal(6, fullRowCount);
        Assert.Equal(1, journalCountAfterOneChange);
        Assert.True(journalCountAfterOneChange < fullRowCount,
            "a single-row change's journal write should hold far fewer entries than the full table");
    }

    /// <summary>The resurrection bug: a row removed from the live snapshot (weight dropping to &lt;= 0) MUST
    /// be recorded in the journal as an explicit tombstone, not left merely absent, or replaying a STALE
    /// (never-overwritten) earlier positive entry resurrects it. Append-only SOURCE events can't produce a
    /// genuine removal on their own (COUNT/SUM only grow), so this test manufactures one directly: an
    /// "upstream" table exists in the catalog purely so its OutputFields are compiled (letting the table
    /// under test resolve it as a TABLE input) but is never started — this test drives its
    /// (TableDeltaNamespace, upstreamName) delta stream by hand, including an explicit weight=-1 removal.
    ///
    /// Observable proof, without relying on row CONTENT surviving a restart (every restart wipes the
    /// snapshot to empty regardless — TableGrain's documented restart-resume limitation, unchanged by Plan
    /// 009 A2): after an insert-then-remove that nets to EXACTLY ZERO real rows, a CORRECT implementation's
    /// resume-detection sees an honestly-empty resumed state and never sets Rebuilding — identical to what
    /// Batched would report for the same net-zero activity. A BUGGY implementation that treats "absent" as
    /// "unchanged" instead of tombstoning the removal leaves the journal holding a STALE, never-overwritten
    /// POSITIVE entry from the earlier insert; replaying that stale entry resurrects a phantom row, which
    /// flips Rebuilding to true where it should stay false — the precise, publicly-observable difference
    /// this test asserts on.</summary>
    [Fact]
    public async Task Journaled_RemovalTombstone_NetsToHonestlyEmpty_NoPhantomRebuild()
    {
        var registry = _cluster.GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var upstreamName = "jrnl_up_" + Guid.NewGuid().ToString("n")[..8];
        var tableName = "jrnl_removal_" + Guid.NewGuid().ToString("n")[..8];
        var sourceName = "trades_up_" + Guid.NewGuid().ToString("n")[..8];

        await registry.UpsertSourceAsync(new SourceDefinition
        {
            Name = sourceName,
            Description = "test source",
            GeneratorProfile = "trades",
            EventsPerSecond = 0,
            Enabled = false,
            Fields = [new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long)],
        });

        // Started for real (SetTableStatusAsync(Running) requires every declared table input to be Running
        // — see RegistryGrain.SetTableStatusAsync's "table input(s) not running" gate — so the downstream
        // table below can't start otherwise) but its underlying source never receives any events (Enabled
        // = false, nothing ever published to it in this test), so it never emits any real deltas of its
        // own. This test drives its (TableDeltaNamespace, upstreamName) delta stream directly instead — the
        // only way to inject an explicit negative-weight removal (see method doc); nothing stops a stream
        // from carrying test-injected messages alongside whatever a real (here: silent) upstream would emit.
        var upstreamCreated = await registry.CreateTableAsync(new TableDefinition
        {
            Name = upstreamName,
            Sql = $"SELECT symbol, qty FROM {sourceName}",
        });
        await registry.SetTableStatusAsync(upstreamCreated.Id, PipelineStatus.Running);

        var def = new TableDefinition
        {
            Name = tableName,
            Sql = $"SELECT symbol, qty FROM {upstreamName}",
            Persistence = TablePersistenceMode.Journaled,
            FlushMs = 50,
            JournalMaxEntries = 100, // large — this test is about ONE removal, not compaction.
        };
        var created = await registry.CreateTableAsync(def);
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);
        var streamProvider = _cluster.Client.GetStreamProvider(StreamConstants.ProviderName);
        var upstreamStream = streamProvider.GetStream<List<TableDeltaDto>>(
            StreamId.Create(StreamConstants.TableDeltaNamespace, upstreamName));

        await upstreamStream.OnNextAsync([
            new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = 10L }, Weight = 1 },
        ]);
        Assert.Equal(1, await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15));

        // Give a journal flush tick time to actually persist the insert, so the removal below genuinely has
        // a prior positive entry on disk to overwrite (not just an in-memory pending one).
        await PollUntilAsync(() => Task.FromResult(ReadPersistedJournalEntryCount(tableName)), c => c >= 1, deadlineSeconds: 15);

        await upstreamStream.OnNextAsync([
            new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = 10L }, Weight = -1 },
        ]);
        Assert.Equal(0, await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 0, deadlineSeconds: 15));

        // The journal must still be tracking the key (Count == 1 — a tombstone, not silently dropped back to
        // Count == 0) — direct proof of the "recorded as a removal, not merely absent" requirement.
        var journalCountAfterRemoval = await PollUntilAsync(
            () => Task.FromResult(ReadPersistedJournalEntryCount(tableName)), c => c == 1, deadlineSeconds: 15);
        Assert.Equal(1, journalCountAfterRemoval);

        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Stopped);
        await registry.SetTableStatusAsync(created.Id, PipelineStatus.Running);

        // The precise, publicly-observable assertion (see method doc): a correct tombstone means the
        // resumed state is honestly net-empty, so NO rebuild is needed — Rebuilding must be false, matching
        // what Batched would report for the same net-zero-activity table. A resurrection bug would instead
        // leave this true (a phantom AAPL row briefly resurrected, then wiped by the shared reset-check).
        var afterRestart = await grain.GetMetricsAsync();
        Assert.False(afterRestart.Rebuilding);
        Assert.Equal(0, afterRestart.RowCount);

        // Confirm the table is genuinely alive and AAPL specifically never resurfaces.
        await upstreamStream.OnNextAsync([
            new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "MSFT", ["qty"] = 5L }, Weight = 1 },
        ]);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15);
        var rows = await grain.GetRowsAsync(10, 0);
        Assert.DoesNotContain(rows, r => (string?)r.Row.GetValueOrDefault("symbol") == "AAPL");
        Assert.Contains(rows, r => (string?)r.Row.GetValueOrDefault("symbol") == "MSFT");
    }

    [Fact]
    public async Task Journaled_ModeSwitch_BothDirections_ResumeConsistently()
    {
        var tableName = "jrnl_switch_" + Guid.NewGuid().ToString("n")[..8];
        var journaledDef = new TableDefinition
        {
            Name = tableName,
            Sql = "SELECT symbol, COUNT(*) AS trades FROM __SOURCE__ GROUP BY symbol",
            Persistence = TablePersistenceMode.Journaled,
            FlushMs = 50,
            JournalMaxEntries = 100, // never compacts here — the switch has to fold REAL journal content, not an already-compacted snapshot.
        };
        var (registry, sourceName, created) = await SeedTradesTableAsync(journaledDef);
        var grain = _cluster.GrainFactory.GetGrain<ITableGrain>(tableName);

        await PublishTradeAsync(sourceName, "AAPL", 100, 10);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15);
        await PollUntilAsync(() => Task.FromResult(ReadPersistedJournalEntryCount(tableName)), c => c >= 1, deadlineSeconds: 15);

        // Journaled -> Batched, on a RUNNING table — RegistryGrain.UpdateTableAsync's own restart-on-
        // persistence-change path (the same path a real console edit takes).
        created.Persistence = TablePersistenceMode.Batched;
        var afterSwitchToBatched = await registry.UpdateTableAsync(created);
        Assert.Equal(PipelineStatus.Running, afterSwitchToBatched!.Status);

        var metricsAfterSwitch1 = await grain.GetMetricsAsync();
        Assert.True(metricsAfterSwitch1.Rebuilding); // AAPL was real prior activity — must be detected as a resume, not silently dropped.
        Assert.Equal(0, metricsAfterSwitch1.RowCount);

        // The journal must be cleared now — proves the switch didn't leave stale Journaled-era entries
        // sitting around for a LATER switch back to replay incorrectly.
        Assert.Equal(0, ReadPersistedJournalEntryCount(tableName));

        await PublishTradeAsync(sourceName, "MSFT", 50, 3);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15);
        var rowsUnderBatched = await grain.GetRowsAsync(10, 0);
        Assert.Contains(rowsUnderBatched, r => (string?)r.Row.GetValueOrDefault("symbol") == "MSFT");
        Assert.DoesNotContain(rowsUnderBatched, r => (string?)r.Row.GetValueOrDefault("symbol") == "AAPL");

        // Batched -> Journaled again.
        created.Persistence = TablePersistenceMode.Journaled;
        var afterSwitchBackToJournaled = await registry.UpdateTableAsync(created);
        Assert.Equal(PipelineStatus.Running, afterSwitchBackToJournaled!.Status);

        var metricsAfterSwitch2 = await grain.GetMetricsAsync();
        Assert.True(metricsAfterSwitch2.Rebuilding); // MSFT was real prior activity under Batched.
        Assert.Equal(0, metricsAfterSwitch2.RowCount);

        await PublishTradeAsync(sourceName, "GOOG", 200, 1);
        await PollUntilAsync(() => grain.GetRowCountAsync(), c => c == 1, deadlineSeconds: 15);
        var rowsUnderJournaledAgain = await grain.GetRowsAsync(10, 0);
        Assert.Contains(rowsUnderJournaledAgain, r => (string?)r.Row.GetValueOrDefault("symbol") == "GOOG");
        // Neither AAPL nor MSFT (stale pre-switch data from earlier activations) resurfaces — the exact
        // resurrection a leftover journal would cause if the mode-switch clear hadn't happened.
        Assert.DoesNotContain(rowsUnderJournaledAgain, r => (string?)r.Row.GetValueOrDefault("symbol") == "AAPL");
        Assert.DoesNotContain(rowsUnderJournaledAgain, r => (string?)r.Row.GetValueOrDefault("symbol") == "MSFT");
    }
}
