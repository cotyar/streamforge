using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using StreamForge.Abstractions;
using StreamForge.AppCore.Json;
using StreamForge.Engine;
using StreamForge.Engine.Runtime;

namespace StreamForge.Host.Grains;

/// <summary>Plan 011 D1: one shard key's persisted state. Small on purpose — this is the object whose
/// size decides whether swapping a key out and back in is cheap, and for the use case that motivated the
/// wave (one financial instrument: a handful of legs, a thousand versions) it is kilobytes.</summary>
public sealed class TableShardGrainState
{
    public string TableName { get; set; } = "";

    /// <summary>The RAW shard key (not the grain-key token — see TableShardKeys.GrainKey for why the two
    /// differ). Persisted so a reactivated shard can name itself without the router telling it again.</summary>
    public string ShardKey { get; set; } = "";

    /// <summary>The last <see cref="TableShardConfig"/> the router stamped onto a batch. Persisted so a
    /// cold read answers from this grain's own state, with no call to the registry — see that type's
    /// "why carried, not looked up" note.</summary>
    public TableShardConfig? Config { get; set; }

    /// <summary>Canonical row key (TableShardKeys.CanonicalRowKey) -&gt; the row and its consolidated
    /// Z-set weight. Only positive weights, exactly like TableGrainState.Snapshot.</summary>
    public Dictionary<string, TableRowDto> Rows { get; set; } = [];

    /// <summary>Row-identity key (RowKeyCodec.EncodeIdentity over Config.IdentityColumns) -&gt; that
    /// logical row's retained version trail.</summary>
    public Dictionary<string, RowHistoryEntry> History { get; set; } = [];

    /// <summary>Highest router sequence applied — see TableShardingInfo.RouterSeq.</summary>
    public long AppliedSeq { get; set; } = -1;

    /// <summary>Per-shard monotonic version counter, feeding HistoryVersion.Seq. Per SHARD rather than per
    /// table (which is what TableHistoryGrain uses) because a shard cannot see the other shards' deltas —
    /// and does not need to: HistoryVersion.Seq only ever orders versions WITHIN one row's trail, and a
    /// trail never spans shards.</summary>
    public long Seq { get; set; }

    public long DeltasApplied { get; set; }
}

/// <summary>
/// Plan 011 wave D1 — THE SHARD. Key = <c>TableShardKeys.GrainKey(table, shardKey)</c>: one activation per
/// distinct shard key of one sharded table, holding that key's consolidated rows and that key's version
/// history and nothing else.
///
/// THE ONE RULE THIS GRAIN EXISTS TO KEEP: it never calls <c>DelayDeactivation</c>. Every other grain in
/// the table path calls <c>DelayDeactivation(TimeSpan.FromDays(365))</c> — TableGrain, TableHistoryGrain,
/// TableStageGrain, ArrangementGrain, TableOutputGrain — which is precisely why nothing in the table path
/// has ever been swapped out, and precisely the defect wave D exists to answer. An idle shard being
/// collected by Orleans' activation collector, with its state sitting on disk until something asks for
/// that key again, IS the memory win. A shard that pinned itself alive would deliver nothing while
/// appearing to work, which is the failure mode worth naming out loud because it is invisible in every
/// functional test.
///
/// The collection age is configured per grain class in Program.cs
/// (<c>GrainCollectionOptions.ClassSpecificCollectionAge</c>) rather than hard-coded here, so a soak or a
/// live check can shorten it without a rebuild. Note that a registered grain TIMER does not extend an
/// activation's lifetime in Orleans (that is why GeneratorGrain has an explicit PingAsync) — so the flush
/// timer below does not, and must not, keep an idle shard alive.
///
/// CONSISTENCY, for free. Per-key reads are strictly consistent by construction: Orleans single-threads
/// one grain's turns, the router forwards one ordered batch per key at a time, and GetViewAsync reads the
/// same live structures ApplyAsync writes. A read therefore observes a whole prefix of the table's delta
/// stream — never a half-applied batch — with no fence, no epoch negotiation and no configuration. That
/// is the query the use case cares about ("everything for this instrument"), and it is why the fenced
/// whole-table scan is a separate, later concern rather than a prerequisite.
///
/// WHAT IT DOES NOT DO. It does not run SQL, it does not hold operator state (join indexes, aggregate
/// accumulators), and it is not consulted by the table's own read path. It is a SECOND materialization of
/// the table's output, fed by the delta stream — the same relationship TableHistoryGrain already has to
/// the table (DESIGN.md D7, "the delta stream is the event log"). Which also bounds what it can promise:
/// a shard reflects the table's OUTPUT, so a table whose SQL cannot be resumed from its output alone (the
/// restart-resume limitation in TableGrain's class doc) has exactly the same limitation here.
///
/// PERSISTENCE. Write-behind under the owning table's own <c>TablePersistenceMode</c>/<c>FlushMs</c>,
/// mirroring TableGrain/TableHistoryGrain — plus, and this is load-bearing rather than incidental, an
/// unconditional final flush in OnDeactivateAsync. For every other table-path grain that final flush is a
/// crash-safety nicety; here it is the swap-out itself, so a shard that failed to write on deactivation
/// would lose data rather than merely lose durability. <c>MemoryOnly</c> is honored literally and is the
/// one configuration where that is the intended behavior: no write means nothing to reactivate from, and
/// the mode's own contract already says a restart brings the table back empty.
///
/// The capture is a full re-clone of this shard's rows and history on each flush, unlike wave C's
/// incremental captures in TableGrain/TableHistoryGrain. That is deliberate: the whole premise is that
/// one shard is SMALL (one instrument), so O(shard) per flush is O(a few rows), and an incremental
/// tracker would add a third structure to keep consistent across activation boundaries for no measurable
/// gain. If a single shard ever grows large enough for this to matter, the shard key was chosen too
/// coarsely and that is the thing to fix.
/// </summary>
public sealed class TableShardGrain(
    [PersistentState("tableShard", StreamConstants.StorageName)] IPersistentState<TableShardGrainState> state,
    ILogger<TableShardGrain> logger)
    : Grain, ITableShardGrain
{
    /// <summary>The live, zero-lag working set — the same live-vs-mirror separation TableGrain and
    /// TableHistoryGrain use, and for the identical reason (see TableHistoryGrain's class doc): a
    /// backgrounded FireAndForget write must never be able to observe a structural mutation of the object
    /// graph it is serializing.</summary>
    private readonly ConsolidationLedger _ledger = new();

    private readonly Dictionary<string, RowHistoryEntry> _liveHistory = new(StringComparer.Ordinal);

    private long _liveSeq;
    private long _appliedSeq = -1;
    private long _deltasApplied;
    private bool _dirty;
    private bool _loaded;

    private IGrainTimer? _flushTimer;
    private TablePersistenceMode _persistenceMode = TablePersistenceMode.Batched;
    private TimeSpan _flushInterval = TimeSpan.FromSeconds(2);

    /// <summary>FireAndForget's single-flight guard — see TableGrain's identically-named field. Never
    /// faulted (WriteStateBestEffortAsync catches internally).</summary>
    private Task _pendingWrite = Task.CompletedTask;

    private string _tableName = "";

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        // NOTE: no DelayDeactivation. See the class doc — this is the entire point of the grain.
        _tableName = TableShardKeys.ParseGrainKey(this.GetPrimaryKeyString()).TableName;

        // Orleans has already run ReadStateAsync by the time activation reaches here, so this is the
        // reactivation-from-cold-storage path: rebuild the live working set from the persisted mirror,
        // deep-copying (never reference-sharing) so the two graphs stay independent from the first turn.
        if (state.RecordExists && state.State.Rows is not null)
        {
            _tableName = string.IsNullOrEmpty(state.State.TableName) ? _tableName : state.State.TableName;

            // NORMALIZE, do not just copy. This state came back through System.Text.Json, which
            // materializes every value of an untyped Dictionary<string, object?> as a boxed JsonElement —
            // so without this a reactivated shard would hold JsonElement where a live shard holds long /
            // double / string / bool, and the two would disagree about everything downstream: Orleans has
            // no serializer for JsonElement (a read of a cold shard fails outright, HTTP 500 — found
            // exactly that way in the wave's live check, not in any unit test, because an in-memory test
            // cluster never round-trips through JSON), and even where it did not fail, the canonical row
            // key would differ from the one the router computes and consolidation would silently split.
            // This is the same normalization the ingress path applies for the same reason.
            foreach (var (key, dto) in state.State.Rows)
            {
                var row = new Dictionary<string, object?>(dto.Row);
                JsonValueNormalizer.NormalizeInPlace(row);
                _ledger.Seed(key, new EventRecord(row), dto.Weight);
            }
            foreach (var (key, entry) in state.State.History)
            {
                _liveHistory[key] = new RowHistoryEntry
                {
                    Versions = [.. entry.Versions.Select(v =>
                    {
                        var row = new Dictionary<string, object?>(v.Row);
                        JsonValueNormalizer.NormalizeInPlace(row);
                        return new HistoryVersion(row, v.TsMs, v.Seq);
                    })],
                    RetractionCount = entry.RetractionCount,
                };
            }
            _liveSeq = state.State.Seq;
            _appliedSeq = state.State.AppliedSeq;
            _deltasApplied = state.State.DeltasApplied;
            _loaded = _ledger.Visible.Count > 0 || _liveHistory.Count > 0;
            ApplyPersistenceConfig(state.State.Config);
        }

        ShardResidency.OnActivated(_tableName, this.GetPrimaryKeyString());
        return Task.CompletedTask;
    }

    public Task<bool> ApplyAsync(TableShardConfig config, long seq, List<TableDeltaDto> deltas)
    {
        state.State.TableName = config.TableName;
        _tableName = config.TableName;
        state.State.Config = config;
        ApplyPersistenceConfig(config);

        // Monotonic by contract (the router only ever hands out increasing numbers), but taken as a max
        // anyway so a router restart that re-issued a reserved block can never walk this backwards.
        if (seq > _appliedSeq) _appliedSeq = seq;

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var delta in deltas)
        {
            _deltasApplied++;
            if (string.IsNullOrEmpty(state.State.ShardKey))
            {
                state.State.ShardKey = TableShardKeys.EncodeShardKey(delta.Row, config.ShardBy);
            }

            var rowKey = TableShardKeys.CanonicalRowKey(delta.Row);
            _ledger.Apply(rowKey, new EventRecord(delta.Row), delta.Weight);

            if (!config.HistoryEnabled) continue;

            var identityKey = RowKeyCodec.EncodeIdentity(delta.Row, config.IdentityColumns);

            // Plan 011 C2 composition, decided here and documented once. A retention EVICTION reclaims the
            // key's version trail instead of counting one more retraction against it — byte-for-byte the
            // rule TableHistoryGrain already applies (see its OnDeltaBatchAsync comment), and applied here
            // for the same reason: history is DERIVED from the table, so a row the table has stopped
            // carrying takes its history with it. Retention and sharding are complementary rather than
            // contradictory — retention bounds what the TABLE holds, sharding bounds what is RESIDENT —
            // and a user who wants to keep everything simply leaves retention off, which is its default.
            if (delta.Evicted)
            {
                _liveHistory.Remove(identityKey);
                continue;
            }

            _liveSeq++;
            if (!_liveHistory.TryGetValue(identityKey, out var entry))
            {
                entry = new RowHistoryEntry();
                _liveHistory[identityKey] = entry;
            }

            if (delta.Weight > 0)
            {
                var version = new HistoryVersion(new Dictionary<string, object?>(delta.Row), nowMs, _liveSeq);
                TableRowHistoryRetention.Append(entry, version, config.HistoryMode, config.HistoryLimit, config.HistoryByField, config.HistoryWindowMs);
            }
            else
            {
                entry.RetractionCount++;
            }
        }

        _loaded = true;
        _dirty = true;
        EnsureFlushTimer();

        // Empty shard: every row retracted AND no version trail left to speak for the key. Clear the
        // persisted state now rather than leaving a zero-row file on disk forever, and tell the router so
        // the directory drops the key — otherwise the directory (the one resident structure here) would
        // only ever grow, which is exactly the ceiling this design is trying not to reproduce.
        if (_ledger.Visible.Count == 0 && _liveHistory.Count == 0)
        {
            return ClearAndReportEmptyAsync();
        }

        return Task.FromResult(true);
    }

    private async Task<bool> ClearAndReportEmptyAsync()
    {
        _dirty = false;
        _flushTimer?.Dispose();
        _flushTimer = null;
        try { await _pendingWrite; } catch { /* never faults */ }
        try { await state.ClearStateAsync(); } catch (Exception ex) { logger.LogWarning(ex, "Shard '{Key}': clearing emptied state failed", this.GetPrimaryKeyString()); }
        state.State = new TableShardGrainState { TableName = _tableName };
        _loaded = false;
        _appliedSeq = -1;
        _deltasApplied = 0;
        _liveSeq = 0;
        DeactivateOnIdle();
        return false;
    }

    public Task<TableShardView> GetViewAsync(int historyLimitPerKey)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var windowMs = state.State.Config?.HistoryWindowMs ?? 0;

        var history = new List<TableShardHistoryEntry>(_liveHistory.Count);
        foreach (var (key, entry) in _liveHistory)
        {
            // Prune-on-read, mirroring TableHistoryGrain.GetHistoryAsync: a time-windowed trail must not
            // report versions the window has already dropped just because no delta has arrived since.
            // Deliberately does NOT set _dirty — a read has never forced a write here either.
            TableRowHistoryRetention.PruneWindow(entry, nowMs, windowMs);

            var ordered = entry.Versions.OrderByDescending(v => v.Seq).ToList();
            history.Add(new TableShardHistoryEntry
            {
                RowKey = key,
                Versions = historyLimitPerKey > 0 ? ordered.Take(historyLimitPerKey).ToList() : ordered,
                RetractionCount = entry.RetractionCount,
                TotalVersions = entry.Versions.Count,
            });
        }

        return Task.FromResult(new TableShardView
        {
            Found = _loaded,
            ShardKey = state.State.ShardKey,
            Rows = [.. _ledger.Visible.Values.Select(v => new TableRowDto { Row = new Dictionary<string, object?>(v.Row), Weight = v.Weight })],
            History = history,
            AppliedSeq = _appliedSeq,
            DeltasApplied = _deltasApplied,
            HistoryEnabled = state.State.Config?.HistoryEnabled ?? false,
        });
    }

    public Task<TableShardStats> GetStatsAsync() => Task.FromResult(new TableShardStats
    {
        ShardKey = state.State.ShardKey,
        RowCount = _ledger.Visible.Count,
        HistoryKeyCount = _liveHistory.Count,
        TotalVersions = _liveHistory.Values.Sum(e => (long)e.Versions.Count),
        AppliedSeq = _appliedSeq,
        DeltasApplied = _deltasApplied,
    });

    public async Task PurgeAsync()
    {
        _dirty = false;
        _flushTimer?.Dispose();
        _flushTimer = null;
        try { await _pendingWrite; } catch { /* never faults */ }
        _ledger.Clear();
        _liveHistory.Clear();
        _loaded = false;
        try { await state.ClearStateAsync(); } catch (Exception ex) { logger.LogWarning(ex, "Shard '{Key}': purge failed", this.GetPrimaryKeyString()); }
        state.State = new TableShardGrainState { TableName = _tableName };
        DeactivateOnIdle();
    }

    private void ApplyPersistenceConfig(TableShardConfig? config)
    {
        if (config is null) return;
        _persistenceMode = config.Persistence;
        _flushInterval = TimeSpan.FromMilliseconds(config.FlushMs > 0 ? config.FlushMs : 2000);
    }

    /// <summary>Registers the write-behind flush timer on first need. Lazy rather than registered at
    /// activation because a shard that is only ever READ (the common case for a cold key someone looked
    /// up) has nothing to flush, and arming a timer for it would be pure overhead per reactivation.</summary>
    private void EnsureFlushTimer()
    {
        if (_flushTimer is not null || _persistenceMode == TablePersistenceMode.MemoryOnly) return;
        _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, _flushInterval, _flushInterval);
    }

    /// <summary>Copies the live working set into the persisted mirror. Full copy per flush, by design —
    /// see the class doc's persistence paragraph for why "incremental" would be the wrong trade here.
    /// Fully synchronous on the grain turn, so nothing can mutate either graph mid-copy.</summary>
    private void CaptureSnapshotIntoState()
    {
        state.State.Rows = _ledger.Visible.ToDictionary(
            kv => kv.Key,
            kv => new TableRowDto { Row = new Dictionary<string, object?>(kv.Value.Row), Weight = kv.Value.Weight },
            StringComparer.Ordinal);
        state.State.History = _liveHistory.ToDictionary(
            kv => kv.Key,
            kv => new RowHistoryEntry { Versions = [.. kv.Value.Versions], RetractionCount = kv.Value.RetractionCount },
            StringComparer.Ordinal);
        state.State.Seq = _liveSeq;
        state.State.AppliedSeq = _appliedSeq;
        state.State.DeltasApplied = _deltasApplied;
        state.State.TableName = _tableName;
        _dirty = false;
    }

    private async Task FlushAsync()
    {
        CaptureSnapshotIntoState();
        await state.WriteStateAsync();
    }

    private async Task WriteStateBestEffortAsync()
    {
        try
        {
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Background (FireAndForget) flush failed for shard '{Key}'", this.GetPrimaryKeyString());
        }
    }

    private async Task OnFlushTickAsync()
    {
        if (!_dirty) return;

        if (_persistenceMode == TablePersistenceMode.FireAndForget)
        {
            if (!_pendingWrite.IsCompleted) return; // single-flight, same guard TableGrain uses
            CaptureSnapshotIntoState();
            _pendingWrite = WriteStateBestEffortAsync();
            return;
        }

        await FlushAsync();
    }

    /// <summary>THE SWAP-OUT. For every other table-path grain the final flush is crash-safety; here it is
    /// the mechanism itself — this is the moment an idle key's state leaves memory for disk, and skipping
    /// it would turn deactivation from "swapped out" into "lost". Runs for every mode except MemoryOnly,
    /// whose contract is precisely that nothing is ever written.</summary>
    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        try { await _pendingWrite; } catch { /* defensive — WriteStateBestEffortAsync never faults */ }
        if (_persistenceMode != TablePersistenceMode.MemoryOnly && _dirty)
        {
            try { await FlushAsync(); } catch (Exception ex) { logger.LogError(ex, "Shard '{Key}': final flush failed — this key's most recent deltas are lost", this.GetPrimaryKeyString()); }
        }
        ShardResidency.OnDeactivated(_tableName, this.GetPrimaryKeyString());
        await base.OnDeactivateAsync(reason, cancellationToken);
    }
}
