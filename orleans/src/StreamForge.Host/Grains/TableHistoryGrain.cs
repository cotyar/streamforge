using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;

namespace StreamForge.Host.Grains;

public sealed class TableHistoryGrainState
{
    public bool HistoryEnabled { get; set; }
    public TableHistoryMode HistoryMode { get; set; } = TableHistoryMode.All;
    public int HistoryLimit { get; set; } = 10;
    public string? HistoryByField { get; set; }
    public long HistoryWindowMs { get; set; }

    /// <summary>Cached result of TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql), recomputed on
    /// every ResetAsync. Null = no derivable GROUP BY identity; row keys fall back to whole-row encoding.</summary>
    public List<string>? IdentityColumns { get; set; }

    /// <summary>Row-identity key (RowKeyCodec.EncodeIdentity) -> retained version history.</summary>
    public Dictionary<string, RowHistoryEntry> Entries { get; set; } = [];

    /// <summary>Monotonic counter, incremented once per observed delta (assertion or retraction) — see
    /// HistoryVersion.Seq's doc comment.</summary>
    public long Seq { get; set; }
}

/// <summary>
/// Key = table name. Feature B (opt-in per-table ROW HISTORY): subscribes to a table's delta stream
/// (StreamConstants.TableDeltaNamespace, table name — the same stream TableGrain publishes consolidated
/// deltas to for downstream tables/StreamBridgeService) and, for every delta, appends an ASSERTION version
/// (weight &gt; 0) or bumps a retraction counter (weight &lt;= 0) against a per-row-identity entry, applying
/// the table's configured HistoryMode retention (see TableRowHistoryRetention).
///
/// KEY DERIVATION: see TableGroupKeyExtractor's class comment for the full rationale. Short version: the
/// frozen Engine doesn't expose which output columns are the GROUP BY identity, and TableExecutor's own
/// CanonicalRowKey changes on every update (it hashes the whole row, aggregates included) so it's unusable
/// as a stable per-group identity. This grain instead derives the identity columns by textually matching
/// the table's SQL's GROUP BY clause against its SELECT list (TableGroupKeyExtractor), falling back to a
/// whole-row canonical key when there's no GROUP BY or the match is ambiguous (RowKeyCodec).
///
/// DESIGN NOTE (state grain vs JournaledGrain): this is a plain [PersistentState] grain with write-behind
/// persistence, fed by the delta stream — not an Orleans JournaledGrain event-sourced grain. That's a
/// deliberate choice, not an oversight: JournaledGrain requires a log-consistency provider, and this
/// project's storage is a plain JSON [PersistentState] provider with no log-consistency provider
/// registered (see Program.cs's storage wiring, which TableGrain and RegistryGrain both already rely on
/// for the same reason). More fundamentally, the delta stream this grain subscribes to already *is* the
/// event log — every TableDelta a table ever emits is durably ordered and replayable via Orleans streams
/// (the same stream downstream tables and StreamBridgeService consume) — layering JournaledGrain's own
/// event-sourcing machinery on top would duplicate that log rather than reuse it. Verdict: agree with the
/// brief's framing; a state grain fed by the existing delta stream is the right shape here, matching
/// TableGrain's own precedent (which persists a consolidated *snapshot* the same way, for the same reason).
///
/// LIFECYCLE: mirrors TableGrain's write-behind cadence (dirty flag, 2s flush timer, flush on
/// deactivate/disable) and its DelayDeactivation pattern (kept alive for as long as history is enabled, so
/// the stream subscription's callback stays registered — Orleans persistent-stream subscriptions survive
/// deactivation at the pub-sub-store level, but the local callback registration does not, so this grain
/// must stay activated to keep observing new deltas, exactly like TableGrain does for "Running").
///
/// PLAN 008 W2.5 — PER-TABLE PERSISTENCE MODE: this grain's write-behind flush has "the identical stall
/// shape" TableGrain's own class doc describes (whole-state serialize + file write, awaited inside the grain
/// turn), driven by the SAME TableDefinition.Persistence/FlushMs the owning table was configured with (see
/// ResetAsync/ResumeAsync, both already take the full TableDefinition). The mechanism mirrors TableGrain's
/// own persistence-mode paragraph almost exactly — Batched (default) awaits the write on the configured
/// interval (0 = pre-008 hardcoded 2000ms), FireAndForget captures then backgrounds the write with the same
/// single-flight guard (<see cref="_pendingWrite"/>), MemoryOnly registers no timer and skips every flush,
/// including the final one on stop/deactivate.
///
/// ONE STRUCTURAL DIFFERENCE FROM TableGrain, and the reason for <see cref="_liveEntries"/>/
/// <see cref="_liveSeq"/> existing at all: TableGrain's live source (<c>_executor</c>/
/// <c>_coordinatorSnapshot</c>) was ALREADY a separate object from <c>state.State.Snapshot</c> pre-008 (a
/// fresh dictionary is built into state.State.Snapshot only at flush time), so a backgrounded write can
/// never race a live mutation — nothing else ever touches the dictionary handed to WriteStateAsync once it's
/// captured. Pre-008 TableHistoryGrain had NO such separation: OnDeltaBatchAsync mutated
/// <c>state.State.Entries</c> (and, in place, each <c>RowHistoryEntry.Versions</c> list /
/// <c>RetractionCount</c>) directly and immediately — reads were zero-lag, only the DISK write lagged behind
/// (_dirty + timer). Backgrounding that write without change would let a later turn's in-place Add/Remove on
/// a <c>List&lt;HistoryVersion&gt;</c> or the <c>Entries</c> dictionary race
/// <c>JsonSerializer.SerializeAsync</c> walking the very same object graph mid-flight — Dictionary/List don't
/// tolerate structural mutation during enumeration (a real, throwable
/// <c>InvalidOperationException</c>, not just a theoretical race, since Orleans schedules the write's
/// continuation back onto this grain's own single-threaded turn queue, interleaved with whatever turn runs
/// next while the write's internal awaits are pending).
///
/// The fix: <see cref="_liveEntries"/> (mirroring TableGrain's <c>_executor</c>) is now the ONLY thing
/// OnDeltaBatchAsync/GetHistoryAsync/GetStatsAsync ever mutate or read for live data — zero-lag reads are
/// fully preserved, unchanged from pre-008. <c>state.State.Entries</c> becomes purely a periodically
/// refreshed, write-only mirror: <see cref="CaptureSnapshotIntoState"/> deep-clones (fresh dictionary, fresh
/// <c>RowHistoryEntry</c> objects with fresh <c>Versions</c> list copies — <c>HistoryVersion</c> instances
/// themselves are effectively immutable once constructed, so sharing THEM by reference across the clone
/// boundary is safe) from <c>_liveEntries</c> into <c>state.State.Entries</c> at every capture, and nothing
/// ever mutates <c>state.State.Entries</c> in place again afterward — exactly the invariant a background
/// write needs to be safe.
///
/// PLAN 011 WAVE C: that clone is now INCREMENTAL — only the entries actually touched since the previous
/// capture are re-cloned (see <see cref="CaptureSnapshotIntoState"/> and <see cref="_touchedKeys"/>). The
/// invariant above is unchanged and still exactly what makes a background write safe; what changed is that
/// a 2s tick no longer allocates a fresh RowHistoryEntry + Versions list for every retained key in the
/// table. What it does NOT fix, stated plainly: <see cref="_liveEntries"/>' KEY COUNT is still unbounded —
/// per-key VERSION counts are capped (TableRowHistoryRetention, AllModeCap/HistoryLimit) but nothing ever
/// removes a key, so a table over an unbounded key space still accumulates one entry per key it has ever
/// seen, here and in TableGrain alike. See orleans/DESIGN.md's "Known ceilings".
///
/// PLAN 011 WAVE C2 gives that key count its bound, but only for a table that OPTS IN: a row evicted by
/// the owning table's retention policy arrives here as a delta with <c>Evicted</c> set, and this grain
/// then REMOVES the key's entry outright rather than bumping its retraction counter — see
/// OnDeltaBatchAsync's own comment for why "history follows the table" is the right reading of an
/// eviction. Without a retention policy nothing changed: the key count is still unbounded.
/// </summary>
public sealed class TableHistoryGrain(
    [PersistentState("tableHistory", StreamConstants.StorageName)] IPersistentState<TableHistoryGrainState> state,
    ILogger<TableHistoryGrain> logger)
    : Grain, ITableHistoryGrain
{
    private StreamSubscriptionHandle<List<TableDeltaDto>>? _sub;
    private IGrainTimer? _flushTimer;
    private bool _dirty;

    /// <summary>The live, zero-lag working set — see class doc's persistence-mode paragraph for why this
    /// exists as a field separate from state.State.Entries. Row-identity key -> retained version history,
    /// mutated directly and immediately by OnDeltaBatchAsync; read directly by GetHistoryAsync/GetStatsAsync.</summary>
    private readonly Dictionary<string, RowHistoryEntry> _liveEntries = [];
    /// <summary>The live counterpart of state.State.Seq (see TableHistoryGrainState.Seq's own doc comment) —
    /// incremented once per observed delta, mirrored into state.State.Seq only at capture time.</summary>
    private long _liveSeq;

    /// <summary>Plan 011 wave C — the row-identity keys whose <see cref="RowHistoryEntry"/> changed since the
    /// last <see cref="CaptureSnapshotIntoState"/>, i.e. exactly the entries of <c>state.State.Entries</c>
    /// that are currently out of date. Drained by the capture, which re-clones only those — see the
    /// capture's own doc comment for the invariant and for why the old whole-map deep clone was the single
    /// biggest allocation source on this grain's turn.
    ///
    /// NOT accumulated for <see cref="TablePersistenceMode.MemoryOnly"/> (which never captures), for the
    /// same reason TableGrain's <c>_touchedRowKeys</c> is not: an undrained set is one more unbounded
    /// per-key structure.</summary>
    private readonly HashSet<string> _touchedKeys = new(StringComparer.Ordinal);

    /// <summary>Plan 011 wave C — forces the next capture to re-clone the whole map instead of applying
    /// <see cref="_touchedKeys"/>. Set wherever <c>_liveEntries</c> and <c>state.State.Entries</c> are
    /// (re)established as two independent object graphs — ResetAsync, ResumeAsync, DisableAsync — so the
    /// incremental path never has to trust state carried across those transitions.</summary>
    private bool _fullCaptureNeeded = true;

    // Plan 008 W2.5 — per-table persistence mode, re-read from the owning table's TableDefinition on every
    // ResetAsync/ResumeAsync (same refresh rule TableGrain uses for its own copies of these fields).
    private TablePersistenceMode _persistenceMode = TablePersistenceMode.Batched;
    private TimeSpan _flushInterval = TimeSpan.FromSeconds(2);
    /// <summary>FireAndForget's single-flight guard — see TableGrain's identically-named field for the full
    /// reasoning. Never faulted (WriteStateBestEffortAsync catches and logs internally).</summary>
    private Task _pendingWrite = Task.CompletedTask;

    public async Task ResetAsync(TableDefinition def)
    {
        await UnsubscribeAsync();

        state.State = new TableHistoryGrainState
        {
            HistoryEnabled = def.HistoryEnabled,
            HistoryMode = def.HistoryMode,
            HistoryLimit = def.HistoryLimit,
            HistoryByField = def.HistoryByField,
            HistoryWindowMs = def.HistoryWindowMs,
            IdentityColumns = TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql),
            Entries = [],
            Seq = 0,
        };
        _liveEntries.Clear();
        _liveSeq = 0;
        _dirty = false;
        _touchedKeys.Clear();
        _fullCaptureNeeded = true;
        _persistenceMode = def.Persistence;
        _flushInterval = TimeSpan.FromMilliseconds(def.FlushMs > 0 ? def.FlushMs : 2000);
        await state.WriteStateAsync();

        _flushTimer?.Dispose();
        _flushTimer = null;
        if (def.HistoryEnabled)
        {
            await SubscribeAsync(def.Name);
            // MemoryOnly registers no flush timer at all — see class doc's persistence-mode paragraph.
            if (_persistenceMode != TablePersistenceMode.MemoryOnly)
            {
                _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, _flushInterval, _flushInterval);
            }
            this.DelayDeactivation(TimeSpan.FromDays(365));
        }
        else
        {
            this.DelayDeactivation(TimeSpan.Zero);
        }
    }

    public async Task ResumeAsync(TableDefinition def)
    {
        if (!def.HistoryEnabled)
        {
            return;
        }

        // Sync config from the source of truth (the RegistryGrain-persisted TableDefinition) every time,
        // rather than trusting this grain's own possibly-never-initialized state — covers both the normal
        // "silo restarted, resume where we left off" path and the edge case of a table whose history was
        // enabled directly in persisted RegistryState without ever going through ResetAsync. Entries/Seq
        // are deliberately left untouched (unlike ResetAsync) so accumulated history survives.
        state.State.HistoryEnabled = true;
        state.State.HistoryMode = def.HistoryMode;
        state.State.HistoryLimit = def.HistoryLimit;
        state.State.HistoryByField = def.HistoryByField;
        state.State.HistoryWindowMs = def.HistoryWindowMs;
        state.State.IdentityColumns = TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql);
        _persistenceMode = def.Persistence;
        _flushInterval = TimeSpan.FromMilliseconds(def.FlushMs > 0 ? def.FlushMs : 2000);

        // Plan 008 W2.5: repopulate the live working set from the just-loaded persisted snapshot (Orleans'
        // ReadStateAsync already ran before activation reached this call) — deep-cloned, not reference-copied
        // (see class doc's persistence-mode paragraph): _liveEntries must never share RowHistoryEntry/
        // Versions-list object identity with state.State.Entries, or the very first background write after
        // a restart would be exposed to the same in-place-mutation race the separation exists to prevent.
        _liveEntries.Clear();
        foreach (var (key, entry) in state.State.Entries)
        {
            _liveEntries[key] = new RowHistoryEntry { Versions = new List<HistoryVersion>(entry.Versions), RetractionCount = entry.RetractionCount };
        }
        _liveSeq = state.State.Seq;
        // Plan 011 wave C: the two graphs were just re-established as exact (deep-cloned) mirrors of each
        // other, so the incremental capture's invariant starts clean from here.
        _touchedKeys.Clear();
        _fullCaptureNeeded = true;

        await state.WriteStateAsync();

        await UnsubscribeAsync();
        await SubscribeAsync(def.Name);
        _flushTimer?.Dispose();
        _flushTimer = null;
        // MemoryOnly registers no flush timer at all — see class doc's persistence-mode paragraph.
        if (_persistenceMode != TablePersistenceMode.MemoryOnly)
        {
            _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, _flushInterval, _flushInterval);
        }
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    public async Task DisableAsync()
    {
        await UnsubscribeAsync();
        _flushTimer?.Dispose();
        _flushTimer = null;

        state.State = new TableHistoryGrainState();
        _liveEntries.Clear();
        _liveSeq = 0;
        _dirty = false;
        _touchedKeys.Clear();
        _fullCaptureNeeded = true;
        await state.WriteStateAsync();
        this.DelayDeactivation(TimeSpan.Zero);
    }

    public Task<TableHistoryQueryResult> GetHistoryAsync(string key, int limit)
    {
        if (!_liveEntries.TryGetValue(key, out var entry))
        {
            return Task.FromResult(new TableHistoryQueryResult
            {
                Mode = state.State.HistoryMode,
                KeyFound = false,
            });
        }

        // Plan 011 wave C: prune-on-read mutates the LIVE entry in place, so its captured clone is stale
        // from here on. This deliberately does NOT set _dirty — a read has never forced a write and still
        // doesn't; it only makes sure that whenever the next capture does happen, this entry is included
        // rather than silently skipped by the incremental path.
        var beforeVersions = entry.Versions.Count;
        TableRowHistoryRetention.PruneWindow(entry, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), state.State.HistoryWindowMs);
        if (entry.Versions.Count != beforeVersions && _persistenceMode != TablePersistenceMode.MemoryOnly)
        {
            _touchedKeys.Add(key);
        }

        // Newest-first for the UI timeline; limit <= 0 means "all retained versions".
        var ordered = entry.Versions.OrderByDescending(v => v.Seq).ToList();
        var limited = limit > 0 ? ordered.Take(limit).ToList() : ordered;

        return Task.FromResult(new TableHistoryQueryResult
        {
            Versions = limited,
            RetractionCount = entry.RetractionCount,
            Mode = state.State.HistoryMode,
            TotalVersions = entry.Versions.Count,
            KeyFound = true,
        });
    }

    public Task<TableHistoryStats> GetStatsAsync() => Task.FromResult(new TableHistoryStats
    {
        Enabled = state.State.HistoryEnabled,
        Mode = state.State.HistoryMode,
        KeyCount = _liveEntries.Count,
        TotalVersions = _liveEntries.Values.Sum(e => (long)e.Versions.Count),
    });

    private async Task SubscribeAsync(string tableName)
    {
        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, tableName));
        _sub = await stream.SubscribeAsync((batch, _) => OnDeltaBatchAsync(batch));
    }

    private async Task UnsubscribeAsync()
    {
        if (_sub is null) return;
        try { await _sub.UnsubscribeAsync(); } catch { /* best-effort */ }
        _sub = null;
    }

    private Task OnDeltaBatchAsync(List<TableDeltaDto> batch)
    {
        if (!state.State.HistoryEnabled || batch.Count == 0) return Task.CompletedTask;

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var delta in batch)
        {
            _liveSeq++;

            var key = RowKeyCodec.EncodeIdentity(delta.Row, state.State.IdentityColumns);

            // Plan 011 wave C2 — A RETENTION EVICTION RECLAIMS THE KEY'S HISTORY, it does not record one
            // more retraction against it. Rationale, stated once here because it is a real semantic
            // decision and not an optimization: an ordinary retraction means "this row is not true right
            // now", and the key may well assert again, so keeping its version trail is exactly right. An
            // eviction means the table has stopped carrying the key at all because the table is a BOUNDED
            // view (see TableDefinition.RetentionMaxRows) — and if history kept the trail anyway, the row
            // count would plateau while _liveEntries kept one entry per key ever seen, i.e. the bound
            // would bound the visible table and none of the memory. History is derived from the table: a
            // row that leaves the table takes its history with it. A later re-assertion of the same key
            // simply starts a fresh trail, which is the honest representation of what the table knows.
            if (delta.Evicted)
            {
                if (_liveEntries.Remove(key) && _persistenceMode != TablePersistenceMode.MemoryOnly)
                {
                    // The capture's removal branch (see CaptureSnapshotIntoState) drops the mirrored entry.
                    _touchedKeys.Add(key);
                    _dirty = true;
                }
                continue;
            }

            if (!_liveEntries.TryGetValue(key, out var entry))
            {
                entry = new RowHistoryEntry();
                _liveEntries[key] = entry;
            }

            // Plan 011 wave C: this entry's clone in state.State.Entries is now stale — mark it so the next
            // capture re-clones exactly it, and not the other N-1 entries that did not change.
            if (_persistenceMode != TablePersistenceMode.MemoryOnly) _touchedKeys.Add(key);

            if (delta.Weight > 0)
            {
                var version = new HistoryVersion(new Dictionary<string, object?>(delta.Row), nowMs, _liveSeq);
                TableRowHistoryRetention.Append(entry, version, state.State.HistoryMode, state.State.HistoryLimit, state.State.HistoryByField, state.State.HistoryWindowMs);
            }
            else
            {
                entry.RetractionCount++;
            }
        }

        _dirty = true;
        return Task.CompletedTask;
    }

    /// <summary>Clones _liveEntries into state.State.Entries — see class doc's persistence-mode
    /// paragraph for why each captured entry MUST be a fresh <see cref="RowHistoryEntry"/> with a fresh
    /// Versions list, never a reference copy: once captured, state.State.Entries must never be mutated in
    /// place again, so a background write serializing it can never race a later OnDeltaBatchAsync mutation
    /// (which only ever touches _liveEntries, a completely separate object graph from this point on).
    ///
    /// PLAN 011 WAVE C — CLONES ONLY WHAT CHANGED. This used to deep-clone the ENTIRE map on every flush
    /// tick: at K retained row keys that is K fresh RowHistoryEntry objects and K fresh
    /// List&lt;HistoryVersion&gt; allocations every FlushMs (default 2000ms), thrown away 2s later, forever —
    /// allocation proportional to the whole history at a rate set by the clock, and the second half (with
    /// TableGrain's own capture) of what turned a slowly-growing table into a GC stall. It now re-clones
    /// only <see cref="_touchedKeys"/>: the entries whose Versions list or RetractionCount actually moved.
    ///
    /// THE INVARIANT: state.State.Entries, with _touchedKeys re-cloned from _liveEntries, always equals
    /// _liveEntries. It holds because the only mutators of a live entry are OnDeltaBatchAsync and
    /// GetHistoryAsync's prune-on-read, both of which record the key; the only places the two maps are
    /// re-established wholesale (ResetAsync/ResumeAsync/DisableAsync) set <see cref="_fullCaptureNeeded"/>;
    /// and a key present in _touchedKeys but absent from _liveEntries cannot happen, because nothing ever
    /// REMOVES a live entry (which is precisely why the key count is unbounded — see orleans/DESIGN.md's
    /// "Known ceilings"). The removal branch below is kept anyway so the invariant survives any future
    /// eviction (wave C2's retention) without silently leaving a resurrected entry in the mirror — which is
/// exactly what OnDeltaBatchAsync's <c>delta.Evicted</c> branch now does.
    ///
    /// The capture stays fully synchronous on the grain turn, exactly as before — the whole point is that
    /// nothing else may touch these two graphs mid-clone.</summary>
    private void CaptureSnapshotIntoState()
    {
        if (_fullCaptureNeeded)
        {
            state.State.Entries = _liveEntries.ToDictionary(
                kv => kv.Key,
                kv => new RowHistoryEntry { Versions = new List<HistoryVersion>(kv.Value.Versions), RetractionCount = kv.Value.RetractionCount });
            _fullCaptureNeeded = false;
        }
        else
        {
            foreach (var key in _touchedKeys)
            {
                if (_liveEntries.TryGetValue(key, out var live))
                {
                    state.State.Entries[key] = new RowHistoryEntry { Versions = new List<HistoryVersion>(live.Versions), RetractionCount = live.RetractionCount };
                }
                else
                {
                    state.State.Entries.Remove(key);
                }
            }
        }

        _touchedKeys.Clear();
        state.State.Seq = _liveSeq;
        _dirty = false;
    }

    /// <summary>Batched (and every final flush, see OnDeactivateAsync): capture + AWAIT the write, exactly
    /// as pre-008 — the grain turn stalls for the duration of the write.</summary>
    private async Task FlushAsync()
    {
        CaptureSnapshotIntoState();
        await state.WriteStateAsync();
    }

    /// <summary>Background half of FireAndForget's write — never throws (caught and logged here), so
    /// <see cref="_pendingWrite"/> can always be awaited/polled without a try/catch at the call site.</summary>
    private async Task WriteStateBestEffortAsync()
    {
        try
        {
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Background (FireAndForget) flush failed for table history '{Table}'", this.GetPrimaryKeyString());
        }
    }

    private async Task OnFlushTickAsync()
    {
        if (!_dirty) return;

        if (_persistenceMode == TablePersistenceMode.FireAndForget)
        {
            // Single-flight: a write from a previous tick still in flight means this tick is skipped
            // outright rather than overlapped — see TableGrain's identical guard for the full reasoning
            // (JsonFileGrainStorage is not safe against two concurrent writers of the same file/state object).
            if (!_pendingWrite.IsCompleted) return;

            CaptureSnapshotIntoState(); // synchronous, still on this turn
            _pendingWrite = WriteStateBestEffortAsync(); // NOT awaited — turn returns immediately
            return;
        }

        // Batched (MemoryOnly never registers this timer at all — see ResetAsync/ResumeAsync).
        await FlushAsync();
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        // Plan 008 W2.5: same reasoning as TableGrain's OnDeactivateAsync — await any in-flight
        // FireAndForget background write first (never faulted, so no try/catch needed), then skip entirely
        // for MemoryOnly, otherwise flush (capture + awaited write) exactly like Batched always has.
        try { await _pendingWrite; } catch { /* defensive only — WriteStateBestEffortAsync never faults */ }
        if (_persistenceMode != TablePersistenceMode.MemoryOnly && _dirty)
        {
            try { await FlushAsync(); } catch { /* best-effort */ }
        }
        await base.OnDeactivateAsync(reason, cancellationToken);
    }
}
