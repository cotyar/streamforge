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
/// </summary>
public sealed class TableHistoryGrain(
    [PersistentState("tableHistory", StreamConstants.StorageName)] IPersistentState<TableHistoryGrainState> state)
    : Grain, ITableHistoryGrain
{
    private StreamSubscriptionHandle<List<TableDeltaDto>>? _sub;
    private IGrainTimer? _flushTimer;
    private bool _dirty;

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
        _dirty = false;
        await state.WriteStateAsync();

        if (def.HistoryEnabled)
        {
            await SubscribeAsync(def.Name);
            _flushTimer ??= this.RegisterGrainTimer(OnFlushTickAsync, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
            this.DelayDeactivation(TimeSpan.FromDays(365));
        }
        else
        {
            _flushTimer?.Dispose();
            _flushTimer = null;
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
        await state.WriteStateAsync();

        await UnsubscribeAsync();
        await SubscribeAsync(def.Name);
        _flushTimer ??= this.RegisterGrainTimer(OnFlushTickAsync, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    public async Task DisableAsync()
    {
        await UnsubscribeAsync();
        _flushTimer?.Dispose();
        _flushTimer = null;

        state.State = new TableHistoryGrainState();
        _dirty = false;
        await state.WriteStateAsync();
        this.DelayDeactivation(TimeSpan.Zero);
    }

    public Task<TableHistoryQueryResult> GetHistoryAsync(string key, int limit)
    {
        if (!state.State.Entries.TryGetValue(key, out var entry))
        {
            return Task.FromResult(new TableHistoryQueryResult
            {
                Mode = state.State.HistoryMode,
                KeyFound = false,
            });
        }

        TableRowHistoryRetention.PruneWindow(entry, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), state.State.HistoryWindowMs);

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
        KeyCount = state.State.Entries.Count,
        TotalVersions = state.State.Entries.Values.Sum(e => (long)e.Versions.Count),
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
            state.State.Seq++;

            var key = RowKeyCodec.EncodeIdentity(delta.Row, state.State.IdentityColumns);
            if (!state.State.Entries.TryGetValue(key, out var entry))
            {
                entry = new RowHistoryEntry();
                state.State.Entries[key] = entry;
            }

            if (delta.Weight > 0)
            {
                var version = new HistoryVersion(new Dictionary<string, object?>(delta.Row), nowMs, state.State.Seq);
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

    private async Task OnFlushTickAsync()
    {
        if (!_dirty) return;
        _dirty = false;
        await state.WriteStateAsync();
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_dirty)
        {
            try { await state.WriteStateAsync(); } catch { /* best-effort */ }
            _dirty = false;
        }
        await base.OnDeactivateAsync(reason, cancellationToken);
    }
}
