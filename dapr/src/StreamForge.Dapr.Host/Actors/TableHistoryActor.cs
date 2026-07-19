using Dapr.Actors.Runtime;
using StreamForge.Abstractions;
using StreamForge.Abstractions.Streaming;
using StreamForge.AppCore.Json;
using StreamForge.Host.Grains;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-B: Dapr counterpart of Orleans' <c>TableHistoryGrain</c>
/// (orleans/src/StreamForge.Host/Grains/TableHistoryGrain.cs) — one actor per table that has ever had row
/// history configured (actor type "TableHistoryActor", key = the table's <see cref="TableDefinition.Name"/>),
/// maintaining per-row-identity version history fed by <see cref="TableHistoryLookupRequest"/>/
/// <see cref="Streaming.TableHistoryDeltaSink"/>'s forwarding of <c>sf-table-delta</c> envelopes, applying
/// the shared, pure <c>StreamForge.Host.Grains.TableRowHistoryRetention</c> math (the SAME class
/// <c>TableHistoryGrain</c> uses — see <c>shared/StreamForge.AppCore/History/TableRowHistory.cs</c>, moved
/// there from Host as part of plan 005's W2 AppCore extraction). Read every method next to its Orleans
/// equivalent; deviations are called out explicitly.
///
/// <para><b>Acyclic, pure LEAF — see <see cref="ITableHistoryActor"/>'s class doc.</b> Everything this
/// actor needs (definition, per-batch deltas) arrives as a method parameter; it never resolves
/// <see cref="ICatalogFacade"/> or any other actor, and unlike <see cref="GeneratorActor"/>/
/// <see cref="PipelineActor"/> it never talks outward to Dapr pub/sub either.</para>
///
/// <para><b>State: persisted continuously via a dirty-flag + periodic flush timer — NOT per-delta,
/// mirroring <c>TableHistoryGrain</c>'s exact write-behind cadence</b> (2-second flush timer, dirty flag
/// set by <see cref="ApplyDeltasAsync"/>, flushed by the timer or on deactivation). <see cref="ResetAsync"/>
/// and <see cref="DisableAsync"/> are the two exceptions — like the grain, both persist IMMEDIATELY
/// (they're rare, config-change-triggered calls, not the hot per-delta-batch path) rather than waiting for
/// the next flush tick.</para>
///
/// <para><b>Self-heal on reactivation — same rationale as <see cref="GeneratorActor"/>/
/// <see cref="PipelineActor"/>'s own doc comments: Dapr actor timers do NOT survive deactivation/
/// reactivation.</b> <see cref="OnActivateAsync"/> reloads persisted state and, if history was left
/// enabled, immediately re-arms the flush timer — otherwise a reactivated actor would silently stop
/// flushing its dirty entries until some other event re-triggered <see cref="ResetAsync"/> (which,
/// unlike the flush timer, never happens on a mere reactivation).</para>
///
/// <para><b>Pure state-transition/query logic is extracted to <see cref="TableHistoryApplication"/></b> —
/// same reason as <c>PipelineCompilation</c>/<c>GeneratorBatching</c>/<c>PipelineResultRing</c> (see those
/// classes' own doc comments): unit-testable without any actor/timer/Dapr-sidecar machinery. This class is
/// the thin actor shell around it: activation/state load-save, timer arm/disarm, and the
/// <see cref="ITableHistoryActor"/> method signatures.</para>
/// </summary>
public sealed class TableHistoryActor(ActorHost host) : Actor(host), ITableHistoryActor
{
    private const string StateName = "tableHistory";
    private const string FlushTimerName = "tableHistory-flush";

    /// <summary>Same cadence as <c>TableHistoryGrain</c>'s own 2-second <c>IGrainTimer</c> flush.</summary>
    private static readonly TimeSpan FlushPeriod = TimeSpan.FromSeconds(2);

    private TableHistoryActorState _state = new();
    private bool _dirty;
    private bool _flushTimerArmed;

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<TableHistoryActorState>(StateName);
        if (existing.HasValue)
        {
            _state = existing.Value;
        }

        if (_state.HistoryEnabled)
        {
            await ArmFlushTimerAsync();
        }
    }

    public async Task ResetAsync(TableDefinition def)
    {
        await DisarmFlushTimerIfArmedAsync();

        _state = TableHistoryApplication.Reset(def);
        _dirty = false;
        await StateManager.SetStateAsync(StateName, _state);

        if (_state.HistoryEnabled)
        {
            await ArmFlushTimerAsync();
        }
    }

    public async Task DisableAsync()
    {
        await DisarmFlushTimerIfArmedAsync();

        _state = new TableHistoryActorState();
        _dirty = false;
        await StateManager.SetStateAsync(StateName, _state);
    }

    public Task ApplyDeltasAsync(TableDeltaEnvelope envelope)
    {
        if (TableHistoryApplication.ApplyDeltas(_state, envelope))
        {
            _dirty = true;
        }

        return Task.CompletedTask;
    }

    public Task<TableHistoryQueryResult> GetHistoryAsync(TableHistoryLookupRequest request) =>
        Task.FromResult(TableHistoryApplication.Query(_state, request.Key, request.Limit));

    public Task<TableHistoryStats> GetStatsAsync() => Task.FromResult(TableHistoryApplication.Stats(_state));

    private async Task OnFlushTickAsync()
    {
        if (!_dirty)
        {
            return;
        }

        _dirty = false;
        await StateManager.SetStateAsync(StateName, _state);
    }

    private async Task ArmFlushTimerAsync()
    {
        await RegisterTimerAsync(FlushTimerName, nameof(OnFlushTickAsync), null, FlushPeriod, FlushPeriod);
        _flushTimerArmed = true;
    }

    private async Task DisarmFlushTimerIfArmedAsync()
    {
        if (!_flushTimerArmed)
        {
            return;
        }

        await UnregisterTimerAsync(FlushTimerName);
        _flushTimerArmed = false;
    }

    /// <summary>Best-effort final flush on deactivation, mirroring <c>TableHistoryGrain.OnDeactivateAsync</c>
    /// exactly (same try/catch-and-swallow — a deactivating actor losing its last few seconds of
    /// unflushed deltas is an accepted, pre-existing tradeoff of the write-behind design, not something
    /// this method should throw over). Unlike the periodic flush tick (itself an ordinary actor method
    /// invocation, auto-saved by the Dapr actor runtime after it returns —
    /// <c>Actor.OnPostActorMethodAsyncInternal</c>), deactivation is NOT a normal method turn, so this
    /// explicitly calls <see cref="ActorStateManager.SaveStateAsync"/> rather than relying on that
    /// auto-save mechanism.</summary>
    protected override async Task OnDeactivateAsync()
    {
        if (_dirty)
        {
            try
            {
                await StateManager.SetStateAsync(StateName, _state);
                await StateManager.SaveStateAsync();
            }
            catch
            {
                // best-effort, mirrors TableHistoryGrain.OnDeactivateAsync's own empty catch.
            }

            _dirty = false;
        }
    }
}

/// <summary>Persisted shape of a TableHistoryActor's state — mirrors
/// <c>StreamForge.Host.Grains.TableHistoryGrainState</c> field-for-field (see that class's doc comments
/// for what each field means). Plain get/set properties, same style as
/// <see cref="GeneratorActorState"/>/<see cref="PipelineActorState"/>/<see cref="Catalog.CatalogState"/>,
/// for a clean System.Text.Json round trip through Dapr's actor state store.</summary>
public sealed class TableHistoryActorState
{
    public bool HistoryEnabled { get; set; }
    public TableHistoryMode HistoryMode { get; set; } = TableHistoryMode.All;
    public int HistoryLimit { get; set; } = 10;
    public string? HistoryByField { get; set; }
    public long HistoryWindowMs { get; set; }

    /// <summary>Cached result of <c>TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql)</c>, recomputed
    /// on every <see cref="TableHistoryActor.ResetAsync"/>. Null = no derivable GROUP BY/LATEST BY
    /// identity; row keys fall back to whole-row encoding (see <c>RowKeyCodec.EncodeIdentity</c>).</summary>
    public List<string>? IdentityColumns { get; set; }

    /// <summary>Row-identity key (<c>RowKeyCodec.EncodeIdentity</c>) -> retained version history.</summary>
    public Dictionary<string, RowHistoryEntry> Entries { get; set; } = [];

    /// <summary>Monotonic counter, incremented once per observed delta (assertion or retraction) — see
    /// <see cref="HistoryVersion.Seq"/>'s doc comment.</summary>
    public long Seq { get; set; }
}

/// <summary>
/// Pure state-transition/query logic for <see cref="TableHistoryActor"/>, extracted for the same
/// testability reason as <c>PipelineCompilation</c>/<c>GeneratorBatching</c>/<c>PipelineResultRing</c> (see
/// dapr/tests/StreamForge.Dapr.Tests/TableHistoryApplicationTests.cs) — every method here is a byte-for-byte
/// port of the matching <c>StreamForge.Host.Grains.TableHistoryGrain</c> method body, with the grain's
/// <c>[PersistentState]</c> field access replaced by an explicit <see cref="TableHistoryActorState"/>
/// parameter.
/// </summary>
public static class TableHistoryApplication
{
    /// <summary>Mirrors <c>TableHistoryGrain.ResetAsync</c>'s state construction exactly: always clears
    /// previously accumulated history (a brand new <see cref="TableHistoryActorState"/>, not a mutation of
    /// an existing one), reconfigures Enabled/Mode/Limit/ByField/WindowMs from <paramref name="def"/>'s
    /// current history settings, and re-derives <see cref="TableHistoryActorState.IdentityColumns"/> from
    /// <paramref name="def"/>.Sql via <c>TableGroupKeyExtractor.ExtractIdentityColumns</c> — so a SQL
    /// change (which can change the GROUP BY identity) always gets a freshly re-derived key mapping, never
    /// a stale one left over from before the change.</summary>
    public static TableHistoryActorState Reset(TableDefinition def) => new()
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

    /// <summary>Applies one <c>sf-table-delta</c> batch to <paramref name="state"/> in place — mirrors
    /// <c>TableHistoryGrain.OnDeltaBatchAsync</c> exactly, including the actor-wire JsonElement
    /// re-normalization (see <see cref="ITableHistoryActor.ApplyDeltasAsync"/>'s doc comment) that MUST run
    /// before <c>RowKeyCodec.EncodeIdentity</c> ever sees a delta's <c>Row</c> — a
    /// <see cref="System.Text.Json.JsonElement"/> falls through <c>RowKeyCodec.EncodeValue</c>'s
    /// unwrapping case for nested values inside a dictionary, but the top-level dictionary itself needs
    /// every value plain before any of this project's row-shape assumptions (identity encoding, retention
    /// comparisons) hold. Returns true if any entry changed (i.e. the caller should mark itself dirty for
    /// the next write-behind flush) — false for a disabled table or an empty batch, the same cheap no-op
    /// the grain's own early return (<c>if (!state.State.HistoryEnabled || batch.Count == 0) return;</c>)
    /// gives.</summary>
    public static bool ApplyDeltas(TableHistoryActorState state, TableDeltaEnvelope envelope)
    {
        if (!state.HistoryEnabled || envelope.Deltas.Count == 0)
        {
            return false;
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var delta in envelope.Deltas)
        {
            JsonValueNormalizer.NormalizeInPlace(delta.Row);

            state.Seq++;

            var key = RowKeyCodec.EncodeIdentity(delta.Row, state.IdentityColumns);
            if (!state.Entries.TryGetValue(key, out var entry))
            {
                entry = new RowHistoryEntry();
                state.Entries[key] = entry;
            }

            if (delta.Weight > 0)
            {
                var version = new HistoryVersion(new Dictionary<string, object?>(delta.Row), nowMs, state.Seq);
                TableRowHistoryRetention.Append(entry, version, state.HistoryMode, state.HistoryLimit, state.HistoryByField, state.HistoryWindowMs);
            }
            else
            {
                entry.RetractionCount++;
            }
        }

        return true;
    }

    /// <summary>Mirrors <c>TableHistoryGrain.GetHistoryAsync</c> exactly: prune-on-read (time-window
    /// pruning happens here too, not only on append, so an entry that hasn't been touched in a while still
    /// reports accurately), newest-first ordering by <c>Seq</c>, <paramref name="limit"/> &lt;= 0 means
    /// "all retained versions".</summary>
    public static TableHistoryQueryResult Query(TableHistoryActorState state, string key, int limit)
    {
        if (!state.Entries.TryGetValue(key, out var entry))
        {
            return new TableHistoryQueryResult { Mode = state.HistoryMode, KeyFound = false };
        }

        TableRowHistoryRetention.PruneWindow(entry, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), state.HistoryWindowMs);

        var ordered = entry.Versions.OrderByDescending(v => v.Seq).ToList();
        var limited = limit > 0 ? ordered.Take(limit).ToList() : ordered;

        return new TableHistoryQueryResult
        {
            Versions = limited,
            RetractionCount = entry.RetractionCount,
            Mode = state.HistoryMode,
            TotalVersions = entry.Versions.Count,
            KeyFound = true,
        };
    }

    /// <summary>Mirrors <c>TableHistoryGrain.GetStatsAsync</c> exactly.</summary>
    public static TableHistoryStats Stats(TableHistoryActorState state) => new()
    {
        Enabled = state.HistoryEnabled,
        Mode = state.HistoryMode,
        KeyCount = state.Entries.Count,
        TotalVersions = state.Entries.Values.Sum(e => (long)e.Versions.Count),
    };
}
