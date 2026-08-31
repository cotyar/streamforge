using Dapr.Actors.Runtime;
using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.AppCore.Json;
using StreamsForge.Host.Grains;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-B: Dapr counterpart of Orleans' <c>TableHistoryGrain</c>
/// (orleans/src/StreamsForge.Host/Grains/TableHistoryGrain.cs) — one actor per table that has ever had row
/// history configured (actor type "TableHistoryActor", key = the table's <see cref="TableDefinition.Name"/>),
/// maintaining per-row-identity version history fed by <see cref="TableHistoryLookupRequest"/>/
/// <see cref="Streaming.TableHistoryDeltaSink"/>'s forwarding of <c>sf-table-delta</c> envelopes, applying
/// the shared, pure <c>StreamsForge.Host.Grains.TableRowHistoryRetention</c> math (the SAME class
/// <c>TableHistoryGrain</c> uses — see <c>shared/StreamsForge.AppCore/History/TableRowHistory.cs</c>, moved
/// there from Host as part of plan 005's W2 AppCore extraction). Read every method next to its Orleans
/// equivalent; deviations are called out explicitly.
///
/// <para><b>Acyclic, pure LEAF — see <see cref="ITableHistoryActor"/>'s class doc.</b> Everything this
/// actor needs (definition, per-batch deltas) arrives as a method parameter; it never resolves
/// <see cref="ICatalogFacade"/> or any other actor, and unlike <see cref="GeneratorActor"/>/
/// <see cref="PipelineActor"/> it never talks outward to Dapr pub/sub either.</para>
///
/// <para><b>State: persisted continuously via a dirty-flag + periodic flush timer — NOT per-delta,
/// mirroring <c>TableHistoryGrain</c>'s exact write-behind cadence</b> (flush timer period from the owning
/// table's <see cref="TableDefinition.FlushMs"/>, 0 → 2000 ms default; dirty flag set by
/// <see cref="ApplyDeltasAsync"/>, flushed by the timer or on deactivation). <see cref="ResetAsync"/>
/// and <see cref="DisableAsync"/> are the two exceptions — like the grain, both persist IMMEDIATELY
/// (they're rare, config-change-triggered calls, not the hot per-delta-batch path) rather than waiting for
/// the next flush tick.</para>
///
/// <para><b>Plan 008 — shares <see cref="TableDefinition.Persistence"/> with <see cref="TableActor"/>
/// (the SAME per-table knob, not a separate one for history)</b>, dispatched through the same pure
/// <see cref="TablePersistencePolicy.DecideFlushAction"/> decision. <see cref="TablePersistenceMode.
/// Batched"/> is the behavior above, unchanged. <see cref="TablePersistenceMode.FireAndForget"/> hands a
/// deep-cloned DTO (<see cref="TableHistoryApplication.CloneForBackgroundPersist"/>) to a single-flight
/// background write — a deep clone, not a reference copy, is required here specifically because
/// <see cref="TableHistoryActorState.Entries"/>' <see cref="RowHistoryEntry.Versions"/> lists are mutated
/// IN PLACE by <see cref="ApplyDeltasAsync"/> as later turns keep landing while the background write is
/// in flight — unlike <see cref="TableActor"/>'s own <c>_flushed</c>, which is wholesale-replaced (never
/// mutated in place) on every capture, so a bare reference copy is NOT safe here; this is exactly the kind
/// of shape difference the plan 008 brief asked to be surfaced rather than papered over.
/// <see cref="TablePersistenceMode.MemoryOnly"/> never touches <c>StateManager</c> on any path AND — since
/// this actor has no separate live-vs-flushed read split the way <see cref="TableActor"/> does
/// (<see cref="GetHistoryAsync"/>/<see cref="GetStatsAsync"/> already read <see cref="_state"/> directly,
/// always live) — the flush timer is never even armed for a MemoryOnly table, since it would have no
/// purpose (see <see cref="ArmFlushTimerAsync"/>'s guard).</para>
///
/// <para><b>Plan 011 wave C — deliberately NOT changed here, and why.</b> The amplifier this wave removed
/// from <c>TableGrain</c>/<c>TableHistoryGrain</c>/<see cref="TableActor"/> was a per-tick, O(whole table)
/// CLONE on the default path. This actor has no such clone on its default path: <see cref="TablePersistenceMode.
/// Batched"/> hands <see cref="_state"/> itself to <c>StateManager</c>, and the only whole-map deep clone —
/// <see cref="TableHistoryApplication.CloneForBackgroundPersist"/> — runs solely for
/// <see cref="TablePersistenceMode.FireAndForget"/>, where it is not an optimization but a CORRECTNESS
/// requirement (see the paragraph above: the Versions lists are mutated in place by later turns while the
/// background write is in flight). Making that clone incremental would need a retained shadow copy of the
/// whole state living between writes — a second full materialization, i.e. more resident memory, to save
/// allocation on a non-default mode. Not worth it; the honest note is that a FireAndForget history actor
/// still pays O(retained keys) per background write, and that what actually grows without bound on both
/// flavors is the KEY COUNT (per-key version counts are capped; nothing evicts keys). See
/// orleans/DESIGN.md's "Known ceilings".</para>
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
public sealed class TableHistoryActor(ActorHost host, ILogger<TableHistoryActor> logger) : Actor(host), ITableHistoryActor
{
    private const string StateName = "tableHistory";
    private const string FlushTimerName = "tableHistory-flush";

    private TableHistoryActorState _state = new();
    private bool _dirty;
    private bool _flushTimerArmed;

    /// <summary>Plan 008 single-flight guard/in-flight handle for <see cref="TablePersistenceMode.
    /// FireAndForget"/> — same role as <see cref="TableActor"/>'s own fields of the same name (see that
    /// class's doc comments); the background task never throws (catches and logs internally), so joining
    /// <see cref="_inFlightPersist"/> is always safe.</summary>
    private volatile bool _persistInProgress;
    private Task? _inFlightPersist;

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<TableHistoryActorState>(StateName);
        if (existing.HasValue)
        {
            _state = existing.Value;
        }

        if (_state.HistoryEnabled && _state.Persistence != TablePersistenceMode.MemoryOnly)
        {
            await ArmFlushTimerAsync();
        }
    }

    public async Task ResetAsync(TableDefinition def)
    {
        await DisarmFlushTimerIfArmedAsync();

        _state = TableHistoryApplication.Reset(def);
        _dirty = false;
        await SaveControlStateAsync();

        if (_state.HistoryEnabled && _state.Persistence != TablePersistenceMode.MemoryOnly)
        {
            await ArmFlushTimerAsync();
        }
    }

    /// <summary>See <see cref="ITableHistoryActor.EnsureConfiguredAsync"/>'s doc comment for the full
    /// rationale — a no-op (preserving <see cref="_state"/> untouched, including its accumulated
    /// <c>Entries</c>) when <paramref name="def"/>'s history-content config already matches, otherwise
    /// delegates to <see cref="ResetAsync"/> exactly like a genuine config change would.
    ///
    /// <para><b>Plan 008 addendum:</b> <see cref="TableDefinition.Persistence"/>/<see cref="TableDefinition.
    /// FlushMs"/> are NOT history-content config (<see cref="TableHistoryApplication.ConfigMatches"/> never
    /// compares them) — a pure persistence-mode change must never wipe accumulated <c>Entries</c> the way a
    /// SQL/history-mode change legitimately does. When content config matches but the persistence config
    /// has drifted, <see cref="ApplyPersistenceChangeAsync"/> re-arms the timer at the new cadence/mode in
    /// place instead.</para></summary>
    public Task EnsureConfiguredAsync(TableDefinition def)
    {
        if (!TableHistoryApplication.ConfigMatches(_state, def))
        {
            return ResetAsync(def);
        }

        return TableHistoryApplication.PersistenceMatches(_state, def) ? Task.CompletedTask : ApplyPersistenceChangeAsync(def);
    }

    /// <summary>In-place persistence-mode/cadence update — see <see cref="EnsureConfiguredAsync"/>'s plan
    /// 008 addendum. Touches only <see cref="TableHistoryActorState.Persistence"/>/<see cref="
    /// TableHistoryActorState.FlushMs"/> and the timer; <c>Entries</c>/<c>Seq</c>/identity mapping are left
    /// exactly as they were.</summary>
    private async Task ApplyPersistenceChangeAsync(TableDefinition def)
    {
        await DisarmFlushTimerIfArmedAsync();

        _state.Persistence = def.Persistence;
        _state.FlushMs = def.FlushMs;
        await SaveControlStateAsync();

        if (_state.HistoryEnabled && _state.Persistence != TablePersistenceMode.MemoryOnly)
        {
            await ArmFlushTimerAsync();
        }
    }

    public async Task DisableAsync()
    {
        await DisarmFlushTimerIfArmedAsync();

        var wasMemoryOnly = _state.Persistence == TablePersistenceMode.MemoryOnly;
        _state = new TableHistoryActorState();
        _dirty = false;

        // MemoryOnly never touches StateManager on any path (see class doc's plan-008 paragraph) — and
        // since nothing was ever persisted for this table, there is nothing to clear either.
        if (!wasMemoryOnly)
        {
            await SaveAsync();
        }
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

    /// <summary>Dispatches the durability write per <see cref="TableHistoryActorState.Persistence"/> via
    /// the same pure <see cref="TablePersistencePolicy.DecideFlushAction"/> decision <see cref="TableActor"/>
    /// uses — see class doc's plan-008 paragraph. Unlike <see cref="TableActor.OnFlushTickAsync"/> there is
    /// no separate in-memory read-cache to refresh here (<see cref="GetHistoryAsync"/>/<see cref="
    /// GetStatsAsync"/> already read <see cref="_state"/> directly, always live) — this tick is purely
    /// about the durability write.</summary>
    private async Task OnFlushTickAsync()
    {
        if (!_dirty)
        {
            return;
        }

        switch (TableHistoryApplication.DecideHistoryFlushAction(_state.Persistence, dirty: true, writeInProgress: _persistInProgress))
        {
            case TablePersistAction.AwaitedWrite:
                _dirty = false;
                await SaveAsync();
                break;

            case TablePersistAction.BackgroundWrite:
                _dirty = false;
                StartBackgroundPersist();
                break;

            case TablePersistAction.Skip:
            default:
                // MemoryOnly (the timer is never armed for it in practice — see ArmFlushTimerAsync's
                // guard — this branch exists for defensiveness only), or a FireAndForget write already in
                // flight: leave _dirty set so the next tick retries once that write completes. Journaled
                // no longer lands here — see DecideHistoryFlushAction.
                break;
        }
    }

    /// <summary>Kicks off the <see cref="TablePersistenceMode.FireAndForget"/> background write — see class
    /// doc's plan-008 paragraph for why this MUST deep-clone (<see cref="TableHistoryApplication.
    /// CloneForBackgroundPersist"/>) rather than capture a bare reference to <see cref="_state"/>: later
    /// turns keep mutating <see cref="TableHistoryActorState.Entries"/>' version lists IN PLACE while this
    /// write is in flight. Single-flight via <see cref="_persistInProgress"/>; a failure is always logged,
    /// never silently swallowed.</summary>
    private void StartBackgroundPersist()
    {
        var snapshot = TableHistoryApplication.CloneForBackgroundPersist(_state);
        _persistInProgress = true;
        _inFlightPersist = Task.Run(async () =>
        {
            try
            {
                await StateManager.SetStateAsync(StateName, snapshot);
                await StateManager.SaveStateAsync();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "TableHistoryActor: FireAndForget background state write failed — this flush's entries may be lost.");
            }
            finally
            {
                _persistInProgress = false;
            }
        });
    }

    private async Task SaveAsync()
    {
        await StateManager.SetStateAsync(StateName, _state);
        await StateManager.SaveStateAsync();
    }

    /// <summary>Lifecycle (Reset/persistence-change) persist — rare, config-change-triggered, not the hot
    /// per-delta-batch path, so this always awaits/blocks for Batched AND FireAndForget alike (same
    /// rationale as <see cref="TableActor.SaveControlStateAsync"/>'s own doc comment) — only <see
    /// cref="TablePersistenceMode.MemoryOnly"/> skips it, on every path. Joins any still-in-flight
    /// background write first so this call is strictly ordered after it.</summary>
    private async Task SaveControlStateAsync()
    {
        if (_state.Persistence == TablePersistenceMode.MemoryOnly)
        {
            return;
        }

        if (_inFlightPersist is { IsCompleted: false } pending)
        {
            await pending;
        }

        await SaveAsync();
    }

    private async Task ArmFlushTimerAsync()
    {
        var period = TimeSpan.FromMilliseconds(TablePersistencePolicy.ResolveFlushIntervalMs(_state.FlushMs));
        await RegisterTimerAsync(FlushTimerName, nameof(OnFlushTickAsync), null, period, period);
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
    /// auto-save mechanism. Plan 008: skipped entirely for <see cref="TablePersistenceMode.MemoryOnly"/>,
    /// same as every other path (see class doc).</summary>
    protected override async Task OnDeactivateAsync()
    {
        if (!_dirty || _state.Persistence == TablePersistenceMode.MemoryOnly)
        {
            return;
        }

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

/// <summary>Persisted shape of a TableHistoryActor's state — mirrors
/// <c>StreamsForge.Host.Grains.TableHistoryGrainState</c> field-for-field (see that class's doc comments
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

    /// <summary>Plan 008: mirrors the owning table's <see cref="TableDefinition.Persistence"/>/<see
    /// cref="TableDefinition.FlushMs"/> at the time of the last <see cref="TableHistoryActor.ResetAsync"/>
    /// or <see cref="TableHistoryActor.EnsureConfiguredAsync"/> persistence-only update — NOT compared by
    /// <see cref="TableHistoryApplication.ConfigMatches"/> (a pure persistence-mode change must never wipe
    /// <see cref="Entries"/>), only by <see cref="TableHistoryApplication.PersistenceMatches"/>.</summary>
    public TablePersistenceMode Persistence { get; set; } = TablePersistenceMode.Batched;

    /// <summary>See <see cref="Persistence"/>'s doc comment.</summary>
    public int FlushMs { get; set; }
}

/// <summary>
/// Pure state-transition/query logic for <see cref="TableHistoryActor"/>, extracted for the same
/// testability reason as <c>PipelineCompilation</c>/<c>GeneratorBatching</c>/<c>PipelineResultRing</c> (see
/// dapr/tests/StreamsForge.Dapr.Tests/TableHistoryApplicationTests.cs) — every method here is a byte-for-byte
/// port of the matching <c>StreamsForge.Host.Grains.TableHistoryGrain</c> method body, with the grain's
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
        Persistence = def.Persistence,
        FlushMs = def.FlushMs,
    };

    /// <summary>Mirrors <c>ITableHistoryActor.EnsureConfiguredAsync</c>'s decision, as a pure function of
    /// (current state, target definition) so it's testable without any actor machinery — see
    /// dapr/tests/StreamsForge.Dapr.Tests/TableHistoryEnsureConfiguredTests.cs. Returns <paramref name="state"/>
    /// UNCHANGED (same reference, <c>Entries</c> untouched — the restart-safe, don't-lose-history case)
    /// when <see cref="ConfigMatches"/> says the config already matches; otherwise returns a freshly
    /// <see cref="Reset"/> state, exactly like a genuine config change would produce.</summary>
    public static TableHistoryActorState EnsureConfigured(TableHistoryActorState state, TableDefinition def) =>
        ConfigMatches(state, def) ? state : Reset(def);

    /// <summary>True when <paramref name="state"/>'s current history configuration already matches
    /// <paramref name="def"/>'s current settings — i.e. nothing for <see cref="EnsureConfigured"/> to do.
    /// When both agree history is OFF, mode/limit/byField/windowMs/identity-columns are irrelevant (a
    /// disabled actor's leftover fields from a previous configuration, or <see cref="DisableAsync"/>'s own
    /// zeroed defaults, never matter) — so this only compares those when <paramref name="def"/>.HistoryEnabled
    /// is true. Identity columns are compared by re-deriving them from <paramref name="def"/>.Sql (the same
    /// pure <c>TableGroupKeyExtractor.ExtractIdentityColumns</c> call <see cref="Reset"/> itself makes) —
    /// covers a SQL change that altered the GROUP BY identity without necessarily changing any of the
    /// other History* fields.</summary>
    public static bool ConfigMatches(TableHistoryActorState state, TableDefinition def)
    {
        if (state.HistoryEnabled != def.HistoryEnabled)
        {
            return false;
        }

        if (!def.HistoryEnabled)
        {
            return true;
        }

        return state.HistoryMode == def.HistoryMode
            && state.HistoryLimit == def.HistoryLimit
            && state.HistoryByField == def.HistoryByField
            && state.HistoryWindowMs == def.HistoryWindowMs
            && IdentityColumnsEqual(state.IdentityColumns, TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql));
    }

    private static bool IdentityColumnsEqual(List<string>? a, List<string>? b)
    {
        if (a is null || b is null)
        {
            return a is null && b is null;
        }

        return a.SequenceEqual(b, StringComparer.Ordinal);
    }

    /// <summary>
    /// Plan 011 wave C2 — history's own flush-action decision, and THE FIX for the defect wave C found,
    /// documented and left in place here.
    ///
    /// THE DEFECT: this actor has no journal of its own, so <see cref="TablePersistencePolicy.
    /// DecideFlushAction"/>'s <see cref="TablePersistAction.JournalWrite"/> fell through the caller's
    /// switch to the Skip branch. The consequence was not a missed optimization: a table configured
    /// <see cref="TablePersistenceMode.Journaled"/> never persisted its HISTORY AT ALL on this flavor — the
    /// tick left the dirty flag set and retried forever, so every entry lived only in that activation and
    /// vanished on deactivation.
    ///
    /// THE FIX, and why this one of the two available: Orleans' TableHistoryGrain has no journal branch
    /// either, but its flush dispatch falls through to a full AWAITED write, so Journaled behaves exactly
    /// as Batched there — durable, just not O(changed). Mapping JournalWrite to AwaitedWrite here makes the
    /// two flavors agree on that same behavior, and changes no contract: FlushMs, durability and the
    /// grain-turn stall are all identical to Batched's, which is what a Journaled table's HISTORY already
    /// got on the other flavor. Giving history its own journal (so it would be O(changed) too) remains a
    /// genuine open design choice; losing the data was never a choice.
    ///
    /// Pure and public for the same reason the rest of this class is: it is testable without any actor,
    /// timer or sidecar machinery — see dapr/tests/StreamsForge.Dapr.Tests/TableHistoryRetentionTests.cs.
    /// </summary>
    public static TablePersistAction DecideHistoryFlushAction(TablePersistenceMode mode, bool dirty, bool writeInProgress)
    {
        var action = TablePersistencePolicy.DecideFlushAction(mode, dirty, writeInProgress);
        return action == TablePersistAction.JournalWrite ? TablePersistAction.AwaitedWrite : action;
    }

    /// <summary>Plan 008: true when <paramref name="state"/>'s currently-recorded persistence mode/cadence
    /// already matches <paramref name="def"/>'s current settings — the persistence-only counterpart of
    /// <see cref="ConfigMatches"/>, deliberately SEPARATE from it (see <see cref="TableHistoryActorState.
    /// Persistence"/>'s doc comment: comparing these inside <see cref="ConfigMatches"/> itself would make a
    /// pure persistence-mode change take the full <see cref="Reset"/> path and wipe <c>Entries"/> for no
    /// content-related reason).</summary>
    public static bool PersistenceMatches(TableHistoryActorState state, TableDefinition def) =>
        state.Persistence == def.Persistence && state.FlushMs == def.FlushMs;

    /// <summary>Plan 008: a DEEP clone of <paramref name="state"/> for handing to a <see
    /// cref="TablePersistenceMode.FireAndForget"/> background write — see <see cref="TableHistoryActor"/>'s
    /// class doc for why a bare reference (or even a shallow <c>Dictionary</c> copy) is not safe: <see
    /// cref="RowHistoryEntry.Versions"/> lists are mutated IN PLACE by <see cref="ApplyDeltas"/> as later
    /// turns keep landing while the clone is being serialized off-turn. Cloning <see cref="RowHistoryEntry"/>
    /// wrapper objects (each with its own new <see cref="List{T}"/>) is sufficient — the individual <see
    /// cref="HistoryVersion"/> elements inside are effectively immutable after construction (only ever
    /// added/removed from the list, never mutated in place), so it's safe to share those instances between
    /// the live state and the clone.</summary>
    public static TableHistoryActorState CloneForBackgroundPersist(TableHistoryActorState state) => new()
    {
        HistoryEnabled = state.HistoryEnabled,
        HistoryMode = state.HistoryMode,
        HistoryLimit = state.HistoryLimit,
        HistoryByField = state.HistoryByField,
        HistoryWindowMs = state.HistoryWindowMs,
        IdentityColumns = state.IdentityColumns is null ? null : [.. state.IdentityColumns],
        Entries = state.Entries.ToDictionary(
            kv => kv.Key,
            kv => new RowHistoryEntry { Versions = [.. kv.Value.Versions], RetractionCount = kv.Value.RetractionCount }),
        Seq = state.Seq,
        Persistence = state.Persistence,
        FlushMs = state.FlushMs,
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

            // Plan 011 C2 — verbatim mirror of TableHistoryGrain.OnDeltaBatchAsync's eviction branch (see
            // its comment for the full reasoning): a delta the owning table's RETENTION policy produced
            // reclaims the key's whole version trail instead of bumping its retraction counter. History is
            // derived from the table; a row that leaves a bounded table takes its history with it,
            // otherwise the bound would bound the visible row count and none of the memory.
            if (delta.Evicted)
            {
                state.Entries.Remove(key);
                continue;
            }

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
