using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Connectors.Crdt;
using StreamForge.Engine;
using Ycs;

namespace StreamForge.Host.Grains;

public sealed class CrdtDocGrainState
{
    public SourceDefinition? Def { get; set; }
    public bool Running { get; set; }

    /// <summary>Plan 020 wave B-2 durability, and its stated ceiling: the WHOLE document, re-encoded from
    /// scratch after every completed merge (<c>YDoc.EncodeStateAsUpdateV1()</c> with no target state
    /// vector — the entire current state, not a delta). Rehydrated with a single <c>ApplyUpdateV1</c> on
    /// activation (<see cref="CrdtDocGrain.RehydrateDoc"/>).
    ///
    /// <para><b>ponytail:</b> this is a whole-state rewrite on every merge — fine for wave B's document
    /// sizes, expensive for a document with a long edit history once <c>Gc</c>-eligible tombstones start
    /// piling up between merges. Plan 020 wave C's job, explicitly NOT this wave's: keep a snapshot plus a
    /// compacted update log (<c>UpdateOperations.MergeUpdates</c>) instead of one growing blob, verified
    /// against a killed-and-restarted instance rather than in-process. This wave stops at "correct and
    /// simple", not "efficient at scale" — see plan 020's wave table, round 3.</para></summary>
    public byte[]? DocBytes { get; set; }

    public long UpdatesMerged { get; set; }
    public long RowsEmittedTotal { get; set; }
}

/// <summary>Plan 020 wave B-2 — the Orleans driver for <see cref="SourceKinds.Crdt"/> sources (D3: a grain
/// of its own, dispatched via <c>SourceKindDispatch.ActorKind.Crdt</c>, never through
/// <see cref="IConnectorGrain"/>). Copies <see cref="ConnectorGrain"/>'s shape, not
/// <c>TableShardGrain</c>'s (D4): <c>[PersistentState]</c> written after every completed merge,
/// <see cref="OnActivateAsync"/> self-resumes (rehydrates the document) when persisted <c>Running</c> is
/// true, and a <see cref="_generation"/> counter so a yielded turn's stale continuation abandons its
/// result rather than clobbering a fresher one — see <see cref="MergeAsync"/>'s own comment for exactly
/// where that matters here (it is a narrower window than <c>ConnectorGrain</c>'s poll cycle: nothing
/// awaits between <c>ApplyUpdateV1</c> and the projection diff, only around stream emission and the
/// state write that follow it).
///
/// <para><b>Emission goes through the one existing door</b> (plan D1/D4): one <see cref="EventRecord"/>
/// per changed row onto <c>(StreamConstants.SourcesNamespace, this.GetPrimaryKeyString())</c>, stamped
/// with <c>_source</c>/<c>_ts</c> exactly like <see cref="ConnectorGrain.EmitRowsAsync"/> does. A table
/// subscribed by name cannot tell a document from a generator or a connector.</para>
///
/// <para><b>D5 — config is carried, never looked up.</b> <see cref="SourceDefinition.Fields"/> and
/// <see cref="CrdtSourceConfig"/> are stamped into grain state by <see cref="StartAsync"/> and read from
/// THERE for every subsequent <see cref="MergeAsync"/> call — never re-fetched from
/// <c>IRegistryGrain</c>/<c>ICatalogFacade</c> mid-merge. <c>RegistryGrain</c> is non-reentrant with a
/// <c>Get*</c>-only <c>[MayInterleave]</c> allowlist; a merge path that "just needs the current schema"
/// is exactly how <c>TableShardRouterGrain</c>'s own doc comment says this deadlock gets
/// reintroduced.</para>
///
/// <para><b>D8 — <c>Gc = true</c>.</b> The Ycs default, left alone: history for a twin already lives in
/// the delta journal and table row history (both with retention), and <c>Gc = false</c> would make
/// personal data in a document effectively undeletable.</para>
///
/// <para><b>Durability ceiling</b> — see <see cref="CrdtDocGrainState.DocBytes"/>'s own doc comment.</para>
/// </summary>
public sealed class CrdtDocGrain(
    [PersistentState("crdtDoc", StreamConstants.StorageName)] IPersistentState<CrdtDocGrainState> state)
    : Grain, ICrdtDocGrain
{
    /// <summary>See <see cref="ConnectorGrain._generation"/>'s own doc comment for the general hazard this
    /// guards against. Here specifically: <see cref="MergeAsync"/> mutates <see cref="_doc"/> and computes
    /// the before/after projection entirely synchronously (no <c>await</c> in between), so the ONLY window
    /// in which a concurrent <see cref="StartAsync"/>/<see cref="StopAsync"/> could interleave is around
    /// the stream-emission loop and the state write that follow — both awaited. A generation mismatch
    /// there means this activation's <see cref="_doc"/> reference may already have been replaced by a
    /// fresh <see cref="RehydrateDoc"/> call, so this call must not persist over whatever the newer
    /// activation already wrote.</summary>
    private int _generation;

    /// <summary>The live document for the CURRENTLY active generation. Null before the first
    /// <see cref="StartAsync"/>/resume, and never read by <see cref="MergeAsync"/>/<see cref="GetStatusAsync"/>
    /// in that state (both check <c>state.State.Running</c> first).</summary>
    private YDoc? _doc;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        if (state.State.Running && state.State.Def is not null)
        {
            RehydrateDoc();
        }
        return base.OnActivateAsync(cancellationToken);
    }

    public async Task StartAsync(SourceDefinition def)
    {
        state.State.Def = def;
        state.State.Running = true;
        _generation++;
        RehydrateDoc();
        await state.WriteStateAsync();
    }

    public async Task StopAsync()
    {
        state.State.Running = false;
        _generation++;
        await state.WriteStateAsync();
    }

    public Task PingAsync() => Task.CompletedTask;

    /// <summary>Rebuilds <see cref="_doc"/> from persisted bytes (or a fresh empty document if none yet) —
    /// called on every <see cref="StartAsync"/> AND on a resuming <see cref="OnActivateAsync"/>, so both a
    /// restart and a config-only re-<c>StartAsync</c> land on the SAME document content; only
    /// <see cref="SourceDefinition"/> (fields/config) changes underneath it. D8: <c>Gc = true</c>, the Ycs
    /// default, stated explicitly rather than left implicit.</summary>
    private void RehydrateDoc()
    {
        _doc = new YDoc(new YDocOptions { Gc = true });
        if (state.State.DocBytes is { Length: > 0 } bytes)
        {
            _doc.ApplyUpdateV1(bytes);
        }
    }

    /// <summary>The whole algorithm plan 020 wave B names: flatten before, apply every update (one bad
    /// frame counted and skipped, never aborting the batch — D7's "a flaky link must not strand every good
    /// one behind it"), flatten after, diff, emit, persist. See this class's own doc comment for the
    /// generation-guard window.</summary>
    public async Task<CrdtMergeResult> MergeAsync(IReadOnlyList<byte[]> updates)
    {
        var def = state.State.Def;
        if (def is null || !state.State.Running || _doc is null)
        {
            // Defensive floor — ICrdtFacade resolves the source (existence + Crdt-kind) before reaching a
            // grain call, so a correctly-wired caller only lands here for a source that exists, is
            // crdt-kind, and is STOPPED. That case must not answer with a bare zero: "0 applied, 0 rows"
            // is byte-identical to a successful idempotent replay (D7), and an edge draining its
            // store-and-forward buffer into a stopped document would read its own data loss as success.
            // The silent middle is the one outcome this platform refuses — see CatalogStore's
            // "WHERE KEY SHARDING IS REFUSED ON THIS FLAVOR" note for the same call made there.
            return new CrdtMergeResult
            {
                Diagnostics =
                {
                    $"document '{state.State.Def?.Name ?? this.GetPrimaryKeyString()}' is not running — "
                    + "nothing was merged and nothing was emitted; start the source and re-send this batch "
                    + "(re-sending is safe: merging is idempotent).",
                },
            };
        }

        var generation = _generation;
        var doc = _doc;
        var config = def.Connector?.Crdt ?? new CrdtSourceConfig();
        var diagnostics = new List<string>();

        var before = CrdtProjector.Flatten(doc, config, def.Fields, diagnostics);

        var applied = 0;
        for (var i = 0; i < updates.Count; i++)
        {
            try
            {
                doc.ApplyUpdateV1(updates[i]);
                applied++;
            }
            catch (Exception ex)
            {
                // D7's "does not abort the batch": one corrupt frame is counted here and the loop
                // continues onto the next update rather than throwing out of MergeAsync entirely.
                diagnostics.Add($"update[{i}]: failed to decode/apply ({ex.GetType().Name}: {ex.Message}) — skipped");
            }
        }

        var after = CrdtProjector.Flatten(doc, config, def.Fields, diagnostics);
        var rows = CrdtProjector.Diff(before, after, config);

        if (rows.Count > 0)
        {
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var stream = this.GetStreamProvider(StreamConstants.ProviderName)
                .GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, this.GetPrimaryKeyString()));
            foreach (var row in rows)
            {
                row.TryAdd("_source", def.Name);
                row.TryAdd("_ts", nowMs);
                await stream.OnNextAsync(new EventRecord(row));
            }
        }

        if (generation != _generation)
        {
            // A StartAsync/StopAsync ran while the emission loop above was awaiting — this activation's
            // _doc may already be a different object (RehydrateDoc replaced it), and whoever restarted it
            // has already written the status it wants. The rows already reached the stream (not
            // undoable), but persisting THIS call's counters/DocBytes over a newer write would be exactly
            // the stale-clobbers-fresh bug ConnectorGrain.RunCycleAsync's identical check exists to
            // prevent. Report what happened; skip the persist.
            return new CrdtMergeResult { UpdatesApplied = applied, RowsEmitted = rows.Count, Diagnostics = diagnostics };
        }

        state.State.DocBytes = doc.EncodeStateAsUpdateV1();
        state.State.UpdatesMerged += applied;
        state.State.RowsEmittedTotal += rows.Count;
        await state.WriteStateAsync();

        return new CrdtMergeResult { UpdatesApplied = applied, RowsEmitted = rows.Count, Diagnostics = diagnostics };
    }

    public Task<CrdtDocStatus> GetStatusAsync()
    {
        var def = state.State.Def;
        if (def is null || !state.State.Running || _doc is null)
        {
            return Task.FromResult(new CrdtDocStatus { Error = "not running" });
        }

        var config = def.Connector?.Crdt ?? new CrdtSourceConfig();
        var rootMapName = string.IsNullOrEmpty(config.RootMap) ? "root" : config.RootMap;

        return Task.FromResult(new CrdtDocStatus
        {
            // Live keys only — a deleted key doesn't enumerate (CrdtSourceConfig's own doc comment), so
            // this is "entities excluding tombstones" for free, with no extra bookkeeping.
            EntityCount = _doc.GetMap(rootMapName).Count,
            UpdatesMerged = state.State.UpdatesMerged,
            RowsEmitted = state.State.RowsEmittedTotal,
        });
    }
}
