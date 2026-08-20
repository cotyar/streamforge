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

    /// <summary>Plan 020 wave C durability: a COMPACTED SNAPSHOT (<c>YDoc.EncodeStateAsUpdateV1()</c> with
    /// no target state vector — the entire document state as of the last compaction, not a delta), no
    /// longer re-encoded on every merge. The raw bytes of updates accepted since this snapshot live in
    /// <see cref="PendingUpdates"/>; <see cref="CrdtDocGrain.RehydrateDoc"/> applies this snapshot first,
    /// then each log entry in order.
    ///
    /// <para><b>Honest accounting</b> (see <see cref="CrdtDocGrain"/>'s own doc comment for the full
    /// version): this buys back the per-merge <c>EncodeStateAsUpdateV1</c> CPU cost wave B paid on every
    /// single call, and it stops a document with a long edit history from re-serializing its own entire
    /// state, tombstones included, on every merge. It does NOT reduce persisted bytes under
    /// <c>JsonFileGrainStorage</c> — <c>IGrainStorage.WriteStateAsync</c> has no append; the whole
    /// <see cref="CrdtDocGrainState"/> blob (snapshot AND log) is serialized in full on every write, same
    /// as wave B's whole-blob-per-merge did. Between compactions the blob is temporarily LARGER than wave
    /// B's (snapshot plus a growing log, where wave B had only ever the snapshot).</para></summary>
    public byte[]? DocBytes { get; set; }

    /// <summary>Raw accepted-update bytes merged since <see cref="DocBytes"/> was last compacted — see
    /// <see cref="CrdtDocGrain.MergeAsync"/>'s append site for what "accepted" means (a frame that failed
    /// to decode/apply never lands here) and <see cref="CrdtDocGrain.CompactionEntryThreshold"/> /
    /// <see cref="CrdtDocGrain.CompactionByteThreshold"/> for when this list gets folded back into
    /// <see cref="DocBytes"/> and cleared. Additive relative to wave B's shape (which had no such
    /// property) — a state file wave B wrote deserializes with this defaulting to an empty list, so
    /// <see cref="CrdtDocGrain.RehydrateDoc"/> reads it back exactly as wave B would have. Never null.</summary>
    public List<byte[]> PendingUpdates { get; set; } = [];

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

    /// <summary>Plan 020 wave D, finding 3 — lazily constructed the first time
    /// <see cref="MergeAttributedAsync"/> actually needs to write an attribution, and ALWAYS reset to
    /// <c>null</c> by <see cref="RehydrateDoc"/> alongside <see cref="_doc"/>: a <see cref="PermanentUserData"/>
    /// instance is bound to the specific <see cref="YDoc"/> it was constructed against (its
    /// <c>YUsers</c> map reference and its internal <c>Clients</c>/<c>Dss</c> caches), so reusing one
    /// across a rehydrate would read and write against an orphaned, no-longer-current document. Staying
    /// null for a document that never opts into <see cref="CrdtSourceConfig.AttributeChanges"/> is the
    /// mechanism that makes the flag cost nothing when off — no "users" map is ever touched, so an
    /// existing document's bytes are unaffected.</summary>
    private PermanentUserData? _pud;

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

    /// <summary>Plan 020 wave C: how large <see cref="CrdtDocGrainState.PendingUpdates"/> is allowed to
    /// grow, by entry count, before <see cref="MergeAsync"/> folds it back into
    /// <see cref="CrdtDocGrainState.DocBytes"/> via <see cref="UpdateOperations.MergeUpdates"/> and clears
    /// it.
    ///
    /// <para><b>ponytail:</b> the trade this knob makes. A HIGHER threshold means fewer
    /// <c>MergeUpdates</c> calls (less CPU spent compacting) but a longer straight-line
    /// <c>ApplyUpdateV1</c> replay in <see cref="RehydrateDoc"/> on the NEXT activation, and a bigger
    /// persisted blob sitting between compactions. A LOWER threshold is the opposite. 32 is picked so the
    /// worst-case replay on activation is "a few dozen small ApplyUpdateV1 calls", not hundreds — cheap
    /// either way relative to the merge/emit work MergeAsync already does per call.</para></summary>
    private const int CompactionEntryThreshold = 32;

    /// <summary>The companion byte-size bound: guards against a SINGLE oversized update (a multi-megabyte
    /// batch from an edge with a long offline history) sitting in the log unbounded just because the entry
    /// COUNT hasn't crossed <see cref="CompactionEntryThreshold"/> yet — one update near or past this size
    /// forces a compaction right after it lands, same as 32 small ones would.</summary>
    private const long CompactionByteThreshold = 2 * 1024 * 1024; // 2 MB

    /// <summary>Rebuilds <see cref="_doc"/> from the persisted snapshot (or a fresh empty document if none
    /// yet), then replays <see cref="CrdtDocGrainState.PendingUpdates"/> in order on top of it — called on
    /// every <see cref="StartAsync"/> AND on a resuming <see cref="OnActivateAsync"/>, so both a restart
    /// and a config-only re-<c>StartAsync</c> land on the SAME document content; only
    /// <see cref="SourceDefinition"/> (fields/config) changes underneath it. D8: <c>Gc = true</c>, the Ycs
    /// default, stated explicitly rather than left implicit.
    ///
    /// <para>Deliberately simple, per wave C's brief: no <see cref="UpdateOperations.MergeUpdates"/> on
    /// this path — that only runs at compaction time (<see cref="CompactLog"/>). A wave-B-shaped state
    /// (full <see cref="CrdtDocGrainState.DocBytes"/>, no log — <see cref="CrdtDocGrainState.PendingUpdates"/>
    /// deserializes to an empty list when the persisted JSON has no such property) replays byte-identically
    /// to how wave B's single-<c>ApplyUpdateV1</c> RehydrateDoc did.</para></summary>
    private void RehydrateDoc()
    {
        _doc = new YDoc(new YDocOptions { Gc = true });
        if (state.State.DocBytes is { Length: > 0 } bytes)
        {
            _doc.ApplyUpdateV1(bytes);
        }

        foreach (var update in state.State.PendingUpdates)
        {
            _doc.ApplyUpdateV1(update);
        }

        // Wave D finding 3 — see _pud's own doc comment for why this MUST be dropped here rather than
        // carried over from the previous _doc.
        _pud = null;
    }

    /// <summary>Folds <see cref="CrdtDocGrainState.PendingUpdates"/> back into
    /// <see cref="CrdtDocGrainState.DocBytes"/> via <see cref="UpdateOperations.MergeUpdates"/> — safe
    /// because merging is associative and idempotent and explicitly accepts updates that were themselves
    /// produced by an earlier merge (the existing snapshot IS exactly that: a prior merge's output), per
    /// that method's own doc comment. Caller's responsibility to call <c>state.WriteStateAsync()</c>
    /// afterward — this only mutates the in-memory <see cref="IPersistentState{T}.State"/> object.</summary>
    private void CompactLog()
    {
        var toMerge = new List<byte[]>();
        if (state.State.DocBytes is { Length: > 0 } snapshot)
        {
            toMerge.Add(snapshot);
        }

        toMerge.AddRange(state.State.PendingUpdates);
        if (toMerge.Count > 0)
        {
            state.State.DocBytes = toMerge.Count == 1 ? toMerge[0] : UpdateOperations.MergeUpdates(toMerge);
        }

        state.State.PendingUpdates.Clear();
    }

    /// <summary>The whole algorithm plan 020 wave B names: flatten before, apply every update (one bad
    /// frame counted and skipped, never aborting the batch — D7's "a flaky link must not strand every good
    /// one behind it"), flatten after, diff, emit, persist. See this class's own doc comment for the
    /// generation-guard window. Delegates to <see cref="MergeCoreAsync"/> with no actor — see that
    /// method for wave D finding 3's attribution step, which this signature stays frozen against.</summary>
    public Task<CrdtMergeResult> MergeAsync(IReadOnlyList<byte[]> updates) => MergeCoreAsync(updates, actor: null);

    /// <summary>Plan 020 wave D, finding 3 — <see cref="MergeCoreAsync"/> with attribution turned on for
    /// this call. See <see cref="ICrdtDocGrain.MergeAttributedAsync"/> and
    /// <see cref="CrdtSourceConfig.AttributeChanges"/> for the contract and its documented boundary.</summary>
    public Task<CrdtMergeResult> MergeAttributedAsync(IReadOnlyList<byte[]> updates, string actor) =>
        MergeCoreAsync(updates, actor);

    private async Task<CrdtMergeResult> MergeCoreAsync(IReadOnlyList<byte[]> updates, string? actor)
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
        // Wave C hazard 1: only bytes that ACTUALLY applied go in acceptedUpdates, which is what feeds the
        // durable log below. A frame that failed to decode/apply must never reach it — if it did,
        // RehydrateDoc would rethrow decoding it on every future activation, and a single corrupt byte
        // from an edge would permanently deny-of-service this grain (it would never activate again).
        var acceptedUpdates = new List<byte[]>(updates.Count);
        for (var i = 0; i < updates.Count; i++)
        {
            try
            {
                doc.ApplyUpdateV1(updates[i]);
                applied++;
                acceptedUpdates.Add(updates[i]);
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

        // Wave D finding 3 — attribution, opt-in (actor is null on the plain MergeAsync path, and this
        // is a no-op even when non-null unless the source has ALSO opted into AttributeChanges: see
        // CrdtSourceConfig.AttributeChanges's own doc comment for what this writes and its documented
        // boundary against GetUserByDeletedId). Placed here — after the diff, before emission — so it
        // can never influence the before/after projection (it writes into the "users" root map, a
        // sibling of config.RootMap that CrdtProjector.Flatten never reads) and stays inside the same
        // synchronous, no-await window MergeCoreAsync's own class doc names for the generation guard.
        //
        // acceptedUpdates gets the PRODUCED bytes appended, not just a side effect recorded elsewhere:
        // CompactLog folds PendingUpdates byte-for-byte and never re-derives DocBytes from the live
        // _doc, so an attribution write that is not captured this way would vanish on the next
        // RehydrateDoc (compaction, or a restart) even though _doc itself still had it moments ago.
        if (actor is not null && config.AttributeChanges && acceptedUpdates.Count > 0)
        {
            AttributeAcceptedUpdates(doc, acceptedUpdates, actor, diagnostics);
        }

        await EmitRowsAsync(rows, def);

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

        // Wave C hazard 3: this append sits inside the SAME generation-guard skip as the rest of the
        // persist, above — a stale continuation whose generation has already moved on must not append to
        // a fresher activation's log any more than it may overwrite its DocBytes/counters.
        //
        // Wave C hazard 4: a redelivered batch (D7) applies cleanly and changes nothing, so its bytes are
        // "accepted" and land here too — acceptable by design, not an oversight. The threshold below
        // bounds how large that gets, and MergeUpdates collapses the redundancy away at compaction time
        // (it is explicitly idempotent per its own doc comment), so a no-op replay costs bounded log
        // growth, never unbounded growth or a wrong document.
        state.State.PendingUpdates.AddRange(acceptedUpdates);
        state.State.UpdatesMerged += applied;
        state.State.RowsEmittedTotal += rows.Count;

        var logBytes = 0L;
        foreach (var u in state.State.PendingUpdates)
        {
            logBytes += u.Length;
        }

        if (state.State.PendingUpdates.Count >= CompactionEntryThreshold || logBytes >= CompactionByteThreshold)
        {
            CompactLog();
        }

        await state.WriteStateAsync();

        return new CrdtMergeResult { UpdatesApplied = applied, RowsEmitted = rows.Count, Diagnostics = diagnostics };
    }

    /// <summary>Plan 020 wave D, finding 3. Maps every Yjs client id that contributed to
    /// <paramref name="acceptedUpdates"/> onto <paramref name="actor"/> via
    /// <see cref="PermanentUserData.SetUserMapping"/>, mutates <paramref name="acceptedUpdates"/> in
    /// place to append the bytes those writes themselves produced (see the call site's own comment for
    /// why that append is load-bearing, not decoration), and never throws past a per-update decode
    /// failure — this runs after <see cref="MergeCoreAsync"/> has already decided which caller-supplied
    /// bytes counted as applied, and a client-id parsing hiccup here must not undo that.</summary>
    private void AttributeAcceptedUpdates(YDoc doc, List<byte[]> acceptedUpdates, string actor, List<string> diagnostics)
    {
        _pud ??= new PermanentUserData(doc);

        var produced = new List<byte[]>();
        void OnUpdate(object? _, (byte[] data, object origin, Transaction transaction) e) => produced.Add(e.data);

        doc.UpdateV1 += OnUpdate;
        try
        {
            var seenClients = new HashSet<int>();
            foreach (var bytes in acceptedUpdates)
            {
                UpdateMeta meta;
                try
                {
                    meta = UpdateOperations.ParseUpdateMeta(bytes);
                }
                catch (Exception ex)
                {
                    // This update already applied cleanly moments ago (it is only in acceptedUpdates
                    // because MergeCoreAsync's own apply loop succeeded on it) — ParseUpdateMeta failing
                    // here would be a genuine surprise, not the expected shape of untrusted input, but
                    // attribution is explicitly a best-effort add-on: the merge itself must not be put
                    // at risk by it.
                    diagnostics.Add($"attribution: could not read client id(s) from an accepted update ({ex.GetType().Name}: {ex.Message}) — that update's writer was not attributed");
                    continue;
                }

                foreach (var clientIdLong in meta.From.Keys)
                {
                    // PermanentUserData's own API is int-keyed (Yjs client ids are generated in the
                    // 32-bit range); this cast matches what any caller of that class is forced to do.
                    var clientId = unchecked((int)clientIdLong);
                    if (!seenClients.Add(clientId))
                    {
                        continue; // Already handled earlier in this same call.
                    }

                    if (_pud.GetUserByClientId(clientId) == actor)
                    {
                        continue; // Already mapped to this exact actor by an earlier merge — do not grow the log for nothing.
                    }

                    _pud.SetUserMapping(doc, clientId, actor);
                }
            }
        }
        finally
        {
            doc.UpdateV1 -= OnUpdate;
        }

        acceptedUpdates.AddRange(produced);
    }

    /// <summary>The one door out of this grain (plan D1/D4) — one <see cref="EventRecord"/> per row onto
    /// <c>(SourcesNamespace, primaryKey)</c>, stamped exactly as <see cref="ConnectorGrain.EmitRowsAsync"/>
    /// stamps a connector's rows, so a subscribed table cannot tell a document from a generator. Shared by
    /// <see cref="MergeCoreAsync"/> (deltas, via <see cref="MergeAsync"/>/<see cref="MergeAttributedAsync"/>)
    /// and <see cref="ReplayAsync"/> (a full re-assert) so the three can never drift apart in how a row
    /// reaches the platform.</summary>
    private async Task EmitRowsAsync(List<Dictionary<string, object?>> rows, SourceDefinition def)
    {
        if (rows.Count == 0)
        {
            return;
        }

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

    /// <summary>Diffing the live projection against THIS is what turns "the document's current state" into
    /// a full set of create rows — no second projection path, just <see cref="CrdtProjector.Diff"/> with
    /// nothing on the before side.</summary>
    private static readonly Dictionary<string, Dictionary<string, object?>> EmptyProjection = new(StringComparer.Ordinal);

    /// <summary>Plan 020 wave C. See <see cref="ICrdtDocGrain.ReplayAsync"/> for why this exists at all —
    /// short version: D7 makes an update replay a no-op, so a consumer that lost its rows cannot be
    /// refilled by re-sending history, only by re-asserting current state.
    ///
    /// <para>Merges nothing and therefore cannot corrupt the document — it only reads it. The generation
    /// guard is the same one <see cref="MergeAsync"/> documents, for the same reason: the emission loop
    /// awaits, so a <see cref="StartAsync"/>/<see cref="StopAsync"/> can land underneath it.</para></summary>
    public async Task<CrdtMergeResult> ReplayAsync()
    {
        var def = state.State.Def;
        if (def is null || !state.State.Running || _doc is null)
        {
            // Same reasoning as MergeAsync's defensive floor: a bare zero here is indistinguishable from
            // "the document is genuinely empty", and the caller asked for a refill precisely because
            // something downstream is empty. Say which one it is.
            return new CrdtMergeResult
            {
                Diagnostics =
                {
                    $"document '{state.State.Def?.Name ?? this.GetPrimaryKeyString()}' is not running — "
                    + "nothing was replayed; start the source and re-issue the replay.",
                },
            };
        }

        var generation = _generation;
        var config = def.Connector?.Crdt ?? new CrdtSourceConfig();
        var diagnostics = new List<string>();

        var rows = CrdtProjector.Diff(
            EmptyProjection,
            CrdtProjector.Flatten(_doc, config, def.Fields, diagnostics),
            config);

        await EmitRowsAsync(rows, def);

        if (generation == _generation)
        {
            state.State.RowsEmittedTotal += rows.Count;
            await state.WriteStateAsync();
        }

        // UpdatesApplied stays 0: nothing was merged. A caller reading "0 applied, N rows" from THIS
        // method is reading the truth, not MergeAsync's replay-was-a-no-op signal.
        return new CrdtMergeResult { RowsEmitted = rows.Count, Diagnostics = diagnostics };
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
