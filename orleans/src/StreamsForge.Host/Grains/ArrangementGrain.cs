using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Engine.Dataflow;
using StreamsForge.Engine.Runtime;

namespace StreamsForge.Host.Grains;

public sealed class ArrangementGrainState
{
    /// <summary>Checkpointed consolidated index: our own dedup key (see ArrangementGrain.CanonicalRowKey,
    /// NOT any Engine-internal canonicalization — this key never crosses a grain boundary, so it only needs
    /// to be stable within this grain) -> (row, weight). Written every CheckpointEveryEpochs flushes.</summary>
    public Dictionary<string, TableRowDto> Snapshot { get; set; } = [];
    public long Epoch { get; set; } = -1;
}

/// <summary>
/// Plan 003 M3: key = "{inputName}:{keySpecHash}:{partition}" — see GrainInterfaces.cs's M3 section doc for
/// the full design (sharing mechanism, refcount lifecycle, race-free seed handoff). Reuses
/// TableIngestGrain's own subscribe/buffer/250ms-or-1000-events-flush/stamp-own-epoch pattern almost
/// verbatim — the difference is (a) this grain filters incoming rows to its OWN partition (each of the P
/// sibling ArrangementGrains for the same (inputName, keySpec) independently subscribes to the FULL input
/// stream and discards rows that don't hash to itself — "subscribes to its input's stream ONCE, per
/// partition" per the M3 task; simpler than a fan-out router at the cost of (P-1)/P redundant stream
/// delivery, acceptable at demo scale — see the M3 report's residue list), (b) it maintains a live
/// consolidated index (not just a pass-through buffer) so AttachAsync/SnapshotAsync can serve a snapshot,
/// and (c) it pushes deltas DIRECTLY to every attached consumer's ITableStageGrain (fan-out, not a single
/// downstream edge) with its OWN partition number as fromPartition — an arrangement partition p is exchanged
/// 1:1 (Local-style, already correctly hash-partitioned) into a consuming join stage's own partition p, never
/// re-hashed — see TableStageGrain's producerCount fix for the consumer side of this contract.
/// </summary>
public sealed class ArrangementGrain(
    [PersistentState("arrangement", StreamConstants.StorageName)] IPersistentState<ArrangementGrainState> state)
    : Grain, IArrangementGrain
{
    private string _inputName = "";
    private bool _isTableInput;
    private List<string> _keyFields = [];
    private string _keySpec = "";
    private int _partitionCount;
    private int _partition;

    private bool _active;
    private bool _rebuilding;
    private StreamSubscriptionHandle<EventRecord>? _streamSub;
    private StreamSubscriptionHandle<List<TableDeltaDto>>? _tableSub;
    private IGrainTimer? _flushTimer;

    private readonly List<TableDelta> _pending = [];
    private long _epochCounter;
    private int _epochsSinceCheckpoint;

    // Our own dedup key (CanonicalRowKey below) -> (row, weight) — the arrangement's authoritative
    // consolidated Z-set index for this partition.
    //
    // Plan 009 wave D: this used to be two hand-rolled dictionaries here (`_index` + a separate `_indexDebt`
    // side-table for outstanding negative running weight, not or no longer visible) — the exact same shape
    // and arithmetic TableExecutorImpl and TableGrain's coordinator mode each separately hand-wrote too. Now
    // a shared ConsolidationLedger (Engine-side, since Host references Engine) — see its own class doc for
    // the full order-independence argument (why a negative delta with no prior positive weight is retained
    // as debt rather than dropped: this grain's input is a delta STREAM whose per-key causal order isn't
    // guaranteed across activation/rebuild-from-checkpoint, and an upstream outer join emits
    // retraction-driven pads). SnapshotAsync/AttachAsync/GetInfoAsync all read `_index.Visible`, which
    // stays exactly the user-visible positive rows — none of them may see debt rows.
    private readonly ConsolidationLedger _index = new();

    // consumerId -> (targetGrainKey, targetEdgeId) for every currently-attached, live-push-eligible consumer.
    private readonly Dictionary<string, (string TargetGrainKey, int TargetEdgeId)> _consumers = [];

    private const int FlushEventThreshold = 1000;
    private static readonly TimeSpan FlushInterval = TimeSpan.FromMilliseconds(250);
    private const int CheckpointEveryEpochs = 40; // ~10s at the 250ms flush cadence

    public async Task AttachAsync(ArrangementAttachRequest request)
    {
        if (!_active)
        {
            await ActivateAsync(request);
        }

        // Atomic seed-then-live handshake — see this class's/GrainInterfaces.cs's doc: push the CURRENT
        // snapshot directly to the new consumer now (consuming one epoch tick, exactly like a real flush
        // does), THEN add it to _consumers so only STRICTLY LATER flushes reach it.
        long snapshotEpoch = _epochCounter++;
        var snapshotDtos = _index.Visible.Values
            .Select(v => new TableDeltaDto { Row = new Dictionary<string, object?>(v.Row), Weight = v.Weight })
            .ToList();
        var target = GrainFactory.GetGrain<ITableStageGrain>(request.TargetGrainKey);
        await target.PushBatchAsync(request.TargetEdgeId, _partition, snapshotEpoch, _inputName, snapshotDtos);

        _consumers[request.ConsumerId] = (request.TargetGrainKey, request.TargetEdgeId);
    }

    public async Task DetachAsync(string consumerId)
    {
        _consumers.Remove(consumerId);
        if (_consumers.Count == 0 && _active)
        {
            await DeactivateAsync();
        }
    }

    public Task<List<TableDeltaDto>> SnapshotAsync() => Task.FromResult(
        _index.Visible.Values.Select(v => new TableDeltaDto { Row = new Dictionary<string, object?>(v.Row), Weight = v.Weight }).ToList());

    public Task<ArrangementInfo> GetInfoAsync() => Task.FromResult(new ArrangementInfo
    {
        InputName = _inputName,
        KeySpec = _keySpec,
        Partition = _partition,
        PartitionCount = _partitionCount,
        RowCount = _index.Visible.Count,
        ConsumerCount = _consumers.Count,
        Rebuilding = _rebuilding,
        Epoch = _epochCounter - 1,
    });

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_active && _consumers.Count > 0)
        {
            try { await CheckpointAsync(); } catch { /* best-effort */ }
        }
        await base.OnDeactivateAsync(reason, cancellationToken);
    }

    private async Task ActivateAsync(ArrangementAttachRequest request)
    {
        _inputName = request.InputName;
        _isTableInput = request.IsTableInput;
        _keyFields = request.KeyFields;
        _keySpec = request.KeySpec;
        _partitionCount = request.PartitionCount;
        _partition = request.Partition;
        _epochsSinceCheckpoint = 0;

        // Rebuild-from-checkpoint (plan 003 M3): if a prior checkpoint survived (silo restart while this
        // arrangement was still attached — refcount never hit 0, so DeactivateAsync never cleared it), seed
        // the index from it and mark Rebuilding until the first live batch confirms catch-up. A DETACH-to-
        // zero always clears the checkpoint (see DeactivateAsync) so a genuinely fresh attach (after every
        // consumer stopped/deleted) always starts from a truly empty state, not stale data.
        if (state.State.Snapshot.Count > 0)
        {
            foreach (var kv in state.State.Snapshot)
            {
                // Seed, not Apply — the checkpoint only ever holds positive-weight rows (see
                // ConsolidationLedger.Seed's doc comment), so the normal weight-folding arithmetic doesn't
                // apply here.
                _index.Seed(kv.Key, new EventRecord(kv.Value.Row), kv.Value.Weight);
            }
            _epochCounter = state.State.Epoch + 1;
            _rebuilding = true;
        }
        else
        {
            _epochCounter = 0;
            _rebuilding = false;
        }

        // Plan 021 D3/D6 — this grain's OWN primary key is "{qualifiedInputName}:{keySpecHash}:{partition}"
        // (TableGrain.StartCoordinatorAsync composes it that way — see its arrangement-attach loop), so the
        // qualified input name is recoverable from the key itself without adding an Environment field to
        // the frozen, shared/-owned ArrangementAttachRequest: split on ':' and take the first component.
        // _inputName (from request.InputName) stays BARE — it is also handed to Engine-facing calls
        // (OnBatch's originName / TableExecutor.OnTableDelta), which compare it against the compiled
        // plan's own bare names.
        var qualifiedInputName = this.GetPrimaryKeyString().Split(':')[0];
        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        if (_isTableInput)
        {
            var stream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, qualifiedInputName));
            _tableSub = await stream.SubscribeAsync((batch, _) => OnTableDeltaBatchAsync(batch));
        }
        else
        {
            var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, qualifiedInputName));
            _streamSub = await stream.SubscribeAsync((evt, _) => OnStreamEventAsync(evt));
        }

        _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, FlushInterval, FlushInterval);
        _active = true;
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    private async Task DeactivateAsync()
    {
        _active = false;
        _flushTimer?.Dispose();
        _flushTimer = null;

        if (_streamSub is not null) { try { await _streamSub.UnsubscribeAsync(); } catch { /* best-effort */ } _streamSub = null; }
        if (_tableSub is not null) { try { await _tableSub.UnsubscribeAsync(); } catch { /* best-effort */ } _tableSub = null; }

        _pending.Clear();
        _index.Clear();
        _rebuilding = false;

        // Last detach clears state (plan 003 M3 refcount+GC requirement) — a future attach for this exact
        // (inputName, keySpec, partition) identity rebuilds lazily from live traffic, not from another
        // table's leftover checkpoint.
        state.State.Snapshot = [];
        state.State.Epoch = -1;
        try { await state.WriteStateAsync(); } catch { /* best-effort */ }

        this.DelayDeactivation(TimeSpan.Zero);
    }

    private Task OnStreamEventAsync(EventRecord evt)
    {
        if (!_active) return Task.CompletedTask;
        if (ArrangementKeySpec.PartitionOfRow(_keyFields, evt, _partitionCount) != _partition) return Task.CompletedTask;
        _pending.Add(new TableDelta(evt, 1)); // a stream event always asserts — same convention as TableIngestGrain
        return _pending.Count >= FlushEventThreshold ? FlushAsync() : Task.CompletedTask;
    }

    private Task OnTableDeltaBatchAsync(List<TableDeltaDto> batch)
    {
        if (!_active) return Task.CompletedTask;
        foreach (var d in batch)
        {
            var evt = new EventRecord(d.Row);
            if (ArrangementKeySpec.PartitionOfRow(_keyFields, evt, _partitionCount) != _partition) continue;
            _pending.Add(new TableDelta(evt, d.Weight));
        }
        return _pending.Count >= FlushEventThreshold ? FlushAsync() : Task.CompletedTask;
    }

    private Task OnFlushTickAsync() => FlushAsync();

    private async Task FlushAsync()
    {
        if (!_active) return;

        var batch = _pending.Count == 0 ? [] : new List<TableDelta>(_pending);
        _pending.Clear();
        long epoch = _epochCounter++;

        foreach (var d in batch) ApplyToIndex(d);
        if (batch.Count > 0) _rebuilding = false; // live traffic observed since (re)activation

        if (_consumers.Count > 0)
        {
            var dtos = batch.Select(d => new TableDeltaDto { Row = new Dictionary<string, object?>(d.Row), Weight = d.Weight }).ToList();
            var tasks = _consumers.Values.Select(c =>
                GrainFactory.GetGrain<ITableStageGrain>(c.TargetGrainKey).PushBatchAsync(c.TargetEdgeId, _partition, epoch, _inputName, dtos));
            await Task.WhenAll(tasks);
        }

        _epochsSinceCheckpoint++;
        if (_epochsSinceCheckpoint >= CheckpointEveryEpochs)
        {
            await CheckpointAsync();
            _epochsSinceCheckpoint = 0;
        }
    }

    private void ApplyToIndex(TableDelta d)
    {
        // CanonicalRowKey (below) is this grain's OWN dedup key, deliberately not Engine's internal
        // canonicalization (see its own doc comment); the weight-folding arithmetic itself lives in
        // ConsolidationLedger.Apply — see its class doc for the order-independence argument this method
        // used to carry locally.
        var key = CanonicalRowKey(d.Row);
        _index.Apply(key, d.Row, d.Weight);
    }

    private async Task CheckpointAsync()
    {
        state.State.Snapshot = _index.Visible.ToDictionary(
            kv => kv.Key,
            kv => new TableRowDto { Row = new Dictionary<string, object?>(kv.Value.Row), Weight = kv.Value.Weight });
        state.State.Epoch = _epochCounter - 1;
        await state.WriteStateAsync();
    }

    /// <summary>Stable dedup key for THIS grain's own index only — never serialized across a grain boundary
    /// or compared against any other grain's canonicalization, so (unlike StreamsForge.Engine's internal
    /// TableKeyEncoding/JsonText canonicalizers, which Host has no InternalsVisibleTo access to — see
    /// TableGrain's own class doc) a plain sorted-keys JSON dump is sufficient: deterministic within this
    /// process/grain for the plain CLR value types (long/double/string/bool/null/nested list/dict from JSON
    /// fields) that ever appear in an EventRecord.</summary>
    private static string CanonicalRowKey(EventRecord row)
    {
        var sorted = new SortedDictionary<string, object?>(row, StringComparer.Ordinal);
        return System.Text.Json.JsonSerializer.Serialize(sorted);
    }
}
