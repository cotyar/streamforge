using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Engine.Dataflow;

namespace StreamForge.Host.Grains;

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
    private readonly Dictionary<string, (EventRecord Row, long Weight)> _index = [];

    // Outstanding NEGATIVE running weight per canonical key, for keys whose net weight-so-far is <= 0 and
    // are therefore not (or no longer) in _index — the same side table (and the same reasoning) as
    // TableExecutorImpl's `_debtWeights`, which this index is the per-partition analogue of. A negative
    // delta can legitimately arrive here for a key with no prior positive weight: this grain's input is a
    // delta STREAM whose per-key causal order isn't guaranteed across activation/rebuild-from-checkpoint,
    // and an upstream outer join emits retraction-driven pads. Discarding that negative (the old bug) loses
    // information — a later positive delta for the same key would then start fresh instead of netting
    // against the outstanding debt, so a row whose true total weight is 0 could resurface at a positive
    // weight depending on arrival order alone.
    //
    // Kept separate from _index rather than letting _index hold weight <= 0 entries so that _index stays
    // exactly the user-visible positive rows — SnapshotAsync/AttachAsync push it verbatim and RowCount
    // counts it, none of which may see debt rows. The two are disjoint by construction (ApplyToIndex always
    // removes from whichever one a key is NOT written into) and a key netting to exactly 0 leaves both, so
    // neither accumulates residue for fully cancelled rows. That is the DBSP invariant making this ledger
    // order-independent: the value looked up before folding in a new delta is always the exact running sum
    // of every delta seen so far for that key, so — integer addition being commutative and associative —
    // a key's final classification depends only on the SUM of its deltas, never on their arrival order.
    private readonly Dictionary<string, long> _indexDebt = [];

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
        var snapshotDtos = _index.Values
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
        _index.Values.Select(v => new TableDeltaDto { Row = new Dictionary<string, object?>(v.Row), Weight = v.Weight }).ToList());

    public Task<ArrangementInfo> GetInfoAsync() => Task.FromResult(new ArrangementInfo
    {
        InputName = _inputName,
        KeySpec = _keySpec,
        Partition = _partition,
        PartitionCount = _partitionCount,
        RowCount = _index.Count,
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
                _index[kv.Key] = (new EventRecord(kv.Value.Row), kv.Value.Weight);
            }
            _epochCounter = state.State.Epoch + 1;
            _rebuilding = true;
        }
        else
        {
            _epochCounter = 0;
            _rebuilding = false;
        }

        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        if (_isTableInput)
        {
            var stream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, _inputName));
            _tableSub = await stream.SubscribeAsync((batch, _) => OnTableDeltaBatchAsync(batch));
        }
        else
        {
            var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, _inputName));
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
        _indexDebt.Clear();
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
        var key = CanonicalRowKey(d.Row);

        // Running weight for this key BEFORE folding in `d`, wherever it currently lives (positive in
        // _index, negative in _indexDebt, or 0/absent from both — never in both at once). See _indexDebt.
        long currentWeight = _index.TryGetValue(key, out var existing)
            ? existing.Weight
            : _indexDebt.GetValueOrDefault(key);

        long newWeight = currentWeight + d.Weight;

        if (newWeight > 0)
        {
            // Same canonical key => same row content, so either representative is equivalent (the Engine's
            // ApplyConsolidation likewise always stores the incoming row).
            _index[key] = (d.Row, newWeight);
            _indexDebt.Remove(key);
        }
        else if (newWeight < 0)
        {
            _index.Remove(key);
            _indexDebt[key] = newWeight;
        }
        else // newWeight == 0: fully cancelled out — no residue in either structure.
        {
            _index.Remove(key);
            _indexDebt.Remove(key);
        }
    }

    private async Task CheckpointAsync()
    {
        state.State.Snapshot = _index.ToDictionary(
            kv => kv.Key,
            kv => new TableRowDto { Row = new Dictionary<string, object?>(kv.Value.Row), Weight = kv.Value.Weight });
        state.State.Epoch = _epochCounter - 1;
        await state.WriteStateAsync();
    }

    /// <summary>Stable dedup key for THIS grain's own index only — never serialized across a grain boundary
    /// or compared against any other grain's canonicalization, so (unlike StreamForge.Engine's internal
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
