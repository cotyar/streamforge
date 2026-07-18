using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Engine.Dataflow;
using StreamForge.Host.Search;

namespace StreamForge.Host.Grains;

public sealed class TableGrainState
{
    /// <summary>Consolidated output snapshot: canonical rowKey -> (row, weight). Write-behind persisted
    /// (dirty flag + periodic flush) — see TableGrain's class comment for the restart-resume tradeoff.</summary>
    public Dictionary<string, TableRowDto> Snapshot { get; set; } = [];
    public long Seq { get; set; }
}

/// <summary>
/// Key = table name. One activation per running table. Subscribes to its SQL's stream inputs (the
/// existing "sources" namespace) and table inputs ("table-delta", upstreamName), feeds every delta
/// through a StreamForge.Engine <see cref="TableExecutor"/> (Z-set / DBSP-style incremental view
/// maintenance), publishes emitted deltas to ("table-delta", ownName) for downstream tables and
/// StreamBridgeService, and persists a consolidated snapshot with write-behind (dirty flag, flushed every
/// 2s or on deactivate — mirrors PipelineGrain's metrics-timer pattern; one JSON write per delta would
/// thrash).
///
/// RESTART-RESUME LIMITATION: the persisted snapshot only ever captures this table's OUTPUT rows, not its
/// operators' internal state (join indexes, GROUP BY multisets/accumulators). That internal state cannot
/// be reconstructed from the output alone, so a full checkpoint/replay is out of scope here (future work).
/// The honest tradeoff taken: on resume, the last-flushed snapshot is served immediately for read
/// availability (GetRowsAsync keeps returning it), but the table is marked "rebuilding" and its executor +
/// snapshot are reset to empty — it rebuilds purely from live traffic going forward, exactly like a table
/// that just started for the first time. GetMetricsAsync exposes the Rebuilding flag.
///
/// PLAN 003 M2 — PARALLELISM &gt;= 2 (coordinator mode): everything above this paragraph describes the
/// Parallelism==1 fast path, kept byte-for-byte unchanged (see the Parallelism&lt;=1 branch in StartAsync/
/// StopAsync below — zero-risk default per the M2 task). For Parallelism &gt;= 2, this grain becomes a
/// coordinator + read grain instead of running the SQL itself: StartAsync deploys the partitioned graph
/// (one ITableOutputGrain, one ITableStageGrain per (non-Ingest stage, partition), one ITableIngestGrain
/// per real external input — see StreamForge.Engine.Dataflow.TableDataflowPlan and TableIngestGrain/
/// TableStageGrain/TableOutputGrain's class docs), then subscribes to its OWN
/// (StreamConstants.TableDeltaNamespace, tableName) delta stream — the same stream TableOutputGrain
/// publishes to — and feeds those deltas into EXACTLY the same read-side machinery
/// (state.State.Snapshot + TableSearchIndex) the Parallelism==1 path already uses, just fed by the
/// partitioned graph's output instead of a locally-run TableExecutor. Rows/search/metrics/history/SignalR
/// all therefore go through the identical code paths regardless of Parallelism — see
/// GetRowsAsync/GetMetricsAsync/SearchAsync below, none of which branch on Parallelism at all. Consolidation
/// of the incoming delta stream (Z-set summation: weight &lt;= 0 removes, else updates) is reimplemented
/// here directly on the public TableRowDto shape rather than reusing StreamForge.Engine.Runtime's internal
/// consolidation (Host has no InternalsVisibleTo into Engine — see AssemblyInfo.cs — matching the existing
/// precedent in TableRowHistory.cs, which re-derives its own key logic rather than reaching into Engine
/// internals); a scratch TableExecutor (created, never fed any events) supplies CanonicalRowKey, the one
/// piece of key-derivation logic that IS already public.
/// </summary>
public sealed class TableGrain(
    [PersistentState("table", StreamConstants.StorageName)] IPersistentState<TableGrainState> state)
    : Grain, ITableGrain
{
    private TableDefinition? _def;
    private PipelineStatus _status = PipelineStatus.Stopped;
    private TableExecutor? _executor;
    private TableSearchIndex? _searchIndex;
    private IGrainTimer? _flushTimer;
    private readonly List<StreamSubscriptionHandle<EventRecord>> _streamSubs = [];
    private readonly List<StreamSubscriptionHandle<List<TableDeltaDto>>> _tableSubs = [];

    private bool _dirty;
    private bool _rebuilding;
    private long _deltasIn;
    private long _deltasOut;
    private long _lastUpdateMs;

    // Plan 003 M2 — Parallelism >= 2 coordinator-mode state (see class doc). Unused, always default, on
    // the Parallelism==1 path.
    private bool _coordinatorMode;
    private int _coordinatorParallelism;
    private List<(int StageId, int PartitionCount)> _deployedStages = [];
    private List<string> _deployedInputs = [];
    private StreamSubscriptionHandle<List<TableDeltaDto>>? _coordinatorSub;
    /// <summary>Coordinator mode's own live consolidated Z-set (canonical row key -> (row, weight)) — the
    /// coordinator-mode analogue of TableExecutor's internal `_consolidated` (not reachable from Host —
    /// see class doc), fed by <see cref="OnCoordinatorDeltaBatchAsync"/> and read by
    /// ReflectDeltasInSearchIndex/FlushAsync/SearchAsync exactly where the classic path reads
    /// `_executor.Snapshot()`.</summary>
    private readonly Dictionary<string, (EventRecord Row, long Weight)> _coordinatorSnapshot = [];

    public async Task StartAsync(TableDefinition def)
    {
        await StopAsync();

        _def = def;

        if (def.Parallelism <= 1)
        {
            await StartClassicAsync(def);
        }
        else
        {
            await StartCoordinatorAsync(def);
        }
    }

    private async Task StartClassicAsync(TableDefinition def)
    {
        var registry = GrainFactory.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var sources = await registry.GetSourcesAsync();
        var streamSchemas = sources.ToDictionary(
            s => s.Name,
            s => new SourceSchema(s.Name, s.Fields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var tables = await registry.GetTablesAsync();
        var tableSchemas = tables
            .Where(t => t.OutputFields.Count > 0)
            .ToDictionary(
                t => t.Name,
                t => new SourceSchema(t.Name, t.OutputFields.ToDictionary(f => f.Name, f => MapFieldKind(f.Type))));

        var compileResult = SqlCompiler.CompileTable(def.Sql, streamSchemas, tableSchemas);
        if (!compileResult.Ok || compileResult.Plan is null)
        {
            var message = string.Join("; ", compileResult.Diagnostics.Select(d => $"{d.Line}:{d.Column} {d.Message}"));
            throw new InvalidOperationException(message);
        }

        _executor = compileResult.Plan.CreateExecutor();
        _status = PipelineStatus.Running;

        // See class-level comment: a non-empty persisted snapshot means this is a resume (not a first
        // start) — operator internal state can't be rebuilt from it, so mark rebuilding and reset to empty.
        if (state.State.Snapshot.Count > 0)
        {
            _rebuilding = true;
            state.State.Snapshot = [];
            state.State.Seq = 0;
            _dirty = true;
        }

        // Either branch above leaves the current row set empty (fresh start, or reset-for-rebuild), so a
        // freshly built (empty) index is accurate here — it fills back in incrementally as
        // ApplyAndPublishAsync observes deltas going forward, exactly like state.State.Snapshot does via
        // FlushAsync (just without the 2s lag, since Snapshot() is an O(1) live dictionary reference).
        _searchIndex = def.SearchEnabled ? new TableSearchIndex(def.SearchMode) : null;

        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        foreach (var name in compileResult.StreamInputs.Distinct())
        {
            var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, name));
            var handle = await stream.SubscribeAsync((evt, _) => OnStreamEventAsync(name, evt));
            _streamSubs.Add(handle);
        }
        foreach (var name in compileResult.TableInputs.Distinct())
        {
            var stream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, name));
            var handle = await stream.SubscribeAsync((deltas, _) => OnTableDeltaBatchAsync(name, deltas));
            _tableSubs.Add(handle);
        }

        _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));

        // Keep this activation alive for as long as the table is running — mirrors PipelineGrain.
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    /// <summary>Plan 003 M2 — see class doc's coordinator-mode paragraph. Deploys the partitioned graph in
    /// dependency order (TableOutputGrain, then every TableStageGrain, THEN every TableIngestGrain last) so
    /// no early delta gets silently dropped by a not-yet-started downstream grain (every M2 grain no-ops a
    /// call received before its own StartAsync — see TableStageGrain/TableOutputGrain.PushBatchAsync/
    /// PublishAsync's `_status != Running` guard); subscribes to this table's own output stream BEFORE any
    /// of that deployment, for the same reason on the read side.</summary>
    private async Task StartCoordinatorAsync(TableDefinition def)
    {
        var (compileResult, dataflow) = await TableDataflowFactory.BuildAsync(GrainFactory, def);

        _executor = compileResult.Plan!.CreateExecutor(); // scratch instance: CanonicalRowKey only, never fed an event
        _status = PipelineStatus.Running;
        _coordinatorMode = true;
        _coordinatorParallelism = def.Parallelism;

        // _coordinatorSnapshot (unlike _executor above) is a grain-instance field that outlives a single
        // StartAsync/StopAsync cycle — the grain activation itself isn't torn down on StopAsync, only its
        // subscriptions/sub-grains are. Without clearing it here, a restart-resume would silently resurrect
        // pre-restart rows into the freshly-reset state.State.Snapshot on the next flush, breaking the same
        // "rebuild purely from live traffic" contract the classic path gets for free by allocating a brand
        // new (empty) TableExecutor on every StartClassicAsync call.
        _coordinatorSnapshot.Clear();

        if (state.State.Snapshot.Count > 0)
        {
            _rebuilding = true;
            state.State.Snapshot = [];
            state.State.Seq = 0;
            _dirty = true;
        }
        _searchIndex = def.SearchEnabled ? new TableSearchIndex(def.SearchMode) : null;

        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        var ownStream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, def.Name));
        _coordinatorSub = await ownStream.SubscribeAsync((batch, _) => OnCoordinatorDeltaBatchAsync(batch));

        await GrainFactory.GetGrain<ITableOutputGrain>(def.Name).StartAsync(def);

        _deployedStages = dataflow.Stages
            .Where(s => s.Kind != TableStageKind.Ingest)
            .Select(s => (s.StageId, dataflow.PartitionCountOf(s.StageId)))
            .ToList();
        foreach (var (stageId, partitionCount) in _deployedStages)
        {
            for (int p = 0; p < partitionCount; p++)
            {
                await GrainFactory.GetGrain<ITableStageGrain>($"{def.Name}:{stageId}:{p}").StartAsync(def, stageId, p);
            }
        }

        _deployedInputs = compileResult.StreamInputs.Concat(compileResult.TableInputs).Distinct().ToList();
        foreach (var inputName in _deployedInputs)
        {
            await GrainFactory.GetGrain<ITableIngestGrain>($"{def.Name}:{inputName}").StartAsync(def, inputName);
        }

        _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    public async Task StopAsync()
    {
        _status = PipelineStatus.Stopped;

        _flushTimer?.Dispose();
        _flushTimer = null;

        foreach (var handle in _streamSubs)
        {
            try { await handle.UnsubscribeAsync(); } catch { /* best-effort */ }
        }
        _streamSubs.Clear();

        foreach (var handle in _tableSubs)
        {
            try { await handle.UnsubscribeAsync(); } catch { /* best-effort */ }
        }
        _tableSubs.Clear();

        if (_coordinatorSub is not null)
        {
            try { await _coordinatorSub.UnsubscribeAsync(); } catch { /* best-effort */ }
            _coordinatorSub = null;
        }

        if (_coordinatorMode && _def is not null)
        {
            foreach (var inputName in _deployedInputs)
            {
                try { await GrainFactory.GetGrain<ITableIngestGrain>($"{_def.Name}:{inputName}").StopAsync(); } catch { /* best-effort */ }
            }
            foreach (var (stageId, partitionCount) in _deployedStages)
            {
                for (int p = 0; p < partitionCount; p++)
                {
                    try { await GrainFactory.GetGrain<ITableStageGrain>($"{_def.Name}:{stageId}:{p}").StopAsync(); } catch { /* best-effort */ }
                }
            }
            try { await GrainFactory.GetGrain<ITableOutputGrain>(_def.Name).StopAsync(); } catch { /* best-effort */ }
            _deployedInputs = [];
            _deployedStages = [];
        }
        _coordinatorMode = false;

        if (_dirty)
        {
            await FlushAsync();
        }

        _executor = null;
        _searchIndex = null;
        this.DelayDeactivation(TimeSpan.Zero);
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_dirty)
        {
            try { await FlushAsync(); } catch { /* best-effort */ }
        }
        await base.OnDeactivateAsync(reason, cancellationToken);
    }

    public Task<List<TableRowDto>> GetRowsAsync(int limit, int offset)
    {
        var rows = state.State.Snapshot.Values
            .Skip(Math.Max(0, offset))
            .Take(Math.Max(0, limit))
            .ToList();
        return Task.FromResult(rows);
    }

    public Task<int> GetRowCountAsync() => Task.FromResult(state.State.Snapshot.Count);

    /// <summary>Plan 003 M2: in coordinator mode (Parallelism &gt;= 2), additively fans out to every
    /// deployed TableStageGrain for per-partition detail (TableMetrics.Partitions) — null/absent on the
    /// Parallelism==1 path, so existing consumers see byte-identical JSON.</summary>
    public async Task<TableMetrics> GetMetricsAsync()
    {
        List<TablePartitionMetrics>? partitions = null;
        if (_coordinatorMode && _def is not null)
        {
            var tasks = _deployedStages
                .SelectMany(s => Enumerable.Range(0, s.PartitionCount)
                    .Select(p => GrainFactory.GetGrain<ITableStageGrain>($"{_def.Name}:{s.StageId}:{p}").GetMetricsAsync()));
            partitions = (await Task.WhenAll(tasks)).ToList();
        }

        return new TableMetrics
        {
            TableId = _def?.Id ?? this.GetPrimaryKeyString(),
            Status = _status,
            RowCount = state.State.Snapshot.Count,
            DeltasIn = _deltasIn,
            DeltasOut = _deltasOut,
            LastUpdateMs = _lastUpdateMs,
            Rebuilding = _rebuilding,
            Partitions = partitions,
        };
    }

    public Task<long> GetSeqAsync() => Task.FromResult(state.State.Seq);

    public Task<List<TableRowDto>> SearchAsync(string query, int limit)
    {
        if (_searchIndex is null || string.IsNullOrWhiteSpace(query))
        {
            return Task.FromResult(new List<TableRowDto>());
        }

        IReadOnlyDictionary<string, (EventRecord Row, long Weight)>? snapshot = _coordinatorMode ? _coordinatorSnapshot : _executor?.Snapshot();
        var hits = _searchIndex.Search(query, limit);
        var rows = hits.Select(h =>
        {
            long weight = snapshot is not null && snapshot.TryGetValue(h.RowKey, out var current) ? current.Weight : 1;
            return new TableRowDto { Row = new Dictionary<string, object?>(h.Row), Weight = weight };
        }).ToList();
        return Task.FromResult(rows);
    }

    private async Task OnStreamEventAsync(string source, EventRecord evt)
    {
        if (_executor is null) return;

        _deltasIn++;
        _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _rebuilding = false; // live traffic observed since resume (or this is a first-ever start — already false)

        var deltas = _executor.OnStreamEvent(source, evt);
        if (deltas.Count > 0)
        {
            await ApplyAndPublishAsync(deltas);
        }
    }

    private async Task OnTableDeltaBatchAsync(string table, List<TableDeltaDto> batch)
    {
        if (_executor is null) return;

        var outAll = new List<TableDelta>();
        foreach (var d in batch)
        {
            _deltasIn++;
            var result = _executor.OnTableDelta(table, new TableDelta(new EventRecord(d.Row), d.Weight));
            outAll.AddRange(result);
        }
        _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _rebuilding = false;

        if (outAll.Count > 0)
        {
            await ApplyAndPublishAsync(outAll);
        }
    }

    /// <summary>Plan 003 M2 coordinator-mode read path: this table's OWN (StreamConstants.TableDeltaNamespace,
    /// tableName) stream — published by TableOutputGrain — feeds directly into the same
    /// snapshot+search machinery the classic path uses, via <see cref="_coordinatorSnapshot"/> in place of
    /// `_executor.Snapshot()` (see class doc). No SQL runs here — the partitioned graph already computed
    /// these deltas; this grain only consolidates + persists + indexes them for reads.</summary>
    private Task OnCoordinatorDeltaBatchAsync(List<TableDeltaDto> batch)
    {
        if (_status != PipelineStatus.Running || _executor is null || batch.Count == 0) return Task.CompletedTask;

        var deltas = new List<TableDelta>(batch.Count);
        foreach (var d in batch)
        {
            var delta = new TableDelta(new EventRecord(d.Row), d.Weight);
            deltas.Add(delta);
            ApplyCoordinatorConsolidation(delta);
        }

        _deltasIn += deltas.Count;
        _deltasOut += deltas.Count; // pure read-side relay: "consumed" and "reflected" are the same count here
        _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _rebuilding = false;
        _dirty = true;

        if (_searchIndex is not null)
        {
            ReflectDeltasInSearchIndex(deltas);
        }
        return Task.CompletedTask;
    }

    private void ApplyCoordinatorConsolidation(TableDelta delta)
    {
        var key = _executor!.CanonicalRowKey(delta.Row);
        if (_coordinatorSnapshot.TryGetValue(key, out var existing))
        {
            long newWeight = existing.Weight + delta.Weight;
            if (newWeight <= 0) _coordinatorSnapshot.Remove(key);
            else _coordinatorSnapshot[key] = (existing.Row, newWeight);
        }
        else if (delta.Weight > 0)
        {
            _coordinatorSnapshot[key] = (delta.Row, delta.Weight);
        }
    }

    private async Task ApplyAndPublishAsync(IReadOnlyList<TableDelta> deltas)
    {
        _dirty = true;
        _deltasOut += deltas.Count;

        if (_searchIndex is not null)
        {
            ReflectDeltasInSearchIndex(deltas);
        }

        var dtos = deltas.Select(d => new TableDeltaDto { Row = new Dictionary<string, object?>(d.Row), Weight = d.Weight }).ToList();
        var stream = this.GetStreamProvider(StreamConstants.ProviderName)
            .GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, _def!.Name));
        await stream.OnNextAsync(dtos);
    }

    /// <summary>Keeps the search index in sync with the consolidated Z-set as deltas land: for each row
    /// touched by this batch, look its canonical key up in the (already-updated, O(1) live) consolidated
    /// snapshot — present with weight &gt; 0 means Add/update, absent means the row's weight returned to 0
    /// (Remove). Only rows actually touched by this batch are re-checked, not the whole table.</summary>
    private void ReflectDeltasInSearchIndex(IReadOnlyList<TableDelta> deltas)
    {
        IReadOnlyDictionary<string, (EventRecord Row, long Weight)> snapshot = _coordinatorMode ? _coordinatorSnapshot : _executor!.Snapshot();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var delta in deltas)
        {
            var key = _executor.CanonicalRowKey(delta.Row);
            if (!seen.Add(key)) continue; // a batch can touch the same row's key more than once

            if (snapshot.TryGetValue(key, out var current))
            {
                _searchIndex!.Add(key, current.Row);
            }
            else
            {
                _searchIndex!.Remove(key);
            }
        }
    }

    private async Task FlushAsync()
    {
        if (_executor is null)
        {
            _dirty = false;
            return;
        }

        var snapshot = _coordinatorMode ? _coordinatorSnapshot : _executor.Snapshot();
        state.State.Snapshot = snapshot.ToDictionary(
            kv => kv.Key,
            kv => new TableRowDto { Row = new Dictionary<string, object?>(kv.Value.Row), Weight = kv.Value.Weight });
        state.State.Seq++;
        _dirty = false;

        await state.WriteStateAsync();
    }

    private async Task OnFlushTickAsync()
    {
        if (_dirty)
        {
            await FlushAsync();
        }
    }

    private static FieldKind MapFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown field type"),
    };
}
