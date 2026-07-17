using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
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

    public async Task StartAsync(TableDefinition def)
    {
        await StopAsync();

        _def = def;

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

    public Task<TableMetrics> GetMetricsAsync() => Task.FromResult(new TableMetrics
    {
        TableId = _def?.Id ?? this.GetPrimaryKeyString(),
        Status = _status,
        RowCount = state.State.Snapshot.Count,
        DeltasIn = _deltasIn,
        DeltasOut = _deltasOut,
        LastUpdateMs = _lastUpdateMs,
        Rebuilding = _rebuilding,
    });

    public Task<long> GetSeqAsync() => Task.FromResult(state.State.Seq);

    public Task<List<TableRowDto>> SearchAsync(string query, int limit)
    {
        if (_searchIndex is null || string.IsNullOrWhiteSpace(query))
        {
            return Task.FromResult(new List<TableRowDto>());
        }

        var snapshot = _executor?.Snapshot();
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
        var snapshot = _executor!.Snapshot();
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

        var snapshot = _executor.Snapshot();
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
