using Orleans;
using Orleans.Streams;
using StreamForge.Abstractions;
using StreamForge.Engine;
using StreamForge.Engine.Dataflow;

namespace StreamForge.Host.Grains;

/// <summary>
/// Plan 003 M2: key = "{tableName}:{inputName}". One activation per (table, real external input) — a
/// stream source name or an upstream table name the table's SQL reads from directly (compileResult.
/// StreamInputs / TableInputs — the exact same input set a Parallelism==1 TableGrain subscribes to).
/// Subscribes to that input's EXISTING stream identity (StreamConstants.SourcesNamespace or
/// TableDeltaNamespace) — unchanged from today, so the source/upstream-table side of the system doesn't
/// know or care whether a subscriber is a plain TableGrain or this partitioned path.
///
/// EPOCHING: buffers admitted (weight-normalized) deltas locally and flushes on a 250ms timer tick OR
/// once 1000 events have buffered, whichever first (plan 003's stated epoch-advance rule) — each flush is
/// one epoch, stamped from a local monotonically-increasing counter.
///
/// ROUTING (this is the "ingest per input" stage's actual home — see StreamForge.Engine.Dataflow.
/// TableDataflowPlan's class doc: an Ingest-kind engine stage always has partition count 1 and exists
/// mainly as a graph-shape concept; this grain performs its real job directly rather than adding an extra
/// grain hop): for each edge TableDataflowPlan.EdgesForExternalInput(inputName) returns, either (a) the
/// edge already targets a Join/SemiAnti stage directly with Broadcast mode (a scalar-subquery/semi-anti
/// join's residual inputs) — route on that edge as-is; or (b) the edge targets an Ingest-kind stage
/// (Mode always Local) — the REAL routing decision is that Ingest stage's own one outbound edge
/// (TableDataflowPlan.OutEdgeOf), always HashPartition — route on THAT edge instead. Every downstream
/// partition gets a call each flush, even an empty one, so a quiet ingest doesn't stall a downstream
/// FrontierTracker (batched per (edge, epoch) per the plan's protocol, not per-row).
/// </summary>
public sealed class TableIngestGrain : Grain, ITableIngestGrain
{
    private string _tableName = "";
    private string _inputName = "";
    private bool _isTableInput;
    private TableDataflowPlan? _dataflow;
    private List<TableEdgeDescriptor> _externalEdges = [];

    private PipelineStatus _status = PipelineStatus.Stopped;
    private StreamSubscriptionHandle<EventRecord>? _streamSub;
    private StreamSubscriptionHandle<List<TableDeltaDto>>? _tableSub;
    private IGrainTimer? _flushTimer;

    private readonly List<TableDelta> _pending = [];
    private long _epochCounter;

    private const int FlushEventThreshold = 1000;
    private static readonly TimeSpan FlushInterval = TimeSpan.FromMilliseconds(250);

    public async Task StartAsync(TableDefinition def, string inputName)
    {
        await StopAsync();

        _tableName = def.Name;
        _inputName = inputName;
        var (compile, dataflow) = await TableDataflowFactory.BuildAsync(GrainFactory, def);
        _dataflow = dataflow;
        _isTableInput = compile.TableInputs.Contains(inputName);
        // Plan 003 M3: an "In" edge whose Ingest-kind target stage's OWN outbound edge (the real routing
        // decision — see this grain's class doc) was marked arrangeable is served by a shared
        // ArrangementGrain instead (see TableGrain.StartCoordinatorAsync's arrangement-attach loop) — exclude
        // it here so this ingest grain doesn't ALSO route to it (that would double-deliver, and the target
        // TableStageGrain's upstream set no longer even reserves fromPartition==0 for this edge — see
        // TableStageGrain.StartAsync's producerCount fix). An input with EVERY edge arrangeable ends up with
        // an empty _externalEdges list; TableGrain doesn't even start this grain for such an input (see its
        // _deployedInputs filter) — the check below is defense-in-depth.
        _externalEdges = dataflow.EdgesForExternalInput(inputName)
            .Where(e => e.Mode == TableEdgeMode.Broadcast || dataflow.OutEdgeOf(e.ToStageId).ArrangeKeyFields is null)
            .ToList();

        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        if (_isTableInput)
        {
            var stream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, inputName));
            _tableSub = await stream.SubscribeAsync((batch, _) => OnTableDeltaBatchAsync(batch));
        }
        else
        {
            var stream = streamProvider.GetStream<EventRecord>(StreamId.Create(StreamConstants.SourcesNamespace, inputName));
            _streamSub = await stream.SubscribeAsync((evt, _) => OnStreamEventAsync(evt));
        }

        _flushTimer = this.RegisterGrainTimer(OnFlushTickAsync, FlushInterval, FlushInterval);
        _status = PipelineStatus.Running;
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    public async Task StopAsync()
    {
        _status = PipelineStatus.Stopped;
        _flushTimer?.Dispose();
        _flushTimer = null;

        if (_streamSub is not null) { try { await _streamSub.UnsubscribeAsync(); } catch { /* best-effort */ } _streamSub = null; }
        if (_tableSub is not null) { try { await _tableSub.UnsubscribeAsync(); } catch { /* best-effort */ } _tableSub = null; }

        _pending.Clear();
        _dataflow = null;
        this.DelayDeactivation(TimeSpan.Zero);
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _status = PipelineStatus.Stopped;
        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    private Task OnStreamEventAsync(EventRecord evt)
    {
        if (_status != PipelineStatus.Running) return Task.CompletedTask;
        _pending.Add(new TableDelta(evt, 1)); // a stream event always asserts — mirrors TableGrain.OnStreamEventAsync's weight=1 convention
        return _pending.Count >= FlushEventThreshold ? FlushAsync() : Task.CompletedTask;
    }

    private Task OnTableDeltaBatchAsync(List<TableDeltaDto> batch)
    {
        if (_status != PipelineStatus.Running) return Task.CompletedTask;
        foreach (var d in batch) _pending.Add(new TableDelta(new EventRecord(d.Row), d.Weight));
        return _pending.Count >= FlushEventThreshold ? FlushAsync() : Task.CompletedTask;
    }

    private Task OnFlushTickAsync() => FlushAsync();

    private async Task FlushAsync()
    {
        if (_status != PipelineStatus.Running || _dataflow is null) return;

        var epoch = _epochCounter++;
        var batch = _pending.Count == 0 ? [] : new List<TableDelta>(_pending);
        _pending.Clear();

        foreach (var edge in _externalEdges)
        {
            if (edge.Mode == TableEdgeMode.Broadcast)
            {
                int fromPartition = Math.Max(0, edge.ExternalInputNames.ToList().IndexOf(_inputName));
                await RouteAsync(edge, fromPartition, epoch, batch);
            }
            else
            {
                var realEdge = _dataflow.OutEdgeOf(edge.ToStageId); // Ingest-kind stage's own (always HashPartition) forward edge
                await RouteAsync(realEdge, 0, epoch, batch);
            }
        }
    }

    private async Task RouteAsync(TableEdgeDescriptor edge, int fromPartition, long epoch, List<TableDelta> batch)
    {
        int pcount = _dataflow!.PartitionCountOf(edge.ToStageId);
        var tasks = new List<Task>(pcount);

        if (edge.Mode == TableEdgeMode.Broadcast)
        {
            for (int p = 0; p < pcount; p++) tasks.Add(SendAsync(edge, fromPartition, p, epoch, batch));
        }
        else
        {
            var byPartition = batch.Count == 0
                ? new Dictionary<int, List<TableDelta>>()
                : batch.GroupBy(d => _dataflow.PartitionOf(edge.EdgeId, d.Row)).ToDictionary(g => g.Key, g => g.ToList());
            for (int p = 0; p < pcount; p++)
                tasks.Add(SendAsync(edge, fromPartition, p, epoch, byPartition.TryGetValue(p, out var l) ? l : []));
        }

        await Task.WhenAll(tasks);
    }

    private Task SendAsync(TableEdgeDescriptor edge, int fromPartition, int targetPartition, long epoch, List<TableDelta> deltas)
    {
        var dtos = deltas.Select(d => new TableDeltaDto { Row = new Dictionary<string, object?>(d.Row), Weight = d.Weight }).ToList();
        var target = GrainFactory.GetGrain<ITableStageGrain>($"{_tableName}:{edge.ToStageId}:{targetPartition}");
        return target.PushBatchAsync(edge.EdgeId.Value, fromPartition, epoch, _inputName, dtos);
    }
}
