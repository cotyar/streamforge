using Orleans;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Engine;
using StreamsForge.Engine.Dataflow;

namespace StreamsForge.Host.Grains;

/// <summary>
/// Plan 003 M2: key = "{tableName}:{stageId}:{partition}". One activation per (table, non-Ingest dataflow
/// stage, partition) — see StreamsForge.Engine.Dataflow.TableDataflowPlan.Stages (Ingest-kind stages have no
/// TableStageGrain of their own; TableIngestGrain performs an Ingest stage's routing directly — see its
/// class doc). Holds one <see cref="ITableStageExecutor"/> (the engine's per-(stage,partition) op wrapper)
/// plus a <see cref="FrontierTracker"/> + <see cref="EpochBuffer"/> (the M0 primitives): PushBatchAsync
/// buffers an inbound (edge, epoch) batch and observes its upstream's frontier; on advance, every
/// newly-ready batch (EpochBuffer.OnFrontier's deterministic (Epoch, EdgeId, FromPartition) order) is fed
/// to the executor, whose emitted deltas are routed to every partition of the ONE downstream stage this
/// stage's outbound edge targets (or gathered to <see cref="ITableOutputGrain"/> when that edge is
/// terminal) — including an empty-delta marker to any downstream partition that got no real output this
/// round, so a quiet stage never stalls a downstream FrontierTracker.
/// </summary>
public sealed class TableStageGrain : Grain, ITableStageGrain
{
    private string _tableName = "";
    private int _partition;
    private TableDataflowPlan? _dataflow;
    private TableStageDescriptor? _stage;
    private TableEdgeDescriptor _outEdge = null!;
    private ITableStageExecutor? _executor;
    private FrontierTracker? _frontier;
    private EpochBuffer? _buffer;
    private readonly Dictionary<(EdgeId, int, Epoch), string> _originByBatch = [];

    private PipelineStatus _status = PipelineStatus.Stopped;
    private long _deltasIn;
    private long _deltasOut;
    private long _lastUpdateMs;

    public async Task StartAsync(TableDefinition def, int stageId, int partition)
    {
        await StopAsync();

        // Plan 021 D3 — the QUALIFIED table name, so every sibling grain key this activation composes
        // below (ITableOutputGrain, downstream ITableStageGrain) lands in the same environment TableGrain
        // started this dataflow in, without this grain having to hold or re-derive `def.Environment` itself.
        _tableName = EnvKeys.Qualify(def.Environment, def.Name);
        _partition = partition;
        var (_, dataflow) = await TableDataflowFactory.BuildAsync(GrainFactory, def);
        _dataflow = dataflow;
        _stage = dataflow.Stages.First(s => s.StageId == stageId);
        _outEdge = dataflow.OutEdgeOf(stageId);

        var upstreams = new List<UpstreamId>();
        foreach (var edge in _stage.InEdges)
        {
            if (edge.ArrangeKeyFields is not null)
            {
                // Plan 003 M3: an arrangeable edge is fed by a shared ArrangementGrain SET, co-partitioned
                // 1:1 with THIS stage (arrangement partition p pushes ONLY to stage partition p — see
                // ArrangementGrain's class doc and TableGrain.StartCoordinatorAsync's attach loop, which
                // attaches arrangement partition p to target grain key "{table}:{stageId}:{p}"). THIS
                // activation (fixed at `partition`) therefore hears from exactly ONE arrangement partition —
                // its own — never the other P-1 (those push to a DIFFERENT stage-partition activation
                // entirely). Registering all P identities here (as if this were a single-partition-producer
                // fan-in, like a private TableIngestGrain) would permanently starve the frontier: the P-1
                // identities that can structurally never be observed on THIS activation would hold its
                // combined Frontier at NegativeInfinity forever (see FrontierTracker's "silent upstream"
                // invariant) — the exact bug this comment replaces.
                upstreams.Add(new UpstreamId(edge.EdgeId, partition));
                continue;
            }
            int producerCount = edge.FromStageId == -1
                ? (edge.Mode == TableEdgeMode.Broadcast ? Math.Max(1, edge.ExternalInputNames.Count) : 1)
                : dataflow.PartitionCountOf(edge.FromStageId);
            for (int p = 0; p < producerCount; p++) upstreams.Add(new UpstreamId(edge.EdgeId, p));
        }
        _frontier = new FrontierTracker(upstreams);
        _buffer = new EpochBuffer();
        _executor = dataflow.CreateStageExecutor(stageId, partition);

        _status = PipelineStatus.Running;
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    public Task StopAsync()
    {
        _status = PipelineStatus.Stopped;
        _dataflow = null;
        _stage = null;
        _executor = null;
        _frontier = null;
        _buffer = null;
        _originByBatch.Clear();
        this.DelayDeactivation(TimeSpan.Zero);
        return Task.CompletedTask;
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _status = PipelineStatus.Stopped;
        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    public async Task PushBatchAsync(int edgeIdValue, int fromPartition, long epochValue, string originName, List<TableDeltaDto> deltaDtos)
    {
        if (_status != PipelineStatus.Running || _executor is null || _frontier is null || _buffer is null || _dataflow is null) return;

        var edgeId = new EdgeId(edgeIdValue);
        var epoch = new Epoch(epochValue);
        var deltas = deltaDtos.Select(d => new TableDelta(new EventRecord(d.Row), d.Weight)).ToList();

        _buffer.Add(new DeltaBatch(edgeId, fromPartition, epoch, deltas));
        if (deltas.Count > 0)
        {
            _deltasIn += deltas.Count;
            _lastUpdateMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (!string.IsNullOrEmpty(originName)) _originByBatch[(edgeId, fromPartition, epoch)] = originName;
        }

        var observation = _frontier.Observe(new UpstreamId(edgeId, fromPartition), epoch);
        if (!observation.Advanced) return;

        var ready = _buffer.OnFrontier(observation.Frontier);
        var outByPartition = new Dictionary<int, List<TableDelta>>();

        foreach (var batch in ready)
        {
            var key = (batch.EdgeId, batch.FromPartition, batch.Epoch);
            var origin = _originByBatch.TryGetValue(key, out var o) ? o : "";
            _originByBatch.Remove(key);
            if (batch.Deltas.Count == 0) continue;

            var outputs = _executor.OnBatch(batch.EdgeId, origin, batch.Epoch, batch.Deltas);
            foreach (var output in outputs)
            {
                foreach (var d in output.Deltas)
                {
                    int target = TargetPartitionOf(output.OutEdge, d);
                    if (!outByPartition.TryGetValue(target, out var list)) outByPartition[target] = list = [];
                    list.Add(d);
                }
            }
        }

        _deltasOut += outByPartition.Values.Sum(l => l.Count);
        await RouteDownstreamAsync(observation.Frontier, outByPartition);
    }

    private int TargetPartitionOf(TableEdgeDescriptor outEdge, TableDelta delta) => outEdge.Mode switch
    {
        TableEdgeMode.Local => _partition,
        TableEdgeMode.HashPartition => _dataflow!.PartitionOf(outEdge.EdgeId, delta.Row),
        TableEdgeMode.Gather => 0,
        _ => throw new InvalidOperationException($"A stage's own outbound edge cannot be Broadcast (edge {outEdge.EdgeId})."),
    };

    private async Task RouteDownstreamAsync(Epoch frontier, Dictionary<int, List<TableDelta>> outByPartition)
    {
        if (_outEdge.ToStageId == -1)
        {
            // Plan 003 M4: always call PublishAsync, even with zero rows — TableOutputGrain now forwards
            // (partition, frontier) to the coordinator's own FrontierTracker (see ITableGrain.
            // OnOutputBatchAsync's doc comment), which needs a marker on every advance of THIS partition's
            // frontier to avoid stalling, exactly like every other downstream hop in the graph already
            // requires (see the non-terminal branch below, which never skips an empty target).
            var all = outByPartition.Values.SelectMany(x => x).ToList();
            var dtos = all.Select(d => new TableDeltaDto { Row = new Dictionary<string, object?>(d.Row), Weight = d.Weight }).ToList();
            await GrainFactory.GetGrain<ITableOutputGrain>(_tableName).PublishAsync(_partition, frontier.Value, dtos);
            return;
        }

        int targetPartitionCount = _dataflow!.PartitionCountOf(_outEdge.ToStageId);
        var tasks = new List<Task>(targetPartitionCount);
        for (int p = 0; p < targetPartitionCount; p++)
        {
            var deltas = outByPartition.TryGetValue(p, out var list) ? list : [];
            var dtos = deltas.Select(d => new TableDeltaDto { Row = new Dictionary<string, object?>(d.Row), Weight = d.Weight }).ToList();
            var target = GrainFactory.GetGrain<ITableStageGrain>($"{_tableName}:{_outEdge.ToStageId}:{p}");
            tasks.Add(target.PushBatchAsync(_outEdge.EdgeId.Value, _partition, frontier.Value, "", dtos));
        }
        await Task.WhenAll(tasks);
    }

    public Task<TablePartitionMetrics> GetMetricsAsync() => Task.FromResult(new TablePartitionMetrics
    {
        StageId = _stage?.StageId ?? -1,
        Partition = _partition,
        DeltasIn = _deltasIn,
        DeltasOut = _deltasOut,
        FrontierEpoch = _frontier?.Frontier.Value ?? -1,
        LastUpdateMs = _lastUpdateMs,
        // Plan 003 M4: real operator name for the M5 dataflow panel — see TableStageKindLabel's doc comment.
        Kind = _stage is not null ? TableStageKindLabel.Of(_stage.Kind) : "",
    });
}
