using StreamForge.Engine.Dataflow;
using StreamForge.Engine.Runtime;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 003 M2 acceptance (5b/5c): "an in-process harness driving stage executors WITHOUT Orleans — the
/// engine seam makes this possible". Simulates the grain-level protocol (batched-per-(edge,epoch) delivery,
/// hash/broadcast/local/gather routing via <see cref="TableDataflowPlan"/>'s own public seam) entirely
/// in-process: no grains, no Orleans streams, no threads. Each top-level <see cref="Admit"/> call is one
/// external admission (mirrors TableExecutor.OnStreamEvent/OnTableDelta — one epoch per call) that gets
/// fully drained to a fixed point (every cross-edge message produced along the way processed exactly once)
/// before returning, with the resulting terminal deltas folded into a DBSP-style consolidated snapshot —
/// the same consolidation TableExecutorImpl.ApplyConsolidation performs, so its output is directly
/// comparable to <see cref="TableExecutor.Snapshot"/>'s (same canonical-row-key scheme, same op classes
/// under the hood — this IS the M2 equivalence oracle's mechanism).
///
/// The `lifo` knob on <see cref="Admit"/> processes one admission's internal work queue LIFO instead of
/// FIFO — a same-epoch intra-admission reordering knob, on top of the caller's own freedom to call
/// <see cref="Admit"/> for a sequence of external events in any order it likes. Both are "arrival order"
/// in the M2 determinism sense; DBSP's per-delta commutativity (Z-set summation) means neither should
/// change the final consolidated snapshot.
/// </summary>
internal sealed class PartitionedTableHarness
{
    private readonly TableDataflowPlan _plan;
    private readonly Dictionary<(int Stage, int Partition), ITableStageExecutor> _executors = [];
    private readonly Dictionary<string, (EventRecord Row, long Weight)> _consolidated = [];
    private long _epochCounter;

    public PartitionedTableHarness(TableDataflowPlan plan) => _plan = plan;

    public IReadOnlyDictionary<string, (EventRecord Row, long Weight)> Snapshot() => _consolidated;

    /// <summary>Admits one external event/table-delta (already weight-normalized by the caller — weight=1
    /// for a genuine stream event, the delta's own signed weight for an upstream table delta, exactly
    /// mirroring TableGrain.OnStreamEventAsync vs OnTableDeltaBatchAsync today) and drains it to a fixed
    /// point before returning.</summary>
    public void Admit(string originName, EventRecord row, long weight, bool lifo = false)
    {
        var epoch = new Epoch(_epochCounter++);
        var initialDelta = new TableDelta(row, weight);
        var seed = new List<(EdgeId Edge, int Partition, IReadOnlyList<TableDelta> Deltas)>();

        foreach (var edge in _plan.EdgesForExternalInput(originName))
        {
            if (edge.Mode == TableEdgeMode.Broadcast)
            {
                for (int p = 0; p < _plan.PartitionCountOf(edge.ToStageId); p++)
                    seed.Add((edge.EdgeId, p, [initialDelta]));
            }
            else
            {
                int p = edge.Mode == TableEdgeMode.HashPartition ? _plan.PartitionOf(edge.EdgeId, row) : 0;
                seed.Add((edge.EdgeId, p, [initialDelta]));
            }
        }

        Drain(originName, seed, epoch, lifo);
    }

    private void Drain(string originName, List<(EdgeId Edge, int Partition, IReadOnlyList<TableDelta> Deltas)> seed, Epoch epoch, bool lifo)
    {
        var edgeById = _plan.Edges.ToDictionary(e => e.EdgeId);
        var work = new List<(TableEdgeDescriptor Edge, int Partition, string Origin, IReadOnlyList<TableDelta> Deltas)>();
        foreach (var (edgeId, partition, deltas) in seed)
        {
            if (deltas.Count == 0) continue;
            work.Add((edgeById[edgeId], partition, originName, deltas));
        }

        while (work.Count > 0)
        {
            int idx = lifo ? work.Count - 1 : 0;
            var (edge, partition, origin, deltas) = work[idx];
            work.RemoveAt(idx);

            if (edge.ToStageId == -1)
            {
                foreach (var d in deltas) ApplyConsolidation(d);
                continue;
            }

            var executor = ExecutorFor(edge.ToStageId, partition);
            var outputs = executor.OnBatch(edge.EdgeId, origin, epoch, deltas);
            foreach (var output in outputs)
            {
                var outEdge = output.OutEdge;
                if (outEdge.ToStageId == -1)
                {
                    work.Add((outEdge, 0, origin, output.Deltas));
                    continue;
                }
                switch (outEdge.Mode)
                {
                    case TableEdgeMode.Broadcast:
                        for (int p = 0; p < _plan.PartitionCountOf(outEdge.ToStageId); p++)
                            work.Add((outEdge, p, origin, output.Deltas));
                        break;
                    case TableEdgeMode.Local:
                        work.Add((outEdge, partition, origin, output.Deltas));
                        break;
                    default: // HashPartition: different deltas in the same output batch may route to different partitions
                        foreach (var byPartition in output.Deltas.GroupBy(d => _plan.PartitionOf(outEdge.EdgeId, d.Row)))
                            work.Add((outEdge, byPartition.Key, origin, byPartition.ToList()));
                        break;
                }
            }
        }
    }

    private ITableStageExecutor ExecutorFor(int stageId, int partition)
    {
        var key = (stageId, partition);
        if (!_executors.TryGetValue(key, out var executor))
        {
            executor = _plan.CreateStageExecutor(stageId, partition);
            _executors[key] = executor;
        }
        return executor;
    }

    private void ApplyConsolidation(TableDelta delta)
    {
        var key = JsonText.SerializeCanonicalRow(delta.Row);
        if (_consolidated.TryGetValue(key, out var existing))
        {
            long newWeight = existing.Weight + delta.Weight;
            if (newWeight <= 0) _consolidated.Remove(key);
            else _consolidated[key] = (existing.Row, newWeight);
        }
        else if (delta.Weight > 0)
        {
            _consolidated[key] = (delta.Row, delta.Weight);
        }
    }
}
