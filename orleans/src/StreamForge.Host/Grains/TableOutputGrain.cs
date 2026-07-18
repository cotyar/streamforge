using Orleans;
using Orleans.Streams;
using StreamForge.Abstractions;

namespace StreamForge.Host.Grains;

/// <summary>
/// Plan 003 M2's terminal-publisher choice: key = table name, one activation per Parallelism &gt;= 2
/// table. Every partition of the plan's terminal stage (see StreamForge.Engine.Dataflow.TableDataflowPlan.
/// TerminalEdge) calls <see cref="PublishAsync"/> as its own epoch advances; this grain does no buffering
/// or reordering of its own — DBSP/Z-set consolidation is commutative (see TableDataflowPlan's class doc),
/// so republishing each incoming batch to the delta stream in receipt order (Orleans already serializes
/// calls to one grain activation turn-by-turn, so there's no interleaving to worry about even with up to
/// Parallelism concurrent senders) is correct. It republishes onto the EXACT SAME
/// (StreamConstants.TableDeltaNamespace, tableName) stream a Parallelism==1 TableGrain publishes to
/// directly — so StreamBridgeService (SignalR tableDelta), TableHistoryGrain, and any downstream
/// table-over-table TableGrain subscriber keep working completely unchanged regardless of which mode
/// produced the table. (Alternative considered and rejected: gather into "partition 0" of the terminal
/// stage — that would make partition 0 an implicit single point of coordination with no clean home for the
/// gather logic; a dedicated grain keeps the terminal-stage TableStageGrains uniform with every other
/// partition and gives the gather point its own identity or per-table lifecycle.)
///
/// PLAN 003 M4: PublishAsync additionally forwards (fromPartition, epoch) to the owning ITableGrain's
/// OnOutputBatchAsync — a second, direct-call delivery path, separate from (and always alongside) the
/// unchanged stream republish above. See ITableGrain.OnOutputBatchAsync's doc comment for why: the
/// coordinator needs (data, epoch) delivered together, atomically, on ONE path it fully controls the
/// buffering of, to make its frontier honest — riding the existing shared delta stream (whose payload type
/// and per-partition-immediate-republish behavior several OTHER consumers depend on unchanged) can't give
/// that guarantee.
/// </summary>
public sealed class TableOutputGrain : Grain, ITableOutputGrain
{
    private PipelineStatus _status = PipelineStatus.Stopped;

    public Task StartAsync(TableDefinition def)
    {
        _status = PipelineStatus.Running;
        this.DelayDeactivation(TimeSpan.FromDays(365));
        return Task.CompletedTask;
    }

    public Task StopAsync()
    {
        _status = PipelineStatus.Stopped;
        this.DelayDeactivation(TimeSpan.Zero);
        return Task.CompletedTask;
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _status = PipelineStatus.Stopped;
        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    public async Task PublishAsync(int fromPartition, long epoch, List<TableDeltaDto> deltas)
    {
        if (_status != PipelineStatus.Running) return;

        if (deltas.Count > 0)
        {
            var stream = this.GetStreamProvider(StreamConstants.ProviderName)
                .GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, this.GetPrimaryKeyString()));
            await stream.OnNextAsync(deltas);
        }

        // Plan 003 M4: always forwarded, even when deltas is empty — an empty-epoch marker, exactly like
        // every other hop in the graph (see this class's doc comment above).
        await GrainFactory.GetGrain<ITableGrain>(this.GetPrimaryKeyString()).OnOutputBatchAsync(fromPartition, epoch, deltas);
    }
}
