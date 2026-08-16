using Orleans;
using Orleans.Streams;
using StreamForge.Abstractions;

namespace StreamForge.Host.Grains;

/// <summary>
/// Plan 003 M2's terminal-publisher choice: key = table name, one activation per Parallelism &gt;= 2
/// table. Every partition of the plan's terminal stage (see StreamForge.Engine.Dataflow.TableDataflowPlan.
/// TerminalEdge) calls <see cref="PublishAsync"/> as its own epoch advances. It republishes onto the EXACT
/// SAME (StreamConstants.TableDeltaNamespace, tableName) stream a Parallelism==1 TableGrain publishes to
/// directly — so StreamBridgeService (SignalR tableDelta), TableHistoryGrain, and any downstream
/// table-over-table TableGrain subscriber keep working completely unchanged regardless of which mode
/// produced the table. (Alternative considered and rejected: gather into "partition 0" of the terminal
/// stage — that would make partition 0 an implicit single point of coordination with no clean home for the
/// gather logic; a dedicated grain keeps the terminal-stage TableStageGrains uniform with every other
/// partition and gives the gather point its own identity or per-table lifecycle.)
///
/// PLAN 003 M4: PublishAsync forwards (fromPartition, epoch, deltas) to the owning ITableGrain's
/// OnOutputBatchAsync, which buffers per (partition, epoch) with the same FrontierTracker+EpochBuffer
/// primitives every other dataflow hop uses and returns THIS epoch's consolidated, ready-to-publish batch —
/// empty when another terminal partition still holds the frontier back, or when this epoch's net effect is
/// empty. See ITableGrain.OnOutputBatchAsync's own doc comment for why.
///
/// WISHLIST #15/#14, PART 2 (superseding the pre-existing "republish immediately, per partition arrival"
/// design): this grain used to call <c>stream.OnNextAsync</c> itself, per partition, BEFORE any frontier
/// consolidation — the claim that Z-set consolidation being commutative made that correct addressed only
/// the FINAL converged state, not what a downstream subscriber could OBSERVE in between: a row one
/// partition retracted and a DIFFERENT partition (re-)asserted within the SAME logical epoch (e.g. a row
/// whose hash-partitioned key changed) reached the wire as two separate, immediately-published messages —
/// the coordinator-mode analogue of wishlist #15's classic-mode NULL-flap bug. The publish now happens HERE,
/// once per fully-advanced frontier round, with whatever <see cref="ITableGrain.OnOutputBatchAsync"/>
/// returns (already netted by canonical row key — see TableGrain.ConsolidateCoordinatorEpochOutput) —
/// exactly the "one upstream batch applied as one epoch with its output consolidated" property the classic
/// path already has. <see cref="ITableGrain.OnOutputBatchAsync"/> itself stays fully synchronous (no
/// `await`) to preserve its MayInterleave safety argument; this method, which already awaits freely and
/// carries no such constraint, is where the actual I/O happens.
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

        // Plan 003 M4: always forwarded, even when deltas is empty — an empty-epoch marker, exactly like
        // every other hop in the graph (see this class's doc comment above). Wishlist #15/#14 PART 2: the
        // return value is this epoch's consolidated, ready-to-publish batch — empty unless THIS call is the
        // one that advanced the frontier past every terminal partition for this epoch.
        var toPublish = await GrainFactory.GetGrain<ITableGrain>(this.GetPrimaryKeyString()).OnOutputBatchAsync(fromPartition, epoch, deltas);

        if (toPublish.Count > 0)
        {
            var stream = this.GetStreamProvider(StreamConstants.ProviderName)
                .GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, this.GetPrimaryKeyString()));
            await stream.OnNextAsync(toPublish);
        }
    }
}
