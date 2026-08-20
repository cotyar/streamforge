using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using StreamForge.Abstractions;

namespace StreamForge.Host.Grains;

public sealed class TableShardRouterGrainState
{
    public bool Enabled { get; set; }
    public List<string> ShardBy { get; set; } = [];

    /// <summary>HiLo reservation high-water mark — see TableShardRouterGrain's sequence-number paragraph.
    /// Every sequence number below this has been reserved (though not necessarily issued); the next
    /// activation starts from here, so a crash can never reissue a number a shard has already applied.</summary>
    public long ReservedSeq { get; set; }
}

/// <summary>
/// Plan 011 wave D1 — THE ROUTER. Key = table name. Subscribes to the table's own
/// <c>(StreamConstants.TableDeltaNamespace, tableName)</c> delta stream, groups each batch by shard key,
/// stamps a monotonic per-table sequence number, and forwards each group to its
/// <see cref="TableShardGrain"/>.
///
/// It is a copy of <see cref="TableHistoryGrain"/>'s structure with the "apply" replaced by a fan-out, and
/// that is on purpose: the same stream, the same Reset/Resume/Disable lifecycle, the same reason for
/// staying activated (Orleans persistent-stream subscriptions survive deactivation at the pub-sub-store
/// level, but the LOCAL callback registration does not, so a stream consumer that wants to keep observing
/// must stay activated). This grain therefore does call <c>DelayDeactivation</c> — and holds no per-key
/// state whatsoever, which is what makes that acceptable: one router per sharded table, a fixed handful
/// of fields each. The per-key state, the part that grows, lives in the shards, which do not.
///
/// WORKS AT EVERY PARALLELISM, without knowing about any of it. At Parallelism == 1 the table's own
/// executor publishes to that stream; at Parallelism &gt;= 2 <see cref="ITableOutputGrain"/> republishes
/// every terminal-stage batch onto the identical stream in receipt order. A delta-stream consumer is
/// therefore blind to the execution topology, which is exactly why the shard tier needs no change to the
/// SQL path, the planner or the partitioned dataflow.
///
/// THE SEQUENCE NUMBER, and why it is not the epoch. Each forwarded batch carries a monotonically
/// increasing per-table number, and each shard records the highest it has applied
/// (<see cref="TableShardView.AppliedSeq"/>). Wave D1 uses it for observability; it exists now because it
/// is what a FENCED consistent whole-table scan will need, and retrofitting an ordering stamp onto a tier
/// that has already accumulated history means reprocessing that history. The dataflow's own
/// <c>Epoch</c>/<c>SnapshotFrontierEpoch</c> cannot serve: it is null for every Parallelism == 1 table
/// (there is no partitioned frontier), so an epoch-based fence would work for half the tables and
/// silently not for the other half. A router-assigned sequence works for both.
///
/// It is allocated HiLo: activation reserves a block of numbers and persists the new high-water mark
/// before issuing any of them, so a crash mid-block leaves a gap (harmless — the contract is monotonic,
/// not gapless) rather than reissuing a number some shard has already recorded as applied.
///
/// ORDERING AND CONSISTENCY. Within one batch the router awaits every shard call before returning, so
/// batch N+1 is never forwarded before N has landed everywhere — which is what makes a per-key read
/// strictly consistent (a shard sees an unbroken, ordered prefix of the deltas for its key). Calls to
/// DIFFERENT shards run concurrently, which is safe precisely because no ordering exists between distinct
/// keys in a Z-set stream.
///
/// THE FENCED SCAN (plan 011 D2) LIVES HERE, and that is the whole trick. Because this grain forwards one
/// batch at a time and AWAITS every shard's apply before returning, and because it is NON-REENTRANT, a
/// scan taken as a call on this grain cannot overlap a forward: no batch is in flight while it runs, and
/// none can start. Every shard therefore holds exactly the deltas of batches &lt;= the last forwarded
/// sequence, and nothing beyond — a genuine consistent cut, with no shard-side waiting, no retained
/// per-sequence versions, and nothing for an idle shard to hang on. See
/// <see cref="TableShardScanResult"/>'s doc for why the obvious alternative ("each shard waits until its
/// AppliedSeq reaches S") is wrong in both directions. The cost is real and is stated where users see it:
/// the shard tier's ingest is paused for as long as the scan takes, which is why it is opt-in.
///
/// REENTRANCY. The call graph is strictly one-way: RegistryGrain → router → shard/directory. Nothing
/// calls back into RegistryGrain, so no cycle is introduced and no <c>[MayInterleave]</c> allowlist entry
/// is needed. That is also why <see cref="TableShardConfig"/> is carried on every batch rather than
/// looked up — a shard asking the registry for its own table's definition would create exactly that cycle.
/// </summary>
public sealed class TableShardRouterGrain(
    [PersistentState("tableShardRouter", StreamConstants.StorageName)] IPersistentState<TableShardRouterGrainState> state,
    ILogger<TableShardRouterGrain> logger)
    : Grain, ITableShardRouterGrain
{
    /// <summary>Sequence numbers reserved per persisted write. Large enough that the write is rare on a
    /// hot table, small enough that a restart's gap is meaningless.</summary>
    private const long SeqReservationBlock = 10_000;

    private StreamSubscriptionHandle<List<TableDeltaDto>>? _sub;
    private TableShardConfig? _config;
    private long _seq = -1;
    private long _seqCeiling;
    private long _routedBatches;
    private long _routedDeltas;

    public async Task ResetAsync(TableDefinition def)
    {
        // RegistryGrain calls this on EVERY table create/update, sharded or not, so that turning ShardBy
        // on or off is one code path. An unsharded table that has never had a tier therefore lands here
        // with nothing to do — and must leave nothing behind: without this guard every plain table in the
        // catalog would acquire an empty router state file it never uses.
        if (def.ShardBy.Count == 0 && !state.RecordExists && !state.State.Enabled)
        {
            _config = null;
            this.DelayDeactivation(TimeSpan.Zero);
            return;
        }

        await UnsubscribeAsync();

        // A ShardBy or SQL change re-keys the whole tier: existing shards were filed under a rule that no
        // longer holds, and leaving them would strand rows under keys nothing will ever look up again.
        await PurgeShardsAsync();

        state.State.Enabled = def.ShardBy.Count > 0;
        state.State.ShardBy = [.. def.ShardBy];
        _routedBatches = 0;
        _routedDeltas = 0;
        await state.WriteStateAsync();

        if (!state.State.Enabled)
        {
            _config = null;
            this.DelayDeactivation(TimeSpan.Zero);
            return;
        }

        _config = BuildConfig(def);
        await ReserveSeqBlockAsync();
        await SubscribeAsync(this.GetPrimaryKeyString());
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    public async Task ResumeAsync(TableDefinition def)
    {
        if (def.ShardBy.Count == 0)
        {
            return;
        }

        state.State.Enabled = true;
        state.State.ShardBy = [.. def.ShardBy];
        _config = BuildConfig(def);
        await ReserveSeqBlockAsync();

        await UnsubscribeAsync();
        await SubscribeAsync(this.GetPrimaryKeyString());
        this.DelayDeactivation(TimeSpan.FromDays(365));
    }

    public async Task DisableAsync()
    {
        await UnsubscribeAsync();
        await PurgeShardsAsync();
        _config = null;
        state.State = new TableShardRouterGrainState();
        await state.WriteStateAsync();
        ShardResidency.Forget(this.GetPrimaryKeyString());
        this.DelayDeactivation(TimeSpan.Zero);
    }

    public async Task<TableShardingInfo> GetInfoAsync()
    {
        var table = this.GetPrimaryKeyString();
        // Directory count only — no shard is contacted, so polling this can never wake an idle key.
        var count = state.State.Enabled
            ? await GrainFactory.GetGrain<ITableShardDirectoryGrain>(table).GetCountAsync()
            : 0;

        return new TableShardingInfo
        {
            Enabled = state.State.Enabled,
            ShardBy = [.. state.State.ShardBy],
            ShardCount = count,
            ResidentShardCount = ShardResidency.ResidentCount(table),
            Activations = ShardResidency.Activations(table),
            Deactivations = ShardResidency.Deactivations(table),
            // -1 until something has actually been routed: the HiLo reservation moves _seq forward at
            // activation, so reporting it raw would claim progress no shard has seen.
            RouterSeq = _routedBatches == 0 ? -1 : _seq,
            RoutedBatches = _routedBatches,
            RoutedDeltas = _routedDeltas,
            RouterActive = _sub is not null,
        };
    }

    /// <summary>Plan 011 D2 — THE FENCED SCAN. See <see cref="TableShardScanResult"/> for the argument;
    /// the mechanism is that this method BEING a call on this non-reentrant grain is itself the fence. No
    /// batch can be forwarded while it runs (Orleans queues the stream delivery behind it, on both stream
    /// transports — memory streams and the push bus both deliver as real Orleans messages), and the last
    /// one that was forwarded completed before returning, because OnDeltaBatchAsync awaits every shard.
    /// So every shard read below holds exactly batches &lt;= <c>_seq</c> and nothing after.
    ///
    /// NOTHING WAITS, and that is the point about idle shards. A shard whose key has seen no traffic since
    /// sequence 12 answers immediately, reporting AppliedSeq 12 against a fence of 9000 — correct, because
    /// its state at 12 IS its state at 9000, and a design that made it wait for 9000 would hang forever on
    /// the most ordinary configuration there is.
    ///
    /// The honest cost, stated where it is paid: the shard tier's ingest stalls for the duration. Shards
    /// are read in chunks and a cold one is a disk read, so a fenced scan over a large page is not cheap.
    /// The table's OWN snapshot, its delta stream and every other consumer of it are unaffected — only
    /// this router's subscription backs up, and it drains afterwards.</summary>
    public async Task<TableShardScanResult> FencedScanAsync(int limit, int offset)
    {
        var table = this.GetPrimaryKeyString();
        var result = new TableShardScanResult
        {
            // -1 until something has actually been routed, for the same reason GetInfoAsync reports it
            // that way: the HiLo reservation moves _seq forward at activation, and reporting it raw would
            // name a fence no shard has ever seen.
            FenceSeq = _routedBatches == 0 ? -1 : _seq,
            RoutedDeltasAtFence = _routedDeltas,
        };

        if (!state.State.Enabled)
        {
            return result;
        }

        var directory = GrainFactory.GetGrain<ITableShardDirectoryGrain>(table);
        result.ShardCount = await directory.GetCountAsync();
        var keys = await directory.GetKeysAsync(limit, offset);

        foreach (var chunk in keys.Chunk(32))
        {
            var stats = await Task.WhenAll(chunk.Select(k =>
                GrainFactory.GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(table, k)).GetStatsAsync()));
            result.Shards.AddRange(stats);
        }

        return result;
    }

    private static TableShardConfig BuildConfig(TableDefinition def) => new()
    {
        TableName = def.Name,
        ShardBy = [.. def.ShardBy],
        // Best-effort textual extraction, used ONLY to group versions of the same logical row inside a
        // shard whose owner was already decided by the explicit ShardBy columns — see TableShardKeys'
        // class doc on why that distinction is what makes best-effort acceptable here.
        IdentityColumns = TableGroupKeyExtractor.ExtractIdentityColumns(def.Sql),
        HistoryEnabled = def.HistoryEnabled,
        HistoryMode = def.HistoryMode,
        HistoryLimit = def.HistoryLimit,
        HistoryByField = def.HistoryByField,
        HistoryWindowMs = def.HistoryWindowMs,
        Persistence = def.Persistence,
        FlushMs = def.FlushMs,
    };

    private async Task ReserveSeqBlockAsync()
    {
        if (_seq < state.State.ReservedSeq)
        {
            _seq = state.State.ReservedSeq;
        }
        _seqCeiling = state.State.ReservedSeq + SeqReservationBlock;
        state.State.ReservedSeq = _seqCeiling;
        await state.WriteStateAsync();
    }

    private async Task SubscribeAsync(string tableName)
    {
        var streamProvider = this.GetStreamProvider(StreamConstants.ProviderName);
        var stream = streamProvider.GetStream<List<TableDeltaDto>>(StreamId.Create(StreamConstants.TableDeltaNamespace, tableName));
        _sub = await stream.SubscribeAsync((batch, _) => OnDeltaBatchAsync(batch));
    }

    private async Task UnsubscribeAsync()
    {
        if (_sub is null) return;
        try { await _sub.UnsubscribeAsync(); } catch { /* best-effort */ }
        _sub = null;
    }

    private async Task OnDeltaBatchAsync(List<TableDeltaDto> batch)
    {
        var config = _config;
        if (config is null || !state.State.Enabled || batch.Count == 0) return;

        // Group by shard key on the router's own turn. This is the only per-key structure the router ever
        // builds and it lives exactly as long as one batch — the router never accumulates a key set, which
        // is what keeps it O(1) in the table's cardinality.
        var groups = new Dictionary<string, List<TableDeltaDto>>(StringComparer.Ordinal);
        foreach (var delta in batch)
        {
            var shardKey = TableShardKeys.EncodeShardKey(delta.Row, config.ShardBy);
            if (!groups.TryGetValue(shardKey, out var list))
            {
                list = [];
                groups[shardKey] = list;
            }
            list.Add(delta);
        }

        var seq = await NextSeqAsync();
        _routedBatches++;
        _routedDeltas += batch.Count;

        var table = this.GetPrimaryKeyString();
        var keys = groups.Keys.ToList();
        var applies = groups.Select(g =>
            GrainFactory.GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(table, g.Key)).ApplyAsync(config, seq, g.Value)).ToList();

        var directory = GrainFactory.GetGrain<ITableShardDirectoryGrain>(table);
        var register = directory.RegisterAsync(keys);

        bool[] live;
        try
        {
            live = await Task.WhenAll(applies);
            await register;
        }
        catch (Exception ex)
        {
            // Best-effort, and said plainly: a failed forward loses those deltas for the shard tier only.
            // The table's own snapshot and delta stream are unaffected — the shard tier is a derived
            // second materialization, exactly like the row-history tier, and neither is in the write path.
            logger.LogError(ex, "Shard router '{Table}': forwarding a batch of {Count} delta(s) failed", table, batch.Count);
            return;
        }

        // Shards that reported themselves empty have already cleared their own state; drop their keys so
        // the directory does not become a graveyard of keys with nothing behind them.
        var emptied = keys.Where((_, i) => !live[i]).ToList();
        if (emptied.Count > 0)
        {
            try { await directory.RemoveAsync(emptied); } catch { /* best-effort */ }
        }
    }

    private async Task<long> NextSeqAsync()
    {
        if (_seq + 1 >= _seqCeiling)
        {
            await ReserveSeqBlockAsync();
        }
        return ++_seq;
    }

    /// <summary>Deletes every shard's persisted state and empties the directory. Necessarily ACTIVATES each
    /// shard once — clearing a grain's persisted state portably means asking the grain to do it. That is
    /// the right trade for an explicit, one-off delete/re-key, and precisely the wrong one for a read,
    /// which is why no read path anywhere goes through the directory to the shards.</summary>
    private async Task PurgeShardsAsync()
    {
        var table = this.GetPrimaryKeyString();
        List<string> keys;
        try
        {
            keys = await GrainFactory.GetGrain<ITableShardDirectoryGrain>(table).DrainAllAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Shard router '{Table}': could not drain the shard directory", table);
            return;
        }

        foreach (var chunk in keys.Chunk(64))
        {
            try
            {
                await Task.WhenAll(chunk.Select(k =>
                    GrainFactory.GetGrain<ITableShardGrain>(TableShardKeys.GrainKey(table, k)).PurgeAsync()));
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Shard router '{Table}': purging a chunk of shards failed", table);
            }
        }
    }
}
