namespace StreamsForge.Abstractions;

// ============================================================================
// Plan 011 wave D1: the sharded-table grain topology. Three grain kinds, additive — every existing grain
// interface is unchanged, and none of these exist at all for a table whose ShardBy is empty.
//
//   TableShardRouterGrain    key = table name.  Subscribes to the table's delta stream, groups each batch
//                            by shard key, stamps a monotonic per-table sequence, forwards. One per
//                            sharded table. Structurally this is TableHistoryGrain's subscribe/apply
//                            lifecycle with the "apply" replaced by a fan-out.
//   TableShardDirectoryGrain key = table name.  The live shard-key set, for enumeration and for deleting
//                            a table's shards. Resident and O(distinct keys) — the design's one
//                            acknowledged unbounded structure.
//   TableShardGrain          key = "{table}|{token}" (TableShardKeys.GrainKey). One key's rows + that
//                            key's history. THE ONE THAT MUST NOT PIN ITSELF ALIVE.
//
// EVERY ONE OF THOSE KEYS IS THE TABLE'S NAME, which is why plan 011 D2 REFUSES to rename a sharded table
// (RegistryGrain.UpdateTableAsync): a rename would leave every existing shard filed under a key nothing
// would ever look up again — silently, with nothing failing, which is the worst shape a data loss can
// take. Keying the tier on the immutable id instead is the better fix and is written up where the refusal
// is raised.
//
// THE INVARIANT THE WHOLE WAVE RESTS ON: TableShardGrain never calls DelayDeactivation. Every other grain
// in the table path calls DelayDeactivation(TimeSpan.FromDays(365)) — TableGrain, TableHistoryGrain,
// TableStageGrain, ArrangementGrain, TableOutputGrain — so nothing in it has ever been swapped out. An
// idle shard deactivating, with its state on disk until the next lookup, IS the memory win; a shard that
// stays activated delivers nothing at all while appearing to work.
// ============================================================================

/// <summary>Plan 011 D1. Key = table name. Lifecycle mirrors <see cref="ITableHistoryGrain"/>'s
/// (Reset on create/config change, Resume on silo restart, Disable on delete) because it is the same
/// shape of thing: a delta-stream consumer that must re-register its local callback after any transition
/// that could have dropped it.</summary>
public interface ITableShardRouterGrain : IGrainWithStringKey
{
    /// <summary>(Re)configures the shard tier from the table's current definition and subscribes to its
    /// delta stream — or, when <c>def.ShardBy</c> is empty, tears the tier down. ALWAYS discards the
    /// existing shards first: a change to ShardBy or to the SQL means every existing shard was keyed by a
    /// rule that no longer holds, and keeping them would leave rows filed under keys nothing will ever
    /// look up again. Call on table create and on any ShardBy/SQL/history-config change.</summary>
    Task ResetAsync(TableDefinition def);

    /// <summary>Re-subscribes WITHOUT discarding accumulated shards — the silo-restart path
    /// (RegistryGrain.EnsureInitializedAsync), so a sharded table's per-key state survives a restart the
    /// same way the persisted shard files already do. No-op when <c>def.ShardBy</c> is empty.</summary>
    Task ResumeAsync(TableDefinition def);

    /// <summary>Unsubscribes and PURGES every shard's persisted state plus the directory — call on table
    /// delete. Purging necessarily activates each shard once (that is how a grain's state is cleared
    /// portably), which is fine for an explicit one-off deletion and would not be for a read.</summary>
    Task DisableAsync();

    /// <summary>Table-level metrics. Wakes no shard — see <see cref="TableShardingInfo"/>.</summary>
    Task<TableShardingInfo> GetInfoAsync();

    /// <summary>Plan 011 D2: a whole-table scan taken as a genuine CONSISTENT CUT at the router's current
    /// sequence. It lives on the router rather than on the facade precisely because the router is the
    /// fence — see <see cref="TableShardScanResult"/> and this grain's own class doc. ACTIVATES every
    /// shard in the page, and PAUSES the shard tier's ingest until it returns.</summary>
    Task<TableShardScanResult> FencedScanAsync(int limit, int offset);
}

/// <summary>Plan 011 D1. Key = table name. The live shard-key set.
///
/// HONEST LIMIT, stated here rather than discovered later: this grain holds one string per distinct shard
/// key and is itself resident — it is O(distinct keys) and nothing evicts from it except a key whose shard
/// genuinely emptied. It holds keys, not rows or version trails, so it is kilobytes where the shards are
/// megabytes; but a table with tens of millions of distinct instruments would feel it, and the honest
/// answer for that shape is that enumeration would have to become an external index rather than a grain.</summary>
public interface ITableShardDirectoryGrain : IGrainWithStringKey
{
    /// <summary>Adds shard keys observed on a routed batch. Idempotent — the router does not track which
    /// keys it has already registered (tracking them would rebuild, in the router, exactly the resident
    /// per-key structure this design moved out).</summary>
    Task RegisterAsync(List<string> shardKeys);

    /// <summary>Drops shard keys whose shard reported itself empty (all rows retracted and no history
    /// left — see <see cref="ITableShardGrain.ApplyAsync"/>'s return).</summary>
    Task RemoveAsync(List<string> shardKeys);

    /// <summary>A page of live shard keys, ordinal-ordered so paging is stable.</summary>
    Task<List<string>> GetKeysAsync(int limit, int offset);

    Task<int> GetCountAsync();

    /// <summary>Returns every key and clears the directory — the delete path.</summary>
    Task<List<string>> DrainAllAsync();
}

/// <summary>Plan 011 D1. Key = <c>TableShardKeys.GrainKey(table, shardKey)</c>. One shard key's rows and
/// version history.
///
/// MUST NOT CALL DelayDeactivation. See this file's header.</summary>
public interface ITableShardGrain : IGrainWithStringKey
{
    /// <summary>Applies one routed batch of deltas for this shard key. <paramref name="seq"/> is the
    /// router's monotonic per-table sequence number; the shard records the highest it has applied.
    /// Returns false when the shard is now EMPTY (no rows and no retained history), in which case it has
    /// already cleared its own persisted state and the router should drop the key from the directory.</summary>
    Task<bool> ApplyAsync(TableShardConfig config, long seq, List<TableDeltaDto> deltas);

    /// <summary>Everything for this key. <paramref name="historyLimitPerKey"/> &lt;= 0 means "all retained
    /// versions".</summary>
    Task<TableShardView> GetViewAsync(int historyLimitPerKey);

    Task<TableShardStats> GetStatsAsync();

    /// <summary>Clears this shard's state and deactivates — the table-delete path.</summary>
    Task PurgeAsync();
}
