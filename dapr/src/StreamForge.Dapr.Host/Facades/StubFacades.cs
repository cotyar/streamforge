using StreamForge.Abstractions;

namespace StreamForge.Dapr.Host.Facades;

/// <summary>W7 replaces this: TableActor doesn't exist yet, so there are no rows/deltas to read.
/// Zeroed/empty shapes — same rationale W4 gave for the (now-replaced, see
/// <see cref="DaprPipelineReadFacade"/>) pipeline stub: a freshly-created/never-started entity should
/// render a coherent empty state, not an error.</summary>
public sealed class StubTableReadFacade : ITableReadFacade
{
    public Task<List<TableRowDto>> GetRowsAsync(string tableName, int limit, int offset) =>
        Task.FromResult(new List<TableRowDto>());

    public Task<int> GetRowCountAsync(string tableName) => Task.FromResult(0);

    public Task<long> GetSeqAsync(string tableName) => Task.FromResult(0L);

    /// <summary>Always null on the Dapr flavor — partitioned execution (and its frontier) is
    /// Orleans-only (decision D-F), independent of whether TableActor has landed yet.</summary>
    public Task<long?> GetSnapshotFrontierEpochAsync(string tableName) => Task.FromResult((long?)null);

    public Task<TableMetrics> GetMetricsAsync(string tableName) =>
        Task.FromResult(new TableMetrics { TableId = tableName, Status = PipelineStatus.Stopped });

    public Task<List<TableRowDto>> SearchAsync(string tableName, string query, int limit) =>
        Task.FromResult(new List<TableRowDto>());
}

/// <summary>W7 replaces this: TableHistoryActor doesn't exist yet.</summary>
public sealed class StubTableHistoryFacade : ITableHistoryFacade
{
    public Task<TableHistoryQueryResult> GetHistoryAsync(string tableName, string key, int limit) =>
        Task.FromResult(new TableHistoryQueryResult { KeyFound = false });

    public Task<TableHistoryStats> GetStatsAsync(string tableName) =>
        Task.FromResult(new TableHistoryStats { Enabled = false });
}

/// <summary>Plan 011 D1: key sharding is Orleans-only, and permanently so on this flavor — not a stub
/// awaiting a later wave. <see cref="Catalog.CatalogStore.CreateTableAsync"/> refuses a non-empty
/// <see cref="TableDefinition.ShardBy"/> at upsert (the same way it refuses Parallelism &gt; 1), so no
/// table on this flavor can ever BE sharded and every member here reports a disabled tier rather than an
/// error. The endpoints reach the same conclusion before they get here — they short-circuit on
/// <c>def.ShardBy.Count == 0</c> — so this exists to satisfy DI and to stay honest if that ever changes.</summary>
public sealed class DisabledTableShardFacade : ITableShardFacade
{
    public Task<TableShardView> GetShardAsync(string tableName, string shardKey, int historyLimitPerKey) =>
        Task.FromResult(new TableShardView { Found = false, ShardKey = shardKey });

    public Task<TableShardingInfo> GetInfoAsync(string tableName) =>
        Task.FromResult(new TableShardingInfo { Enabled = false });

    public Task<List<string>> GetKeysAsync(string tableName, int limit, int offset) =>
        Task.FromResult(new List<string>());

    public Task<List<TableShardStats>> ScanAsync(string tableName, int limit, int offset) =>
        Task.FromResult(new List<TableShardStats>());

    /// <summary>Plan 011 D2. A fence over nothing is still a well-defined answer: no shards, and a
    /// FenceSeq of -1 saying nothing has ever been routed — which is exactly true on this flavor.</summary>
    public Task<TableShardScanResult> ScanFencedAsync(string tableName, int limit, int offset) =>
        Task.FromResult(new TableShardScanResult());
}

/// <summary>Partitioned execution (and therefore shared arrangements) is Orleans-only — decision D-F.
/// Always empty on the Dapr flavor, permanently (not a W-something stub — see plan's parity matrix).</summary>
public sealed class EmptyArrangementMetaFacade : IArrangementMetaFacade
{
    public Task<IReadOnlyList<ArrangementMetaInfo>> GetArrangementsAsync() =>
        Task.FromResult<IReadOnlyList<ArrangementMetaInfo>>([]);
}
