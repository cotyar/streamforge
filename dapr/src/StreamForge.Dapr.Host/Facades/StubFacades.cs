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
/// awaiting a later wave. A non-empty <see cref="TableDefinition.ShardBy"/> is refused at START, NOT at
/// upsert — deliberately, so the field round-trips and the definition can be promoted back to an Orleans
/// instance without loss — the long "WHERE KEY SHARDING IS REFUSED ON THIS FLAVOR" note in
/// <c>Catalog/CatalogStore.cs</c> gives the reason. This is the one way it differs from the
/// Parallelism &gt; 1 refusal, which DOES happen at upsert. So no table on
/// this flavor can ever RUN sharded and every member here reports a disabled tier rather than an
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

/// <summary>Plan 020 D9: the CRDT document runtime is Orleans-only, permanently — not a W-something stub
/// awaiting a later wave, the same "permanently disabled, not merely unimplemented" shape
/// <see cref="DisabledTableShardFacade"/> gives key sharding. <see cref="ICrdtFacade.Enabled"/> being
/// false is the whole point: <c>CrdtEndpoints</c> reads it and answers 501 before ever calling
/// <see cref="MergeAsync"/>/<see cref="GetStatusAsync"/>, so both members below are defensive-only and
/// should never actually run.</summary>
public sealed class DisabledCrdtFacade : ICrdtFacade
{
    public bool Enabled => false;

    public Task<CrdtMergeResult?> MergeAsync(string sourceName, IReadOnlyList<byte[]> updates) =>
        Task.FromResult<CrdtMergeResult?>(null);

    public Task<CrdtDocStatus?> GetStatusAsync(string sourceName) =>
        Task.FromResult<CrdtDocStatus?>(null);
}
