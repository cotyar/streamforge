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

/// <summary>Partitioned execution (and therefore shared arrangements) is Orleans-only — decision D-F.
/// Always empty on the Dapr flavor, permanently (not a W-something stub — see plan's parity matrix).</summary>
public sealed class EmptyArrangementMetaFacade : IArrangementMetaFacade
{
    public Task<IReadOnlyList<ArrangementMetaInfo>> GetArrangementsAsync() =>
        Task.FromResult<IReadOnlyList<ArrangementMetaInfo>>([]);
}
