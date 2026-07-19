using Orleans;
using StreamForge.Abstractions;
using StreamForge.Engine.Dataflow;
using StreamForge.Host.Grains;

namespace StreamForge.Host.Facades;

// ============================================================================
// Plan 005 (Dapr sibling runtime) W3: Orleans-side implementations of the runtime-neutral facade
// interfaces (StreamForge.Abstractions/Facades.cs) that shared/StreamForge.Api's endpoints depend on.
//
//   - ICatalogFacade / IUserStoreFacade: IRegistryGrain/IUserStoreGrain already inherit these
//     interfaces (see GrainInterfaces.cs), so a real grain reference IS-A facade with zero adapter
//     code — registered as singletons resolving the grain proxy once.
//   - IPipelineReadFacade / ITableReadFacade / ITableHistoryFacade: keyed read surfaces: the grain's
//     implicit key becomes an explicit first parameter, so these need tiny per-call adapter classes
//     that resolve IClusterClient.GetGrain<T>(key) on every call. Deliberately dumb — all
//     id/name-resolution logic (e.g. mapping a table's REST {id} to its grain-key Name) already lives
//     in the shared endpoints (TablesEndpoints/PipelinesEndpoints), which call these adapters with the
//     already-resolved key.
//   - IArrangementMetaFacade: backs GET /api/meta/arrangements. Partitioned execution (and shared
//     arrangements) is Orleans-only (decision D-F), so this is the one facade whose entire body is
//     Orleans-specific (TableDataflowFactory + IArrangementGrain) — moved here verbatim from the old
//     MetaEndpoints.cs's /arrangements handler.
// ============================================================================

public static class OrleansFacadesExtensions
{
    public static IServiceCollection AddOrleansFacades(this IServiceCollection services)
    {
        services.AddSingleton<ICatalogFacade>(sp =>
            sp.GetRequiredService<IClusterClient>().GetGrain<IRegistryGrain>(StreamConstants.RegistryKey));
        services.AddSingleton<IUserStoreFacade>(sp =>
            sp.GetRequiredService<IClusterClient>().GetGrain<IUserStoreGrain>(StreamConstants.UsersKey));
        services.AddSingleton<IPipelineReadFacade, OrleansPipelineReadFacade>();
        services.AddSingleton<ITableReadFacade, OrleansTableReadFacade>();
        services.AddSingleton<ITableHistoryFacade, OrleansTableHistoryFacade>();
        services.AddSingleton<IArrangementMetaFacade, OrleansArrangementMetaFacade>();
        services.AddSingleton<IConnectorStatusFacade, OrleansConnectorStatusFacade>();
        return services;
    }
}

internal sealed class OrleansPipelineReadFacade(IClusterClient client) : IPipelineReadFacade
{
    public Task<List<ResultEnvelope>> GetRecentResultsAsync(string pipelineId, int limit) =>
        client.GetGrain<IPipelineGrain>(pipelineId).GetRecentResultsAsync(limit);

    public Task<PipelineMetrics> GetMetricsAsync(string pipelineId) =>
        client.GetGrain<IPipelineGrain>(pipelineId).GetMetricsAsync();
}

internal sealed class OrleansTableReadFacade(IClusterClient client) : ITableReadFacade
{
    public Task<List<TableRowDto>> GetRowsAsync(string tableName, int limit, int offset) =>
        client.GetGrain<ITableGrain>(tableName).GetRowsAsync(limit, offset);

    public Task<int> GetRowCountAsync(string tableName) =>
        client.GetGrain<ITableGrain>(tableName).GetRowCountAsync();

    public Task<long> GetSeqAsync(string tableName) =>
        client.GetGrain<ITableGrain>(tableName).GetSeqAsync();

    public Task<long?> GetSnapshotFrontierEpochAsync(string tableName) =>
        client.GetGrain<ITableGrain>(tableName).GetSnapshotFrontierEpochAsync();

    public Task<TableMetrics> GetMetricsAsync(string tableName) =>
        client.GetGrain<ITableGrain>(tableName).GetMetricsAsync();

    public Task<List<TableRowDto>> SearchAsync(string tableName, string query, int limit) =>
        client.GetGrain<ITableGrain>(tableName).SearchAsync(query, limit);
}

internal sealed class OrleansTableHistoryFacade(IClusterClient client) : ITableHistoryFacade
{
    public Task<TableHistoryQueryResult> GetHistoryAsync(string tableName, string key, int limit) =>
        client.GetGrain<ITableHistoryGrain>(tableName).GetHistoryAsync(key, limit);

    public Task<TableHistoryStats> GetStatsAsync(string tableName) =>
        client.GetGrain<ITableHistoryGrain>(tableName).GetStatsAsync();
}

/// <summary>Plan 006, D-C: connector runtime status. Generator-kind sources (Kind unset/"generator") and
/// unknown source names both return null — mirroring RegistryGrain's own Kind-dispatch rule (see
/// RegistryGrain.IsGeneratorKind) so this facade never spins up an IConnectorGrain activation for a
/// source that was never a connector in the first place.</summary>
internal sealed class OrleansConnectorStatusFacade(IClusterClient client) : IConnectorStatusFacade
{
    public async Task<ConnectorRuntimeStatus?> GetStatusAsync(string sourceName)
    {
        var registry = client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var def = await registry.GetSourceAsync(sourceName);
        if (def is null || string.IsNullOrEmpty(def.Kind) || def.Kind == SourceKinds.Generator)
        {
            return null;
        }
        return await client.GetGrain<IConnectorGrain>(sourceName).GetStatusAsync();
    }
}

internal sealed class OrleansArrangementMetaFacade(IClusterClient client) : IArrangementMetaFacade
{
    // Moved verbatim from the old StreamForge.Host.Api.MetaEndpoints's /arrangements handler — see
    // that endpoint's original doc comment (plan 003 M3) for the full "recompile-per-grain" rationale.
    public async Task<IReadOnlyList<ArrangementMetaInfo>> GetArrangementsAsync()
    {
        var registry = client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
        var tables = await registry.GetTablesAsync();
        var running = tables.Where(t => t.Status == PipelineStatus.Running && t.Parallelism > 1).ToList();

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<ArrangementMetaInfo>();

        foreach (var def in running)
        {
            TableDataflowPlan dataflow;
            try
            {
                (_, dataflow) = await TableDataflowFactory.BuildAsync(client, def);
            }
            catch
            {
                continue; // best-effort — a table that doesn't currently compile just isn't reported
            }

            foreach (var edge in dataflow.ArrangeableExternalEdges)
            {
                var inputName = dataflow.ExternalInputNameOf(edge);
                var keySpec = dataflow.KeySpecOf(edge);
                var hash = ArrangementKeySpec.HashOf(keySpec);
                int pcount = dataflow.PartitionCountOf(edge.ToStageId);
                var setKey = $"{inputName}|{hash}|{pcount}";
                if (!seen.Add(setKey))
                {
                    continue;
                }

                var infos = new List<ArrangementInfo>(pcount);
                for (int p = 0; p < pcount; p++)
                {
                    var key = $"{inputName}:{hash}:{p}";
                    infos.Add(await client.GetGrain<IArrangementGrain>(key).GetInfoAsync());
                }

                if (infos.All(i => i.ConsumerCount == 0))
                {
                    continue; // structurally arrangeable but nothing currently attached — not "live"
                }

                result.Add(new ArrangementMetaInfo
                {
                    InputName = inputName,
                    KeySpec = keySpec,
                    Partitions = pcount,
                    // Every attaching table attaches ALL P partitions (one consumer id per partition —
                    // see TableGrain.StartCoordinatorAsync's attach loop), so ConsumerCount is uniform
                    // across an arrangement set's partitions; Max is a defensive read (vs. Sum, which
                    // would misleadingly scale with P) in case of a transient in-flight attach/detach.
                    Consumers = infos.Count > 0 ? infos.Max(i => i.ConsumerCount) : 0,
                    TotalRows = infos.Sum(i => i.RowCount),
                });
            }
        }

        return result;
    }
}
