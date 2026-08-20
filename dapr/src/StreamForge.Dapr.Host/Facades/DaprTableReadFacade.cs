using Dapr.Actors;
using Dapr.Actors.Client;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.Dapr.Host.Actors;

namespace StreamForge.Dapr.Host.Facades;

/// <summary>Plan 005 (Dapr sibling runtime) W7-A: real <see cref="ITableReadFacade"/> — replaces
/// <see cref="StubTableReadFacade"/> (registered by <c>Actors/TableRuntimeSetup.cs</c>'s
/// <c>AddServices</c>, which runs AFTER <c>Facades.DaprFacadesExtensions.AddDaprFacades</c>'s stub
/// registration, so this last registration wins per ASP.NET Core DI's "last one wins" resolution rule for
/// a singleton service type). Resolves a fresh <see cref="ITableActor"/> proxy per call (same per-call
/// resolution style as <see cref="DaprPipelineReadFacade"/> — a table actor's id varies per call, so
/// there is no single proxy instance to cache in a field). Backs
/// <c>GET /api/tables/{id}/rows|metrics|search</c>. A table id that was never started (or doesn't exist)
/// still answers cleanly — <see cref="TableActor"/>'s own <c>OnActivateAsync</c> finds no persisted state,
/// so every read method below returns the same "nothing has run yet" empty/zeroed shape
/// <see cref="StubTableReadFacade"/> used to return unconditionally.
///
/// <para><see cref="GetSnapshotFrontierEpochAsync"/> always returns null, independent of whether
/// <see cref="TableActor"/> exists — partitioned execution (and its frontier) is Orleans-only (decision
/// D-F), so this is a permanent answer, not a W7 stub (see <see cref="StubTableReadFacade"/>'s own doc
/// comment for the same permanent-null rationale).</para>
/// </summary>
public sealed class DaprTableReadFacade : ITableReadFacade
{
    public Task<List<TableRowDto>> GetRowsAsync(string tableName, int limit, int offset) =>
        TableActorProxy(tableName).GetRowsAsync(limit, offset);

    public Task<int> GetRowCountAsync(string tableName) => TableActorProxy(tableName).GetRowCountAsync();

    public Task<long> GetSeqAsync(string tableName) => TableActorProxy(tableName).GetSeqAsync();

    public Task<long?> GetSnapshotFrontierEpochAsync(string tableName) => Task.FromResult((long?)null);

    public Task<TableMetrics> GetMetricsAsync(string tableName) => TableActorProxy(tableName).GetMetricsAsync();

    public Task<List<TableRowDto>> SearchAsync(string tableName, string query, int limit) =>
        TableActorProxy(tableName).SearchAsync(query, limit);

    /// <summary>Plan 021: <paramref name="tableName"/> arrives bare (the shared REST endpoints resolve it
    /// from the catalog before calling here) — this facade backs a REST request, so it is one of the few
    /// places in this track allowed to read <see cref="EnvironmentAmbient.Current"/> directly (the wave
    /// brief's facade rule).</summary>
    private static ITableActor TableActorProxy(string tableName) =>
        ActorProxy.Create<ITableActor>(new ActorId(EnvKeys.Qualify(EnvironmentAmbient.Current, tableName)), nameof(TableActor), ActorProxyDefaults.Options);
}
