using Dapr.Actors;
using Dapr.Actors.Client;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Actors;

namespace StreamForge.Dapr.Host.Facades;

// ============================================================================
// Plan 005 (Dapr sibling runtime) W4: Dapr-side implementations of the runtime-neutral facade
// interfaces (StreamForge.Abstractions/Facades.cs) that shared/StreamForge.Api's endpoints depend on —
// the Dapr counterpart of orleans/src/StreamForge.Host/Facades/OrleansFacades.cs.
//
//   - ICatalogFacade / IUserStoreFacade: resolve an ActorProxy for the "catalog"/"users" singleton actor
//     once (per adapter instance) and forward every call, packing multi-argument members into the
//     actor's request records and unwrapping ActorResult<T> failures into a thrown
//     InvalidOperationException (see ActorResult<T>'s class doc for why this boundary uses an explicit
//     result type rather than relying on Dapr's own actor-exception marshaling).
//   - IPipelineReadFacade: real as of W6 (DaprPipelineReadFacade below) — forwards to the pipeline's
//     PipelineActor.
//   - ITableReadFacade / ITableHistoryFacade / IArrangementMetaFacade: stubbed in this wave (see
//     StubFacades.cs) — TableActor/TableHistoryActor don't exist until W7, and partitioned execution
//     (arrangements) is Orleans-only (decision D-F).
//
// Plan 006 (ingestion connectors) W3-B addendum: IConnectorStatusFacade's real implementation,
// DaprConnectorStatusFacade, is defined below (next to DaprPipelineReadFacade — same per-call proxy-
// resolution shape) but registered by Actors/ConnectorRuntimeSetup.cs's AddServices, not by
// AddDaprFacades() above — see that method's own doc comment for why.
// ============================================================================

public static class DaprFacadesExtensions
{
    public static IServiceCollection AddDaprFacades(this IServiceCollection services)
    {
        services.AddSingleton<ICatalogFacade, DaprCatalogFacade>();
        services.AddSingleton<IUserStoreFacade, DaprUserStoreFacade>();
        services.AddSingleton<IPipelineReadFacade, DaprPipelineReadFacade>();
        services.AddSingleton<ITableReadFacade, StubTableReadFacade>();
        services.AddSingleton<ITableHistoryFacade, StubTableHistoryFacade>();
        services.AddSingleton<IArrangementMetaFacade, EmptyArrangementMetaFacade>();
        return services;
    }
}

internal sealed class DaprCatalogFacade : ICatalogFacade
{
    private readonly IRegistryActor _actor =
        ActorProxy.Create<IRegistryActor>(new ActorId(StreamConstants.RegistryKey), nameof(RegistryActor), ActorProxyDefaults.Options);

    public Task<List<SourceDefinition>> GetSourcesAsync() => _actor.GetSourcesAsync();

    public Task<SourceDefinition?> GetSourceAsync(string name) => _actor.GetSourceAsync(name);

    public Task UpsertSourceAsync(SourceDefinition def) => _actor.UpsertSourceAsync(def);

    public Task<bool> DeleteSourceAsync(string name) => _actor.DeleteSourceAsync(name);

    public Task<List<PipelineDefinition>> GetPipelinesAsync() => _actor.GetPipelinesAsync();

    public Task<PipelineDefinition?> GetPipelineAsync(string id) => _actor.GetPipelineAsync(id);

    public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => _actor.CreatePipelineAsync(def);

    public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => _actor.UpdatePipelineAsync(def);

    public Task<bool> DeletePipelineAsync(string id) => _actor.DeletePipelineAsync(id);

    public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) =>
        _actor.SetPipelineStatusAsync(new SetStatusRequest(id, status));

    public Task<List<TableDefinition>> GetTablesAsync() => _actor.GetTablesAsync();

    public Task<TableDefinition?> GetTableAsync(string id) => _actor.GetTableAsync(id);

    public async Task<TableDefinition> CreateTableAsync(TableDefinition def)
    {
        var result = await _actor.CreateTableAsync(def);
        return result.Ok ? result.Value! : throw new InvalidOperationException(result.Error);
    }

    public async Task<TableDefinition?> UpdateTableAsync(TableDefinition def)
    {
        var result = await _actor.UpdateTableAsync(def);
        return result.Ok ? result.Value : throw new InvalidOperationException(result.Error);
    }

    public async Task<bool> DeleteTableAsync(string id)
    {
        var result = await _actor.DeleteTableAsync(id);
        return result.Ok ? result.Value : throw new InvalidOperationException(result.Error);
    }

    public async Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status)
    {
        var result = await _actor.SetTableStatusAsync(new SetStatusRequest(id, status));
        return result.Ok ? result.Value : throw new InvalidOperationException(result.Error);
    }

    public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields) =>
        _actor.EnsureFieldNumbersAsync(new EnsureFieldNumbersRequest(entityKey, fields));
}

/// <summary>Plan 005 W6: real <see cref="IPipelineReadFacade"/> — resolves a fresh
/// <see cref="IPipelineActor"/> proxy per call (same per-call-resolution style as
/// <see cref="Lifecycle.DaprLifecycleOrchestrator"/>'s own actor proxies; unlike the singleton
/// catalog/user-store actors, a pipeline actor's id varies per call, so there is no single proxy instance
/// to cache in a field). Backs <c>GET /api/pipelines/{id}/results|metrics</c>. A pipeline id that was
/// never started (or doesn't exist) still answers cleanly — <see cref="PipelineActor"/>'s own
/// <c>OnActivateAsync</c> finds no persisted state, so <c>GetRecentResultsAsync</c>/<c>GetMetricsAsync</c>
/// return the same "nothing has run yet" empty/zeroed shape <see cref="StubTableReadFacade"/> still
/// returns for tables.</summary>
internal sealed class DaprPipelineReadFacade : IPipelineReadFacade
{
    public Task<List<ResultEnvelope>> GetRecentResultsAsync(string pipelineId, int limit) =>
        PipelineActorProxy(pipelineId).GetRecentResultsAsync(limit);

    public Task<PipelineMetrics> GetMetricsAsync(string pipelineId) =>
        PipelineActorProxy(pipelineId).GetMetricsAsync();

    private static IPipelineActor PipelineActorProxy(string pipelineId) =>
        ActorProxy.Create<IPipelineActor>(new ActorId(pipelineId), nameof(PipelineActor), ActorProxyDefaults.Options);
}

/// <summary>Plan 006 (ingestion connectors) W3-B: real <see cref="IConnectorStatusFacade"/> — resolves the
/// source via <see cref="ICatalogFacade"/> first (an actor-proxy call into the singleton
/// <c>RegistryActor</c>, exactly like <see cref="DaprCatalogFacade"/> itself), and only then, for a
/// non-generator kind, resolves a per-source <see cref="IConnectorActor"/> proxy — same per-call
/// resolution style as <see cref="DaprPipelineReadFacade"/> (a connector actor's id varies per source, so
/// there is no single proxy instance to cache in a field). Registered by
/// <see cref="Actors.ConnectorRuntimeSetup.AddServices"/>, not here — see that method's doc comment for
/// why.</summary>
internal sealed class DaprConnectorStatusFacade(ICatalogFacade catalog) : IConnectorStatusFacade
{
    public async Task<ConnectorRuntimeStatus?> GetStatusAsync(string sourceName)
    {
        var def = await catalog.GetSourceAsync(sourceName);
        if (def is null || IsGeneratorKind(def.Kind))
        {
            return null;
        }

        var actor = ActorProxy.Create<IConnectorActor>(new ActorId(sourceName), nameof(ConnectorActor), ActorProxyDefaults.Options);
        return await actor.GetStatusAsync();
    }

    /// <summary>Same null/empty/"generator" classification as
    /// <see cref="Lifecycle.DaprLifecycleOrchestrator"/>'s own <c>SourceKindDispatch.Classify</c> — kept
    /// as an independent one-liner here rather than shared, since the two call sites live in different
    /// wave-owned files (Facades/ vs Lifecycle/) and the rule is a single, stable, one-line string
    /// comparison unlikely to drift.</summary>
    private static bool IsGeneratorKind(string? kind) => string.IsNullOrEmpty(kind) || kind == SourceKinds.Generator;
}

internal sealed class DaprUserStoreFacade : IUserStoreFacade
{
    private readonly IUserStoreActor _actor =
        ActorProxy.Create<IUserStoreActor>(new ActorId(StreamConstants.UsersKey), nameof(UserStoreActor), ActorProxyDefaults.Options);

    public Task<UserRecord?> ValidateCredentialsAsync(string username, string password) =>
        _actor.ValidateCredentialsAsync(new ValidateCredentialsRequest(username, password));

    public Task<List<UserRecord>> GetUsersAsync() => _actor.GetUsersAsync();

    public Task<bool> CreateUserAsync(string username, string displayName, string role, string password) =>
        _actor.CreateUserAsync(new CreateUserActorRequest(username, displayName, role, password));

    public Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password) =>
        _actor.UpdateUserAsync(new UpdateUserActorRequest(username, displayName, role, password));

    public Task<bool> DeleteUserAsync(string username) => _actor.DeleteUserAsync(username);
}
