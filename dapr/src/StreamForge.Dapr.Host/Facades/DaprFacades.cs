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
        // Plan 011 D1: key sharding is Orleans-only and refused at upsert here — see
        // DisabledTableShardFacade and CatalogStore.ValidateShardBy.
        services.AddSingleton<ITableShardFacade, DisabledTableShardFacade>();
        // Plan 015 W1: the access-policy singleton. Registered here (not in a runtime-setup class) because
        // it is a plain singleton-actor adapter with no runtime to set up, exactly like the user store.
        services.AddSingleton<IAccessPolicyFacade, DaprAccessPolicyFacade>();
        return services;
    }
}

internal sealed class DaprCatalogFacade : ICatalogFacade
{
    private readonly IRegistryActor _actor =
        ActorProxy.Create<IRegistryActor>(new ActorId(StreamConstants.RegistryKey), nameof(RegistryActor), ActorProxyDefaults.Options);

    public Task<List<SourceDefinition>> GetSourcesAsync() => _actor.GetSourcesAsync();

    public Task<SourceDefinition?> GetSourceAsync(string name) => _actor.GetSourceAsync(name);

    /// <summary>Wishlist #8's run-on-demand, the Dapr mirror of RegistryGrain.RunSourceAsync. Goes
    /// straight to the generator actor rather than through the registry actor: the batch has to be
    /// published from the activation that owns the source's pub/sub topic, and unlike Orleans — where
    /// IRegistryGrain inherits this facade and already has a GrainFactory — there is nothing to gain by
    /// routing an extra actor hop through the registry first.</summary>
    public async Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request)
    {
        if (await _actor.GetSourceAsync(name) is null)
        {
            return new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound };
        }

        var generator = ActorProxy.Create<IGeneratorActor>(new ActorId(name), nameof(GeneratorActor), ActorProxyDefaults.Options);
        return await generator.RunAsync(request);
    }

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
/// connector kind, resolves a per-source <see cref="IConnectorActor"/> proxy — same per-call
/// resolution style as <see cref="DaprPipelineReadFacade"/> (a connector actor's id varies per source, so
/// there is no single proxy instance to cache in a field). Registered by
/// <see cref="Actors.ConnectorRuntimeSetup.AddServices"/>, not here — see that method's doc comment for
/// why.
///
/// <para><b>Plan 008 W4c fix:</b> this used to gate on <c>kind != "generator"</c> (a private
/// <c>IsGeneratorKind</c> one-liner, independent of but mirroring <see cref="SourceKindDispatch"/>'s own
/// binary classification at the time) — which meant an <see cref="SourceKinds.Ingest"/> source, being
/// "not generator", fell through to the connector branch and got a pointless <see cref="ConnectorActor"/>
/// ACTIVATED (and then immediately asked for a status it was never going to have, since nothing ever
/// starts an ingest-kind source's connector actor). Now that <see cref="SourceKindDispatch.Classify"/>
/// is a real three-way (plan 008 W4c), this checks for <see cref="SourceKindDispatch.ActorKind.Connector"/>
/// specifically — Generator and Ingest both correctly return null (no connector status exists for either)
/// without ever resolving an actor proxy.</para></summary>
internal sealed class DaprConnectorStatusFacade(ICatalogFacade catalog) : IConnectorStatusFacade
{
    public async Task<ConnectorRuntimeStatus?> GetStatusAsync(string sourceName)
    {
        var def = await catalog.GetSourceAsync(sourceName);
        if (def is null || SourceKindDispatch.Classify(def.Kind) != SourceKindDispatch.ActorKind.Connector)
        {
            return null;
        }

        var actor = ActorProxy.Create<IConnectorActor>(new ActorId(sourceName), nameof(ConnectorActor), ActorProxyDefaults.Options);
        return await actor.GetStatusAsync();
    }
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


/// <summary>Plan 015 W1: <see cref="IAccessPolicyFacade"/> over the "access" singleton actor — the Dapr
/// counterpart of the Orleans <c>IAccessPolicyGrain</c> adapter. Caches one proxy in a field, like
/// <see cref="DaprUserStoreFacade"/> and <see cref="DaprCatalogFacade"/> do and unlike the per-call
/// resolution the keyed facades need: the id never varies, there is exactly one access document.
///
/// <para>The multi-argument members are packed into the request records on
/// <see cref="IAccessPolicyActor"/> (a Dapr actor method takes at most one parameter); everything else is a
/// straight forward. No <see cref="ActorResult{T}"/> unwrapping, because nothing on this actor fails with an
/// error string — a refused mutation comes back as null or false.</para>
///
/// <para><b><see cref="GetVersionAsync"/> is the hot one.</b> Every replica's permission resolver calls it
/// every <c>Auth:PolicyCacheSeconds</c> (default 10) and only refetches the document when the number moves.
/// On this flavour each call is a sidecar round trip — which is the entire reason plan 015 refused to look
/// the policy up per request (015 D:"Permissions resolve server-side per request"). Anything added to this
/// path is paid for by every replica, forever.</para></summary>
internal sealed class DaprAccessPolicyFacade : IAccessPolicyFacade
{
    private readonly IAccessPolicyActor _actor =
        ActorProxy.Create<IAccessPolicyActor>(new ActorId(StreamConstants.AccessKey), nameof(AccessPolicyActor), ActorProxyDefaults.Options);

    public Task<AccessPolicyDocument> GetPolicyAsync() => _actor.GetPolicyAsync();

    public Task<long> GetVersionAsync() => _actor.GetVersionAsync();

    public Task<RoleDefinition?> UpsertRoleAsync(RoleDefinition role, string actor) =>
        _actor.UpsertRoleAsync(new UpsertRoleActorRequest(role, actor));

    public Task<bool> DeleteRoleAsync(string name) => _actor.DeleteRoleAsync(name);

    public Task<GroupDefinition?> UpsertGroupAsync(GroupDefinition group, string actor) =>
        _actor.UpsertGroupAsync(new UpsertGroupActorRequest(group, actor));

    public Task<bool> DeleteGroupAsync(string name) => _actor.DeleteGroupAsync(name);

    public Task<UserAccessEntry?> UpsertUserAccessAsync(UserAccessEntry entry, string actor) =>
        _actor.UpsertUserAccessAsync(new UpsertUserAccessActorRequest(entry, actor));

    public Task<bool> DeleteUserAccessAsync(string username) => _actor.DeleteUserAccessAsync(username);

    public Task<ApprovalTemplate?> UpsertApprovalTemplateAsync(ApprovalTemplate template, string actor) =>
        _actor.UpsertApprovalTemplateAsync(new UpsertApprovalTemplateActorRequest(template, actor));

    public Task<bool> DeleteApprovalTemplateAsync(string name) => _actor.DeleteApprovalTemplateAsync(name);
}
