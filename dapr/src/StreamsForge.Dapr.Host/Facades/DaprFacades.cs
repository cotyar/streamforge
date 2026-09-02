using Dapr.Actors;
using Dapr.Actors.Client;
using StreamsForge.Abstractions;
using StreamsForge.Api.Facades;
using StreamsForge.AppCore.Environments;
using StreamsForge.Dapr.Host.Access;
using StreamsForge.Dapr.Host.Actors;

namespace StreamsForge.Dapr.Host.Facades;

// ============================================================================
// Plan 005 (Dapr sibling runtime) W4: Dapr-side implementations of the runtime-neutral facade
// interfaces (StreamsForge.Abstractions/Facades.cs) that shared/StreamsForge.Api's endpoints depend on —
// the Dapr counterpart of orleans/src/StreamsForge.Host/Facades/OrleansFacades.cs.
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
        // Plan 021: ICatalogFacade stops being a singleton that resolves ONE registry actor proxy at
        // container-build time (that resolved to the default environment forever, no matter what a later
        // request's X-StreamsForge-Environment header said). ICatalogFacadeFactory is the singleton now —
        // it holds no per-environment state — and THIS registration is what makes every request-scoped
        // consumer (an endpoint handler, or anything else that resolves ICatalogFacade fresh per request)
        // see the environment ITS OWN request selected: AddTransient means a fresh resolution reads
        // EnvironmentAmbient.Current every time, not once. See CatalogFacadeFactory.cs's class doc for the
        // other half — background services that must act on every environment, not the (empty outside a
        // request) ambient one, inject ICatalogFacadeFactory directly instead of ICatalogFacade.
        services.AddSingleton<ICatalogFacadeFactory, DaprCatalogFacadeFactory>();
        services.AddTransient<ICatalogFacade>(sp => sp.GetRequiredService<ICatalogFacadeFactory>().For(EnvironmentAmbient.Current));
        // Plan 021: the environment directory itself — never environment-qualified (StreamConstants.
        // EnvironmentsKey's own doc comment), so a single actor-proxy adapter, cached like the user-store/
        // access-policy singletons below, is enough.
        services.AddSingleton<IEnvironmentFacade, DaprEnvironmentFacade>();
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
        // Plan 015 W4-C: approvals (one singleton actor) and audit (one actor per day, plus the day
        // index). Same reasoning as the access policy above — plain actor adapters with no runtime to
        // set up. Both are resolved by StreamsForge.Api through GetService<T>(), so a host that does not
        // register them degrades to "no store" rather than failing to start; on this flavour they are
        // always here.
        services.AddSingleton<IApprovalFacade, DaprApprovalFacade>();
        services.AddSingleton<IAuditFacade, DaprAuditFacade>();
        // Plan 020 D9: the CRDT document runtime is Orleans-only, permanently — see
        // DisabledCrdtFacade's own doc comment (same shape as DisabledTableShardFacade above).
        // CRDT-as-a-plugin: DisabledCrdtFacade moved to shared/StreamsForge.Api/Facades so both flavors'
        // hosts reference the SAME default rather than each keeping its own copy (Orleans now needs it
        // too — see OrleansFacadesExtensions.AddOrleansFacades).
        services.AddSingleton<ICrdtFacade, DisabledCrdtFacade>();
        return services;
    }
}

/// <summary>Plan 021: takes its environment explicitly at construction — <see cref="ICatalogFacadeFactory"/>
/// is the only thing that constructs one now (see that interface's class doc for the two call shapes,
/// ambient-per-request and enumerate-every-environment, that both funnel through it). The actor id is
/// <c>EnvKeys.Qualify(environment, StreamConstants.RegistryKey)</c> — for <see cref="EnvKeys.Default"/>
/// (the empty string) that is byte-identical to the pre-021 literal <c>StreamConstants.RegistryKey</c>,
/// which is D2's whole point.</summary>
internal sealed class DaprCatalogFacade(string environment) : ICatalogFacade
{
    private readonly IRegistryActor _actor =
        ActorProxy.Create<IRegistryActor>(new ActorId(EnvKeys.Qualify(environment, StreamConstants.RegistryKey)), nameof(RegistryActor), ActorProxyDefaults.Options);

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

        var generator = ActorProxy.Create<IGeneratorActor>(new ActorId(EnvKeys.Qualify(environment, name)), nameof(GeneratorActor), ActorProxyDefaults.Options);
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
/// without ever resolving an actor proxy.</para>
///
/// <para><b>Plan 021:</b> registered as a SINGLETON (<c>ConnectorRuntimeSetup.AddServices</c>) but backs a
/// per-request read (<c>GET /api/sources/{name}/status</c>) — exactly the singleton-captures-a-transient
/// hazard <c>ICatalogFacadeFactory</c>'s own class doc warns about, so this takes the FACTORY, not
/// <see cref="ICatalogFacade"/> directly, and reads <see cref="EnvironmentAmbient.Current"/> fresh on every
/// call (the one thing this class is allowed to do — it serves a REST request, see the wave brief's facade
/// rule). The environment that matters is the SOURCE's own, once found (<c>def.Environment</c>), not
/// necessarily the ambient — but the ambient is what selects WHICH environment's catalog to look the source
/// up in in the first place, so both are needed: ambient to find <paramref name="sourceName"/>, then the
/// resolved definition's own <c>Environment</c> to address its <see cref="ConnectorActor"/>.</para></summary>
internal sealed class DaprConnectorStatusFacade(ICatalogFacadeFactory catalogFactory) : IConnectorStatusFacade
{
    public async Task<ConnectorRuntimeStatus?> GetStatusAsync(string sourceName)
    {
        var catalog = catalogFactory.For(EnvironmentAmbient.Current);
        var def = await catalog.GetSourceAsync(sourceName);
        if (def is null || SourceKindDispatch.Classify(def.Kind) != SourceKindDispatch.ActorKind.Connector)
        {
            return null;
        }

        var actor = ActorProxy.Create<IConnectorActor>(new ActorId(EnvKeys.Qualify(def.Environment, sourceName)), nameof(ConnectorActor), ActorProxyDefaults.Options);
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

/// <summary>Plan 015 W4-C: <see cref="IApprovalFacade"/> over the "approvals" singleton actor. One cached
/// proxy in a field, like <see cref="DaprAccessPolicyFacade"/> and for the same reason — the id never
/// varies, there is exactly one approvals document.
///
/// <para><see cref="RequestAsync"/> is the only member that unwraps an <see cref="ActorResult{T}"/>: filing
/// can be refused for a reason an operator must see (no enabled template covers the action; the draft names
/// no requester), and re-throwing it as an <see cref="InvalidOperationException"/> here puts it on the
/// shared endpoints' existing 409 pathway — the same shape <see cref="DaprCatalogFacade.CreateTableAsync"/>
/// uses. Everything else answers null when the transition did not happen, which is wave 1's convention for
/// this flavour's stores.</para></summary>
internal sealed class DaprApprovalFacade : IApprovalFacade
{
    private readonly IApprovalActor _actor =
        ActorProxy.Create<IApprovalActor>(new ActorId(StreamConstants.ApprovalsKey), nameof(ApprovalActor), ActorProxyDefaults.Options);

    public async Task<ApprovalRequest> RequestAsync(ApprovalRequest request)
    {
        var result = await _actor.RequestAsync(request);
        return result.Ok ? result.Value! : throw new InvalidOperationException(result.Error);
    }

    public Task<ApprovalRequest?> GetAsync(string id) => _actor.GetAsync(id);

    public Task<List<ApprovalRequest>> ListAsync(ApprovalState? state, int limit) =>
        _actor.ListAsync(new ApprovalListActorRequest(state, limit));

    public Task<ApprovalRequest?> VoteAsync(string id, ApprovalVote vote) =>
        _actor.VoteAsync(new ApprovalVoteActorRequest(id, vote));

    public Task<ApprovalRequest?> CancelAsync(string id, string username) =>
        _actor.CancelAsync(new ApprovalCancelActorRequest(id, username));

    public Task<ApprovalRequest?> RecordOutcomeAsync(string id, bool executed, string outcome) =>
        _actor.RecordOutcomeAsync(new ApprovalOutcomeActorRequest(id, executed, outcome));

    public Task<int> SweepAsync(long nowMs) => _actor.SweepAsync(nowMs);
}

/// <summary>Plan 015 W4-C: <see cref="IAuditFacade"/> over the day-sharded audit actors. Resolves a proxy
/// per call — unlike the singleton facades above and exactly like <see cref="DaprPipelineReadFacade"/>,
/// because the id IS the day and therefore varies per call.
///
/// <para>The routing function is <see cref="AuditLogStore.ActorIdFor"/>, shared with the tests rather than
/// re-derived here, so "two different days land in two different keys" is asserted on the same code the
/// facade runs.</para></summary>
internal sealed class DaprAuditFacade : IAuditFacade
{
    public Task AppendAsync(AuditEntry entry)
    {
        // An unstamped entry would shard into 1970 and be invisible in every day the operator looks at.
        // The sink stamps AtMs already; this is the belt, and it stamps the ENTRY rather than just the
        // routing key so the row and the shard it lives in cannot disagree about when it happened.
        if (entry.AtMs <= 0)
        {
            entry.AtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }

        return Day(AuditLogStore.ActorIdFor(entry.AtMs)).AppendAsync(entry);
    }

    public Task<AuditPage> QueryAsync(string day, string? actor, string? actionPrefix, int limit, int offset) =>
        Day(AuditLogStore.ActorIdForDay(day)).QueryAsync(new AuditQueryActorRequest(actor, actionPrefix, limit, offset));

    public Task<List<string>> GetDaysAsync() => Day(AuditLogStore.IndexActorId).GetDaysAsync();

    private static IAuditLogActor Day(string actorId) =>
        ActorProxy.Create<IAuditLogActor>(new ActorId(actorId), nameof(AuditLogActor), ActorProxyDefaults.Options);
}

/// <summary>Plan 021: <see cref="IEnvironmentFacade"/> over the "environments" singleton actor — one
/// cached proxy, like <see cref="DaprUserStoreFacade"/>/<see cref="DaprAccessPolicyFacade"/> and for the
/// same reason (the id never varies — <c>StreamConstants.EnvironmentsKey</c> is NEVER itself qualified,
/// see that constant's own doc comment). <see cref="Actors.EnvironmentRegistryActor"/> returns
/// <see cref="ActorResult{T}"/> for the two mutating members, unwrapped here into the exact exception
/// types <see cref="IEnvironmentFacade.CreateAsync"/>/<see cref="IEnvironmentFacade.DeleteAsync"/>'s own
/// doc comments promise — <see cref="ActorResult{T}.BadRequest"/> is what tells this adapter which one to
/// throw (see that record's own doc comment for why the field exists at all).</summary>
internal sealed class DaprEnvironmentFacade : IEnvironmentFacade
{
    private readonly IEnvironmentRegistryActor _actor = ActorProxy.Create<IEnvironmentRegistryActor>(
        new ActorId(StreamConstants.EnvironmentsKey), nameof(EnvironmentRegistryActor), ActorProxyDefaults.Options);

    public Task<List<EnvironmentRecord>> ListAsync() => _actor.ListAsync();

    public Task<bool> ExistsAsync(string name) => _actor.ExistsAsync(name);

    public async Task<EnvironmentRecord> CreateAsync(string name, string description, string createdBy)
    {
        var result = await _actor.CreateAsync(new CreateEnvironmentRequest(name, description, createdBy));
        if (result.Ok)
        {
            return result.Value!;
        }

        throw result.BadRequest ? new ArgumentException(result.Error) : new InvalidOperationException(result.Error);
    }

    public async Task<bool> DeleteAsync(string name, bool force)
    {
        var result = await _actor.DeleteAsync(new DeleteEnvironmentRequest(name, force));
        if (result.Ok)
        {
            return result.Value;
        }

        throw result.BadRequest ? new ArgumentException(result.Error) : new InvalidOperationException(result.Error);
    }
}
