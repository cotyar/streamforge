using Dapr.Actors.Runtime;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Dapr.Host.Facades;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 021 (environment isolation), Dapr track: the environment directory — singleton actor, id =
/// <c>StreamConstants.EnvironmentsKey</c> ("environments"), itself NEVER environment-qualified (it is the
/// thing that says which environments exist). Mirrors <see cref="RegistryActor"/>'s own thin-shell-over-a-
/// pure-store shape: all name/duplicate/reserved-word logic lives in the actor-framework-free
/// <see cref="EnvironmentRegistryStore"/>; this class loads/saves <see cref="EnvironmentRegistryState"/>
/// via Dapr's <c>StateManager</c> and does the one thing the pure store cannot — decide whether an
/// environment is "empty" (needs another actor's catalog) and, on a forced delete, tear down that
/// catalog's contents by calling back into the SAME per-entity delete paths a user-driven DELETE would
/// (<see cref="ICatalogFacade.DeletePipelineAsync"/>/<c>DeleteTableAsync</c>/<c>DeleteSourceAsync</c>) —
/// see <see cref="DeleteAsync"/>'s own doc comment for exactly what that does and does not clean up.
///
/// <para><b>Acyclic by construction, like <see cref="Lifecycle.DaprLifecycleOrchestrator"/>:</b> this actor
/// calls OUT to another environment's <see cref="RegistryActor"/> (via <see cref="ICatalogFacadeFactory"/>,
/// itself an actor-proxy adapter) but nothing ever calls back INTO this actor from inside a
/// <see cref="RegistryActor"/> turn — there is no cycle to deadlock on, the same acyclic shape the
/// reentrancy decision in dapr/ARCHITECTURE.md requires.</para>
/// </summary>
public sealed class EnvironmentRegistryActor(ActorHost host, ICatalogFacadeFactory catalogFactory, ILogger<EnvironmentRegistryActor> logger)
    : Actor(host), IEnvironmentRegistryActor
{
    private const string StateName = "environments";

    private EnvironmentRegistryState _state = new();
    private EnvironmentRegistryStore _store = null!;

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<EnvironmentRegistryState>(StateName);
        _state = existing.HasValue ? existing.Value : new EnvironmentRegistryState();
        _store = new EnvironmentRegistryStore(_state);
    }

    private Task SaveAsync() => StateManager.SetStateAsync(StateName, _state);

    /// <summary>Fills <see cref="EnvironmentRecord.EntityCount"/> with a real catalog read per environment
    /// — see that property's own doc comment for why it is never persisted. Rarely called (an
    /// environments list, not a per-request hot path like <see cref="ExistsAsync"/>), so N catalog round
    /// trips per call is an acceptable cost for an honest count.</summary>
    public async Task<List<EnvironmentRecord>> ListAsync()
    {
        var records = _store.ListWithDefault();
        foreach (var record in records)
        {
            var env = EnvKeys.Normalize(record.Name);
            var catalog = catalogFactory.For(env);
            var sources = await catalog.GetSourcesAsync();
            var pipelines = await catalog.GetPipelinesAsync();
            var tables = await catalog.GetTablesAsync();
            record.EntityCount = sources.Count + pipelines.Count + tables.Count;
        }

        return records;
    }

    public Task<bool> ExistsAsync(string name) => Task.FromResult(_store.Exists(EnvKeys.Normalize(name)));

    public async Task<ActorResult<EnvironmentRecord>> CreateAsync(CreateEnvironmentRequest request)
    {
        try
        {
            var record = _store.Create(request.Name, request.Description, request.CreatedBy, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            await SaveAsync();
            return ActorResult<EnvironmentRecord>.Success(record);
        }
        catch (ArgumentException ex)
        {
            return ActorResult<EnvironmentRecord>.Failure(ex.Message, badRequest: true);
        }
        catch (InvalidOperationException ex)
        {
            return ActorResult<EnvironmentRecord>.Failure(ex.Message);
        }
    }

    /// <summary>Refuses <see cref="EnvKeys.Default"/> outright, always (D7 — it cannot be created, deleted
    /// or renamed). Returns <c>Success(false)</c> — not a failure — when the name doesn't exist, per
    /// <see cref="IEnvironmentFacade.DeleteAsync"/>'s own doc comment. Refuses a non-empty environment
    /// unless <paramref name="request"/>.<see cref="DeleteEnvironmentRequest.Force"/> is set.
    ///
    /// <para><b>What a forced delete does:</b> deletes every pipeline, then every table (retried in a
    /// worklist so a table that still has a RUNNING dependent — <c>CatalogStore.ThrowIfRunningDependents</c>
    /// — succeeds once its dependent is gone, without this actor needing to compute a topological order
    /// itself), then every source, each through <see cref="ICatalogFacade"/>'s ordinary delete methods — the
    /// exact same stop/teardown/history-disable path a user-driven <c>DELETE</c> takes, per entity. Only
    /// then is the environment's own directory ROW removed.</para>
    ///
    /// <para><b>What it deliberately does NOT clean up</b> (documented per the wave brief, not silently
    /// dropped): the deleted environment's own <see cref="RegistryActor"/> Dapr actor-state entry
    /// (<c>{appId}||RegistryActor||{env}.catalog||catalog</c>) is left behind as an empty, orphaned blob —
    /// Dapr gives an actor no supported way to erase another actor's persisted state from outside it, only
    /// to ask that actor to stop doing things with it. The same is true of any stopped
    /// <see cref="GeneratorActor"/>/<see cref="ConnectorActor"/>/<see cref="TableActor"/>/
    /// <see cref="TableHistoryActor"/> state left over from an entity this sweep deleted — its own delete
    /// path stops the actor (StopAsync clears its running/started flags and re-saves), it does not erase the
    /// Redis key that actor's state lives under. All of these are small (a few KB each) and harmless — an
    /// actor that never reactivates never re-reads them — but a re-CREATED environment or entity with the
    /// same name would reactivate the SAME actor id and see that leftover state on its very first
    /// activation, exactly as re-creating an entity in the SAME environment already can today (this is not
    /// a new hazard 021 introduces, just one more place it was already true).</para></summary>
    public async Task<ActorResult<bool>> DeleteAsync(DeleteEnvironmentRequest request)
    {
        var name = EnvKeys.Normalize(request.Name);
        var workflow = new EnvironmentDeleteWorkflow(_store, catalogFactory);
        var result = await workflow.DeleteAsync(name, request.Force, remaining => logger.LogWarning(
            "EnvironmentRegistryActor: force-delete of '{Environment}' could not remove {Count} table(s) with unresolvable running dependents — left in place.",
            name, remaining));

        if (result is { Ok: true, Value: true })
        {
            // Only the actual removal needs persisting — Success(false) (unknown name) and every Failure
            // leave _store untouched, so saving unconditionally would just be a wasted round trip on the
            // two most common non-mutating outcomes.
            await SaveAsync();
        }

        return result;
    }
}
