using Dapr.Actors.Runtime;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Catalog;
using StreamForge.Dapr.Host.Lifecycle;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W4: Dapr counterpart of Orleans' <c>RegistryGrain</c> — singleton
/// actor, id = <see cref="StreamConstants.RegistryKey"/> ("catalog"). All catalog CRUD/validation/
/// compile logic lives in the actor-framework-free <see cref="CatalogStore"/>; this class is the thin
/// actor shell: loads/saves <see cref="CatalogState"/> via Dapr's <c>StateManager</c> (one state entry
/// named "catalog", persisted to the Redis actor state store configured in
/// dapr/components/statestore.yaml — see dapr/ARCHITECTURE.md for the state-store/serialization
/// decisions) and translates <see cref="CatalogStore"/>'s thrown <see cref="InvalidOperationException"/>s
/// into <see cref="ActorResult{T}"/> failures for the table-mutation methods (see that type's class doc).
///
/// <para><b>Reentrancy:</b> this actor never calls back into itself, another actor, or an actor proxy
/// from inside one of its own method bodies — all "start the runtime for X" side effects go through
/// <see cref="ILifecycleOrchestrator"/> (constructor-injected, ordinary DI, not an actor reference), which
/// in W4 only logs. There is therefore no A→B→A call cycle to reason about yet, and the actor is
/// registered with Dapr's DEFAULT (non-reentrant) turn-based concurrency — see
/// dapr/ARCHITECTURE.md's reentrancy decision for what later waves must preserve to keep it that way.</para>
///
/// <para><b>State granularity:</b> one "catalog" state blob holds sources+pipelines+tables+field-number
/// maps together (mirrors RegistryGrain's single RegistryState) — fine at this scale (a handful of
/// entities; the whole blob is a few KB of JSON) and keeps load/save trivially atomic per turn, at the
/// cost of rewriting the whole blob on every single mutation. Revisit only if profiling ever shows this
/// matters (it won't, at demo scale).</para>
/// </summary>
public sealed class RegistryActor(ActorHost host, ILifecycleOrchestrator orchestrator, ILogger<RegistryActor> logger)
    : Actor(host), IRegistryActor
{
    private const string StateName = "catalog";

    private CatalogState _state = new();
    private CatalogStore _store = null!;

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<CatalogState>(StateName);
        _state = existing.HasValue ? existing.Value : new CatalogState();
        _store = new CatalogStore(_state, orchestrator);
    }

    private Task SaveAsync() => StateManager.SetStateAsync(StateName, _state);

    public async Task EnsureInitializedAsync()
    {
        var dirty = _store.EnsureInitialized();
        if (dirty)
        {
            await SaveAsync();
            logger.LogInformation("catalog seeded ({Sources} sources, {Pipelines} pipelines, {Tables} tables)",
                _state.Sources.Count, _state.Pipelines.Count, _state.Tables.Count);
        }
    }

    public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(_store.GetSources());

    public Task<SourceDefinition?> GetSourceAsync(string name) => Task.FromResult(_store.GetSource(name));

    public async Task UpsertSourceAsync(SourceDefinition def)
    {
        await _store.UpsertSourceAsync(def);
        await SaveAsync();
    }

    public async Task<bool> DeleteSourceAsync(string name)
    {
        var removed = await _store.DeleteSourceAsync(name);
        if (removed)
        {
            await SaveAsync();
        }
        return removed;
    }

    public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(_store.GetPipelines());

    public Task<PipelineDefinition?> GetPipelineAsync(string id) => Task.FromResult(_store.GetPipeline(id));

    public async Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def)
    {
        var created = await _store.CreatePipelineAsync(def);
        await SaveAsync();
        return created;
    }

    public async Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def)
    {
        var updated = await _store.UpdatePipelineAsync(def);
        if (updated is not null)
        {
            await SaveAsync();
        }
        return updated;
    }

    public async Task<bool> DeletePipelineAsync(string id)
    {
        var removed = await _store.DeletePipelineAsync(id);
        if (removed)
        {
            await SaveAsync();
        }
        return removed;
    }

    public async Task<PipelineDefinition?> SetPipelineStatusAsync(SetStatusRequest request)
    {
        var updated = await _store.SetPipelineStatusAsync(request.Id, request.Status);
        if (updated is not null)
        {
            await SaveAsync();
        }
        return updated;
    }

    public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(_store.GetTables());

    public Task<TableDefinition?> GetTableAsync(string id) => Task.FromResult(_store.GetTable(id));

    public async Task<ActorResult<TableDefinition>> CreateTableAsync(TableDefinition def)
    {
        try
        {
            var created = await _store.CreateTableAsync(def);
            await SaveAsync();
            return ActorResult<TableDefinition>.Success(created);
        }
        catch (InvalidOperationException ex)
        {
            return ActorResult<TableDefinition>.Failure(ex.Message);
        }
    }

    public async Task<ActorResult<TableDefinition?>> UpdateTableAsync(TableDefinition def)
    {
        try
        {
            var updated = await _store.UpdateTableAsync(def);
            if (updated is not null)
            {
                await SaveAsync();
            }
            return ActorResult<TableDefinition?>.Success(updated);
        }
        catch (InvalidOperationException ex)
        {
            return ActorResult<TableDefinition?>.Failure(ex.Message);
        }
    }

    public async Task<ActorResult<bool>> DeleteTableAsync(string id)
    {
        try
        {
            var removed = await _store.DeleteTableAsync(id);
            if (removed)
            {
                await SaveAsync();
            }
            return ActorResult<bool>.Success(removed);
        }
        catch (InvalidOperationException ex)
        {
            return ActorResult<bool>.Failure(ex.Message);
        }
    }

    public async Task<ActorResult<TableDefinition?>> SetTableStatusAsync(SetStatusRequest request)
    {
        try
        {
            var updated = await _store.SetTableStatusAsync(request.Id, request.Status);
            if (updated is not null)
            {
                await SaveAsync();
            }
            return ActorResult<TableDefinition?>.Success(updated);
        }
        catch (InvalidOperationException ex)
        {
            return ActorResult<TableDefinition?>.Failure(ex.Message);
        }
    }

    public async Task<string> EnsureFieldNumbersAsync(EnsureFieldNumbersRequest request)
    {
        var json = _store.EnsureFieldNumbers(request.EntityKey, request.Fields);
        await SaveAsync();
        return json;
    }
}
