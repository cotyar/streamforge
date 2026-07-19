using Dapr.Actors;
using StreamForge.Abstractions;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>Request payload for methods with more than one logical argument — Dapr actor interface
/// methods support at most one parameter (unlike Orleans grain methods, which allow arbitrary parameter
/// lists via Orleans' own serializer), so every multi-argument <see cref="Abstractions.ICatalogFacade"/>
/// member is wrapped in a small record here.</summary>
public sealed record SetStatusRequest(string Id, PipelineStatus Status);

public sealed record EnsureFieldNumbersRequest(string EntityKey, List<FieldDef> Fields);

/// <summary>
/// Actor-invocation surface for the "catalog" singleton actor (id = <see cref="StreamConstants.RegistryKey"/>).
/// Plan 005 W4: this is NOT the same interface as Orleans' <c>IRegistryGrain</c> and does NOT inherit
/// <see cref="ICatalogFacade"/> directly (a Dapr actor method takes 0 or 1 parameters, so several
/// ICatalogFacade members — e.g. <c>SetPipelineStatusAsync(string, PipelineStatus)</c>,
/// <c>EnsureFieldNumbersAsync(string, List&lt;FieldDef&gt;)</c> — can't be exposed 1:1). Instead,
/// <see cref="Facades.DaprCatalogFacade"/> is a small adapter translating each ICatalogFacade call into
/// one of these actor methods (packing multi-arg calls into <see cref="SetStatusRequest"/>/
/// <see cref="EnsureFieldNumbersRequest"/>, and unwrapping <see cref="ActorResult{T}"/> results back into
/// return values or a thrown <see cref="InvalidOperationException"/>) — mirroring how the Orleans side
/// has its own small per-call adapters for the KEYED facades (IPipelineReadFacade etc.), just needed here
/// for the SINGLETON facades too because of the one-parameter constraint.
/// </summary>
public interface IRegistryActor : IActor
{
    /// <summary>Seeds the demo catalog on first activation (empty state) — see
    /// Catalog.CatalogStore.EnsureInitialized. Idempotent; safe to call on every activation.</summary>
    Task EnsureInitializedAsync();

    Task<List<SourceDefinition>> GetSourcesAsync();
    Task<SourceDefinition?> GetSourceAsync(string name);
    Task UpsertSourceAsync(SourceDefinition def);
    Task<bool> DeleteSourceAsync(string name);

    Task<List<PipelineDefinition>> GetPipelinesAsync();
    Task<PipelineDefinition?> GetPipelineAsync(string id);
    Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def);
    Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def);
    Task<bool> DeletePipelineAsync(string id);
    Task<PipelineDefinition?> SetPipelineStatusAsync(SetStatusRequest request);

    Task<List<TableDefinition>> GetTablesAsync();
    Task<TableDefinition?> GetTableAsync(string id);
    Task<ActorResult<TableDefinition>> CreateTableAsync(TableDefinition def);
    Task<ActorResult<TableDefinition?>> UpdateTableAsync(TableDefinition def);
    Task<ActorResult<bool>> DeleteTableAsync(string id);
    Task<ActorResult<TableDefinition?>> SetTableStatusAsync(SetStatusRequest request);

    Task<string> EnsureFieldNumbersAsync(EnsureFieldNumbersRequest request);
}
