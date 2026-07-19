using StreamForge.Abstractions;

namespace StreamForge.Dapr.Host.Catalog;

/// <summary>
/// Persisted shape of the "catalog" actor state — mirrors Orleans' <c>RegistryState</c>
/// (orleans/src/StreamForge.Host/Grains/RegistryGrain.cs) field for field, so both flavors persist the
/// identical logical catalog (just via a different storage backend: JSON-file grain storage vs. this
/// state serialized as one Redis actor-state entry under key "catalog", see dapr/ARCHITECTURE.md).
/// Serialized via plain System.Text.Json (RegistryActor's own state-manager call, not the actor
/// method-invocation wire) — see RegistryActor's class doc for the serialization decision.
/// </summary>
public sealed class CatalogState
{
    public List<SourceDefinition> Sources { get; set; } = [];
    public List<PipelineDefinition> Pipelines { get; set; } = [];
    public List<TableDefinition> Tables { get; set; } = [];

    /// <summary>entityKey ("source:{name}" / "pipeline:{id}" / "table:{id}") → FieldNumberMap JSON. See
    /// ICatalogFacade.EnsureFieldNumbersAsync.</summary>
    public Dictionary<string, string> FieldNumberMaps { get; set; } = [];
}
