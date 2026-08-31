using StreamsForge.Abstractions;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Minimal in-memory <see cref="IRegistryGrain"/> stand-in for unit-testing code that only needs
/// catalog reads + field-number bookkeeping (e.g. <see cref="Host.Grpc.Dynamic.DynamicDescriptorSet"/>)
/// without spinning up an Orleans cluster. Only the members those callers actually exercise are
/// implemented for real; everything else throws NotImplementedException so an accidental new dependency
/// on unimplemented grain behavior fails loudly in a test rather than silently no-op'ing.
/// </summary>
internal sealed class FakeRegistryGrain : IRegistryGrain
{
    /// <summary>Interface conformance only — wishlist #8's run-on-demand needs a real runtime to
    /// publish, so a fake correctly reports that there is nothing to run.</summary>
    public Task<ScenarioRunResult> RunSourceAsync(string name, ScenarioRunRequest request) =>
        Task.FromResult(new ScenarioRunResult { Outcome = ScenarioRunOutcome.NotFound });

    public List<SourceDefinition> Sources { get; } = [];
    public List<PipelineDefinition> Pipelines { get; } = [];
    public List<TableDefinition> Tables { get; } = [];

    /// <summary>entityKey -> FieldNumberMap JSON, exactly like RegistryGrain's own persisted state.</summary>
    public Dictionary<string, string> FieldNumberMaps { get; } = [];

    /// <summary>Number of EnsureFieldNumbersAsync calls made so far, for assertions on caller behavior.</summary>
    public int EnsureFieldNumbersCallCount { get; private set; }

    public Task<List<SourceDefinition>> GetSourcesAsync() => Task.FromResult(Sources);

    public Task<List<PipelineDefinition>> GetPipelinesAsync() => Task.FromResult(Pipelines);

    public Task<List<TableDefinition>> GetTablesAsync() => Task.FromResult(Tables);

    public Task<string> EnsureFieldNumbersAsync(string entityKey, List<FieldDef> fields)
    {
        EnsureFieldNumbersCallCount++;
        var existingJson = FieldNumberMaps.GetValueOrDefault(entityKey);
        var existing = existingJson is null
            ? null
            : System.Text.Json.JsonSerializer.Deserialize<Host.Grpc.Dynamic.FieldNumberMap>(existingJson);
        var updated = System.Text.Json.JsonSerializer.Serialize(Host.Grpc.Dynamic.FieldNumberMap.Assign(fields, existing));
        FieldNumberMaps[entityKey] = updated;
        return Task.FromResult(updated);
    }

    public Task EnsureInitializedAsync() => throw new NotImplementedException();
    public Task<SourceDefinition?> GetSourceAsync(string name) => Task.FromResult(Sources.FirstOrDefault(s => s.Name == name));
    public Task UpsertSourceAsync(SourceDefinition def) => throw new NotImplementedException();
    public Task<bool> DeleteSourceAsync(string name) => throw new NotImplementedException();
    public Task<PipelineDefinition?> GetPipelineAsync(string id) => Task.FromResult(Pipelines.FirstOrDefault(p => p.Id == id));
    public Task<PipelineDefinition> CreatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
    public Task<PipelineDefinition?> UpdatePipelineAsync(PipelineDefinition def) => throw new NotImplementedException();
    public Task<bool> DeletePipelineAsync(string id) => throw new NotImplementedException();
    public Task<PipelineDefinition?> SetPipelineStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();
    public Task<TableDefinition?> GetTableAsync(string id) => Task.FromResult(Tables.FirstOrDefault(t => t.Id == id));
    public Task<TableDefinition> CreateTableAsync(TableDefinition def) => throw new NotImplementedException();
    public Task<TableDefinition?> UpdateTableAsync(TableDefinition def) => throw new NotImplementedException();
    public Task<bool> DeleteTableAsync(string id) => throw new NotImplementedException();
    public Task<TableDefinition?> SetTableStatusAsync(string id, PipelineStatus status) => throw new NotImplementedException();

    // IGrainWithStringKey / IAddressable / IGrain -- not exercised by any code under test.
    public string GetPrimaryKeyString() => throw new NotImplementedException();
}
