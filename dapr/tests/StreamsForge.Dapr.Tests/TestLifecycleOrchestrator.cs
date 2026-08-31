using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Lifecycle;

namespace StreamsForge.Dapr.Tests;

/// <summary>Records every call CatalogStore makes and lets a test force a Start* call to fail, so tests
/// can assert both the "happy path" (Running, no Error) and the Failed/Error bookkeeping path without a
/// real actor runtime.</summary>
public sealed class TestLifecycleOrchestrator : ILifecycleOrchestrator
{
    public List<string> Calls { get; } = [];

    public bool FailStarts { get; set; }

    public string FailureMessage { get; set; } = "forced failure";

    public Task NotifySourceChangedAsync(SourceDefinition def)
    {
        Calls.Add($"NotifySourceChanged:{def.Name}:{def.Enabled}");
        return Task.CompletedTask;
    }

    public Task NotifySourceRemovedAsync(string name, string environment)
    {
        Calls.Add($"NotifySourceRemoved:{name}");
        return Task.CompletedTask;
    }

    public Task<LifecycleOutcome> StartPipelineAsync(PipelineDefinition def, IReadOnlyList<SourceDefinition> sources)
    {
        Calls.Add($"StartPipeline:{def.Id}:{sources.Count}");
        return Task.FromResult(FailStarts ? LifecycleOutcome.Failure(FailureMessage) : LifecycleOutcome.Success);
    }

    public Task StopPipelineAsync(string pipelineId)
    {
        Calls.Add($"StopPipeline:{pipelineId}");
        return Task.CompletedTask;
    }

    public Task<LifecycleOutcome> StartTableAsync(TableDefinition def, IReadOnlyList<SourceDefinition> sources, IReadOnlyList<TableDefinition> tables)
    {
        Calls.Add($"StartTable:{def.Name}:{sources.Count}:{tables.Count}");
        return Task.FromResult(FailStarts ? LifecycleOutcome.Failure(FailureMessage) : LifecycleOutcome.Success);
    }

    public Task StopTableAsync(string tableName, string environment)
    {
        Calls.Add($"StopTable:{tableName}");
        return Task.CompletedTask;
    }

    public Task ResetTableHistoryAsync(TableDefinition def)
    {
        Calls.Add($"ResetTableHistory:{def.Name}");
        return Task.CompletedTask;
    }

    public Task DisableTableHistoryAsync(string tableName, string environment)
    {
        Calls.Add($"DisableTableHistory:{tableName}");
        return Task.CompletedTask;
    }

    public Task PublishLifecycleAsync(string entityId, string kind, PipelineStatus status)
    {
        Calls.Add($"Lifecycle:{entityId}:{kind}:{status}");
        return Task.CompletedTask;
    }
}
