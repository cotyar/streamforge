using StreamForge.Abstractions;

namespace StreamForge.Dapr.Host.Lifecycle;

/// <summary>
/// Plan 005 W4 implementation of <see cref="ILifecycleOrchestrator"/>: logs a warning and reports success
/// for every start/stop/reset call — there is no generator/pipeline/table/history runtime behind any of
/// these yet (W5/W6/W7). This keeps the catalog's CRUD + status bookkeeping fully functional today
/// (start/stop toggles Running/Stopped and persists) while being honest in logs and docs (see
/// dapr/ARCHITECTURE.md's "seed status" note) that nothing is actually processing events on this flavor
/// until later waves land.
/// </summary>
public sealed class NoopLifecycleOrchestrator(ILogger<NoopLifecycleOrchestrator> logger) : ILifecycleOrchestrator
{
    private void WarnNoRuntime(string action, string id) =>
        logger.LogWarning("{Action}({Id}): no runtime yet (W5/W6/W7) — catalog status updated, no process started.", action, id);

    public Task NotifySourceChangedAsync(SourceDefinition def)
    {
        WarnNoRuntime(def.Enabled ? "StartGenerator" : "StopGenerator", def.Name);
        return Task.CompletedTask;
    }

    public Task NotifySourceRemovedAsync(string name, string environment)
    {
        WarnNoRuntime("RemoveGenerator", name);
        return Task.CompletedTask;
    }

    public Task<LifecycleOutcome> StartPipelineAsync(PipelineDefinition def, IReadOnlyList<SourceDefinition> sources)
    {
        WarnNoRuntime("StartPipeline", def.Id);
        return Task.FromResult(LifecycleOutcome.Success);
    }

    public Task StopPipelineAsync(string pipelineId)
    {
        WarnNoRuntime("StopPipeline", pipelineId);
        return Task.CompletedTask;
    }

    public Task<LifecycleOutcome> StartTableAsync(TableDefinition def, IReadOnlyList<SourceDefinition> sources, IReadOnlyList<TableDefinition> tables)
    {
        WarnNoRuntime("StartTable", def.Name);
        return Task.FromResult(LifecycleOutcome.Success);
    }

    public Task StopTableAsync(string tableName, string environment)
    {
        WarnNoRuntime("StopTable", tableName);
        return Task.CompletedTask;
    }

    public Task ResetTableHistoryAsync(TableDefinition def)
    {
        WarnNoRuntime("ResetTableHistory", def.Name);
        return Task.CompletedTask;
    }

    public Task DisableTableHistoryAsync(string tableName, string environment)
    {
        WarnNoRuntime("DisableTableHistory", tableName);
        return Task.CompletedTask;
    }

    public Task PublishLifecycleAsync(string entityId, string kind, PipelineStatus status)
    {
        logger.LogInformation("lifecycle event (not yet published — W5 wires sf-lifecycle): {Kind} {Id} -> {Status}", kind, entityId, status);
        return Task.CompletedTask;
    }
}
