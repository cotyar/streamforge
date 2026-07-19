using StreamForge.Abstractions;

namespace StreamForge.Dapr.Host.Lifecycle;

/// <summary>
/// W7-B's half of the orchestrator (partial class so wave W7's two parallel agents own disjoint
/// files: table methods live in DaprLifecycleOrchestrator.cs — W7-A; history methods here — W7-B).
/// </summary>
public sealed partial class DaprLifecycleOrchestrator
{
    public Task ResetTableHistoryAsync(TableDefinition def)
    {
        WarnNoRuntime("ResetTableHistory", def.Name);
        return Task.CompletedTask;
    }

    public Task DisableTableHistoryAsync(string tableName)
    {
        WarnNoRuntime("DisableTableHistory", tableName);
        return Task.CompletedTask;
    }
}
