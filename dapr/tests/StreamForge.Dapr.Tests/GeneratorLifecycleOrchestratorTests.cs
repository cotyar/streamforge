using Dapr.Client;
using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Lifecycle;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W5-A/W6: unit tests for the parts of
/// <see cref="DaprLifecycleOrchestrator"/> that are "orchestrator routing decisions ... factorable
/// without a live sidecar" — the table/history methods, which this wave still keeps as W4's warn+no-op
/// behavior verbatim (W7 replaces them) and whose <see cref="LifecycleOutcome"/> contract must not
/// regress.
///
/// <para>Deliberately NOT covered here: <see cref="DaprLifecycleOrchestrator.NotifySourceChangedAsync"/>,
/// <see cref="DaprLifecycleOrchestrator.NotifySourceRemovedAsync"/>,
/// <see cref="DaprLifecycleOrchestrator.PublishLifecycleAsync"/>, and — as of W6 —
/// <see cref="DaprLifecycleOrchestrator.StartPipelineAsync"/>/<see cref="DaprLifecycleOrchestrator.StopPipelineAsync"/>
/// — all five now make a real Dapr call (an actor-proxy invocation or a pub/sub publish) that requires a
/// live sidecar to complete; live verification is covered by this wave's scripted live-check log, not a
/// unit test. Constructing a <see cref="DaprClient"/> itself does no I/O (the gRPC channel is lazy),
/// which is what makes constructing <see cref="DaprLifecycleOrchestrator"/> itself safe in a unit
/// test.</para>
/// </summary>
public class GeneratorLifecycleOrchestratorTests
{
    private static DaprLifecycleOrchestrator NewOrchestrator() =>
        new(new DaprClientBuilder().Build(), new StreamForge.Dapr.Host.Streaming.PipelineEventRouter(NullLogger<StreamForge.Dapr.Host.Streaming.PipelineEventRouter>.Instance), NullLogger<DaprLifecycleOrchestrator>.Instance);

    [Fact]
    public async Task StartTableAsync_ReturnsSuccess_NoRuntimeYet()
    {
        var orchestrator = NewOrchestrator();

        var outcome = await orchestrator.StartTableAsync(new TableDefinition { Name = "t1", Sql = "SELECT 1" });

        Assert.True(outcome.Ok);
        Assert.Null(outcome.Error);
    }

    [Fact]
    public async Task StopTableAsync_CompletesWithoutThrowing()
    {
        var orchestrator = NewOrchestrator();

        await orchestrator.StopTableAsync("t1");
    }

    [Fact]
    public async Task ResetTableHistoryAsync_CompletesWithoutThrowing()
    {
        var orchestrator = NewOrchestrator();

        await orchestrator.ResetTableHistoryAsync(new TableDefinition { Name = "t1", Sql = "SELECT 1" });
    }

    [Fact]
    public async Task DisableTableHistoryAsync_CompletesWithoutThrowing()
    {
        var orchestrator = NewOrchestrator();

        await orchestrator.DisableTableHistoryAsync("t1");
    }
}
