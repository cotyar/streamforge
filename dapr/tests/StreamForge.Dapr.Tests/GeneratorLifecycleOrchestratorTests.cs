using Dapr.Client;
using Microsoft.Extensions.Logging.Abstractions;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Lifecycle;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W5-A: unit tests for the parts of
/// <see cref="DaprLifecycleOrchestrator"/> that are "orchestrator routing decisions ... factorable
/// without a live sidecar" (per the W5-A wave brief) — the pipeline/table/history methods, which this
/// wave keeps as W4's warn+no-op behavior verbatim (W6/W7 replace them) and whose
/// <see cref="LifecycleOutcome"/> contract must not regress.
///
/// <para>Deliberately NOT covered here: <see cref="DaprLifecycleOrchestrator.NotifySourceChangedAsync"/>,
/// <see cref="DaprLifecycleOrchestrator.NotifySourceRemovedAsync"/>, and
/// <see cref="DaprLifecycleOrchestrator.PublishLifecycleAsync"/> — all three make a real Dapr call (an
/// actor-proxy invocation or a pub/sub publish) that requires a live sidecar to complete; this wave's
/// brief explicitly leaves live verification to the other W5 agent / the orchestrator's joint check.
/// Constructing a <see cref="DaprClient"/> itself does no I/O (the gRPC channel is lazy), which is what
/// makes constructing <see cref="DaprLifecycleOrchestrator"/> itself safe in a unit test.</para>
/// </summary>
public class GeneratorLifecycleOrchestratorTests
{
    private static DaprLifecycleOrchestrator NewOrchestrator() =>
        new(new DaprClientBuilder().Build(), NullLogger<DaprLifecycleOrchestrator>.Instance);

    [Fact]
    public async Task StartPipelineAsync_ReturnsSuccess_NoRuntimeYet()
    {
        var orchestrator = NewOrchestrator();

        var outcome = await orchestrator.StartPipelineAsync(new PipelineDefinition { Id = "p1", Sql = "SELECT 1" });

        Assert.True(outcome.Ok);
        Assert.Null(outcome.Error);
    }

    [Fact]
    public async Task StopPipelineAsync_CompletesWithoutThrowing()
    {
        var orchestrator = NewOrchestrator();

        await orchestrator.StopPipelineAsync("p1");
    }

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
