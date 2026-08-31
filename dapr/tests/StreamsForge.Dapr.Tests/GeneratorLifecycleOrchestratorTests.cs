using Dapr.Client;
using Microsoft.Extensions.Logging.Abstractions;
using StreamsForge.Dapr.Host.Lifecycle;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W5-A/W6/W7: as of W7, every <see cref="DaprLifecycleOrchestrator"/>
/// method (<see cref="DaprLifecycleOrchestrator.NotifySourceChangedAsync"/>/
/// <see cref="DaprLifecycleOrchestrator.NotifySourceRemovedAsync"/>/
/// <see cref="DaprLifecycleOrchestrator.PublishLifecycleAsync"/>,
/// <see cref="DaprLifecycleOrchestrator.StartPipelineAsync"/>/<see cref="DaprLifecycleOrchestrator.StopPipelineAsync"/>
/// (W6), <see cref="DaprLifecycleOrchestrator.StartTableAsync"/>/<see cref="DaprLifecycleOrchestrator.StopTableAsync"/>
/// (W7-A), and <see cref="DaprLifecycleOrchestrator.ResetTableHistoryAsync"/>/
/// <see cref="DaprLifecycleOrchestrator.DisableTableHistoryAsync"/> (W7-B, see
/// <c>Lifecycle/DaprLifecycleOrchestrator.History.cs</c>)) now makes a real Dapr call (an actor-proxy
/// invocation or a pub/sub publish) that requires a live sidecar to complete — invoking any of them here
/// throws <c>HttpRequestException</c> ("Connection refused (localhost:3500)"), the same finding
/// <see cref="TableHistoryDeltaSinkTests"/>'s own doc comment references for the analogous table-history
/// actor calls. Live verification for all of them is covered by each wave's scripted live-check log, not a
/// unit test — this file used to hold "CompletesWithoutThrowing" tests for the table/history pair
/// specifically because THOSE were still W4's no-op stub through W6; W7 made every remaining method real,
/// so there is nothing left here to unit-test beyond the one invariant below.
///
/// <para>Constructing a <see cref="DaprClient"/> itself does no I/O (the gRPC channel is lazy), which is
/// what makes constructing <see cref="DaprLifecycleOrchestrator"/> itself — with NO method invoked — safe
/// in a unit test.</para>
/// </summary>
public class GeneratorLifecycleOrchestratorTests
{
    private static DaprLifecycleOrchestrator NewOrchestrator() => new(
        new DaprClientBuilder().Build(),
        new StreamsForge.Dapr.Host.Streaming.PipelineEventRouter(NullLogger<StreamsForge.Dapr.Host.Streaming.PipelineEventRouter>.Instance),
        new StreamsForge.Dapr.Host.Streaming.TableEventRouter(NullLogger<StreamsForge.Dapr.Host.Streaming.TableEventRouter>.Instance),
        NullLogger<DaprLifecycleOrchestrator>.Instance);

    [Fact]
    public void Construction_DoesNoIoAndDoesNotThrow()
    {
        NewOrchestrator();
    }
}
