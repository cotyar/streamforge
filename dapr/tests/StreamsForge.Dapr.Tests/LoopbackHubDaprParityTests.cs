using StreamsForge.Abstractions;
using StreamsForge.AppCore.Sinks;
using StreamsForge.Host.Generators;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>Wishlist #9(b): Dapr-flavor parity smoke test for <see cref="LoopbackHub"/> and
/// <see cref="LoopbackSinkClient"/> — both live in shared/StreamsForge.AppCore, referenced identically by
/// Orleans' <c>GeneratorGrain</c> and Dapr's <c>GeneratorActor</c> (see <c>GeneratorActor.DrainLoopbackAsync</c>).
/// The exhaustive behavioral coverage (maxDepth guard reuse, unattached-target failure reporting, FIFO
/// drain) already lives flavor-agnostically in
/// orleans/tests/StreamsForge.Host.Tests/LoopbackSinkClientTests.cs; this file only confirms the SAME pure
/// calls resolve and behave identically from the Dapr test project, same convention as
/// <see cref="ScenarioGeneratorDaprParityTests"/>. No Dapr ActorHost/sidecar is involved — see this wave's
/// report for why an actor-host-level integration test of <c>GeneratorActor</c>'s drain timer was not
/// attempted (this repo's existing Dapr tests consistently avoid needing a live sidecar).</summary>
public class LoopbackHubDaprParityTests
{
    [Fact]
    public async Task LoopbackSinkClient_writes_through_LoopbackHub_when_called_from_the_dapr_project()
    {
        var target = "dapr_loop_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(target);
        try
        {
            await using var client = new LoopbackSinkClient(
                new LoopbackSinkConfig { TargetSourceName = target }, "table", "t1");

            await client.PublishAsync(
                new NatsTableDeltaMessage { Table = "t1", Row = new() { ["x"] = 1L }, Weight = 1 },
                CancellationToken.None);

            var drained = LoopbackHub.Drain(target, 10);
            Assert.Single(drained);
            Assert.Equal(1, client.Counters.Published);
        }
        finally
        {
            LoopbackHub.Detach(target);
        }
    }

    [Fact]
    public async Task MaxDepth_drops_a_row_at_the_bound_from_the_dapr_project_too()
    {
        var target = "dapr_loop_guard_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(target);
        try
        {
            await using var client = new LoopbackSinkClient(
                new LoopbackSinkConfig { TargetSourceName = target, MaxDepth = 2 }, "table", "loop");

            await client.PublishAsync(
                new NatsTableDeltaMessage { Table = "loop", Row = new() { ["step"] = 2L }, Weight = 1 },
                CancellationToken.None);

            Assert.Empty(LoopbackHub.Drain(target, 10));
            Assert.Equal(1, client.Counters.Failed);
        }
        finally
        {
            LoopbackHub.Detach(target);
        }
    }
}
