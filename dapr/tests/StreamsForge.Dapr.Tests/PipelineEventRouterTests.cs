using Microsoft.Extensions.Logging.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.Dapr.Host.Streaming;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W6: unit tests for <see cref="PipelineEventRouter"/>'s routing-table
/// logic (register/unregister/lookup) — the actual <see cref="PipelineEventRouter.OnSourceEventsAsync"/>
/// fan-out path resolves a live <c>IPipelineActor</c> proxy per subscriber, which needs a Dapr sidecar to
/// complete, so these tests exercise <see cref="PipelineEventRouter.SubscribersOf"/> (the table itself)
/// directly — mirroring how <see cref="CatalogStoreTests"/> exercises the pure logic layer rather than
/// the actor shell.
/// </summary>
public class PipelineEventRouterTests
{
    private static PipelineEventRouter NewRouter() => new(NullLogger<PipelineEventRouter>.Instance);

    [Fact]
    public void Register_NewPipeline_AddsToEverySourceItDependsOn()
    {
        var router = NewRouter();

        router.Register("p1", ["trades", "quotes"]);

        Assert.Contains("p1", router.SubscribersOf("trades"));
        Assert.Contains("p1", router.SubscribersOf("quotes"));
        Assert.Empty(router.SubscribersOf("orders"));
    }

    [Fact]
    public void Register_MultiplePipelinesOnSameSource_BothTracked()
    {
        var router = NewRouter();

        router.Register("p1", ["trades"]);
        router.Register("p2", ["trades"]);

        var subs = router.SubscribersOf("trades");
        Assert.Contains("p1", subs);
        Assert.Contains("p2", subs);
        Assert.Equal(2, subs.Count);
    }

    [Fact]
    public void Register_CalledAgainForSamePipeline_ReplacesItsPreviousSubscriptionSet()
    {
        var router = NewRouter();
        router.Register("p1", ["trades", "quotes"]);

        router.Register("p1", ["orders"]);

        Assert.DoesNotContain("p1", router.SubscribersOf("trades"));
        Assert.DoesNotContain("p1", router.SubscribersOf("quotes"));
        Assert.Contains("p1", router.SubscribersOf("orders"));
    }

    [Fact]
    public void Register_EmptySourceList_LeavesPipelineWithNoSubscriptions()
    {
        var router = NewRouter();
        router.Register("p1", ["trades"]);

        router.Register("p1", []);

        Assert.DoesNotContain("p1", router.SubscribersOf("trades"));
    }

    [Fact]
    public void Unregister_RemovesFromEverySourceItWasSubscribedTo()
    {
        var router = NewRouter();
        router.Register("p1", ["trades", "quotes"]);
        router.Register("p2", ["trades"]);

        router.Unregister("p1");

        Assert.DoesNotContain("p1", router.SubscribersOf("trades"));
        Assert.DoesNotContain("p1", router.SubscribersOf("quotes"));
        // p2's own subscription on "trades" must survive p1's removal.
        Assert.Contains("p2", router.SubscribersOf("trades"));
    }

    [Fact]
    public void Unregister_UnknownPipeline_IsANoOp()
    {
        var router = NewRouter();

        router.Unregister("never-registered");
        // No assertion beyond "didn't throw".
    }

    [Fact]
    public async Task OnSourceEventsAsync_SourceWithNoSubscribers_CompletesWithoutError()
    {
        var router = NewRouter();

        await router.OnSourceEventsAsync(new SourceEventsEnvelope { Source = "unrouted" });
        // No assertion beyond "didn't throw" — an envelope for a source nothing subscribes to must be a
        // silent no-op, not an error.
    }
}
