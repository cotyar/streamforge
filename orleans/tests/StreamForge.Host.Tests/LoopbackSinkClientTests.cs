using StreamForge.Abstractions;
using StreamForge.AppCore.Sinks;
using StreamForge.Host.Generators;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Wishlist item 9(b): unit tests for <see cref="LoopbackSinkClient"/> and <see cref="LoopbackHub"/> —
/// no Orleans, no TestCluster, no grain: <see cref="LoopbackHub"/> is a plain in-process static registry,
/// so it (and the sink that writes to it) is fully testable by attaching/draining it directly, exactly
/// the way <c>HttpSinkClientTests</c> exercises <c>HttpSinkClient</c> against a stub HTTP server. Proves
/// three things the wishlist explicitly asks for: (1) the maxDepth guard behaves IDENTICALLY to the HTTP
/// sink's (same shared <see cref="SinkStepGuard"/>), (2) an unattached target is a reported failure, never
/// silent, (3) delivery is a genuine in-process hand-off (no serialization: the exact same
/// <c>Dictionary&lt;string, object?&gt;</c> reference reaches the reader for reference-typed values,
/// proving there is no JSON round trip in the middle). The end-to-end proof — a real
/// <c>GeneratorGrain</c> draining this hub onto a real stream, plus the required unbounded-cycle test —
/// lives in <c>LoopbackCycleTests.cs</c> in this same directory.
/// </summary>
public class LoopbackSinkClientTests
{
    // ------------------------------------------------------------------
    // Basic delivery — no HTTP, no serialization.
    // ------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_WritesDirectlyIntoTheHub_ForAnAttachedTarget()
    {
        var target = "loop_target_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(target);
        try
        {
            await using var client = new LoopbackSinkClient(
                new LoopbackSinkConfig { TargetSourceName = target }, "table", "t1");

            await client.PublishAsync(new NatsTableDeltaMessage { Table = "t1", Row = new() { ["x"] = 1L }, Weight = 1L }, CancellationToken.None);

            var drained = LoopbackHub.Drain(target, 10);
            Assert.Single(drained);
            Assert.Equal(1L, drained[0]["x"]);
            Assert.Equal(1L, drained[0]["_weight"]); // stamped by SinkStepGuard.RowOf from the message's Weight

            Assert.Equal(1, client.Counters.Published);
            Assert.Equal(0, client.Counters.Failed);
        }
        finally
        {
            LoopbackHub.Detach(target);
        }
    }

    [Fact]
    public async Task PublishAsync_ExpandsNameInTargetSourceName()
    {
        var target = "loop_named_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(target);
        try
        {
            await using var client = new LoopbackSinkClient(
                new LoopbackSinkConfig { TargetSourceName = "{name}" }, "table", target);

            await client.PublishAsync(new NatsPipelineRowMessage { PipelineId = "p", Row = new() { ["y"] = 2L } }, CancellationToken.None);

            Assert.Equal(target, client.TargetSourceName);
            var drained = LoopbackHub.Drain(target, 10);
            Assert.Single(drained);
        }
        finally
        {
            LoopbackHub.Detach(target);
        }
    }

    // ------------------------------------------------------------------
    // Unattached target — observable failure, never silent.
    // ------------------------------------------------------------------

    [Fact]
    public async Task PublishAsync_AgainstAnUnattachedTarget_NeverThrowsButCountsAFailure()
    {
        var target = "loop_missing_" + Guid.NewGuid().ToString("n")[..8]; // never Attach'd
        await using var client = new LoopbackSinkClient(new LoopbackSinkConfig { TargetSourceName = target }, "table", "t1");

        await client.PublishAsync(new NatsTableDeltaMessage { Table = "t1", Row = new() { ["x"] = 1L } }, CancellationToken.None);

        Assert.Equal(0, client.Counters.Published);
        Assert.Equal(1, client.Counters.Failed);
        Assert.NotNull(client.Counters.LastError);
        Assert.Contains(target, client.Counters.LastError);
    }

    [Fact]
    public async Task PublishAsync_AfterDetach_IsAlsoAReportedFailure()
    {
        var target = "loop_detached_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(target);
        LoopbackHub.Detach(target); // stopped before the sink ever gets to publish

        await using var client = new LoopbackSinkClient(new LoopbackSinkConfig { TargetSourceName = target }, "table", "t1");
        await client.PublishAsync(new NatsTableDeltaMessage { Table = "t1", Row = new() { ["x"] = 1L } }, CancellationToken.None);

        Assert.Equal(1, client.Counters.Failed);
    }

    // ------------------------------------------------------------------
    // Wishlist #9: the maxDepth guard, reused verbatim from the HTTP sink's SinkStepGuard.
    // ------------------------------------------------------------------

    [Fact]
    public async Task MaxDepth_DropsARowWhoseStepHasReachedTheBound_WithoutWritingToTheHub()
    {
        var target = "loop_guard_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(target);
        try
        {
            await using var client = new LoopbackSinkClient(
                new LoopbackSinkConfig { TargetSourceName = target, MaxDepth = 3 }, "table", "loop");

            await client.PublishAsync(new NatsTableDeltaMessage { Table = "loop", Row = new() { ["step"] = 3L } }, CancellationToken.None);

            Assert.Empty(LoopbackHub.Drain(target, 10));
            Assert.Equal(0, client.Counters.Published);
            Assert.Equal(1, client.Counters.Failed);
            Assert.Contains("maxDepth", client.Counters.LastError, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            LoopbackHub.Detach(target);
        }
    }

    [Fact]
    public async Task MaxDepth_AllowsARowBelowTheBound()
    {
        var target = "loop_guard_ok_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(target);
        try
        {
            await using var client = new LoopbackSinkClient(
                new LoopbackSinkConfig { TargetSourceName = target, MaxDepth = 3 }, "table", "loop");

            await client.PublishAsync(new NatsTableDeltaMessage { Table = "loop", Row = new() { ["step"] = 2L } }, CancellationToken.None);

            Assert.Single(LoopbackHub.Drain(target, 10));
            Assert.Equal(1, client.Counters.Published);
            Assert.Equal(0, client.Counters.Failed);
        }
        finally
        {
            LoopbackHub.Detach(target);
        }
    }

    [Fact]
    public async Task MaxDepth_ZeroMeansTheGuardIsOff_EvenForAHugeStep()
    {
        var target = "loop_guard_off_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(target);
        try
        {
            await using var client = new LoopbackSinkClient(new LoopbackSinkConfig { TargetSourceName = target }, "table", "loop");

            await client.PublishAsync(new NatsTableDeltaMessage { Table = "loop", Row = new() { ["step"] = 1_000_000L } }, CancellationToken.None);

            Assert.Single(LoopbackHub.Drain(target, 10));
            Assert.Equal(1, client.Counters.Published);
        }
        finally
        {
            LoopbackHub.Detach(target);
        }
    }

    [Fact]
    public async Task MaxDepth_ARowWithNoStepField_IsNotDropped()
    {
        var target = "loop_guard_nostep_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(target);
        try
        {
            await using var client = new LoopbackSinkClient(
                new LoopbackSinkConfig { TargetSourceName = target, MaxDepth = 1 }, "table", "loop");

            await client.PublishAsync(new NatsTableDeltaMessage { Table = "loop", Row = new() { ["symbol"] = "AAPL" } }, CancellationToken.None);

            Assert.Single(LoopbackHub.Drain(target, 10));
            Assert.Equal(1, client.Counters.Published);
        }
        finally
        {
            LoopbackHub.Detach(target);
        }
    }

    [Fact]
    public async Task MaxDepth_HonorsACustomStepFieldName()
    {
        var target = "loop_guard_custom_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(target);
        try
        {
            await using var client = new LoopbackSinkClient(
                new LoopbackSinkConfig { TargetSourceName = target, MaxDepth = 2, StepField = "iteration" }, "table", "loop");

            await client.PublishAsync(
                new NatsTableDeltaMessage { Table = "loop", Row = new() { ["step"] = 99L, ["iteration"] = 2L } },
                CancellationToken.None);

            // "step" is way past 2 but irrelevant — only "iteration" is the configured counter, and it's
            // AT the bound, so this row is dropped.
            Assert.Empty(LoopbackHub.Drain(target, 10));
            Assert.Equal(1, client.Counters.Failed);
        }
        finally
        {
            LoopbackHub.Detach(target);
        }
    }
}

/// <summary>Wishlist #9(b): direct coverage of <see cref="LoopbackHub"/>'s own contract (Attach/Detach/
/// TryPublish/Drain), independent of any sink client.</summary>
public class LoopbackHubTests
{
    [Fact]
    public void TryPublish_ReturnsFalse_WhenNothingIsAttached()
    {
        var name = "hub_unattached_" + Guid.NewGuid().ToString("n")[..8];
        Assert.False(LoopbackHub.TryPublish(name, new Dictionary<string, object?> { ["x"] = 1 }));
    }

    [Fact]
    public void Attach_ThenTryPublish_ThenDrain_RoundTripsTheRow()
    {
        var name = "hub_roundtrip_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(name);
        try
        {
            var row = new Dictionary<string, object?> { ["x"] = 42L };
            Assert.True(LoopbackHub.TryPublish(name, row));

            var drained = LoopbackHub.Drain(name, 10);
            Assert.Single(drained);
            // Same reference reaches the reader — no serialize/deserialize round trip in between.
            Assert.Same(row, drained[0]);
        }
        finally
        {
            LoopbackHub.Detach(name);
        }
    }

    [Fact]
    public void Drain_ReturnsRowsInFifoOrder_AndLeavesTheRestForNextTime()
    {
        var name = "hub_fifo_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(name);
        try
        {
            for (var i = 0; i < 10; i++)
            {
                Assert.True(LoopbackHub.TryPublish(name, new Dictionary<string, object?> { ["i"] = (long)i }));
            }

            var firstBatch = LoopbackHub.Drain(name, 4);
            Assert.Equal([0L, 1L, 2L, 3L], firstBatch.Select(r => r["i"]));

            var secondBatch = LoopbackHub.Drain(name, 100);
            Assert.Equal([4L, 5L, 6L, 7L, 8L, 9L], secondBatch.Select(r => r["i"]));

            Assert.Empty(LoopbackHub.Drain(name, 10)); // nothing left
        }
        finally
        {
            LoopbackHub.Detach(name);
        }
    }

    [Fact]
    public void Detach_CompletesTheChannel_AndFurtherPublishesFail()
    {
        var name = "hub_detach_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(name);
        LoopbackHub.Detach(name);

        Assert.False(LoopbackHub.TryPublish(name, new Dictionary<string, object?> { ["x"] = 1 }));
        Assert.Empty(LoopbackHub.Drain(name, 10));
    }

    [Fact]
    public void ReAttach_ReplacesTheChannel_DroppingWhateverTheStaleOneStillHeld()
    {
        var name = "hub_reattach_" + Guid.NewGuid().ToString("n")[..8];
        LoopbackHub.Attach(name);
        try
        {
            Assert.True(LoopbackHub.TryPublish(name, new Dictionary<string, object?> { ["stale"] = true }));

            LoopbackHub.Attach(name); // idempotent-ish replace, mirrors StartAsync's own convention

            Assert.Empty(LoopbackHub.Drain(name, 10)); // the stale row is gone, not carried over
            Assert.True(LoopbackHub.TryPublish(name, new Dictionary<string, object?> { ["fresh"] = true }));
            Assert.Single(LoopbackHub.Drain(name, 10));
        }
        finally
        {
            LoopbackHub.Detach(name);
        }
    }
}
