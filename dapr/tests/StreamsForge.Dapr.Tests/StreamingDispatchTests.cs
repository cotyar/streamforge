using StreamsForge.Abstractions.Streaming;
using StreamsForge.Dapr.Host.Streaming;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 W5-B: proves the fan-out contract Streaming/Sinks.cs documents — every registered
/// <see cref="ISourceEventsSink"/>/<see cref="ITableDeltaSink"/> observes the SAME envelope instance,
/// in registration order, none skipped — using StreamingRuntimeSetup's extracted Dispatch* methods
/// directly (no HTTP/DI-container involved; those methods are exactly what the sf-sources/sf-table-delta
/// endpoint lambdas call after normalization).
/// </summary>
public class StreamingDispatchTests
{
    private sealed class FakeSourceSink : ISourceEventsSink
    {
        public List<SourceEventsEnvelope> Received { get; } = [];

        public Task OnSourceEventsAsync(SourceEventsEnvelope envelope)
        {
            Received.Add(envelope);
            return Task.CompletedTask;
        }
    }

    private sealed class FakeTableDeltaSink : ITableDeltaSink
    {
        public List<TableDeltaEnvelope> Received { get; } = [];

        public Task OnTableDeltaAsync(TableDeltaEnvelope envelope)
        {
            Received.Add(envelope);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task DispatchSourceEventsAsync_MultipleRegisteredSinks_AllReceiveTheEnvelope()
    {
        var sinkA = new FakeSourceSink();
        var sinkB = new FakeSourceSink();
        var envelope = new SourceEventsEnvelope { Source = "trades", Events = [new() { ["symbol"] = "AAPL" }] };

        await StreamingRuntimeSetup.DispatchSourceEventsAsync(envelope, [sinkA, sinkB]);

        Assert.Single(sinkA.Received);
        Assert.Same(envelope, sinkA.Received[0]);
        Assert.Single(sinkB.Received);
        Assert.Same(envelope, sinkB.Received[0]);
    }

    [Fact]
    public async Task DispatchSourceEventsAsync_NoRegisteredSinks_CompletesWithoutError()
    {
        var envelope = new SourceEventsEnvelope { Source = "trades" };

        await StreamingRuntimeSetup.DispatchSourceEventsAsync(envelope, []);
        // No assertion beyond "didn't throw" — an empty sink list (a host wired with no consumers at
        // all) must not be an error condition.
    }

    [Fact]
    public async Task DispatchTableDeltaAsync_MultipleRegisteredSinks_AllReceiveTheEnvelope()
    {
        var sinkA = new FakeTableDeltaSink();
        var sinkB = new FakeTableDeltaSink();
        var sinkC = new FakeTableDeltaSink();
        var envelope = new TableDeltaEnvelope { Table = "positions", Seq = 7 };

        await StreamingRuntimeSetup.DispatchTableDeltaAsync(envelope, [sinkA, sinkB, sinkC]);

        Assert.Same(envelope, Assert.Single(sinkA.Received));
        Assert.Same(envelope, Assert.Single(sinkB.Received));
        Assert.Same(envelope, Assert.Single(sinkC.Received));
    }

    [Fact]
    public async Task DispatchSourceEventsAsync_OneSinkThrows_SubsequentSinksAreNotInvoked()
    {
        // Documents present behavior (sequential await, no per-sink try/catch) rather than prescribing
        // it — a future wave that wants isolation between sinks (one misbehaving consumer shouldn't
        // block another) would add that at this exact seam.
        var before = new FakeSourceSink();
        var throwing = new ThrowingSourceSink();
        var after = new FakeSourceSink();
        var envelope = new SourceEventsEnvelope { Source = "trades" };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            StreamingRuntimeSetup.DispatchSourceEventsAsync(envelope, [before, throwing, after]));

        Assert.Single(before.Received);
        Assert.Empty(after.Received);
    }

    private sealed class ThrowingSourceSink : ISourceEventsSink
    {
        public Task OnSourceEventsAsync(SourceEventsEnvelope envelope) =>
            throw new InvalidOperationException("simulated sink failure");
    }
}
