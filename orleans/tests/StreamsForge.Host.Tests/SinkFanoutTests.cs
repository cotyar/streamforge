using StreamsForge.AppCore.Sinks;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 014 wave C: unit tests for <see cref="SinkFanout"/> — the one place the "batch client gets one
/// call, everyone else gets N" rule lives, so <c>NatsPublisherService</c> / <c>NatsSinkPublisherService</c>
/// (waves E/F, not this wave) can swap their loop bodies for a single <see cref="SinkFanout.PublishAllAsync{T}"/>
/// call. No mocking framework — hand-written fakes, matching this repo's convention elsewhere in this
/// directory (see e.g. <see cref="TransportRegistryTests"/>'s <c>FizzTransport</c>).
/// </summary>
public class SinkFanoutTests
{
    // ------------------------------------------------------------------
    // Fakes
    // ------------------------------------------------------------------

    /// <summary>A plain <see cref="ISinkClient"/> — no batch capability — that records every
    /// <see cref="PublishAsync{T}"/> call in order, exactly the shape a real <c>NatsSinkClient</c> /
    /// <c>FileSinkClient</c> caller sees from the outside.</summary>
    private sealed class RecordingSinkClient : ISinkClient
    {
        public List<object?> Received { get; } = [];

        public string EntityName => "recording";

        public SinkPublishCounters Counters => new(Received.Count, 0, null, 0);

        public Task PublishAsync<T>(T payload, CancellationToken ct)
        {
            Received.Add(payload);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>An <see cref="IBatchSinkClient"/> that records batch calls separately from single-message
    /// calls, so a test can assert the fan-out picked the batch path and never fell back to the serial
    /// one.</summary>
    private sealed class RecordingBatchSinkClient : IBatchSinkClient
    {
        public List<IReadOnlyList<object?>> Batches { get; } = [];

        public int SinglePublishCalls { get; private set; }

        public string EntityName => "recording-batch";

        public SinkPublishCounters Counters => new(Batches.Sum(b => b.Count), 0, null, 0);

        public Task PublishBatchAsync<T>(IReadOnlyList<T> payloads, CancellationToken ct)
        {
            Batches.Add([.. payloads.Cast<object?>()]);
            return Task.CompletedTask;
        }

        public Task PublishAsync<T>(T payload, CancellationToken ct)
        {
            SinglePublishCalls++;
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>A client whose <see cref="PublishAsync{T}"/> throws — i.e. one that violates
    /// <see cref="ISinkClient"/>'s own "never throws" contract. Used only to pin what
    /// <see cref="SinkFanout.PublishAllAsync{T}"/> does when that contract is broken, which — per this
    /// wave's brief — must match what the four pre-existing call sites do today rather than what would be
    /// "nicer". Records how many calls it actually received before throwing.</summary>
    private sealed class ThrowingSinkClient : ISinkClient
    {
        private readonly int _throwOnCallNumber;

        public ThrowingSinkClient(int throwOnCallNumber = 1) => _throwOnCallNumber = throwOnCallNumber;

        public List<object?> ReceivedBeforeThrow { get; } = [];

        public string EntityName => "throwing";

        public SinkPublishCounters Counters => new(ReceivedBeforeThrow.Count, 0, null, 0);

        public Task PublishAsync<T>(T payload, CancellationToken ct)
        {
            if (ReceivedBeforeThrow.Count + 1 == _throwOnCallNumber)
            {
                throw new InvalidOperationException("simulated contract violation: PublishAsync threw");
            }

            ReceivedBeforeThrow.Add(payload);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // ------------------------------------------------------------------
    // Non-batch client: N serial PublishAsync calls, in order.
    // ------------------------------------------------------------------

    [Fact]
    public async Task NonBatchClient_ReceivesSerialPublishCallsInOrder()
    {
        var client = new RecordingSinkClient();
        var messages = new List<string> { "a", "b", "c" };

        await SinkFanout.PublishAllAsync([client], messages, CancellationToken.None);

        Assert.Equal<object?>(messages, client.Received);
    }

    // ------------------------------------------------------------------
    // Batch client: exactly one PublishBatchAsync call, zero PublishAsync calls.
    // ------------------------------------------------------------------

    [Fact]
    public async Task BatchClient_ReceivesExactlyOneBatchCallWithAllMessages()
    {
        var client = new RecordingBatchSinkClient();
        var messages = new List<string> { "a", "b", "c" };

        await SinkFanout.PublishAllAsync([client], messages, CancellationToken.None);

        var batch = Assert.Single(client.Batches);
        Assert.Equal<object?>(messages, batch);
        Assert.Equal(0, client.SinglePublishCalls);
    }

    // ------------------------------------------------------------------
    // Mixed client list: both treatments in one fan-out.
    // ------------------------------------------------------------------

    [Fact]
    public async Task MixedClientList_EachClientGetsItsOwnTreatment()
    {
        var plain = new RecordingSinkClient();
        var batch = new RecordingBatchSinkClient();
        var messages = new List<string> { "a", "b", "c" };

        await SinkFanout.PublishAllAsync([plain, batch], messages, CancellationToken.None);

        Assert.Equal<object?>(messages, plain.Received);
        var onlyBatch = Assert.Single(batch.Batches);
        Assert.Equal<object?>(messages, onlyBatch);
        Assert.Equal(0, batch.SinglePublishCalls);
    }

    // ------------------------------------------------------------------
    // A throwing client — pinning the ACTUAL discipline of the four call sites this wave replaces, not an
    // idealized one. Read NatsPublisherService.SubscribePipelineAsync/SubscribeTableAsync and
    // NatsSinkPublisherService.OnPipelineResultsAsync/OnTableDeltaAsync: none of the four wraps its
    // PublishAsync call in a try/catch. They rely entirely on ISinkClient.PublishAsync's own "never
    // throws" contract, so a client that breaks that contract propagates the exception straight out —
    // and, because that exception unwinds the whole call, clients not yet reached are simply never
    // called. SinkFanout reproduces exactly that: no try/catch of its own.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ThrowingClient_PropagatesAndStopsRemainingClients_MatchingTheExistingCallSites()
    {
        var throwing = new ThrowingSinkClient(throwOnCallNumber: 1);
        var next = new RecordingSinkClient();
        var messages = new List<string> { "a", "b" };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => SinkFanout.PublishAllAsync([throwing, next], messages, CancellationToken.None));

        // The throw happened on the first client's first message — it never got to record "a", and the
        // second client (later in the list) was never reached at all.
        Assert.Empty(throwing.ReceivedBeforeThrow);
        Assert.Empty(next.Received);
    }

    [Fact]
    public async Task ThrowingClient_StopsAfterItsOwnPriorMessagesOnTheSameClient()
    {
        // Throws on its second call — the first message it was NOT the one that failed, so a serial
        // (non-batch) client keeps whatever it already accepted before the exception unwound the loop.
        var throwing = new ThrowingSinkClient(throwOnCallNumber: 2);
        var messages = new List<string> { "a", "b", "c" };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => SinkFanout.PublishAllAsync([throwing], messages, CancellationToken.None));

        Assert.Equal<object?>(["a"], throwing.ReceivedBeforeThrow);
    }

    // ------------------------------------------------------------------
    // Empty inputs are no-ops.
    // ------------------------------------------------------------------

    [Fact]
    public async Task EmptyMessageList_IsANoOp()
    {
        var client = new RecordingSinkClient();

        await SinkFanout.PublishAllAsync([client], new List<string>(), CancellationToken.None);

        Assert.Empty(client.Received);
    }

    [Fact]
    public async Task EmptyClientList_IsANoOp()
    {
        // No client to observe — this only asserts it does not throw and completes.
        await SinkFanout.PublishAllAsync([], new List<string> { "a" }, CancellationToken.None);
    }
}
