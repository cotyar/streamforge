using System.Collections.Concurrent;
using Orleans.Runtime;
using StreamsForge.Abstractions;
using StreamsForge.Host.Streaming;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Unit tests for the <c>Streams:Transport=push</c> transport core (StreamsForge.Host.Streaming.
/// PushStreamBus) — the in-process alternative to Orleans' pull-based memory streams. These exercise the
/// bus directly with non-grain (delegate) subscribers, which is the same delivery path StreamBridgeService
/// and the gRPC streaming services take; the grain-turn delivery path is covered end-to-end by
/// PushStreamClusterTests.
/// </summary>
public sealed class PushStreamBusTests
{
    private static readonly StreamId Trades = StreamId.Create(StreamConstants.SourcesNamespace, "trades");
    private static readonly StreamId Quotes = StreamId.Create(StreamConstants.SourcesNamespace, "quotes");
    private static readonly StreamId TradesDelta = StreamId.Create(StreamConstants.TableDeltaNamespace, "trades");

    private static async Task<bool> WaitForAsync(Func<bool> condition, int timeoutMs = 10_000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            await Task.Delay(10);
        }
        return condition();
    }

    private static (PushStreamBus Bus, ConcurrentQueue<object?> Seen, PushSubscriptionEntry Entry) SubscribeRecorder(
        StreamId streamId, int capacity = 4096, Func<Task>? gate = null)
    {
        var bus = new PushStreamBus(capacity);
        var seen = new ConcurrentQueue<object?>();
        var entry = bus.Subscribe(streamId, async item =>
        {
            if (gate is not null) await gate();
            seen.Enqueue(item);
        }, context: null);
        bus.StartPump(entry);
        return (bus, seen, entry);
    }

    [Fact]
    public async Task Publish_DeliversEveryItemInFifoOrder()
    {
        var (bus, seen, _) = SubscribeRecorder(Trades);

        for (var i = 0; i < 1000; i++) bus.Publish(Trades, i);

        Assert.True(await WaitForAsync(() => seen.Count >= 1000), $"only {seen.Count} of 1000 items delivered");
        Assert.Equal(Enumerable.Range(0, 1000), seen.Cast<int>());
        Assert.Equal(0, bus.TotalDropped);
    }

    /// <summary>Per-key ordering under CONCURRENT producers: interleaving between producers is inherent
    /// (and is what memory streams do too), but each producer's own sequence must arrive monotonically —
    /// that is what "one channel + one pump per subscriber" buys, and what a shared unordered worker pool
    /// would break.</summary>
    [Fact]
    public async Task ConcurrentPublishers_PreserveEachProducersOrder()
    {
        const int producers = 6;
        const int perProducer = 500;
        var (bus, seen, _) = SubscribeRecorder(Trades, capacity: 16_384);

        await Task.WhenAll(Enumerable.Range(0, producers).Select(p => Task.Run(() =>
        {
            for (var i = 0; i < perProducer; i++) bus.Publish(Trades, (p, i));
        })));

        Assert.True(await WaitForAsync(() => seen.Count >= producers * perProducer),
            $"only {seen.Count} of {producers * perProducer} items delivered");
        Assert.Equal(0, bus.TotalDropped);

        var lastSeen = new int[producers];
        Array.Fill(lastSeen, -1);
        foreach (var (producer, seq) in seen.Cast<(int, int)>())
        {
            Assert.Equal(lastSeen[producer] + 1, seq);
            lastSeen[producer] = seq;
        }
        Assert.All(lastSeen, last => Assert.Equal(perProducer - 1, last));
    }

    [Fact]
    public async Task Publish_FansOutToEverySubscriberIndependently()
    {
        var bus = new PushStreamBus();
        var sinks = new List<ConcurrentQueue<object?>>();
        for (var s = 0; s < 3; s++)
        {
            var sink = new ConcurrentQueue<object?>();
            sinks.Add(sink);
            var entry = bus.Subscribe(Trades, item => { sink.Enqueue(item); return Task.CompletedTask; }, context: null);
            bus.StartPump(entry);
        }

        for (var i = 0; i < 200; i++) bus.Publish(Trades, i);

        Assert.True(await WaitForAsync(() => sinks.All(s => s.Count >= 200)));
        foreach (var sink in sinks) Assert.Equal(Enumerable.Range(0, 200), sink.Cast<int>());
    }

    /// <summary>Routing is keyed by the full StreamId (namespace + key) exactly like Orleans' own — a
    /// subscriber to ("sources","trades") must never see ("sources","quotes") or
    /// ("table-delta","trades") traffic.</summary>
    [Fact]
    public async Task Publish_NeverCrossesNamespaceOrKey()
    {
        var (bus, seen, _) = SubscribeRecorder(Trades);

        bus.Publish(Quotes, "wrong-key");
        bus.Publish(TradesDelta, "wrong-namespace");
        bus.Publish(StreamId.Create(StreamConstants.LifecycleNamespace, StreamConstants.LifecycleEventsKey), "wrong-both");
        bus.Publish(Trades, "right");

        Assert.True(await WaitForAsync(() => seen.Count >= 1));
        await Task.Delay(100); // give any (incorrect) extra delivery a chance to show up
        Assert.Equal(["right"], seen.Cast<string>());
    }

    [Fact]
    public async Task Unsubscribe_StopsDelivery()
    {
        var (bus, seen, entry) = SubscribeRecorder(Trades);

        bus.Publish(Trades, 1);
        Assert.True(await WaitForAsync(() => seen.Count == 1));

        await bus.UnsubscribeAsync(entry.Id);
        for (var i = 0; i < 50; i++) bus.Publish(Trades, i);

        await Task.Delay(150);
        Assert.Single(seen);
    }

    /// <summary>Bounded-channel overflow policy (documented in PushStreamBus's class doc): publishing NEVER
    /// blocks a producer's turn — a full subscriber backlog drops the INCOMING item and counts it, leaving
    /// the retained items a clean FIFO prefix.</summary>
    [Fact]
    public async Task BoundedBacklog_DropsIncomingItemsAndCountsThem()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = true;

        var bus = new PushStreamBus(capacity: 4);
        var seen = new ConcurrentQueue<object?>();
        var entry = bus.Subscribe(Trades, async item =>
        {
            if (first)
            {
                first = false;
                entered.TrySetResult();
                await release.Task;
            }
            seen.Enqueue(item);
        }, context: null);
        bus.StartPump(entry);

        // Item 0 is picked up by the pump, which then blocks inside the handler.
        bus.Publish(Trades, 0);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // 1..4 fill the 4-slot backlog; 5..19 have nowhere to go.
        for (var i = 1; i <= 19; i++) bus.Publish(Trades, i);

        Assert.Equal(15, entry.Dropped);
        Assert.Equal(15, bus.TotalDropped);

        release.SetResult();
        Assert.True(await WaitForAsync(() => seen.Count >= 5));
        await Task.Delay(100);
        Assert.Equal([0, 1, 2, 3, 4], seen.Cast<int>());
    }

    [Fact]
    public async Task Publish_WithNoSubscribers_IsANoOp()
    {
        var bus = new PushStreamBus();
        bus.Publish(Trades, 1);

        var seen = new ConcurrentQueue<object?>();
        var entry = bus.Subscribe(Trades, item => { seen.Enqueue(item); return Task.CompletedTask; }, context: null);
        bus.StartPump(entry);
        bus.Publish(Trades, 2);

        Assert.True(await WaitForAsync(() => seen.Count >= 1));
        await Task.Delay(100);
        Assert.Equal([2], seen.Cast<int>()); // the pre-subscription publish is gone, not replayed
    }
}
