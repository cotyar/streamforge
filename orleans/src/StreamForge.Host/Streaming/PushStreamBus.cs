using System.Collections.Immutable;
using System.Threading.Channels;
using Orleans.Runtime;
using Orleans.Serialization;

namespace StreamForge.Host.Streaming;

/// <summary>
/// One live subscription on the push bus: its own bounded channel, its own pump task, and the delivery
/// target. Exactly one pump per subscription is what preserves per-(stream, subscriber) FIFO ordering —
/// see <see cref="PushStreamBus"/>'s class doc.
/// </summary>
public sealed class PushSubscriptionEntry
{
    public Guid Id { get; init; }
    public StreamId StreamId { get; init; }
    public Channel<object?> Channel { get; init; } = null!;

    /// <summary>The subscriber's own callback, type-erased. Invoked directly for non-grain subscribers
    /// (hosted services, gRPC call scopes); for grain subscribers it is invoked only from inside the
    /// grain's own turn, by <see cref="PushStreamBus.DispatchAsync"/> via <see cref="Target"/>.</summary>
    public Func<object?, Task> Handler { get; init; } = null!;

    /// <summary>Non-null when the subscriber is a grain activation (captured at subscribe time from
    /// <see cref="IGrainContextAccessor"/>).</summary>
    public IGrainContext? Context { get; init; }

    /// <summary>Grain-extension reference used to hop delivery INTO the grain's turn (null for non-grain
    /// subscribers). See PushDeliveryExtension.</summary>
    public IPushDeliveryExtension? Target { get; set; }

    public CancellationTokenSource Cancellation { get; } = new();

    internal long DroppedCount;
    internal long LastDropLogTicks;
    internal Task Pump = Task.CompletedTask;

    public long Dropped => Interlocked.Read(ref DroppedCount);
}

/// <summary>
/// The in-process PUSH transport behind <c>Streams:Transport=push</c> (the alternative to Orleans' stock
/// PULL-based memory streams — see Program.cs's <c>Streams:PullPeriodMs</c> comment and
/// <c>orleans/docs/comparison.html</c>'s "Root cause, found and proven" note).
///
/// WHY THIS EXISTS: memory streams hand every event to a pulling agent that polls its in-memory queue on a
/// timer, so each stream hop adds Uniform(0, pullPeriod) latency; the tableDelta path crosses two hops and
/// measured p50 125ms against Dapr's push transport at 7ms. Nothing about the platform's semantics needs a
/// queue+poll: this flavor is documented as single-silo localhost clustering, so a direct in-process bus is
/// a legitimate transport for it.
///
/// SHAPE: publish is a synchronous, non-blocking <c>TryWrite</c> onto one bounded channel PER SUBSCRIBER;
/// a dedicated pump task per subscriber drains that channel and delivers. Producers never await a consumer.
///
/// DEADLOCK SAFETY (CLAUDE.md hard rule 3): the channel is the decoupling point. A producer grain's turn
/// never awaits a downstream grain call as part of publishing (TableOutputGrain republishing to every
/// downstream TableIngestGrain, TableGrain-over-TableGrain chains, ...), so the orchestrator↔worker call
/// cycles that deadlock non-reentrant grains simply cannot form through the transport. Pump tasks run on
/// the default TaskScheduler with execution context suppressed, so a pump is never mistaken for a
/// continuation of the publishing grain's turn.
///
/// TURN SAFETY: a grain subscriber's callback closes over grain state, so it MUST run inside that grain's
/// own turn — never on the pump thread. The pump therefore delivers to grains with a real grain call
/// (<see cref="IPushDeliveryExtension.DeliverAsync"/>, an Orleans grain extension installed on the
/// subscribing activation), which Orleans queues and dispatches exactly like any other message: honoring
/// non-reentrancy and any <c>[MayInterleave]</c> allowlist, identical to how memory streams deliver to an
/// explicit in-grain subscription today. Non-grain subscribers (StreamBridgeService, the gRPC streaming
/// services) have no turn to enter, so their callbacks run on the pump directly — again exactly what
/// memory streams do for a client-side subscription.
///
/// ORDERING: one channel + one pump per subscription, and the pump awaits each delivery before reading the
/// next item, so per (stream key, subscriber) FIFO holds regardless of how many producers publish
/// concurrently (Channel writes are linearizable). Fan-out is per-subscriber and independent — a slow
/// subscriber cannot reorder or stall another.
///
/// BACKPRESSURE (demo-platform choice, documented deliberately): channels are bounded (default 10k,
/// <c>Streams:PushCapacity</c>) with <see cref="BoundedChannelFullMode.Wait"/> + <c>TryWrite</c>, i.e. a
/// full channel DROPS THE INCOMING ITEM and increments a counter, logged at most once every 5s per
/// subscription. Rationale: publishing must never block a grain turn (that would reintroduce the very
/// coupling this design removes), and an exact drop counter is more honest than silently discarding older
/// items. At the platform's demo rates (~10^3 events/s) a 10k backlog means a consumer roughly ten seconds
/// behind — already a broken system, not a tuning knob.
///
/// ISOLATION: payloads handed to non-grain subscribers are deep-copied (when a <see cref="DeepCopier"/> is
/// available) so subscribers cannot observe each other's mutations — memory streams give the same isolation
/// for free by serializing. Grain deliveries get Orleans' own argument copy on the grain call.
/// </summary>
public sealed class PushStreamBus
{
    private const int DropLogIntervalSeconds = 5;

    private readonly ILogger? _logger;
    private readonly DeepCopier? _copier;
    private readonly int _capacity;

    private readonly object _mutex = new();
    private readonly Dictionary<StreamId, ImmutableArray<PushSubscriptionEntry>> _byStream = [];
    private readonly Dictionary<Guid, PushSubscriptionEntry> _byId = [];

    public PushStreamBus(int capacity = 10_000, ILogger? logger = null, DeepCopier? copier = null)
    {
        _capacity = Math.Max(1, capacity);
        _logger = logger;
        _copier = copier;
    }

    public int Capacity => _capacity;

    /// <summary>Total items dropped across every subscription since process start (bounded-channel
    /// overflow — see class doc's backpressure paragraph).</summary>
    public long TotalDropped
    {
        get
        {
            lock (_mutex)
            {
                long total = 0;
                foreach (var entry in _byId.Values) total += entry.Dropped;
                return total;
            }
        }
    }

    /// <summary>Fan-out to every current subscriber of <paramref name="streamId"/>. Synchronous and
    /// non-blocking by construction — see class doc.</summary>
    public void Publish(StreamId streamId, object? item)
    {
        ImmutableArray<PushSubscriptionEntry> subs;
        lock (_mutex)
        {
            if (!_byStream.TryGetValue(streamId, out subs)) return;
        }

        foreach (var entry in subs)
        {
            if (entry.Channel.Writer.TryWrite(item)) continue;
            OnDropped(entry);
        }
    }

    public PushSubscriptionEntry Subscribe(StreamId streamId, Func<object?, Task> handler, IGrainContext? context)
    {
        var entry = new PushSubscriptionEntry
        {
            Id = Guid.NewGuid(),
            StreamId = streamId,
            Handler = handler,
            Context = context,
            Channel = System.Threading.Channels.Channel.CreateBounded<object?>(new BoundedChannelOptions(_capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            }),
        };

        lock (_mutex)
        {
            _byId[entry.Id] = entry;
            _byStream[streamId] = _byStream.TryGetValue(streamId, out var existing)
                ? existing.Add(entry)
                : [entry];
        }

        _logger?.LogDebug(
            "[push-streams] subscribed {SubscriptionId} to {StreamId} ({Mode})",
            entry.Id, streamId, context is null ? "delegate" : $"grain-turn {context.GrainId}");

        return entry;
    }

    /// <summary>Starts the subscription's pump. Called after the caller has had a chance to install the
    /// grain-extension <see cref="PushSubscriptionEntry.Target"/> (which must happen on the grain's own
    /// thread, inside the subscribing turn).</summary>
    public void StartPump(PushSubscriptionEntry entry)
    {
        // Escape the caller's scheduler AND its ambient execution context: a pump must never look like a
        // continuation of the subscribing grain's turn (see class doc's deadlock-safety paragraph).
        using (ExecutionContext.SuppressFlow())
        {
            entry.Pump = Task.Factory
                .StartNew(() => PumpAsync(entry), CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default)
                .Unwrap();
        }
    }

    /// <summary>
    /// Detaches a subscription: it is removed from routing and from dispatch before this returns, and the
    /// pump stops before its next delivery.
    ///
    /// It deliberately does NOT wait for the pump to finish. Unsubscribing almost always happens inside the
    /// SUBSCRIBING grain's own turn (PipelineGrain.StopAsync, TableGrain.StopAsync, TableIngestGrain.
    /// StopAsync, ...), and at that instant the pump may be awaiting a delivery grain call that Orleans has
    /// queued BEHIND that very turn — waiting for the pump would then block the stop until that call
    /// completes, which it cannot. Cancel-and-walk-away is both correct (the entry is already unreachable
    /// for new publishes and for DispatchAsync) and free of that self-inflicted stall.
    /// </summary>
    public Task UnsubscribeAsync(Guid subscriptionId)
    {
        PushSubscriptionEntry? entry;
        lock (_mutex)
        {
            if (!_byId.Remove(subscriptionId, out entry)) return Task.CompletedTask;
            if (_byStream.TryGetValue(entry.StreamId, out var subs))
            {
                var remaining = subs.Remove(entry);
                if (remaining.IsEmpty) _byStream.Remove(entry.StreamId);
                else _byStream[entry.StreamId] = remaining;
            }
        }

        entry.Channel.Writer.TryComplete();
        entry.Cancellation.Cancel();
        entry.Pump.ContinueWith(
            (_, state) => ((PushSubscriptionEntry)state!).Cancellation.Dispose(),
            entry, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return Task.CompletedTask;
    }

    /// <summary>Drops every subscription owned by a grain activation. Wired to
    /// <see cref="IGrainContext.Deactivated"/> so a deactivated grain's stale closures are never invoked
    /// (mirrors how an explicit memory-stream subscription's in-grain callback dies with the activation).</summary>
    public void RemoveAllFor(IGrainContext context)
    {
        List<Guid> ids;
        lock (_mutex)
        {
            ids = _byId.Values.Where(e => ReferenceEquals(e.Context, context)).Select(e => e.Id).ToList();
        }

        foreach (var id in ids)
        {
            _ = UnsubscribeAsync(id);
        }
    }

    public IReadOnlyList<PushSubscriptionEntry> SubscriptionsFor(StreamId streamId, IGrainContext? context)
    {
        lock (_mutex)
        {
            if (!_byStream.TryGetValue(streamId, out var subs)) return [];
            return subs.Where(e => ReferenceEquals(e.Context, context)).ToList();
        }
    }

    /// <summary>Invoked by <see cref="PushDeliveryExtension"/> from INSIDE the subscribing grain's turn.
    /// The activation identity check makes a stale delivery (subscription registered by a previous
    /// activation of the same grain) a no-op instead of a call into a dead instance's closure.</summary>
    public Task DispatchAsync(Guid subscriptionId, object? item, IGrainContext context)
    {
        PushSubscriptionEntry? entry;
        lock (_mutex)
        {
            _byId.TryGetValue(subscriptionId, out entry);
        }

        if (entry is null) return Task.CompletedTask;
        if (!ReferenceEquals(entry.Context, context))
        {
            _ = UnsubscribeAsync(subscriptionId);
            return Task.CompletedTask;
        }

        return entry.Handler(item);
    }

    private async Task PumpAsync(PushSubscriptionEntry entry)
    {
        var reader = entry.Channel.Reader;
        var token = entry.Cancellation.Token;

        try
        {
            while (await reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                {
                    if (token.IsCancellationRequested) return;

                    try
                    {
                        await DeliverAsync(entry, item).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogWarning(ex, "[push-streams] delivery failed on {StreamId}", entry.StreamId);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // normal shutdown path
        }
        catch (ChannelClosedException)
        {
            // unsubscribed while waiting
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "[push-streams] pump for {StreamId} terminated unexpectedly", entry.StreamId);
        }
    }

    private Task DeliverAsync(PushSubscriptionEntry entry, object? item)
    {
        // Grain subscriber: hop into its turn with a real grain call (see class doc's turn-safety note).
        if (entry.Target is not null) return entry.Target.DeliverAsync(entry.Id, item);

        // Non-grain subscriber: run the callback here, on an isolated copy.
        return entry.Handler(Copy(item));
    }

    private object? Copy(object? item)
    {
        if (item is null || _copier is null) return item;
        try { return _copier.Copy(item); }
        catch { return item; }
    }

    private void OnDropped(PushSubscriptionEntry entry)
    {
        var total = Interlocked.Increment(ref entry.DroppedCount);
        if (_logger is null) return;

        var now = DateTime.UtcNow.Ticks;
        var last = Interlocked.Read(ref entry.LastDropLogTicks);
        if (now - last < TimeSpan.TicksPerSecond * DropLogIntervalSeconds) return;
        if (Interlocked.CompareExchange(ref entry.LastDropLogTicks, now, last) != last) return;

        _logger.LogWarning(
            "[push-streams] subscriber backlog full on {StreamId} (capacity {Capacity}); dropped {Dropped} item(s) so far",
            entry.StreamId, _capacity, total);
    }
}
