namespace StreamForge.Dapr.Host.Streaming;

/// <summary>
/// Pure per-key rate limiter mirroring orleans/src/StreamForge.Host/Services/StreamBridgeService.cs's
/// <c>SourceRelayMinIntervalMs</c> constant (50ms minimum interval between relayed sends for a given
/// source name, i.e. a ~20 msg/s cap): a key's first call always relays; a subsequent call for the same
/// key relays only if at least <see cref="MinIntervalMs"/> milliseconds have elapsed since the last
/// relayed call for that key, and every call (relayed or not) that observes enough elapsed time updates
/// the "last relayed" timestamp.
///
/// Deliberately framework-free (no SignalR/Dapr/DateTime.UtcNow hard dependency baked in) so the
/// sampling DECISION is unit-testable with an injected clock, independent of any hub or wall-clock
/// timing flakiness — see StreamingSourceRateSamplerTests.
///
/// <para><b>Per-event vs per-batch sampling (design note):</b> the Orleans bridge subscribes a stream
/// of individual <c>EventRecord</c> items — one item, one sampling decision, one potential SignalR send.
/// The Dapr flavor's <see cref="StreamForge.Abstractions.Streaming.SourceEventsEnvelope"/> instead
/// carries a whole tick's batch of events for one source. To keep OBSERVABLE behavior equivalent (same
/// ~20 msg/s ceiling per source, same "sourceEvent" per-event SignalR shape) rather than merely similar,
/// <see cref="DaprStreamBridge.OnSourceEventsAsync"/> iterates the batch and calls
/// <see cref="ShouldRelay"/> once per event, not once per batch — a single call keyed off the whole
/// batch would either (a) send/drop the entire batch together, which is a materially different
/// admission curve than per-event sampling once ticks batch more than one event, or (b) require an
/// arbitrary "one representative event per batch" rule with no Orleans equivalent to justify it.</para>
/// </summary>
public sealed class SourceRateSampler(Func<DateTime>? clock = null)
{
    /// <summary>Same value as Orleans' StreamBridgeService.SourceRelayMinIntervalMs.</summary>
    public const double MinIntervalMs = 50;

    private readonly Func<DateTime> _now = clock ?? (() => DateTime.UtcNow);
    private readonly Dictionary<string, DateTime> _lastRelayed = new();

    /// <summary>Returns true if a send for <paramref name="key"/> should happen now — and, only in that
    /// case, records this instant as the new "last relayed" timestamp for <paramref name="key"/>.
    /// Returns false (recording nothing) if fewer than <see cref="MinIntervalMs"/> milliseconds have
    /// elapsed since the last relayed call for the same key.</summary>
    public bool ShouldRelay(string key)
    {
        var now = _now();
        if (_lastRelayed.TryGetValue(key, out var last) && (now - last).TotalMilliseconds < MinIntervalMs)
        {
            return false;
        }

        _lastRelayed[key] = now;
        return true;
    }
}
