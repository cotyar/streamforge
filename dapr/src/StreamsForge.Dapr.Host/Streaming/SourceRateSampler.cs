namespace StreamsForge.Dapr.Host.Streaming;

/// <summary>Which of the three things <see cref="SourceRateSampler.Evaluate"/> tells the bridge to do
/// with an event: relay it immediately, relay it after waiting out the rest of the pacing slot, or drop
/// it outright (only once a source has been trailing the cap long enough — see
/// <see cref="SourceRateSampler.MaxPacedStreak"/>).</summary>
public enum RelayDecision
{
    SendNow,
    SendAfterDelay,
    Drop,
}

/// <summary>The result of one <see cref="SourceRateSampler.Evaluate"/> call: what to do, and — only for
/// <see cref="RelayDecision.SendAfterDelay"/> — how many milliseconds to wait first. Zero for the other
/// two decisions.</summary>
public readonly record struct RelayPlan(RelayDecision Decision, double DelayMs);

/// <summary>
/// Pure per-key relay PACER mirroring orleans/src/StreamsForge.Host/Services/StreamBridgeService.cs's
/// <c>SubscribeToSourceAsync</c> pacing rule (decision D5, plan 023): a too-early event no longer DROPS —
/// it WAITS OUT the remainder of its 50ms slot and is then sent, so a burst (the normal shape of a polled
/// source, one poll cycle emitting every row of a file in a tight loop) relays in full, in order, merely
/// spread over time, instead of reaching an operator's live tape as one or two rows with the rest simply
/// missing. Only a SUSTAINED producer that stays ahead of the cap for <see cref="MaxPacedStreak"/>
/// consecutive events in a row (~2s of accumulated lag at the 50ms slot) degrades to the OLD drop
/// behavior, so this relay's own backlog cannot grow unbounded trailing a firehose.
///
/// <para><b>Decision, not action:</b> this class is a pure function of "now" and its own per-key state —
/// it never calls <c>Task.Delay</c> itself. <see cref="DaprStreamBridge.OnSourceEventsAsync"/> is what
/// actually awaits <see cref="RelayPlan.DelayMs"/> before sending, exactly as the Orleans side awaits
/// inside its own stream subscription callback (see that class's method for why awaiting IN the callback
/// is what makes the pacing apply back-pressure to this relay alone, never reordering what reaches the
/// hub). Keeping the decision pure — no clock, no I/O, no framework type — is what makes <see cref="Evaluate"/>
/// unit-testable deterministically with an injected clock, independent of any hub or real-time sleep; see
/// StreamingSourceRateSamplerTests.</para>
///
/// <para><b>Predicted send instant, not observed one (a deliberate Orleans deviation, forced by purity):</b>
/// the Orleans side records its "last sent at" timestamp AFTER <c>await Task.Delay</c> actually returns —
/// i.e. the real wall-clock instant the send happened. This class has no way to be told "the delay you
/// asked for is done now" (the bridge owns the delay, not this class), so instead it records the PREDICTED
/// instant the delayed send will occur — <c>now + remaining</c> — at decision time, and the next call for
/// the same key paces against that. Given `Task.Delay`'s scheduling is only ever a small amount looser than
/// requested, never tighter, this prediction is a lower bound on the real send instant, which if anything
/// makes this side SLIGHTLY more conservative (a hair more likely to pace or drop) than Orleans under the
/// exact same load — never less. It is also exactly what makes the decision testable without a real sleep:
/// a test can call <see cref="Evaluate"/> repeatedly against a <see cref="FakeClock"/>-style injected clock
/// that never itself advances during an unslept "delay", and get the same decisions a real run would.</para>
///
/// <para><b>Per-event vs per-batch pacing (design note, carried over from the old sampler):</b> the Orleans
/// bridge subscribes a stream of individual <c>EventRecord</c> items — one item, one pacing decision, one
/// potential SignalR send. The Dapr flavor's <see cref="StreamsForge.Abstractions.Streaming.SourceEventsEnvelope"/>
/// instead carries a whole tick's batch of events for one source. To keep OBSERVABLE behavior equivalent
/// (same ~20 msg/s steady-state ceiling per source, same "sourceEvent" per-event SignalR shape) rather than
/// merely similar, <see cref="DaprStreamBridge.OnSourceEventsAsync"/> iterates the batch and calls
/// <see cref="Evaluate"/> once per event, not once per batch.</para>
///
/// <para><b>Thread safety (found live, W7-A — carried over unchanged from the old sampler):</b>
/// <see cref="DaprStreamBridge"/> is a DI singleton, so a single <see cref="SourceRateSampler"/> instance
/// is shared across every concurrent <c>sf-sources</c> pub/sub HTTP callback the Dapr sidecar delivers, and
/// enough sources tick concurrently in practice that overlapping requests are not rare. Plain
/// <see cref="Dictionary{TKey, TValue}"/> is not thread-safe: concurrent <see cref="Evaluate"/> calls racing
/// on the same key corrupted the dictionary's internal bucket array under load, observed live as an
/// intermittent <c>IndexOutOfRangeException</c> inside <c>Dictionary.set_Item</c> that took the whole
/// <c>sf-sources</c> dispatch down for that request. Fixed with a single lock around the read-then-write
/// below — contention is negligible (the critical section is a couple of dictionary lookups/assignments,
/// not I/O) and correctness matters far more than lock-free cleverness here.</para>
/// </summary>
public sealed class SourceRateSampler(Func<DateTime>? clock = null)
{
    /// <summary>Same value as Orleans' StreamBridgeService.SourceRelayMinIntervalMs — a ~20 msg/s cap.</summary>
    public const double MinIntervalMs = 50;

    /// <summary>Same value as Orleans' StreamBridgeService.SourceRelayMaxPacedStreak — the number of
    /// CONSECUTIVE paced (delayed, not dropped) events for one key before this degrades to dropping,
    /// i.e. ~2s of accumulated lag at the 50ms slot.</summary>
    public const int MaxPacedStreak = 40;

    private readonly Func<DateTime> _now = clock ?? (() => DateTime.UtcNow);
    private readonly Dictionary<string, DateTime> _lastSend = new();
    private readonly Dictionary<string, int> _pacedStreak = new();
    private readonly object _gate = new();

    /// <summary>Decides what to do with the NEXT event for <paramref name="key"/> and updates this key's
    /// state accordingly (see the class doc's "predicted send instant" note for exactly what "last send"
    /// means once a delay is involved). Never blocks — the caller performs any wait.</summary>
    public RelayPlan Evaluate(string key)
    {
        var now = _now();
        lock (_gate)
        {
            var remaining = _lastSend.TryGetValue(key, out var last)
                ? MinIntervalMs - (now - last).TotalMilliseconds
                : 0;

            if (remaining > 0)
            {
                // A producer that stays ahead of the cap indefinitely would otherwise make this relay
                // trail further and further behind reality, one 50ms slot at a time, with no bound. Past
                // the streak cap we drop instead — degrading to exactly the OLD sampling behavior — until
                // an event finally arrives owing no delay, which resets the streak below.
                var streak = _pacedStreak.GetValueOrDefault(key);
                if (streak >= MaxPacedStreak)
                {
                    return new RelayPlan(RelayDecision.Drop, 0);
                }

                _pacedStreak[key] = streak + 1;
                _lastSend[key] = now.AddMilliseconds(remaining);
                return new RelayPlan(RelayDecision.SendAfterDelay, remaining);
            }

            _pacedStreak.Remove(key);
            _lastSend[key] = now;
            return new RelayPlan(RelayDecision.SendNow, 0);
        }
    }

    /// <summary>Drops <paramref name="key"/>'s pacing state entirely — mirrors Orleans'
    /// <c>UnsubscribeFromSourceAsync</c> clearing <c>_lastSourceSend</c>/<c>_sourcePacedStreak</c> on a
    /// deleted source: a source deleted and re-created under the SAME qualified name must not inherit the
    /// old one's "last sent at"/streak and silently delay or drop its first event. Safe to call for a key
    /// this pacer has never seen (a no-op) — callers are not expected to check first.</summary>
    public void Forget(string key)
    {
        lock (_gate)
        {
            _lastSend.Remove(key);
            _pacedStreak.Remove(key);
        }
    }
}
