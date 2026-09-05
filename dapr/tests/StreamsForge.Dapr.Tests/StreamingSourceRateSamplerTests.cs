using StreamsForge.Dapr.Host.Streaming;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 025 (Dapr parity, D5/D6): unit coverage for the pure pacing decision
/// <see cref="DaprStreamBridge"/> uses to cap source-event relays at ~20 msg/s per source, mirroring
/// orleans/src/StreamsForge.Host/Services/StreamBridgeService.cs's <c>SubscribeToSourceAsync</c> pacing
/// rule (decision D5, plan 023): a too-early event WAITS OUT the remainder of its slot instead of being
/// dropped, so a burst relays in full; only a sustained producer past <see cref="SourceRateSampler.MaxPacedStreak"/>
/// consecutive paced events degrades to the OLD drop behavior. Every test uses an injected clock and NEVER
/// sleeps for real — a "delay" is simulated by advancing the fake clock (or not advancing it, to prove a
/// tight burst is paced rather than dropped), never by actually waiting.
/// </summary>
public class StreamingSourceRateSamplerTests
{
    private sealed class FakeClock
    {
        public DateTime Now { get; set; } = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public DateTime Get() => Now;

        public void AdvanceMs(double ms) => Now = Now.AddMilliseconds(ms);
    }

    [Fact]
    public void Evaluate_FirstCallForAKey_SendsNow()
    {
        var clock = new FakeClock();
        var pacer = new SourceRateSampler(clock.Get);

        var plan = pacer.Evaluate("trades");

        Assert.Equal(RelayDecision.SendNow, plan.Decision);
        Assert.Equal(0, plan.DelayMs);
    }

    [Fact]
    public void Evaluate_SecondCallImmediatelyAfter_SendsAfterTheRemainderOfTheSlot()
    {
        // This is the D5 behavior change from the old sampler: a too-early event is no longer DROPPED —
        // it is told to wait out the rest of its 50ms slot and still relay.
        var clock = new FakeClock();
        var pacer = new SourceRateSampler(clock.Get);

        Assert.Equal(RelayDecision.SendNow, pacer.Evaluate("trades").Decision);
        var plan = pacer.Evaluate("trades"); // 0ms elapsed — well under the 50ms floor

        Assert.Equal(RelayDecision.SendAfterDelay, plan.Decision);
        Assert.Equal(SourceRateSampler.MinIntervalMs, plan.DelayMs);
    }

    [Fact]
    public void Evaluate_TightBatchOfThree_RelaysAllThreeInOrder_NoneDropped()
    {
        // The pinned regression from the old (drop-based) sampler asserted exactly ONE relayed event for
        // a tight batch of 3 — that was the bug (plan 023/025 D5): a burst is the normal shape of a
        // polled source's tick, and dropping 2 of 3 reads as data loss to an operator watching the live
        // tape. Now all three must be admitted (Send now / after a delay), never Dropped, and the delays
        // must be monotonically non-decreasing, in order, since the bridge sends each in turn.
        var clock = new FakeClock();
        var pacer = new SourceRateSampler(clock.Get);

        var first = pacer.Evaluate("trades");
        var second = pacer.Evaluate("trades");
        var third = pacer.Evaluate("trades");

        Assert.Equal(RelayDecision.SendNow, first.Decision);
        Assert.Equal(RelayDecision.SendAfterDelay, second.Decision);
        Assert.Equal(RelayDecision.SendAfterDelay, third.Decision);
        Assert.Equal(SourceRateSampler.MinIntervalMs, second.DelayMs);
        // The third event's wait is measured against the SECOND event's predicted (not yet real) send
        // instant — now + 50ms — so it owes the full next slot on top of that: ~100ms from "now".
        Assert.Equal(SourceRateSampler.MinIntervalMs * 2, third.DelayMs);
    }

    [Fact]
    public void Evaluate_CallJustUnderMinInterval_SendsAfterTheRemainder()
    {
        var clock = new FakeClock();
        var pacer = new SourceRateSampler(clock.Get);

        Assert.Equal(RelayDecision.SendNow, pacer.Evaluate("trades").Decision);
        clock.AdvanceMs(SourceRateSampler.MinIntervalMs - 1);
        var plan = pacer.Evaluate("trades");

        Assert.Equal(RelayDecision.SendAfterDelay, plan.Decision);
        Assert.Equal(1, plan.DelayMs);
    }

    [Fact]
    public void Evaluate_CallAtOrAfterMinInterval_SendsNow()
    {
        var clock = new FakeClock();
        var pacer = new SourceRateSampler(clock.Get);

        Assert.Equal(RelayDecision.SendNow, pacer.Evaluate("trades").Decision);
        clock.AdvanceMs(SourceRateSampler.MinIntervalMs);
        var plan = pacer.Evaluate("trades");

        Assert.Equal(RelayDecision.SendNow, plan.Decision);
    }

    [Fact]
    public void Evaluate_SustainedFirehosePastTheStreakCap_DegradesToDropping()
    {
        // A producer that never lets up keeps owing a full slot forever — this relay's own backlog
        // cannot be allowed to grow unbounded trailing it, so once MaxPacedStreak consecutive events have
        // each been paced (never given an instant of headroom), the next one is dropped instead.
        var clock = new FakeClock();
        var pacer = new SourceRateSampler(clock.Get);

        var decisions = new List<RelayDecision>();
        for (var i = 0; i < SourceRateSampler.MaxPacedStreak + 2; i++)
        {
            decisions.Add(pacer.Evaluate("trades").Decision);
        }

        Assert.Equal(RelayDecision.SendNow, decisions[0]);
        // Every call in between is paced, not dropped.
        for (var i = 1; i <= SourceRateSampler.MaxPacedStreak; i++)
        {
            Assert.Equal(RelayDecision.SendAfterDelay, decisions[i]);
        }

        // Once MaxPacedStreak consecutive events have been paced, the next is dropped.
        Assert.Equal(RelayDecision.Drop, decisions[SourceRateSampler.MaxPacedStreak + 1]);
    }

    [Fact]
    public void Evaluate_AQuietGapAfterDropping_ResetsTheStreakAndSendsNowAgain()
    {
        var clock = new FakeClock();
        var pacer = new SourceRateSampler(clock.Get);

        for (var i = 0; i < SourceRateSampler.MaxPacedStreak + 2; i++)
        {
            pacer.Evaluate("trades");
        }

        // A quiet gap: the producer stops for well over a slot, e.g. 200ms — the predicted "last send"
        // instant (however far in the future the streak had pushed it) is now safely in the past.
        clock.AdvanceMs(10_000);
        var afterGap = pacer.Evaluate("trades");
        Assert.Equal(RelayDecision.SendNow, afterGap.Decision);

        // The streak reset means the VERY NEXT tight call is paced again, not dropped — proving the
        // streak counter, not just the timestamp, was cleared by the gap.
        var immediatelyAfter = pacer.Evaluate("trades");
        Assert.Equal(RelayDecision.SendAfterDelay, immediatelyAfter.Decision);
    }

    [Fact]
    public void Evaluate_DifferentKeys_ArePacedIndependently()
    {
        var clock = new FakeClock();
        var pacer = new SourceRateSampler(clock.Get);

        Assert.Equal(RelayDecision.SendNow, pacer.Evaluate("trades").Decision);
        Assert.Equal(RelayDecision.SendAfterDelay, pacer.Evaluate("trades").Decision);
        // A different source name has never been seen — its own window hasn't started yet.
        Assert.Equal(RelayDecision.SendNow, pacer.Evaluate("quotes").Decision);
    }

    [Fact]
    public void Forget_ClearsPacingState_SoARecreatedSourceDoesNotInheritTheOldOnesDelay()
    {
        // Mirrors Orleans' UnsubscribeFromSourceAsync clearing _lastSourceSend/_sourcePacedStreak on a
        // deleted source: a source deleted and recreated under the same qualified name must relay its
        // first event immediately, not inherit whatever delay/streak the deleted source had accrued.
        var clock = new FakeClock();
        var pacer = new SourceRateSampler(clock.Get);

        Assert.Equal(RelayDecision.SendNow, pacer.Evaluate("trades").Decision);
        Assert.Equal(RelayDecision.SendAfterDelay, pacer.Evaluate("trades").Decision);

        pacer.Forget("trades");

        Assert.Equal(RelayDecision.SendNow, pacer.Evaluate("trades").Decision);
    }

    [Fact]
    public void Forget_UnknownKey_IsANoOp()
    {
        var pacer = new SourceRateSampler(new FakeClock().Get);

        pacer.Forget("never-seen");

        Assert.Equal(RelayDecision.SendNow, pacer.Evaluate("never-seen").Decision);
    }

    [Fact]
    public void Evaluate_DefaultClock_UsesRealTime()
    {
        // No injected clock — exercises the DateTime.UtcNow default path at least once.
        var pacer = new SourceRateSampler();
        Assert.Equal(RelayDecision.SendNow, pacer.Evaluate("default-clock-key").Decision);
    }
}
