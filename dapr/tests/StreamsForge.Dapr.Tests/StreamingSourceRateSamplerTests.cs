using StreamsForge.Dapr.Host.Streaming;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 W5-B: unit coverage for the pure sampling decision <see cref="DaprStreamBridge"/> uses to
/// cap source-event relays at ~20 msg/s per source, mirroring
/// orleans/src/StreamsForge.Host/Services/StreamBridgeService.cs's <c>SourceRelayMinIntervalMs</c>
/// (50ms). Uses an injected clock throughout so every test is deterministic — no real-time sleeps, no
/// flakiness under CI load.
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
    public void ShouldRelay_FirstCallForAKey_AlwaysRelays()
    {
        var clock = new FakeClock();
        var sampler = new SourceRateSampler(clock.Get);

        Assert.True(sampler.ShouldRelay("trades"));
    }

    [Fact]
    public void ShouldRelay_SecondCallImmediatelyAfter_IsDropped()
    {
        var clock = new FakeClock();
        var sampler = new SourceRateSampler(clock.Get);

        Assert.True(sampler.ShouldRelay("trades"));
        Assert.False(sampler.ShouldRelay("trades")); // 0ms elapsed — well under the 50ms floor
    }

    [Fact]
    public void ShouldRelay_CallJustUnderMinInterval_IsDropped()
    {
        var clock = new FakeClock();
        var sampler = new SourceRateSampler(clock.Get);

        Assert.True(sampler.ShouldRelay("trades"));
        clock.AdvanceMs(SourceRateSampler.MinIntervalMs - 1);
        Assert.False(sampler.ShouldRelay("trades"));
    }

    [Fact]
    public void ShouldRelay_CallAtOrAfterMinInterval_Relays()
    {
        var clock = new FakeClock();
        var sampler = new SourceRateSampler(clock.Get);

        Assert.True(sampler.ShouldRelay("trades"));
        clock.AdvanceMs(SourceRateSampler.MinIntervalMs);
        Assert.True(sampler.ShouldRelay("trades"));
    }

    [Fact]
    public void ShouldRelay_DifferentKeys_AreSampledIndependently()
    {
        var clock = new FakeClock();
        var sampler = new SourceRateSampler(clock.Get);

        Assert.True(sampler.ShouldRelay("trades"));
        Assert.False(sampler.ShouldRelay("trades"));
        // A different source name has never been seen — its own window hasn't started yet.
        Assert.True(sampler.ShouldRelay("quotes"));
    }

    [Fact]
    public void ShouldRelay_ManyRapidCalls_AdmitsOnlyOnePerWindow()
    {
        var clock = new FakeClock();
        var sampler = new SourceRateSampler(clock.Get);

        var admitted = 0;
        for (var i = 0; i < 10; i++)
        {
            if (sampler.ShouldRelay("trades"))
            {
                admitted++;
            }
        }

        Assert.Equal(1, admitted);
    }

    [Fact]
    public void ShouldRelay_DefaultClock_UsesRealTime()
    {
        // No injected clock — exercises the DateTime.UtcNow default path at least once.
        var sampler = new SourceRateSampler();
        Assert.True(sampler.ShouldRelay("default-clock-key"));
    }
}
