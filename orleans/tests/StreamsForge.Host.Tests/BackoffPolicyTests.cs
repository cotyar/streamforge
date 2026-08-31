using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors.Scheduling;
using Xunit;

namespace StreamsForge.Host.Tests;

public class BackoffPolicyTests
{
    // ---- Delay curve (D-E: min(30s * 2^(k-1), 15min)) ----

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void Delay_Non_positive_failure_count_is_zero(int consecutiveFailures)
    {
        Assert.Equal(TimeSpan.Zero, BackoffPolicy.Delay(consecutiveFailures));
    }

    [Theory]
    [InlineData(1, 30)]
    [InlineData(2, 60)]
    [InlineData(3, 120)]
    [InlineData(4, 240)]
    [InlineData(5, 480)]
    public void Delay_Curve_below_the_cap_doubles_each_failure(int k, int expectedSeconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), BackoffPolicy.Delay(k));
    }

    [Fact]
    public void Delay_At_k6_the_raw_doubling_would_exceed_the_cap_so_it_clamps_to_15_minutes()
    {
        // Raw would be 30s * 2^5 = 960s = 16min > 15min cap.
        Assert.Equal(TimeSpan.FromMinutes(15), BackoffPolicy.Delay(6));
    }

    [Theory]
    [InlineData(7)]
    [InlineData(20)]
    [InlineData(1000)]
    public void Delay_Stays_capped_at_15_minutes_for_arbitrarily_large_failure_counts(int k)
    {
        Assert.Equal(TimeSpan.FromMinutes(15), BackoffPolicy.Delay(k));
    }

    // ---- NextRun: interval spec ----

    [Fact]
    public void NextRun_Interval_no_failures_is_just_now_plus_interval()
    {
        var spec = new ScheduleSpec { IntervalMs = 10_000 };
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var next = BackoffPolicy.NextRun(spec, now, consecutiveFailures: 0);

        Assert.Equal(now.AddSeconds(10), next);
    }

    [Fact]
    public void NextRun_Interval_backoff_shorter_than_interval_the_interval_wins()
    {
        // Interval is 5 minutes; k=1 backoff is only 30s — the schedule shouldn't speed up.
        var spec = new ScheduleSpec { IntervalMs = 5 * 60_000 };
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var next = BackoffPolicy.NextRun(spec, now, consecutiveFailures: 1);

        Assert.Equal(now.AddMinutes(5), next);
    }

    [Fact]
    public void NextRun_Interval_backoff_longer_than_interval_the_backoff_wins()
    {
        // Interval is 10s; k=3 backoff is 120s — backoff should push the run out further.
        var spec = new ScheduleSpec { IntervalMs = 10_000 };
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var next = BackoffPolicy.NextRun(spec, now, consecutiveFailures: 3);

        Assert.Equal(now.AddSeconds(120), next);
    }

    // ---- NextRun: cron spec ----

    [Fact]
    public void NextRun_Cron_no_failures_is_the_first_occurrence_at_or_after_now()
    {
        var spec = new ScheduleSpec { Cron = "*/5 * * * *" };
        var now = new DateTimeOffset(2026, 1, 1, 0, 5, 0, TimeSpan.Zero); // exactly on a boundary

        var next = BackoffPolicy.NextRun(spec, now, consecutiveFailures: 0);

        // Inclusive at "now" itself (unlike ScheduleCalc.NextOccurrence, which is strict).
        Assert.Equal(now, next);
    }

    [Fact]
    public void NextRun_Cron_with_backoff_skips_to_the_first_occurrence_at_or_after_the_backoff_point()
    {
        // Every-5-minutes cron; k=1 → 30s backoff. now=00:00:00 → earliest=00:00:30 →
        // next natural slot at/after that is 00:05:00 (00:00:00 itself is before earliest).
        var spec = new ScheduleSpec { Cron = "*/5 * * * *" };
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var next = BackoffPolicy.NextRun(spec, now, consecutiveFailures: 1);

        Assert.Equal(new DateTimeOffset(2026, 1, 1, 0, 5, 0, TimeSpan.Zero), next);
    }

    [Fact]
    public void NextRun_Cron_backoff_landing_exactly_on_a_boundary_still_returns_that_boundary()
    {
        // Every-minute cron; k=2 → Delay = 60s exactly, so earliest (now + 60s) lands precisely on
        // the next minute boundary. NextRun must return that boundary itself, not skip past it —
        // this guards the "inclusive at earliest" behavior against an off-by-one.
        var spec = new ScheduleSpec { Cron = "* * * * *" };
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        Assert.Equal(TimeSpan.FromSeconds(60), BackoffPolicy.Delay(2)); // sanity-check the premise

        var next = BackoffPolicy.NextRun(spec, now, consecutiveFailures: 2);

        Assert.Equal(new DateTimeOffset(2026, 1, 1, 0, 1, 0, TimeSpan.Zero), next);
    }

    // ---- NextRun: invalid spec ----

    [Fact]
    public void NextRun_Invalid_spec_returns_null()
    {
        var next = BackoffPolicy.NextRun(new ScheduleSpec { Cron = "garbage" }, DateTimeOffset.UtcNow, 2);
        Assert.Null(next);
    }

    [Fact]
    public void NextRun_Spec_with_both_cron_and_interval_returns_null()
    {
        var spec = new ScheduleSpec { Cron = "* * * * *", IntervalMs = 5000 };
        Assert.Null(BackoffPolicy.NextRun(spec, DateTimeOffset.UtcNow, 0));
    }
}
