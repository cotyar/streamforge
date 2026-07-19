using StreamForge.Abstractions;

namespace StreamForge.AppCore.Connectors.Scheduling;

/// <summary>Failure backoff for connector polling (plan 006, D-E). Pure and stateless — the caller
/// (grain/actor) owns the consecutive-failure counter and resets it to 0 on success. All
/// timestamps in and out are UTC.</summary>
public static class BackoffPolicy
{
    private static readonly TimeSpan BaseDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(15);

    /// <summary>Delay before the next attempt given <paramref name="consecutiveFailures"/> (k).
    /// k &lt;= 0 → <see cref="TimeSpan.Zero"/> (no backoff — before the first attempt, or right
    /// after a success). k &gt;= 1 → min(30 s * 2^(k-1), 15 min), per D-E.</summary>
    public static TimeSpan Delay(int consecutiveFailures)
    {
        if (consecutiveFailures <= 0)
            return TimeSpan.Zero;

        // Clamp the exponent well before double overflow — 2^30 * 30s already dwarfs the 15 min
        // cap many times over, so the clamp never changes the (already-capped) result.
        var exponent = Math.Min(consecutiveFailures - 1, 30);
        var scaledMs = BaseDelay.TotalMilliseconds * Math.Pow(2, exponent);
        return TimeSpan.FromMilliseconds(Math.Min(scaledMs, MaxDelay.TotalMilliseconds));
    }

    /// <summary>Next run honoring backoff. Interval spec: nowUtc + max(interval, Delay(k)) — a
    /// fast schedule doesn't out-run a live backoff. Cron spec: the first occurrence &gt;= nowUtc +
    /// Delay(k) (cron sources skip straight to their next natural slot at/after the backoff
    /// point). Null when the spec fails <see cref="ScheduleCalc.Validate"/>.</summary>
    public static DateTimeOffset? NextRun(ScheduleSpec spec, DateTimeOffset nowUtc, int consecutiveFailures)
    {
        if (ScheduleCalc.Validate(spec).Count > 0)
            return null;

        var delay = Delay(consecutiveFailures);

        if (spec.IntervalMs.HasValue)
        {
            var interval = TimeSpan.FromMilliseconds(spec.IntervalMs.Value);
            return nowUtc + (delay > interval ? delay : interval);
        }

        // ScheduleCalc.NextOccurrence is strictly-after; searching from one tick before the
        // earliest-acceptable instant makes an exact match AT that instant still come back (the
        // trick is exact — cron granularity is whole seconds, so a single 100 ns tick can never
        // hide or duplicate an occurrence).
        var earliest = nowUtc + delay;
        return ScheduleCalc.NextOccurrence(spec, earliest - TimeSpan.FromTicks(1));
    }
}
