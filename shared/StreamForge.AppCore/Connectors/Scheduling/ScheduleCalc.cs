using Cronos;
using StreamForge.Abstractions;

namespace StreamForge.AppCore.Connectors.Scheduling;

/// <summary>Pure validation and next-occurrence calculator for <see cref="ScheduleSpec"/> (plan
/// 006, D-E). Never reads the wall clock — every "now" comes in as a parameter. All timestamps in
/// and out are UTC.</summary>
public static class ScheduleCalc
{
    /// <summary>1 s minimum poll interval (D-E floor) — no hot-looping.</summary>
    public const int MinIntervalMs = 1000;

    /// <summary>Validates a schedule: exactly one of <see cref="ScheduleSpec.Cron"/> /
    /// <see cref="ScheduleSpec.IntervalMs"/> must be set; Cron must parse via Cronos as a 5-field
    /// standard expression, or a 6-field expression with a leading seconds field
    /// (<see cref="CronFormat.IncludeSeconds"/>) — the field count is detected by whitespace-token
    /// count, not guessed from content; IntervalMs must be at least <see cref="MinIntervalMs"/>.
    /// Returns human-readable diagnostics; an empty list means the spec is valid. A null spec is
    /// invalid (one diagnostic).</summary>
    public static IReadOnlyList<string> Validate(ScheduleSpec? spec)
    {
        if (spec is null)
            return ["Schedule is required."];

        var hasCron = !string.IsNullOrWhiteSpace(spec.Cron);
        var hasInterval = spec.IntervalMs.HasValue;

        if (hasCron == hasInterval)
        {
            return
            [
                hasCron
                    ? "Exactly one of Cron or IntervalMs must be set, not both."
                    : "Exactly one of Cron or IntervalMs must be set."
            ];
        }

        if (hasInterval)
        {
            return spec.IntervalMs!.Value < MinIntervalMs
                ? [$"IntervalMs must be at least {MinIntervalMs} (1 s floor); got {spec.IntervalMs.Value}."]
                : [];
        }

        try
        {
            ParseCron(spec.Cron!);
            return [];
        }
        catch (CronFormatException ex)
        {
            return [$"Invalid cron expression '{spec.Cron}': {ex.Message}"];
        }
    }

    /// <summary>Next occurrence strictly after <paramref name="afterUtc"/>, in UTC. Null when the
    /// spec fails <see cref="Validate"/>, or (cron only) when Cronos reports no further
    /// occurrence.</summary>
    public static DateTimeOffset? NextOccurrence(ScheduleSpec spec, DateTimeOffset afterUtc)
    {
        if (Validate(spec).Count > 0)
            return null;

        if (spec.IntervalMs.HasValue)
            return afterUtc.AddMilliseconds(spec.IntervalMs.Value);

        // Validate() above already proved this parses.
        var expression = ParseCron(spec.Cron!);
        var next = expression.GetNextOccurrence(afterUtc.UtcDateTime, inclusive: false);
        return next is null ? null : new DateTimeOffset(next.Value);
    }

    /// <summary>Parses "every 30s" / "every 5m" / "every 2h" (case-insensitive, single unit,
    /// optional whitespace around the number) into a fixed-interval <see cref="ScheduleSpec"/>.
    /// Null when the text doesn't match this closed shorthand grammar — no combined units, no
    /// other words. The resulting IntervalMs is NOT floor-checked here; run it through
    /// <see cref="Validate"/> if that matters to the caller.</summary>
    public static ScheduleSpec? TryParseShorthand(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return null;

        var match = ShorthandPattern.Match(text);
        if (!match.Success)
            return null;

        var amount = int.Parse(match.Groups["amount"].Value);
        var unitMs = match.Groups["unit"].Value.ToLowerInvariant() switch
        {
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            _ => 0, // unreachable — the regex only captures s/m/h
        };

        return new ScheduleSpec { IntervalMs = amount * unitMs };
    }

    private static readonly System.Text.RegularExpressions.Regex ShorthandPattern = new(
        @"^\s*every\s+(?<amount>\d+)\s*(?<unit>[smh])\s*$",
        System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled);

    private static CronExpression ParseCron(string cron)
    {
        var tokenCount = cron.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Length;
        var format = tokenCount >= 6 ? CronFormat.IncludeSeconds : CronFormat.Standard;
        return CronExpression.Parse(cron, format);
    }
}
