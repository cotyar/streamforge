using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors.Scheduling;
using Xunit;

namespace StreamsForge.Host.Tests;

public class ScheduleCalcTests
{
    // ---- Validate ----

    [Fact]
    public void Validate_Null_spec_is_invalid()
    {
        var diagnostics = ScheduleCalc.Validate(null);
        Assert.Single(diagnostics);
    }

    [Fact]
    public void Validate_Neither_cron_nor_interval_set_is_invalid()
    {
        var diagnostics = ScheduleCalc.Validate(new ScheduleSpec());
        Assert.Single(diagnostics);
    }

    [Fact]
    public void Validate_Both_cron_and_interval_set_is_invalid()
    {
        var diagnostics = ScheduleCalc.Validate(new ScheduleSpec { Cron = "* * * * *", IntervalMs = 5000 });
        Assert.Single(diagnostics);
    }

    [Fact]
    public void Validate_Interval_at_floor_is_valid()
    {
        var diagnostics = ScheduleCalc.Validate(new ScheduleSpec { IntervalMs = ScheduleCalc.MinIntervalMs });
        Assert.Empty(diagnostics);
    }

    [Fact]
    public void Validate_Interval_below_floor_is_invalid()
    {
        var diagnostics = ScheduleCalc.Validate(new ScheduleSpec { IntervalMs = 999 });
        Assert.Single(diagnostics);
    }

    [Fact]
    public void Validate_Interval_zero_is_invalid()
    {
        var diagnostics = ScheduleCalc.Validate(new ScheduleSpec { IntervalMs = 0 });
        Assert.Single(diagnostics);
    }

    [Fact]
    public void Validate_5_field_cron_is_valid()
    {
        var diagnostics = ScheduleCalc.Validate(new ScheduleSpec { Cron = "*/5 * * * *" });
        Assert.Empty(diagnostics);
    }

    [Fact]
    public void Validate_6_field_cron_with_seconds_is_valid()
    {
        var diagnostics = ScheduleCalc.Validate(new ScheduleSpec { Cron = "*/30 * * * * *" });
        Assert.Empty(diagnostics);
    }

    [Theory]
    [InlineData("not a cron")]
    [InlineData("* * * *")]      // 4 fields — neither 5 nor 6
    [InlineData("60 * * * *")]   // minute out of range
    [InlineData("* * * * * * *")] // 7 fields — too many even for IncludeSeconds
    public void Validate_Malformed_cron_reports_a_diagnostic(string cron)
    {
        var diagnostics = ScheduleCalc.Validate(new ScheduleSpec { Cron = cron });
        Assert.NotEmpty(diagnostics);
    }

    [Fact]
    public void Validate_Whitespace_only_cron_is_treated_as_unset()
    {
        // Neither Cron nor IntervalMs meaningfully set → "exactly one" violation, not a parse error.
        var diagnostics = ScheduleCalc.Validate(new ScheduleSpec { Cron = "   " });
        Assert.Single(diagnostics);
    }

    // ---- NextOccurrence ----

    [Fact]
    public void NextOccurrence_Interval_adds_the_interval_to_afterUtc()
    {
        var spec = new ScheduleSpec { IntervalMs = 30_000 };
        var after = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var next = ScheduleCalc.NextOccurrence(spec, after);

        Assert.Equal(after.AddSeconds(30), next);
    }

    [Fact]
    public void NextOccurrence_5_field_cron_finds_the_next_5_minute_boundary()
    {
        var spec = new ScheduleSpec { Cron = "*/5 * * * *" };
        var after = new DateTimeOffset(2026, 1, 1, 0, 2, 0, TimeSpan.Zero);

        var next = ScheduleCalc.NextOccurrence(spec, after);

        Assert.Equal(new DateTimeOffset(2026, 1, 1, 0, 5, 0, TimeSpan.Zero), next);
    }

    [Fact]
    public void NextOccurrence_6_field_cron_finds_the_next_30_second_boundary()
    {
        var spec = new ScheduleSpec { Cron = "*/30 * * * * *" };
        var after = new DateTimeOffset(2026, 1, 1, 0, 0, 10, TimeSpan.Zero);

        var next = ScheduleCalc.NextOccurrence(spec, after);

        Assert.Equal(new DateTimeOffset(2026, 1, 1, 0, 0, 30, TimeSpan.Zero), next);
    }

    [Fact]
    public void NextOccurrence_cron_result_is_exactly_on_the_boundary_not_a_second_later()
    {
        // Regression guard against an off-by-one in the strictly-after search.
        var spec = new ScheduleSpec { Cron = "0 0 * * *" };
        var after = new DateTimeOffset(2026, 3, 14, 0, 0, 0, TimeSpan.Zero); // exactly midnight

        var next = ScheduleCalc.NextOccurrence(spec, after);

        // Strictly after midnight → the FOLLOWING midnight, not the same instant.
        Assert.Equal(new DateTimeOffset(2026, 3, 15, 0, 0, 0, TimeSpan.Zero), next);
    }

    [Fact]
    public void NextOccurrence_Invalid_spec_returns_null()
    {
        var next = ScheduleCalc.NextOccurrence(new ScheduleSpec { Cron = "garbage" }, DateTimeOffset.UtcNow);
        Assert.Null(next);
    }

    [Fact]
    public void NextOccurrence_Spec_with_neither_field_set_returns_null()
    {
        var next = ScheduleCalc.NextOccurrence(new ScheduleSpec(), DateTimeOffset.UtcNow);
        Assert.Null(next);
    }

    // ---- TryParseShorthand ----

    [Theory]
    [InlineData("every 30s", 30_000)]
    [InlineData("every 5m", 300_000)]
    [InlineData("every 2h", 7_200_000)]
    [InlineData("EVERY 10S", 10_000)]     // case-insensitive
    [InlineData("Every 1M", 60_000)]
    [InlineData("every  45s", 45_000)]    // extra internal whitespace tolerated
    public void TryParseShorthand_Recognized_forms_parse_to_the_right_interval(string text, int expectedMs)
    {
        var spec = ScheduleCalc.TryParseShorthand(text);

        Assert.NotNull(spec);
        Assert.Equal(expectedMs, spec!.IntervalMs);
        Assert.Null(spec.Cron);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("every 30 seconds")]  // full unit word, not a single letter
    [InlineData("every 5")]           // missing unit
    [InlineData("30s")]               // missing "every"
    [InlineData("every -5s")]         // negative not supported by \d+
    [InlineData("every 5m5s")]        // combined units unsupported
    [InlineData("0 0 * * *")]         // a cron string, not shorthand
    public void TryParseShorthand_Rejects_anything_outside_the_closed_grammar(string text)
    {
        Assert.Null(ScheduleCalc.TryParseShorthand(text));
    }
}
