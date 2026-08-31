using StreamsForge.Dapr.Host.Actors;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W5-A: unit tests for <see cref="GeneratorBatching.NextBatchCount"/> —
/// the pure EPS×Δt batching math extracted from <see cref="GeneratorActor"/> specifically so it can be
/// tested without any actor/timer/Dapr-sidecar machinery (mirrors how CatalogStoreTests exercises
/// CatalogStore rather than RegistryActor directly).
/// </summary>
public class GeneratorBatchingTests
{
    [Fact]
    public void NextBatchCount_ExactMultiple_ReturnsExactCountWithZeroCarry()
    {
        double carry = 0;

        var count = GeneratorBatching.NextBatchCount(5, TimeSpan.FromSeconds(1), ref carry);

        Assert.Equal(5, count);
        Assert.Equal(0, carry, precision: 9);
    }

    [Fact]
    public void NextBatchCount_ZeroOrNegativeEventsPerSecond_ReturnsZeroAndLeavesCarryUntouched()
    {
        double carry = 0.7;

        Assert.Equal(0, GeneratorBatching.NextBatchCount(0, TimeSpan.FromSeconds(1), ref carry));
        Assert.Equal(0.7, carry, precision: 9);

        Assert.Equal(0, GeneratorBatching.NextBatchCount(-3, TimeSpan.FromSeconds(1), ref carry));
        Assert.Equal(0.7, carry, precision: 9);
    }

    [Fact]
    public void NextBatchCount_ZeroOrNegativeElapsed_ReturnsZeroAndLeavesCarryUntouched()
    {
        double carry = 0.3;

        Assert.Equal(0, GeneratorBatching.NextBatchCount(10, TimeSpan.Zero, ref carry));
        Assert.Equal(0.3, carry, precision: 9);

        Assert.Equal(0, GeneratorBatching.NextBatchCount(10, TimeSpan.FromMilliseconds(-50), ref carry));
        Assert.Equal(0.3, carry, precision: 9);
    }

    [Fact]
    public void NextBatchCount_SubOneRatePerTick_CarriesFractionForwardAcrossTicks()
    {
        // 3 events/sec at a 200ms tick period => 0.6 events/tick — below one event/tick, so a naive
        // per-tick floor (no carry) would emit nothing, ever. With carry: tick1 0.6 (carry 0.6, count 0),
        // tick2 1.2 (count 1, carry 0.2), tick3 0.8 (count 0, carry 0.8), tick4 1.4 (count 1, carry 0.4).
        double carry = 0;
        var period = TimeSpan.FromMilliseconds(200);

        var c1 = GeneratorBatching.NextBatchCount(3, period, ref carry);
        var c2 = GeneratorBatching.NextBatchCount(3, period, ref carry);
        var c3 = GeneratorBatching.NextBatchCount(3, period, ref carry);
        var c4 = GeneratorBatching.NextBatchCount(3, period, ref carry);

        Assert.Equal([0, 1, 0, 1], new[] { c1, c2, c3, c4 });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(8)]
    [InlineData(10)]
    [InlineData(0.5)]
    public void NextBatchCount_OverManyTicks_ConvergesOnConfiguredRate(double eventsPerSecond)
    {
        // Simulate 100 ticks of a fixed 200ms period (20 seconds of wall-clock time) and check the total
        // emitted count matches the configured rate within one event of rounding error — this is the
        // property that matters (the long-run average rate is honest), not any single tick's count.
        double carry = 0;
        var period = TimeSpan.FromMilliseconds(200);
        const int ticks = 100;
        var total = 0;

        for (var i = 0; i < ticks; i++)
        {
            total += GeneratorBatching.NextBatchCount(eventsPerSecond, period, ref carry);
        }

        var expected = eventsPerSecond * ticks * period.TotalSeconds;
        Assert.InRange(total, Math.Floor(expected) - 1, Math.Ceiling(expected) + 1);
    }

    [Fact]
    public void NextBatchCount_CarryNeverGoesNegativeOrExceedsOne()
    {
        // The fractional remainder (exact - floor(exact)) is always in [0, 1) by construction — assert
        // this invariant holds across a long, varied run so a future refactor can't quietly break it.
        double carry = 0;
        var rng = new Random(42);

        for (var i = 0; i < 500; i++)
        {
            var eps = rng.NextDouble() * 20;
            var elapsedMs = rng.Next(50, 500);
            GeneratorBatching.NextBatchCount(eps, TimeSpan.FromMilliseconds(elapsedMs), ref carry);

            Assert.InRange(carry, 0.0, 1.0);
        }
    }

    [Fact]
    public void NextBatchCount_HighRateBurst_ProducesLargeSingleTickCount()
    {
        double carry = 0;

        // A high-EPS source (e.g. 500/sec) over a 200ms tick should batch ~100 events into one call —
        // this is the whole point of batching instead of per-event ticks.
        var count = GeneratorBatching.NextBatchCount(500, TimeSpan.FromMilliseconds(200), ref carry);

        Assert.Equal(100, count);
    }
}
