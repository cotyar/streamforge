using StreamForge.Engine.Dataflow;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>Epoch is a thin monotone-long wrapper; these tests pin its comparison, Min/Max, and
/// NegativeInfinity-floor semantics that FrontierTracker and EpochBuffer both depend on.</summary>
public class DataflowEpochTests
{
    [Fact]
    public void OrderingMatchesUnderlyingLongValue()
    {
        Assert.True(new Epoch(1) < new Epoch(2));
        Assert.True(new Epoch(2) > new Epoch(1));
        Assert.True(new Epoch(2) <= new Epoch(2));
        Assert.True(new Epoch(2) >= new Epoch(2));
        Assert.False(new Epoch(2) < new Epoch(2));
    }

    [Fact]
    public void EqualityIsValueBased()
    {
        Assert.Equal(new Epoch(42), new Epoch(42));
        Assert.NotEqual(new Epoch(42), new Epoch(43));
    }

    [Fact]
    public void NextIncrementsByOne()
    {
        Assert.Equal(new Epoch(6), new Epoch(5).Next());
    }

    [Fact]
    public void MinAndMaxPickTheExpectedSide()
    {
        Assert.Equal(new Epoch(3), Epoch.Min(new Epoch(3), new Epoch(9)));
        Assert.Equal(new Epoch(3), Epoch.Min(new Epoch(9), new Epoch(3)));
        Assert.Equal(new Epoch(9), Epoch.Max(new Epoch(3), new Epoch(9)));
        Assert.Equal(new Epoch(9), Epoch.Max(new Epoch(9), new Epoch(3)));
    }

    [Fact]
    public void NegativeInfinityIsBelowEveryRealEpoch()
    {
        Assert.True(Epoch.NegativeInfinity < Epoch.Zero);
        Assert.True(Epoch.NegativeInfinity < new Epoch(long.MinValue + 1));
        Assert.Equal(Epoch.NegativeInfinity, Epoch.Min(Epoch.NegativeInfinity, Epoch.Zero));
    }
}
