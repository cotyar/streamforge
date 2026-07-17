using StreamForge.Engine.Dataflow;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>ExchangeRouter maps canonical key bytes (TableKeyEncoding output) to a stable
/// partition index via FNV-1a. These tests pin: determinism across calls/processes,
/// partitionCount=1 degeneracy, and a "chi-square-lite" uniformity bound so a bad hash choice
/// (e.g. a low-entropy fold) would fail loudly.</summary>
public class DataflowExchangeRouterTests
{
    [Fact]
    public void SamePartitionCountOfOneAlwaysReturnsZero()
    {
        var rng = new Random(1);
        for (var i = 0; i < 100; i++)
        {
            var key = RandomKey(rng);
            Assert.Equal(0, ExchangeRouter.PartitionOf(key, 1));
        }
    }

    [Fact]
    public void ResultIsAlwaysInRange()
    {
        var rng = new Random(2);
        for (var i = 0; i < 500; i++)
        {
            var partitionCount = rng.Next(1, 64);
            var key = RandomKey(rng);
            var p = ExchangeRouter.PartitionOf(key, partitionCount);
            Assert.InRange(p, 0, partitionCount - 1);
        }
    }

    [Fact]
    public void IsDeterministicAcrossRepeatedCalls_SameKeySamePartitionEveryTime()
    {
        const string key = "S:AAPL";
        var first = ExchangeRouter.PartitionOf(key, 16);
        for (var i = 0; i < 50; i++)
        {
            Assert.Equal(first, ExchangeRouter.PartitionOf(key, 16));
        }
    }

    [Fact]
    public void StringOverloadMatchesUtf8ByteOverload()
    {
        const string key = "S:MSFT|L:42";
        var viaString = ExchangeRouter.PartitionOf(key, 8);
        var viaBytes = ExchangeRouter.PartitionOf(System.Text.Encoding.UTF8.GetBytes(key), 8);
        Assert.Equal(viaBytes, viaString);
    }

    [Fact]
    public void DifferentKeysCanLandOnDifferentPartitions()
    {
        // Not a strict requirement of any single pair, but with 10k distinct keys over 16
        // partitions we should see more than one partition used — a constant router would fail
        // this immediately.
        var used = new HashSet<int>();
        for (var i = 0; i < 10_000; i++)
        {
            used.Add(ExchangeRouter.PartitionOf($"S:key-{i}", 16));
        }
        Assert.True(used.Count > 1, "router mapped every key to a single partition");
    }

    [Fact]
    public void ZeroOrNegativePartitionCountThrows()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ExchangeRouter.PartitionOf("k", 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => ExchangeRouter.PartitionOf("k", -1));
    }

    [Fact]
    public void UniformDistributionOverTenThousandRandomKeys_ChiSquareLiteBound()
    {
        const int partitionCount = 16;
        const int sampleSize = 10_000;
        var counts = new int[partitionCount];
        var rng = new Random(2026_07_18);

        for (var i = 0; i < sampleSize; i++)
        {
            var key = RandomKey(rng);
            var p = ExchangeRouter.PartitionOf(key, partitionCount);
            counts[p]++;
        }

        var expected = (double)sampleSize / partitionCount; // 625
        var chiSquare = counts.Sum(c => (c - expected) * (c - expected) / expected);

        // Chi-square-lite bound: with 15 degrees of freedom, the 99.9% critical value is ~37.7.
        // A well-mixed hash should sit far below that; a broken one (e.g. hashing only the first
        // byte) would blow way past it. Generous margin to keep this test non-flaky.
        Assert.True(chiSquare < 60.0, $"chi-square statistic {chiSquare} suggests a poorly-mixed router (counts: {string.Join(",", counts)})");

        // No partition should be wildly over/under-represented either.
        foreach (var c in counts)
        {
            Assert.InRange(c, expected * 0.7, expected * 1.3);
        }
    }

    [Fact]
    public void ExchangeSpecRejectsNonPositivePartitionCount()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ExchangeSpec(new EdgeId(0), 0));
    }

    private static byte[] RandomKey(Random rng)
    {
        var bytes = new byte[rng.Next(4, 32)];
        rng.NextBytes(bytes);
        return bytes;
    }
}
