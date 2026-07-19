using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Actors;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W6: unit tests for <see cref="PipelineResultRing"/> — the pure
/// bounded-ring append/read logic extracted from <see cref="PipelineActor"/>'s recent-results cache
/// (backs <c>GET /api/pipelines/{id}/results</c>), same capacity/eviction contract as
/// <c>PipelineGrain</c>'s inline equivalent.
/// </summary>
public class PipelineResultRingTests
{
    private static ResultEnvelope Row(long seq) => new() { PipelineId = "p1", Seq = seq, TimestampMs = seq };

    [Fact]
    public void Append_UnderCapacity_KeepsEveryEntry()
    {
        var ring = new List<ResultEnvelope>();

        for (var i = 1; i <= 5; i++)
        {
            PipelineResultRing.Append(ring, Row(i), capacity: 100);
        }

        Assert.Equal(5, ring.Count);
        Assert.Equal(1, ring[0].Seq);
        Assert.Equal(5, ring[^1].Seq);
    }

    [Fact]
    public void Append_OverCapacity_EvictsOldestFirst()
    {
        var ring = new List<ResultEnvelope>();

        for (var i = 1; i <= 10; i++)
        {
            PipelineResultRing.Append(ring, Row(i), capacity: 3);
        }

        Assert.Equal(3, ring.Count);
        Assert.Equal([8, 9, 10], ring.Select(r => r.Seq));
    }

    [Fact]
    public void Take_LimitSmallerThanRing_ReturnsMostRecentEntriesOldestFirst()
    {
        var ring = new List<ResultEnvelope> { Row(1), Row(2), Row(3), Row(4), Row(5) };

        var take = PipelineResultRing.Take(ring, 2);

        Assert.Equal([4, 5], take.Select(r => r.Seq));
    }

    [Fact]
    public void Take_LimitLargerThanRing_ReturnsWholeRing()
    {
        var ring = new List<ResultEnvelope> { Row(1), Row(2) };

        var take = PipelineResultRing.Take(ring, 100);

        Assert.Equal([1, 2], take.Select(r => r.Seq));
    }

    [Fact]
    public void Take_ZeroOrNegativeLimit_ReturnsEmpty()
    {
        var ring = new List<ResultEnvelope> { Row(1), Row(2) };

        Assert.Empty(PipelineResultRing.Take(ring, 0));
        Assert.Empty(PipelineResultRing.Take(ring, -5));
    }

    [Fact]
    public void Take_EmptyRing_ReturnsEmpty()
    {
        var ring = new List<ResultEnvelope>();

        Assert.Empty(PipelineResultRing.Take(ring, 10));
    }
}
