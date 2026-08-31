using StreamsForge.AppCore.Connectors.Polling;
using Xunit;

namespace StreamsForge.Engine.Tests;

/// <summary>Plan 009 A1.1: <see cref="DedupTracker"/>'s new optional per-instance <c>maxKeys</c>
/// override (needed so <c>IngestConfig.DedupWindow</c> can bound row-level ingress dedup below the
/// 10k default) — kept in a separate NEW file rather than editing the existing
/// <c>ConnectorPollingStateTests.DedupTrackerTests</c>, which is pinned to the pre-009 constructor
/// shape.</summary>
public class DedupTrackerCustomBoundTests
{
    [Fact]
    public void Default_constructor_still_uses_the_10k_bound()
    {
        var tracker = new DedupTracker();
        for (var i = 0; i < DedupTracker.MaxKeys; i++)
        {
            tracker.Seen($"k{i}");
        }

        Assert.Equal(DedupTracker.MaxKeys, tracker.Count);
    }

    [Fact]
    public void A_positive_maxKeys_override_bounds_below_the_default()
    {
        var tracker = new DedupTracker(maxKeys: 3);

        tracker.Seen("a");
        tracker.Seen("b");
        tracker.Seen("c");
        tracker.Seen("d"); // evicts "a"

        Assert.Equal(3, tracker.Count);
        Assert.False(tracker.Seen("a")); // forgotten — no longer "seen"
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void A_non_positive_maxKeys_falls_back_to_the_default(int maxKeys)
    {
        var tracker = new DedupTracker(maxKeys: maxKeys);
        for (var i = 0; i < 20; i++)
        {
            tracker.Seen($"k{i}");
        }

        Assert.Equal(20, tracker.Count); // nowhere near the 10k default, so nothing evicted yet
    }

    [Fact]
    public void A_persisted_list_longer_than_a_smaller_custom_bound_is_trimmed_immediately()
    {
        var persisted = Enumerable.Range(0, 10).Select(i => $"k{i}").ToList();

        var tracker = new DedupTracker(persisted, maxKeys: 4);

        Assert.Equal(4, tracker.Count);
        // Oldest-first trim: only the last 4 of the persisted (insertion-order) list survive.
        Assert.True(tracker.Seen("k6")); // still remembered
        Assert.True(tracker.Seen("k9")); // still remembered
        Assert.False(tracker.Seen("k0")); // trimmed away, so re-seeing it looks like a fresh key
    }
}
