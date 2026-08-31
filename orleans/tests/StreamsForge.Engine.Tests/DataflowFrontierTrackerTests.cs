using StreamsForge.Engine.Dataflow;
using Xunit;

namespace StreamsForge.Engine.Tests;

/// <summary>FrontierTracker is the primitive every historical dataflow bug traces back to (plan's
/// words) — these tests drive its min-combine, regression-detection, idempotency, and
/// silent-upstream semantics directly, independent of any executor.</summary>
public class DataflowFrontierTrackerTests
{
    private static readonly EdgeId Edge0 = new(0);
    private static readonly EdgeId Edge1 = new(1);
    private static readonly UpstreamId UA = new(Edge0, 0);
    private static readonly UpstreamId UB = new(Edge0, 1);

    [Fact]
    public void SingleUpstreamFrontierTracksItsObservations()
    {
        var tracker = new FrontierTracker([UA]);

        var obs = tracker.Observe(UA, new Epoch(5));

        Assert.True(obs.Advanced);
        Assert.Equal(new Epoch(5), obs.Frontier);
        Assert.Equal(new Epoch(5), tracker.Frontier);
    }

    [Fact]
    public void DuplicateObservationIsIdempotentAndReportsNoChange()
    {
        var tracker = new FrontierTracker([UA]);
        tracker.Observe(UA, new Epoch(5));

        var repeat = tracker.Observe(UA, new Epoch(5));

        Assert.Equal(FrontierObserveResult.NoChange, repeat.Result);
        Assert.False(repeat.Advanced);
        Assert.False(repeat.Regressed);
        Assert.Equal(new Epoch(5), tracker.Frontier);
    }

    [Fact]
    public void MultiUpstreamFrontierIsTheMinimum()
    {
        var tracker = new FrontierTracker([UA, UB]);

        tracker.Observe(UA, new Epoch(10));
        var afterB = tracker.Observe(UB, new Epoch(3));

        Assert.True(afterB.Advanced);
        Assert.Equal(new Epoch(3), tracker.Frontier);

        var afterAAdvancesFurther = tracker.Observe(UA, new Epoch(20));
        // B still holds it at 3 — advancing A past B must NOT move the combined frontier.
        Assert.Equal(FrontierObserveResult.NoChange, afterAAdvancesFurther.Result);
        Assert.Equal(new Epoch(3), tracker.Frontier);
    }

    [Fact]
    public void OneSilentUpstreamHoldsTheFrontierAtNegativeInfinity()
    {
        var tracker = new FrontierTracker([UA, UB]);

        var obs = tracker.Observe(UA, new Epoch(100));

        // UB has never observed — the combined frontier cannot move past "no data yet".
        Assert.Equal(FrontierObserveResult.NoChange, obs.Result);
        Assert.Equal(Epoch.NegativeInfinity, tracker.Frontier);

        var afterB = tracker.Observe(UB, new Epoch(1));
        Assert.True(afterB.Advanced);
        Assert.Equal(new Epoch(1), tracker.Frontier);
    }

    [Fact]
    public void RegressionIsDetectedAndRejectedNotApplied()
    {
        var tracker = new FrontierTracker([UA]);
        tracker.Observe(UA, new Epoch(10));

        var regressed = tracker.Observe(UA, new Epoch(4));

        Assert.True(regressed.Regressed);
        Assert.Equal(FrontierObserveResult.Regression, regressed.Result);
        // Rejected, not applied: the tracker's frontier is unaffected by the bad observation.
        Assert.Equal(new Epoch(10), tracker.Frontier);
        Assert.Equal(new Epoch(10), regressed.Frontier);
    }

    [Fact]
    public void RegressionOnOneUpstreamDoesNotCorruptAnotherUpstreamsState()
    {
        var tracker = new FrontierTracker([UA, UB]);
        tracker.Observe(UA, new Epoch(10));
        tracker.Observe(UB, new Epoch(10));
        Assert.Equal(new Epoch(10), tracker.Frontier);

        tracker.Observe(UA, new Epoch(2)); // regression, rejected

        // Frontier is untouched; A's high-water mark is still 10 (the rejected 2 was never
        // applied), so advancing A further is accepted as a normal (non-regression) update — it
        // just doesn't move the combined frontier yet because B still holds it at 10.
        Assert.Equal(new Epoch(10), tracker.Frontier);
        var advanceA = tracker.Observe(UA, new Epoch(15));
        Assert.False(advanceA.Regressed);
        Assert.Equal(FrontierObserveResult.NoChange, advanceA.Result);
        Assert.Equal(new Epoch(10), tracker.Frontier);

        // Once B catches up past A's new mark, the combined frontier finally moves to A's mark.
        var advanceB = tracker.Observe(UB, new Epoch(20));
        Assert.True(advanceB.Advanced);
        Assert.Equal(new Epoch(15), tracker.Frontier);
    }

    [Fact]
    public void EmptyEpochMarkerStillAdvancesTheFrontier()
    {
        // An epoch with zero deltas is still a real observation — DeltaBatch.IsMarker batches
        // carry no rows but MUST still move the frontier, otherwise a quiet upstream would wedge
        // the whole operator.
        var tracker = new FrontierTracker([UA]);
        var marker = new DeltaBatch(Edge0, 0, new Epoch(7), []);

        Assert.True(marker.IsMarker);
        var obs = tracker.Observe(marker.Upstream, marker.Epoch);

        Assert.True(obs.Advanced);
        Assert.Equal(new Epoch(7), tracker.Frontier);
    }

    [Fact]
    public void UnknownUpstreamThrows()
    {
        var tracker = new FrontierTracker([UA]);
        Assert.Throws<ArgumentException>(() => tracker.Observe(UB, new Epoch(1)));
    }

    [Fact]
    public void RegisteringDuplicateUpstreamThrows()
    {
        var tracker = new FrontierTracker([UA]);
        Assert.Throws<ArgumentException>(() => tracker.RegisterUpstream(UA));
    }

    [Fact]
    public void RegisteringAfterObservationStartedThrows()
    {
        var tracker = new FrontierTracker([UA]);
        tracker.Observe(UA, new Epoch(1));

        Assert.Throws<InvalidOperationException>(() => tracker.RegisterUpstream(UB));
    }

    [Fact]
    public void FrontierAcrossDifferentEdgesStillMinCombines()
    {
        var upstreamEdge0 = new UpstreamId(Edge0, 0);
        var upstreamEdge1 = new UpstreamId(Edge1, 0);
        var tracker = new FrontierTracker([upstreamEdge0, upstreamEdge1]);

        tracker.Observe(upstreamEdge0, new Epoch(50));
        var obs = tracker.Observe(upstreamEdge1, new Epoch(2));

        Assert.True(obs.Advanced);
        Assert.Equal(new Epoch(2), tracker.Frontier);
    }

    [Fact]
    public void SeededRandomWalk_FrontierNeverExceedsTrueMinimumOfHighWaterMarks()
    {
        var rng = new Random(20260718);
        var upstreams = Enumerable.Range(0, 5).Select(i => new UpstreamId(Edge0, i)).ToArray();
        var tracker = new FrontierTracker(upstreams);
        var highWater = new long[upstreams.Length];
        var observed = new bool[upstreams.Length];

        for (var step = 0; step < 2000; step++)
        {
            var idx = rng.Next(upstreams.Length);
            // Bias toward advances but occasionally attempt (and expect rejection of) a regression.
            var delta = rng.Next(0, 100) < 90 ? rng.Next(0, 5) : -rng.Next(1, 5);
            var candidate = (observed[idx] ? highWater[idx] : 0) + delta;

            var result = tracker.Observe(upstreams[idx], new Epoch(candidate));

            if (observed[idx] && candidate < highWater[idx])
            {
                Assert.True(result.Regressed);
            }
            else
            {
                highWater[idx] = candidate;
                observed[idx] = true;
                Assert.False(result.Regressed);
            }

            var expectedFrontier = observed.All(o => o) ? new Epoch(highWater.Min()) : Epoch.NegativeInfinity;
            Assert.Equal(expectedFrontier, tracker.Frontier);
        }
    }
}
