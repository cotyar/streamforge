using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Host.Services;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Wishlist item 16's netting half ("the hub coalesces a table's deltas into one message per flush
/// window but does not net them"): <see cref="StreamBridgeService.NetByRowIdentity"/> collapses same-row
/// retract+assert pairs within one flush window's <see cref="TableDeltaDto"/> batch to their net effect,
/// using the exact row-identity rule <c>TableExecutorImpl.ConsolidateEpochOutput</c> already applies
/// within one engine epoch — <see cref="TableExecutor.CanonicalRowKeyOf"/>, the additive PublicApi.cs
/// static this fix added specifically because the bridge has no live TableExecutor to ask (see
/// PublicApi.cs's own doc comment on that method).
///
/// <see cref="StreamBridgeService"/> is a BackgroundService wired to a live Orleans cluster client/SignalR
/// hub — this repo has no harness that spins those up for a bridge-level integration test, so (matching
/// SourcesEndpointsLogicTests.cs's own "no HTTP-level test harness" precedent) <c>NetByRowIdentity</c> is
/// made <c>public static</c> and pure specifically so it can be tested directly, the same seam-extraction
/// pattern used for <c>IngestGrpcService.BuildRetractErrors</c>/<c>RejectedResult</c> elsewhere in this
/// wave.
/// </summary>
public class StreamBridgeTableDeltaNettingTests
{
    private static TableDeltaDto Delta(long weight, params (string Field, object? Value)[] fields)
    {
        var row = new Dictionary<string, object?>();
        foreach (var (f, v) in fields)
        {
            row[f] = v;
        }
        return new TableDeltaDto { Row = row, Weight = weight };
    }

    private static TableDeltaDto EvictedDelta(long weight, params (string Field, object? Value)[] fields)
    {
        var d = Delta(weight, fields);
        d.Evicted = true;
        return d;
    }

    // ------------------------------------------------------------------
    // Basic shape.
    // ------------------------------------------------------------------

    [Fact]
    public void ASingleDelta_PassesThroughUnchanged()
    {
        var single = new List<TableDeltaDto> { Delta(1, ("id", "A")) };

        var netted = StreamBridgeService.NetByRowIdentity(single);

        var d = Assert.Single(netted);
        Assert.Equal(1, d.Weight);
        Assert.Equal("A", d.Row["id"]);
    }

    [Fact]
    public void AnEmptyBatch_StaysEmpty()
    {
        var netted = StreamBridgeService.NetByRowIdentity([]);

        Assert.Empty(netted);
    }

    [Fact]
    public void TwoDeltasForDifferentRows_BothSurvive_OrderPreserved()
    {
        var batch = new List<TableDeltaDto> { Delta(1, ("id", "A")), Delta(1, ("id", "B")) };

        var netted = StreamBridgeService.NetByRowIdentity(batch);

        Assert.Equal(2, netted.Count);
        Assert.Equal("A", netted[0].Row["id"]);
        Assert.Equal("B", netted[1].Row["id"]);
    }

    // ------------------------------------------------------------------
    // The actual netting.
    // ------------------------------------------------------------------

    [Fact]
    public void AssertThenRetractOfTheSameRow_NetsToNothing()
    {
        // Exactly the scenario wishlist item 16 names: an assert immediately undone by a retract within
        // the same flush window. This must produce ZERO messages, not two that cancel out on the wire.
        var batch = new List<TableDeltaDto>
        {
            Delta(1, ("order_id", "O1"), ("stage", "NEW")),
            Delta(-1, ("order_id", "O1"), ("stage", "NEW")),
        };

        var netted = StreamBridgeService.NetByRowIdentity(batch);

        Assert.Empty(netted);
    }

    [Fact]
    public void RetractThenAssertOfTheSameRow_AlsoNetsToNothing()
    {
        // Same cancellation, opposite arrival order — the rule is symmetric (it is a SUM, not a sequence
        // match).
        var batch = new List<TableDeltaDto>
        {
            Delta(-1, ("order_id", "O1"), ("stage", "NEW")),
            Delta(1, ("order_id", "O1"), ("stage", "NEW")),
        };

        var netted = StreamBridgeService.NetByRowIdentity(batch);

        Assert.Empty(netted);
    }

    [Fact]
    public void TwoAssertsOfTheSameRow_NetToOneEntryWithTheSummedWeight()
    {
        var batch = new List<TableDeltaDto>
        {
            Delta(1, ("order_id", "O1")),
            Delta(1, ("order_id", "O1")),
        };

        var netted = StreamBridgeService.NetByRowIdentity(batch);

        var d = Assert.Single(netted);
        Assert.Equal(2, d.Weight);
    }

    [Fact]
    public void SurvivingEntry_KeepsTheFirstOccurrencesPositionAndRowContent()
    {
        var batch = new List<TableDeltaDto>
        {
            Delta(1, ("id", "B")),
            Delta(1, ("id", "A")), // first occurrence of "A" — position 1
            Delta(1, ("id", "A")), // nets into the entry above; does not move it
        };

        var netted = StreamBridgeService.NetByRowIdentity(batch);

        Assert.Equal(2, netted.Count);
        Assert.Equal("B", netted[0].Row["id"]);
        Assert.Equal("A", netted[1].Row["id"]);
        Assert.Equal(2, netted[1].Weight);
    }

    [Fact]
    public void EvictedFlag_IsTakenFromTheFirstOccurrence()
    {
        var batch = new List<TableDeltaDto>
        {
            EvictedDelta(1, ("id", "A")),
            Delta(1, ("id", "A")),
        };

        var netted = StreamBridgeService.NetByRowIdentity(batch);

        var d = Assert.Single(netted);
        Assert.True(d.Evicted);
    }

    [Fact]
    public void ThreeKeysInterleaved_OnlyTheCancellingOneDisappears()
    {
        var batch = new List<TableDeltaDto>
        {
            Delta(1, ("id", "A")),
            Delta(1, ("id", "B")),
            Delta(-1, ("id", "A")), // cancels A
            Delta(1, ("id", "C")),
            Delta(1, ("id", "B")), // B nets to weight 2
        };

        var netted = StreamBridgeService.NetByRowIdentity(batch);

        Assert.Equal(2, netted.Count);
        Assert.Equal("B", netted[0].Row["id"]);
        Assert.Equal(2, netted[0].Weight);
        Assert.Equal("C", netted[1].Row["id"]);
        Assert.Equal(1, netted[1].Weight);
    }

    [Fact]
    public void RowIdentityIsByContentNotReferenceOrFieldOrder()
    {
        // Two independently-built dictionaries with the same key/value pairs (built in a different
        // insertion order) must be recognized as the SAME row — this is exactly why netting reuses
        // TableExecutor.CanonicalRowKeyOf instead of, say, a reference-equality or hand-written compare.
        var rowA1 = new Dictionary<string, object?> { ["order_id"] = "O1", ["stage"] = "NEW" };
        var rowA2 = new Dictionary<string, object?> { ["stage"] = "NEW", ["order_id"] = "O1" };
        var batch = new List<TableDeltaDto>
        {
            new() { Row = rowA1, Weight = 1 },
            new() { Row = rowA2, Weight = -1 },
        };

        var netted = StreamBridgeService.NetByRowIdentity(batch);

        Assert.Empty(netted);
    }

    // ------------------------------------------------------------------
    // THE CONVERGENCE PROOF: a client applying the netted batch ends up with exactly the same live rows
    // as a client that applied the raw, un-netted batch — netting changes message size, never the result.
    // ------------------------------------------------------------------

    /// <summary>Simulates a client-side Z-set: applies each delta's weight against a running total per
    /// canonical row key (the SAME identity rule the netting itself uses), and returns the SURVIVING rows
    /// (net weight != 0) as (canonicalKey -> netWeight). This is deliberately independent of
    /// NetByRowIdentity's own internals — it re-derives convergence from first principles rather than
    /// asserting the method didn't change its own intermediate state.</summary>
    private static Dictionary<string, long> ApplyAsClientZSet(IEnumerable<TableDeltaDto> deltas)
    {
        var weights = new Dictionary<string, long>();
        foreach (var d in deltas)
        {
            var key = TableExecutor.CanonicalRowKeyOf(d.Row);
            weights[key] = weights.GetValueOrDefault(key) + d.Weight;
        }
        return weights.Where(kv => kv.Value != 0).ToDictionary(kv => kv.Key, kv => kv.Value);
    }

    [Fact]
    public void NettedAndRawBatches_ConvergeToTheIdenticalClientRowSet()
    {
        var raw = new List<TableDeltaDto>
        {
            Delta(1, ("order_id", "O1"), ("stage", "NEW")),
            Delta(1, ("order_id", "O2"), ("stage", "NEW")),
            Delta(-1, ("order_id", "O1"), ("stage", "NEW")), // O1 asserted then retracted
            Delta(1, ("order_id", "O1"), ("stage", "FILLED")), // O1 re-asserted under a NEW row shape
            Delta(1, ("order_id", "O3"), ("stage", "NEW")),
            Delta(-1, ("order_id", "O3"), ("stage", "NEW")), // O3 fully cancels within the window
        };

        var netted = StreamBridgeService.NetByRowIdentity(raw);

        // The proof: identical converged Z-set...
        Assert.Equal(ApplyAsClientZSet(raw), ApplyAsClientZSet(netted));
        // ...via strictly fewer (or equal) wire messages — this is what the feature is actually for.
        Assert.True(netted.Count < raw.Count, $"expected netting to shrink the batch: raw={raw.Count} netted={netted.Count}");
    }

    [Fact]
    public void ABatchWithNoCancellation_ConvergesIdenticallyAndIsUntouchedInSize()
    {
        var raw = new List<TableDeltaDto>
        {
            Delta(1, ("order_id", "O1")),
            Delta(1, ("order_id", "O2")),
            Delta(1, ("order_id", "O3")),
        };

        var netted = StreamBridgeService.NetByRowIdentity(raw);

        Assert.Equal(ApplyAsClientZSet(raw), ApplyAsClientZSet(netted));
        Assert.Equal(raw.Count, netted.Count); // nothing to net — no shrinkage, no growth
    }

    [Fact]
    public void ABatchThatFullyCancels_ConvergesToAnEmptyRowSetOnBothSides()
    {
        var raw = new List<TableDeltaDto>
        {
            Delta(1, ("order_id", "O1")),
            Delta(1, ("order_id", "O2")),
            Delta(-1, ("order_id", "O1")),
            Delta(-1, ("order_id", "O2")),
        };

        var netted = StreamBridgeService.NetByRowIdentity(raw);

        Assert.Empty(netted);
        Assert.Empty(ApplyAsClientZSet(raw));
        Assert.Empty(ApplyAsClientZSet(netted));
    }
}
