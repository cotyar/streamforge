using StreamsForge.Engine.Dataflow;
using StreamsForge.Engine.Runtime.Ops;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 008 W3 — <see cref="TableDistinctOp"/> exercised directly (NOT through TableExecutor's façade), same
/// per-op unit-test style as TableOpsUnitTests. Pins the zero-crossing presence rule (weight &gt; 0, NOT
/// != 0 — see the op's own class doc for why that specific boundary matters) and order independence
/// (a negative delta arriving before its matching positive one is recorded as debt, not dropped).
/// UnionDistinctTableTests covers the same behavior end to end through a compiled UNION query; this file
/// isolates the op's own state transitions.
/// </summary>
public class TableDistinctOpUnitTests
{
    private static readonly Epoch E0 = new(0);

    private static EventRecord Row(string symbol) => Evt(0, "u", ("symbol", symbol));

    [Fact]
    public void FirstAssertionOfARowEmitsPlusOne()
    {
        var op = new TableDistinctOp();

        var outp = op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);

        var d = Assert.Single(outp);
        Assert.Equal(1, d.Weight);
        Assert.Equal("AAPL", d.Row["symbol"]);
    }

    [Fact]
    public void SecondAssertionOfTheIdenticalRowEmitsNothing()
    {
        var op = new TableDistinctOp();
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);

        var outp = op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);

        Assert.Empty(outp);
    }

    [Fact]
    public void ThirdAssertionAlsoEmitsNothing()
    {
        var op = new TableDistinctOp();
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);

        var outp = op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);

        Assert.Empty(outp);
    }

    [Fact]
    public void RetractingOneOfTwoContributionsEmitsNothing()
    {
        var op = new TableDistinctOp();
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);

        var outp = op.OnBatch(E0, [new TableDelta(Row("AAPL"), -1)]);

        Assert.Empty(outp);
    }

    [Fact]
    public void RetractingTheLastContributionEmitsMinusOne()
    {
        var op = new TableDistinctOp();
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), -1)]);

        var outp = op.OnBatch(E0, [new TableDelta(Row("AAPL"), -1)]);

        var d = Assert.Single(outp);
        Assert.Equal(-1, d.Weight);
    }

    [Fact]
    public void OutOfOrderRetractionBeforeAnyAssertionEmitsNothingAndRecordsDebt()
    {
        var op = new TableDistinctOp();

        var outp = op.OnBatch(E0, [new TableDelta(Row("AAPL"), -1)]);

        Assert.Empty(outp); // weight went 0 -> -1: neither endpoint is positive, so no emission
    }

    [Fact]
    public void AssertionThatOnlyCancelsAnEarlierOutOfOrderRetractionEmitsNothing()
    {
        var op = new TableDistinctOp();
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), -1)]); // debt: weight = -1

        var outp = op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]); // weight -1 -> 0

        Assert.Empty(outp); // never crossed into positive — the row was never truly "distinct-present"
    }

    [Fact]
    public void ATrueAssertionAfterTheDebtCancelsOutDoesEmit()
    {
        var op = new TableDistinctOp();
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), -1)]); // weight -1
        op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);  // weight 0 (cancelled, silent)

        var outp = op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]); // weight 0 -> 1: genuine crossing

        var d = Assert.Single(outp);
        Assert.Equal(1, d.Weight);
    }

    [Fact]
    public void DistinctRowsAreTrackedIndependently()
    {
        var op = new TableDistinctOp();

        var outp1 = op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1)]);
        var outp2 = op.OnBatch(E0, [new TableDelta(Row("MSFT"), 1)]);

        Assert.Single(outp1);
        Assert.Single(outp2);
    }

    [Fact]
    public void OnFrontierIsADocumentedPassThrough()
    {
        var op = new TableDistinctOp();
        Assert.Empty(op.OnFrontier(E0));
        Assert.Empty(op.OnFrontier(new Epoch(999)));
    }

    [Fact]
    public void MultipleDeltasInOneBatchAreEachEvaluatedAgainstTheRunningTotal_NotNettedAcrossTheBatch()
    {
        var op = new TableDistinctOp();

        // Within a single batch: assert then retract the SAME row. Each delta is folded into the running
        // weight and evaluated for a crossing IN ORDER (same per-delta model as TableReduceOp's own
        // OnDelta) — so this is +1 (0 -> 1, crossing) immediately followed by -1 (1 -> 0, crossing back),
        // NOT netted into "no change" — that distinction is exactly what lets a genuine same-batch
        // assert-then-retract of the SAME logical row still self-heal correctly one batch later.
        var outp = op.OnBatch(E0, [new TableDelta(Row("AAPL"), 1), new TableDelta(Row("AAPL"), -1)]);

        Assert.Equal(2, outp.Count);
        Assert.Equal(1, outp[0].Weight);
        Assert.Equal(-1, outp[1].Weight);
    }
}
