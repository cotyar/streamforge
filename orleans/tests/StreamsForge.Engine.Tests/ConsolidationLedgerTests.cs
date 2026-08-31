using StreamsForge.Engine.Runtime;
using Xunit;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 009 wave D: direct unit tests for the extracted <see cref="ConsolidationLedger"/> itself — the type
/// factored out of three previously hand-written, separately-maintained copies (TableExecutorImpl's own
/// `_consolidated`/`_debtWeights`, TableGrain's coordinator-mode `_coordinatorSnapshot`/`_coordinatorDebt`,
/// and ArrangementGrain's per-partition `_index`/`_indexDebt`). TableConsolidationLedgerTests.cs already
/// proves the same order-independence property end-to-end through TableExecutor's public SQL surface (a
/// real compiled plan feeding OnTableDelta); this file instead drives <see cref="ConsolidationLedger"/>
/// directly, with no SQL/plan/op machinery at all — the narrowest possible proof that the extracted type
/// itself is correct, independent of any one of its three call sites.
/// </summary>
public class ConsolidationLedgerTests
{
    private static EventRecord Row(string symbol) => new() { ["symbol"] = symbol };

    [Fact]
    public void RetractionArrivingBeforeItsAssertion_NetsCorrectly()
    {
        var ledger = new ConsolidationLedger();

        // The retraction arrives FIRST — no matching assertion has ever been folded in for this key.
        ledger.Apply("AAPL", Row("AAPL"), -1);
        Assert.Empty(ledger.Visible); // never visible — a non-positive weight is never exposed
        Assert.Equal(1, ledger.DebtCount); // but retained as debt, not silently dropped

        // The assertion the retraction was always going to cancel, folded in second.
        ledger.Apply("AAPL", Row("AAPL"), 1);

        // Correct net across the whole sequence is 0 — must never resurface at a spurious +1 the way
        // discarding the out-of-order negative delta (the bug commit 9de443e fixed) would have produced.
        Assert.Empty(ledger.Visible);
        Assert.Equal(0, ledger.DebtCount);
    }

    [Fact]
    public void ZeroCrossing_PositiveToNegative_MovesFromVisibleToDebt()
    {
        var ledger = new ConsolidationLedger();

        ledger.Apply("AAPL", Row("AAPL"), 2);
        Assert.True(ledger.Visible.ContainsKey("AAPL"));
        Assert.Equal(2, ledger.Visible["AAPL"].Weight);
        Assert.Equal(0, ledger.DebtCount);

        // -3 crosses zero: 2 + (-3) = -1, strictly negative.
        ledger.Apply("AAPL", Row("AAPL"), -3);
        Assert.False(ledger.Visible.ContainsKey("AAPL"));
        Assert.Equal(1, ledger.DebtCount);
    }

    [Fact]
    public void ZeroCrossing_NegativeToPositive_MovesFromDebtToVisible()
    {
        var ledger = new ConsolidationLedger();

        ledger.Apply("AAPL", Row("AAPL"), -1);
        Assert.Equal(1, ledger.DebtCount);
        Assert.Empty(ledger.Visible);

        // +4 crosses zero: -1 + 4 = 3, strictly positive.
        ledger.Apply("AAPL", Row("AAPL"), 4);
        Assert.Equal(0, ledger.DebtCount);
        Assert.Equal(3, ledger.Visible["AAPL"].Weight);
    }

    [Fact]
    public void FullyCancelledKey_LeavesNoResidueInEitherMap()
    {
        var ledger = new ConsolidationLedger();

        ledger.Apply("AAPL", Row("AAPL"), 1);
        ledger.Apply("AAPL", Row("AAPL"), 1);
        ledger.Apply("AAPL", Row("AAPL"), -2); // nets to exactly 0

        Assert.Empty(ledger.Visible);
        Assert.Equal(0, ledger.DebtCount);

        // A fresh assertion afterward must start from a clean slate (0), not resurrect any leftover debt.
        ledger.Apply("AAPL", Row("AAPL"), 1);
        Assert.Equal(1, ledger.Visible["AAPL"].Weight);
    }

    [Fact]
    public void Visible_IsTheSameDictionaryInstanceAcrossCalls()
    {
        // TableExecutor.Snapshot() depends on this: the `/rows` read path holds onto the reference rather
        // than copying, so Visible must return the SAME instance every time, live-updated in place.
        var ledger = new ConsolidationLedger();
        var first = ledger.Visible;
        ledger.Apply("AAPL", Row("AAPL"), 1);
        var second = ledger.Visible;

        Assert.Same(first, second);
        Assert.True(second.ContainsKey("AAPL")); // the mutation is visible through the held-onto reference
    }

    [Fact]
    public void Seed_PopulatesVisibleWithoutTouchingDebtOrRunningArithmetic()
    {
        // ArrangementGrain's rebuild-from-checkpoint path: the checkpoint only ever holds positive-weight
        // rows, so Seed bypasses Apply's weight-folding entirely.
        var ledger = new ConsolidationLedger();
        ledger.Seed("AAPL", Row("AAPL"), 5);

        Assert.Equal(5, ledger.Visible["AAPL"].Weight);
        Assert.Equal(0, ledger.DebtCount);

        // A subsequent Apply folds against the seeded weight exactly like it would against one reached via
        // ordinary Apply calls.
        ledger.Apply("AAPL", Row("AAPL"), -5);
        Assert.Empty(ledger.Visible);
        Assert.Equal(0, ledger.DebtCount);
    }

    [Fact]
    public void Clear_ResetsBothMapsToEmpty()
    {
        var ledger = new ConsolidationLedger();
        ledger.Apply("AAPL", Row("AAPL"), 1);
        ledger.Apply("MSFT", Row("MSFT"), -1);
        Assert.NotEmpty(ledger.Visible);
        Assert.NotEqual(0, ledger.DebtCount);

        ledger.Clear();

        Assert.Empty(ledger.Visible);
        Assert.Equal(0, ledger.DebtCount);
    }

    [Theory]
    [InlineData(new long[] { 3, -1, -2 }, 0L)] // forward: +3 then -1 then -2 — nets to 0
    [InlineData(new long[] { -2, -1, 3 }, 0L)] // fully reversed order of the same multiset
    [InlineData(new long[] { 3, -1, -2, 5 }, 5L)] // same shape, plus a trailing +5 — nets to +5
    [InlineData(new long[] { 5, -2, -1, 3 }, 5L)] // reversed order of that multiset
    public void ArrivalOrder_NeverAffectsTheFinalWeight_OnlyTheSumOfDeltasDoes(long[] weightsInOrder, long expectedFinalWeight)
    {
        var ledger = new ConsolidationLedger();
        foreach (var w in weightsInOrder) ledger.Apply("AAPL", Row("AAPL"), w);

        if (expectedFinalWeight == 0)
        {
            Assert.Empty(ledger.Visible);
        }
        else
        {
            Assert.Equal(expectedFinalWeight, ledger.Visible["AAPL"].Weight);
        }

        Assert.Equal(0, ledger.DebtCount); // every sequence here fully resolves — none end mid-debt
    }
}
