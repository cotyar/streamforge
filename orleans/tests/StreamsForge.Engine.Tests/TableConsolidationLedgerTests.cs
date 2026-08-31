using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Direct, join/aggregate-free proof of TableExecutorImpl.ApplyConsolidation's order-independence fix (see
/// that method's own doc comment, and the `_debtWeights` field doc above it, for the argument this file
/// exercises empirically). TableOuterJoinPartitionedTests covers the same fix through a FULL OUTER join's
/// own retraction-driven pad — this file isolates the ledger itself: every test here drives a plain
/// passthrough table-mode plan ("SELECT symbol, price FROM prices", no JOIN, no GROUP BY) so a call to
/// OnTableDelta's weight flows straight through TableIngestOp and TableFilterProjectOp.OnBatchTerminal
/// unchanged and lands in ApplyConsolidation exactly as given — full, direct arithmetic control over the
/// ledger with none of a join or aggregate's own retraction/assertion re-emission in the way.
/// </summary>
public class TableConsolidationLedgerTests
{
    private static readonly SourceSchema Prices = Schema("prices", ("symbol", FieldKind.String), ("price", FieldKind.Double));

    private static TableExecutor CreatePassthrough() =>
        CompileTableAndCreate("SELECT symbol, price FROM prices", [], [Prices]);

    private static EventRecord Row(string symbol, double price) => Evt(0, "prices", ("symbol", symbol), ("price", price));

    private static TableDelta Delta(string symbol, double price, long weight) => new(Row(symbol, price), weight);

    private static string Canon(TableExecutor exec) =>
        string.Join("\n", exec.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void NegativeDeltaArrivingBeforeItsAssertion_NetsToNothing()
    {
        var exec = CreatePassthrough();

        // The retraction arrives FIRST — no matching assertion has ever been seen for this canonical row.
        exec.OnTableDelta("prices", Delta("AAPL", 100.0, -1));
        Assert.Empty(exec.Snapshot()); // never exposed — Snapshot() never shows a non-positive row
        Assert.Equal(1, exec.DebtCount); // but NOT silently dropped either — recorded as outstanding debt

        // The assertion the retraction was always going to cancel, admitted second.
        exec.OnTableDelta("prices", Delta("AAPL", 100.0, 1));

        // Correct net across the whole sequence is 0 — the row must never resurface at a spurious +1 the
        // way the old ("discard negative deltas for absent keys") ledger would have produced.
        Assert.Empty(exec.Snapshot());
        Assert.Equal(0, exec.DebtCount);
    }

    [Fact]
    public void RowThatNetsToExactlyZero_LeavesNoEntryAndNoResidualDebt()
    {
        var exec = CreatePassthrough();

        exec.OnTableDelta("prices", Delta("AAPL", 100.0, 1)); // ordinary causal-order assertion
        Assert.Single(exec.Snapshot());
        Assert.Equal(0, exec.DebtCount);

        exec.OnTableDelta("prices", Delta("AAPL", 100.0, -1)); // ordinary causal-order retraction

        // No entry in the positive ledger...
        Assert.Empty(exec.Snapshot());
        // ...and no residual debt either — the fix must not trade the original correctness bug for a memory
        // leak in the new side table (see TableExecutorImpl's own "Removal at exactly zero must still
        // prune" requirement).
        Assert.Equal(0, exec.DebtCount);
    }

    [Fact]
    public void InterleavingsOfSameMultiset_ConvergeToIdenticalSnapshot()
    {
        // AAPL nets to +1 (1 + 1 - 1), MSFT nets to 0 (vanishes entirely), GOOG's retraction arrives before
        // its own assertion (nets to +1: -1 + 2) — a mix deliberately chosen to exercise every ledger
        // transition (fresh positive, accumulate, net-to-zero, debt-then-cancel-into-positive) at once.
        (string Symbol, double Price, long Weight)[] multiset =
        [
            ("AAPL", 100.0, 1),
            ("AAPL", 100.0, 1),
            ("AAPL", 100.0, -1),
            ("MSFT", 200.0, 1),
            ("MSFT", 200.0, -1),
            ("GOOG", 50.0, -1),
            ("GOOG", 50.0, 2),
        ];

        var forward = CreatePassthrough();
        foreach (var (s, p, w) in multiset) forward.OnTableDelta("prices", Delta(s, p, w));
        var forwardCanon = Canon(forward);

        Assert.Equal(2, forward.Snapshot().Count); // AAPL=+1, GOOG=+1 — MSFT fully cancelled out
        Assert.Equal(0, forward.DebtCount);

        var reversed = CreatePassthrough();
        foreach (var (s, p, w) in multiset.Reverse()) reversed.OnTableDelta("prices", Delta(s, p, w));
        Assert.Equal(forwardCanon, Canon(reversed));
        Assert.Equal(0, reversed.DebtCount);

        // A third interleaving that is neither forward nor fully reversed: GOOG's retraction (index 5)
        // pulled to the very front, everything else left in its original relative order.
        List<(string, double, long)> custom = [multiset[5]];
        custom.AddRange(multiset.Where((_, i) => i != 5));
        var interleaved = CreatePassthrough();
        foreach (var (s, p, w) in custom) interleaved.OnTableDelta("prices", Delta(s, p, w));
        Assert.Equal(forwardCanon, Canon(interleaved));
        Assert.Equal(0, interleaved.DebtCount);
    }

    [Theory]
    [InlineData(new long[] { 3, -1, -2 }, 0L)] // forward: +3 then -1 then -2 — nets to 0
    [InlineData(new long[] { -2, -1, 3 }, 0L)] // fully reversed order of the same multiset
    [InlineData(new long[] { 3, -1, -2, 5 }, 5L)] // same shape, plus a trailing +5 — nets to +5
    [InlineData(new long[] { 5, -2, -1, 3 }, 5L)] // reversed order of that multiset
    public void MultiWeightSequences_ConvergeToTheDeltaSum_RegardlessOfOrder(long[] weightsInOrder, long expectedFinalWeight)
    {
        var exec = CreatePassthrough();
        foreach (var w in weightsInOrder) exec.OnTableDelta("prices", Delta("AAPL", 100.0, w));

        if (expectedFinalWeight == 0)
        {
            Assert.Empty(exec.Snapshot());
        }
        else
        {
            var key = exec.CanonicalRowKey(Row("AAPL", 100.0));
            Assert.Equal(expectedFinalWeight, exec.Snapshot()[key].Weight);
        }

        // Every sequence here fully resolves (no still-pending negative running total) — none of them are
        // meant to end mid-debt, so the side table must always drain back to empty too.
        Assert.Equal(0, exec.DebtCount);
    }

    [Fact]
    public void SnapshotNeverContainsANonPositiveWeight()
    {
        var exec = CreatePassthrough();
        (string Symbol, double Price, long Weight)[] rows =
        [
            ("AAPL", 100.0, -1), ("AAPL", 100.0, 1), ("AAPL", 100.0, 1), // net +1, reached via a debt-first path
            ("MSFT", 200.0, 1), ("MSFT", 200.0, -1), // net 0
            ("GOOG", 50.0, -3), ("GOOG", 50.0, 1), // net -2 — must stay OUT of the snapshot
            ("TSLA", 10.0, 2), ("TSLA", 10.0, -5), // net -3 — must stay OUT of the snapshot too
        ];
        foreach (var (s, p, w) in rows) exec.OnTableDelta("prices", Delta(s, p, w));

        Assert.All(exec.Snapshot().Values, v => Assert.True(v.Weight > 0));

        // GOOG (-2) and TSLA (-3) never crossed into positive territory — confirm they were tracked as
        // debt, not silently lost the way the pre-fix ledger would have lost them.
        Assert.Equal(2, exec.DebtCount);
        Assert.Single(exec.Snapshot()); // only AAPL (net +1) is user-visible
    }
}
