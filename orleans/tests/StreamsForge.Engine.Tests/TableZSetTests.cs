using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>Z-set aggregation semantics: running (unwindowed) GROUP BY aggregates that emit
/// retraction/assertion pairs as groups change, and vanish (retraction only) when their contributing
/// weight reaches zero. Retraction requires a weighted delta — raw stream events always arrive at +1, so
/// these tests drive the executor via OnTableDelta (simulating an upstream table's output), exactly the
/// path a real table-over-table chain uses.</summary>
public class TableZSetTests
{
    private static readonly SourceSchema Prices = Schema("prices", ("symbol", FieldKind.String), ("price", FieldKind.Double), ("id", FieldKind.String));

    private static TableExecutor CreateCountBySymbol() =>
        CompileTableAndCreate("SELECT symbol, COUNT(*) AS cnt FROM prices GROUP BY symbol", [], [Prices]);

    private static EventRecord Row(string symbol, double price, string id) =>
        Evt(0, "prices", ("symbol", symbol), ("price", price), ("id", id));

    [Fact]
    public void FirstContributionToAGroupEmitsOnlyAnAssertion()
    {
        var exec = CreateCountBySymbol();
        var deltas = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), 1));

        var assertion = Assert.Single(deltas);
        Assert.Equal(1, assertion.Weight);
        Assert.Equal(1L, assertion.Row["cnt"]);
        Assert.Equal("AAPL", assertion.Row["symbol"]);
    }

    [Fact]
    public void SubsequentContributionEmitsRetractThenAssert()
    {
        var exec = CreateCountBySymbol();
        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), 1));

        var deltas = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 101.0, "r2"), 1));

        Assert.Equal(2, deltas.Count);
        Assert.Equal(-1, deltas[0].Weight);
        Assert.Equal(1L, deltas[0].Row["cnt"]);
        Assert.Equal(1, deltas[1].Weight);
        Assert.Equal(2L, deltas[1].Row["cnt"]);
    }

    [Fact]
    public void GroupVanishesWhenContributingWeightReachesZero()
    {
        var exec = CreateCountBySymbol();
        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), 1));

        var deltas = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), -1));

        // Group's total weight went from 1 to 0 — only the retraction is emitted, group is dropped.
        var retraction = Assert.Single(deltas);
        Assert.Equal(-1, retraction.Weight);
        Assert.Equal(1L, retraction.Row["cnt"]);

        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void MinRetractionWithDuplicates_RemovingOneLeavesMinUnchanged_RemovingBothExposesNextValue()
    {
        var exec = CompileTableAndCreate("SELECT symbol, MIN(price) AS low FROM prices GROUP BY symbol", [], [Prices]);

        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), 1)); // low=100
        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r2"), 1)); // still low=100 (dup min)
        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 105.0, "r3"), 1)); // still low=100

        var afterRemoveOne = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), -1));
        var lastAssertAfterRemoveOne = afterRemoveOne[^1];
        Assert.Equal(100.0, lastAssertAfterRemoveOne.Row["low"]); // r2 still contributes 100 — min unchanged

        var afterRemoveBoth = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r2"), -1));
        var lastAssertAfterRemoveBoth = afterRemoveBoth[^1];
        Assert.Equal(105.0, lastAssertAfterRemoveBoth.Row["low"]); // both 100s gone — min moves to next value
    }

    [Fact]
    public void MaxRetractionWithDuplicates_RemovingOneLeavesMaxUnchanged_RemovingBothExposesNextValue()
    {
        var exec = CompileTableAndCreate("SELECT symbol, MAX(price) AS high FROM prices GROUP BY symbol", [], [Prices]);

        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), 1));
        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r2"), 1));
        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 95.0, "r3"), 1));

        var afterRemoveOne = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), -1));
        Assert.Equal(100.0, afterRemoveOne[^1].Row["high"]);

        var afterRemoveBoth = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r2"), -1));
        Assert.Equal(95.0, afterRemoveBoth[^1].Row["high"]);
    }

    [Fact]
    public void AvgIsCorrectUnderRetraction()
    {
        var exec = CompileTableAndCreate("SELECT symbol, AVG(price) AS avgp FROM prices GROUP BY symbol", [], [Prices]);

        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), 1));
        var afterSecond = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 200.0, "r2"), 1));
        Assert.Equal(150.0, afterSecond[^1].Row["avgp"]);

        var afterRetract = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), -1));
        Assert.Equal(200.0, afterRetract[^1].Row["avgp"]);

        var afterReAdd = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), 1));
        Assert.Equal(150.0, afterReAdd[^1].Row["avgp"]);
    }

    [Fact]
    public void SumIsSubtractableUnderRetraction()
    {
        var exec = CompileTableAndCreate("SELECT symbol, SUM(price) AS total FROM prices GROUP BY symbol", [], [Prices]);

        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), 1));
        var afterSecond = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 50.0, "r2"), 1));
        Assert.Equal(150.0, afterSecond[^1].Row["total"]);

        var afterRetract = exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), -1));
        Assert.Equal(50.0, afterRetract[^1].Row["total"]);
    }

    [Fact]
    public void GlobalAggregateWithoutGroupByIsASingleGroup()
    {
        var exec = CompileTableAndCreate("SELECT COUNT(*) AS cnt FROM prices", [], [Prices]);

        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), 1));
        var afterSecond = exec.OnTableDelta("prices", new TableDelta(Row("MSFT", 200.0, "r2"), 1));

        Assert.Equal(2L, afterSecond[^1].Row["cnt"]);
    }

    [Fact]
    public void EndToEndSnapshotReflectsConsolidatedState()
    {
        var exec = CreateCountBySymbol();
        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 100.0, "r1"), 1));
        exec.OnTableDelta("prices", new TableDelta(Row("AAPL", 101.0, "r2"), 1));
        exec.OnTableDelta("prices", new TableDelta(Row("MSFT", 300.0, "r3"), 1));

        var snapshot = exec.Snapshot();
        Assert.Equal(2, snapshot.Count);
        Assert.Contains(snapshot.Values, v => Equals(v.Row["symbol"], "AAPL") && Equals(v.Row["cnt"], 2L));
        Assert.Contains(snapshot.Values, v => Equals(v.Row["symbol"], "MSFT") && Equals(v.Row["cnt"], 1L));
    }
}
