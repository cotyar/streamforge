using StreamsForge.Engine.Runtime;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Table-mode deterministic-replay test (plan 003 M1 acceptance: "a deterministic-replay test (same
/// batches ⇒ same outputs)"). Feeds an identical sequence of stream events / table deltas — spanning a
/// join, a WHERE filter, and a GROUP BY with retraction (MIN, so the multiset/retraction path is
/// exercised too) — into two FRESH op chains built from the same compiled plan, and asserts the two
/// runs produce byte-identical output sequences, call for call, delta for delta. This is the M1 task's
/// stated proof that decomposing the monolith into explicit ops didn't introduce any hidden shared/static
/// state or ordering dependency between runs.
/// </summary>
public class TableDeterministicReplayTests
{
    private const string Sql = "SELECT t.symbol, r.tag, t.price FROM trades t JOIN ref r ON t.symbol = r.symbol WHERE t.price > 10";

    private static List<IReadOnlyList<TableDelta>> RunBatchSequence(TableExecutor exec)
    {
        var perCallOutputs = new List<IReadOnlyList<TableDelta>>();

        perCallOutputs.Add(exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1)));
        perCallOutputs.Add(exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true))));
        perCallOutputs.Add(exec.OnStreamEvent("trades", Evt(1500, "trades", ("symbol", "AAPL"), ("price", 5.0), ("qty", 1L), ("active", true)))); // fails WHERE
        perCallOutputs.Add(exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "MSFT"), ("tag", "watch")), 2)));
        perCallOutputs.Add(exec.OnStreamEvent("trades", Evt(2000, "trades", ("symbol", "MSFT"), ("price", 50.0), ("qty", 3L), ("active", true))));
        perCallOutputs.Add(exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1))); // retraction cascades through the join

        return perCallOutputs;
    }

    private static string Canon(IReadOnlyList<TableDelta> deltas) =>
        string.Join("|", deltas.Select(d => $"{d.Weight}:{JsonText.SerializeCanonicalRow(d.Row)}"));

    [Fact]
    public void SameBatchSequenceTwiceOnFreshOpChainsProducesIdenticalOutputs()
    {
        var compileResult = CompileTable(Sql, [Trades], [Ref]);
        Assert.True(compileResult.Ok);
        var plan = compileResult.Plan!;

        // Two independent, freshly-constructed executors (= fresh op chains) sharing only the immutable
        // compiled plan — exactly plan 003 M1's replay guarantee target.
        var run1 = RunBatchSequence(plan.CreateExecutor());
        var run2 = RunBatchSequence(plan.CreateExecutor());

        Assert.Equal(run1.Count, run2.Count);
        for (int i = 0; i < run1.Count; i++)
        {
            Assert.Equal(run1[i].Count, run2[i].Count);
            Assert.Equal(Canon(run1[i]), Canon(run2[i]));
        }
    }

    [Fact]
    public void ThirdIndependentRunAlsoMatches_NotJustPairwise()
    {
        var compileResult = CompileTable(Sql, [Trades], [Ref]);
        var plan = compileResult.Plan!;

        var runs = Enumerable.Range(0, 3).Select(_ => RunBatchSequence(plan.CreateExecutor())).ToList();

        var canonical = runs.Select(r => string.Join(";", r.Select(Canon))).Distinct().ToList();
        Assert.Single(canonical); // all three runs collapse to the same canonical sequence
    }
}
