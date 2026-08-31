using StreamsForge.Engine.Runtime;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 008 wave 2c-A table-mode outer-join deterministic-replay test, modeled on
/// TableDeterministicReplayTests: three fresh op chains fed an IDENTICAL batch sequence must produce
/// byte-identical output, call for call, delta for delta -- proof the pad/flip machinery doesn't hide any
/// shared/static state or ordering dependency between runs. Each sequence below runs a pad -> product ->
/// pad ROUND TRIP (assert a lone unmatched row, match it, then retract the match) so the flip re-emissions
/// -- the part of TableOuterJoinOp's state machine most likely to leak hidden per-run state -- sit inside
/// the compared window, not just in the final snapshot.
/// </summary>
public class TableOuterJoinReplayTests
{
    private const string LeftSql = "SELECT t.symbol, r.tag FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol";
    private const string FullSql = "SELECT t.symbol, r.tag FROM trades t FULL JOIN ref r ON t.symbol = r.symbol";

    private static List<IReadOnlyList<TableDelta>> RunLeftBatchSequence(TableExecutor exec)
    {
        var perCallOutputs = new List<IReadOnlyList<TableDelta>>();

        perCallOutputs.Add(exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true)))); // pad
        perCallOutputs.Add(exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1))); // product + pad retract
        perCallOutputs.Add(exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1))); // round trip: retract product + repad
        perCallOutputs.Add(exec.OnStreamEvent("trades", Evt(1500, "trades", ("symbol", null), ("price", 1.0), ("qty", 1L), ("active", true)))); // NULL-key pad, never indexed
        perCallOutputs.Add(exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "MSFT"), ("tag", "other")), 1))); // no counterpart -> no output

        return perCallOutputs;
    }

    private static List<IReadOnlyList<TableDelta>> RunFullBatchSequence(TableExecutor exec)
    {
        var perCallOutputs = new List<IReadOnlyList<TableDelta>>();

        perCallOutputs.Add(exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true)))); // left own-pad
        perCallOutputs.Add(exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "MSFT"), ("tag", "other")), 1))); // right own-pad, no counterpart
        perCallOutputs.Add(exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1))); // flip: product + left-pad retract
        perCallOutputs.Add(exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1))); // round trip: retract product + repad left
        perCallOutputs.Add(exec.OnStreamEvent("trades", Evt(1500, "trades", ("symbol", null), ("price", 1.0), ("qty", 1L), ("active", true)))); // left NULL-key pad

        return perCallOutputs;
    }

    private static string Canon(IReadOnlyList<TableDelta> deltas) =>
        string.Join("|", deltas.Select(d => $"{d.Weight}:{JsonText.SerializeCanonicalRow(d.Row)}"));

    private static void AssertReplayIdentical(string sql, Func<TableExecutor, List<IReadOnlyList<TableDelta>>> runBatchSequence)
    {
        var compileResult = CompileTable(sql, [Trades], [Ref]);
        Assert.True(compileResult.Ok, string.Join(";", compileResult.Diagnostics));
        var plan = compileResult.Plan!;

        var runs = Enumerable.Range(0, 3).Select(_ => runBatchSequence(plan.CreateExecutor())).ToList();

        for (int r = 1; r < runs.Count; r++)
        {
            Assert.Equal(runs[0].Count, runs[r].Count);
            for (int i = 0; i < runs[0].Count; i++)
            {
                Assert.Equal(runs[0][i].Count, runs[r][i].Count);
                Assert.Equal(Canon(runs[0][i]), Canon(runs[r][i]));
            }
        }
    }

    [Fact]
    public void LeftJoin_SameBatchSequenceOnThreeFreshExecutorsProducesByteIdenticalOutputs_PadProductPadRoundTrip() =>
        AssertReplayIdentical(LeftSql, RunLeftBatchSequence);

    [Fact]
    public void FullJoin_SameBatchSequenceOnThreeFreshExecutorsProducesByteIdenticalOutputs_PadProductPadRoundTrip() =>
        AssertReplayIdentical(FullSql, RunFullBatchSequence);
}
