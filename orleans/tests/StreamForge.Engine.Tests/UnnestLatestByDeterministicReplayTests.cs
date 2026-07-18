using StreamForge.Engine.Runtime;
using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 002 L2/L3 — deterministic-replay proof (same shape as TableDeterministicReplayTests, extended to a
/// plan that combines UNNEST (1-to-N row expansion, weight-linear retraction) AND LATEST BY (per-key
/// running argmax-by-_ts, retract/assert on replacement) in one query: feeds an identical batch sequence —
/// including an out-of-order (ignored) arrival, a same-key replacement, and an upstream retraction that
/// only affects ONE of an unnested row's several element-keys — into two fresh op chains built from the
/// same compiled plan, and asserts byte-identical output sequences.
/// </summary>
public class UnnestLatestByDeterministicReplayTests
{
    private const string Sql =
        "SELECT s.trade_id AS id, l ->> 'ccy' AS ccy, l -> 'notional' AS notional " +
        "FROM structures s, UNNEST(s.legs) AS l " +
        "LATEST BY (s.trade_id, l ->> 'ccy')";

    private static List<object?> Legs(params (string Ccy, double Notional)[] legs) =>
        legs.Select(l => (object?)new Dictionary<string, object?> { ["ccy"] = l.Ccy, ["notional"] = l.Notional }).ToList();

    private static List<IReadOnlyList<TableDelta>> RunBatchSequence(TableExecutor exec)
    {
        var perCallOutputs = new List<IReadOnlyList<TableDelta>>();

        var row1 = Evt(1000, "structures", ("trade_id", "T1"), ("legs", Legs(("USD", 100), ("EUR", 200))), ("payload", null));
        perCallOutputs.Add(exec.OnStreamEvent("structures", row1)); // 2 new keys: assert, assert

        var olderRow = Evt(500, "structures", ("trade_id", "T1"), ("legs", Legs(("USD", 999))), ("payload", null));
        perCallOutputs.Add(exec.OnStreamEvent("structures", olderRow)); // strictly older _ts for (T1,USD): ignored

        var newerRow = Evt(2000, "structures", ("trade_id", "T1"), ("legs", Legs(("USD", 150))), ("payload", null));
        perCallOutputs.Add(exec.OnStreamEvent("structures", newerRow)); // (T1,USD) replaces: retract, assert

        // Retract row1 upstream (e.g. a filtered-out WHERE flip further up a real chain) — its (T1,EUR)
        // element still matches what's currently retained (drop that key); its (T1,USD) element does NOT
        // match what's currently retained (newerRow superseded it) — a documented no-op for that one.
        perCallOutputs.Add(exec.OnTableDelta("structures", new TableDelta(row1, -1)));

        return perCallOutputs;
    }

    private static string Canon(IReadOnlyList<TableDelta> deltas) =>
        string.Join("|", deltas.Select(d => $"{d.Weight}:{JsonText.SerializeCanonicalRow(d.Row)}"));

    [Fact]
    public void UnnestPlusLatestByPlanReplaysDeterministically()
    {
        var compileResult = CompileTable(Sql, [Structures]);
        Assert.True(compileResult.Ok, string.Join(";", compileResult.Diagnostics));
        var plan = compileResult.Plan!;

        var run1 = RunBatchSequence(plan.CreateExecutor());
        var run2 = RunBatchSequence(plan.CreateExecutor());

        Assert.Equal(run1.Count, run2.Count);
        for (int i = 0; i < run1.Count; i++)
        {
            Assert.Equal(run1[i].Count, run2[i].Count);
            Assert.Equal(Canon(run1[i]), Canon(run2[i]));
        }

        // Sanity: the batch actually exercised replacement/retraction/no-op, not a degenerate all-empty run.
        Assert.True(run1.Sum(r => r.Count) > 0);
    }

    [Fact]
    public void ThirdIndependentRunAlsoMatches_NotJustPairwise()
    {
        var compileResult = CompileTable(Sql, [Structures]);
        var plan = compileResult.Plan!;

        var runs = Enumerable.Range(0, 3).Select(_ => RunBatchSequence(plan.CreateExecutor())).ToList();

        var canonical = runs.Select(r => string.Join(";", r.Select(Canon))).Distinct().ToList();
        Assert.Single(canonical);
    }
}
