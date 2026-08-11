using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 008 W3 — `UNION ALL` end to end in TABLE mode. Unlike pipeline mode, table mode's union root needs
/// no dedup op at all: TableExecutorImpl's own consolidation ledger (ApplyConsolidation) already sums
/// weights per canonical row key regardless of which branch a delta came from — concatenating each branch's
/// own emitted TableDeltas straight into that ledger IS UNION ALL's "no dedup, straight weight
/// concatenation" semantics for free (see TableExecutorImpl.EnsureInitUnion's doc comment). These tests
/// prove that weight arithmetic directly: two branches asserting the SAME row nets to weight 2 (not 1 —
/// contrast with UnionDistinctTableTests, where the identical scenario nets to weight 1).
/// </summary>
public class UnionAllTableTests
{
    // Two distinctly-named table inputs with the identical shape, so a canonically-identical row can be fed
    // through either branch and land on the exact same consolidated key.
    private static readonly SourceSchema Left = Schema("left_src", ("symbol", FieldKind.String), ("tag", FieldKind.String));
    private static readonly SourceSchema Right = Schema("right_src", ("symbol", FieldKind.String), ("tag", FieldKind.String));
    private const string Sql = "SELECT symbol, tag FROM left_src UNION ALL SELECT symbol, tag FROM right_src";

    private static TableExecutor CreateExec() => CompileTableAndCreate(Sql, [], [Left, Right]);

    [Fact]
    public void CompilesWithAUnifiedOutputSchema()
    {
        var r = CompileTable(Sql, [], [Left, Right]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(2, r.OutputSchema!.Fields.Count);
        Assert.Equal(new[] { "left_src", "right_src" }.OrderBy(x => x), r.StreamInputs.Concat(r.TableInputs).OrderBy(x => x));
    }

    [Fact]
    public void SingleBranchAssertionProducesWeightOne()
    {
        var exec = CreateExec();

        var delta = Assert.Single(exec.OnTableDelta("left_src", new TableDelta(Evt(0, "left_src", ("symbol", "AAPL"), ("tag", "watch")), 1)));
        Assert.Equal(1, delta.Weight);

        var snapshot = exec.Snapshot();
        var only = Assert.Single(snapshot.Values);
        Assert.Equal(1, only.Weight);
    }

    [Fact]
    public void IdenticalRowAssertedFromBothBranchesSumsWeightsToTwo_NoDedup()
    {
        var exec = CreateExec();

        exec.OnTableDelta("left_src", new TableDelta(Evt(0, "left_src", ("symbol", "AAPL"), ("tag", "watch")), 1));
        exec.OnTableDelta("right_src", new TableDelta(Evt(0, "right_src", ("symbol", "AAPL"), ("tag", "watch")), 1));

        var snapshot = exec.Snapshot();
        var only = Assert.Single(snapshot.Values);
        Assert.Equal(2, only.Weight); // UNION ALL: no dedup — contrast with UnionDistinctTableTests (weight 1)
    }

    [Fact]
    public void RetractingOneBranchLeavesTheOtherBranchsAssertionStanding()
    {
        var exec = CreateExec();
        var row = () => Evt(0, "left_src", ("symbol", "AAPL"), ("tag", "watch"));
        var rowR = () => Evt(0, "right_src", ("symbol", "AAPL"), ("tag", "watch"));

        exec.OnTableDelta("left_src", new TableDelta(row(), 1));
        exec.OnTableDelta("right_src", new TableDelta(rowR(), 1));
        Assert.Equal(2, exec.Snapshot().Values.Single().Weight);

        var retracted = exec.OnTableDelta("left_src", new TableDelta(row(), -1));
        var d = Assert.Single(retracted);
        Assert.Equal(-1, d.Weight);

        Assert.Equal(1, exec.Snapshot().Values.Single().Weight);
    }

    [Fact]
    public void RetractingBothBranchesEmptiesTheSnapshot()
    {
        var exec = CreateExec();
        var row = () => Evt(0, "left_src", ("symbol", "AAPL"), ("tag", "watch"));
        var rowR = () => Evt(0, "right_src", ("symbol", "AAPL"), ("tag", "watch"));

        exec.OnTableDelta("left_src", new TableDelta(row(), 1));
        exec.OnTableDelta("right_src", new TableDelta(rowR(), 1));
        exec.OnTableDelta("left_src", new TableDelta(row(), -1));
        exec.OnTableDelta("right_src", new TableDelta(rowR(), -1));

        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void DistinctRowsFromEachBranchBothAppearIndependently()
    {
        var exec = CreateExec();

        exec.OnTableDelta("left_src", new TableDelta(Evt(0, "left_src", ("symbol", "AAPL"), ("tag", "watch")), 1));
        exec.OnTableDelta("right_src", new TableDelta(Evt(0, "right_src", ("symbol", "MSFT"), ("tag", "core")), 1));

        var snapshot = exec.Snapshot();
        Assert.Equal(2, snapshot.Count);
        var symbols = snapshot.Values.Select(v => (string)v.Row["symbol"]!).OrderBy(x => x).ToList();
        Assert.Equal(new[] { "AAPL", "MSFT" }, symbols);
    }

    [Fact]
    public void PlanSummaryNamesUnionAll()
    {
        var r = CompileTable(Sql, [], [Left, Right]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("UNION ALL", r.PlanSummary);
        Assert.Contains("SELECT 2", r.PlanSummary);
    }
}
