using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 008 W3 — `UNION` (distinct) end to end in TABLE mode: the only mode that supports it (pipeline mode
/// rejects it — see SetOperationValidatorTests). Wires a <see cref="TableDistinctOp"/> downstream of the
/// union's own branch concatenation (see TableExecutorImpl.EnsureInitUnion) — these tests exercise the
/// user-visible dedup behavior end to end; TableDistinctOpUnitTests exercises the op itself in isolation.
/// Highest-value cases per plan 008: a row asserted from both branches emits exactly one +1; retracting one
/// branch emits nothing; retracting both emits -1; an out-of-order retraction (arriving before its
/// assertion) self-heals.
/// </summary>
public class UnionDistinctTableTests
{
    private static readonly SourceSchema Left = Schema("left_src", ("symbol", FieldKind.String), ("tag", FieldKind.String));
    private static readonly SourceSchema Right = Schema("right_src", ("symbol", FieldKind.String), ("tag", FieldKind.String));
    private const string Sql = "SELECT symbol, tag FROM left_src UNION SELECT symbol, tag FROM right_src";

    private static readonly System.Func<EventRecord> LeftRow = () => Evt(0, "left_src", ("symbol", "AAPL"), ("tag", "watch"));
    private static readonly System.Func<EventRecord> RightRow = () => Evt(0, "right_src", ("symbol", "AAPL"), ("tag", "watch"));

    private static TableExecutor CreateExec() => CompileTableAndCreate(Sql, [], [Left, Right]);

    [Fact]
    public void CompilesAndPlanSummaryNamesPlainUnion()
    {
        var r = CompileTable(Sql, [], [Left, Right]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("UNION", r.PlanSummary);
        Assert.DoesNotContain("UNION ALL", r.PlanSummary);
    }

    [Fact]
    public void FirstBranchsAssertionEmitsExactlyOnePlusOne()
    {
        var exec = CreateExec();

        var delta = Assert.Single(exec.OnTableDelta("left_src", new TableDelta(LeftRow(), 1)));
        Assert.Equal(1, delta.Weight);
        Assert.Equal(1, exec.Snapshot().Values.Single().Weight);
    }

    [Fact]
    public void SecondBranchsAssertionOfTheIdenticalRowEmitsNothing()
    {
        var exec = CreateExec();

        exec.OnTableDelta("left_src", new TableDelta(LeftRow(), 1));
        var secondEmission = exec.OnTableDelta("right_src", new TableDelta(RightRow(), 1));

        Assert.Empty(secondEmission); // no +1 fired — the row was already present from branch 0
        Assert.Equal(1, exec.Snapshot().Values.Single().Weight); // distinct: weight stays 1, not 2
    }

    [Fact]
    public void RetractingOneOfTwoAssertingBranchesEmitsNothing()
    {
        var exec = CreateExec();
        exec.OnTableDelta("left_src", new TableDelta(LeftRow(), 1));
        exec.OnTableDelta("right_src", new TableDelta(RightRow(), 1));

        var retraction = exec.OnTableDelta("left_src", new TableDelta(LeftRow(), -1));

        Assert.Empty(retraction); // the OTHER branch still asserts this row — nothing to retract yet
        Assert.Single(exec.Snapshot());
        Assert.Equal(1, exec.Snapshot().Values.Single().Weight);
    }

    [Fact]
    public void RetractingBothAssertingBranchesEmitsMinusOne()
    {
        var exec = CreateExec();
        exec.OnTableDelta("left_src", new TableDelta(LeftRow(), 1));
        exec.OnTableDelta("right_src", new TableDelta(RightRow(), 1));
        exec.OnTableDelta("left_src", new TableDelta(LeftRow(), -1));

        var finalRetraction = exec.OnTableDelta("right_src", new TableDelta(RightRow(), -1));

        var d = Assert.Single(finalRetraction);
        Assert.Equal(-1, d.Weight);
        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void ARetractionArrivingBeforeItsAssertionSelfHeals()
    {
        var exec = CreateExec();

        // Out-of-order: a retraction for a row NEITHER branch has ever asserted yet.
        var earlyRetraction = exec.OnTableDelta("left_src", new TableDelta(LeftRow(), -1));
        Assert.Empty(earlyRetraction); // never claimed presence — nothing to retract

        // The matching assertion arrives later — nets against the debt, still never surfaces.
        var lateAssertion = exec.OnTableDelta("left_src", new TableDelta(LeftRow(), 1));
        Assert.Empty(lateAssertion);
        Assert.Empty(exec.Snapshot());

        // A THIRD, genuinely-new assertion (from the other branch) is the true first presence — and DOES surface.
        var trueAssertion = exec.OnTableDelta("right_src", new TableDelta(RightRow(), 1));
        var d = Assert.Single(trueAssertion);
        Assert.Equal(1, d.Weight);
        Assert.Equal(1, exec.Snapshot().Values.Single().Weight);
    }

    [Fact]
    public void DistinctRowsFromEachBranchBothAppearIndependently()
    {
        var exec = CreateExec();

        exec.OnTableDelta("left_src", new TableDelta(Evt(0, "left_src", ("symbol", "AAPL"), ("tag", "watch")), 1));
        exec.OnTableDelta("right_src", new TableDelta(Evt(0, "right_src", ("symbol", "MSFT"), ("tag", "core")), 1));

        var snapshot = exec.Snapshot();
        Assert.Equal(2, snapshot.Count);
        Assert.All(snapshot.Values, v => Assert.Equal(1, v.Weight));
    }

    [Fact]
    public void DuplicateAssertionsFromTheSameBranchAlsoDedup()
    {
        // Not just cross-branch — two identical asserting deltas from the SAME branch (e.g. a source that
        // legitimately re-sends) must dedup too, since DISTINCT counts contributing weight, not distinct
        // branches specifically.
        var exec = CreateExec();

        exec.OnTableDelta("left_src", new TableDelta(LeftRow(), 1));
        var second = exec.OnTableDelta("left_src", new TableDelta(LeftRow(), 1));

        Assert.Empty(second);
        Assert.Equal(1, exec.Snapshot().Values.Single().Weight);
    }
}
