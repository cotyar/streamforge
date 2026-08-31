using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 008 W3 — set-operation tables are pinned to Parallelism = 1: TableDataflowBuilder.Build rejects a
/// union-root CompiledTablePlan unconditionally (mirroring the existing derived-table-in-FROM-position
/// guard right next to it — see that file's own comment on why this check must come BEFORE the
/// `compiled.Sources[0]` dereference, since a union root's Sources list is empty). A real N-ary merge stage
/// would need a new TableStageKind plus breaking the documented "1 or 2 in-edges per stage" invariant —
/// deliberately out of scope for this wave. Same shape as TableCrossJoinTests.CreateDataflowThrowsAboveParallelism1.
/// </summary>
public class UnionParallelismRejectionTests
{
    private static readonly SourceSchema Left = Schema("left_src", ("symbol", FieldKind.String));
    private static readonly SourceSchema Right = Schema("right_src", ("symbol", FieldKind.String));
    private const string Sql = "SELECT symbol FROM left_src UNION ALL SELECT symbol FROM right_src";

    [Fact]
    public void UnionAllTableCompiles()
    {
        var r = CompileTable(Sql, [], [Left, Right]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void CreateDataflowAtParallelism4ThrowsWithTheIntendedMessage()
    {
        var result = CompileTable(Sql, [], [Left, Right]);
        Assert.True(result.Ok, string.Join(";", result.Diagnostics));

        var ex = Assert.Throws<NotSupportedException>(() => result.Plan!.CreateDataflow(4));

        Assert.Contains("Use Parallelism = 1", ex.Message);
        Assert.Contains("UNION", ex.Message);
    }

    [Fact]
    public void CreateDataflowAtParallelism4ThrowsForUnionDistinctToo()
    {
        var distinctSql = "SELECT symbol FROM left_src UNION SELECT symbol FROM right_src";
        var result = CompileTable(distinctSql, [], [Left, Right]);
        Assert.True(result.Ok, string.Join(";", result.Diagnostics));

        var ex = Assert.Throws<NotSupportedException>(() => result.Plan!.CreateDataflow(4));
        Assert.Contains("Use Parallelism = 1", ex.Message);
    }

    [Fact]
    public void CreateDataflowThrowsBeforeEverDereferencingTheEmptySourcesList()
    {
        // A union-root CompiledTablePlan has an EMPTY Sources list (see CompiledTablePlan.UnionBranches's
        // doc comment) — if the union guard in TableDataflowBuilder.Build were missing or ordered after the
        // `compiled.Sources[0].DerivedPlan` check, this would throw IndexOutOfRangeException instead of the
        // clear NotSupportedException asserted in the other tests here. Pinning the exception TYPE (not
        // just message) makes that regression visible even if the message text were to drift.
        var result = CompileTable(Sql, [], [Left, Right]);
        Assert.True(result.Ok, string.Join(";", result.Diagnostics));

        var ex = Record.Exception(() => result.Plan!.CreateDataflow(4));
        Assert.IsType<NotSupportedException>(ex);
    }
}
