using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 008 W3 — branch-compatibility validation for `UNION`/`UNION ALL`: arity, positional kind
/// unification (Long+Double → Double, everything else must match exactly, Json/Timestamp only with
/// themselves), output column naming from branch 0, pipeline-mode UNION (distinct) rejection naming
/// UNION ALL as the fix, and end-to-end confirmation that IN/EXISTS/scalar-subquery position rejects a
/// set operation (parser-level mechanics are SetOperationParserTests' job — these assert on the compiled
/// Diagnostics/OutputSchema an end user actually sees).
/// </summary>
public class SetOperationValidatorTests
{
    // Local fixtures — deliberately different shapes from TestHelpers' own schemas so arity/kind mismatches
    // are easy to construct without perturbing anything else.
    private static readonly SourceSchema OneStringCol = Schema("one_col", ("a", FieldKind.String));
    private static readonly SourceSchema TwoStringCols = Schema("two_col", ("a", FieldKind.String), ("b", FieldKind.String));
    private static readonly SourceSchema LongCol = Schema("long_src", ("a", FieldKind.Long));
    private static readonly SourceSchema DoubleCol = Schema("double_src", ("a", FieldKind.Double));
    private static readonly SourceSchema StringCol = Schema("string_src", ("a", FieldKind.String));
    private static readonly SourceSchema BoolCol = Schema("bool_src", ("a", FieldKind.Bool));
    private static readonly SourceSchema JsonCol = Schema("json_src", ("a", FieldKind.Json));
    private static readonly SourceSchema TimestampCol = Schema("ts_src", ("a", FieldKind.Timestamp));

    // ------------------------------------------------------------------
    // Arity
    // ------------------------------------------------------------------

    [Fact]
    public void ArityMismatchIsRejected()
    {
        var r = CompileTable("SELECT a FROM one_col UNION ALL SELECT a, b FROM two_col", [], [OneStringCol, TwoStringCols]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("same number of columns"));
    }

    [Fact]
    public void EqualAritySucceeds()
    {
        var r = CompileTable("SELECT a FROM long_src UNION ALL SELECT a FROM long_src", [], [LongCol]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    // ------------------------------------------------------------------
    // Kind unification
    // ------------------------------------------------------------------

    [Fact]
    public void LongAndDoubleUnifyToDouble()
    {
        var r = CompileTable("SELECT a FROM long_src UNION ALL SELECT a FROM double_src", [], [LongCol, DoubleCol]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.Double, r.OutputSchema!.Fields["a"]);
    }

    [Fact]
    public void DoubleAndLongUnifyToDoubleRegardlessOfBranchOrder()
    {
        var r = CompileTable("SELECT a FROM double_src UNION ALL SELECT a FROM long_src", [], [DoubleCol, LongCol]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.Double, r.OutputSchema!.Fields["a"]);
    }

    [Fact]
    public void IdenticalKindsUnifyToThemselves()
    {
        var r = CompileTable("SELECT a FROM string_src UNION ALL SELECT a FROM string_src", [], [StringCol]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.String, r.OutputSchema!.Fields["a"]);
    }

    [Fact]
    public void StringAndLongDoNotUnify()
    {
        var r = CompileTable("SELECT a FROM string_src UNION ALL SELECT a FROM long_src", [], [StringCol, LongCol]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("not compatible"));
    }

    [Fact]
    public void BoolAndStringDoNotUnify()
    {
        var r = CompileTable("SELECT a FROM bool_src UNION ALL SELECT a FROM string_src", [], [BoolCol, StringCol]);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("not compatible"));
    }

    [Fact]
    public void JsonOnlyUnifiesWithJson()
    {
        var ok = CompileTable("SELECT a FROM json_src UNION ALL SELECT a FROM json_src", [], [JsonCol]);
        Assert.True(ok.Ok, string.Join(";", ok.Diagnostics));

        var bad = CompileTable("SELECT a FROM json_src UNION ALL SELECT a FROM string_src", [], [JsonCol, StringCol]);
        Assert.False(bad.Ok);
        Assert.Contains(bad.Diagnostics, d => d.Message.Contains("not compatible"));
    }

    [Fact]
    public void TimestampOnlyUnifiesWithTimestamp()
    {
        var ok = CompileTable("SELECT a FROM ts_src UNION ALL SELECT a FROM ts_src", [], [TimestampCol]);
        Assert.True(ok.Ok, string.Join(";", ok.Diagnostics));

        var bad = CompileTable("SELECT a FROM ts_src UNION ALL SELECT a FROM long_src", [], [TimestampCol, LongCol]);
        Assert.False(bad.Ok);
        Assert.Contains(bad.Diagnostics, d => d.Message.Contains("not compatible"));
    }

    // ------------------------------------------------------------------
    // Output column names come from branch 0
    // ------------------------------------------------------------------

    [Fact]
    public void OutputColumnNameIsTakenFromBranchZero()
    {
        var sql = "SELECT a AS renamed FROM string_src UNION ALL SELECT a FROM string_src";
        var r = CompileTable(sql, [], [StringCol]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("renamed", r.OutputSchema!.Fields.Keys);
        Assert.DoesNotContain("a", r.OutputSchema.Fields.Keys);
    }

    // ------------------------------------------------------------------
    // Pipeline-mode UNION (distinct) is rejected, naming UNION ALL as the fix
    // ------------------------------------------------------------------

    [Fact]
    public void PipelineModeUnionWithoutAllIsRejected()
    {
        var r = Compile("SELECT symbol FROM trades UNION SELECT symbol FROM quotes", Trades, Quotes);
        Assert.False(r.Ok);
        var d = Assert.Single(r.Diagnostics, d => d.Message.Contains("pipeline mode"));
        Assert.Contains("UNION ALL", d.Message);
    }

    [Fact]
    public void PipelineModeUnionAllSucceeds()
    {
        var r = Compile("SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes", Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void TableModeUnionWithoutAllSucceeds()
    {
        var r = CompileTable("SELECT symbol FROM trades UNION SELECT symbol FROM quotes", [Trades, Quotes]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void DerivedTablePositionUnionInPipelineModeAlsoRejectsWithoutAll()
    {
        var sql = "SELECT u.symbol FROM (SELECT symbol FROM trades UNION SELECT symbol FROM quotes) u";
        var r = Compile(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("pipeline mode") && d.Message.Contains("UNION ALL"));
    }

    // ------------------------------------------------------------------
    // Plan summary names the set operation
    // ------------------------------------------------------------------

    [Fact]
    public void PlanSummaryDescribesTheSetOperation()
    {
        var r = Compile("SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes", Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("UNION ALL", r.PlanSummary);
        Assert.Contains("SELECT 1", r.PlanSummary);
    }

    [Fact]
    public void TableModePlanSummaryDistinguishesPlainUnionFromUnionAll()
    {
        var r = CompileTable("SELECT symbol FROM trades UNION SELECT symbol FROM quotes", [Trades, Quotes]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("UNION", r.PlanSummary);
        Assert.DoesNotContain("UNION ALL", r.PlanSummary);
    }

    // ------------------------------------------------------------------
    // Subquery-position rejection, end to end (Ok=false with a positioned diagnostic)
    // ------------------------------------------------------------------

    [Fact]
    public void UnionInSubqueryPositionMakesTheWholeCompileFail()
    {
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM quotes UNION SELECT symbol FROM ref)";
        var r = Compile(sql, Trades, Quotes, Ref);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("UNION") && d.Line >= 1 && d.Column >= 1);
    }
}
