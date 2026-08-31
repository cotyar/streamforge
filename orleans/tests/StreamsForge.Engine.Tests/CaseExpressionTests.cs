using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Searched `CASE WHEN … THEN … [ELSE …] END`, which Sql/Parser.cs desugars into nested three-argument
/// `IF` calls rather than a new AST node. These tests deliberately cover BOTH spellings for the same
/// behaviors — if the desugar ever stops producing an IF, or IF stops matching CASE, one of these fails.
/// </summary>
public class CaseExpressionTests
{
    private static readonly SourceSchema Mixed = Schema(
        "mixed",
        ("s", FieldKind.String), ("l", FieldKind.Long), ("d", FieldKind.Double), ("b", FieldKind.Bool));

    private static object? EvalSingle(string selectExpr, params (string Field, object? Value)[] fields)
    {
        var exec = CompileAndCreate($"SELECT {selectExpr} AS x FROM mixed", Mixed);
        var results = exec.OnEvent("mixed", Evt(1000, "mixed", fields));
        Assert.Single(results);
        return results[0]["x"];
    }

    private static (string S, long L, double D, bool B) Defaults => ("a", 1L, 1.5, true);

    private static (string, object?)[] Row(long l) => [("s", Defaults.S), ("l", l), ("d", Defaults.D), ("b", Defaults.B)];

    // ------------------------------------------------------------------
    // Evaluation
    // ------------------------------------------------------------------

    [Theory]
    [InlineData(0L, "zero")]
    [InlineData(1L, "one")]
    [InlineData(9L, "many")]
    public void First_matching_branch_wins_and_else_catches_the_rest(long l, string expected)
    {
        Assert.Equal(expected, EvalSingle(
            "CASE WHEN l = 0 THEN 'zero' WHEN l = 1 THEN 'one' ELSE 'many' END", Row(l)));
    }

    [Fact]
    public void An_omitted_ELSE_yields_NULL_rather_than_a_missing_column()
    {
        Assert.Null(EvalSingle("CASE WHEN l = 0 THEN 'zero' END", Row(7)));
    }

    [Fact]
    public void IF_is_the_same_function_the_desugar_targets()
    {
        Assert.Equal("hit", EvalSingle("IF(l = 3, 'hit', 'miss')", Row(3)));
        Assert.Equal("miss", EvalSingle("IF(l = 3, 'hit', 'miss')", Row(4)));
    }

    /// <summary>A NULL condition is not true, so it takes the else-branch — the same `value is true`
    /// truthiness every other boolean position in this dialect uses, not SQL's UNKNOWN third state
    /// getting its own arm.</summary>
    [Fact]
    public void A_null_condition_takes_the_else_branch()
    {
        var exec = CompileAndCreate("SELECT CASE WHEN l = 1 THEN 'yes' ELSE 'no' END AS x FROM mixed", Mixed);
        var results = exec.OnEvent("mixed", Evt(1000, "mixed", ("s", "a"), ("l", null), ("d", 1.5), ("b", true)));
        Assert.Single(results);
        Assert.Equal("no", results[0]["x"]);
    }

    /// <summary>The demo's actual shape: a numeric distance turned into a status label in SQL instead of
    /// in every client.</summary>
    [Fact]
    public void Nested_branches_fold_right_and_evaluate_in_written_order()
    {
        const string sql = "CASE WHEN l <= 0 THEN 'BREACHED' WHEN l <= 2 THEN 'WATCH' ELSE 'OK' END";
        Assert.Equal("BREACHED", EvalSingle(sql, Row(-1)));
        Assert.Equal("WATCH", EvalSingle(sql, Row(2)));
        Assert.Equal("OK", EvalSingle(sql, Row(3)));
    }

    // ------------------------------------------------------------------
    // Typing
    // ------------------------------------------------------------------

    [Theory]
    [InlineData("CASE WHEN b THEN s ELSE 'x' END", FieldKind.String)]
    [InlineData("CASE WHEN b THEN l ELSE 0 END", FieldKind.Long)]
    [InlineData("CASE WHEN b THEN d ELSE 0.0 END", FieldKind.Double)]
    [InlineData("CASE WHEN b THEN l ELSE d END", FieldKind.Double)] // widens, as mixed arithmetic does
    [InlineData("CASE WHEN b THEN l END", FieldKind.Long)]          // implicit NULL else defers
    public void Result_kind_is_the_branches_common_kind(string expr, FieldKind expected)
    {
        var r = Compile($"SELECT {expr} AS x FROM mixed", Mixed);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics.Select(d => d.Message)));
        Assert.Equal(expected, r.OutputSchema!.Fields["x"]);
    }

    [Theory]
    [InlineData("CASE WHEN b THEN s ELSE l END")]
    [InlineData("IF(b, s, l)")]
    public void Branches_that_disagree_on_type_are_rejected(string expr)
    {
        var r = Compile($"SELECT {expr} AS x FROM mixed", Mixed);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("same type"));
    }

    [Fact]
    public void A_non_boolean_condition_is_rejected_rather_than_always_taking_the_else_branch()
    {
        var r = Compile("SELECT CASE WHEN s THEN 1 ELSE 2 END AS x FROM mixed", Mixed);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("must be a boolean"));
    }

    [Theory]
    [InlineData("IF(b, 1)")]
    [InlineData("IF(b, 1, 2, 3)")]
    public void IF_takes_exactly_three_arguments(string expr)
    {
        var r = Compile($"SELECT {expr} AS x FROM mixed", Mixed);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("wrong number of arguments"));
    }

    // ------------------------------------------------------------------
    // Parsing edges
    // ------------------------------------------------------------------

    /// <summary>CASE is intercepted only when the very next token is WHEN (the same rule CAST uses for
    /// '('), so a column named "case" still resolves as a column.</summary>
    [Fact]
    public void A_column_named_case_still_parses_as_an_identifier()
    {
        var schema = Schema("t", ("case", FieldKind.String));
        var r = Compile("SELECT case AS x FROM t", schema);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics.Select(d => d.Message)));
        Assert.Equal(FieldKind.String, r.OutputSchema!.Fields["x"]);
    }

    [Fact]
    public void An_unterminated_CASE_is_a_diagnostic_not_a_crash()
    {
        var r = Compile("SELECT CASE WHEN b THEN 1 AS x FROM mixed", Mixed);
        Assert.False(r.Ok);
        Assert.NotEmpty(r.Diagnostics);
    }
}
