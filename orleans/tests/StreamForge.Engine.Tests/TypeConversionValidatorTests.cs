using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 009 Round C wave C1 — Validator-level coverage of TO_LONG/TO_DOUBLE/TO_BOOL/TO_TIMESTAMP/
/// TO_STRING: they're KnownFunctions, arity-checked (exactly one argument), and each has a fixed result
/// FieldKind independent of the argument's own kind (unlike ABS/ROUND/COALESCE) — plus the CAST(expr AS
/// type) sugar's parser-level acceptance/rejection. Runtime evaluation semantics live in
/// <see cref="TypeConversionFunctionsTests"/>; this file is result-kind/arity/diagnostics only.
/// </summary>
public class TypeConversionValidatorTests
{
    private static readonly SourceSchema Mixed = Schema(
        "mixed",
        ("s", FieldKind.String), ("l", FieldKind.Long), ("d", FieldKind.Double),
        ("b", FieldKind.Bool), ("j", FieldKind.Json));

    [Theory]
    [InlineData("TO_LONG(s)", FieldKind.Long)]
    [InlineData("TO_DOUBLE(s)", FieldKind.Double)]
    [InlineData("TO_BOOL(s)", FieldKind.Bool)]
    [InlineData("TO_TIMESTAMP(s)", FieldKind.Timestamp)]
    [InlineData("TO_STRING(l)", FieldKind.String)]
    public void ResultKindIsFixedByFunctionNameNotArgumentKind(string expr, FieldKind expectedKind)
    {
        var r = Compile($"SELECT {expr} AS x FROM mixed", Mixed);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics.Select(d => d.Message)));
        Assert.Equal(expectedKind, r.OutputSchema!.Fields["x"]);
    }

    [Theory]
    [InlineData("TO_LONG")]
    [InlineData("TO_DOUBLE")]
    [InlineData("TO_BOOL")]
    [InlineData("TO_TIMESTAMP")]
    [InlineData("TO_STRING")]
    public void EachFunctionRequiresExactlyOneArgument(string fn)
    {
        var zero = Compile($"SELECT {fn}() AS x FROM mixed", Mixed);
        Assert.False(zero.Ok);
        Assert.Contains(zero.Diagnostics, d => d.Message.Contains("wrong number of arguments"));

        var two = Compile($"SELECT {fn}(s, l) AS x FROM mixed", Mixed);
        Assert.False(two.Ok);
        Assert.Contains(two.Diagnostics, d => d.Message.Contains("wrong number of arguments"));
    }

    [Fact]
    public void FunctionsAcceptAJsonKindArgumentFromAnArrowAccess()
    {
        // Plan's own motivating case: a terminal '->' result is FieldKind.Json — these functions must
        // accept it directly (no "extract with ->> first" diagnostic the way bare comparisons/
        // arithmetic on a Json value get — see Validator.CheckBareJsonOperand, which functions bypass).
        var r = Compile("SELECT TO_DOUBLE(j -> 'qty') AS x FROM mixed", Mixed);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics.Select(d => d.Message)));
        Assert.Equal(FieldKind.Double, r.OutputSchema!.Fields["x"]);
    }

    [Fact]
    public void TableModeAcceptsTheSameFunctions()
    {
        var r = CompileTable("SELECT TO_LONG(s) AS x FROM mixed", Mixed);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics.Select(d => d.Message)));
        Assert.Equal(FieldKind.Long, r.OutputSchema!.Fields["x"]);
    }

    // ------------------------------------------------------------------
    // CAST(expr AS type) sugar — desugars to the same FunctionCallExpr the TO_* spelling produces
    // ------------------------------------------------------------------

    [Theory]
    [InlineData("CAST(s AS STRING)", FieldKind.String)]
    [InlineData("CAST(s AS TEXT)", FieldKind.String)]
    [InlineData("CAST(s AS DOUBLE)", FieldKind.Double)]
    [InlineData("CAST(s AS DOUBLE PRECISION)", FieldKind.Double)]
    [InlineData("CAST(s AS LONG)", FieldKind.Long)]
    [InlineData("CAST(s AS BIGINT)", FieldKind.Long)]
    [InlineData("CAST(s AS INT)", FieldKind.Long)]
    [InlineData("CAST(s AS BOOL)", FieldKind.Bool)]
    [InlineData("CAST(s AS BOOLEAN)", FieldKind.Bool)]
    [InlineData("CAST(s AS TIMESTAMP)", FieldKind.Timestamp)]
    public void CastSugarAcceptsDialectAndCommonSqlSpellings(string expr, FieldKind expectedKind)
    {
        var r = Compile($"SELECT {expr} AS x FROM mixed", Mixed);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics.Select(d => d.Message)));
        Assert.Equal(expectedKind, r.OutputSchema!.Fields["x"]);
    }

    [Fact]
    public void CastSugarRejectsAnUnknownTargetType()
    {
        var r = Compile("SELECT CAST(s AS BANANA) AS x FROM mixed", Mixed);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unknown CAST target type"));
    }

    [Fact]
    public void CastSugarRejectsJsonAsATarget()
    {
        // No TO_JSON function exists in this wave — JSON only ever arises from '->' access — so JSON
        // is not a legal CAST target; the parser reports it the same way as any other unknown type.
        var r = Compile("SELECT CAST(s AS JSON) AS x FROM mixed", Mixed);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unknown CAST target type"));
    }

    [Fact]
    public void BareCastIdentifierWithoutParensIsStillAPlainColumnReference()
    {
        // "CAST" is only intercepted when immediately followed by '(' — this proves it doesn't become
        // a reserved word that breaks a hypothetical column/alias literally named "cast".
        var castNamed = Schema("castNamed", ("cast", FieldKind.String));
        var r = Compile("SELECT cast FROM castNamed", castNamed);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics.Select(d => d.Message)));
    }
}
