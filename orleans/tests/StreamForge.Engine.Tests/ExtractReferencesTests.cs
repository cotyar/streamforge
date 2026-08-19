using StreamForge.Engine;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 016 wave 3-A — <see cref="SqlCompiler.ExtractReferences"/>. The contract under test is: every
/// relation the statement READS FROM, as authored, distinct by ordinal comparison, in first-appearance
/// (reading) order; empty on any parse failure and empty on "reads nothing", with those two deliberately
/// indistinguishable; and never an exception, for any input.
///
/// <para>What this grammar actually has, and therefore what is tested: FROM, five JOIN kinds plus UNNEST,
/// derived tables (<c>FROM ( SELECT … ) alias</c>), WHERE-position IN / NOT IN / EXISTS / NOT EXISTS /
/// scalar subqueries, WITH-list CTEs (substituted at parse time), and UNION / UNION ALL / UNION DISTINCT.
/// It has NO INTERSECT and NO EXCEPT — those are pinned here as parse failures (empty), not as set
/// operations, so a later grammar addition shows up as a failing test rather than as a silent gap.</para>
/// </summary>
public class ExtractReferencesTests
{
    // ------------------------------------------------------------------
    // FROM / JOIN
    // ------------------------------------------------------------------

    [Fact]
    public void PlainFromReturnsTheOneRelation() =>
        Assert.Equal(["trades"], SqlCompiler.ExtractReferences("SELECT symbol FROM trades"));

    [Fact]
    public void AliasIsNotTheReference() =>
        Assert.Equal(["trades"], SqlCompiler.ExtractReferences("SELECT t.symbol FROM trades t"));

    [Fact]
    public void JoinSourceIsReported() =>
        Assert.Equal(["trades", "instruments"], SqlCompiler.ExtractReferences(
            "SELECT t.symbol FROM trades t JOIN instruments i WITHIN 1 MINUTE ON t.symbol = i.symbol"));

    [Fact]
    public void EveryJoinInAChainIsReported() =>
        Assert.Equal(["trades", "instruments", "quotes"], SqlCompiler.ExtractReferences(
            "SELECT t.symbol FROM trades t " +
            "LEFT JOIN instruments i ON t.symbol = i.symbol " +
            "JOIN quotes q ON t.symbol = q.symbol"));

    [Fact]
    public void CrossJoinIsReported() =>
        Assert.Equal(["trades", "instruments"], SqlCompiler.ExtractReferences(
            "SELECT * FROM trades CROSS JOIN instruments"));

    [Fact]
    public void UnnestJoinAddsNoRelationOfItsOwn() =>
        Assert.Equal(["trades"], SqlCompiler.ExtractReferences(
            "SELECT t.symbol FROM trades t, UNNEST(t.legs) AS leg"));

    // ------------------------------------------------------------------
    // Subqueries — the defect the FROM-only regex had
    // ------------------------------------------------------------------

    [Fact]
    public void DerivedTableInFromReportsTheInnerRelationNotTheAlias() =>
        Assert.Equal(["trades"], SqlCompiler.ExtractReferences(
            "SELECT d.symbol FROM ( SELECT symbol FROM trades ) d"));

    [Fact]
    public void NestedDerivedTablesRecurse() =>
        Assert.Equal(["trades"], SqlCompiler.ExtractReferences(
            "SELECT o.symbol FROM ( SELECT i.symbol FROM ( SELECT symbol FROM trades ) i ) o"));

    [Fact]
    public void DerivedTableJoinedToANamedSourceReportsBoth() =>
        Assert.Equal(["trades", "instruments"], SqlCompiler.ExtractReferences(
            "SELECT t.symbol FROM trades t JOIN ( SELECT symbol FROM instruments ) i ON t.symbol = i.symbol"));

    [Fact]
    public void InSubqueryInWhereIsReported() =>
        Assert.Equal(["trades", "hot"], SqlCompiler.ExtractReferences(
            "SELECT symbol FROM trades WHERE symbol IN ( SELECT symbol FROM hot )"));

    [Fact]
    public void NotInSubqueryInWhereIsReported() =>
        Assert.Equal(["trades", "banned"], SqlCompiler.ExtractReferences(
            "SELECT symbol FROM trades WHERE symbol NOT IN ( SELECT symbol FROM banned )"));

    [Fact]
    public void ExistsSubqueryInWhereIsReported() =>
        Assert.Equal(["trades", "hot"], SqlCompiler.ExtractReferences(
            "SELECT symbol FROM trades WHERE EXISTS ( SELECT symbol FROM hot )"));

    [Fact]
    public void NotExistsSubqueryInWhereIsReported() =>
        Assert.Equal(["trades", "hot"], SqlCompiler.ExtractReferences(
            "SELECT symbol FROM trades WHERE NOT EXISTS ( SELECT symbol FROM hot )"));

    [Fact]
    public void ScalarSubqueryInWhereIsReported() =>
        Assert.Equal(["trades", "limits"], SqlCompiler.ExtractReferences(
            "SELECT symbol FROM trades WHERE price > ( SELECT MAX(price) FROM limits )"));

    [Fact]
    public void ScalarSubqueryInSelectListIsReported() =>
        Assert.Equal(["trades", "limits"], SqlCompiler.ExtractReferences(
            "SELECT symbol, ( SELECT MAX(price) FROM limits ) AS cap FROM trades"));

    // ------------------------------------------------------------------
    // Set operations — UNION family only; INTERSECT/EXCEPT do not exist in this grammar
    // ------------------------------------------------------------------

    [Fact]
    public void UnionAllBranchesAreAllReported() =>
        Assert.Equal(["trades", "quotes"], SqlCompiler.ExtractReferences(
            "SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes"));

    [Fact]
    public void UnionDistinctBranchesAreAllReported() =>
        Assert.Equal(["trades", "quotes"], SqlCompiler.ExtractReferences(
            "SELECT symbol FROM trades UNION SELECT symbol FROM quotes"));

    [Fact]
    public void ThreeBranchUnionReportsAllThree() =>
        Assert.Equal(["a", "b", "c"], SqlCompiler.ExtractReferences(
            "SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM c"));

    [Fact]
    public void SetOperationInDerivedTablePositionIsReported() =>
        Assert.Equal(["trades", "quotes"], SqlCompiler.ExtractReferences(
            "SELECT u.symbol FROM ( SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes ) u"));

    /// <summary>This dialect has no INTERSECT and no EXCEPT (see Sql/Parser.cs ParseSelectOrSetOperation —
    /// UNION is the only set-operation keyword). Both therefore fail to parse and come back EMPTY, which is
    /// the honest answer for a statement this Engine cannot read.</summary>
    [Theory]
    [InlineData("SELECT x FROM a INTERSECT SELECT x FROM b")]
    [InlineData("SELECT x FROM a EXCEPT SELECT x FROM b")]
    public void IntersectAndExceptAreNotInThisGrammarAndYieldEmpty(string sql) =>
        Assert.Empty(SqlCompiler.ExtractReferences(sql));

    /// <summary>Mixing UNION and UNION ALL is a parser diagnostic, so it takes the empty path too.</summary>
    [Fact]
    public void MixedUnionKindsYieldEmpty() =>
        Assert.Empty(SqlCompiler.ExtractReferences("SELECT x FROM a UNION ALL SELECT x FROM b UNION SELECT x FROM c"));

    // ------------------------------------------------------------------
    // CTEs — the parser substitutes them at parse time, so a CTE name is never a reference
    // ------------------------------------------------------------------

    [Fact]
    public void CteNameIsNotAReferenceButItsBodyIs() =>
        Assert.Equal(["trades"], SqlCompiler.ExtractReferences(
            "WITH recent AS ( SELECT symbol FROM trades ) SELECT * FROM recent"));

    [Fact]
    public void ChainedCtesResolveToTheirLeafRelations() =>
        Assert.Equal(["trades"], SqlCompiler.ExtractReferences(
            "WITH a AS ( SELECT symbol FROM trades ), b AS ( SELECT symbol FROM a ) SELECT * FROM b"));

    [Fact]
    public void CteReferencedTwiceIsStillOneReferenceToItsBody() =>
        Assert.Equal(["trades"], SqlCompiler.ExtractReferences(
            "WITH r AS ( SELECT symbol FROM trades ) " +
            "SELECT l.symbol FROM r l JOIN r x ON l.symbol = x.symbol"));

    [Fact]
    public void CteUnderAUnionIsSubstitutedInEveryBranch() =>
        Assert.Equal(["trades", "quotes"], SqlCompiler.ExtractReferences(
            "WITH r AS ( SELECT symbol FROM trades ) " +
            "SELECT symbol FROM r UNION ALL SELECT symbol FROM quotes"));

    /// <summary>Documented edge, pinned so it is a decision and not a surprise: an UNREFERENCED CTE is
    /// dropped by the parser's substitution pass, so the relation only its body reads is not reported. The
    /// statement does not read it either.</summary>
    [Fact]
    public void UnreferencedCteBodyIsNotReported() =>
        Assert.Equal(["quotes"], SqlCompiler.ExtractReferences(
            "WITH unused AS ( SELECT symbol FROM trades ) SELECT symbol FROM quotes"));

    // ------------------------------------------------------------------
    // Dedup / ordering
    // ------------------------------------------------------------------

    [Fact]
    public void SelfJoinReportsTheNameOnce() =>
        Assert.Equal(["trades"], SqlCompiler.ExtractReferences(
            "SELECT a.symbol FROM trades a JOIN trades b ON a.symbol = b.symbol"));

    [Fact]
    public void NameRepeatedAcrossUnionBranchesIsReportedOnce() =>
        Assert.Equal(["trades", "quotes"], SqlCompiler.ExtractReferences(
            "SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes UNION ALL SELECT symbol FROM trades"));

    [Fact]
    public void OrderIsFirstAppearanceInReadingOrderFromThenJoinThenWhere() =>
        Assert.Equal(["trades", "instruments", "hot"], SqlCompiler.ExtractReferences(
            "SELECT t.symbol FROM trades t JOIN instruments i ON t.symbol = i.symbol " +
            "WHERE t.symbol IN ( SELECT symbol FROM hot )"));

    /// <summary>Distinctness is ORDINAL: two spellings of the same relation both come back, exactly as
    /// authored, and matching them against a catalog case-insensitively is the caller's job.</summary>
    [Fact]
    public void DistinctnessIsOrdinalSoCasingVariantsBothAppear() =>
        Assert.Equal(["Trades", "trades"], SqlCompiler.ExtractReferences(
            "SELECT a.symbol FROM Trades a JOIN trades b ON a.symbol = b.symbol"));

    // ------------------------------------------------------------------
    // Never throws — empty and parse failure are the same answer, by design
    // ------------------------------------------------------------------

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\t\n  ")]
    [InlineData("SELECT")]
    [InlineData("SELECT * FROM")]
    [InlineData("SELECT * FROM trades WHERE")]
    [InlineData("this is not sql at all")]
    [InlineData("SELECT * FROM trades ) ) )")]
    [InlineData("DROP TABLE trades")]
    // A trailing ';' is an "Unexpected character" to this tokenizer — the dialect has no statement
    // terminator — so it takes the empty path here exactly as it would fail SqlCompiler.Compile.
    [InlineData("SELECT symbol FROM trades;")]
    [InlineData("SELECT * FROM trades; SELECT * FROM quotes")]
    [InlineData("SELECT 'unterminated FROM trades")]
    [InlineData("INSERT INTO warehouse SELECT symbol FROM trades")]
    [InlineData("WITH r AS ( SELECT symbol FROM r ) SELECT * FROM r")]
    public void BadOrUnsupportedInputReturnsEmptyAndDoesNotThrow(string sql) =>
        Assert.Empty(SqlCompiler.ExtractReferences(sql));

    [Fact]
    public void NullReturnsEmptyAndDoesNotThrow() =>
        Assert.Empty(SqlCompiler.ExtractReferences(null));

    /// <summary>The conflation stated as a test: a parse failure and a genuine "no references" are the same
    /// observable value, so wave 3-B cannot (and must not) branch on the difference.</summary>
    [Fact]
    public void ParseFailureAndEmptyResultAreIndistinguishable()
    {
        var broken = SqlCompiler.ExtractReferences("SELECT * FROM");
        var empty = SqlCompiler.ExtractReferences("");
        Assert.Equal(empty, broken);
    }
}
