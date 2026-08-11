using StreamForge.Engine.Sql;
using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 008 W3 — `UNION` / `UNION ALL` grammar. Parser-level tests exercise <see cref="Parser.ParseStatement"/>
/// directly (same pattern GroupByAllTests uses for GROUP BY ALL) to pin down exactly which AST shape a
/// set-operation chain parses to — the `SetOperationQuery { All, Branches }` node introduced above
/// <see cref="SelectQuery"/>. End-to-end compile tests cover the two accepted positions (top level,
/// derived-table position) and the explicitly out-of-scope ones (IN/EXISTS/scalar subquery).
/// </summary>
public class SetOperationParserTests
{
    private static (SelectQuery? Query, SetOperationQuery? SetOp, System.Collections.Generic.List<SqlDiagnostic> Diagnostics) ParseStatement(string sql)
    {
        var (tokens, tokDiags) = new Tokenizer(sql).Tokenize();
        Assert.Empty(tokDiags);
        return Parser.ParseStatement(tokens);
    }

    // ------------------------------------------------------------------
    // Grammar shape
    // ------------------------------------------------------------------

    [Fact]
    public void UnionAllParsesToATwoBranchSetOperationQuery()
    {
        var (query, setOp, diags) = ParseStatement("SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes");

        Assert.Empty(diags);
        Assert.Null(query);
        Assert.NotNull(setOp);
        Assert.True(setOp!.All);
        Assert.Equal(2, setOp.Branches.Count);
    }

    [Fact]
    public void PlainUnionParsesWithAllFalse()
    {
        var (_, setOp, diags) = ParseStatement("SELECT symbol FROM trades UNION SELECT symbol FROM quotes");

        Assert.Empty(diags);
        Assert.NotNull(setOp);
        Assert.False(setOp!.All);
    }

    [Fact]
    public void UnionDistinctIsSugarForPlainUnion()
    {
        var (_, setOp, diags) = ParseStatement("SELECT symbol FROM trades UNION DISTINCT SELECT symbol FROM quotes");

        Assert.Empty(diags);
        Assert.NotNull(setOp);
        Assert.False(setOp!.All);
    }

    [Fact]
    public void ThreeBranchChainFlattensIntoOneSetOperationQuery()
    {
        var (_, setOp, diags) = ParseStatement(
            "SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes UNION ALL SELECT symbol FROM ref");

        Assert.Empty(diags);
        Assert.NotNull(setOp);
        Assert.Equal(3, setOp!.Branches.Count);
    }

    [Fact]
    public void NoUnionKeywordParsesToAPlainSelectQueryNotASetOp()
    {
        var (query, setOp, diags) = ParseStatement("SELECT symbol FROM trades");

        Assert.Empty(diags);
        Assert.NotNull(query);
        Assert.Null(setOp);
    }

    // ------------------------------------------------------------------
    // The silent-mis-parse bug this wave's ClauseKeywords fix prevents
    // ------------------------------------------------------------------

    [Fact]
    public void TrailingUnionKeywordIsNotSwallowedAsAnAsLessAlias()
    {
        // Before "UNION" was added to Parser.ClauseKeywords, `SELECT symbol FROM trades UNION SELECT ...`
        // would parse "symbol" with an implicit (AS-less) alias of "UNION" and then fail on the dangling
        // "SELECT" — a confusing diagnostic far from the real issue. Now it must parse as a genuine two-
        // branch set operation, with the first branch's own select item carrying NO alias at all.
        var (_, setOp, diags) = ParseStatement("SELECT symbol FROM trades UNION SELECT symbol FROM quotes");

        Assert.Empty(diags);
        Assert.NotNull(setOp);
        Assert.Equal(2, setOp!.Branches.Count);
        Assert.Null(setOp.Branches[0].Select.Items[0].Alias);
    }

    // ------------------------------------------------------------------
    // Mixed ALL-ness is rejected, not silently resolved one way
    // ------------------------------------------------------------------

    [Fact]
    public void MixingUnionAllAndPlainUnionInOneChainIsRejected()
    {
        var (_, setOp, diags) = ParseStatement(
            "SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes UNION SELECT symbol FROM ref");

        Assert.Null(setOp);
        var d = Assert.Single(diags);
        Assert.Contains("Mixing UNION and UNION ALL", d.Message);
    }

    // ------------------------------------------------------------------
    // WITH ... UNION ... — CTE substitution must recurse into every branch
    // ------------------------------------------------------------------

    [Fact]
    public void WithCteMainQueryCanBeAUnionChain()
    {
        var sql = "WITH hot AS (SELECT symbol FROM trades) SELECT hot.symbol FROM hot UNION ALL SELECT symbol FROM quotes";
        var (_, setOp, diags) = ParseStatement(sql);

        Assert.Empty(diags);
        Assert.NotNull(setOp);
        Assert.Equal(2, setOp!.Branches.Count);
        // Branch 0's FROM item was substituted from a NamedSource("hot") into a DerivedSource wrapping the
        // CTE's own body — exactly the same desugaring a non-union WITH query gets.
        Assert.IsType<DerivedSource>(setOp.Branches[0].From.Source);
    }

    [Fact]
    public void WithCteReferencedFromASecondBranchAlsoDesugars()
    {
        var sql = "WITH hot AS (SELECT symbol FROM trades) SELECT symbol FROM quotes UNION ALL SELECT hot.symbol FROM hot";
        var r = Compile(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    // ------------------------------------------------------------------
    // Derived-table position: FROM ( ... UNION ... ) alias
    // ------------------------------------------------------------------

    [Fact]
    public void DerivedTablePositionAcceptsAUnionAll()
    {
        var (_, setOp, diags) = ParseStatement(
            "SELECT u.symbol FROM (SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes) u");

        Assert.Empty(diags);
        Assert.Null(setOp); // the OUTER statement is a plain SelectQuery — the union sits inside its FROM
    }

    [Fact]
    public void DerivedTablePositionUnionEndToEndCompiles()
    {
        var sql = "SELECT u.symbol FROM (SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes) u";
        var r = Compile(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void DerivedTablePositionUnionRequiresAnAliasLikeAnyOtherDerivedTable()
    {
        var sql = "SELECT symbol FROM (SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes)";
        var r = Compile(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("requires an alias"));
    }

    // ------------------------------------------------------------------
    // Out-of-scope positions: IN / EXISTS / scalar subquery — rejected with a positioned diagnostic
    // ------------------------------------------------------------------

    [Fact]
    public void UnionInsideInSubqueryIsRejected()
    {
        var sql = "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM quotes UNION SELECT symbol FROM ref)";
        var r = Compile(sql, Trades, Quotes, Ref);
        Assert.False(r.Ok);
        var d = Assert.Single(r.Diagnostics, d => d.Message.Contains("UNION"));
        Assert.Contains("IN (...)", d.Message);
        Assert.True(d.Line >= 1 && d.Column >= 1);
    }

    [Fact]
    public void UnionInsideExistsIsRejected()
    {
        var sql = "SELECT symbol FROM trades WHERE EXISTS (SELECT symbol FROM quotes UNION SELECT symbol FROM ref)";
        var r = Compile(sql, Trades, Quotes, Ref);
        Assert.False(r.Ok);
        var d = Assert.Single(r.Diagnostics, d => d.Message.Contains("UNION"));
        Assert.Contains("EXISTS (...)", d.Message);
    }

    [Fact]
    public void UnionInsideScalarSubqueryIsRejected()
    {
        var sql = "SELECT symbol, (SELECT COUNT(*) FROM quotes UNION SELECT COUNT(*) FROM ref) AS c FROM trades";
        var r = Compile(sql, Trades, Quotes, Ref);
        Assert.False(r.Ok);
        var d = Assert.Single(r.Diagnostics, d => d.Message.Contains("UNION"));
        Assert.Contains("scalar subquery", d.Message);
    }
}
