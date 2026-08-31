using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 008 wave 2b: the table-mode outer-join validator gate (TableValidatorTests used to cover this as
/// "LeftJoinIsNotSupportedInTableMode" / a CROSS-forbidden test — both deleted, this file is their positive
/// successor) is lifted — LEFT, RIGHT, FULL, and CROSS all compile now. What must NOT have moved: the WITHIN
/// ban, the ON-clause requirement, and the equi-comparison requirement all still fire, for every kind
/// (including the newly-allowed ones) that isn't CROSS (which structurally has no ON at all).
/// </summary>
public class TableOuterJoinValidatorTests
{
    [Theory]
    [InlineData("LEFT")]
    [InlineData("RIGHT")]
    [InlineData("FULL")]
    public void OuterJoinCompilesInTableMode(string kind)
    {
        var sql = $"SELECT t.symbol, q.bid FROM trades t {kind} JOIN quotes q ON t.symbol = q.symbol";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void CrossJoinCompilesInTableMode()
    {
        var sql = "SELECT t.symbol, q.bid FROM trades t CROSS JOIN quotes q";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Theory]
    [InlineData("LEFT")]
    [InlineData("RIGHT")]
    [InlineData("FULL")]
    [InlineData("")] // plain INNER — same restriction, unaffected by plan 008
    public void WithinClauseStillForbiddenInTableMode(string kind)
    {
        var joinKw = kind.Length == 0 ? "JOIN" : $"{kind} JOIN";
        var sql = $"SELECT t.symbol FROM trades t {joinKw} quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("WITHIN clause in table mode"));
    }

    [Theory]
    [InlineData("LEFT")]
    [InlineData("RIGHT")]
    [InlineData("FULL")]
    public void OuterJoinStillRequiresAnOnClause(string kind)
    {
        // Grammar-level for every non-CROSS join kind (LEFT/RIGHT/FULL included, same as pre-008 INNER) —
        // omitting ON is caught by the parser itself ("Expected 'ON'") before the Validator's own "requires
        // an ON clause" diagnostic (Sql/Validator.cs) would even run; either way, still a hard reject.
        var sql = $"SELECT t.symbol FROM trades t {kind} JOIN quotes q";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("ON"));
    }

    [Theory]
    [InlineData("LEFT")]
    [InlineData("RIGHT")]
    [InlineData("FULL")]
    public void OuterJoinStillRequiresAnEquiComparison(string kind)
    {
        var sql = $"SELECT t.symbol FROM trades t {kind} JOIN quotes q ON t.price > q.bid";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("equi-comparison"));
    }

    [Fact]
    public void CrossJoinHasNoOnClauseAtAll_GrammarLevel()
    {
        // CROSS JOIN's grammar forbids ON outright (a parse-level restriction, not this wave's concern) —
        // asserted here only as a boundary marker: CROSS needs no equi-comparison because it has no ON to
        // put one in, unlike every other newly-allowed kind above.
        var sql = "SELECT t.symbol FROM trades t CROSS JOIN quotes q ON t.symbol = q.symbol";
        var r = CompileTable(sql, Trades, Quotes);
        Assert.False(r.Ok);
    }
}
