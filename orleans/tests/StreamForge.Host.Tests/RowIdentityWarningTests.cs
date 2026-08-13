using StreamForge.Abstractions;
using StreamForge.Host.Grains;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// The silent-fallback footgun, made loud.
///
/// <para><c>TableGroupKeyExtractor</c> maps a table's GROUP BY / LATEST BY keys to output columns
/// TEXTUALLY, and returns null — whole-row identity — whenever it cannot. That is a deliberate, safe
/// degradation, and for a table with no declared identity at all it is simply correct. But for a table
/// that DID declare one (an expression key, a CAST, a JSON path that doesn't match its projection
/// character-for-character), falling back destroys the very thing history was enabled for: every version
/// of a row is keyed by its whole content, so no two versions ever collide under one key and the trail
/// never forms. Nothing used to say so. These tests pin the reporting that now does.</para>
///
/// <para>The two assertions that matter, and they pull in opposite directions: an unmappable declared
/// key on a table that keeps a version trail MUST produce a warning, and a table with no GROUP BY /
/// LATEST BY at all MUST NOT — the whole row genuinely is its identity there, that is the case the
/// fallback was designed for, and flagging it would train everyone to ignore the warning.</para>
///
/// <para>New file rather than an edit to HistoryKeyExtractorTests.cs: this is additive reporting, so
/// every pre-existing expectation about what the extractor COMPUTES stays green, unmodified.</para>
/// </summary>
public class RowIdentityWarningTests
{
    /// <summary>The shape that started this: a bucketing expression as the LATEST BY key, projected under
    /// an alias whose expression text does not match the key character-for-character.</summary>
    private const string ExpressionKeySql =
        "SELECT ts_ms - ts_ms % 43000 AS bucket, symbol, price FROM trades LATEST BY (ts_ms - ts_ms% 43000)";

    /// <summary>Same intent, written the way that works. Note what "corrected" has to mean here: the
    /// clause cannot name the ALIAS (LATEST BY keys resolve against the source's columns, not the SELECT
    /// list's aliases — see the Engine's Validator), so the fix is for the clause expression and the
    /// projected expression to be the same text. Which is exactly how easy this is to get wrong by a
    /// single space, and why it needed reporting.</summary>
    private const string CorrectedSql =
        "SELECT ts_ms - ts_ms % 43000 AS bucket, symbol, price FROM trades LATEST BY (ts_ms - ts_ms % 43000)";

    /// <summary>No declared identity at all — the healthy whole-row case, which must stay silent.</summary>
    private const string NoIdentitySql = "SELECT symbol, price FROM trades";

    private static TableDefinition Table(string sql, bool history = false, params string[] shardBy) => new()
    {
        Name = "t", Sql = sql, HistoryEnabled = history, ShardBy = [.. shardBy],
    };

    // ------------------------------------------------------------------
    // Describe: what the extractor SAW, not just what it resolved.
    // ------------------------------------------------------------------

    [Fact]
    public void Describe_UnmappableLatestByKey_ReportsTheClauseAndTheKeyButNoColumns()
    {
        var identity = TableGroupKeyExtractor.Describe(ExpressionKeySql);

        Assert.Equal(TableRowIdentity.LatestByClause, identity.Clause);
        Assert.Equal(new[] { "ts_ms - ts_ms% 43000" }, identity.DeclaredKeys.ToArray());
        Assert.Null(identity.Columns);
        Assert.True(identity.IsDeclared);
        Assert.True(identity.FellBackToWholeRow);
    }

    [Fact]
    public void Describe_NoGroupByOrLatestBy_IsNotDeclaredAndIsNotDegraded()
    {
        var identity = TableGroupKeyExtractor.Describe(NoIdentitySql);

        Assert.False(identity.IsDeclared);
        // The distinction this whole feature rests on: no columns AND no warning, because nothing was
        // declared. Same null Columns as the degraded case above, opposite meaning.
        Assert.Null(identity.Columns);
        Assert.False(identity.FellBackToWholeRow);
    }

    [Fact]
    public void Describe_MappableGroupBy_ResolvesColumnsAndIsNotDegraded()
    {
        var identity = TableGroupKeyExtractor.Describe("SELECT symbol, count(*) AS n FROM trades GROUP BY symbol");

        Assert.Equal(TableRowIdentity.GroupByClause, identity.Clause);
        Assert.Equal(new[] { "symbol" }, identity.DeclaredKeys.ToArray());
        Assert.Equal(new[] { "symbol" }, identity.Columns!.ToArray());
        Assert.False(identity.FellBackToWholeRow);
    }

    [Fact]
    public void Describe_AgreesWithExtractIdentityColumns_OnEveryShape()
    {
        foreach (var sql in new[] { ExpressionKeySql, CorrectedSql, NoIdentitySql, "SELECT symbol FROM trades GROUP BY symbol" })
        {
            Assert.Equal(TableGroupKeyExtractor.ExtractIdentityColumns(sql), TableGroupKeyExtractor.Describe(sql).Columns);
        }
    }

    // ------------------------------------------------------------------
    // The warning itself.
    // ------------------------------------------------------------------

    [Fact]
    public void Warning_HistoryEnabledOnUnmappableExpressionKey_IsReported()
    {
        var warning = TableRowIdentityWarning.For(Table(ExpressionKeySql, history: true));

        Assert.NotNull(warning);
        // It must name the clause, the offending key, and the fix — a bare "degraded" tells the reader
        // nothing they can act on.
        Assert.Contains("LATEST BY", warning);
        Assert.Contains("ts_ms - ts_ms% 43000", warning);
        Assert.Contains("Row history", warning);
        Assert.Contains("character-for-character", warning);
    }

    [Fact]
    public void Warning_NoGroupByAtAll_IsNeverReported_EvenWithHistoryAndSharding()
    {
        // THE regression guard: the whole-row fallback is correct and long-standing here. Flagging it
        // would fire on ordinary tables across every existing install.
        Assert.Null(TableRowIdentityWarning.For(Table(NoIdentitySql, history: true)));
        Assert.Null(TableRowIdentityWarning.For(Table(NoIdentitySql, history: true, "symbol")));
    }

    [Fact]
    public void Warning_KeyProjectedAsAPlainColumn_IsNotReported()
    {
        Assert.Null(TableRowIdentityWarning.For(Table(CorrectedSql, history: true)));
    }

    [Fact]
    public void Warning_UnmappableKeyButNeitherHistoryNorSharding_IsNotReported()
    {
        // Nothing consumes the row identity on such a table, so there is no version trail to degrade and
        // nothing worth interrupting anybody about.
        Assert.Null(TableRowIdentityWarning.For(Table(ExpressionKeySql)));
    }

    [Fact]
    public void Warning_ShardedWithoutHistory_SpeaksAboutTheShardTrail()
    {
        var warning = TableRowIdentityWarning.For(Table(ExpressionKeySql, history: false, "symbol"));

        Assert.NotNull(warning);
        Assert.Contains("shard", warning);
        Assert.DoesNotContain("Row history is", warning);
    }

    [Fact]
    public void Warning_HistoryAndShardingTogether_MentionsBoth()
    {
        var warning = TableRowIdentityWarning.For(Table(ExpressionKeySql, history: true, "symbol"));

        Assert.NotNull(warning);
        Assert.Contains("Row history", warning);
        Assert.Contains("shard", warning);
    }

    [Fact]
    public void Warning_UnmappableGroupBy_ReportsEveryDeclaredKey()
    {
        // A CAST key, which looks like it should match its own projection and does not: the select-item
        // parser splits on the FIRST "AS" it sees, and here that one is inside the cast's parentheses, so
        // the item's expression text comes out as "cast(bucket" and nothing lines up. Exactly the class of
        // near-miss a reader cannot be expected to spot by eye — the warning has to name the keys.
        const string sql =
            "SELECT cast(bucket AS long) AS b, venue, count(*) AS n FROM trades GROUP BY cast(bucket AS long), venue";

        var warning = TableRowIdentityWarning.For(Table(sql, history: true));

        Assert.NotNull(warning);
        Assert.Contains("GROUP BY", warning);
        Assert.Contains("cast(bucket AS long)", warning);
        Assert.Contains("venue", warning);
    }

    [Fact]
    public void Warning_IsAPureFunctionOfTheDefinition_SoItCannotGoStale()
    {
        // The reason nothing persists this verdict: fix the SQL, and the very next read is silent — no
        // backfill, no recompute step anybody can forget to call.
        var def = Table(ExpressionKeySql, history: true);
        Assert.NotNull(TableRowIdentityWarning.For(def));

        def.Sql = CorrectedSql;
        Assert.Null(TableRowIdentityWarning.For(def));
    }
}
