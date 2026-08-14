using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

/// <summary>
/// The generated SQL, asserted as text. That looks brittle and is deliberate: this connector's entire
/// contract with two servers is the string it sends them, and the failures that matter — a missing
/// semicolon after a MERGE, an OFFSET/FETCH without an ORDER BY, a parameter list whose order does not
/// match the column list — are all invisible to any assertion weaker than the text itself. A live
/// database would catch the syntax errors but not the ORDER mistakes, and it is wave M's job anyway.
/// </summary>
public class SqlDialectTests
{
    private static readonly PostgresDialect Pg = new();
    private static readonly SqlServerDialect Ms = new();

    [Fact]
    public void QuotingUsesEachDialectsOwnDelimiterAndEscapesIt()
    {
        Assert.Equal("\"trades\"", Pg.QuoteIdent("trades"));
        Assert.Equal("[trades]", Ms.QuoteIdent("trades"));

        // The one thing an injection through an identifier would rely on.
        Assert.Equal("\"a\"\"b\"", Pg.QuoteIdent("a\"b"));
        Assert.Equal("[a]]b]", Ms.QuoteIdent("a]b"));
    }

    [Fact]
    public void PageClauseDiffersAndTheSqlServerFormNeedsTheOrderByEveryGeneratedSelectHas()
    {
        Assert.Equal("LIMIT 250", Pg.PageClause(250));
        Assert.Equal("OFFSET 0 ROWS FETCH NEXT 250 ROWS ONLY", Ms.PageClause(250));
    }

    [Fact]
    public void PostgresUpsertIsInsertOnConflictDoUpdate()
    {
        var sql = Pg.UpsertStatement("\"public\".\"trades\"", ["symbol", "venue", "qty"], ["symbol"], rowCount: 2, firstParameter: 0);

        Assert.Equal(
            "INSERT INTO \"public\".\"trades\" (\"symbol\", \"venue\", \"qty\") VALUES (@p0, @p1, @p2), (@p3, @p4, @p5) " +
            "ON CONFLICT (\"symbol\") DO UPDATE SET \"venue\" = EXCLUDED.\"venue\", \"qty\" = EXCLUDED.\"qty\"",
            sql);
    }

    [Fact]
    public void PostgresUpsertDegradesToDoNothingWhenEveryColumnIsAKey()
    {
        // `DO UPDATE SET` with an empty assignment list is a syntax error, and the row already there is
        // byte-identical to the one being written — so DO NOTHING is the honest equivalent.
        var sql = Pg.UpsertStatement("\"public\".\"t\"", ["a", "b"], ["a", "b"], rowCount: 1, firstParameter: 0);

        Assert.Equal("INSERT INTO \"public\".\"t\" (\"a\", \"b\") VALUES (@p0, @p1) ON CONFLICT (\"a\", \"b\") DO NOTHING", sql);
    }

    [Fact]
    public void SqlServerUpsertIsAMergeAndEndsInASemicolon()
    {
        var sql = Ms.UpsertStatement("[dbo].[trades]", ["symbol", "venue", "qty"], ["symbol"], rowCount: 2, firstParameter: 0);

        Assert.Equal(
            "MERGE [dbo].[trades] AS t USING (VALUES (@p0, @p1, @p2), (@p3, @p4, @p5)) AS s ([symbol], [venue], [qty]) " +
            "ON (t.[symbol] = s.[symbol]) " +
            "WHEN MATCHED THEN UPDATE SET t.[venue] = s.[venue], t.[qty] = s.[qty] " +
            "WHEN NOT MATCHED THEN INSERT ([symbol], [venue], [qty]) VALUES (s.[symbol], s.[venue], s.[qty]);",
            sql);

        // Stated separately because the failure mode is so misleading: without it SQL Server reports a
        // syntax error naming the NEXT statement in the batch.
        Assert.EndsWith(";", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SqlServerMergeOmitsTheUpdateClauseWhenEveryColumnIsAKey()
    {
        var sql = Ms.UpsertStatement("[dbo].[t]", ["a", "b"], ["a", "b"], rowCount: 1, firstParameter: 0);

        Assert.Equal(
            "MERGE [dbo].[t] AS t USING (VALUES (@p0, @p1)) AS s ([a], [b]) ON (t.[a] = s.[a] AND t.[b] = s.[b]) " +
            "WHEN NOT MATCHED THEN INSERT ([a], [b]) VALUES (s.[a], s.[b]);",
            sql);
        Assert.DoesNotContain("WHEN MATCHED", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SingleKeyDeletesAreAPlainInListInBothDialects()
    {
        Assert.Equal(
            "DELETE FROM \"public\".\"trades\" WHERE \"symbol\" IN (@p0, @p1, @p2)",
            Pg.DeleteStatement("\"public\".\"trades\"", ["symbol"], rowCount: 3, firstParameter: 0));

        Assert.Equal(
            "DELETE FROM [dbo].[trades] WHERE [symbol] IN (@p0, @p1, @p2)",
            Ms.DeleteStatement("[dbo].[trades]", ["symbol"], rowCount: 3, firstParameter: 0));
    }

    [Fact]
    public void CompositeKeyDeletesDivergeBecauseSqlServerHasNoRowValueIn()
    {
        Assert.Equal(
            "DELETE FROM \"public\".\"trades\" WHERE (\"symbol\", \"venue\") IN ((@p0, @p1), (@p2, @p3))",
            Pg.DeleteStatement("\"public\".\"trades\"", ["symbol", "venue"], rowCount: 2, firstParameter: 0));

        Assert.Equal(
            "DELETE FROM [dbo].[trades] WHERE ([symbol] = @p0 AND [venue] = @p1) OR ([symbol] = @p2 AND [venue] = @p3)",
            Ms.DeleteStatement("[dbo].[trades]", ["symbol", "venue"], rowCount: 2, firstParameter: 0));
    }

    [Fact]
    public void TheParameterCeilingIsTheServersNotARoundNumber()
    {
        Assert.Equal(2100, Ms.MaxCommandParameters);
        Assert.Equal(65535, Pg.MaxCommandParameters);
        Assert.Equal(5432, Pg.DefaultPort);
        Assert.Equal(1433, Ms.DefaultPort);
        Assert.Equal("public", Pg.DefaultSchema);
        Assert.Equal("dbo", Ms.DefaultSchema);
    }

    [Fact]
    public void ConnectionsAreBuiltFromTheStructuredFieldsAndOverriddenWholesaleByAConnectionString()
    {
        using var structured = Pg.CreateConnection(new DbEndpoint("db.internal", 0, "market", "sf", "pw", Tls: false, ConnectionString: null));
        Assert.Contains("db.internal", structured.ConnectionString, StringComparison.Ordinal);
        Assert.Contains("market", structured.ConnectionString, StringComparison.Ordinal);

        // The contract's own rule ("when set it WINS over every structured field above"), enforced.
        using var raw = Pg.CreateConnection(new DbEndpoint("ignored", 1, "ignored", "u", "p", Tls: true, "Host=elsewhere;Database=other"));
        Assert.DoesNotContain("ignored", raw.ConnectionString, StringComparison.Ordinal);
        Assert.Contains("elsewhere", raw.ConnectionString, StringComparison.Ordinal);
    }

    [Fact]
    public void AnArbitraryExceptionIsNotClassifiedAsTransient()
    {
        // The retry is gated on the DRIVER's own classification; nothing else may sneak through it, or a
        // constraint violation would be replayed forever.
        Assert.False(Pg.IsTransient(new InvalidOperationException("nope")));
        Assert.False(Ms.IsTransient(new InvalidOperationException("nope")));
    }
}
