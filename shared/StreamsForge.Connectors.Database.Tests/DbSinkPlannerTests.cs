using StreamsForge.Abstractions;
using StreamsForge.Connectors.Database;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests;

/// <summary>
/// What one delivered batch becomes. The statements and the PARAMETER ORDER are both asserted: a column
/// list and a value list that disagree produce perfectly valid SQL that writes the wrong data into the
/// wrong columns, which no server will ever complain about.
/// </summary>
public class DbSinkPlannerTests
{
    private static readonly PostgresDialect Pg = new();
    private static readonly SqlServerDialect Ms = new();

    private const string PgTable = "\"public\".\"trades\"";
    private const string MsTable = "[dbo].[trades]";

    private static DbSinkConfig Append() => new() { Host = "db", Database = "market", Table = "trades", Mode = DbSinkModes.Append };

    private static DbSinkConfig Upsert() => new()
    {
        Host = "db", Database = "market", Table = "trades", Mode = DbSinkModes.Upsert, KeyColumns = "symbol",
    };

    private static SinkRow Row(string symbol, long qty, long weight = 1) => new(
        new Dictionary<string, object?>(StringComparer.Ordinal) { ["symbol"] = symbol, ["qty"] = qty },
        weight);

    // ------------------------------------------------------------------
    // Append
    // ------------------------------------------------------------------

    [Fact]
    public void AppendIsOneChunkedParameterizedInsert()
    {
        var plan = DbSinkPlanner.Plan(Append(), Pg, PgTable, [Row("AAPL", 1), Row("MSFT", 2)]);

        var statement = Assert.Single(plan.Statements);
        Assert.Equal(
            "INSERT INTO \"public\".\"trades\" (\"symbol\", \"qty\") VALUES (@p0, @p1), (@p2, @p3)",
            statement.Sql);
        Assert.Equal<object?>(["AAPL", 1L, "MSFT", 2L], statement.Parameters);
    }

    [Fact]
    public void AppendOnSqlServerDiffersOnlyInQuoting()
    {
        var plan = DbSinkPlanner.Plan(Append(), Ms, MsTable, [Row("AAPL", 1)]);

        Assert.Equal("INSERT INTO [dbo].[trades] ([symbol], [qty]) VALUES (@p0, @p1)", Assert.Single(plan.Statements).Sql);
    }

    [Fact]
    public void ColumnsAreTheUnionOfTheBatchSoALaterRowsExtraColumnIsNotDroppedSilently()
    {
        SinkRow late = new(new Dictionary<string, object?>(StringComparer.Ordinal) { ["symbol"] = "MSFT", ["venue"] = "NASDAQ" }, 1);

        var plan = DbSinkPlanner.Plan(Append(), Pg, PgTable, [Row("AAPL", 1), late]);
        var statement = Assert.Single(plan.Statements);

        Assert.Contains("(\"symbol\", \"qty\", \"venue\")", statement.Sql, StringComparison.Ordinal);
        // The first row simply has no value for the column it never carried.
        Assert.Equal<object?>(["AAPL", 1L, null, "MSFT", null, "NASDAQ"], statement.Parameters);
    }

    [Fact]
    public void AnExplicitColumnListWinsOverTheBatchesOwnKeys()
    {
        var config = Append();
        config.Columns = "qty, symbol";

        var statement = Assert.Single(DbSinkPlanner.Plan(config, Pg, PgTable, [Row("AAPL", 7)]).Statements);

        Assert.Equal("INSERT INTO \"public\".\"trades\" (\"qty\", \"symbol\") VALUES (@p0, @p1)", statement.Sql);
        Assert.Equal<object?>([7L, "AAPL"], statement.Parameters);
    }

    [Fact]
    public void IncludeWeightAddsTheWeightColumnOnAppendOnly()
    {
        var config = Append();
        config.IncludeWeight = true;

        var statement = Assert.Single(DbSinkPlanner.Plan(config, Pg, PgTable, [Row("AAPL", 1, weight: -1)]).Statements);

        Assert.Equal("INSERT INTO \"public\".\"trades\" (\"symbol\", \"qty\", \"_weight\") VALUES (@p0, @p1, @p2)", statement.Sql);
        Assert.Equal<object?>(["AAPL", 1L, -1L], statement.Parameters);
    }

    [Fact]
    public void WithoutIncludeWeightTheWeightIsNotWritten()
    {
        var statement = Assert.Single(DbSinkPlanner.Plan(Append(), Pg, PgTable, [Row("AAPL", 1, weight: -1)]).Statements);

        Assert.DoesNotContain("_weight", statement.Sql, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------
    // The parameter ceiling
    // ------------------------------------------------------------------

    [Fact]
    public void SqlServerChunksAtItsTwentyOneHundredParameterCeilingNotAtARoundRowCount()
    {
        // 2100 is a SERVER limit on parameters per batch, so the chunk is 2100 ÷ columns = 700 rows here.
        List<SinkRow> rows = [.. Enumerable.Range(0, 1500).Select(i => new SinkRow(
            new Dictionary<string, object?>(StringComparer.Ordinal) { ["a"] = i, ["b"] = i, ["c"] = i }, 1))];

        var plan = DbSinkPlanner.Plan(Append(), Ms, MsTable, rows);

        Assert.Equal(3, plan.Statements.Count);
        Assert.Equal([2100, 2100, 300], plan.Statements.Select(s => s.Parameters.Count));
        Assert.All(plan.Statements, s => Assert.True(s.Parameters.Count <= Ms.MaxCommandParameters));
    }

    [Fact]
    public void AWiderRowHitsThatSameCeilingSooner()
    {
        // 40 columns → 2100 ÷ 40 = 52 rows per statement, which is the whole reason the chunk is computed
        // from the column count rather than fixed.
        Assert.Equal(52, DbSinkPlanner.ChunkSize(Ms, 40));
        Assert.Equal(700, DbSinkPlanner.ChunkSize(Ms, 3));

        // A row wider than the ceiling cannot be split; one row per statement and let the server say so.
        Assert.Equal(1, DbSinkPlanner.ChunkSize(Ms, 5000));
    }

    [Fact]
    public void PostgresHasItsOwnFarHigherCeilingAndDoesNotChunkTheSameBatch()
    {
        List<SinkRow> rows = [.. Enumerable.Range(0, 1500).Select(i => new SinkRow(
            new Dictionary<string, object?>(StringComparer.Ordinal) { ["a"] = i, ["b"] = i, ["c"] = i }, 1))];

        Assert.Single(DbSinkPlanner.Plan(Append(), Pg, PgTable, rows).Statements);
    }

    // ------------------------------------------------------------------
    // Upsert
    // ------------------------------------------------------------------

    [Fact]
    public void PositiveWeightsUpsertAndNegativeOnesDeleteWithTheDeletesLast()
    {
        var plan = DbSinkPlanner.Plan(Upsert(), Pg, PgTable, [Row("AAPL", 1), Row("MSFT", 2, weight: -1)]);

        Assert.Equal(2, plan.Statements.Count);
        Assert.StartsWith("INSERT INTO", plan.Statements[0].Sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (\"symbol\") DO UPDATE SET \"qty\" = EXCLUDED.\"qty\"", plan.Statements[0].Sql, StringComparison.Ordinal);
        Assert.Equal<object?>(["AAPL", 1L], plan.Statements[0].Parameters);

        Assert.Equal("DELETE FROM \"public\".\"trades\" WHERE \"symbol\" IN (@p0)", plan.Statements[1].Sql);
        Assert.Equal<object?>(["MSFT"], plan.Statements[1].Parameters);
    }

    [Fact]
    public void OnSqlServerTheUpsertHalfIsAMerge()
    {
        var plan = DbSinkPlanner.Plan(Upsert(), Ms, MsTable, [Row("AAPL", 1), Row("MSFT", 2, weight: -1)]);

        Assert.StartsWith("MERGE [dbo].[trades] AS t USING (VALUES (@p0, @p1)) AS s ([symbol], [qty])", plan.Statements[0].Sql, StringComparison.Ordinal);
        Assert.EndsWith(";", plan.Statements[0].Sql, StringComparison.Ordinal);
        Assert.Equal("DELETE FROM [dbo].[trades] WHERE [symbol] IN (@p0)", plan.Statements[1].Sql);
    }

    [Fact]
    public void AnUpdateDeltaPairResolvesToTheNewRowAndIssuesNoDelete()
    {
        // A table UPDATE arrives as -1 carrying the old row and +1 carrying the new one, same key. A sink
        // that applied the delete after the upsert (because deletes go last) would delete the row the
        // update just wrote — the single most important case in this file.
        var plan = DbSinkPlanner.Plan(Upsert(), Pg, PgTable, [Row("AAPL", 1, weight: -1), Row("AAPL", 2)]);

        var statement = Assert.Single(plan.Statements);
        Assert.StartsWith("INSERT INTO", statement.Sql, StringComparison.Ordinal);
        Assert.Equal<object?>(["AAPL", 2L], statement.Parameters);
    }

    [Fact]
    public void ADeleteThenReinsertOfTheSameKeyLandsAsTheCallerMeant()
    {
        var plan = DbSinkPlanner.Plan(Upsert(), Pg, PgTable, [Row("AAPL", 1, weight: -1), Row("AAPL", 5, weight: 1)]);

        Assert.DoesNotContain(plan.Statements, s => s.Sql.StartsWith("DELETE", StringComparison.Ordinal));
    }

    [Fact]
    public void AReinsertThenDeleteOfTheSameKeyResolvesToTheDelete()
    {
        var plan = DbSinkPlanner.Plan(Upsert(), Pg, PgTable, [Row("AAPL", 5), Row("AAPL", 5, weight: -1)]);

        var statement = Assert.Single(plan.Statements);
        Assert.StartsWith("DELETE", statement.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ARepeatedKeyIsNeverNamedTwiceInOneStatement()
    {
        // Both servers refuse it — SQL Server error 8672, PostgreSQL "cannot affect row a second time".
        var plan = DbSinkPlanner.Plan(Upsert(), Ms, MsTable, [Row("AAPL", 1), Row("AAPL", 2), Row("AAPL", 3)]);

        var statement = Assert.Single(plan.Statements);
        Assert.Equal<object?>(["AAPL", 3L], statement.Parameters);
    }

    [Fact]
    public void CompositeKeysAreResolvedComponentWiseNotByConcatenation()
    {
        var config = Upsert();
        config.KeyColumns = "a,b";
        SinkRow one = new(new Dictionary<string, object?>(StringComparer.Ordinal) { ["a"] = "x", ["b"] = "yz" }, 1);
        SinkRow two = new(new Dictionary<string, object?>(StringComparer.Ordinal) { ["a"] = "xy", ["b"] = "z" }, 1);

        var statement = Assert.Single(DbSinkPlanner.Plan(config, Pg, PgTable, [one, two]).Statements);

        // Two identities, not one — a plain concatenation would have merged "x"+"yz" with "xy"+"z".
        Assert.Equal<object?>(["x", "yz", "xy", "z"], statement.Parameters);
    }

    [Fact]
    public void ARowMissingAKeyColumnIsSkippedAndCountedRatherThanGuessedAt()
    {
        SinkRow keyless = new(new Dictionary<string, object?>(StringComparer.Ordinal) { ["qty"] = 1L }, 1);

        var plan = DbSinkPlanner.Plan(Upsert(), Pg, PgTable, [Row("AAPL", 1), keyless]);

        Assert.Equal(1, plan.Skipped);
        Assert.NotNull(plan.SkipReason);
        Assert.Equal<object?>(["AAPL", 1L], Assert.Single(plan.Statements).Parameters);
    }

    [Fact]
    public void ANullKeyIsTreatedTheSameWayAsAMissingOne()
    {
        SinkRow nulled = new(new Dictionary<string, object?>(StringComparer.Ordinal) { ["symbol"] = null, ["qty"] = 1L }, 1);

        Assert.Equal(1, DbSinkPlanner.Plan(Upsert(), Pg, PgTable, [nulled]).Skipped);
    }

    [Fact]
    public void UpsertWithoutKeyColumnsRefusesTheWholeBatchRatherThanInventingAnIdentity()
    {
        var config = Upsert();
        config.KeyColumns = "";

        var plan = DbSinkPlanner.Plan(config, Pg, PgTable, [Row("AAPL", 1)]);

        Assert.Empty(plan.Statements);
        Assert.Equal(1, plan.Skipped);
    }

    [Fact]
    public void AnExplicitColumnListThatOmitsAKeyColumnIsRefused()
    {
        var config = Upsert();
        config.Columns = "qty";

        var plan = DbSinkPlanner.Plan(config, Pg, PgTable, [Row("AAPL", 1)]);

        Assert.Empty(plan.Statements);
        Assert.Contains("symbol", plan.SkipReason!, StringComparison.Ordinal);
    }

    [Fact]
    public void DeletesChunkAgainstTheParameterCeilingToo()
    {
        List<SinkRow> rows = [.. Enumerable.Range(0, 5000).Select(i => new SinkRow(
            new Dictionary<string, object?>(StringComparer.Ordinal) { ["symbol"] = "S" + i }, -1))];

        var plan = DbSinkPlanner.Plan(Upsert(), Ms, MsTable, rows);

        Assert.All(plan.Statements, s => Assert.True(s.Parameters.Count <= Ms.MaxCommandParameters));
        Assert.All(plan.Statements, s => Assert.StartsWith("DELETE", s.Sql, StringComparison.Ordinal));
        Assert.Equal(5000, plan.Statements.Sum(s => s.Parameters.Count));
    }

    [Fact]
    public void AnEmptyBatchPlansNothing()
    {
        var plan = DbSinkPlanner.Plan(Append(), Pg, PgTable, []);

        Assert.Empty(plan.Statements);
        Assert.Equal(0, plan.Skipped);
    }
}
