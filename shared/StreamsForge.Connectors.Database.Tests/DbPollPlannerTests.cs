using StreamsForge.Abstractions;
using StreamsForge.Connectors.Database;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests;

/// <summary>
/// The cursor rules — the part of this connector that can lose data silently, and therefore the part that
/// gets covered by tests that run in the ordinary suite rather than only against a container.
/// </summary>
public class DbPollPlannerTests
{
    private static readonly PostgresDialect Pg = new();
    private static readonly SqlServerDialect Ms = new();

    private static DbSourceConfig Config() => new()
    {
        Host = "db",
        Database = "market",
        Table = "trades",
        CursorColumn = "id",
        CursorKind = CursorKinds.Long,
        BatchSize = 500,
    };

    private static List<Dictionary<string, object?>> Rows(params long[] ids)
        => [.. ids.Select(id => new Dictionary<string, object?>(StringComparer.Ordinal) { ["id"] = id, ["symbol"] = "AAPL" })];

    // ------------------------------------------------------------------
    // Branch 1 — a persisted cursor exists
    // ------------------------------------------------------------------

    [Fact]
    public void AnExistingCursorReadsStrictlyAfterItAndBindsItAsAParameter()
    {
        var plan = DbPollPlanner.Plan(Config(), Pg, "17");

        Assert.Equal(
            "SELECT * FROM \"public\".\"trades\" WHERE \"id\" > @cursor ORDER BY \"id\" ASC LIMIT 500",
            plan.Sql);
        Assert.False(plan.Seed);

        // Bound, never interpolated — injection, and type fidelity across a DST boundary.
        var parameter = Assert.Single(plan.Parameters);
        Assert.Equal("cursor", parameter.Key);
        Assert.Equal(17L, parameter.Value);
        Assert.DoesNotContain("17", plan.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSameCursorOnSqlServerDiffersOnlyInQuotingAndThePageTail()
    {
        var plan = DbPollPlanner.Plan(Config(), Ms, "17");

        Assert.Equal(
            "SELECT * FROM [dbo].[trades] WHERE [id] > @cursor ORDER BY [id] ASC OFFSET 0 ROWS FETCH NEXT 500 ROWS ONLY",
            plan.Sql);
    }

    [Fact]
    public void AWhereClauseIsAndedOntoTheGeneratedPredicate()
    {
        var config = Config();
        config.Where = "status = 'settled'";

        Assert.Equal(
            "SELECT * FROM \"public\".\"trades\" WHERE \"id\" > @cursor AND (status = 'settled') ORDER BY \"id\" ASC LIMIT 500",
            DbPollPlanner.Plan(config, Pg, "17").Sql);
    }

    [Fact]
    public void ADedupKeyColumnSwitchesTheComparisonToGreaterOrEqual()
    {
        // Not a hidden mode: it is exactly the shape CursorColumn's own doc recommends for a timestamp
        // watermark, expressed through the fields that exist rather than a fourth one nobody would set.
        var config = Config();
        config.DedupKeyColumn = "id";

        Assert.Contains("\"id\" >= @cursor", DbPollPlanner.Plan(config, Pg, "17").Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ANonDefaultSchemaIsHonouredAndQuoted()
    {
        var config = Config();
        config.Schema = "market data";

        Assert.StartsWith("SELECT * FROM \"market data\".\"trades\"", DbPollPlanner.Plan(config, Pg, "1").Sql, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------
    // Branch 2 — InitialCursor. The transport's job; neither driver seeds it.
    // ------------------------------------------------------------------

    [Fact]
    public void InitialCursorIsHonouredOnTheFirstEverCycle()
    {
        var config = Config();
        config.InitialCursor = "1000";

        var plan = DbPollPlanner.Plan(config, Pg, cursor: null);

        Assert.False(plan.Seed);
        Assert.Contains("\"id\" > @cursor", plan.Sql, StringComparison.Ordinal);
        Assert.Equal(1000L, Assert.Single(plan.Parameters).Value);
    }

    [Fact]
    public void InitialCursorIsIgnoredOnceACursorIsPersisted()
    {
        // A config edit must not reset the cursor — both drivers re-run their start path on every catalog
        // upsert, and re-seeding there would re-read the entire table on any edit.
        var config = Config();
        config.InitialCursor = "1000";

        Assert.Equal(2000L, Assert.Single(DbPollPlanner.Plan(config, Pg, "2000").Parameters).Value);
    }

    [Fact]
    public void InitialCursorWinsOverSnapshotBecauseItIsMoreSpecific()
    {
        var config = Config();
        config.Snapshot = true;
        config.InitialCursor = "1000";

        Assert.Contains("@cursor", DbPollPlanner.Plan(config, Pg, null).Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ATimestampInitialCursorBindsAsATimestampNotAString()
    {
        var config = Config();
        config.CursorKind = CursorKinds.Timestamp;
        config.CursorColumn = "updated_at";
        config.InitialCursor = "2026-08-14T10:30:00.0000000Z";

        var value = Assert.IsType<DateTime>(Assert.Single(DbPollPlanner.Plan(config, Pg, null).Parameters).Value);
        Assert.Equal(DateTimeKind.Utc, value.Kind);
    }

    // ------------------------------------------------------------------
    // Branch 3 — snapshot
    // ------------------------------------------------------------------

    [Fact]
    public void SnapshotPageOneHasNoCursorPredicateAtAll()
    {
        var config = Config();
        config.Snapshot = true;

        var plan = DbPollPlanner.Plan(config, Pg, cursor: null);

        Assert.Equal("SELECT * FROM \"public\".\"trades\" ORDER BY \"id\" ASC LIMIT 500", plan.Sql);
        Assert.Empty(plan.Parameters);
        Assert.False(plan.Seed);
    }

    [Fact]
    public void SnapshotPageOneStillAppliesTheWhereClause()
    {
        var config = Config();
        config.Snapshot = true;
        config.Where = "qty > 0";

        Assert.Equal(
            "SELECT * FROM \"public\".\"trades\" WHERE (qty > 0) ORDER BY \"id\" ASC LIMIT 500",
            DbPollPlanner.Plan(config, Pg, null).Sql);
    }

    [Fact]
    public void AFullSnapshotPageReArmsAndPersistsItsOwnCursorSoARestartResumesMidSnapshot()
    {
        var config = Config();
        config.Snapshot = true;
        config.BatchSize = 3;

        var batch = DbPollPlanner.Complete(config, Rows(1, 2, 3), incoming: null);

        Assert.True(batch.HasMore);
        Assert.Equal("3", batch.Cursor);
        Assert.Equal(3, batch.Rows.Count);

        // …and page 2 is an ordinary cursored read. A snapshot is not a second code path.
        Assert.Contains("\"id\" > @cursor", DbPollPlanner.Plan(config, Pg, batch.Cursor).Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AShortPageEndsTheSnapshotAndFallsBackToTheSchedule()
    {
        var config = Config();
        config.BatchSize = 3;

        var batch = DbPollPlanner.Complete(config, Rows(4, 5), incoming: "3");

        Assert.False(batch.HasMore);
        Assert.Equal("5", batch.Cursor);
    }

    // ------------------------------------------------------------------
    // Branch 4 — seed from MAX and tail
    // ------------------------------------------------------------------

    [Fact]
    public void NoCursorNoInitialNoSnapshotSeedsFromMaxAndEmitsNothing()
    {
        var plan = DbPollPlanner.Plan(Config(), Pg, cursor: null);

        Assert.True(plan.Seed);
        Assert.Equal("SELECT MAX(\"id\") FROM \"public\".\"trades\"", plan.Sql);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void TheSeedRespectsTheWhereClauseSoTailingStartsAtTheTopOfTheFilteredSet()
    {
        var config = Config();
        config.Where = "qty > 0";

        Assert.Equal("SELECT MAX(\"id\") FROM \"public\".\"trades\" WHERE (qty > 0)", DbPollPlanner.Plan(config, Pg, null).Sql);
    }

    // ------------------------------------------------------------------
    // Query mode
    // ------------------------------------------------------------------

    [Fact]
    public void QueryModePassesTheOperatorsSqlThroughUntouchedAndBindsTheCursor()
    {
        var config = Config();
        config.Query = "SELECT id, CAST(px AS text) AS px FROM trades WHERE id > @cursor ORDER BY id LIMIT 10";
        config.InitialCursor = "5";

        var plan = DbPollPlanner.Plan(config, Pg, cursor: null);

        // Nothing added: no ORDER BY, no page clause. The SQL is the operator's.
        Assert.Equal(config.Query, plan.Sql);
        Assert.Equal(5L, Assert.Single(plan.Parameters).Value);
    }

    [Fact]
    public void QueryModeWithNoStartingPointFailsLoudlyRatherThanInventingASentinel()
    {
        // There is no MAX to seed from in arbitrary SQL, and no sentinel is safe: long.MinValue is a real
        // key in someone's table and DateTime.MinValue will not even bind to a timestamptz.
        var config = Config();
        config.Query = "SELECT * FROM t WHERE id > @cursor";

        var ex = Assert.Throws<InvalidOperationException>(() => DbPollPlanner.Plan(config, Pg, null));
        Assert.Contains("initialCursor", ex.Message, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------
    // Completing a batch
    // ------------------------------------------------------------------

    [Fact]
    public void AnEmptyPollLeavesThePersistedCursorExactlyAsItWas()
    {
        // Null Cursor means "unchanged". It is NOT "reset to the beginning".
        var batch = DbPollPlanner.Complete(Config(), [], incoming: "17");

        Assert.Null(batch.Cursor);
        Assert.False(batch.HasMore);
        Assert.Empty(batch.Rows);
    }

    [Fact]
    public void TheCursorIsTheMaximumOverTheBatchNotTheLastRow()
    {
        // Query mode is the operator's SQL and may not order at all; taking the last row would move the
        // watermark backwards and re-read the same window forever.
        var batch = DbPollPlanner.Complete(Config(), Rows(9, 3, 7), incoming: "1");

        Assert.Equal("9", batch.Cursor);
    }

    [Fact]
    public void AllNullCursorValuesLeaveTheCursorUnchangedRatherThanClearingIt()
    {
        List<Dictionary<string, object?>> rows =
            [new(StringComparer.Ordinal) { ["id"] = null, ["symbol"] = "AAPL" }];

        Assert.Null(DbPollPlanner.Complete(Config(), rows, incoming: "17").Cursor);
    }

    [Fact]
    public void AFullPageThatDidNotMoveTheCursorDoesNotReArmBecauseThatWouldSpin()
    {
        // The >= case where every row in a full page shares the watermark value. Re-arming would drive the
        // same page at full speed forever; the schedule paces it instead.
        var config = Config();
        config.DedupKeyColumn = "id";
        config.BatchSize = 2;

        var batch = DbPollPlanner.Complete(config, Rows(17, 17), incoming: "17");

        Assert.Equal("17", batch.Cursor);
        Assert.False(batch.HasMore);
    }

    [Fact]
    public void ABatchWithoutTheCursorColumnFailsLoudly()
    {
        // Almost always a custom Query whose SELECT list omits it. Without it the watermark can never
        // advance and the same rows are re-emitted on every cycle.
        List<Dictionary<string, object?>> rows = [new(StringComparer.Ordinal) { ["symbol"] = "AAPL" }];

        var ex = Assert.Throws<InvalidOperationException>(() => DbPollPlanner.Complete(Config(), rows, null));
        Assert.Contains("cursor column 'id'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ATimestampCursorIsEncodedBackToItsRoundTrippableForm()
    {
        var config = Config();
        config.CursorKind = CursorKinds.Timestamp;
        config.CursorColumn = "updated_at";
        var at = new DateTime(2026, 8, 14, 10, 30, 0, DateTimeKind.Utc);

        List<Dictionary<string, object?>> rows = [new(StringComparer.Ordinal) { ["updated_at"] = at }];
        var batch = DbPollPlanner.Complete(config, rows, null);

        Assert.Equal(at, DbCursor.Decode(batch.Cursor!, CursorKinds.Timestamp));
    }
}
