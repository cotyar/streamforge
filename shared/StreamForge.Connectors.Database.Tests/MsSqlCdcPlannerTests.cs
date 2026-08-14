using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;
using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

/// <summary>
/// <see cref="MsSqlCdcPlanner"/> is the part of the SQL Server CDC connector that can lose data silently —
/// where to start, when retention has already discarded the requested range, how to cut a batch on a
/// transaction boundary — so it is the part covered here, with no SQL Server needed.
/// </summary>
public class MsSqlCdcPlannerTests
{
    // Deliberately distinct, ordered LSNs — 20 lowercase hex characters, CdcLsn's own encoded shape.
    private const string Lsn1 = "00000000000000000001";
    private const string Lsn2 = "00000000000000000002";
    private const string Lsn3 = "00000000000000000003";
    private const string Lsn5 = "00000000000000000005";

    private static DbSourceConfig Config() => new()
    {
        Host = "db",
        Database = "market",
        CaptureInstance = "dbo_Orders",
        BatchSize = 500,
    };

    // ------------------------------------------------------------------
    // PlanFrom — the four starting points
    // ------------------------------------------------------------------

    [Fact]
    public void AnExistingCursorAsksForOneLsnPastItNeverAReRead()
    {
        var step = MsSqlCdcPlanner.PlanFrom(Config(), Lsn1);

        Assert.Equal(MsSqlCdcFromKind.Cursor, step.Kind);
        Assert.Equal(MsSqlCdcPlanner.IncrementLsnSql, step.Sql);
        Assert.Equal("SELECT sys.fn_cdc_increment_lsn(@from)", step.Sql);
        var parameter = Assert.Single(step.Parameters);
        Assert.Equal("from", parameter.Key);
        Assert.Equal(CdcLsn.DecodeMsSql(Lsn1), parameter.Value);
        Assert.Null(step.ResolvedFrom);
    }

    [Fact]
    public void InitialCursorIsDecodedDirectlyWithNoRoundTrip()
    {
        var config = Config();
        config.InitialCursor = Lsn5;

        var step = MsSqlCdcPlanner.PlanFrom(config, cursor: null);

        Assert.Equal(MsSqlCdcFromKind.InitialCursor, step.Kind);
        Assert.Null(step.Sql);
        Assert.Empty(step.Parameters);
        Assert.Equal(Lsn5, step.ResolvedFrom);
    }

    [Fact]
    public void AMalformedInitialCursorThrowsNamingTheOffendingText()
    {
        var config = Config();
        config.InitialCursor = "not-an-lsn";

        var ex = Assert.Throws<FormatException>(() => MsSqlCdcPlanner.PlanFrom(config, cursor: null));
        Assert.Contains("not-an-lsn", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnExistingCursorWinsOverInitialCursorBecauseAConfigEditMustNotResetProgress()
    {
        var config = Config();
        config.InitialCursor = Lsn5;

        var step = MsSqlCdcPlanner.PlanFrom(config, Lsn1);

        Assert.Equal(MsSqlCdcFromKind.Cursor, step.Kind);
    }

    [Fact]
    public void InitialCursorWinsOverSnapshotBecauseItIsMoreSpecific()
    {
        var config = Config();
        config.Snapshot = true;
        config.InitialCursor = Lsn5;

        var step = MsSqlCdcPlanner.PlanFrom(config, cursor: null);

        Assert.Equal(MsSqlCdcFromKind.InitialCursor, step.Kind);
    }

    [Fact]
    public void SnapshotAsksForTheRetentionFloor()
    {
        var config = Config();
        config.Snapshot = true;

        var step = MsSqlCdcPlanner.PlanFrom(config, cursor: null);

        Assert.Equal(MsSqlCdcFromKind.Snapshot, step.Kind);
        Assert.Equal("SELECT sys.fn_cdc_get_min_lsn(@capture)", step.Sql);
        var parameter = Assert.Single(step.Parameters);
        Assert.Equal("capture", parameter.Key);
        Assert.Equal("dbo_Orders", parameter.Value);
    }

    [Fact]
    public void NoCursorNoInitialNoSnapshotAsksForTheTailAndIsASeedCycle()
    {
        var step = MsSqlCdcPlanner.PlanFrom(Config(), cursor: null);

        Assert.Equal(MsSqlCdcFromKind.Tail, step.Kind);
        Assert.Equal("SELECT sys.fn_cdc_get_max_lsn()", step.Sql);
        Assert.Empty(step.Parameters);
    }

    // ------------------------------------------------------------------
    // Retention breach
    // ------------------------------------------------------------------

    [Fact]
    public void ARequestedLsnOlderThanTheRetentionFloorThrowsNamingCaptureRequestedAndMinimum()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => MsSqlCdcPlanner.CheckRetention("dbo_Orders", Lsn1, Lsn5));

        Assert.Contains("dbo_Orders", ex.Message, StringComparison.Ordinal);
        Assert.Contains(Lsn1, ex.Message, StringComparison.Ordinal);
        Assert.Contains(Lsn5, ex.Message, StringComparison.Ordinal);
        Assert.Contains("retention", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("3 days", ex.Message, StringComparison.Ordinal);
        Assert.Contains("discarded", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ARequestedLsnAtOrAfterTheRetentionFloorDoesNotThrow()
    {
        var exNewer = Record.Exception(() => MsSqlCdcPlanner.CheckRetention("dbo_Orders", Lsn5, Lsn1));
        var exEqual = Record.Exception(() => MsSqlCdcPlanner.CheckRetention("dbo_Orders", Lsn1, Lsn1));

        Assert.Null(exNewer);
        Assert.Null(exEqual);
    }

    [Fact]
    public void RetentionComparisonUsesTheByteWiseCodecNotTextOrdering()
    {
        // A regression guard against the exact bug CdcLsn.CompareMsSql exists to avoid — this is not a
        // second test of that class, just proof the planner actually calls it rather than a text compare.
        var high = "000000000000000000ff";
        var higher = "00000000000000000100";

        Assert.Null(Record.Exception(() => MsSqlCdcPlanner.CheckRetention("dbo_Orders", higher, high)));
        Assert.Throws<InvalidOperationException>(() => MsSqlCdcPlanner.CheckRetention("dbo_Orders", high, higher));
    }

    // ------------------------------------------------------------------
    // Empty range
    // ------------------------------------------------------------------

    [Fact]
    public void AnEmptyRangeIsDetectedWhenToIsOlderThanFrom()
    {
        Assert.True(MsSqlCdcPlanner.IsEmptyRange(from: Lsn5, to: Lsn1));
    }

    [Fact]
    public void ARangeWhereToEqualsFromIsNotEmpty()
    {
        Assert.False(MsSqlCdcPlanner.IsEmptyRange(from: Lsn1, to: Lsn1));
    }

    [Fact]
    public void ARangeWhereToIsNewerThanFromIsNotEmpty()
    {
        Assert.False(MsSqlCdcPlanner.IsEmptyRange(from: Lsn1, to: Lsn5));
    }

    // ------------------------------------------------------------------
    // The main read
    // ------------------------------------------------------------------

    [Fact]
    public void TheReadUsesAllNotAllUpdateOldAndOrdersByLsnThenSeqval()
    {
        var plan = MsSqlCdcPlanner.PlanRead(Config(), Lsn1, Lsn5);

        Assert.Equal(
            "SELECT TOP (@batch) *, sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS __ts " +
            "FROM cdc.fn_cdc_get_all_changes_dbo_Orders(@from, @to, 'all') " +
            "ORDER BY __$start_lsn, __$seqval",
            plan.Sql);
    }

    [Fact]
    public void TheReadsParametersAreBoundInBatchFromToOrder()
    {
        var config = Config();
        config.BatchSize = 250;

        var plan = MsSqlCdcPlanner.PlanRead(config, Lsn1, Lsn5);

        Assert.Collection(
            plan.Parameters,
            p => { Assert.Equal("batch", p.Key); Assert.Equal(250, p.Value); },
            p => { Assert.Equal("from", p.Key); Assert.Equal(CdcLsn.DecodeMsSql(Lsn1), p.Value); },
            p => { Assert.Equal("to", p.Key); Assert.Equal(CdcLsn.DecodeMsSql(Lsn5), p.Value); });
    }

    [Fact]
    public void AZeroBatchSizeFallsBackToOneThousand()
    {
        var config = Config();
        config.BatchSize = 0;

        var plan = MsSqlCdcPlanner.PlanRead(config, Lsn1, Lsn5);

        Assert.Equal(1000, plan.Parameters.Single(p => p.Key == "batch").Value);
    }

    [Fact]
    public void TheCaptureInstanceIsNeverBoundAsAParameterItIsPartOfTheFunctionName()
    {
        var plan = MsSqlCdcPlanner.PlanRead(Config(), Lsn1, Lsn5);

        Assert.DoesNotContain(plan.Parameters, p => p.Key == "capture");
        Assert.Contains("fn_cdc_get_all_changes_dbo_Orders", plan.Sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("dbo_Orders")]
    [InlineData("_leading_underscore")]
    [InlineData("A1")]
    public void ValidCaptureInstanceNamesAreAccepted(string name)
        => Assert.True(MsSqlCdcPlanner.IsValidCaptureInstance(name));

    [Theory]
    [InlineData("dbo'; DROP TABLE x; --")]
    [InlineData("dbo.Orders")]
    [InlineData("dbo Orders")]
    [InlineData("dbo;Orders")]
    [InlineData("1dbo")]
    [InlineData("")]
    [InlineData(null)]
    public void CaptureInstanceNamesWithQuotesSemicolonsSpacesOrABadLeadingCharacterAreRejected(string? name)
        => Assert.False(MsSqlCdcPlanner.IsValidCaptureInstance(name));

    [Fact]
    public void PlanReadThrowsRatherThanInterpolateAnInvalidCaptureInstance()
    {
        var config = Config();
        config.CaptureInstance = "dbo'; DROP TABLE x; --";

        var ex = Assert.Throws<InvalidOperationException>(() => MsSqlCdcPlanner.PlanRead(config, Lsn1, Lsn5));
        Assert.Contains("not a valid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ------------------------------------------------------------------
    // Complete — op mapping, column stripping, table filter
    // ------------------------------------------------------------------

    private static Dictionary<string, object?> ChangeRow(string startLsn, int operation, DateTime? ts, params (string Key, object? Value)[] extra)
    {
        var row = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["__$start_lsn"] = CdcLsn.DecodeMsSql(startLsn),
            ["__$seqval"] = CdcLsn.DecodeMsSql(startLsn),
            ["__$operation"] = operation,
            ["__$update_mask"] = null,
        };
        if (ts is not null)
        {
            row["__ts"] = ts.Value;
        }

        foreach (var (key, value) in extra)
        {
            row[key] = value;
        }

        return row;
    }

    /// <summary>Runs <see cref="MsSqlCdcPlanner.Complete"/> and asserts it returned a batch rather than a
    /// re-read signal — the shorthand every test uses except the ones specifically about the re-read path.</summary>
    private static PolledBatch CompleteBatch(DbSourceConfig config, IReadOnlyList<Dictionary<string, object?>> rows, bool capped = false)
    {
        var result = MsSqlCdcPlanner.Complete(config, rows, capped);
        Assert.False(result.NeedsReread);
        return result.Batch!;
    }

    [Fact]
    public void OperationOneIsMappedToDeleteWithNegativeWeight()
    {
        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 1, null, ("id", 7))];

        var batch = CompleteBatch(Config(), rows);

        var row = Assert.Single(batch.Rows);
        Assert.Equal(CdcStamp.OpDelete, row[CdcStamp.OpColumn]);
        Assert.Equal(-1, row[CdcStamp.WeightColumn]);
    }

    [Fact]
    public void OperationTwoIsMappedToCreateWithPositiveWeight()
    {
        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, null, ("id", 7))];

        var row = Assert.Single(CompleteBatch(Config(), rows).Rows);
        Assert.Equal(CdcStamp.OpCreate, row[CdcStamp.OpColumn]);
        Assert.Equal(1, row[CdcStamp.WeightColumn]);
    }

    [Fact]
    public void OperationFourIsMappedToUpdateWithPositiveWeight()
    {
        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 4, null, ("id", 7))];

        var row = Assert.Single(CompleteBatch(Config(), rows).Rows);
        Assert.Equal(CdcStamp.OpUpdate, row[CdcStamp.OpColumn]);
        Assert.Equal(1, row[CdcStamp.WeightColumn]);
    }

    [Fact]
    public void OperationThreeTheUpdateBeforeImageNeverAppearsWithAllAndIsRejectedIfItSomehowDid()
    {
        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 3, null, ("id", 7))];

        var ex = Assert.Throws<InvalidOperationException>(() => MsSqlCdcPlanner.Complete(Config(), rows, capped: false));
        Assert.Contains("3", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnknownOperationCodeThrowsRatherThanGuessing()
    {
        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 99, null, ("id", 7))];

        Assert.Throws<InvalidOperationException>(() => MsSqlCdcPlanner.Complete(Config(), rows, capped: false));
    }

    [Fact]
    public void EveryDollarColumnAndTheTsAliasAreStrippedButBusinessColumnsSurvive()
    {
        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, new DateTime(2026, 8, 14, 10, 0, 0, DateTimeKind.Utc), ("id", 7), ("symbol", "AAPL"))];

        var row = Assert.Single(CompleteBatch(Config(), rows).Rows);

        Assert.False(row.ContainsKey("__$start_lsn"));
        Assert.False(row.ContainsKey("__$seqval"));
        Assert.False(row.ContainsKey("__$operation"));
        Assert.False(row.ContainsKey("__$update_mask"));
        Assert.False(row.ContainsKey("__ts"));
        Assert.Equal(7, row["id"]);
        Assert.Equal("AAPL", row["symbol"]);
    }

    [Fact]
    public void TsMsComesFromTheTsAliasColumn()
    {
        var at = new DateTime(2026, 8, 14, 10, 0, 0, DateTimeKind.Utc);
        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, at, ("id", 7))];

        var row = Assert.Single(CompleteBatch(Config(), rows).Rows);

        Assert.Equal(new DateTimeOffset(at).ToUnixTimeMilliseconds(), row[CdcStamp.TsColumn]);
    }

    [Fact]
    public void AMissingTsAliasLeavesTheTsColumnUnstampedRatherThanFabricatingATime()
    {
        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, null, ("id", 7))];

        var row = Assert.Single(CompleteBatch(Config(), rows).Rows);

        Assert.False(row.ContainsKey(CdcStamp.TsColumn));
    }

    [Fact]
    public void NoRowsLeavesTheCursorUnchangedRatherThanResettingIt()
    {
        var batch = CompleteBatch(Config(), []);

        Assert.Null(batch.Cursor);
        Assert.False(batch.HasMore);
        Assert.Empty(batch.Rows);
    }

    [Fact]
    public void ASingleGroupUnderBatchSizeIsEmittedWithHasMoreFalse()
    {
        // Uncapped: this read was not TOP-limited (or is itself the answer to a bounded re-read), so the
        // one group it contains is provably the whole transaction.
        var config = Config();
        config.BatchSize = 10;

        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, null, ("id", 1)), ChangeRow(Lsn1, 2, null, ("id", 2))];

        var batch = CompleteBatch(config, rows, capped: false);

        Assert.Equal(2, batch.Rows.Count);
        Assert.Equal(Lsn1, batch.Cursor);
        Assert.False(batch.HasMore);
    }

    [Fact]
    public void ACappedSingleGroupYieldsAReReadSignalRatherThanRowsOrAnAdvancedCursor()
    {
        // The fix this test pins: Complete must NEVER emit rows or move the cursor past a group whose
        // completeness it cannot prove. TOP(@batch) truncation and "this transaction had exactly this many
        // rows" are indistinguishable from inside this method — capped is the caller's own signal saying
        // which one happened, and here it says "possibly truncated".
        var config = Config();
        config.BatchSize = 2;

        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, null, ("id", 1)), ChangeRow(Lsn1, 2, null, ("id", 2))];

        var result = MsSqlCdcPlanner.Complete(config, rows, capped: true);

        Assert.True(result.NeedsReread);
        Assert.Equal(Lsn1, result.RereadBoundLsn);
        // The invariant worth pinning: no batch at all comes back, so there is no cursor value a caller
        // could accidentally persist here. Completeness was not established, so nothing advances.
        Assert.Null(result.Batch);
    }

    [Fact]
    public void AnUncappedSingleOversizedGroupIsEmittedWholeAndAdvancesTheCursor()
    {
        // The bounded re-read MsSqlCdcSource issues after the signal above: same rows, but this time the
        // caller KNOWS (no TOP was used) that this is the complete transaction — deliberately over the
        // configured BatchSize. BatchSize is a target for a read, not a hard ceiling on a batch.
        var config = Config();
        config.BatchSize = 2;

        List<Dictionary<string, object?>> rows =
        [
            ChangeRow(Lsn1, 2, null, ("id", 1)),
            ChangeRow(Lsn1, 2, null, ("id", 2)),
            ChangeRow(Lsn1, 2, null, ("id", 3)),
        ];

        var batch = CompleteBatch(config, rows, capped: false);

        Assert.Equal(3, batch.Rows.Count);
        Assert.Equal(Lsn1, batch.Cursor);
        Assert.False(batch.HasMore);
    }

    [Fact]
    public void MultipleGroupsDropTheTrailingOneAndAdvanceTheCursorOnlyPastTheKeptOnes()
    {
        var config = Config();
        config.BatchSize = 100;

        List<Dictionary<string, object?>> rows =
        [
            ChangeRow(Lsn1, 2, null, ("id", 1)),
            ChangeRow(Lsn1, 2, null, ("id", 2)),
            ChangeRow(Lsn2, 4, null, ("id", 1)),
            ChangeRow(Lsn3, 1, null, ("id", 3)), // trailing group — dropped, possibly incomplete
        ];

        var batch = CompleteBatch(config, rows, capped: false);

        Assert.Equal(3, batch.Rows.Count);
        Assert.Equal(Lsn2, batch.Cursor);
        Assert.True(batch.HasMore);
    }

    [Fact]
    public void ThreeOrMoreGroupsStillDropOnlyTheLastOne()
    {
        var config = Config();
        config.BatchSize = 100;

        List<Dictionary<string, object?>> rows =
        [
            ChangeRow(Lsn1, 2, null, ("id", 1)),
            ChangeRow(Lsn2, 2, null, ("id", 2)),
            ChangeRow(Lsn3, 2, null, ("id", 3)),
            ChangeRow(Lsn5, 1, null, ("id", 4)),
        ];

        var batch = CompleteBatch(config, rows, capped: false);

        Assert.Equal(3, batch.Rows.Count);
        Assert.Equal(Lsn3, batch.Cursor);
        Assert.True(batch.HasMore);
    }

    [Fact]
    public void ACappedMultiGroupBatchStillJustDropsItsTrailingGroupRatherThanSignallingAReRead()
    {
        // The re-read signal is reserved for the single-group case — with two or more groups every group
        // except the last is already PROVEN complete (a different __$start_lsn appeared after it), so
        // there is no ambiguity to resolve and the ordinary "drop the trailing group" rule still applies
        // even though this read WAS capped.
        var config = Config();
        config.BatchSize = 4;

        List<Dictionary<string, object?>> rows =
        [
            ChangeRow(Lsn1, 2, null, ("id", 1)),
            ChangeRow(Lsn2, 2, null, ("id", 2)),
            ChangeRow(Lsn3, 2, null, ("id", 3)),
            ChangeRow(Lsn5, 1, null, ("id", 4)), // trailing group, dropped — read WAS capped (4 rows == BatchSize)
        ];

        var result = MsSqlCdcPlanner.Complete(config, rows, capped: true);

        Assert.False(result.NeedsReread);
        Assert.Equal(3, result.Batch!.Rows.Count);
        Assert.Equal(Lsn3, result.Batch.Cursor);
        Assert.True(result.Batch.HasMore);
    }

    [Fact]
    public void TablesFilterKeepsRowsMatchingThisSourcesConfiguredTable()
    {
        var config = Config();
        config.Schema = "dbo";
        config.Table = "Orders";
        config.Tables = "dbo.Orders";

        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, null, ("id", 1))];

        var row = Assert.Single(CompleteBatch(config, rows).Rows);
        Assert.Equal("dbo.Orders", row[CdcStamp.TableColumn]);
    }

    [Fact]
    public void TablesFilterDropsRowsWhenThisSourcesTableIsNotInTheList()
    {
        var config = Config();
        config.Schema = "dbo";
        config.Table = "Orders";
        config.Tables = "dbo.SomethingElse";

        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, null, ("id", 1))];

        var batch = CompleteBatch(config, rows);

        Assert.Empty(batch.Rows);
        // The cursor still advances — it tracks how far the CDC log was read, not what was emitted.
        Assert.Equal(Lsn1, batch.Cursor);
    }

    [Fact]
    public void AnEmptyTablesFilterKeepsEverythingEvenWithNoTableConfigured()
    {
        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, null, ("id", 1))];

        var row = Assert.Single(CompleteBatch(Config(), rows).Rows);
        Assert.False(row.ContainsKey(CdcStamp.TableColumn));
    }

    [Fact]
    public void ATablesFilterWithNoTableConfiguredDropsEverythingSinceMembershipCannotBeProven()
    {
        var config = Config();
        config.Tables = "dbo.Orders";

        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, null, ("id", 1))];

        Assert.Empty(CompleteBatch(config, rows).Rows);
    }

    [Fact]
    public void ATableWithNoSchemaConfiguredDefaultsToDbo()
    {
        var config = Config();
        config.Table = "Orders";

        List<Dictionary<string, object?>> rows = [ChangeRow(Lsn1, 2, null, ("id", 1))];

        var row = Assert.Single(CompleteBatch(config, rows).Rows);
        Assert.Equal("dbo.Orders", row[CdcStamp.TableColumn]);
    }
}
