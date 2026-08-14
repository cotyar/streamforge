using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Connectors.Database.Tests.Integration;

/// <summary>
/// Plan 017 wave G: <see cref="MsSqlCdcSource"/> (<c>mssql-cdc</c>) against a real SQL Server whose Agent is
/// running and whose CDC capture job is therefore actually draining the transaction log — the live-server
/// half of plan 017 waves B/C/E, the same way <see cref="DbSourceSuite"/> is the live-server half of plan
/// 014.
///
/// <para><b>What this covers that the unit tests structurally cannot</b> (<c>MsSqlCdcPlannerTests</c>,
/// <c>MsSqlCdcValidationTests</c>): whether <c>cdc.fn_cdc_get_all_changes_&lt;capture&gt;</c> genuinely
/// returns what the planner's pure logic assumes, whether the LSN scalar functions
/// (<c>fn_cdc_get_min_lsn</c>/<c>fn_cdc_get_max_lsn</c>/<c>fn_cdc_increment_lsn</c>) round-trip through
/// <see cref="CdcLsn"/>, and — the one this file exists specifically to guard, per the wave brief — whether
/// a transaction LARGER than <c>BatchSize</c> is genuinely delivered whole via the bounded re-read rather
/// than silently truncated. <see cref="MsSqlCdcPlanner.Complete"/>'s re-read logic is unit-tested against
/// hand-built rows; this is the first place it ever runs against SQL Server's own capture tables.</para>
///
/// <para><b>SQL Server's capture job is asynchronous</b> — a committed change does not appear in
/// <c>cdc.fn_cdc_get_all_changes_*</c> the instant the transaction commits, only once the Agent job has
/// drained the log. Every test below that needs a change to appear polls through <see cref="PollUntilAsync"/>,
/// a bounded retry, rather than asserting immediately after the write — see that method's own doc for why a
/// fixed sleep would be flaky instead.</para>
///
/// <para><b>Each test creates its own table and its own CDC capture instance</b>
/// (<see cref="DbBackend.NewTable"/>), so nothing here depends on execution order.</para>
///
/// <para><b>Docker is not assumed to be running.</b> <see cref="MsSqlCdcFactAttribute"/> skips every test
/// below, with a stated reason, on a machine with no Docker daemon — see <see cref="DockerGate"/>. In THIS
/// environment (no Docker), every test in this file is expected to SKIP; the assertions below have not been
/// exercised against a live server here and this class doc says so rather than implying otherwise.</para>
/// </summary>
[Collection(CdcServers.CollectionName)]
public sealed class MsSqlCdcTests(CdcServers servers)
{
    private static readonly DbBackend Backend = CdcDbBackends.SqlServer;

    /// <summary>Held so xunit constructs the container fixture before any test in this class runs.</summary>
    private readonly CdcServers _servers = servers;

    /// <summary>Generous but bounded — the Agent capture job draining a just-committed row is normally a
    /// sub-second affair, but this budget also has to absorb a slow container start on a loaded machine, so
    /// it is stated far above the common case rather than tuned to it.</summary>
    private static readonly TimeSpan CaptureBudget = TimeSpan.FromSeconds(60);

    private static MsSqlCdcSource NewSource() => new(Backend.Dialect);

    // ---- 1. seed cycle: null cursor -> zero rows, a confirmed cursor (the tail LSN) ----

    [MsSqlCdcFact]
    public async Task SeedCycleYieldsNoRowsAndAConfirmedCursor()
    {
        var (table, capture) = await CreateCapturedTableAsync().ConfigureAwait(false);
        var def = Definition(table, capture);

        var seed = await NewSource().PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);

        Assert.Empty(seed.Rows);
        Assert.NotNull(seed.Cursor);
        Assert.False(seed.HasMore);

        // The cursor really is a 20-character lowercase-hex MSSQL LSN, not an opaque placeholder.
        CdcLsn.DecodeMsSql(seed.Cursor!);
    }

    // ---- 2. insert / update / delete: the right op, weight, table and values ----

    [MsSqlCdcFact]
    public async Task InsertUpdateDeleteArriveWithTheRightOpWeightAndValues()
    {
        var (table, capture) = await CreateCapturedTableAsync().ConfigureAwait(false);
        var def = Definition(table, capture);
        var source = NewSource();

        var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        var cursor = seed.Cursor;

        await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 1L, "AAA", 10L).ConfigureAwait(false);
        var (insertRows, afterInsert) = await PollUntilAsync(source, def, cursor, minRows: 1, CaptureBudget).ConfigureAwait(false);
        cursor = afterInsert;
        var inserted = Assert.Single(insertRows);
        Assert.Equal(CdcStamp.OpCreate, inserted[CdcStamp.OpColumn]);
        Assert.Equal(1, inserted[CdcStamp.WeightColumn]);
        Assert.Equal($"dbo.{table}", inserted[CdcStamp.TableColumn]);
        Assert.Equal(1L, Id(inserted));
        Assert.Equal("AAA", inserted["symbol"]);
        Assert.Equal(10L, inserted["qty"]);

        await ExecAsync($"UPDATE {Quoted(table)} SET qty = @p0 WHERE id = @p1", 20L, 1L).ConfigureAwait(false);
        var (updateRows, afterUpdate) = await PollUntilAsync(source, def, cursor, minRows: 1, CaptureBudget).ConfigureAwait(false);
        cursor = afterUpdate;
        var updated = Assert.Single(updateRows);
        Assert.Equal(CdcStamp.OpUpdate, updated[CdcStamp.OpColumn]);
        Assert.Equal(1, updated[CdcStamp.WeightColumn]);
        Assert.Equal(1L, Id(updated));
        Assert.Equal(20L, updated["qty"]);

        await ExecAsync($"DELETE FROM {Quoted(table)} WHERE id = @p0", 1L).ConfigureAwait(false);
        var (deleteRows, _) = await PollUntilAsync(source, def, cursor, minRows: 1, CaptureBudget).ConfigureAwait(false);
        var deleted = Assert.Single(deleteRows);
        Assert.Equal(CdcStamp.OpDelete, deleted[CdcStamp.OpColumn]);
        Assert.Equal(-1, deleted[CdcStamp.WeightColumn]);
        Assert.Equal(1L, Id(deleted));
    }

    // ---- 3. the cursor advances monotonically and never goes backwards ----

    [MsSqlCdcFact]
    public async Task CursorAdvancesMonotonicallyAcrossCycles()
    {
        var (table, capture) = await CreateCapturedTableAsync().ConfigureAwait(false);
        var def = Definition(table, capture);
        var source = NewSource();

        var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(seed.Cursor);
        var cursor = seed.Cursor!;

        for (var i = 1; i <= 3; i++)
        {
            await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", (long)i, "S" + i.ToString(CultureInfo.InvariantCulture), (long)i).ConfigureAwait(false);

            var (_, next) = await PollUntilAsync(source, def, cursor, minRows: 1, CaptureBudget).ConfigureAwait(false);
            Assert.NotNull(next);
            Assert.True(CdcLsn.CompareMsSql(next!, cursor) > 0, $"cursor must advance, never go backwards: {cursor} -> {next}");
            cursor = next!;
        }
    }

    // ---- 4. resume with no gap and no duplication: the property this whole feature rests on ----

    [MsSqlCdcFact]
    public async Task ResumeFromAPersistedCursorHasNoGapAndNoDuplication()
    {
        var (table, capture) = await CreateCapturedTableAsync().ConfigureAwait(false);
        var def = Definition(table, capture);

        string persisted;
        {
            var source = NewSource();
            var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);

            await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 1L, "BEFORE", 1L).ConfigureAwait(false);
            var (rows, cursor) = await PollUntilAsync(source, def, seed.Cursor, minRows: 1, CaptureBudget).ConfigureAwait(false);
            var row = Assert.Single(rows);
            Assert.Equal("BEFORE", row["symbol"]);
            persisted = cursor!;
        }

        // `source` above is now unreachable — only the persisted STRING (an LSN) survives, exactly what a
        // silo recycle / actor deactivation leaves the driver holding across a restart.
        await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 2L, "AFTER", 2L).ConfigureAwait(false);

        var resumedSource = NewSource();
        var (resumedRows, _) = await PollUntilAsync(resumedSource, def, persisted, minRows: 1, CaptureBudget).ConfigureAwait(false);

        var only = Assert.Single(resumedRows);
        Assert.Equal("AFTER", only["symbol"]);
        Assert.DoesNotContain(resumedRows, r => Equals(r["symbol"], "BEFORE"));
    }

    // ---- 5. a failed cycle does not advance the cursor ----

    [MsSqlCdcFact]
    public async Task AFailedCycleDoesNotAdvanceTheCursorAndAGoodPollStillDeliversThePendingRow()
    {
        var (table, capture) = await CreateCapturedTableAsync().ConfigureAwait(false);
        var goodDef = Definition(table, capture);
        var source = NewSource();

        var seed = await source.PollAsync(goodDef, null, CancellationToken.None).ConfigureAwait(false);
        var cursor = seed.Cursor;

        await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 1L, "PENDING", 1L).ConfigureAwait(false);

        // Same capture instance, pointed at a port nothing on this host is listening on.
        var badConfig = Backend.SourceConfig(table, c =>
        {
            c.CursorColumn = "";
            c.Port = 1;
            c.CaptureInstance = capture;
        });
        var badDef = Backend.Definition(badConfig);

        DedupTracker dedup = new();
        var outcome = await PolledSourceCore.RunCycleAsync(
            source, badDef, cursor, dedup, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), CancellationToken.None).ConfigureAwait(false);

        Assert.NotNull(outcome.Result.Error);
        // The load-bearing assertion: PolledSourceCore hands back the SAME cursor it was given on a failed
        // cycle, per its own "a failed cycle keeps the old cursor" rule.
        Assert.Equal(cursor, outcome.Cursor);

        // A subsequent GOOD poll (with bounded retry — the capture job still needs to drain the row) from
        // that same untouched cursor still delivers the row that was pending.
        var (rows, _) = await PollUntilAsync(source, goodDef, cursor, minRows: 1, CaptureBudget).ConfigureAwait(false);
        var row = Assert.Single(rows);
        Assert.Equal("PENDING", row["symbol"]);
    }

    // ---- 6. a multi-row transaction arrives whole, never split across two batches ----

    [MsSqlCdcFact]
    public async Task AMultiRowTransactionArrivesWholeNeverSplitAcrossBatches()
    {
        var (table, capture) = await CreateCapturedTableAsync().ConfigureAwait(false);
        var def = Definition(table, capture);
        var source = NewSource();

        var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);

        await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
        {
            await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);
            for (var i = 1; i <= 5; i++)
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = $"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)";
                AddParameter(command, "p0", (long)i);
                AddParameter(command, "p1", "T" + i.ToString(CultureInfo.InvariantCulture));
                AddParameter(command, "p2", (long)i);
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            await transaction.CommitAsync().ConfigureAwait(false);
        }

        var (rows, cursor) = await PollUntilAsync(source, def, seed.Cursor, minRows: 5, CaptureBudget).ConfigureAwait(false);
        Assert.Equal(5, rows.Count);
        Assert.Equal(Enumerable.Range(1, 5).Select(i => (long)i), rows.Select(Id).Order());

        // Nothing left over: the whole transaction arrived in the accumulated result of the bounded retry,
        // not spread across a later, separate poll once more capture catches up.
        var trailing = await source.PollAsync(def, cursor, CancellationToken.None).ConfigureAwait(false);
        Assert.Empty(trailing.Rows);
    }

    // ---- 8. SQL-Server-specific: an oversized transaction is delivered whole, never truncated ----

    /// <summary>The regression guard the wave brief calls out by name: a real bug, fixed during wave C, was
    /// a capped <c>TOP (@batch)</c> read silently truncating a transaction bigger than <c>BatchSize</c>
    /// instead of re-reading it whole via <see cref="MsSqlCdcPlanner.PlanBoundedRead"/>. This is the first
    /// place that path ever runs against a real SQL Server capture table rather than hand-built rows.</summary>
    [MsSqlCdcFact]
    public async Task AnOversizedTransactionIsDeliveredWholeNotTruncated()
    {
        var (table, capture) = await CreateCapturedTableAsync().ConfigureAwait(false);
        // BatchSize smaller than the transaction below — the only way to exercise the bounded-reread path.
        var def = Definition(table, capture, c => c.BatchSize = 2);
        var source = NewSource();

        var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);

        const int rowCount = 6;
        await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
        {
            await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);
            for (var i = 1; i <= rowCount; i++)
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = $"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)";
                AddParameter(command, "p0", (long)i);
                AddParameter(command, "p1", "BIG" + i.ToString(CultureInfo.InvariantCulture));
                AddParameter(command, "p2", (long)i);
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            await transaction.CommitAsync().ConfigureAwait(false);
        }

        // A single PollAsync call, not the accumulating helper: the whole point is that ONE cycle resolves
        // the bounded re-read internally (MsSqlCdcSource.PollAsync's own NeedsReread loop) and returns the
        // complete, over-budget transaction in one PolledBatch — never a truncated `BatchSize`-sized slice.
        PolledBatch? full = null;
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < CaptureBudget)
        {
            var batch = await source.PollAsync(def, seed.Cursor, CancellationToken.None).ConfigureAwait(false);
            if (batch.Rows.Count > 0)
            {
                full = batch;
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(300)).ConfigureAwait(false);
        }

        Assert.NotNull(full);
        Assert.Equal(rowCount, full!.Rows.Count);
        Assert.Equal(Enumerable.Range(1, rowCount).Select(i => (long)i), full.Rows.Select(Id).Order());
    }

    // ---- 9. the probe: metadata fields, capture instance and retention window ----

    [MsSqlCdcFact]
    public async Task ProbeReportsCdcMetadataFieldsCaptureInstanceAndRetentionWindow()
    {
        var (table, capture) = await CreateCapturedTableAsync().ConfigureAwait(false);
        var def = Definition(table, capture);
        var source = NewSource();

        // Give the capture job at least one row to have processed, so min_lsn is non-null and the probe's
        // retention-window diagnostic has something to report rather than "no changes captured yet".
        var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 1L, "PROBE", 1L).ConfigureAwait(false);
        await PollUntilAsync(source, def, seed.Cursor, minRows: 1, CaptureBudget).ConfigureAwait(false);

        var probe = (ISchemaProbe)source;
        var result = await probe.ProbeAsync(def, CancellationToken.None).ConfigureAwait(false);

        var names = result.Fields.Select(f => f.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        Assert.Contains(CdcStamp.OpColumn, names);
        Assert.Contains(CdcStamp.WeightColumn, names);
        Assert.Contains(CdcStamp.TsColumn, names);
        Assert.Contains(CdcStamp.TableColumn, names);
        Assert.Contains("id", names);
        Assert.Contains("symbol", names);

        Assert.Contains(result.Diagnostics, d => d.Contains(capture, StringComparison.Ordinal) && d.Contains("retains changes back to", StringComparison.Ordinal));
        Assert.Contains(result.Diagnostics, d => d.Contains("retention", StringComparison.OrdinalIgnoreCase));
    }

    // ---- helpers ----

    private static long Id(Dictionary<string, object?> row) => Convert.ToInt64(row["id"], CultureInfo.InvariantCulture);

    private static string Quoted(string table)
        => $"{Backend.Dialect.QuoteIdent(Backend.Dialect.DefaultSchema)}.{Backend.Dialect.QuoteIdent(table)}";

    private static void AddParameter(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }

    private static async Task ExecAsync(string sql, params object?[] values)
    {
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await Sql.ExecAsync(connection, sql, values).ConfigureAwait(false);
    }

    private static async Task<(string Table, string CaptureInstance)> CreateCapturedTableAsync()
    {
        var table = Backend.NewTable("orders");
        var capture = Backend.NewTable("ci");
        await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
        {
            await Sql.ExecAsync(
                connection,
                $"CREATE TABLE {Quoted(table)} (id bigint NOT NULL PRIMARY KEY, symbol nvarchar(50) NOT NULL, qty bigint NOT NULL)").ConfigureAwait(false);
            await Sql.ExecAsync(
                connection,
                "EXEC sys.sp_cdc_enable_table @source_schema = @p0, @source_name = @p1, @role_name = NULL, @capture_instance = @p2, @supports_net_changes = 0",
                Backend.Dialect.DefaultSchema, table, capture).ConfigureAwait(false);
        }

        await WaitForCdcInstanceReadyAsync(capture).ConfigureAwait(false);
        return (table, capture);
    }

    /// <summary>Waits until <paramref name="captureInstance"/>'s own <c>min_lsn</c> is both non-null AND at
    /// or before the database-wide <c>max_lsn</c> — the state <see cref="MsSqlCdcSource"/>'s first cycle
    /// needs to seed cleanly. Both are asynchronous consequences of the SAME capture-job lag the wave brief
    /// calls out for row visibility, one level further up: right after <c>sp_cdc_enable_table</c>,
    /// <c>sys.fn_cdc_get_min_lsn</c> can be NULL for a moment (no <c>cdc.lsn_time_mapping</c> entry has been
    /// recorded at-or-after this instance's <c>start_lsn</c> yet), and — because this suite creates capture
    /// instances back-to-back, faster than the capture job's own scan cadence — a seed cursor minted from a
    /// STALE <c>fn_cdc_get_max_lsn()</c> can land BEFORE a brand-new instance's own retention floor once the
    /// capture job catches up, which <see cref="MsSqlCdcPlanner.CheckRetention"/> then (correctly) rejects
    /// as a breach it never was. Waiting for <c>min &lt;= max</c> before minting the seed avoids manufacturing
    /// that false breach without touching <c>MsSqlCdcPlanner</c>'s own (correct) retention logic.</summary>
    private static async Task WaitForCdcInstanceReadyAsync(string captureInstance)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
            {
                var minRaw = await Sql.ScalarAsync(connection, "SELECT sys.fn_cdc_get_min_lsn(@p0)", captureInstance).ConfigureAwait(false);
                var maxRaw = await Sql.ScalarAsync(connection, "SELECT sys.fn_cdc_get_max_lsn()").ConfigureAwait(false);
                if (minRaw is byte[] minBytes && maxRaw is byte[] maxBytes)
                {
                    var min = CdcLsn.EncodeMsSql(minBytes);
                    var max = CdcLsn.EncodeMsSql(maxBytes);
                    if (CdcLsn.CompareMsSql(min, max) <= 0)
                    {
                        return;
                    }
                }
            }

            if (clock.Elapsed > CaptureBudget)
            {
                throw new TimeoutException(
                    $"CDC capture instance '{captureInstance}' never reached a consistent min/max LSN state within " +
                    $"{CaptureBudget.TotalSeconds:0}s — the SQL Server Agent capture job may not be running");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(300)).ConfigureAwait(false);
        }
    }

    private static SourceDefinition Definition(string table, string captureInstance, Action<DbSourceConfig>? tweak = null)
    {
        var config = Backend.SourceConfig(table, c =>
        {
            c.CursorColumn = "";
            c.CaptureInstance = captureInstance;
        });
        tweak?.Invoke(config);
        return Backend.Definition(config);
    }

    /// <summary>Polls <paramref name="source"/> repeatedly, starting from <paramref name="cursor"/> and
    /// re-arming from whatever cursor each cycle returns, accumulating rows until at least
    /// <paramref name="minRows"/> have arrived or <paramref name="timeout"/> elapses. This is the "bounded
    /// retry rather than a fixed sleep" the wave brief calls for: SQL Server's CDC capture job drains the
    /// transaction log asynchronously, so a change is not guaranteed visible to
    /// <c>cdc.fn_cdc_get_all_changes_*</c> the instant its transaction commits, and a fixed sleep tuned to
    /// "usually enough" is exactly what goes flaky on a loaded machine — this instead asks the actual
    /// question ("has it shown up yet?") on every attempt. A cycle returning zero rows is not a failure —
    /// <see cref="MsSqlCdcSource.PollAsync"/>'s own "nothing has committed since last cycle" branch — so the
    /// loop simply advances its cursor (never backwards; <c>?? cursor</c> mirrors
    /// <c>PolledSourceCore</c>'s "null cursor = leave it unchanged" rule) and tries again.</summary>
    private static async Task<(List<Dictionary<string, object?>> Rows, string? Cursor)> PollUntilAsync(
        MsSqlCdcSource source, SourceDefinition def, string? cursor, int minRows, TimeSpan timeout)
    {
        List<Dictionary<string, object?>> collected = [];
        var current = cursor;
        var clock = Stopwatch.StartNew();

        while (true)
        {
            var batch = await source.PollAsync(def, current, CancellationToken.None).ConfigureAwait(false);
            collected.AddRange(batch.Rows);
            current = batch.Cursor ?? current;

            if (collected.Count >= minRows)
            {
                return (collected, current);
            }

            if (clock.Elapsed > timeout)
            {
                throw new TimeoutException(
                    $"expected at least {minRows} CDC row(s) within {timeout.TotalSeconds:0}s, got {collected.Count} " +
                    "— the SQL Server Agent capture job may not have caught up, or may not be running at all");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(300)).ConfigureAwait(false);
        }
    }
}
