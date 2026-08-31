using System.Globalization;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Sinks;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests.Integration;

/// <summary>
/// The sink half of plan 014, against a real server — written once, run against both engines.
///
/// <para><b>What only a server can answer.</b> The planner's SQL text and parameter order are already
/// covered by unit tests, but a <c>MERGE</c> is not valid because a test says its text looks right, an
/// <c>ON CONFLICT</c> target is only accepted if a unique index really covers it, SQL Server's 2100-
/// parameter ceiling is enforced by the SERVER and not by the driver, and "the failed batch was rolled
/// back and dropped" is a statement about a table, not about a counter. Every one of those is a first
/// execution here.</para>
///
/// <para><b>The one that had never run anywhere:</b> a table UPDATE reaches a sink as TWO deltas on the
/// same key — <c>-1</c> carrying the old row and <c>+1</c> carrying the new one. A sink that applied
/// "deletes last" literally would delete the row the update just wrote. <c>DbSinkPlanner</c>'s
/// last-delta-per-key resolution exists to prevent precisely that, and
/// <see cref="AnUpdateArrivingAsTwoDeltasLeavesTheNewRow"/> is the first thing that has ever checked it
/// against a database rather than against the planner's own output.</para>
/// </summary>
public abstract class DbSinkSuite(DbServers servers)
{
    protected abstract DbBackend Backend { get; }

    protected DbServers Servers { get; } = servers;

    /// <summary>"schema"."table" in this dialect's quoting — a deliberate re-implementation of the
    /// internal production helper, so the tests' own SQL cannot agree with the connector by sharing its bug.</summary>
    protected string Quoted(string table)
        => $"{Backend.Dialect.QuoteIdent(Backend.Dialect.DefaultSchema)}.{Backend.Dialect.QuoteIdent(table)}";

    protected string Column(string name) => Backend.Dialect.QuoteIdent(name);

    // ---- 8. append accumulates, and never deletes ----

    /// <summary>Append is a log: every delivered row becomes an INSERT, a NEGATIVE weight is still an
    /// INSERT, and nothing this sink can be handed removes a row. <c>includeWeight</c> is on so the sign
    /// is visible in the table rather than inferred from the row count.</summary>
    protected async Task AppendAccumulatesAndNeverDeletes()
    {
        var table = Backend.NewTable("append");
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await Sql.ExecAsync(
            connection,
            $"CREATE TABLE {Quoted(table)} (" +
            $"{Column("symbol")} {Backend.TextType} NOT NULL, " +
            $"{Column("qty")} bigint NOT NULL, " +
            $"{Column("_weight")} bigint NULL)").ConfigureAwait(false);

        await using var client = Client(Backend.SinkConfig(table, c => c.IncludeWeight = true));

        await client.PublishBatchAsync([Delta("AAPL", 1), Delta("MSFT", 2), Delta("NVDA", 3)], CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(3, await Sql.CountAsync(connection, $"SELECT COUNT(*) FROM {Quoted(table)}").ConfigureAwait(false));

        // A retraction and a re-insert of a symbol already written. Append means both land.
        await client.PublishBatchAsync([Delta("AAPL", 1, weight: -1), Delta("AAPL", 7)], CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(5, await Sql.CountAsync(connection, $"SELECT COUNT(*) FROM {Quoted(table)}").ConfigureAwait(false));
        Assert.Equal(3, await Sql.CountAsync(
            connection, $"SELECT COUNT(*) FROM {Quoted(table)} WHERE {Column("symbol")} = @p0", "AAPL").ConfigureAwait(false));
        Assert.Equal(-1, await Sql.CountAsync(
            connection, $"SELECT MIN({Column("_weight")}) FROM {Quoted(table)}").ConfigureAwait(false));
        Assert.Equal(5, client.Counters.Published);
        Assert.Equal(0, client.Counters.Failed);
    }

    // ---- 9 + 10. upsert mirrors current state ----

    /// <summary>Insert then update the same key leaves ONE row carrying the new values — the whole claim
    /// of "mirror current state" — and a negative weight then removes it. Both dialects need a unique
    /// index over the key columns for their own reasons (PostgreSQL's <c>ON CONFLICT</c> target must BE
    /// one; SQL Server's <c>MERGE</c> merely performs badly without one), which is why the table has a
    /// primary key and why the descriptor says so.</summary>
    protected async Task UpsertMirrorsCurrentStateAndANegativeWeightDeletes()
    {
        var table = await KeyedTableAsync("mirror").ConfigureAwait(false);
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await using var client = Client(Backend.SinkConfig(table, c =>
        {
            c.Mode = DbSinkModes.Upsert;
            c.KeyColumns = "symbol";
        }));

        await client.PublishBatchAsync([Delta("AAPL", 1), Delta("MSFT", 2)], CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(2, await Sql.CountAsync(connection, $"SELECT COUNT(*) FROM {Quoted(table)}").ConfigureAwait(false));

        await client.PublishBatchAsync([Delta("AAPL", 99)], CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(2, await Sql.CountAsync(connection, $"SELECT COUNT(*) FROM {Quoted(table)}").ConfigureAwait(false));
        Assert.Equal(99, await Sql.CountAsync(
            connection, $"SELECT {Column("qty")} FROM {Quoted(table)} WHERE {Column("symbol")} = @p0", "AAPL").ConfigureAwait(false));

        await client.PublishBatchAsync([Delta("AAPL", 99, weight: -1)], CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(0, await Sql.CountAsync(
            connection, $"SELECT COUNT(*) FROM {Quoted(table)} WHERE {Column("symbol")} = @p0", "AAPL").ConfigureAwait(false));
        Assert.Equal(1, await Sql.CountAsync(connection, $"SELECT COUNT(*) FROM {Quoted(table)}").ConfigureAwait(false));
        Assert.Equal(0, client.Counters.Failed);
    }

    // ---- 11. the update that arrives as two deltas ----

    /// <summary>The case the last-delta resolution exists for, and the one that had never been run against
    /// a server. <c>-1</c> old row and <c>+1</c> new row for ONE key, in one batch: the row must survive
    /// carrying the NEW value. A sink that took "deletes last" literally would leave nothing behind. The
    /// mirror case — <c>+1</c> then <c>-1</c> — must still end deleted, or the resolution would just be an
    /// unconditional refusal to delete.</summary>
    protected async Task AnUpdateArrivingAsTwoDeltasLeavesTheNewRow()
    {
        var table = await KeyedTableAsync("twodelta").ConfigureAwait(false);
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await using var client = Client(Backend.SinkConfig(table, c =>
        {
            c.Mode = DbSinkModes.Upsert;
            c.KeyColumns = "symbol";
        }));

        await client.PublishBatchAsync([Delta("AAPL", 1)], CancellationToken.None).ConfigureAwait(false);

        // Exactly what a table UPDATE looks like on the delta stream.
        await client.PublishBatchAsync(
            [Delta("AAPL", 1, weight: -1), Delta("AAPL", 42), Delta("MSFT", 7), Delta("MSFT", 7, weight: -1)],
            CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(42, await Sql.CountAsync(
            connection, $"SELECT {Column("qty")} FROM {Quoted(table)} WHERE {Column("symbol")} = @p0", "AAPL").ConfigureAwait(false));
        Assert.Equal(0, await Sql.CountAsync(
            connection, $"SELECT COUNT(*) FROM {Quoted(table)} WHERE {Column("symbol")} = @p0", "MSFT").ConfigureAwait(false));
        Assert.Equal(1, await Sql.CountAsync(connection, $"SELECT COUNT(*) FROM {Quoted(table)}").ConfigureAwait(false));
        Assert.Equal(0, client.Counters.Failed);
    }

    // ---- 12. the parameter ceiling ----

    /// <summary>A genuinely wide table — 31 columns x 100 rows = 3100 bound parameters — against SQL
    /// Server's documented per-batch ceiling of 2100. The chunker has to SPLIT rather than let the server
    /// refuse the command, and the split is by column count rather than by a round number of rows, which
    /// is why the same batch is one statement on PostgreSQL (65535) and several on SQL Server. The planner
    /// is asked what it intends and the server is asked what actually landed.</summary>
    protected async Task WideBatchesAreChunkedAgainstTheParameterCeiling()
    {
        const int columnCount = 30;
        const int rowCount = 100;

        var table = Backend.NewTable("wide");
        var columns = Enumerable.Range(0, columnCount).Select(i => "c" + i.ToString(CultureInfo.InvariantCulture)).ToList();
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await Sql.ExecAsync(
            connection,
            $"CREATE TABLE {Quoted(table)} ({Column("id")} bigint NOT NULL, " +
            string.Join(", ", columns.Select(c => $"{Column(c)} {Backend.TextType} NULL")) + ")").ConfigureAwait(false);

        List<NatsTableDeltaMessage> batch = [];
        for (var r = 0; r < rowCount; r++)
        {
            Dictionary<string, object?> row = new(StringComparer.Ordinal) { ["id"] = (long)r };
            foreach (var c in columns)
            {
                row[c] = $"{c}-{r.ToString(CultureInfo.InvariantCulture)}";
            }

            batch.Add(new NatsTableDeltaMessage { Table = table, Seq = r, Weight = 1, Row = row });
        }

        var config = Backend.SinkConfig(table);
        var perStatement = DbSinkPlanner.ChunkSize(Backend.Dialect, columnCount + 1);
        var plan = DbSinkPlanner.Plan(
            config, Backend.Dialect, Quoted(table),
            [.. batch.Select(m => new SinkRow(m.Row, m.Weight))]);

        Assert.Equal((rowCount + perStatement - 1) / perStatement, plan.Statements.Count);
        Assert.All(plan.Statements, s => Assert.True(
            s.Parameters.Count <= Backend.Dialect.MaxCommandParameters,
            $"a chunk of {s.Parameters.Count} parameters would exceed this dialect's ceiling of {Backend.Dialect.MaxCommandParameters}"));

        await using var client = Client(config);
        await client.PublishBatchAsync(batch, CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(0, client.Counters.Failed);
        Assert.Equal(rowCount, await Sql.CountAsync(connection, $"SELECT COUNT(*) FROM {Quoted(table)}").ConfigureAwait(false));
        Assert.Equal("c29-99", await Sql.ScalarAsync(
            connection, $"SELECT {Column("c29")} FROM {Quoted(table)} WHERE {Column("id")} = @p0", 99L).ConfigureAwait(false));
    }

    // ---- 13. a failed commit does not throw, and the batch is dropped ----

    /// <summary>A table that does not exist: the sink must count the rows as failed, report once, and
    /// return — never throw, because the publisher services await it with no try/catch and a throwing sink
    /// takes the host down. This is the "no DDL, ever" case an operator will actually hit.</summary>
    protected async Task AMissingTableIsCountedAndDroppedRatherThanThrown()
    {
        Exception? reported = null;
        await using var client = Client(
            Backend.SinkConfig(Backend.NewTable("does_not_exist")), (_, ex) => reported = ex);

        await client.PublishBatchAsync([Delta("AAPL", 1), Delta("MSFT", 2)], CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(2, client.Counters.Failed);
        Assert.Equal(0, client.Counters.Published);
        Assert.NotNull(client.Counters.LastError);
        Assert.NotNull(reported);
    }

    /// <summary>The other half of "one delivered batch = one transaction": a constraint violated by ONE
    /// row of the batch rolls back the rows that were fine too. Asserted against the table, because a
    /// counter cannot tell the difference between a rollback and a partial write.</summary>
    protected async Task AConstraintViolationRollsTheWholeBatchBack()
    {
        var table = await KeyedTableAsync("constraint").ConfigureAwait(false);
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await Sql.ExecAsync(
            connection,
            $"INSERT INTO {Quoted(table)} ({Column("symbol")}, {Column("qty")}) VALUES (@p0, @p1)", "AAPL", 1L).ConfigureAwait(false);

        Exception? reported = null;
        await using var client = Client(Backend.SinkConfig(table), (_, ex) => reported = ex);

        // Append mode: the third row collides with the primary key that is already there.
        await client.PublishBatchAsync(
            [Delta("MSFT", 2), Delta("NVDA", 3), Delta("AAPL", 4)], CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(3, client.Counters.Failed);
        Assert.Equal(0, client.Counters.Published);
        Assert.NotNull(reported);

        // Rolled back: the two rows that would have succeeded are not there either, and the pre-existing
        // row is untouched.
        Assert.Equal(1, await Sql.CountAsync(connection, $"SELECT COUNT(*) FROM {Quoted(table)}").ConfigureAwait(false));
        Assert.Equal(1, await Sql.CountAsync(
            connection, $"SELECT {Column("qty")} FROM {Quoted(table)} WHERE {Column("symbol")} = @p0", "AAPL").ConfigureAwait(false));
    }

    // ---- helpers ----

    /// <summary>An UNKEYED table — no primary key, no unique index — for the two dialect-specific tests
    /// below, which pin what each server does when <c>keyColumns</c> names something no index covers.</summary>
    protected async Task<string> UnkeyedTableAsync(string prefix)
    {
        var table = Backend.NewTable(prefix);
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await Sql.ExecAsync(
            connection,
            $"CREATE TABLE {Quoted(table)} (" +
            $"{Column("symbol")} {Backend.TextType} NOT NULL, " +
            $"{Column("qty")} bigint NOT NULL)").ConfigureAwait(false);
        return table;
    }

    protected DbSinkClient Client(DbSinkConfig config, Action<string, Exception>? onFailure = null)
        => new(config, Backend.Dialect, "table", "live", onFailure);

    protected static NatsTableDeltaMessage Delta(string symbol, long qty, long weight = 1) => new()
    {
        Table = "live",
        Seq = 1,
        Weight = weight,
        Row = new Dictionary<string, object?>(StringComparer.Ordinal) { ["symbol"] = symbol, ["qty"] = qty },
    };

    private async Task<string> KeyedTableAsync(string prefix)
    {
        var table = Backend.NewTable(prefix);
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await Sql.ExecAsync(
            connection,
            $"CREATE TABLE {Quoted(table)} (" +
            $"{Column("symbol")} {Backend.TextType} NOT NULL PRIMARY KEY, " +
            $"{Column("qty")} bigint NOT NULL)").ConfigureAwait(false);
        return table;
    }
}

/// <summary>The sink suite against a live PostgreSQL 17 container.</summary>
[Collection(DbServers.CollectionName)]
public sealed class PostgresSinkTests(DbServers servers) : DbSinkSuite(servers)
{
    protected override DbBackend Backend => DbBackends.Postgres;

    [PostgresFact]
    public Task AppendAccumulates() => AppendAccumulatesAndNeverDeletes();

    [PostgresFact]
    public Task UpsertMirrors() => UpsertMirrorsCurrentStateAndANegativeWeightDeletes();

    [PostgresFact]
    public Task UpdateAsTwoDeltas() => AnUpdateArrivingAsTwoDeltasLeavesTheNewRow();

    [PostgresFact]
    public Task WideBatchesChunk() => WideBatchesAreChunkedAgainstTheParameterCeiling();

    [PostgresFact]
    public Task MissingTableIsDropped() => AMissingTableIsCountedAndDroppedRatherThanThrown();

    [PostgresFact]
    public Task ConstraintViolationRollsBack() => AConstraintViolationRollsTheWholeBatchBack();

    /// <summary>
    /// <c>PostgresDialect</c>'s doc makes a promise about a case it deliberately does not pre-flight: an
    /// <c>ON CONFLICT</c> target that no unique index covers fails AT THE SERVER, with PostgreSQL's own
    /// message, counted and surfaced through the failure callback — rather than silently degrading to a
    /// plain INSERT, which would turn "mirror current state" into an append-only pile nobody notices. This
    /// is the test of that promise, and it is the operator error most likely to actually happen.
    /// </summary>
    [PostgresFact]
    public async Task AnUpsertKeyNoUniqueIndexCoversIsRefusedByTheServerAndCounted()
    {
        var table = await UnkeyedTableAsync("noindex").ConfigureAwait(false);
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);

        Exception? reported = null;
        await using var client = Client(
            Backend.SinkConfig(table, c => { c.Mode = DbSinkModes.Upsert; c.KeyColumns = "symbol"; }),
            (_, ex) => reported = ex);

        await client.PublishBatchAsync([Delta("AAPL", 1)], CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(1, client.Counters.Failed);
        Assert.Equal(0, client.Counters.Published);
        Assert.Contains("ON CONFLICT", client.Counters.LastError!, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(reported);

        // Not silently degraded to an INSERT: nothing was written at all.
        Assert.Equal(0, await Sql.CountAsync(connection, $"SELECT COUNT(*) FROM {Quoted(table)}").ConfigureAwait(false));
    }
}

/// <summary>The same suite against a live SQL Server 2022 container — where the parameter ceiling is 2100
/// and the upsert is a MERGE.</summary>
[Collection(DbServers.CollectionName)]
public sealed class MsSqlSinkTests(DbServers servers) : DbSinkSuite(servers)
{
    protected override DbBackend Backend => DbBackends.SqlServer;

    [MsSqlFact]
    public Task AppendAccumulates() => AppendAccumulatesAndNeverDeletes();

    [MsSqlFact]
    public Task UpsertMirrors() => UpsertMirrorsCurrentStateAndANegativeWeightDeletes();

    [MsSqlFact]
    public Task UpdateAsTwoDeltas() => AnUpdateArrivingAsTwoDeltasLeavesTheNewRow();

    [MsSqlFact]
    public Task WideBatchesChunk() => WideBatchesAreChunkedAgainstTheParameterCeiling();

    [MsSqlFact]
    public Task MissingTableIsDropped() => AMissingTableIsCountedAndDroppedRatherThanThrown();

    [MsSqlFact]
    public Task ConstraintViolationRollsBack() => AConstraintViolationRollsTheWholeBatchBack();

    /// <summary>
    /// The same configuration PostgreSQL refuses outright, on SQL Server: <c>MERGE</c> matches by VALUE
    /// rather than through an index, so it still mirrors correctly with no unique index — it just scans to
    /// do it. The descriptor tells operators to have a unique index for BOTH engines, and this test is why
    /// that advice is worded as advice on one engine and as a hard requirement on the other. Pinned rather
    /// than left to be discovered, because the two engines silently disagreeing is worse than either
    /// behaviour on its own.
    /// </summary>
    [MsSqlFact]
    public async Task WithoutAUniqueIndexMergeStillMirrorsItJustScans()
    {
        var table = await UnkeyedTableAsync("noindex").ConfigureAwait(false);
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await using var client = Client(
            Backend.SinkConfig(table, c => { c.Mode = DbSinkModes.Upsert; c.KeyColumns = "symbol"; }));

        await client.PublishBatchAsync([Delta("AAPL", 1)], CancellationToken.None).ConfigureAwait(false);
        await client.PublishBatchAsync([Delta("AAPL", 77)], CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(0, client.Counters.Failed);
        Assert.Equal(1, await Sql.CountAsync(connection, $"SELECT COUNT(*) FROM {Quoted(table)}").ConfigureAwait(false));
        Assert.Equal(77, await Sql.CountAsync(
            connection, $"SELECT {Column("qty")} FROM {Quoted(table)} WHERE {Column("symbol")} = @p0", "AAPL").ConfigureAwait(false));
    }
}
