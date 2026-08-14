using System.Data.Common;
using System.Globalization;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Connectors.Database.Tests.Integration;

/// <summary>
/// The source half of plan 014, against a real server. Written once and inherited by one class per
/// dialect, so PostgreSQL and SQL Server are held to the same statements about the same behaviour — a
/// divergence between the two engines shows up here as one red test rather than as a scenario somebody
/// only ever wrote for one of them.
///
/// <para><b>What these cover that the 176 unit tests structurally cannot.</b> <c>DbPollPlanner</c> is pure
/// and its rules are already covered; what was never covered is whether the SQL it emits PARSES, whether
/// the <c>@cursor</c> parameter binds to the column's actual type, and whether the cursor
/// <c>DbCursor.Encode</c> mints out of a value the server returned decodes back into something the server
/// will accept on the next cycle. That round trip — server value → opaque string → bound parameter →
/// server comparison — is the entire mechanism by which this connector can silently lose rows, and until
/// this file existed no test had ever closed it.</para>
///
/// <para><b>Each test seeds its own table</b> (<see cref="DbBackend.NewTable"/>), so nothing here depends
/// on execution order and a failure leaves its own data behind to inspect inside the container — right up
/// until the fixture removes it.</para>
/// </summary>
public abstract class DbSourceSuite(DbServers servers)
{
    private static readonly DateTime Epoch = new(2026, 8, 14, 9, 0, 0, DateTimeKind.Utc);

    protected abstract DbBackend Backend { get; }

    /// <summary>Held so xunit constructs the container fixture before any test in this class runs.</summary>
    protected DbServers Servers { get; } = servers;

    protected DbSource Source => new(Backend.Dialect);

    /// <summary>"schema"."table" in this dialect's quoting. A deliberate re-implementation of the
    /// production <c>SqlDialectExtensions.QualifiedTable</c>, which is internal: the tests' own fixture SQL
    /// must not be able to agree with the connector by sharing its bug.</summary>
    protected string Quoted(string table)
        => $"{Backend.Dialect.QuoteIdent(Backend.Dialect.DefaultSchema)}.{Backend.Dialect.QuoteIdent(table)}";

    protected string Column(string name) => Backend.Dialect.QuoteIdent(name);

    // ---- 1. snapshot-then-tail, one page per cycle ----

    /// <summary>The claim <c>Snapshot</c> exists to make: a table larger than one batch pages through in
    /// SUCCESSIVE driver cycles, each returning its own cursor and <c>HasMore</c> until a short page ends
    /// it. Paging inside one <c>PollAsync</c> would put those intermediate cursors back in memory, which
    /// is the failure the whole polled seam was designed to avoid.</summary>
    protected async Task SnapshotPagesThroughSuccessiveCycles()
    {
        var table = await SeedNumberedAsync(25).ConfigureAwait(false);
        var def = Backend.Definition(Backend.SourceConfig(table, c => { c.Snapshot = true; c.BatchSize = 10; }));
        var source = Source;

        var first = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(Enumerable.Range(1, 10).Select(i => (long)i), Ids(first));
        Assert.Equal("10", first.Cursor);
        Assert.True(first.HasMore, "a full page means there is more waiting: re-arm now, do not wait for the schedule");

        var second = await source.PollAsync(def, first.Cursor, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(Enumerable.Range(11, 10).Select(i => (long)i), Ids(second));
        Assert.Equal("20", second.Cursor);
        Assert.True(second.HasMore);

        var third = await source.PollAsync(def, second.Cursor, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(Enumerable.Range(21, 5).Select(i => (long)i), Ids(third));
        Assert.Equal("25", third.Cursor);
        Assert.False(third.HasMore, "a short page is the end of the snapshot");

        var fourth = await source.PollAsync(def, third.Cursor, CancellationToken.None).ConfigureAwait(false);
        Assert.Empty(fourth.Rows);
        Assert.Null(fourth.Cursor);
        Assert.False(fourth.HasMore);
    }

    // ---- 2. resume mid-snapshot from nothing but the persisted cursor ----

    /// <summary>The reason the cursor is persisted per PAGE rather than per snapshot. Everything except
    /// the cursor string is thrown away between the two halves of this test — new transport instance, new
    /// <c>SourceDefinition</c>, new config object — because that is exactly what a silo recycle leaves the
    /// driver holding. Continuing means no row is read twice and none is skipped.</summary>
    protected async Task CursorResumesMidSnapshotAfterEverythingElseIsDiscarded()
    {
        var table = await SeedNumberedAsync(25).ConfigureAwait(false);

        string? persisted;
        {
            var before = Backend.Definition(Backend.SourceConfig(table, c => { c.Snapshot = true; c.BatchSize = 10; }));
            var page = await Source.PollAsync(before, null, CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(10, page.Rows.Count);
            persisted = page.Cursor;
        }

        // Nothing above is in scope any more. This is the restarted process.
        var after = Backend.Definition(Backend.SourceConfig(table, c => { c.Snapshot = true; c.BatchSize = 10; }));
        var resumed = Source;

        var second = await resumed.PollAsync(after, persisted, CancellationToken.None).ConfigureAwait(false);
        var third = await resumed.PollAsync(after, second.Cursor, CancellationToken.None).ConfigureAwait(false);

        Assert.Equal(Enumerable.Range(11, 15).Select(i => (long)i), [.. Ids(second), .. Ids(third)]);
        Assert.Equal("25", third.Cursor);
    }

    // ---- 3. tailing: inserts and updates arrive after the snapshot ----

    /// <summary>An <c>updated_at</c> cursor with a dedup key — the shape the descriptor recommends — sees
    /// both a new row and a CHANGE to an old one, which an id cursor by construction cannot. The updated
    /// row arrives carrying its new value, not the snapshot's.</summary>
    protected async Task TailingSeesInsertsAndUpdates()
    {
        var table = await SeedTimestampedAsync(3).ConfigureAwait(false);
        var def = Backend.Definition(Backend.SourceConfig(table, c =>
        {
            c.CursorColumn = "updated_at";
            c.CursorKind = CursorKinds.Timestamp;
            c.DedupKeyColumn = "id";
            c.Snapshot = true;
            c.BatchSize = 10;
        }));
        var source = Source;

        var snapshot = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(3, snapshot.Rows.Count);
        Assert.NotNull(snapshot.Cursor);

        await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
        {
            await Sql.ExecAsync(
                connection,
                $"INSERT INTO {Quoted(table)} ({Column("id")}, {Column("symbol")}, {Column("updated_at")}) VALUES (@p0, @p1, @p2)",
                4L, "NEW", Backend.Timestamp(Epoch.AddSeconds(30))).ConfigureAwait(false);
            await Sql.ExecAsync(
                connection,
                $"UPDATE {Quoted(table)} SET {Column("symbol")} = @p0, {Column("updated_at")} = @p1 WHERE {Column("id")} = @p2",
                "CHANGED", Backend.Timestamp(Epoch.AddSeconds(40)), 1L).ConfigureAwait(false);
        }

        var tail = await source.PollAsync(def, snapshot.Cursor, CancellationToken.None).ConfigureAwait(false);

        var inserted = Assert.Single(tail.Rows, r => Id(r) == 4);
        Assert.Equal("NEW", inserted["symbol"]);
        var updated = Assert.Single(tail.Rows, r => Id(r) == 1);
        Assert.Equal("CHANGED", updated["symbol"]);
    }

    // ---- 4. no snapshot, no cursor: seed from MAX and emit nothing ----

    /// <summary>Branch 4 of the four starting states. It emits NOTHING on its first cycle by design —
    /// "new rows only" — and the row that arrives after it is the first one the source was ever meant to
    /// see. On the empty table the seed stays null rather than fixing the source at zero.</summary>
    protected async Task NoSnapshotSeedsFromMaxAndEmitsNothingOnTheFirstCycle()
    {
        var table = await SeedNumberedAsync(25).ConfigureAwait(false);
        var def = Backend.Definition(Backend.SourceConfig(table, c => { c.Snapshot = false; c.BatchSize = 10; }));
        var source = Source;

        var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        Assert.Empty(seed.Rows);
        Assert.Equal("25", seed.Cursor);
        Assert.False(seed.HasMore);

        await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
        {
            await Sql.ExecAsync(
                connection,
                $"INSERT INTO {Quoted(table)} ({Column("id")}, {Column("symbol")}) VALUES (@p0, @p1)",
                26L, "AFTER").ConfigureAwait(false);
        }

        var tail = await source.PollAsync(def, seed.Cursor, CancellationToken.None).ConfigureAwait(false);
        var row = Assert.Single(tail.Rows);
        Assert.Equal(26L, Id(row));
    }

    /// <summary>The empty-table half of branch 4, stated separately because the alternative — seeding to
    /// zero — would make the first row ever inserted invisible forever.</summary>
    protected async Task SeedingAnEmptyTableLeavesTheCursorUnset()
    {
        var table = await SeedNumberedAsync(0).ConfigureAwait(false);
        var def = Backend.Definition(Backend.SourceConfig(table, c => c.Snapshot = false));

        var seed = await Source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);

        Assert.Empty(seed.Rows);
        Assert.Null(seed.Cursor);
    }

    // ---- 5. InitialCursor, in both modes ----

    /// <summary>"Start here" is the TRANSPORT's job — neither driver seeds its persisted cursor from the
    /// config — so if this were not implemented the field would be silently inert. Table mode and query
    /// mode are asserted together because query mode REQUIRES it and table mode merely allows it.</summary>
    protected async Task InitialCursorIsHonouredInTableAndQueryMode()
    {
        var table = await SeedNumberedAsync(25).ConfigureAwait(false);

        var tableMode = Backend.Definition(Backend.SourceConfig(table, c =>
        {
            c.InitialCursor = "20";
            c.BatchSize = 10;
        }));
        var fromTable = await Source.PollAsync(tableMode, null, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(Enumerable.Range(21, 5).Select(i => (long)i), Ids(fromTable));

        var queryMode = Backend.Definition(Backend.SourceConfig(table, c =>
        {
            c.Query = $"SELECT * FROM {Quoted(table)} WHERE {Column("id")} > @cursor ORDER BY {Column("id")} ASC";
            c.InitialCursor = "23";
        }));

        List<string> errors = [];
        Source.Validate(queryMode, errors);
        Assert.Empty(errors);

        var fromQuery = await Source.PollAsync(queryMode, null, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal([24L, 25L], Ids(fromQuery));
        Assert.Equal("25", fromQuery.Cursor);
    }

    // ---- 6. the >= re-read, and the dedup that pays for it ----

    /// <summary>
    /// The bargain a timestamp cursor makes, end to end and against the server that has to honour it.
    /// <c>&gt;=</c> deliberately RE-READS every row sharing the watermark's instant — that is how a row
    /// written in the same millisecond as the watermark stops being lost — and
    /// <c>DedupKeyColumn</c> is what stops those re-reads from being re-emitted. Both halves are asserted:
    /// the raw poll really does return the repeats, and the driven cycle really does emit none of them,
    /// while a genuinely new row at the SAME instant still gets through.
    /// </summary>
    protected async Task GreaterOrEqualRereadsTheWatermarkAndDedupSuppressesIt()
    {
        var table = Backend.NewTable("tick");
        var shared = Epoch.AddSeconds(10);
        await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
        {
            await Sql.ExecAsync(
                connection,
                $"CREATE TABLE {Quoted(table)} ({Column("id")} bigint NOT NULL PRIMARY KEY, {Column("updated_at")} {Backend.TimestampType} NOT NULL)").ConfigureAwait(false);
            await InsertTickAsync(connection, table, 1, Epoch).ConfigureAwait(false);
            await InsertTickAsync(connection, table, 2, shared).ConfigureAwait(false);
            await InsertTickAsync(connection, table, 3, shared).ConfigureAwait(false);
        }

        var config = Backend.SourceConfig(table, c =>
        {
            c.CursorColumn = "updated_at";
            c.CursorKind = CursorKinds.Timestamp;
            c.DedupKeyColumn = "id";
            c.Snapshot = true;
            c.BatchSize = 10;
        });
        var def = Backend.Definition(config);
        var source = Source;
        DedupTracker dedup = new();

        var first = await Run(source, def, null, dedup).ConfigureAwait(false);
        Assert.Equal([1L, 2L, 3L], first.Result.Rows.Select(Id).Order());

        // The re-read is real: rows 2 and 3 share the watermark and >= reads them again.
        var raw = await source.PollAsync(def, first.Cursor, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal([2L, 3L], Ids(raw).Order());

        // ...and the driven cycle emits none of them.
        var second = await Run(source, def, first.Cursor, dedup).ConfigureAwait(false);
        Assert.Empty(second.Result.Rows);
        Assert.Equal(first.Cursor, second.Cursor);

        // The row `>` would have lost: same instant as the watermark, inserted afterwards.
        await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
        {
            await InsertTickAsync(connection, table, 4, shared).ConfigureAwait(false);
        }

        var third = await Run(source, def, second.Cursor, dedup).ConfigureAwait(false);
        Assert.Equal(4L, Id(Assert.Single(third.Result.Rows)));
    }

    // ---- the third cursor kind ----

    /// <summary>The <c>string</c> cursor kind against a real text column. It is the kind whose ordering
    /// belongs to the SERVER's collation rather than to .NET, so the only shape it is honestly good for is
    /// the one the codec's own doc names — zero-padded ids, where the two orderings cannot disagree. That
    /// is what is tested here, deliberately, rather than a mixed-case column whose result would be a
    /// statement about a collation instead of about this connector.</summary>
    protected async Task AStringCursorPagesInOrder()
    {
        var table = Backend.NewTable("padded");
        await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
        {
            await Sql.ExecAsync(
                connection,
                $"CREATE TABLE {Quoted(table)} ({Column("id")} {Backend.TextType} NOT NULL PRIMARY KEY)").ConfigureAwait(false);
            for (var i = 1; i <= 5; i++)
            {
                await Sql.ExecAsync(
                    connection,
                    $"INSERT INTO {Quoted(table)} ({Column("id")}) VALUES (@p0)",
                    i.ToString("D4", CultureInfo.InvariantCulture)).ConfigureAwait(false);
            }
        }

        var def = Backend.Definition(Backend.SourceConfig(table, c =>
        {
            c.CursorKind = CursorKinds.String;
            c.Snapshot = true;
            c.BatchSize = 2;
        }));
        var source = Source;

        var first = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(["0001", "0002"], first.Rows.Select(r => r["id"]));
        Assert.Equal("0002", first.Cursor);

        var second = await source.PollAsync(def, first.Cursor, CancellationToken.None).ConfigureAwait(false);
        var third = await source.PollAsync(def, second.Cursor, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(["0003", "0004", "0005"], [.. second.Rows.Select(r => r["id"]), .. third.Rows.Select(r => r["id"])]);
    }

    // ---- helpers ----

    private static Task<PolledCycleOutcome> Run(IPolledTransport transport, SourceDefinition def, string? cursor, DedupTracker dedup)
        => PolledSourceCore.RunCycleAsync(
            transport, def, cursor, dedup, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), CancellationToken.None,
            // Exactly what ConnectorGrain/ConnectorActor pass: the dedup column comes off the kind's own
            // config, because a polled row source has no mapping document to read one out of.
            dedupKeyField: def.Connector?.Db?.DedupKeyColumn is { Length: > 0 } key ? key : null);

    private static long Id(Dictionary<string, object?> row) => Convert.ToInt64(row["id"], CultureInfo.InvariantCulture);

    private static IEnumerable<long> Ids(PolledBatch batch) => batch.Rows.Select(Id);

    private async Task<string> SeedNumberedAsync(int count)
    {
        var table = Backend.NewTable("orders");
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await Sql.ExecAsync(
            connection,
            $"CREATE TABLE {Quoted(table)} ({Column("id")} bigint NOT NULL PRIMARY KEY, {Column("symbol")} {Backend.TextType} NOT NULL)").ConfigureAwait(false);

        for (var i = 1; i <= count; i++)
        {
            await Sql.ExecAsync(
                connection,
                $"INSERT INTO {Quoted(table)} ({Column("id")}, {Column("symbol")}) VALUES (@p0, @p1)",
                (long)i, "SYM" + i.ToString(CultureInfo.InvariantCulture)).ConfigureAwait(false);
        }

        return table;
    }

    private async Task<string> SeedTimestampedAsync(int count)
    {
        var table = Backend.NewTable("ticks");
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await Sql.ExecAsync(
            connection,
            $"CREATE TABLE {Quoted(table)} (" +
            $"{Column("id")} bigint NOT NULL PRIMARY KEY, " +
            $"{Column("symbol")} {Backend.TextType} NOT NULL, " +
            $"{Column("updated_at")} {Backend.TimestampType} NOT NULL)").ConfigureAwait(false);

        for (var i = 1; i <= count; i++)
        {
            await Sql.ExecAsync(
                connection,
                $"INSERT INTO {Quoted(table)} ({Column("id")}, {Column("symbol")}, {Column("updated_at")}) VALUES (@p0, @p1, @p2)",
                (long)i, "SYM" + i.ToString(CultureInfo.InvariantCulture), Backend.Timestamp(Epoch.AddSeconds(i))).ConfigureAwait(false);
        }

        return table;
    }

    private Task InsertTickAsync(DbConnection connection, string table, long id, DateTime at)
        => Sql.ExecAsync(
            connection,
            $"INSERT INTO {Quoted(table)} ({Column("id")}, {Column("updated_at")}) VALUES (@p0, @p1)",
            id, Backend.Timestamp(at));
}

/// <summary>The source suite against a live PostgreSQL 17 container.</summary>
[Collection(DbServers.CollectionName)]
public sealed class PostgresSourceTests(DbServers servers) : DbSourceSuite(servers)
{
    protected override DbBackend Backend => DbBackends.Postgres;

    [PostgresFact]
    public Task SnapshotPagesThrough() => SnapshotPagesThroughSuccessiveCycles();

    [PostgresFact]
    public Task CursorResumesAfterARestart() => CursorResumesMidSnapshotAfterEverythingElseIsDiscarded();

    [PostgresFact]
    public Task TailingSeesChanges() => TailingSeesInsertsAndUpdates();

    [PostgresFact]
    public Task SeedingFromMax() => NoSnapshotSeedsFromMaxAndEmitsNothingOnTheFirstCycle();

    [PostgresFact]
    public Task SeedingAnEmptyTable() => SeedingAnEmptyTableLeavesTheCursorUnset();

    [PostgresFact]
    public Task InitialCursor() => InitialCursorIsHonouredInTableAndQueryMode();

    [PostgresFact]
    public Task DedupSuppressesTheReread() => GreaterOrEqualRereadsTheWatermarkAndDedupSuppressesIt();

    [PostgresFact]
    public Task StringCursor() => AStringCursorPagesInOrder();

    /// <summary>
    /// The branch <c>DbCursor</c> was written for and which nothing had ever run against a server: a
    /// cursor on a ZONELESS <c>timestamp</c> column. Every other timestamp test in this file uses
    /// <c>timestamptz</c>, which round-trips through a UTC <see cref="DateTime"/>; a zoneless column must
    /// round-trip through an UNSPECIFIED one, because that is the only thing Npgsql will bind back to it —
    /// bind a UTC value and the comparison silently shifts by the server's zone offset, re-reading or
    /// skipping hours of rows depending on the sign. The persisted cursor is asserted to carry NEITHER a
    /// trailing Z NOR an offset, which is the single bit the encoding exists to preserve.
    /// </summary>
    [PostgresFact]
    public async Task AZonelessTimestampCursorKeepsItsZonelessness()
    {
        var table = Backend.NewTable("zoneless");
        var noon = new DateTime(2026, 8, 14, 12, 0, 0, DateTimeKind.Unspecified);
        await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
        {
            await Sql.ExecAsync(
                connection,
                $"CREATE TABLE {Quoted(table)} ({Column("id")} bigint NOT NULL PRIMARY KEY, {Column("at")} timestamp NOT NULL)").ConfigureAwait(false);
            for (var i = 1; i <= 3; i++)
            {
                await Sql.ExecAsync(
                    connection,
                    $"INSERT INTO {Quoted(table)} ({Column("id")}, {Column("at")}) VALUES (@p0, @p1)",
                    (long)i, noon.AddMinutes(i)).ConfigureAwait(false);
            }
        }

        var def = Backend.Definition(Backend.SourceConfig(table, c =>
        {
            c.CursorColumn = "at";
            c.CursorKind = CursorKinds.Timestamp;
            c.Snapshot = true;
            c.BatchSize = 2;
        }));
        var source = Source;

        var first = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(2, first.Rows.Count);
        Assert.NotNull(first.Cursor);
        Assert.DoesNotContain("Z", first.Cursor!, StringComparison.Ordinal);
        Assert.DoesNotContain("+", first.Cursor!, StringComparison.Ordinal);

        // The proof that the bound parameter compared correctly rather than being shifted: exactly the
        // third row, not all three again and not none.
        var second = await source.PollAsync(def, first.Cursor, CancellationToken.None).ConfigureAwait(false);
        var row = Assert.Single(second.Rows);
        Assert.Equal(3L, Convert.ToInt64(row["id"], CultureInfo.InvariantCulture));
        Assert.Equal(DateTimeKind.Unspecified, Assert.IsType<DateTime>(row["at"]).Kind);
    }
}

/// <summary>The same suite, word for word, against a live SQL Server 2022 container.</summary>
[Collection(DbServers.CollectionName)]
public sealed class MsSqlSourceTests(DbServers servers) : DbSourceSuite(servers)
{
    protected override DbBackend Backend => DbBackends.SqlServer;

    [MsSqlFact]
    public Task SnapshotPagesThrough() => SnapshotPagesThroughSuccessiveCycles();

    [MsSqlFact]
    public Task CursorResumesAfterARestart() => CursorResumesMidSnapshotAfterEverythingElseIsDiscarded();

    [MsSqlFact]
    public Task TailingSeesChanges() => TailingSeesInsertsAndUpdates();

    [MsSqlFact]
    public Task SeedingFromMax() => NoSnapshotSeedsFromMaxAndEmitsNothingOnTheFirstCycle();

    [MsSqlFact]
    public Task SeedingAnEmptyTable() => SeedingAnEmptyTableLeavesTheCursorUnset();

    [MsSqlFact]
    public Task InitialCursor() => InitialCursorIsHonouredInTableAndQueryMode();

    [MsSqlFact]
    public Task DedupSuppressesTheReread() => GreaterOrEqualRereadsTheWatermarkAndDedupSuppressesIt();

    [MsSqlFact]
    public Task StringCursor() => AStringCursorPagesInOrder();

    /// <summary>
    /// The other branch of the timestamp codec, and SQL Server is the only engine here that can reach it:
    /// a <c>datetimeoffset</c> cursor comes back as a <see cref="DateTimeOffset"/>, encodes with an
    /// explicit <c>±hh:mm</c>, and must decode back to a <see cref="DateTimeOffset"/> rather than to a
    /// <see cref="DateTime"/> — the conversion <c>DbCursor.DecodeTimestamp</c> refuses to write as a
    /// conditional expression precisely because the implicit conversion would silently reinterpret it in
    /// the HOST's local zone.
    /// </summary>
    [MsSqlFact]
    public async Task ADateTimeOffsetCursorKeepsItsOffset()
    {
        var table = Backend.NewTable("offset");
        var noon = new DateTimeOffset(2026, 8, 14, 12, 0, 0, TimeSpan.FromHours(2));
        await using (var connection = await Backend.OpenAsync().ConfigureAwait(false))
        {
            await Sql.ExecAsync(
                connection,
                $"CREATE TABLE {Quoted(table)} ({Column("id")} bigint NOT NULL PRIMARY KEY, {Column("at")} datetimeoffset NOT NULL)").ConfigureAwait(false);
            for (var i = 1; i <= 3; i++)
            {
                await Sql.ExecAsync(
                    connection,
                    $"INSERT INTO {Quoted(table)} ({Column("id")}, {Column("at")}) VALUES (@p0, @p1)",
                    (long)i, noon.AddMinutes(i)).ConfigureAwait(false);
            }
        }

        var def = Backend.Definition(Backend.SourceConfig(table, c =>
        {
            c.CursorColumn = "at";
            c.CursorKind = CursorKinds.Timestamp;
            c.Snapshot = true;
            c.BatchSize = 2;
        }));
        var source = Source;

        var first = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(2, first.Rows.Count);
        Assert.Contains("+02:00", first.Cursor!, StringComparison.Ordinal);

        var second = await source.PollAsync(def, first.Cursor, CancellationToken.None).ConfigureAwait(false);
        var row = Assert.Single(second.Rows);
        Assert.Equal(3L, Convert.ToInt64(row["id"], CultureInfo.InvariantCulture));
        Assert.Equal(TimeSpan.FromHours(2), Assert.IsType<DateTimeOffset>(row["at"]).Offset);
    }
}
