using System.Data.Common;
using System.Globalization;
using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Connectors.Database.Tests.Integration;

/// <summary>
/// Plan 017 wave G: <see cref="PgCdcSource"/> (<c>postgres-cdc</c>) against a real PostgreSQL logical
/// replication slot — the live-server half of plan 017 waves D/E, the same way <see cref="DbSourceSuite"/>
/// is the live-server half of plan 014.
///
/// <para><b>What this covers that the unit tests structurally cannot</b> (<c>CdcStampTests</c>,
/// <c>PgCdcValidationTests</c>, <c>PgTupleDecoderTests</c>, ...): whether <c>Npgsql.Replication</c> actually
/// hands this reader a working <c>pgoutput</c> stream, whether the LSN this reader mints round-trips through
/// <see cref="CdcLsn"/> into something Postgres accepts back as a starting position, and whether the
/// transaction-boundary batching survives a REAL commit rather than a hand-built <c>CommitMessage</c>. That
/// round trip — server LSN → opaque cursor string → bound replication position → server acceptance — is
/// exactly the mechanism a native CDC source can silently lose rows through, and nothing short of a real
/// slot exercises it.</para>
///
/// <para><b>Each test creates its own table, publication and (via <c>CreateSlotIfMissing</c>) slot</b>, all
/// under a unique name (<see cref="DbBackend.NewTable"/>), so nothing here depends on execution order and a
/// failure leaves its own slot/publication/table behind to inspect — right up until <see cref="CdcServers"/>
/// removes the container. A replication slot nobody drops PINS WAL for as long as the container lives, which
/// is exactly why every test that creates one runs against a container this fixture itself tears down at
/// the end of the run, never against a long-lived developer database.</para>
///
/// <para><b>Docker is not assumed to be running.</b> <see cref="PostgresCdcFactAttribute"/> skips every test
/// below, with a stated reason, on a machine with no Docker daemon — see <see cref="DockerGate"/>. In THIS
/// environment (no Docker), every test in this file is expected to SKIP; the assertions below have not been
/// exercised against a live server here and this class doc says so rather than implying otherwise.</para>
/// </summary>
[Collection(CdcServers.CollectionName)]
public sealed class PostgresCdcTests(CdcServers servers)
{
    private static readonly DbBackend Backend = CdcDbBackends.Postgres;

    /// <summary>Held so xunit constructs the container fixture before any test in this class runs.</summary>
    private readonly CdcServers _servers = servers;

    private static PgCdcSource NewSource() => new(Backend.Dialect);

    // ---- 1. seed cycle: null cursor -> zero rows, a confirmed cursor ----

    [PostgresCdcFact]
    public async Task SeedCycleYieldsNoRowsAndAConfirmedCursor()
    {
        var table = await CreateOrdersTableAsync().ConfigureAwait(false);
        var (slot, publication) = await NewSlotAndPublicationAsync(table).ConfigureAwait(false);
        var def = Definition(table, slot, publication);

        var seed = await NewSource().PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);

        Assert.Empty(seed.Rows);
        Assert.NotNull(seed.Cursor);
        Assert.False(seed.HasMore);

        // The cursor really is a Postgres LSN, not an opaque placeholder — CdcLsn.DecodePg throws on
        // anything that isn't one.
        CdcLsn.DecodePg(seed.Cursor!);
    }

    // ---- 2. insert / update / delete: the right op, weight, table and values ----

    [PostgresCdcFact]
    public async Task InsertUpdateDeleteArriveWithTheRightOpWeightAndValues()
    {
        var table = await CreateOrdersTableAsync().ConfigureAwait(false);
        var (slot, publication) = await NewSlotAndPublicationAsync(table).ConfigureAwait(false);
        var def = Definition(table, slot, publication);
        var source = NewSource();

        var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);

        await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 1L, "AAA", 10L).ConfigureAwait(false);
        var afterInsert = await source.PollAsync(def, seed.Cursor, CancellationToken.None).ConfigureAwait(false);
        var inserted = Assert.Single(afterInsert.Rows);
        Assert.Equal(CdcStamp.OpCreate, inserted[CdcStamp.OpColumn]);
        Assert.Equal(1, inserted[CdcStamp.WeightColumn]);
        Assert.Equal($"public.{table}", inserted[CdcStamp.TableColumn]);
        Assert.Equal(1L, Id(inserted));
        Assert.Equal("AAA", inserted["symbol"]);
        Assert.Equal(10L, inserted["qty"]);

        await ExecAsync($"UPDATE {Quoted(table)} SET qty = @p0 WHERE id = @p1", 20L, 1L).ConfigureAwait(false);
        var afterUpdate = await source.PollAsync(def, afterInsert.Cursor, CancellationToken.None).ConfigureAwait(false);
        var updated = Assert.Single(afterUpdate.Rows);
        Assert.Equal(CdcStamp.OpUpdate, updated[CdcStamp.OpColumn]);
        Assert.Equal(1, updated[CdcStamp.WeightColumn]);
        Assert.Equal(1L, Id(updated));
        Assert.Equal(20L, updated["qty"]);

        await ExecAsync($"DELETE FROM {Quoted(table)} WHERE id = @p0", 1L).ConfigureAwait(false);
        var afterDelete = await source.PollAsync(def, afterUpdate.Cursor, CancellationToken.None).ConfigureAwait(false);
        var deleted = Assert.Single(afterDelete.Rows);
        Assert.Equal(CdcStamp.OpDelete, deleted[CdcStamp.OpColumn]);
        Assert.Equal(-1, deleted[CdcStamp.WeightColumn]);
        Assert.Equal(1L, Id(deleted));
    }

    // ---- 3. the cursor advances monotonically and never goes backwards ----

    [PostgresCdcFact]
    public async Task CursorAdvancesMonotonicallyAcrossCycles()
    {
        var table = await CreateOrdersTableAsync().ConfigureAwait(false);
        var (slot, publication) = await NewSlotAndPublicationAsync(table).ConfigureAwait(false);
        var def = Definition(table, slot, publication);
        var source = NewSource();

        var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        Assert.NotNull(seed.Cursor);
        var cursor = seed.Cursor;
        var previous = CdcLsn.DecodePg(cursor!);

        for (var i = 1; i <= 3; i++)
        {
            await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", (long)i, "S" + i.ToString(CultureInfo.InvariantCulture), (long)i).ConfigureAwait(false);

            var batch = await source.PollAsync(def, cursor, CancellationToken.None).ConfigureAwait(false);
            Assert.NotNull(batch.Cursor);
            var next = CdcLsn.DecodePg(batch.Cursor!);
            Assert.True(next > previous, $"cursor must advance, never go backwards: {previous} -> {next}");
            previous = next;
            cursor = batch.Cursor;
        }
    }

    // ---- 4. resume with no gap and no duplication: the property this whole feature rests on ----

    [PostgresCdcFact]
    public async Task ResumeFromAPersistedCursorHasNoGapAndNoDuplication()
    {
        var table = await CreateOrdersTableAsync().ConfigureAwait(false);
        var (slot, publication) = await NewSlotAndPublicationAsync(table).ConfigureAwait(false);
        var def = Definition(table, slot, publication);

        string? persisted;
        {
            var source = NewSource();
            var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);

            await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 1L, "BEFORE", 1L).ConfigureAwait(false);

            var before = await source.PollAsync(def, seed.Cursor, CancellationToken.None).ConfigureAwait(false);
            var row = Assert.Single(before.Rows);
            Assert.Equal("BEFORE", row["symbol"]);
            persisted = before.Cursor;
        }

        // `source` above is now unreachable — only the persisted STRING survives, exactly what a silo
        // recycle / actor deactivation leaves the driver holding across a restart.
        await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 2L, "AFTER", 2L).ConfigureAwait(false);

        var resumedSource = NewSource();
        var resumed = await resumedSource.PollAsync(def, persisted, CancellationToken.None).ConfigureAwait(false);

        // The new row arrives, the old one does not reappear, and nothing else is missing: the slot lives
        // server-side, keyed by SlotName in the config, not by which client object confirmed it last.
        var only = Assert.Single(resumed.Rows);
        Assert.Equal("AFTER", only["symbol"]);
        Assert.DoesNotContain(resumed.Rows, r => Equals(r["symbol"], "BEFORE"));
    }

    // ---- 5. a failed cycle does not advance the cursor ----

    [PostgresCdcFact]
    public async Task AFailedCycleDoesNotAdvanceTheCursorAndAGoodPollStillDeliversThePendingRow()
    {
        var table = await CreateOrdersTableAsync().ConfigureAwait(false);
        var (slot, publication) = await NewSlotAndPublicationAsync(table).ConfigureAwait(false);
        var goodDef = Definition(table, slot, publication);
        var source = NewSource();

        var seed = await source.PollAsync(goodDef, null, CancellationToken.None).ConfigureAwait(false);
        var cursor = seed.Cursor;

        await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 1L, "PENDING", 1L).ConfigureAwait(false);

        // Same slot/publication, pointed at a port nothing on this host is listening on.
        var badConfig = Backend.SourceConfig(table, c =>
        {
            c.CursorColumn = "";
            c.Port = 1;
            c.SlotName = slot;
            c.PublicationName = publication;
        });
        var badDef = Backend.Definition(badConfig);

        DedupTracker dedup = new();
        var outcome = await PolledSourceCore.RunCycleAsync(
            source, badDef, cursor, dedup, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), CancellationToken.None).ConfigureAwait(false);

        Assert.NotNull(outcome.Result.Error);
        // The load-bearing assertion: PolledSourceCore hands back the SAME cursor it was given on a failed
        // cycle, per its own "a failed cycle keeps the old cursor" rule.
        Assert.Equal(cursor, outcome.Cursor);

        // A subsequent GOOD poll from that same (untouched) cursor still delivers the row that was pending.
        var recovered = await source.PollAsync(goodDef, cursor, CancellationToken.None).ConfigureAwait(false);
        var row = Assert.Single(recovered.Rows);
        Assert.Equal("PENDING", row["symbol"]);
    }

    // ---- 6. a multi-row transaction arrives whole, never split across two batches ----

    [PostgresCdcFact]
    public async Task AMultiRowTransactionArrivesWholeNeverSplitAcrossBatches()
    {
        var table = await CreateOrdersTableAsync().ConfigureAwait(false);
        var (slot, publication) = await NewSlotAndPublicationAsync(table).ConfigureAwait(false);
        // A small BatchSize so the cap COULD, in principle, cut the transaction mid-way — and the class
        // doc's rule is that it never does: a batch only ends on a transaction's own COMMIT.
        var def = Definition(table, slot, publication, c => c.BatchSize = 2);
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

        var batch = await source.PollAsync(def, seed.Cursor, CancellationToken.None).ConfigureAwait(false);
        Assert.Equal(5, batch.Rows.Count);
        Assert.Equal(Enumerable.Range(1, 5).Select(i => (long)i), batch.Rows.Select(Id).Order());

        // Nothing left over: the whole transaction arrived in that ONE call, not split across two.
        var next = await source.PollAsync(def, batch.Cursor, CancellationToken.None).ConfigureAwait(false);
        Assert.Empty(next.Rows);
    }

    // ---- 7. Postgres-specific: REPLICA IDENTITY controls what a DELETE carries ----

    [PostgresCdcFact]
    public async Task ReplicaIdentityControlsWhatADeleteCarries()
    {
        var table = await CreateOrdersTableAsync().ConfigureAwait(false);
        var (slot, publication) = await NewSlotAndPublicationAsync(table).ConfigureAwait(false);
        var def = Definition(table, slot, publication);
        var source = NewSource();

        var seed = await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);
        var cursor = seed.Cursor;

        // DEFAULT replica identity (backed by the primary key): a delete carries KEY COLUMNS ONLY.
        await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 1L, "DEFAULT_IDENTITY", 1L).ConfigureAwait(false);
        var afterInsert1 = await source.PollAsync(def, cursor, CancellationToken.None).ConfigureAwait(false);
        cursor = afterInsert1.Cursor;

        await ExecAsync($"DELETE FROM {Quoted(table)} WHERE id = @p0", 1L).ConfigureAwait(false);
        var defaultDelete = await source.PollAsync(def, cursor, CancellationToken.None).ConfigureAwait(false);
        cursor = defaultDelete.Cursor;

        var partial = Assert.Single(defaultDelete.Rows);
        Assert.Equal(CdcStamp.OpDelete, partial[CdcStamp.OpColumn]);
        Assert.Equal(1L, Id(partial));
        Assert.False(partial.ContainsKey("symbol"), "a DEFAULT-replica-identity delete must not carry non-key columns");
        Assert.False(partial.ContainsKey("qty"), "a DEFAULT-replica-identity delete must not carry non-key columns");

        // REPLICA IDENTITY FULL: the whole old row survives onto the delete event.
        await ExecAsync($"ALTER TABLE {Quoted(table)} REPLICA IDENTITY FULL").ConfigureAwait(false);
        await ExecAsync($"INSERT INTO {Quoted(table)} (id, symbol, qty) VALUES (@p0, @p1, @p2)", 2L, "FULL_IDENTITY", 2L).ConfigureAwait(false);
        var afterInsert2 = await source.PollAsync(def, cursor, CancellationToken.None).ConfigureAwait(false);
        cursor = afterInsert2.Cursor;

        await ExecAsync($"DELETE FROM {Quoted(table)} WHERE id = @p0", 2L).ConfigureAwait(false);
        var fullDelete = await source.PollAsync(def, cursor, CancellationToken.None).ConfigureAwait(false);

        var full = Assert.Single(fullDelete.Rows);
        Assert.Equal(CdcStamp.OpDelete, full[CdcStamp.OpColumn]);
        Assert.Equal(2L, Id(full));
        Assert.Equal("FULL_IDENTITY", full["symbol"]);
        Assert.Equal(2L, full["qty"]);
    }

    // ---- 9. the probe: metadata fields, WAL lag and replica identity diagnostics ----

    [PostgresCdcFact]
    public async Task ProbeReportsCdcMetadataFieldsAndWalLagAndReplicaIdentity()
    {
        var table = await CreateOrdersTableAsync().ConfigureAwait(false);
        var (slot, publication) = await NewSlotAndPublicationAsync(table).ConfigureAwait(false);
        var def = Definition(table, slot, publication);
        var source = NewSource();

        // The probe reads catalog state only, but this connector's own slot is created lazily, on the
        // first PollAsync cycle (CreateSlotIfMissing) — run one seed cycle first so the slot exists for the
        // probe to describe; a probe against a not-yet-created slot is exercised by the unit tests instead.
        await source.PollAsync(def, null, CancellationToken.None).ConfigureAwait(false);

        var probe = (ISchemaProbe)source;
        var result = await probe.ProbeAsync(def, CancellationToken.None).ConfigureAwait(false);

        var names = result.Fields.Select(f => f.Name).ToHashSet(StringComparer.Ordinal);
        Assert.Contains(CdcStamp.OpColumn, names);
        Assert.Contains(CdcStamp.WeightColumn, names);
        Assert.Contains(CdcStamp.TsColumn, names);
        Assert.Contains(CdcStamp.TableColumn, names);
        // The table's own columns are inferred too, alongside the CDC metadata fields.
        Assert.Contains("id", names);
        Assert.Contains("symbol", names);

        Assert.Contains(result.Diagnostics, d => d.Contains("behind the current WAL position", StringComparison.Ordinal));
        Assert.Contains(result.Diagnostics, d => d.Contains("REPLICA IDENTITY DEFAULT", StringComparison.Ordinal));
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

    private static async Task<string> CreateOrdersTableAsync()
    {
        var table = Backend.NewTable("orders");
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await Sql.ExecAsync(
            connection,
            $"CREATE TABLE {Quoted(table)} (id bigint NOT NULL PRIMARY KEY, symbol text NOT NULL, qty bigint NOT NULL)").ConfigureAwait(false);
        return table;
    }

    /// <summary>A fresh publication covering exactly <paramref name="table"/>, and a unique slot NAME for
    /// the test to hand the source — the slot itself is created lazily by <c>CreateSlotIfMissing</c> on the
    /// source's own first cycle (see <see cref="Definition"/>), matching how an operator actually configures
    /// this kind rather than pre-creating the slot out-of-band the way the publication has to be.</summary>
    private static async Task<(string Slot, string Publication)> NewSlotAndPublicationAsync(string table)
    {
        var slot = Backend.NewTable("slot");
        var publication = Backend.NewTable("pub");
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);
        await Sql.ExecAsync(connection, $"CREATE PUBLICATION {Backend.Dialect.QuoteIdent(publication)} FOR TABLE {Quoted(table)}").ConfigureAwait(false);
        return (slot, publication);
    }

    private static SourceDefinition Definition(string table, string slot, string publication, Action<DbSourceConfig>? tweak = null)
    {
        var config = Backend.SourceConfig(table, c =>
        {
            c.CursorColumn = "";
            c.SlotName = slot;
            c.PublicationName = publication;
            c.CreateSlotIfMissing = true;
            c.MaxPollMs = 3000;
        });
        tweak?.Invoke(config);
        return Backend.Definition(config);
    }
}
