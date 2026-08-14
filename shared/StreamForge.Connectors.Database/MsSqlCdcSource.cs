using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Database;

/// <summary>
/// Plan 017 wave C: SQL Server's own CDC capture tables as an <see cref="IPolledTransport"/> — the
/// Microsoft Data namespace equivalent of the Postgres logical-replication reader (<c>PgCdcSource</c>),
/// but pull-shaped rather than streamed, because that is what <c>cdc.fn_cdc_get_all_changes_*</c> already
/// is: a table-valued function over a range of a <c>binary(10)</c> LSN. All the rules that can lose data —
/// where to start, when retention has already discarded the range, how to cut a batch on a transaction
/// boundary — live in <see cref="MsSqlCdcPlanner"/>, which is pure and tested; this class does nothing but
/// run the handful of scalar queries that planner asks for, open one more connection for the main read,
/// and hand the result to <see cref="MsSqlCdcPlanner.Complete"/>.
///
/// <para><b>Delivery is at-least-once, same ceiling as every polled kind.</b> A cycle that fails after the
/// main read but before the caller persists the returned cursor is re-read in full on the next cycle
/// (<c>PolledSourceCore</c>'s rule) — CDC changes nothing about that contract, it only replaces "which
/// rows come back on a re-read" with a real transactional boundary instead of a timestamp's blind spot.</para>
///
/// <para><b>This is genuinely the cheap half of plan 017.</b> SQL Server's CDC is already pull-shaped —
/// scalar LSN functions plus a table-valued read — so there is no subscription to keep alive between
/// cycles, no relation cache, no tuple decoder: everything Postgres logical replication needs and this
/// does not. The entire connector is four small round trips per cycle plus the planner's pure logic.</para>
/// </summary>
public sealed class MsSqlCdcSource(ISqlDialect dialect) : IPolledTransport, ISchemaProbe
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly ISqlDialect _dialect = dialect;

    /// <summary>Fixed at the CDC kind, never <see cref="ISqlDialect.Kind"/> — the dialect underneath is
    /// still SQL Server, but "mssql" is the polled kind's identity, not this one's.</summary>
    public string Kind => SourceKinds.MsSqlCdc;

    public void Validate(SourceDefinition def, List<string> errors)
    {
        ArgumentNullException.ThrowIfNull(def);
        ArgumentNullException.ThrowIfNull(errors);

        var config = def.Connector?.Db;
        if (config is null)
        {
            errors.Add($"kind '{Kind}' requires connector.db");
            return;
        }

        if (!DbEndpoint.From(config).Addressable)
        {
            errors.Add("connector.db needs host + database (or a connectionString)");
        }

        var capture = config.CaptureInstance.Trim();
        if (capture.Length == 0)
        {
            errors.Add("connector.db needs a captureInstance");
        }
        else if (!MsSqlCdcPlanner.IsValidCaptureInstance(capture))
        {
            errors.Add(
                "connector.db.captureInstance must match ^[A-Za-z_][A-Za-z0-9_]*$ — it is interpolated " +
                "into a cdc.fn_cdc_get_all_changes_<capture> function name and can never be a bound parameter");
        }

        if (!string.IsNullOrWhiteSpace(config.CursorColumn))
        {
            errors.Add($"connector.db.cursorColumn belongs to the '{SourceKinds.MsSql}' polled kind, not to CDC — the CDC cursor is the LSN, not a column");
        }

        // CursorKind defaults to "long" on every DbSourceConfig regardless of kind, so only an EXPLICIT
        // change away from that default is a signal the operator meant to configure the polled kind's
        // cursor parsing here — flagging the untouched default would reject every well-formed CDC config.
        if (!string.IsNullOrEmpty(config.CursorKind) && config.CursorKind != CursorKinds.Long)
        {
            errors.Add($"connector.db.cursorKind belongs to the '{SourceKinds.MsSql}' polled kind, not to CDC");
        }

        if (!string.IsNullOrWhiteSpace(config.Query))
        {
            errors.Add($"connector.db.query belongs to the '{SourceKinds.MsSql}' polled kind, not to CDC");
        }

        if (!string.IsNullOrWhiteSpace(config.Where))
        {
            errors.Add($"connector.db.where belongs to the '{SourceKinds.MsSql}' polled kind, not to CDC");
        }

        if (!string.IsNullOrWhiteSpace(config.SlotName))
        {
            errors.Add($"connector.db.slotName belongs to the '{SourceKinds.PostgresCdc}' kind, not to {Kind}");
        }

        if (!string.IsNullOrWhiteSpace(config.PublicationName))
        {
            errors.Add($"connector.db.publicationName belongs to the '{SourceKinds.PostgresCdc}' kind, not to {Kind}");
        }
    }

    public async Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(def);
        var config = def.Connector?.Db
            ?? throw new InvalidOperationException($"source '{def.Name}' is kind '{Kind}' but has no connector.db");

        var capture = config.CaptureInstance.Trim();

        await using var connection = _dialect.CreateConnection(DbEndpoint.From(config));
        await connection.OpenAsync(ct).ConfigureAwait(false);

        var fromStep = MsSqlCdcPlanner.PlanFrom(config, cursor);
        var from = fromStep.Sql is null
            ? fromStep.ResolvedFrom!
            : CdcLsn.EncodeMsSql(await LsnScalarAsync(connection, config, fromStep.Sql, fromStep.Parameters, ct).ConfigureAwait(false));

        if (fromStep.Kind == MsSqlCdcFromKind.Tail)
        {
            // Seed cycle (DbPollPlanner's branch-4 vocabulary): persist the tail, emit nothing, so the
            // next cycle tails from here instead of replaying everything retention still has.
            return new PolledBatch([], from, HasMore: false);
        }

        // Retention check is skipped only for Snapshot, where `from` IS the min by construction — the
        // comparison could only ever come back equal, never a breach.
        if (fromStep.Kind != MsSqlCdcFromKind.Snapshot)
        {
            var min = CdcLsn.EncodeMsSql(await LsnScalarAsync(
                connection, config, MsSqlCdcPlanner.MinLsnSql, [new(MsSqlCdcPlanner.CaptureParameterName, capture)], ct).ConfigureAwait(false));
            MsSqlCdcPlanner.CheckRetention(capture, from, min);
        }

        var to = CdcLsn.EncodeMsSql(await LsnScalarAsync(connection, config, MsSqlCdcPlanner.MaxLsnSql, [], ct).ConfigureAwait(false));

        if (MsSqlCdcPlanner.IsEmptyRange(from, to))
        {
            // Nothing has committed since the last cycle. Null cursor = leave the persisted one alone,
            // never a reset — same convention DbPollPlanner uses for an empty poll.
            return new PolledBatch([], null, HasMore: false);
        }

        var readPlan = MsSqlCdcPlanner.PlanRead(config, from, to);
        var rawRows = await ExecuteReadAsync(connection, readPlan, config.CommandTimeoutSeconds, ct).ConfigureAwait(false);
        var capped = rawRows.Count >= (config.BatchSize > 0 ? config.BatchSize : 1000);

        var result = MsSqlCdcPlanner.Complete(config, rawRows, capped);

        if (result.NeedsReread)
        {
            // The only group in a capped read cannot be proven complete (MsSqlCdcPlanner.Complete's doc).
            // Re-read once, bounded exactly at that transaction's own LSN and with no TOP at all — the
            // range can then only ever contain that one transaction, so the result is complete by
            // construction and this loop body runs at most once per cycle.
            var boundedPlan = MsSqlCdcPlanner.PlanBoundedRead(config, from, result.RereadBoundLsn!);
            var boundedRows = await ExecuteReadAsync(connection, boundedPlan, config.CommandTimeoutSeconds, ct).ConfigureAwait(false);
            result = MsSqlCdcPlanner.Complete(config, boundedRows, capped: false);

            if (result.NeedsReread)
            {
                // Impossible by construction — PlanBoundedRead has no TOP and its `to` is pinned at exactly
                // one LSN, so it cannot itself be truncated. If this ever fires, that contract broke.
                throw new InvalidOperationException(
                    $"CDC bounded re-read for capture instance '{capture}' still reports truncation — this should be impossible with no TOP");
            }
        }

        var batch = result.Batch!;
        foreach (var row in batch.Rows)
        {
            CoerceInPlace(row);
        }

        return batch;
    }

    public async Task<SchemaProbeResult> ProbeAsync(SourceDefinition def, CancellationToken ct) => await CdcPreflight.ProbeMsSqlAsync(def, ct).ConfigureAwait(false);

    public TransportDescriptor Describe() => new()
    {
        Kind = Kind,
        Label = "SQL Server (CDC)",
        Help =
            "Reads SQL Server's own CDC capture tables (cdc.fn_cdc_get_all_changes_<capture instance>) — " +
            "the LSN itself is the cursor. AT-LEAST-ONCE, same as every polled kind: a cycle that fails " +
            "after reading keeps the old cursor and re-reads. CDC must already be enabled on the database " +
            "(sys.sp_cdc_enable_db) and on the table (sys.sp_cdc_enable_table), and — outside Azure SQL " +
            "Database — the SQL Server Agent job that drains the transaction log into the capture tables " +
            "must be running, or nothing ever appears here no matter how healthy this source looks. CDC " +
            "retention defaults to 3 DAYS: a source left stopped longer than that has PERMANENTLY LOST the " +
            "changes retention already discarded, and the next cycle fails loudly instead of silently " +
            "skipping the gap. 'Replay retained history' below means replaying whatever the capture table " +
            "STILL RETAINS — it is NOT a full-table snapshot. For a true backfill, run the 'mssql' polled " +
            "kind first to load history, then switch this source on to tail new changes from there.",
        ConfigProperty = "db",
        Polled = true,
        Mapping = false,
        CanProbe = true,
        Groups =
        [
            new TransportGroup
            {
                Key = "advanced",
                Label = "Advanced",
                Help = "Escape hatches. connectionString overrides every connection field above it.",
            },
        ],
        Fields =
        [
            new TransportField { Key = "host", Label = "Host", Required = true, Mono = true, Placeholder = "db.internal" },
            new TransportField { Key = "port", Label = "Port", Type = TransportFieldTypes.Number, Placeholder = _dialect.DefaultPort.ToString(CultureInfo.InvariantCulture), Help = "0 uses the default." },
            new TransportField { Key = "database", Label = "Database", Required = true, Mono = true },
            new TransportField { Key = "username", Label = "Username" },
            new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret },
            new TransportField
            {
                Key = "captureInstance", Label = "Capture instance", Required = true, Mono = true, Placeholder = "dbo_Orders",
                Help =
                    "The name sys.sp_cdc_enable_table registered — conventionally <schema>_<table>. Must match " +
                    "^[A-Za-z_][A-Za-z0-9_]*$: it is interpolated into cdc.fn_cdc_get_all_changes_<capture>, which " +
                    "cannot take an identifier as a bound parameter.",
            },
            new TransportField { Key = "schema", Label = "Schema", Mono = true, Placeholder = _dialect.DefaultSchema, Help = "Informational only — used to stamp _table on emitted rows, not to build the read." },
            new TransportField { Key = "table", Label = "Table", Mono = true, Help = "Informational only — stamps _table on emitted rows and is what the Tables filter below compares against." },
            new TransportField { Key = "tables", Label = "Tables filter", Mono = true, Group = "advanced", Help = "CSV of schema.table to keep; empty = everything this capture instance carries. Informational — this source already reads exactly one capture instance." },
            new TransportField { Key = "snapshot", Label = "Replay retained history", Type = TransportFieldTypes.Bool, Help = "Starts from sys.fn_cdc_get_min_lsn — replays whatever the capture table STILL RETAINS. NOT a full-table snapshot; for a true backfill, run the 'mssql' polled kind first, then switch this on." },
            new TransportField { Key = "initialCursor", Label = "Start at LSN", Mono = true, Help = "A 20-character lowercase-hex LSN to start from when nothing is persisted yet. Empty starts at the tail (new changes only)." },
            new TransportField { Key = "batchSize", Label = "Rows per poll", Type = TransportFieldTypes.Number, Default = "1000" },
            new TransportField { Key = "commandTimeoutSeconds", Label = "Command timeout (s)", Type = TransportFieldTypes.Number, Default = "30", Group = "advanced" },
            new TransportField { Key = "tls", Label = "Require TLS", Type = TransportFieldTypes.Bool, Group = "advanced" },
            new TransportField
            {
                Key = "connectionString", Label = "Connection string", Type = TransportFieldTypes.Secret, Group = "advanced",
                Help = "Overrides host/port/database/username/password/TLS entirely. Masked wholesale, so the host stops being visible in the console — that is the cost of the escape hatch.",
            },
        ],
    };

    /// <summary>Runs one <see cref="MsSqlCdcReadPlan"/> (the normal <c>TOP</c>-capped read or the bounded
    /// re-read) and returns the raw rows. Shared so the two call sites in <see cref="PollAsync"/> cannot
    /// drift on timeout handling or column reading.</summary>
    private static async Task<List<Dictionary<string, object?>>> ExecuteReadAsync(DbConnection connection, MsSqlCdcReadPlan plan, int commandTimeoutSeconds, CancellationToken ct)
    {
        await using var command = Command(connection, plan.Sql, commandTimeoutSeconds, plan.Parameters);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        return await ReadRawAsync(reader, ct).ConfigureAwait(false);
    }

    private static async Task<byte[]> LsnScalarAsync(DbConnection connection, DbSourceConfig config, string sql, IReadOnlyList<KeyValuePair<string, object?>> parameters, CancellationToken ct)
    {
        await using var command = Command(connection, sql, config.CommandTimeoutSeconds, parameters);
        var scalar = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        if (scalar is not byte[] bytes)
        {
            // NULL back from fn_cdc_get_min_lsn/fn_cdc_get_max_lsn/fn_cdc_increment_lsn means the capture
            // instance does not exist or CDC was never enabled on it — a config problem, not a transient
            // one, so it is named here rather than surfacing as a downstream cast exception.
            throw new InvalidOperationException(
                $"'{sql}' returned no LSN — capture instance '{config.CaptureInstance}' may not exist, or CDC is not enabled on this database/table");
        }

        return bytes;
    }

    private static DbCommand Command(DbConnection connection, string sql, int timeoutSeconds, IReadOnlyList<KeyValuePair<string, object?>> parameters)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = timeoutSeconds > 0 ? timeoutSeconds : 30;
        foreach (var (name, value) in parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }

        return command;
    }

    private static async Task<List<Dictionary<string, object?>>> ReadRawAsync(DbDataReader reader, CancellationToken ct)
    {
        // Deliberately NOT coerced here (unlike DbSource.ReadAsync): MsSqlCdcPlanner.Complete needs the raw
        // byte[] LSN and the raw DateTime timestamp to do the transaction cut and stamp _ts, so coercion of
        // the remaining business columns happens afterward, in CoerceInPlace.
        List<Dictionary<string, object?>> rows = [];
        var names = new string[reader.FieldCount];
        for (var i = 0; i < names.Length; i++)
        {
            names[i] = reader.GetName(i);
        }

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var row = new Dictionary<string, object?>(names.Length, StringComparer.Ordinal);
            for (var i = 0; i < names.Length; i++)
            {
                row[names[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    /// <summary>Coerces every value in <paramref name="row"/> to what the platform's field types can hold —
    /// the same conversion <c>DbSource.Cell</c> applies, run here rather than during <see cref="ReadRawAsync"/>
    /// because <see cref="MsSqlCdcPlanner.Complete"/> needs the untouched raw types first. Applied to the
    /// WHOLE row, including the columns <see cref="CdcStamp.Apply"/> just added — safe, since those are
    /// already plain string/int/long values Cell passes through unchanged.</summary>
    private static void CoerceInPlace(Dictionary<string, object?> row)
    {
        foreach (var key in row.Keys.ToList())
        {
            row[key] = Cell(row[key]);
        }
    }

    private static object? Cell(object? raw) => raw switch
    {
        null or DBNull => null,
        string or bool or byte or sbyte or short or ushort or int or uint or long or ulong => raw,
        float or double or decimal => raw,
        DateTime or DateTimeOffset => raw,
        byte[] bytes => Convert.ToBase64String(bytes),
        Guid guid => guid.ToString(),
        DateOnly date => date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        TimeOnly time => time.ToString("O", CultureInfo.InvariantCulture),
        TimeSpan span => span.ToString("c", CultureInfo.InvariantCulture),
        char c => c.ToString(),
        _ => JsonSerializer.Serialize(raw, raw.GetType(), JsonOptions),
    };
}
