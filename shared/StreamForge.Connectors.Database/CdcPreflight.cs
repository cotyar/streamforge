using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Npgsql;
using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Database;

/// <summary>
/// Plan 017: the <see cref="ISchemaProbe"/> half of the two native CDC source kinds, in ONE place so
/// <c>PgCdcSource</c> and <c>MsSqlCdcSource</c> delegate rather than each growing a probe of its own.
///
/// <para><b>Why the probe carries most of this feature's weight.</b> For the polled kinds a probe is a
/// convenience — it fills the field list the operator would otherwise type. For a CDC kind it is the only
/// place the platform can tell an operator that the source database is about to hurt them: a replication
/// slot nobody drains pins WAL until the SOURCE database's disk fills, and a capture-table cursor that
/// falls behind CDC retention has already lost data by the time the next poll notices. Neither is
/// visible from StreamForge's side of the connection, so the probe asks and reports through
/// <see cref="SchemaProbeResult.Diagnostics"/> — which is not an error channel, but the "what a
/// SUCCESSFUL probe still wants you to know" channel.</para>
///
/// <para><b>Failure discipline.</b> Cannot connect, denied, no such database → THROW (wrapped so the
/// message always names the host — a driver's own connect-refused message does not reliably do that on
/// every platform, and an operator staring at a stack trace needs to know which endpoint failed without
/// reading it). A catalog query that is denied on an otherwise-good connection (the realistic case is
/// <c>msdb</c> on SQL Server, locked down on many managed instances) becomes a diagnostic saying what
/// could not be checked — never a thrown probe, and never silently skipped, because "I could not verify
/// this" and "this is fine" must never render the same way to an operator.</para>
///
/// <para>Orchestrator-placed seam (wave B/C/D compile against these signatures while wave E fills the
/// bodies).</para>
/// </summary>
public static class CdcPreflight
{
    private static readonly PostgresDialect PgDialect = new();
    private static readonly SqlServerDialect MsDialect = new();

    /// <summary>Postgres <c>relreplident</c> and MSSQL capture-instance identifiers both come from
    /// catalog metadata, but <see cref="ValidateCaptureInstance"/> defends the one that a caller could
    /// otherwise be tempted to splice into SQL text instead of binding — see its own doc comment.</summary>
    private static readonly Regex CaptureInstancePattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled, TimeSpan.FromSeconds(1));

    /// <summary>Wave E: <c>wal_level</c>, slot existence/activity/<c>confirmed_flush_lsn</c>, WAL lag
    /// against <c>max_slot_wal_keep_size</c>, publication coverage, per-table <c>relreplident</c>, and the
    /// column list for field inference.
    ///
    /// <para>Runs over an ORDINARY connection — no replication protocol connection is needed to read
    /// catalog views. Every catalog value gathered here is turned into a full sentence a human can act on
    /// (what, measured value, fix), because a preflight that only reports codes is not a preflight an
    /// operator can use under time pressure.</para></summary>
    public static async Task<SchemaProbeResult> ProbePostgresAsync(SourceDefinition def, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(def);
        var config = def.Connector?.Db
            ?? throw new InvalidOperationException($"source '{def.Name}' is kind '{def.Kind}' but has no connector.db");

        var slot = (config.SlotName ?? "").Trim();
        var publication = (config.PublicationName ?? "").Trim();
        List<string> diagnostics = [];
        List<FieldDef> fields = [];

        await using var connection = await OpenAsync(PgDialect, config, ct).ConfigureAwait(false);

        // 1. wal_level — nothing below matters if this is not "logical".
        var walLevel = Convert.ToString(await ExecuteScalarAsync(connection, "SHOW wal_level", ct).ConfigureAwait(false), CultureInfo.InvariantCulture);
        if (!string.Equals(walLevel?.Trim(), "logical", StringComparison.OrdinalIgnoreCase))
        {
            diagnostics.Add(
                $"wal_level is '{walLevel}', not 'logical' — logical replication (and therefore this CDC source) cannot " +
                "work at all until it is. Set wal_level = logical in postgresql.conf and RESTART the server; this " +
                "setting is not reloadable.");
        }

        // 2 + 3. Replication slot existence/activity and WAL lag — the single most valuable line this
        // probe emits, because a slot nobody drains pins WAL until the SOURCE database's disk fills.
        if (slot.Length == 0)
        {
            diagnostics.Add("connector.db.slotName is empty — postgres-cdc needs a replication slot to read from.");
        }
        else
        {
            try
            {
                // max_slot_wal_keep_size is folded into THIS query via current_setting(...) — which returns
                // the same text SHOW does — rather than issued as a second command. Npgsql has no MARS: a
                // second command on the same connection while this reader is still open throws
                // NpgsqlOperationInProgressException, and it throws on almost every real slot, because almost
                // every real slot has non-zero lag. One round trip, one reader, no overlap.
                await using var reader = await Command(connection, SlotAndWalLagSql, [new("@slot", slot)]).ExecuteReaderAsync(ct).ConfigureAwait(false);

                if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    diagnostics.Add(
                        $"replication slot '{slot}' does not exist. Create it with: " +
                        $"SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput'); " +
                        "creating it BEGINS pinning WAL on this database immediately — only do this once something is " +
                        "ready to drain the slot.");
                }
                else
                {
                    if (!reader.GetBoolean(0))
                    {
                        diagnostics.Add($"replication slot '{slot}' exists but is not currently active (nothing is reading from it). It is still pinning WAL while idle.");
                    }

                    var restartLsn = reader.IsDBNull(1) ? null : reader.GetString(1);
                    var currentLsn = reader.IsDBNull(3) ? null : reader.GetString(3);
                    if (!reader.IsDBNull(4))
                    {
                        var maxKeepSize = reader.IsDBNull(5) ? "" : reader.GetString(5);
                        diagnostics.Add(WalLagDiagnostic(slot, reader.GetInt64(4), restartLsn, currentLsn, maxKeepSize));
                    }
                }
            }
            catch (PostgresException ex) when (string.Equals(ex.SqlState, "42501", StringComparison.Ordinal))
            {
                diagnostics.Add(
                    $"could not verify replication slot '{slot}' (permission denied reading pg_replication_slots or calling " +
                    $"pg_current_wal_lsn): grant the pg_monitor role to '{config.Username}', or connect as a superuser, to see slot status and WAL lag.");
            }
        }

        // 4. Publication coverage — a table not in the publication produces silence, not an error.
        var configuredTables = ParseTables(config.Tables, config.Schema, config.Table);
        List<(string Schema, string Table)> publicationTables = [];
        if (publication.Length == 0)
        {
            diagnostics.Add("connector.db.publicationName is empty — postgres-cdc needs a publication to stream from.");
        }
        else
        {
            bool publicationExists;
            await using (var pubReader = await Command(connection, "SELECT 1 FROM pg_publication WHERE pubname = @pub", [new("@pub", publication)])
                             .ExecuteReaderAsync(ct).ConfigureAwait(false))
            {
                publicationExists = await pubReader.ReadAsync(ct).ConfigureAwait(false);
            }

            if (!publicationExists)
            {
                diagnostics.Add(
                    $"publication '{publication}' does not exist. Create it with: CREATE PUBLICATION {publication} " +
                    "FOR TABLE <schema>.<table>, ...; (or FOR ALL TABLES).");
            }
            else
            {
                // pg_publication_tables expands FOR ALL TABLES publications too, so this one query is the
                // definitive coverage list regardless of puballtables.
                await using var tablesReader = await Command(connection, "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = @pub", [new("@pub", publication)])
                    .ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await tablesReader.ReadAsync(ct).ConfigureAwait(false))
                {
                    publicationTables.Add((tablesReader.GetString(0), tablesReader.GetString(1)));
                }

                if (configuredTables.Count > 0)
                {
                    foreach (var t in configuredTables.Where(t => !publicationTables.Contains(t)))
                    {
                        diagnostics.Add(
                            $"table '{t.Schema}.{t.Table}' is configured but is NOT in publication '{publication}' — " +
                            "changes to it will never arrive, with no error anywhere to say so. Add it with: " +
                            $"ALTER PUBLICATION {publication} ADD TABLE {t.Schema}.{t.Table};");
                    }
                }
                else if (publicationTables.Count == 0)
                {
                    diagnostics.Add($"publication '{publication}' exists but covers no tables.");
                }
                else
                {
                    diagnostics.Add(
                        $"no table/tables configured; publication '{publication}' covers: " +
                        string.Join(", ", publicationTables.Take(20).Select(x => $"{x.Schema}.{x.Table}")) +
                        (publicationTables.Count > 20 ? $", and {publicationTables.Count - 20} more" : "") + ".");
                }
            }
        }

        // 5. Per covered table, relreplident — what a DELETE (and, for 'n', an UPDATE too) can carry.
        var covered = configuredTables.Count > 0 ? configuredTables : publicationTables;
        foreach (var t in covered)
        {
            var raw = await ExecuteScalarAsync(
                connection,
                "SELECT c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = @s AND c.relname = @t",
                ct,
                [new("@s", t.Schema), new("@t", t.Table)]).ConfigureAwait(false);

            if (raw is null)
            {
                diagnostics.Add($"table '{t.Schema}.{t.Table}' was not found in pg_class — check the schema/table name.");
                continue;
            }

            var code = Convert.ToString(raw, CultureInfo.InvariantCulture);
            if (ReplicaIdentityDiagnostic($"{t.Schema}.{t.Table}", string.IsNullOrEmpty(code) ? ' ' : code[0]) is { } message)
            {
                diagnostics.Add(message);
            }
        }

        // 6 + 7. Fields, inferred from the configured table or the first table the publication covers.
        var fieldTable = configuredTables.Count > 0 ? configuredTables[0]
            : publicationTables.Count > 0 ? publicationTables[0]
            : ((string Schema, string Table)?)null;

        if (fieldTable is { } ft)
        {
            var sawToastable = false;
            await using var colReader = await Command(
                connection,
                "SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema = @s AND table_name = @t ORDER BY ordinal_position",
                [new("@s", ft.Schema), new("@t", ft.Table)]).ExecuteReaderAsync(ct).ConfigureAwait(false);

            while (await colReader.ReadAsync(ct).ConfigureAwait(false))
            {
                var name = colReader.GetString(0);
                var typeName = PgArrayAwareTypeName(colReader.GetString(1), colReader.GetString(2));

                var mapped = PgDialect.MapType(typeName, null);
                fields.Add(new FieldDef(name, mapped.Type));
                if (mapped.Note is not null)
                {
                    diagnostics.Add($"{name}: {mapped.Note}");
                }

                sawToastable |= IsToastable(typeName);
            }

            if (fields.Count == 0)
            {
                diagnostics.Add($"table '{ft.Schema}.{ft.Table}' has no columns in information_schema — check the schema/table name.");
            }

            if (sawToastable)
            {
                diagnostics.Add(
                    "one or more inferred columns can be TOASTed: if a row is updated without changing that column, " +
                    "pgoutput sends the sentinel string '__debezium_unavailable_value' instead of its real content, " +
                    "unless REPLICA IDENTITY FULL is set on the table — treat that literal string as \"unknown\", never as real data.");
            }
        }
        else
        {
            diagnostics.Add("no table is configured and the publication covers none — the field list could not be inferred; set connector.db.table or connector.db.tables.");
        }

        fields.AddRange(CdcMetadataFields());
        return new SchemaProbeResult(fields, diagnostics);
    }

    /// <summary>Wave E: <c>is_cdc_enabled</c>, the <c>cdc.change_tables</c> row for the capture instance,
    /// <c>min_lsn</c>/<c>max_lsn</c>, retention, whether a SQL Agent job exists, and the captured-column
    /// list for field inference.
    ///
    /// <para><b>Azure SQL Database has no Agent by design</b> — CDC runs on an internal scheduler there,
    /// so a missing capture/cleanup job is reported as information, never as a failure; getting this wrong
    /// would tell every Azure user their working setup is broken.</para></summary>
    public static async Task<SchemaProbeResult> ProbeMsSqlAsync(SourceDefinition def, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(def);
        var config = def.Connector?.Db
            ?? throw new InvalidOperationException($"source '{def.Name}' is kind '{def.Kind}' but has no connector.db");

        var captureInstance = (config.CaptureInstance ?? "").Trim();
        if (captureInstance.Length > 0)
        {
            // Reaches sys.fn_cdc_get_all_changes_<capture_instance> as a NAME, not a parameter, wherever the
            // CDC reader itself queries it — a parameter is impossible there, so the identifier is validated
            // once here, at the point this connector first learns it, and refused rather than trusted.
            ValidateCaptureInstance(captureInstance);
        }

        List<string> diagnostics = [];
        List<FieldDef> fields = [];

        await using var connection = await OpenAsync(MsDialect, config, ct).ConfigureAwait(false);

        // 1. is_cdc_enabled on the current database.
        var enabledRaw = await ExecuteScalarAsync(connection, "SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()", ct).ConfigureAwait(false);
        var enabled = enabledRaw is bool b ? b : Convert.ToBoolean(enabledRaw, CultureInfo.InvariantCulture);
        if (!enabled)
        {
            diagnostics.Add($"CDC is not enabled on database '{config.Database}' — run EXEC sys.sp_cdc_enable_db; while connected to it (needs sysadmin or db_owner).");
        }

        string? sourceSchema = null;
        string? sourceTable = null;
        int? sourceObjectId = null;

        // 2. The cdc.change_tables row for this capture instance.
        if (captureInstance.Length == 0)
        {
            diagnostics.Add("connector.db.captureInstance is empty — mssql-cdc needs a capture instance to read from.");
        }
        else
        {
            await using (var reader = await Command(
                connection,
                "SELECT s.name, t.name, ct.source_object_id FROM cdc.change_tables ct " +
                "JOIN sys.tables t ON t.object_id = ct.source_object_id JOIN sys.schemas s ON s.schema_id = t.schema_id " +
                "WHERE ct.capture_instance = @instance",
                [new("@instance", captureInstance)]).ExecuteReaderAsync(ct).ConfigureAwait(false))
            {
                if (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    sourceSchema = reader.GetString(0);
                    sourceTable = reader.GetString(1);
                    sourceObjectId = reader.GetInt32(2);
                }
            }

            if (sourceObjectId is null)
            {
                List<string> known = [];
                await using (var reader = await Command(connection, "SELECT capture_instance FROM cdc.change_tables ORDER BY capture_instance")
                                 .ExecuteReaderAsync(ct).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        known.Add(reader.GetString(0));
                    }
                }

                diagnostics.Add(known.Count == 0
                    ? $"capture instance '{captureInstance}' does not exist, and cdc.change_tables has NO capture instances at all — CDC has not been enabled on any table in this database."
                    : $"capture instance '{captureInstance}' does not exist. Capture instances that DO exist: {string.Join(", ", known)}.");
            }
            else
            {
                // 3. min/max LSN, and how far back capture actually reaches in wall-clock terms.
                var minLsnBytes = await ExecuteScalarAsync(connection, "SELECT sys.fn_cdc_get_min_lsn(@instance)", ct, [new("@instance", captureInstance)]).ConfigureAwait(false) as byte[];
                var maxLsnBytes = await ExecuteScalarAsync(connection, "SELECT sys.fn_cdc_get_max_lsn()", ct).ConfigureAwait(false) as byte[];

                if (minLsnBytes is null)
                {
                    diagnostics.Add($"capture instance '{captureInstance}' has no changes captured yet (min_lsn is NULL) — it was either just created or the capture job has not run yet.");
                }
                else
                {
                    var minTimeRaw = await ExecuteScalarAsync(connection, "SELECT sys.fn_cdc_map_lsn_to_time(@lsn)", ct, [new("@lsn", minLsnBytes)]).ConfigureAwait(false);
                    if (minTimeRaw is DateTime minTime)
                    {
                        diagnostics.Add(
                            $"capture instance '{captureInstance}' retains changes back to {minTime:yyyy-MM-dd HH:mm:ss} UTC " +
                            $"(about {FormatElapsed(DateTime.UtcNow - minTime)} ago) — that is the actual retention window in " +
                            "the only units anyone reasons in; anything a consumer needed older than that is already gone.");
                    }

                    if (maxLsnBytes is not null && CdcLsn.CompareMsSql(CdcLsn.EncodeMsSql(minLsnBytes), CdcLsn.EncodeMsSql(maxLsnBytes)) > 0)
                    {
                        diagnostics.Add($"capture instance '{captureInstance}' reports min_lsn AFTER max_lsn — this should not happen; check for a very recently re-enabled capture instance.");
                    }
                }

                // 4. Retention and cleanup from msdb — a successful connection does not guarantee msdb access.
                try
                {
                    await using var reader = await Command(connection, "SELECT [retention], [threshold] FROM msdb.dbo.cdc_jobs WHERE job_type = 'cleanup' AND database_id = DB_ID()")
                        .ExecuteReaderAsync(ct).ConfigureAwait(false);
                    if (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        var retentionMinutes = Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture);
                        var threshold = Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
                        diagnostics.Add(
                            $"CDC cleanup job retention is {retentionMinutes} minutes ({FormatElapsed(TimeSpan.FromMinutes(retentionMinutes))}), " +
                            $"cleaning up in batches of {threshold} rows.");
                    }
                    else
                    {
                        diagnostics.Add("no CDC cleanup job row was found in msdb.dbo.cdc_jobs for this database.");
                    }
                }
                catch (SqlException ex)
                {
                    diagnostics.Add(
                        $"could not read msdb.dbo.cdc_jobs to verify CDC retention/cleanup settings (permission denied, or msdb is unreachable): {ex.Message}. " +
                        "This is unverified, not confirmed fine — check it manually or grant access to msdb.");
                }

                // 5. SQL Server Agent capture job — absent-by-design on Azure SQL Database.
                var engineEdition = Convert.ToInt32(await ExecuteScalarAsync(connection, "SELECT CAST(SERVERPROPERTY('EngineEdition') AS int)", ct).ConfigureAwait(false), CultureInfo.InvariantCulture);
                if (engineEdition == 5)
                {
                    diagnostics.Add("this is Azure SQL Database (EngineEdition 5): CDC runs on an internal scheduler here, there is NO SQL Server Agent, and a missing Agent job is normal — not a problem.");
                }
                else
                {
                    try
                    {
                        var captureJobCount = await ExecuteScalarAsync(connection, "SELECT COUNT(*) FROM msdb.dbo.cdc_jobs WHERE job_type = 'capture' AND database_id = DB_ID()", ct).ConfigureAwait(false);
                        if (Convert.ToInt64(captureJobCount, CultureInfo.InvariantCulture) == 0)
                        {
                            diagnostics.Add("no SQL Server Agent CDC capture job was found for this database — run EXEC sys.sp_cdc_add_job 'capture'; or captured log records will never be turned into change rows.");
                        }
                    }
                    catch (SqlException ex)
                    {
                        diagnostics.Add($"could not verify whether a SQL Server Agent capture job exists (permission denied, or msdb is unreachable): {ex.Message}. This is unverified, not confirmed fine.");
                    }
                }

                // 6. Fields — cdc.captured_columns is authoritative: a column added to the source table
                // after CDC was enabled on it is NOT here, silently, which is worth its own diagnostic.
                HashSet<string> capturedNames = new(StringComparer.OrdinalIgnoreCase);
                await using (var reader = await Command(
                    connection,
                    "SELECT cc.column_name, cc.column_type FROM cdc.captured_columns cc " +
                    "JOIN cdc.change_tables ct ON ct.object_id = cc.object_id " +
                    "WHERE ct.capture_instance = @instance ORDER BY cc.column_ordinal",
                    [new("@instance", captureInstance)]).ExecuteReaderAsync(ct).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        var name = reader.GetString(0);
                        var typeName = reader.GetString(1);
                        capturedNames.Add(name);

                        var mapped = MsDialect.MapType(typeName, null);
                        fields.Add(new FieldDef(name, mapped.Type));
                        if (mapped.Note is not null)
                        {
                            diagnostics.Add($"{name}: {mapped.Note}");
                        }
                    }
                }

                if (fields.Count == 0)
                {
                    diagnostics.Add($"capture instance '{captureInstance}' has no rows in cdc.captured_columns — nothing will be captured.");
                }

                if (sourceObjectId is { } objectId)
                {
                    List<string> missing = [];
                    await using var reader = await Command(connection, "SELECT name FROM sys.columns WHERE object_id = @objectId ORDER BY column_id", [new("@objectId", objectId)])
                        .ExecuteReaderAsync(ct).ConfigureAwait(false);
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        var name = reader.GetString(0);
                        if (!capturedNames.Contains(name))
                        {
                            missing.Add(name);
                        }
                    }

                    if (missing.Count > 0)
                    {
                        diagnostics.Add(
                            $"column(s) {string.Join(", ", missing)} exist on {sourceSchema}.{sourceTable} but are NOT covered by " +
                            $"capture instance '{captureInstance}' — most likely added after CDC was enabled; changes to them will " +
                            "never appear. Re-run sys.sp_cdc_enable_table with a new capture_instance to pick them up.");
                    }
                }
            }
        }

        fields.AddRange(CdcMetadataFields());
        return new SchemaProbeResult(fields, diagnostics);
    }

    // ---- Pure helpers, extracted so this probe has something real to unit-test. ----

    /// <summary>The slot-existence/activity/WAL-lag query, exposed as a constant so a test can pin its
    /// shape without a live connection. <c>max_slot_wal_keep_size</c> is folded in via
    /// <c>current_setting(...)</c> (same text <c>SHOW</c> returns) rather than fetched with a SEPARATE
    /// command — Npgsql has no MARS, and a second command issued while this query's reader is still open
    /// throws <c>NpgsqlOperationInProgressException</c>. That used to be a live SHOW statement here, and it
    /// broke on almost every real slot (any with non-zero lag) — see the plan 017 wave G bug report. The
    /// full regression (an actual thrown exception on the old shape) can only be observed against a real
    /// open connection, which belongs to the Docker-backed Integration suite this file does not own; this
    /// constant is the honest substitute — a test can assert both facts live in ONE query text rather than
    /// two, which is the whole difference between the fixed and broken shapes.</summary>
    public const string SlotAndWalLagSql =
        "SELECT active, restart_lsn::text, confirmed_flush_lsn::text, pg_current_wal_lsn()::text, " +
        "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), current_setting('max_slot_wal_keep_size') " +
        "FROM pg_replication_slots WHERE slot_name = @slot";

    /// <summary>The CDC metadata columns the reader stamps onto every change row, so the console's
    /// inferred field list matches what actually arrives. A fresh list every call — these are handed to a
    /// caller that owns them from here on, and a shared mutable <see cref="FieldDef"/> instance reused
    /// across probes would let one caller's edit bleed into another's.</summary>
    public static List<FieldDef> CdcMetadataFields() =>
    [
        new FieldDef("_op", FieldType.String),
        new FieldDef("_weight", FieldType.Long),
        new FieldDef("_ts", FieldType.Timestamp),
        new FieldDef("_table", FieldType.String),
    ];

    /// <summary>Bytes, human-readable, to two significant units at most (<c>"1.5 GB"</c>) — the shape an
    /// operator can compare against a disk's free space at a glance, which a raw byte count is not.</summary>
    public static string FormatBytes(long bytes)
    {
        string[] units = ["B", "KB", "MB", "GB", "TB"];
        double value = bytes;
        var unit = 0;
        while (value >= 1024 && unit < units.Length - 1)
        {
            value /= 1024;
            unit++;
        }

        return unit == 0 ? $"{bytes} {units[0]}" : $"{value.ToString("F1", CultureInfo.InvariantCulture)} {units[unit]}";
    }

    /// <summary>Composes the single most valuable line this probe emits: how far the slot has fallen
    /// behind, and what the safety valve (<c>max_slot_wal_keep_size</c>) will do about it. <c>-1</c> and
    /// <c>0</c> both mean "no maximum" in PostgreSQL — i.e. the source database's disk is the only limit —
    /// so both are called out identically rather than leaving an operator to look up what <c>0</c> means.</summary>
    public static string WalLagDiagnostic(string slotName, long lagBytes, string? restartLsn, string? currentLsn, string maxSlotWalKeepSizeRaw)
    {
        var keep = (maxSlotWalKeepSizeRaw ?? "").Trim();
        var unbounded = keep is "-1" or "0" or "";
        var safety = unbounded
            ? "max_slot_wal_keep_size is '-1'/'0' (unbounded) — the SOURCE DATABASE'S DISK is the only limit on how far this slot can fall behind; it pins WAL until the disk fills if nothing drains it."
            : $"max_slot_wal_keep_size is '{keep}' — PostgreSQL will drop WAL past that and INVALIDATE this slot if the connector falls further behind than that.";

        var positions = restartLsn is null || currentLsn is null ? "" : $" (restart_lsn {restartLsn}, current WAL position {currentLsn})";

        return $"replication slot '{slotName}' is {FormatBytes(lagBytes)} behind the current WAL position{positions}. {safety}";
    }

    /// <summary>What a Postgres <c>relreplident</c> code means for what a DELETE (and, for <c>n</c>, an
    /// UPDATE too) can carry — <c>null</c> for <c>f</c> (FULL), the one setting that loses nothing.</summary>
    public static string? ReplicaIdentityDiagnostic(string qualifiedTable, char relReplIdent) => relReplIdent switch
    {
        'f' => null,
        'n' => $"table '{qualifiedTable}' has REPLICA IDENTITY NOTHING — updates and deletes cannot be replicated at all (no columns identify the changed row). Fix: ALTER TABLE {qualifiedTable} REPLICA IDENTITY FULL; (or DEFAULT once it has a primary key).",
        'd' => $"table '{qualifiedTable}' uses REPLICA IDENTITY DEFAULT — a DELETE carries only its primary key columns, not the full row. If a downstream consumer needs the whole deleted row: ALTER TABLE {qualifiedTable} REPLICA IDENTITY FULL; (this increases WAL volume per UPDATE).",
        'i' => $"table '{qualifiedTable}' uses REPLICA IDENTITY USING INDEX — a DELETE carries only the indexed columns, not the full row. If a downstream consumer needs the whole deleted row: ALTER TABLE {qualifiedTable} REPLICA IDENTITY FULL; (this increases WAL volume per UPDATE).",
        _ => $"table '{qualifiedTable}' has an unrecognized replica identity code '{relReplIdent}'.",
    };

    /// <summary>Elapsed time, in the coarsest two units that keep it readable (<c>"3d 4h"</c>,
    /// <c>"2h 30m"</c>, <c>"45m"</c>) — the shape CDC retention windows are always discussed in, never as
    /// a raw <see cref="TimeSpan"/> or a row/byte count nobody can compare against "how long ago".</summary>
    public static string FormatElapsed(TimeSpan elapsed)
    {
        if (elapsed < TimeSpan.Zero)
        {
            elapsed = TimeSpan.Zero;
        }

        if (elapsed.TotalDays >= 1)
        {
            return $"{(int)elapsed.TotalDays}d {elapsed.Hours}h";
        }

        if (elapsed.TotalHours >= 1)
        {
            return $"{(int)elapsed.TotalHours}h {elapsed.Minutes}m";
        }

        return $"{(int)elapsed.TotalMinutes}m";
    }

    /// <summary>True for a Postgres type name that can be TOASTed — i.e. one where an unchanged column on
    /// an UPDATE can arrive over pgoutput as the sentinel <c>__debezium_unavailable_value</c> rather than
    /// its real content, unless the table has REPLICA IDENTITY FULL. Fixed-width types (integers, bools,
    /// timestamps, uuid) are never TOASTed and are deliberately absent from this set.</summary>
    public static bool IsToastable(string pgTypeName)
    {
        if (string.IsNullOrWhiteSpace(pgTypeName))
        {
            return false;
        }

        if (pgTypeName.EndsWith("[]", StringComparison.Ordinal))
        {
            return true;
        }

        return ToastableBaseTypes.Contains(pgTypeName);
    }

    private static readonly HashSet<string> ToastableBaseTypes = new(StringComparer.Ordinal)
    {
        "text", "varchar", "character varying", "bpchar", "character", "json", "jsonb", "xml", "bytea", "hstore", "numeric", "decimal",
    };

    /// <summary><c>information_schema.columns</c> reports an array as <c>data_type = 'ARRAY'</c> with the
    /// element type in <c>udt_name</c> prefixed by Postgres's internal <c>_</c> convention (<c>_int4</c>
    /// for <c>integer[]</c>) — neither form is a key <see cref="SqlTypeTables"/> recognizes on its own, so
    /// this rebuilds the <c>elem[]</c> spelling the type table actually keys on.</summary>
    public static string PgArrayAwareTypeName(string dataType, string udtName)
    {
        if (!string.Equals(dataType, "ARRAY", StringComparison.OrdinalIgnoreCase))
        {
            return udtName;
        }

        var elem = udtName.StartsWith("_", StringComparison.Ordinal) ? udtName[1..] : udtName;
        return elem + "[]";
    }

    /// <summary>Parses <see cref="DbSourceConfig.Tables"/> (CSV of <c>schema.table</c>, schema optional and
    /// defaulting to <see cref="ISqlDialect.DefaultSchema"/>) into pairs; falls back to the single
    /// <see cref="DbSourceConfig.Schema"/>/<see cref="DbSourceConfig.Table"/> pair when <c>Tables</c> is
    /// empty; empty list when neither is set (the publication's own coverage is then the only answer).</summary>
    public static List<(string Schema, string Table)> ParseTables(string tablesCsv, string schema, string table)
    {
        List<(string, string)> list = [];
        var defaultSchema = string.IsNullOrWhiteSpace(schema) ? PgDialect.DefaultSchema : schema.Trim();

        if (!string.IsNullOrWhiteSpace(tablesCsv))
        {
            foreach (var entry in tablesCsv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                var dot = entry.IndexOf('.', StringComparison.Ordinal);
                list.Add(dot < 0 ? (defaultSchema, entry) : (entry[..dot], entry[(dot + 1)..]));
            }
        }
        else if (!string.IsNullOrWhiteSpace(table))
        {
            list.Add((defaultSchema, table.Trim()));
        }

        return list;
    }

    /// <summary>Refuses a capture instance whose shape could ever be mistaken for anything other than a
    /// plain identifier — letters, digits and underscores, not starting with a digit. Throws rather than
    /// sanitizing, because a silently-mangled capture instance name would probe the wrong table (or none)
    /// without saying so.</summary>
    public static string ValidateCaptureInstance(string instance)
    {
        if (!CaptureInstancePattern.IsMatch(instance))
        {
            throw new FormatException($"'{instance}' is not a valid CDC capture instance identifier — expected letters, digits and underscores only, not starting with a digit.");
        }

        return instance;
    }

    // ---- Connection/command plumbing — deliberately not shared with DbSource: that class's Command()
    // is private, and duplicating six lines here is cheaper than exporting a seam neither owner asked for. ----

    /// <summary>Opens <paramref name="config"/>'s connection, wrapping any failure so the thrown message
    /// always names the endpoint — a bare driver exception does not reliably do that on every platform,
    /// and an operator reading a stack trace needs to know which host failed without decoding it.</summary>
    private static async Task<DbConnection> OpenAsync(ISqlDialect dialect, DbSourceConfig config, CancellationToken ct)
    {
        var endpoint = DbEndpoint.From(config);
        var connection = dialect.CreateConnection(endpoint);
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            return connection;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            var target = string.IsNullOrWhiteSpace(endpoint.ConnectionString)
                ? $"{(string.IsNullOrWhiteSpace(config.Host) ? "(no host)" : config.Host)}:{(config.Port > 0 ? config.Port : dialect.DefaultPort)}"
                : "(connection string)";
            throw new InvalidOperationException($"could not connect to {dialect.Label} at {target} (database '{config.Database}'): {ex.Message}", ex);
        }
    }

    private static DbCommand Command(DbConnection connection, string sql, IReadOnlyList<KeyValuePair<string, object?>>? parameters = null)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 30;
        if (parameters is not null)
        {
            foreach (var (name, value) in parameters)
            {
                var parameter = command.CreateParameter();
                parameter.ParameterName = name;
                parameter.Value = value ?? DBNull.Value;
                command.Parameters.Add(parameter);
            }
        }

        return command;
    }

    private static async Task<object?> ExecuteScalarAsync(DbConnection connection, string sql, CancellationToken ct, IReadOnlyList<KeyValuePair<string, object?>>? parameters = null)
    {
        await using var command = Command(connection, sql, parameters);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is DBNull ? null : result;
    }
}
