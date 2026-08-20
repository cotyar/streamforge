using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Database;

/// <summary>
/// A PostgreSQL or SQL Server table (or query) as an <see cref="IPolledTransport"/>, parameterized by
/// <see cref="ISqlDialect"/> — one class, two registered kinds. All the rules that can lose data live in
/// <see cref="DbPollPlanner"/> and <see cref="DbCursor"/>, which are pure and tested; what is left here is
/// open a connection, run one statement, read the rows.
///
/// <para><b>Delivery is at-least-once, with holes on a timestamp cursor.</b> A cycle that fails after
/// reading but before emitting keeps the old cursor and re-reads (<c>PolledSourceCore</c>'s rule), so rows
/// can repeat — <see cref="DbSourceConfig.DedupKeyColumn"/> is how an operator collapses that. And no
/// polled source on a timestamp column can see a transaction that commits after a later-timestamped one
/// already moved the watermark past it. Both limits are the source's, not this implementation's, and are
/// restated in the descriptor where an operator will read them.</para>
///
/// <para><b>Values are handed over as CLR values, not JSON.</b> <c>ConnectorPollCycle.ExecuteRows</c>
/// exists precisely so a <c>numeric</c> or a <c>timestamptz</c> never round-trips through text on the way
/// in. The only conversions here are the ones with no CLR representation the platform's six field types
/// can hold: bytes become base64, a GUID becomes its canonical string, and an array/composite/range
/// becomes JSON text — each of which is what the corresponding <see cref="FieldType"/> means.</para>
/// </summary>
public sealed class DbSource(ISqlDialect dialect) : IPolledTransport, ISchemaProbe
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly ISqlDialect _dialect = dialect;

    public string Kind => _dialect.Kind;

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

        var query = config.Query.Trim();
        var hasQuery = query.Length > 0;
        if (!hasQuery && string.IsNullOrWhiteSpace(config.Table))
        {
            errors.Add("connector.db needs a table (or a query)");
        }

        if (string.IsNullOrWhiteSpace(config.CursorColumn))
        {
            // Without it there is no ordering, no watermark and no way to page — a polled database source
            // is a cursor with a table attached, not the other way round.
            errors.Add("connector.db needs a cursorColumn");
        }

        if (hasQuery)
        {
            if (!query.Contains(DbCursor.Placeholder, StringComparison.Ordinal))
            {
                errors.Add($"connector.db.query must contain {DbCursor.Placeholder} — it is bound as a parameter, never interpolated");
            }

            if (string.IsNullOrWhiteSpace(config.InitialCursor))
            {
                errors.Add("connector.db.query mode needs an initialCursor: there is no MAX(cursorColumn) to seed from in a custom query");
            }
        }

        if (config.CursorKind is not (CursorKinds.Long or CursorKinds.Timestamp or CursorKinds.String))
        {
            errors.Add($"connector.db.cursorKind must be one of: {CursorKinds.Long}, {CursorKinds.Timestamp}, {CursorKinds.String}");
        }

        if (DbCursor.Problem(config.InitialCursor, config.CursorKind) is { } problem)
        {
            errors.Add($"connector.db.initialCursor: {problem}");
        }

        if (config.BatchSize <= 0)
        {
            errors.Add("connector.db.batchSize must be greater than 0");
        }
    }

    public async Task<PolledBatch> PollAsync(SourceDefinition def, string? cursor, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(def);
        var config = def.Connector?.Db
            ?? throw new InvalidOperationException($"source '{def.Name}' is kind '{Kind}' but has no connector.db");

        var plan = DbPollPlanner.Plan(config, _dialect, cursor);

        await using var connection = _dialect.CreateConnection(DbEndpoint.From(config));
        await connection.OpenAsync(ct).ConfigureAwait(false);
        await using var command = Command(connection, plan.Sql, config.CommandTimeoutSeconds, plan.Parameters);

        if (plan.Seed)
        {
            // Branch 4: persist MAX(cursor) and emit nothing, so the next cycle tails. A NULL scalar (empty
            // table) encodes to null, i.e. "leave the cursor unset" — the cycle after this one tries again.
            var max = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return new PolledBatch([], DbCursor.Encode(max, config.CursorKind), HasMore: false);
        }

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        var rows = await ReadAsync(reader, ct).ConfigureAwait(false);
        return DbPollPlanner.Complete(config, rows, cursor);
    }

    public async Task<SchemaProbeResult> ProbeAsync(SourceDefinition def, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(def);
        var config = def.Connector?.Db
            ?? throw new InvalidOperationException($"source '{def.Name}' is kind '{Kind}' but has no connector.db");

        var sql = string.IsNullOrWhiteSpace(config.Query)
            ? $"SELECT * FROM {_dialect.QualifiedTable(config.Schema, config.Table)}"
            : config.Query;

        // A custom query still has to be describable, so @cursor is bound — with the operator's
        // initialCursor when there is one, else NULL. Nothing executes under SchemaOnly; the value only
        // has to exist and, ideally, carry the right type.
        List<KeyValuePair<string, object?>> parameters = string.IsNullOrWhiteSpace(config.Query)
            ? []
            : [new(DbCursor.ParameterName, string.IsNullOrWhiteSpace(config.InitialCursor)
                ? DBNull.Value
                : DbCursor.Decode(config.InitialCursor, config.CursorKind))];

        await using var connection = _dialect.CreateConnection(DbEndpoint.From(config));
        await connection.OpenAsync(ct).ConfigureAwait(false);
        await using var command = Command(connection, sql, config.CommandTimeoutSeconds, parameters);
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SchemaOnly, ct).ConfigureAwait(false);

        var schema = await reader.GetSchemaTableAsync(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException("the server returned no schema for this table or query");

        List<FieldDef> fields = [];
        List<string> diagnostics = [];
        var hasTypeName = schema.Columns.Contains("DataTypeName");
        foreach (DataRow column in schema.Rows)
        {
            var name = Convert.ToString(column["ColumnName"], CultureInfo.InvariantCulture) ?? "";
            var typeName = hasTypeName ? Convert.ToString(column["DataTypeName"], CultureInfo.InvariantCulture) : null;
            var clr = column["DataType"] as Type;

            var mapped = _dialect.MapType(typeName, clr);
            fields.Add(new FieldDef(name, mapped.Type));
            if (mapped.Note is not null)
            {
                diagnostics.Add($"{name}: {mapped.Note}");
            }
        }

        if (!string.IsNullOrWhiteSpace(config.CursorColumn) &&
            !fields.Any(f => string.Equals(f.Name, config.CursorColumn.Trim(), StringComparison.Ordinal)))
        {
            // Not fatal — the probe still succeeded — but the source it describes could never advance.
            diagnostics.Add($"cursorColumn '{config.CursorColumn.Trim()}' is not in this result set, so the cursor could never advance");
        }

        return new SchemaProbeResult(fields, diagnostics);
    }

    public TransportDescriptor Describe() => new()
    {
        Kind = Kind,
        Version = "1.0.0", // plan 016 wave 4: explicit contract version — see TransportDescriptor.Version.
        Label = _dialect.Label,
        Help =
            "Polls a table (or your own query) on a schedule, keeping a durable high-water mark. " +
            "AT-LEAST-ONCE: a cycle that fails after reading keeps the old cursor and re-reads, so set a " +
            "dedup key if repeats matter. A polled source also never sees a transaction that commits after " +
            $"a later-timestamped one — for that you want the '{SourceKinds.PostgresCdc}' or " +
            $"'{SourceKinds.MsSqlCdc}' kind, which reads this database's own change log instead of a " +
            "cursor column. Debezium into a NATS source is still the route for a database this connector " +
            "does not speak natively (MySQL, Oracle, MongoDB).",
        ConfigProperty = "db",
        // Polled: this kind runs on the source's Schedule. Mapping: false — for a row source the SELECT
        // list IS the mapping. CanProbe: this class implements ISchemaProbe, so the console's Discover
        // button has something to call.
        Polled = true,
        Mapping = false,
        CanProbe = true,
        Groups =
        [
            new TransportGroup
            {
                Key = "cursor",
                Label = "Cursor",
                Help = "The high-water mark. It is persisted by the platform and is NOT reset when you edit this source.",
            },
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
            new TransportField { Key = "schema", Label = "Schema", Mono = true, Placeholder = _dialect.DefaultSchema },
            new TransportField { Key = "table", Label = "Table", Mono = true, Help = "Ignored when a query is set." },
            new TransportField
            {
                Key = "where", Label = "Where", Type = TransportFieldTypes.Text, Mono = true, Placeholder = "status = 'settled'",
                Help = "ANDed onto the generated predicate. Table mode only, and it is SQL — it is not escaped.",
            },
            new TransportField
            {
                Key = "cursorColumn", Label = "Cursor column", Required = true, Mono = true, Group = "cursor",
                Help =
                    "The monotonic column the watermark is taken from. A surrogate key is safe. AN updated_at " +
                    "IS NOT: compared with > it LOSES every row written in the same millisecond as the watermark. " +
                    "Set a dedup key column and this connector switches to >=, which re-reads that millisecond " +
                    "instead and dedups the repeats. Neither variant sees a transaction that commits after a " +
                    "later-timestamped one — that is the honest argument for CDC.",
            },
            new TransportField
            {
                Key = "cursorKind", Label = "Cursor type", Type = TransportFieldTypes.Select, Group = "cursor",
                Options = [CursorKinds.Long, CursorKinds.Timestamp, CursorKinds.String], Default = CursorKinds.Long,
                Help = "How the stored watermark is parsed back into a bound parameter. It cannot be inferred: an epoch second and an id are the same digits.",
            },
            new TransportField
            {
                Key = "initialCursor", Label = "Start at", Mono = true, Group = "cursor",
                Help = "Where to start when nothing is stored yet. Empty + snapshot = the beginning; empty without it = MAX(cursor column), i.e. new rows only. REQUIRED with a custom query.",
            },
            new TransportField
            {
                Key = "dedupKeyColumn", Label = "Dedup key column", Mono = true, Group = "cursor",
                Help = "Setting this also switches the comparison to >= — see the cursor column's note.",
            },
            new TransportField { Key = "snapshot", Label = "Read the whole table first", Type = TransportFieldTypes.Bool, Group = "cursor", Help = "Pages through the table one batch per cycle, persisting as it goes, so a restart resumes mid-snapshot." },
            new TransportField { Key = "batchSize", Label = "Rows per poll", Type = TransportFieldTypes.Number, Default = "1000", Group = "cursor" },
            new TransportField
            {
                Key = "query", Label = "Query", Type = TransportFieldTypes.Text, Mono = true, Group = "advanced",
                Placeholder = "SELECT id, symbol, CAST(px AS text) AS px FROM trades WHERE id > @cursor ORDER BY id LIMIT 1000",
                Help = "Your own SQL. It MUST contain @cursor, which is bound as a parameter. Nothing is added to it — supply your own ORDER BY and row limit, and select the cursor column or the watermark can never move.",
            },
            new TransportField { Key = "commandTimeoutSeconds", Label = "Command timeout (s)", Type = TransportFieldTypes.Number, Default = "30", Group = "advanced" },
            new TransportField { Key = "tls", Label = "Require TLS", Type = TransportFieldTypes.Bool, Group = "advanced" },
            new TransportField
            {
                Key = "connectionString", Label = "Connection string", Type = TransportFieldTypes.Secret, Group = "advanced",
                Help = "Overrides host/port/database/username/password/TLS entirely. Masked wholesale, so the host stops being visible in the console — that is the cost of the escape hatch.",
            },
        ],
    };

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

    private static async Task<List<Dictionary<string, object?>>> ReadAsync(DbDataReader reader, CancellationToken ct)
    {
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
                // A duplicate column name in a hand-written SELECT (a join selecting both `id`s) would
                // otherwise throw mid-batch. Last one wins, exactly as a JSON object with a repeated key does.
                row[names[i]] = Cell(reader.IsDBNull(i) ? null : reader.GetValue(i));
            }

            rows.Add(row);
        }

        return rows;
    }

    /// <summary>One column value as the platform's field types can hold it. Anything with no such
    /// representation — arrays, composites, ranges, hstore — becomes JSON text, which is exactly what
    /// <see cref="FieldType.Json"/> means and what the SQL dialect's <c>-&gt;</c> operators can read.</summary>
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
