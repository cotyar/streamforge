using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using StreamForge.Abstractions;
using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Database;

/// <summary>
/// One configured database sink on one pipeline or table. The platform's first
/// <see cref="IBatchSinkClient"/>: the batch <c>SinkFanout</c> delivers IS the transaction, which is the
/// whole reason that interface exists.
///
/// <para><b>The delivery ceiling, in plain words, and it is in the descriptor's Help too:
/// AT-MOST-ONCE.</b> A batch whose transaction fails is rolled back, counted, reported once per
/// <see cref="LogThrottleWindow"/> — and DROPPED. There is exactly one retry, and only when the driver
/// itself classifies the failure as a transient connection fault (a pooled connection the server closed
/// between batches, a failover): that case is common, costs nothing, and is the one where replaying cannot
/// double-write, because nothing was committed. Every other failure — a constraint violation, a missing
/// table, a type mismatch, a deadlock victim — is counted and dropped, because retrying it would replay
/// the same failure and because this sink has no durable queue to hold the batch in. Nothing upstream is
/// slowed down or informed; that is the same fire-and-forget contract every other sink in this repo
/// carries, restated rather than implied.</para>
///
/// <para><b>No DDL, ever.</b> The destination table must already exist. A streaming sink that could CREATE
/// is a trust escalation over one that can only INSERT, so a missing table surfaces as the server's own
/// error on every batch until an operator creates it.</para>
///
/// <para><b>The time budget is <see cref="DbSinkConfig.CommandTimeoutSeconds"/>, not the ~3s
/// <see cref="IBatchSinkClient"/>'s doc names</b> — and that divergence is deliberate rather than an
/// oversight. A transaction cannot be abandoned at 3 seconds and left to commit anyway; abandoning it
/// means rolling it back, and 3s is below the default command timeout of both drivers, so the 3s bound
/// would turn every ordinarily-slow batch into a dropped one. The caller is still bounded — by a number
/// the operator sets, defaulting to 30s.</para>
///
/// <para><b>Nothing is buffered across calls.</b> No linger, no accumulation, so there is never anything
/// to lose when <c>SinkSelection.Signature</c> tears this client down on a config edit — see
/// <see cref="IBatchSinkClient"/>'s doc for why that matters. Consequently there is nothing to dispose
/// either: connections are opened per batch and returned to ADO.NET's pool at the end of it.</para>
/// </summary>
public sealed class DbSinkClient : IBatchSinkClient
{
    /// <summary>Minimum gap between two <c>onFailure</c> invocations for the SAME client — see
    /// <see cref="NatsSinkClient.LogThrottleWindow"/>, same reason and same value.</summary>
    public static readonly TimeSpan LogThrottleWindow = TimeSpan.FromSeconds(30);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly DbSinkConfig _config;
    private readonly ISqlDialect _dialect;
    private readonly Action<string, Exception>? _onFailure;
    private readonly string _table;
    private readonly string? _refusal;

    private long _published;
    private long _failed;
    private string? _lastError;
    private long _lastFailureAtMs;
    private long _lastLoggedAtMs;

    public DbSinkClient(
        DbSinkConfig config, ISqlDialect dialect, string entityKind, string entityName, Action<string, Exception>? onFailure = null)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(dialect);
        _config = config;
        _dialect = dialect;
        _onFailure = onFailure;
        EntityName = entityName;
        EntityKind = entityKind;

        var name = config.Table.Replace("{name}", entityName, StringComparison.Ordinal);
        _table = _dialect.QualifiedTable(config.Schema, name);
        Destination = $"{_dialect.Kind}:{name}";

        // ISinkTransport.Validate is handed a SinkSpec and nothing else, so it CANNOT see whether the sink
        // hangs off a pipeline or a table — this constructor is the first place that is known. A pipeline
        // emits results, not deltas: no identity, no weight, nothing for "mirror current state" to mean.
        // Refusing here (loudly, on every batch, through the same counters an operator already watches)
        // beats writing every result row as a +1 upsert and calling it a mirror.
        _refusal = string.Equals(config.Mode, DbSinkModes.Upsert, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(entityKind, "pipeline", StringComparison.OrdinalIgnoreCase)
            ? "upsert mode is not valid on a pipeline sink: a pipeline emits results, not deltas, so there is no row identity and no weight to apply"
            : null;
    }

    /// <summary>"{schema}.{table}" as configured, with <c>{name}</c> expanded — what failure callbacks
    /// name, mirroring the NATS sink's subject and the file sink's path.</summary>
    public string Destination { get; }

    public string EntityName { get; }

    /// <summary>"pipeline" | "table". Load-bearing: upsert mode is refused on a pipeline.</summary>
    public string EntityKind { get; }

    public SinkPublishCounters Counters => new(
        Interlocked.Read(ref _published),
        Interlocked.Read(ref _failed),
        Volatile.Read(ref _lastError),
        Interlocked.Read(ref _lastFailureAtMs));

    /// <summary>One message is a batch of one — one transaction. Present because
    /// <see cref="ISinkClient"/> requires it, not because it is a good way to drive this sink; every call
    /// site that matters goes through <c>SinkFanout</c>, which finds <see cref="IBatchSinkClient"/>.</summary>
    public Task PublishAsync<T>(T payload, CancellationToken ct) => PublishBatchAsync([payload], ct);

    /// <summary>Applies the whole batch as one transaction. NEVER throws — see this class's doc.</summary>
    public async Task PublishBatchAsync<T>(IReadOnlyList<T> payloads, CancellationToken ct)
    {
        if (payloads is null || payloads.Count == 0)
        {
            return;
        }

        if (_refusal is not null)
        {
            Fail(new InvalidOperationException(_refusal), payloads.Count);
            return;
        }

        DbSinkPlan plan;
        try
        {
            plan = DbSinkPlanner.Plan(_config, _dialect, _table, [.. payloads.Select(RowOf)]);
        }
        catch (Exception ex)
        {
            // A planning failure is a config failure and would repeat on every batch; count and drop.
            Fail(ex, payloads.Count);
            return;
        }

        if (plan.Skipped > 0)
        {
            Fail(new InvalidOperationException($"{plan.Skipped} row(s) dropped: {plan.SkipReason}"), plan.Skipped);
        }

        var written = payloads.Count - plan.Skipped;
        if (plan.Statements.Count == 0)
        {
            return;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(_config.CommandTimeoutSeconds > 0 ? _config.CommandTimeoutSeconds : 30));

        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await ApplyAsync(plan, cts.Token).ConfigureAwait(false);
                Interlocked.Add(ref _published, written);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Host shutdown / sink reconfigured mid-publish — not a sink failure, and not retried.
                return;
            }
            catch (Exception ex)
            {
                if (attempt == 0 && _dialect.IsTransient(ex) && !cts.IsCancellationRequested)
                {
                    // Nothing committed, so a replay cannot double-write. Exactly one attempt: a second
                    // one would just be the retry loop this sink deliberately does not have.
                    continue;
                }

                Fail(ex, written);
                return;
            }
        }
    }

    /// <summary>Opens, runs every statement in order, commits. A failure anywhere rolls the whole batch
    /// back — which is what "one delivered batch = one transaction" buys, and the reason the upsert's
    /// deletes can safely be planned as separate statements.</summary>
    private async Task ApplyAsync(DbSinkPlan plan, CancellationToken ct)
    {
        await using var connection = _dialect.CreateConnection(DbEndpoint.From(_config));
        await connection.OpenAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        try
        {
            foreach (var statement in plan.Statements)
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = statement.Sql;
                command.CommandTimeout = _config.CommandTimeoutSeconds > 0 ? _config.CommandTimeoutSeconds : 30;
                for (var i = 0; i < statement.Parameters.Count; i++)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "p" + i.ToString(CultureInfo.InvariantCulture);
                    parameter.Value = statement.Parameters[i] ?? DBNull.Value;
                    command.Parameters.Add(parameter);
                }

                await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            await transaction.CommitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            try
            {
                // Best effort, and with its own token: rolling back with the token that just fired would
                // be a no-op, and disposing an uncommitted transaction rolls back anyway — this only makes
                // the rollback explicit and prompt.
                await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // The connection is already gone. Nothing was committed; there is nothing left to undo.
            }

            throw;
        }
    }

    /// <summary>Flattens a sink message into cells plus its weight — the same shapes
    /// <see cref="FileSinkClient"/> renders, so a file sink and a database sink on one entity write the
    /// same rows. A pipeline result has no weight and is +1; upsert mode refuses pipelines anyway.</summary>
    private static SinkRow RowOf<T>(T payload) => payload switch
    {
        NatsTableDeltaMessage d => new SinkRow(new Dictionary<string, object?>(d.Row, StringComparer.Ordinal), d.Weight),
        NatsPipelineRowMessage p => new SinkRow(new Dictionary<string, object?>(p.Row, StringComparer.Ordinal), 1),
        _ => new SinkRow(
            new Dictionary<string, object?>(StringComparer.Ordinal) { ["value"] = JsonSerializer.Serialize(payload, JsonOptions) },
            1),
    };

    private void Fail(Exception ex, long rows)
    {
        Interlocked.Add(ref _failed, rows);
        Volatile.Write(ref _lastError, $"{ex.GetType().Name}: {ex.Message}");
        Interlocked.Exchange(ref _lastFailureAtMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        if (_onFailure is null)
        {
            return;
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var last = Interlocked.Read(ref _lastLoggedAtMs);
        if (now - last < LogThrottleWindow.TotalMilliseconds)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref _lastLoggedAtMs, now, last) != last)
        {
            return;
        }

        _onFailure(Destination, ex);
    }

    /// <summary>Nothing to release: no connection outlives a batch and nothing is buffered between them.
    /// See this class's doc.</summary>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>A database as an <see cref="ISinkTransport"/>, parameterized by dialect — one class, two
/// registered kinds, the mirror of <see cref="DbSource"/>.</summary>
public sealed class DbSink(ISqlDialect dialect) : ISinkTransport
{
    private readonly ISqlDialect _dialect = dialect;

    public string Kind => _dialect.Kind;

    public bool IsConfigured(SinkSpec spec) =>
        spec?.Db is { } db && DbEndpoint.From(db).Addressable && !string.IsNullOrWhiteSpace(db.Table);

    public ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure) =>
        new DbSinkClient(spec.Db!, _dialect, entityKind, entityName, onFailure);

    public void Validate(SinkSpec spec, List<string> errors)
    {
        ArgumentNullException.ThrowIfNull(errors);
        var config = spec?.Db;
        if (config is null)
        {
            errors.Add($"sink kind '{Kind}' requires db configuration");
            return;
        }

        if (!DbEndpoint.From(config).Addressable)
        {
            errors.Add("db sink needs host + database (or a connectionString)");
        }

        if (string.IsNullOrWhiteSpace(config.Table))
        {
            errors.Add("db sink needs a table — it must already exist, this sink issues no DDL");
        }

        var upsert = string.Equals(config.Mode, DbSinkModes.Upsert, StringComparison.OrdinalIgnoreCase);
        var append = string.Equals(config.Mode, DbSinkModes.Append, StringComparison.OrdinalIgnoreCase);
        if (!upsert && !append)
        {
            errors.Add($"db sink mode must be '{DbSinkModes.Append}' or '{DbSinkModes.Upsert}'");
        }

        if (upsert && DbSinkPlanner.Keys(config).Count == 0)
        {
            errors.Add("db sink upsert mode needs keyColumns — the identity is explicit because a sink only ever sees the entity name, never its SQL");
        }

        if (upsert && config.IncludeWeight)
        {
            // In upsert mode the weight IS the operation; persisting it would store a number already spent.
            errors.Add("db sink includeWeight is append-only: in upsert mode the weight is the operation, not a column");
        }

        // NOT checkable here: upsert on a PIPELINE sink. SinkSpec carries no entity kind — the owning
        // pipeline/table is known only at Create time — so DbSinkClient refuses it there instead, on
        // every batch, through the counters and the failure callback.
    }

    public TransportDescriptor Describe() => new()
    {
        Kind = Kind,
        Version = "1.0.0", // plan 016 wave 4: explicit contract version — see TransportDescriptor.Version.
        Label = _dialect.Label,
        Help =
            "Writes rows into an EXISTING table — this sink issues no DDL. One delivered batch is one " +
            "transaction. AT MOST ONCE: a batch whose transaction fails is rolled back, counted and " +
            "DROPPED; there is one retry, and only for a connection fault the driver itself calls " +
            "transient. Upsert mode mirrors a table's deltas and is refused on a pipeline sink.",
        ConfigProperty = "db",
        Groups =
        [
            new TransportGroup { Key = "write", Label = "Write mode", Help = "Append is a log. Upsert mirrors current state and needs a unique index on the key columns." },
            new TransportGroup { Key = "advanced", Label = "Advanced", Help = "Escape hatches. connectionString overrides every connection field above it." },
        ],
        Fields =
        [
            new TransportField { Key = "host", Label = "Host", Required = true, Mono = true, Placeholder = "db.internal" },
            new TransportField { Key = "port", Label = "Port", Type = TransportFieldTypes.Number, Placeholder = _dialect.DefaultPort.ToString(CultureInfo.InvariantCulture), Help = "0 uses the default." },
            new TransportField { Key = "database", Label = "Database", Required = true, Mono = true },
            new TransportField { Key = "username", Label = "Username" },
            new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret },
            new TransportField { Key = "schema", Label = "Schema", Mono = true, Placeholder = _dialect.DefaultSchema },
            new TransportField
            {
                Key = "table", Label = "Table", Required = true, Mono = true, Placeholder = "{name}",
                Help = "{name} expands to this pipeline's id / table's name. It must already exist.",
            },
            new TransportField
            {
                Key = "mode", Label = "Mode", Type = TransportFieldTypes.Select, Group = "write",
                Options = [DbSinkModes.Append, DbSinkModes.Upsert], Default = DbSinkModes.Append,
                Help = "Upsert applies positive weights as upserts and negative ones as deletes, deletes last, in the one transaction. Table sinks only.",
            },
            new TransportField
            {
                Key = "keyColumns", Label = "Key columns", Mono = true, Group = "write", Placeholder = "symbol,venue",
                Help = "Comma-separated. REQUIRED for upsert, and a UNIQUE INDEX must already cover them or the server refuses the statement.",
            },
            new TransportField
            {
                Key = "includeWeight", Label = "Write _weight column", Type = TransportFieldTypes.Bool, Group = "write",
                Help = "Append mode only. In upsert mode the weight is the operation, not a column.",
            },
            new TransportField
            {
                Key = "columns", Label = "Columns", Type = TransportFieldTypes.Text, Mono = true, Group = "advanced",
                Help = "Explicit column order, comma-separated. Empty = the union of the batch's own keys.",
            },
            new TransportField { Key = "commandTimeoutSeconds", Label = "Transaction timeout (s)", Type = TransportFieldTypes.Number, Default = "30", Group = "advanced" },
            new TransportField { Key = "tls", Label = "Require TLS", Type = TransportFieldTypes.Bool, Group = "advanced" },
            new TransportField
            {
                Key = "connectionString", Label = "Connection string", Type = TransportFieldTypes.Secret, Group = "advanced",
                Help = "Overrides host/port/database/username/password/TLS entirely. Masked wholesale, so the host stops being visible in the console.",
            },
        ],
    };
}
