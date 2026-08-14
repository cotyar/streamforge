using System.Data.Common;
using System.Globalization;
using StreamForge.Abstractions;

namespace StreamForge.Connectors.Database.Tests.Integration;

/// <summary>
/// One live database under test: the container that provides it, the credentials that reach it, and the
/// handful of DDL words that genuinely differ between the two servers. Everything else the tests say is
/// dialect-neutral, which is the point — a source test written once runs against both engines and any
/// divergence shows up as a failure rather than as a test that only ever existed for one of them.
///
/// <para><b>Connections are opened through <see cref="ISqlDialect.CreateConnection"/> with the same
/// <see cref="DbEndpoint"/> the connector itself builds</b>, not through a hand-written connection string.
/// That is deliberate: the STRUCTURED connection fields (host/port/database/username/password/tls) are
/// production code no unit test can exercise, and pointing the tests' own fixture SQL at them means a
/// mistake in that builder — a wrong TLS default, a port that never reaches the DataSource — fails
/// everything here immediately instead of hiding behind an escape-hatch connection string.</para>
///
/// <para><b>Ports are 65432 and 61433, not 5432 and 1433.</b> A developer's own PostgreSQL is far more
/// likely to be on the default port than on these, and a test suite that silently seeded tables into
/// someone's real database would be a much worse bug than a test suite that fails to connect.</para>
/// </summary>
public abstract class DbBackend
{
    private static int _tableCounter;

    /// <summary>Four hex digits of this PROCESS, mixed into every table name. Two test runs can be alive
    /// against one adopted container at the same time in this repo (see <see cref="DbServers"/>), and a
    /// per-run counter alone would have them both creating <c>orders_1</c>.</summary>
    private static readonly string ProcessTag = Environment.ProcessId.ToString("x4", CultureInfo.InvariantCulture)[^4..];

    /// <summary>"postgres" | "mssql" — the same string as the registered transport kind.</summary>
    public abstract string Name { get; }

    public abstract ISqlDialect Dialect { get; }

    public abstract string Image { get; }

    public abstract string ContainerName { get; }

    /// <summary>Host-side port. Deliberately not the engine's default — see the class doc.</summary>
    public abstract int HostPort { get; }

    /// <summary>The <c>docker run</c> arguments after <c>-d --rm --name &lt;container&gt;</c>.</summary>
    public abstract IReadOnlyList<string> RunArguments { get; }

    public abstract string Database { get; }

    /// <summary>The database to connect to before <see cref="Database"/> exists — "master" on SQL Server,
    /// which is also where <see cref="PrepareAsync"/> creates it. PostgreSQL's image creates the database
    /// itself from <c>POSTGRES_DB</c>, so the two are the same there.</summary>
    public abstract string AdminDatabase { get; }

    public abstract string Username { get; }

    public abstract string Password { get; }

    /// <summary>How long this engine is allowed to take before it accepts a real query. SQL Server under
    /// x86-64 emulation on an arm64 host genuinely needs ~30s; PostgreSQL native needs ~2.</summary>
    public abstract TimeSpan StartupBudget { get; }

    // ---- the DDL vocabulary that actually differs ----

    /// <summary>A wide-enough text column.</summary>
    public abstract string TextType { get; }

    /// <summary>A timestamp column with no zone conversion surprises: <c>timestamptz</c> /
    /// <c>datetime2</c>. Which one matters — see <see cref="Timestamp"/>.</summary>
    public abstract string TimestampType { get; }

    /// <summary>Double-precision float.</summary>
    public abstract string DoubleType { get; }

    /// <summary>
    /// The CLR value to bind for <paramref name="utc"/> against <see cref="TimestampType"/>. Npgsql binds
    /// a <see cref="DateTimeKind.Utc"/> <see cref="DateTime"/> to <c>timestamptz</c> and REFUSES an
    /// unspecified one; SQL Server's <c>datetime2</c> has no zone at all and reads back as
    /// <see cref="DateTimeKind.Unspecified"/>. That asymmetry is exactly what <c>DbCursor</c>'s timestamp
    /// encoding preserves one bit for, so the tests reproduce it rather than papering over it.
    /// </summary>
    public abstract object Timestamp(DateTime utc);

    /// <summary>A fresh table name, unique within the run, so tests inside one collection never collide
    /// and a failure leaves its own table behind to look at.</summary>
    public string NewTable(string prefix)
        => $"{prefix}_{ProcessTag}_{Interlocked.Increment(ref _tableCounter).ToString(CultureInfo.InvariantCulture)}";

    public DbEndpoint Endpoint(string? database = null)
        => new("127.0.0.1", HostPort, database ?? Database, Username, Password, Tls: false, ConnectionString: null);

    /// <summary>An OPEN connection to <paramref name="database"/> (default: the test database).</summary>
    public async Task<DbConnection> OpenAsync(string? database = null, CancellationToken ct = default)
    {
        var connection = Dialect.CreateConnection(Endpoint(database));
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Everything a source needs bar the table and the cursor — filled in per test.</summary>
    public DbSourceConfig SourceConfig(string table, Action<DbSourceConfig>? tweak = null)
    {
        DbSourceConfig config = new()
        {
            Host = "127.0.0.1",
            Port = HostPort,
            Database = Database,
            Username = Username,
            Password = Password,
            Table = table,
            CursorColumn = "id",
            CursorKind = CursorKinds.Long,
            BatchSize = 1000,
            CommandTimeoutSeconds = 30,
        };
        tweak?.Invoke(config);
        return config;
    }

    public DbSinkConfig SinkConfig(string table, Action<DbSinkConfig>? tweak = null)
    {
        DbSinkConfig config = new()
        {
            Host = "127.0.0.1",
            Port = HostPort,
            Database = Database,
            Username = Username,
            Password = Password,
            Table = table,
            Mode = DbSinkModes.Append,
            CommandTimeoutSeconds = 30,
        };
        tweak?.Invoke(config);
        return config;
    }

    /// <summary>The source definition a driver would hand to <c>PollAsync</c>. <c>Fields</c> stays empty
    /// on purpose: a database source declares no schema, so coercion has nothing to apply and the values
    /// the tests assert are the ones the DRIVER produced, not ones a declared type rewrote.</summary>
    public SourceDefinition Definition(DbSourceConfig config, string name = "live")
        => new() { Name = name, Kind = Name, Connector = new ConnectorConfig { Db = config } };

    /// <summary>Runs once after the container answers: whatever the engine needs before the tests can
    /// assume <see cref="Database"/> exists.</summary>
    public abstract Task PrepareAsync(CancellationToken ct);

    public override string ToString() => Name;
}

/// <summary>The two backends, as singletons — the attributes, the fixture and the tests must all agree on
/// exactly one instance each, because the container name and port live on it.</summary>
public static class DbBackends
{
    public static DbBackend Postgres { get; } = new PostgresBackend();

    public static DbBackend SqlServer { get; } = new SqlServerBackend();

    public static IReadOnlyList<DbBackend> All { get; } = [Postgres, SqlServer];
}

internal sealed class PostgresBackend : DbBackend
{
    public override string Name => "postgres";

    public override ISqlDialect Dialect { get; } = new PostgresDialect();

    public override string Image => "postgres:17";

    public override string ContainerName => "sf-it-postgres";

    public override int HostPort => 65432;

    public override IReadOnlyList<string> RunArguments =>
    [
        "-e", "POSTGRES_PASSWORD=" + Password,
        "-e", "POSTGRES_USER=" + Username,
        "-e", "POSTGRES_DB=" + Database,
        "-p", $"{HostPort}:5432",
        Image,
    ];

    public override string Database => "streamforge";

    public override string AdminDatabase => "streamforge";

    public override string Username => "streamforge";

    public override string Password => "streamforge";

    /// <summary>The image boots in ~2s, but <c>pg_isready</c> is NOT what this waits on — it reports
    /// success while the server is still starting up and refusing connections, which is why
    /// <see cref="DbServers"/> gates on a real query instead. The budget is generous for a cold volume.</summary>
    public override TimeSpan StartupBudget => TimeSpan.FromSeconds(60);

    public override string TextType => "text";

    public override string TimestampType => "timestamptz";

    public override string DoubleType => "double precision";

    public override object Timestamp(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Utc);

    public override Task PrepareAsync(CancellationToken ct) => Task.CompletedTask;
}

internal sealed class SqlServerBackend : DbBackend
{
    public override string Name => "mssql";

    public override ISqlDialect Dialect { get; } = new SqlServerDialect();

    public override string Image => "mcr.microsoft.com/mssql/server:2022-latest";

    public override string ContainerName => "sf-it-mssql";

    public override int HostPort => 61433;

    /// <summary><c>--platform linux/amd64</c> is not optional: there is no arm64 SQL Server image, so on
    /// an Apple-silicon host this runs under emulation — which is also why <see cref="StartupBudget"/> is
    /// what it is.</summary>
    public override IReadOnlyList<string> RunArguments =>
    [
        "--platform", "linux/amd64",
        "-e", "ACCEPT_EULA=Y",
        "-e", "MSSQL_SA_PASSWORD=" + Password,
        "-e", "MSSQL_PID=Developer",
        "-p", $"{HostPort}:1433",
        Image,
    ];

    public override string Database => "streamforge";

    public override string AdminDatabase => "master";

    public override string Username => "sa";

    /// <summary>SQL Server refuses a weak SA password outright and the container then exits during
    /// start-up with the reason buried in its log — hence the shape.</summary>
    public override string Password => "Str0ngForge!2026";

    public override TimeSpan StartupBudget => TimeSpan.FromSeconds(180);

    public override string TextType => "nvarchar(200)";

    public override string TimestampType => "datetime2";

    public override string DoubleType => "float";

    public override object Timestamp(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>Unlike the PostgreSQL image there is no "create this database on first boot" variable, so
    /// the test database is created here, from master, once the server answers.</summary>
    public override async Task PrepareAsync(CancellationToken ct)
    {
        await using var connection = await OpenAsync(AdminDatabase, ct).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"IF DB_ID('{Database}') IS NULL CREATE DATABASE [{Database}];";
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
