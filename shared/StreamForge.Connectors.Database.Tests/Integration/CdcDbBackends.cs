using StreamForge.Abstractions;
using Xunit;

namespace StreamForge.Connectors.Database.Tests.Integration;

/// <summary>
/// Plan 017 wave G: the two native CDC kinds need servers configured differently from plan 014 wave M's
/// <see cref="DbBackends"/> — a PostgreSQL with <c>wal_level = logical</c> so a replication slot can even
/// be opened at all, and a SQL Server with its Agent enabled so the capture job that turns log records into
/// CDC change rows actually runs. <see cref="DbBackend.RunArguments"/> on the existing <see cref="DbBackends.Postgres"/>
/// / <see cref="DbBackends.SqlServer"/> singletons is wave M's file and out of this wave's ownership to
/// edit — and editing it would ALSO be wrong even if it were in scope, since it would silently change what
/// the polled-kind (<c>postgres</c>/<c>mssql</c>) integration tests run against. So this is a SECOND pair of
/// backends: same <see cref="DbBackend"/> contract, same dialects, different images-with-flags, different
/// container names, different ports — started (and torn down) by <see cref="CdcServers"/> exactly the way
/// <see cref="DbServers"/> starts the originals.
/// </summary>
internal sealed class PgCdcBackend : DbBackend
{
    public override string Name => SourceKinds.PostgresCdc;

    public override ISqlDialect Dialect { get; } = new PostgresDialect();

    public override string Image => "postgres:17";

    public override string ContainerName => "sf-it-postgres-cdc";

    /// <summary>Its own port, distinct from <see cref="DbBackends.Postgres"/>'s 65432 — this is a
    /// DIFFERENT server, not a shared one, because <c>wal_level</c> is a boot-time setting the wave-M
    /// container was never started with and cannot be changed on a running instance.</summary>
    public override int HostPort => 65442;

    /// <summary>Everything <c>PostgresBackend</c> passes, plus an overridden CMD
    /// (<c>postgres -c wal_level=logical</c>) — the officially documented way to hand the stock image an
    /// extra <c>postgresql.conf</c> setting from the command line. <c>wal_level</c> is not reloadable, so it
    /// has to be set before the server's very first start, which is exactly what overriding CMD does.</summary>
    public override IReadOnlyList<string> RunArguments =>
    [
        "-e", "POSTGRES_PASSWORD=" + Password,
        "-e", "POSTGRES_USER=" + Username,
        "-e", "POSTGRES_DB=" + Database,
        "-p", $"{HostPort}:5432",
        Image,
        "postgres", "-c", "wal_level=logical",
    ];

    public override string Database => "streamforge";

    public override string AdminDatabase => "streamforge";

    public override string Username => "streamforge";

    public override string Password => "streamforge";

    public override TimeSpan StartupBudget => TimeSpan.FromSeconds(60);

    public override string TextType => "text";

    public override string TimestampType => "timestamptz";

    public override string DoubleType => "double precision";

    public override object Timestamp(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Utc);

    /// <summary>Nothing to prepare database-wide: <see cref="Username"/> is the image's bootstrap role (the
    /// <c>POSTGRES_USER</c> initdb creates), which is a superuser and therefore already carries the
    /// REPLICATION privilege a logical-replication connection needs — no GRANT required. The slot and
    /// publication are per-TEST state (each test seeds its own table and needs its own coverage), so the
    /// tests create them themselves rather than this method creating one upfront.</summary>
    public override Task PrepareAsync(CancellationToken ct) => Task.CompletedTask;
}

internal sealed class MsSqlCdcBackend : DbBackend
{
    public override string Name => SourceKinds.MsSqlCdc;

    public override ISqlDialect Dialect { get; } = new SqlServerDialect();

    public override string Image => "mcr.microsoft.com/mssql/server:2022-latest";

    public override string ContainerName => "sf-it-mssql-cdc";

    /// <summary>Its own port, distinct from <see cref="DbBackends.SqlServer"/>'s 61433 — SQL Server Agent
    /// cannot be switched on after the fact on a running container; the wave-M container was never started
    /// with <c>MSSQL_AGENT_ENABLED</c>.</summary>
    public override int HostPort => 61443;

    public override IReadOnlyList<string> RunArguments =>
    [
        "--platform", "linux/amd64",
        "-e", "ACCEPT_EULA=Y",
        "-e", "MSSQL_SA_PASSWORD=" + Password,
        "-e", "MSSQL_PID=Developer",
        "-e", "MSSQL_AGENT_ENABLED=true",
        "-p", $"{HostPort}:1433",
        Image,
    ];

    public override string Database => "streamforge";

    public override string AdminDatabase => "master";

    public override string Username => "sa";

    public override string Password => "Str0ngForge!2026";

    /// <summary>Same emulation cost as <see cref="DbBackends.SqlServer"/>'s budget, kept generous rather
    /// than tight — a slow-starting Agent here costs the tests a slightly longer bounded retry (see
    /// <c>MsSqlCdcTests.PollUntilAsync</c>), never a hang, because CDC's own capture job is asynchronous by
    /// design regardless of how promptly Agent itself comes up.</summary>
    public override TimeSpan StartupBudget => TimeSpan.FromSeconds(180);

    public override string TextType => "nvarchar(200)";

    public override string TimestampType => "datetime2";

    public override string DoubleType => "float";

    public override object Timestamp(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>Creates the test database (the same statement <c>SqlServerBackend</c> uses), then enables
    /// CDC on IT — <c>sys.sp_cdc_enable_db</c>, database-wide and idempotent. Enabling CDC on each
    /// individual TABLE is per-test state, done by the tests themselves against their own freshly-seeded
    /// table (each with its own capture instance), exactly like slot/publication on the Postgres side.</summary>
    public override async Task PrepareAsync(CancellationToken ct)
    {
        await using (var admin = await OpenAsync(AdminDatabase, ct).ConfigureAwait(false))
        await using (var create = admin.CreateCommand())
        {
            create.CommandText = $"IF DB_ID('{Database}') IS NULL CREATE DATABASE [{Database}];";
            await create.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using var db = await OpenAsync(Database, ct).ConfigureAwait(false);
        await using var enable = db.CreateCommand();
        enable.CommandText = "IF ((SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()) = 0) EXEC sys.sp_cdc_enable_db;";
        await enable.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

/// <summary>The two CDC-tuned backends, as singletons — same discipline as <see cref="DbBackends"/>: the
/// attributes, the fixture and the tests must all agree on exactly one instance each, because the container
/// name and port live on it.</summary>
public static class CdcDbBackends
{
    public static DbBackend Postgres { get; } = new PgCdcBackend();

    public static DbBackend SqlServer { get; } = new MsSqlCdcBackend();

    public static IReadOnlyList<DbBackend> All { get; } = [Postgres, SqlServer];
}

/// <summary>A <c>[Fact]</c> that skips itself, with a reason, unless the CDC-tuned live PostgreSQL container
/// can be run. <see cref="DockerGate.SkipReason"/> takes any <see cref="DbBackend"/>, so it serves
/// <see cref="CdcDbBackends.Postgres"/> exactly as it already serves <see cref="DbBackends.Postgres"/>,
/// unmodified — see <see cref="DockerGate"/>'s own class doc.</summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1813:Avoid unsealed attributes", Justification = "sealed")]
public sealed class PostgresCdcFactAttribute : FactAttribute
{
    public override string? Skip
    {
        get => DockerGate.SkipReason(CdcDbBackends.Postgres) ?? base.Skip;
        set => base.Skip = value;
    }
}

/// <summary>A <c>[Fact]</c> that skips itself, with a reason, unless the CDC-tuned live SQL Server
/// container can be run — see <see cref="DockerGate"/>.</summary>
public sealed class MsSqlCdcFactAttribute : FactAttribute
{
    public override string? Skip
    {
        get => DockerGate.SkipReason(CdcDbBackends.SqlServer) ?? base.Skip;
        set => base.Skip = value;
    }
}
