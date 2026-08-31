using StreamsForge.Abstractions;
using StreamsForge.AppCore.Discovery;
using StreamsForge.Connectors.Database;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests;

/// <summary>Plan 016 wave 6, track A — <c>@name</c> resolving at the two real connect sites in this
/// project: <see cref="PostgresDialect.CreateConnection"/> and <see cref="SqlServerDialect.CreateConnection"/>,
/// via <see cref="DbEndpoint.Resolved"/> (see that method's doc comment for why resolution lives THERE and
/// not in <see cref="DbEndpoint.From(DbSourceConfig)"/>/<see cref="DbEndpoint.From(DbSinkConfig)"/>: those
/// two are also what <c>DbSource.Validate</c>/<c>DbSink.Validate</c>/<c>DbSink.IsConfigured</c> build
/// <see cref="DbEndpoint.Addressable"/> from, at SAVE time, and must accept a bare <c>@name</c> as
/// "present" without ever trying to resolve it).
///
/// <para>No live database is needed: neither dialect's <c>CreateConnection</c> opens anything — it only
/// builds a connection-string object — so asserting on the returned <see cref="System.Data.Common.DbConnection.ConnectionString"/>
/// proves resolution happened without touching <see cref="Integration.DockerGate"/> at all.</para>
///
/// <para><see cref="NamedEndpoints"/> is process-wide, but this test project (<c>StreamsForge.Connectors.Database.Tests</c>)
/// runs as its own test host process, separate from <c>StreamsForge.AppCore.Tests</c> and
/// <c>StreamsForge.Connectors.Fix.Tests</c> — so the only classes that can race this file's use of it are
/// OTHER classes in this same assembly, and no other class here touches <see cref="NamedEndpoints"/>. Every
/// test still clears it via try/finally, matching the discipline the other two tracks' test files use, in
/// case a future class in this project ever needs the same registry.</para></summary>
public class NamedEndpointResolutionTests
{
    private static readonly PostgresDialect Pg = new();
    private static readonly SqlServerDialect Ms = new();

    // ------------------------------------------------------------------
    // Host — literal unchanged; @known resolved; @unknown throws; embedded @ untouched.
    // ------------------------------------------------------------------

    [Fact]
    public void Postgres_LiteralHost_PassesThroughUnchanged()
    {
        NamedEndpoints.Clear();
        try
        {
            var endpoint = new DbEndpoint("literal-host", 5432, "trades", "svc", "pw", false, null);

            using var connection = Pg.CreateConnection(endpoint);

            Assert.Contains("Host=literal-host", connection.ConnectionString);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void Postgres_KnownHostReference_ResolvesToTheConfiguredValue()
    {
        NamedEndpoints.Clear();
        try
        {
            NamedEndpoints.Configure([new("primary-oltp", "prod-pg-host")]);
            var endpoint = new DbEndpoint("@primary-oltp", 5432, "trades", "svc", "pw", false, null);

            using var connection = Pg.CreateConnection(endpoint);

            Assert.Contains("Host=prod-pg-host", connection.ConnectionString);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void Postgres_UnknownHostReference_ThrowsTheResolversActionableMessage()
    {
        NamedEndpoints.Clear();
        try
        {
            var endpoint = new DbEndpoint("@no-such-db-host", 5432, "trades", "svc", "pw", false, null);

            var ex = Assert.Throws<InvalidOperationException>(() => Pg.CreateConnection(endpoint));

            Assert.Contains("no-such-db-host", ex.Message);
            Assert.Contains("not configured here", ex.Message);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void SqlServer_KnownHostReference_ResolvesToTheConfiguredValue()
    {
        NamedEndpoints.Clear();
        try
        {
            NamedEndpoints.Configure([new("reporting-mssql", "prod-ms-host")]);
            var endpoint = new DbEndpoint("@reporting-mssql", 0, "trades", "svc", "pw", false, null);

            using var connection = Ms.CreateConnection(endpoint);

            Assert.Contains("prod-ms-host", connection.ConnectionString, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void SqlServer_UnknownHostReference_Throws()
    {
        NamedEndpoints.Clear();
        try
        {
            var endpoint = new DbEndpoint("@no-such-mssql-host", 0, "trades", "svc", "pw", false, null);

            var ex = Assert.Throws<InvalidOperationException>(() => Ms.CreateConnection(endpoint));

            Assert.Contains("no-such-mssql-host", ex.Message);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    // ------------------------------------------------------------------
    // ConnectionString — the OTHER field DbEndpoint.Resolved touches, and the one that WINS over Host
    // when set (ISqlDialect's own documented rule).
    // ------------------------------------------------------------------

    [Fact]
    public void Postgres_KnownConnectionStringReference_Resolves()
    {
        NamedEndpoints.Clear();
        try
        {
            NamedEndpoints.Configure([new("primary-oltp-cs", "Host=prod-pg-host;Database=trades;Username=svc;Password=pw")]);
            var endpoint = new DbEndpoint("ignored-host", 5432, "trades", "svc", "pw", false, "@primary-oltp-cs");

            using var connection = Pg.CreateConnection(endpoint);

            Assert.Equal("Host=prod-pg-host;Database=trades;Username=svc;Password=pw", connection.ConnectionString);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void Postgres_LiteralConnectionStringWithEmbeddedAtSign_IsNotTreatedAsAReference()
    {
        // A literal Npgsql connection string never legitimately starts with '@', but this proves the
        // "entirely a reference" rule holds for ConnectionString too, not just Host/Url.
        NamedEndpoints.Clear();
        try
        {
            var endpoint = new DbEndpoint("ignored", 0, "trades", "svc", "pw", false, "Host=host;Username=svc@example.com;Password=pw");

            using var connection = Pg.CreateConnection(endpoint);

            Assert.Equal("Host=host;Username=svc@example.com;Password=pw", connection.ConnectionString);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    // ------------------------------------------------------------------
    // Never written back — the connect-site half of "an export still reads @name". DbEndpoint.Resolved()
    // returns a NEW record; From(...) copies fields raw. Nothing in this project's connect path ever
    // assigns back into a DbSourceConfig/DbSinkConfig, so re-reading the original config after a connect
    // attempt must still show the literal @name it was configured with — proven directly rather than
    // through the export/import machinery, which is the other track's file ownership.
    // ------------------------------------------------------------------

    [Fact]
    public void ResolvingAConnectionNeverMutatesTheOriginalSourceConfig()
    {
        NamedEndpoints.Clear();
        try
        {
            NamedEndpoints.Configure([new("primary-oltp", "prod-pg-host")]);
            var config = new DbSourceConfig { Host = "@primary-oltp", Database = "trades", Username = "svc" };

            using var connection = Pg.CreateConnection(DbEndpoint.From(config));

            Assert.Contains("Host=prod-pg-host", connection.ConnectionString);
            Assert.Equal("@primary-oltp", config.Host); // unchanged — this is what "never written back" means.
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    // ------------------------------------------------------------------
    // Addressable/Validate must NOT resolve — a bare @name is "present" regardless of whether this
    // environment can resolve it, because Validate runs at SAVE time, not connect time.
    // ------------------------------------------------------------------

    [Fact]
    public void AddressableTreatsAnUnresolvableReferenceAsPresent_WithoutResolvingIt()
    {
        NamedEndpoints.Clear();
        try
        {
            // No mapping configured anywhere - if Addressable resolved eagerly this would throw instead
            // of returning a plain bool.
            var endpoint = new DbEndpoint("@not-configured-anywhere", 0, "trades", "svc", "pw", false, null);

            Assert.True(endpoint.Addressable);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }
}
