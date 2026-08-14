using StreamForge.Abstractions;
using StreamForge.AppCore.Sinks;
using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

/// <summary>
/// The sink's side of the fire-and-forget contract, plus the one rule <c>ISinkTransport.Validate</c>
/// structurally cannot enforce. No server is involved: every case here is either a refusal decided before
/// a connection is attempted, or a connection that is guaranteed to fail.
/// </summary>
public class DbSinkClientTests
{
    private static DbSinkConfig Config(string mode = DbSinkModes.Append) => new()
    {
        // Port 1 on loopback: connection refused immediately, which is exactly the "cannot open" case.
        Host = "127.0.0.1",
        Port = 1,
        Database = "market",
        Username = "sf",
        Password = "pw",
        Table = "trades",
        Mode = mode,
        KeyColumns = "symbol",
        CommandTimeoutSeconds = 5,
    };

    private static NatsTableDeltaMessage Delta(string symbol, long weight = 1) => new()
    {
        Table = "trades",
        Seq = 1,
        Weight = weight,
        Row = new Dictionary<string, object?>(StringComparer.Ordinal) { ["symbol"] = symbol, ["qty"] = 1L },
    };

    [Fact]
    public async Task PublishBatchDoesNotThrowWhenTheConnectionCannotBeOpened()
    {
        // The callers deliberately await this with no try/catch around it — a sink that propagated would
        // take the publisher service down with it.
        Exception? reported = null;
        await using var client = new DbSinkClient(Config(), new PostgresDialect(), "table", "trades", (_, ex) => reported = ex);

        await client.PublishBatchAsync([Delta("AAPL"), Delta("MSFT")], CancellationToken.None);

        Assert.Equal(2, client.Counters.Failed);
        Assert.Equal(0, client.Counters.Published);
        Assert.NotNull(client.Counters.LastError);
        Assert.NotNull(reported);
    }

    [Fact]
    public async Task TheSameIsTrueOfTheSingleMessagePathAndOfSqlServer()
    {
        await using var client = new DbSinkClient(
            new DbSinkConfig
            {
                Table = "trades",
                CommandTimeoutSeconds = 5,
                // Connect Timeout is set through the escape hatch: SqlClient's own default is 15s, which
                // would make this test slow for no additional coverage.
                ConnectionString = "Server=127.0.0.1,1;Database=market;User ID=sf;Password=pw;Connect Timeout=1;TrustServerCertificate=true",
            },
            new SqlServerDialect(), "table", "trades");

        await client.PublishAsync(Delta("AAPL"), CancellationToken.None);

        Assert.Equal(1, client.Counters.Failed);
    }

    [Fact]
    public async Task UpsertOnAPipelineSinkIsRefusedBeforeAnyConnectionIsAttempted()
    {
        // SinkSpec carries no entity kind, so Validate cannot see this — the client is the first place it
        // is known. A pipeline emits results, not deltas: no identity, no weight, nothing for "mirror
        // current state" to mean.
        Exception? reported = null;
        await using var client = new DbSinkClient(Config(DbSinkModes.Upsert), new PostgresDialect(), "pipeline", "p1", (_, ex) => reported = ex);

        var started = DateTimeOffset.UtcNow;
        await client.PublishBatchAsync([Delta("AAPL")], CancellationToken.None);

        Assert.Equal(1, client.Counters.Failed);
        Assert.Contains("pipeline", reported!.Message, StringComparison.Ordinal);
        // No connection attempt at all — the refusal is structural, not a failed round-trip.
        Assert.True(DateTimeOffset.UtcNow - started < TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task UpsertOnATableSinkIsNotRefused()
    {
        await using var client = new DbSinkClient(Config(DbSinkModes.Upsert), new PostgresDialect(), "table", "trades");

        await client.PublishBatchAsync([Delta("AAPL")], CancellationToken.None);

        // It still fails (nothing is listening) — but on the connection, not on the mode.
        Assert.DoesNotContain("pipeline", client.Counters.LastError!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnEmptyBatchIsAValidNoOpRatherThanAnError()
    {
        await using var client = new DbSinkClient(Config(), new PostgresDialect(), "table", "trades");

        await client.PublishBatchAsync(Array.Empty<NatsTableDeltaMessage>(), CancellationToken.None);

        Assert.Equal(0, client.Counters.Failed);
        Assert.Equal(0, client.Counters.Published);
    }

    [Fact]
    public async Task AnAlreadyCancelledTokenIsAShutdownNotASinkFailure()
    {
        await using var client = new DbSinkClient(Config(), new PostgresDialect(), "table", "trades");
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await client.PublishBatchAsync([Delta("AAPL")], cts.Token);

        Assert.Equal(0, client.Counters.Failed);
    }

    [Fact]
    public void TheDestinationNamedInFailuresHasTheEntityNameSubstitutedIn()
    {
        var config = Config();
        config.Table = "sf_{name}";

        var client = new DbSinkClient(config, new PostgresDialect(), "table", "trades");

        Assert.Equal("postgres:sf_trades", client.Destination);
        Assert.Equal("trades", client.EntityName);
    }

    [Fact]
    public async Task FailureCallbacksAreThrottledSoADownServerDoesNotProduceOneLogLinePerBatch()
    {
        var calls = 0;
        await using var client = new DbSinkClient(Config(), new PostgresDialect(), "table", "trades", (_, _) => calls++);

        await client.PublishBatchAsync([Delta("AAPL")], CancellationToken.None);
        await client.PublishBatchAsync([Delta("MSFT")], CancellationToken.None);

        Assert.Equal(1, calls);
        Assert.Equal(2, client.Counters.Failed);
    }

    [Fact]
    public async Task ARowMissingItsKeyIsCountedAsFailedRatherThanVanishing()
    {
        await using var client = new DbSinkClient(Config(DbSinkModes.Upsert), new PostgresDialect(), "table", "trades");
        NatsTableDeltaMessage keyless = new() { Table = "trades", Row = new Dictionary<string, object?> { ["qty"] = 1L }, Weight = 1 };

        await client.PublishBatchAsync([keyless], CancellationToken.None);

        Assert.Equal(1, client.Counters.Failed);
        Assert.Contains("dropped", client.Counters.LastError!, StringComparison.Ordinal);
    }
}
