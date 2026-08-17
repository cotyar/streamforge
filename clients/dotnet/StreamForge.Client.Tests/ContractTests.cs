using StreamForge.Client.Tests.Fixtures;
using Xunit;

namespace StreamForge.Client.Tests;

/// <summary>
/// Contract tests against a real, isolated StreamForge instance (see <see cref="EngineFixture"/>),
/// run over BOTH live transports. The fixture wires one ingest source, one LATEST BY table and one
/// aggregate over that derived LATEST BY -- the same shape the Python client's contract fixture
/// uses -- and each test pushes fresh, uniquely-keyed rows so the theory cases (and the two
/// transports) never interfere with each other despite sharing one long-lived engine process.
/// </summary>
[Collection(nameof(EngineCollection))]
public sealed class ContractTests
{
    private readonly EngineFixture _engine;

    public ContractTests(EngineFixture engine) => _engine = engine;

    private void SkipIfEngineUnavailable()
    {
        // Plain xunit v2 has no dynamic runtime skip for [Theory]/[Fact] -- this fails loudly with
        // the fixture's own diagnosis (port collision, missing dotnet, publish failure) rather than
        // silently colliding with whatever is already on these ports.
        Assert.True(_engine.SkipReason is null, $"skipped: {_engine.SkipReason}");
    }

    private Task<StreamForgeClient> ConnectAsync(TransportKind kind) =>
        StreamForgeClient.ConnectAsync(new ConnectOptions
        {
            Url = _engine.BaseUrl,
            GrpcTarget = _engine.GrpcTarget,
            User = EngineFixture.AdminUser,
            Password = EngineFixture.AdminPassword,
            Transport = kind,
        });

    private static Dictionary<string, object?> Row(string tradeId, string desk, double notional) =>
        new() { ["trade_id"] = tradeId, ["desk"] = desk, ["notional"] = notional };

    [Theory]
    [InlineData(TransportKind.Grpc)]
    [InlineData(TransportKind.SignalR)]
    public async Task PushThenLiveTableSeesTheRow(TransportKind kind)
    {
        SkipIfEngineUnavailable();
        await using var client = await ConnectAsync(kind);
        Assert.Equal(kind == TransportKind.Grpc ? "grpc" : "signalr:ws", client.TransportName);

        var tradeId = $"t-{Guid.NewGuid():N}";
        await using var table = await client.TableAsync(EngineFixture.LatestTable, ["trade_id"], TimeSpan.FromSeconds(20));

        var ack = await client.PushAsync(EngineFixture.SourceName, [Row(tradeId, "Rates", 100.0)]);
        Assert.Equal("INGEST_OUTCOME_ACCEPTED", ack.Outcome);
        Assert.Equal(1, ack.Accepted);

        var rows = await table.WaitForAsync(rs => rs.Any(r => Equals(r["trade_id"], tradeId)), TimeSpan.FromSeconds(20));
        var row = rows.First(r => Equals(r["trade_id"], tradeId));
        Assert.Equal("Rates", row["desk"]);
    }

    [Theory]
    [InlineData(TransportKind.Grpc)]
    [InlineData(TransportKind.SignalR)]
    public async Task LatestBySupersedesThePreviousRowForTheSameKey(TransportKind kind)
    {
        SkipIfEngineUnavailable();
        await using var client = await ConnectAsync(kind);
        var tradeId = $"t-{Guid.NewGuid():N}";
        await using var table = await client.TableAsync(EngineFixture.LatestTable, ["trade_id"], TimeSpan.FromSeconds(20));

        await client.PushAsync(EngineFixture.SourceName, [Row(tradeId, "Rates", 100.0)]);
        await table.WaitForAsync(
            rs => rs.Any(r => Equals(r["trade_id"], tradeId) && Convert.ToDouble(r["notional"]) == 100.0),
            TimeSpan.FromSeconds(20));

        await client.PushAsync(EngineFixture.SourceName, [Row(tradeId, "Rates", 250.0)]);
        var rows = await table.WaitForAsync(
            rs => rs.Any(r => Equals(r["trade_id"], tradeId) && Convert.ToDouble(r["notional"]) == 250.0),
            TimeSpan.FromSeconds(20));

        // Supersession: the group ("trade_id") keeps exactly one row, even though the wire never
        // explicitly retracted the first one -- the reducer's core hazard, exercised end to end.
        Assert.Single(rows, r => Equals(r["trade_id"], tradeId));
    }

    [Theory]
    [InlineData(TransportKind.Grpc)]
    [InlineData(TransportKind.SignalR)]
    public async Task SnapshotMatchesLiveTableRows(TransportKind kind)
    {
        SkipIfEngineUnavailable();
        await using var client = await ConnectAsync(kind);
        var tradeId = $"t-{Guid.NewGuid():N}";
        await client.PushAsync(EngineFixture.SourceName, [Row(tradeId, "Credit", 42.0)]);

        await using var table = await client.TableAsync(EngineFixture.LatestTable, ["trade_id"], TimeSpan.FromSeconds(20));
        await table.WaitForAsync(rs => rs.Any(r => Equals(r["trade_id"], tradeId)), TimeSpan.FromSeconds(20));

        // SnapshotAsync is a plain REST read (see StreamForgeClient's class doc: catalog reads are
        // always REST regardless of transport), a genuinely separate request/response cycle from
        // the live subscription above -- poll briefly rather than assume the two are
        // read-your-writes consistent with zero propagation gap.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        IReadOnlyList<IReadOnlyDictionary<string, object?>> snapshot;
        do
        {
            snapshot = await client.SnapshotAsync(EngineFixture.LatestTable, limit: 1000);
            if (snapshot.Any(r => Equals(r["trade_id"], tradeId))) break;
            await Task.Delay(100);
        } while (DateTime.UtcNow < deadline);

        Assert.Contains(snapshot, r => Equals(r["trade_id"], tradeId));
    }

    [Theory]
    [InlineData(TransportKind.Grpc)]
    [InlineData(TransportKind.SignalR)]
    public async Task AggregateTableReflectsTheSumAcrossDesks(TransportKind kind)
    {
        SkipIfEngineUnavailable();
        await using var client = await ConnectAsync(kind);
        var desk = $"D{Guid.NewGuid():N}"[..10];
        var t1 = $"t-{Guid.NewGuid():N}";
        var t2 = $"t-{Guid.NewGuid():N}";

        await using var agg = await client.TableAsync(EngineFixture.AggTable, ["desk"], TimeSpan.FromSeconds(20));

        await client.PushAsync(EngineFixture.SourceName, [Row(t1, desk, 10.0)]);
        await client.PushAsync(EngineFixture.SourceName, [Row(t2, desk, 15.0)]);

        var rows = await agg.WaitForAsync(
            rs => rs.Any(r => Equals(r["desk"], desk) && Convert.ToDouble(r["total"]) == 25.0),
            TimeSpan.FromSeconds(20));
        Assert.Contains(rows, r => Equals(r["desk"], desk));
    }

    [Fact]
    public async Task ValidateReportsDiagnosticsForBadSql()
    {
        SkipIfEngineUnavailable();
        await using var client = await ConnectAsync(TransportKind.Grpc);
        var result = await client.ValidateAsync("SELECT this is not valid sql !!!");
        Assert.False(result.Ok);
        Assert.NotEmpty(result.Diagnostics);
    }

    [Fact]
    public async Task SqlAsyncCreatesQueriesAndDropsAnAdhocTable()
    {
        SkipIfEngineUnavailable();
        await using var client = await ConnectAsync(TransportKind.Grpc);
        var rawName = $"dotnet contract test {Guid.NewGuid():N}";
        var adhocName = StreamForgeClient.AdhocTableName(rawName);
        Assert.StartsWith("adhoc_", adhocName);

        await using (var table = await client.SqlAsync(
            $"SELECT trade_id, desk, notional FROM {EngineFixture.SourceName} LATEST BY (trade_id)",
            rawName, ["trade_id"], TimeSpan.FromSeconds(20)))
        {
            Assert.NotNull(table);
        }

        var adhoc = await client.AdhocTablesAsync();
        Assert.Contains(adhoc, t => t.Name == adhocName);

        var dropped = await client.DropAdhocAsync(adhocName);
        Assert.True(dropped);

        await Assert.ThrowsAsync<StreamForgeException>(() => client.DropAdhocAsync("not_prefixed"));
    }
}
