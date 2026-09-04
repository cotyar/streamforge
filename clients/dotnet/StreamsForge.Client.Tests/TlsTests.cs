using StreamsForge.Client.Tests.Fixtures;
using Xunit;

namespace StreamsForge.Client.Tests;

/// <summary>
/// Live TLS tests against <see cref="TlsEngineFixture"/> (HTTPS + TLS gRPC on 7399/7499, a
/// self-signed dev cert from <c>tools/tls/dev-cert.sh</c> as its own trust anchor). Proves the
/// smallest end-to-end slice per transport (gRPC, SignalR) plus the negative case: connecting over
/// TLS with no configured CA and no <see cref="ConnectOptions.AcceptAnyCertificate"/> must fail, not
/// silently downgrade to trusting an unknown certificate.
/// </summary>
[Collection(nameof(TlsEngineCollection))]
public sealed class TlsTests
{
    private readonly TlsEngineFixture _engine;

    public TlsTests(TlsEngineFixture engine) => _engine = engine;

    private void SkipIfEngineUnavailable()
    {
        // Same rationale as ContractTests.SkipIfEngineUnavailable: plain xunit v2 has no dynamic
        // runtime skip for [Fact], so report the fixture's own diagnosis loudly instead of either
        // colliding with a running instance or passing vacuously.
        Assert.True(_engine.SkipReason is null, $"skipped: {_engine.SkipReason}");
    }

    private static Dictionary<string, object?> Row(string tradeId, string desk, double notional) =>
        new() { ["trade_id"] = tradeId, ["desk"] = desk, ["notional"] = notional };

    [Fact]
    public async Task ConnectOverTlsViaGrpc_ListsTablesAndReceivesASeededRow()
    {
        SkipIfEngineUnavailable();
        await using var client = await StreamsForgeClient.ConnectAsync(new ConnectOptions
        {
            Url = _engine.BaseUrl,
            GrpcTarget = _engine.GrpcTarget,
            User = TlsEngineFixture.AdminUser,
            Password = TlsEngineFixture.AdminPassword,
            CaCertificatePath = _engine.CaCertificatePath,
            Transport = TransportKind.Grpc,
        });
        Assert.Equal("grpc", client.TransportName);

        var tables = await client.ListTablesAsync();
        Assert.Contains(tables, t => t.Name == TlsEngineFixture.LatestTable);

        var tradeId = $"t-{Guid.NewGuid():N}";
        await using var table = await client.TableAsync(TlsEngineFixture.LatestTable, ["trade_id"], TimeSpan.FromSeconds(20));
        var ack = await client.PushAsync(TlsEngineFixture.SourceName, [Row(tradeId, "Rates", 100.0)]);
        Assert.Equal("INGEST_OUTCOME_ACCEPTED", ack.Outcome);

        var rows = await table.WaitForAsync(rs => rs.Any(r => Equals(r["trade_id"], tradeId)), TimeSpan.FromSeconds(20));
        Assert.Contains(rows, r => Equals(r["trade_id"], tradeId));
    }

    [Fact]
    public async Task ConnectOverTlsViaSignalR_ListsTablesAndReceivesASeededRow()
    {
        SkipIfEngineUnavailable();
        await using var client = await StreamsForgeClient.ConnectAsync(new ConnectOptions
        {
            Url = _engine.BaseUrl,
            User = TlsEngineFixture.AdminUser,
            Password = TlsEngineFixture.AdminPassword,
            CaCertificatePath = _engine.CaCertificatePath,
            Transport = TransportKind.SignalR,
        });
        Assert.StartsWith("signalr:", client.TransportName);

        var tables = await client.ListTablesAsync();
        Assert.Contains(tables, t => t.Name == TlsEngineFixture.LatestTable);

        var tradeId = $"t-{Guid.NewGuid():N}";
        await using var table = await client.TableAsync(TlsEngineFixture.LatestTable, ["trade_id"], TimeSpan.FromSeconds(20));
        var ack = await client.PushAsync(TlsEngineFixture.SourceName, [Row(tradeId, "Credit", 42.0)]);
        Assert.Equal("INGEST_OUTCOME_ACCEPTED", ack.Outcome);

        var rows = await table.WaitForAsync(rs => rs.Any(r => Equals(r["trade_id"], tradeId)), TimeSpan.FromSeconds(20));
        Assert.Contains(rows, r => Equals(r["trade_id"], tradeId));
    }

    [Fact]
    public async Task ConnectOverTlsWithoutTheConfiguredCa_Throws()
    {
        SkipIfEngineUnavailable();
        // No CaCertificatePath, no AcceptAnyCertificate: the self-signed dev cert is trusted by
        // neither the machine's own store nor anything this client was told to trust, so the
        // handshake must fail -- not silently succeed against an unverified certificate.
        await Assert.ThrowsAnyAsync<Exception>(() => StreamsForgeClient.ConnectAsync(new ConnectOptions
        {
            Url = _engine.BaseUrl,
            User = TlsEngineFixture.AdminUser,
            Password = TlsEngineFixture.AdminPassword,
            Transport = TransportKind.Grpc,
        }));
    }
}
