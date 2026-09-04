using Xunit;

namespace StreamsForge.Chain.Tests;

/// <summary>
/// Two independent StreamsForge hosts, A and B, booted in parallel as separate OS processes with
/// separate data directories — the only shape in this repo that can prove a federated chain end to
/// end, because a <c>TestCluster</c> has one silo, one catalog, and no gRPC listener to dial.
///
/// <para><b>B knows A by NAME, not by address.</b> B is started with a
/// <c>Discovery:Peers:0</c> entry called <c>a</c> pointing at A's REST and gRPC endpoints, so B's
/// federated source names <c>peer: "a"</c> and carries no address at all
/// (<c>GrpcSubConfig.Peer</c> resolves both addresses fresh at every reconnect and WINS over any
/// literal address). That is the plan-016 federation path, exercised across a real process boundary.</para>
///
/// <para><b>Ports</b> (reserved for the chain tests in CLAUDE.md, deliberately far from the dev
/// servers): A = 9399 REST / 9499 gRPC / 11399 silo / 30399 gateway;
/// B = 9599 / 9699 / 11599 / 30599. Two silos on one machine MUST have distinct
/// <c>Silo:Port</c>/<c>Silo:GatewayPort</c> — the defaults are shared and the second host would fail
/// to join.</para>
///
/// <para>Both hosts seed the demo catalog into their fresh data directories and their seeded
/// generators tick throughout. That is accepted, not worked around: the chain test asserts on its own
/// named source and table only, and the seeded traffic makes the test MORE representative of a live
/// instance, not less.</para>
///
/// <para>Cannot-run conditions (no host build, no <c>~/.dotnet/dotnet</c>, a busy port) are recorded
/// in <see cref="SkipReason"/> rather than thrown, and each test turns it into an explicit
/// "skipped: ..." assertion — the same convention as
/// <c>clients/dotnet/StreamsForge.Client.Tests/Fixtures/EngineFixture.cs</c>, because plain xunit v2
/// cannot skip dynamically at runtime.</para>
/// </summary>
public sealed class TwoHostFixture : IAsyncLifetime
{
    public const int AHttpPort = 9399;
    public const int AGrpcPort = 9499;
    public const int ASiloPort = 11399;
    public const int AGatewayPort = 30399;

    public const int BHttpPort = 9599;
    public const int BGrpcPort = 9699;
    public const int BSiloPort = 11599;
    public const int BGatewayPort = 30599;

    /// <summary>The name B knows A by. The federated source names this and nothing else.</summary>
    public const string PeerName = "a";

    public HostProcess? A { get; private set; }

    public HostProcess? B { get; private set; }

    public string? SkipReason { get; private set; }

    public async Task InitializeAsync()
    {
        SkipReason = HostProcess.Preflight(
            AHttpPort, AGrpcPort, ASiloPort, AGatewayPort,
            BHttpPort, BGrpcPort, BSiloPort, BGatewayPort);
        if (SkipReason is not null)
        {
            return;
        }

        A = new HostProcess("a", AHttpPort, AGrpcPort, ASiloPort, AGatewayPort);
        B = new HostProcess(
            "b", BHttpPort, BGrpcPort, BSiloPort, BGatewayPort,
            "--Discovery:Peers:0:Name", PeerName,
            "--Discovery:Peers:0:RestEndpoint", $"http://127.0.0.1:{AHttpPort}",
            "--Discovery:Peers:0:GrpcEndpoint", $"http://127.0.0.1:{AGrpcPort}");

        try
        {
            A.Start();
            B.Start();
            // In parallel: two cold Orleans silos are ~10-20s each, and they do not depend on one
            // another to become healthy (B resolves the peer lazily, at connect time).
            await Task.WhenAll(A.WaitHealthyAsync(), B.WaitHealthyAsync());
        }
        catch (Exception ex)
        {
            SkipReason = $"the two-host fixture did not come up cleanly: {ex.Message}";
            await DisposeAsync();
        }
    }

    public async Task DisposeAsync()
    {
        if (A is not null)
        {
            await A.DisposeAsync();
            A = null;
        }
        if (B is not null)
        {
            await B.DisposeAsync();
            B = null;
        }
    }
}

/// <summary>
/// One xunit collection for every test in this project, so the host processes run ONE CLASS AT A TIME.
/// These tests bind fixed ports and spawn full Orleans silos; running two such classes concurrently
/// would collide on nothing (the port sets are disjoint) but would put three or four silos on the
/// machine at once, and every assertion here is time-bounded. Serializing them is the cheap fix.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ChainHostCollection
{
    public const string Name = "StreamsForge chain hosts";
}
