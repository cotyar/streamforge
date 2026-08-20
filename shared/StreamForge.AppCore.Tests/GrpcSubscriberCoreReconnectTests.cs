using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Grpc;
using StreamForge.AppCore.Discovery;
using Xunit;

namespace StreamForge.AppCore.Tests;

/// <summary>Plan 016 wave 5, track B — proves the WIRING, not just the pure resolver:
/// <see cref="GrpcSubscriberCore.RunAsync"/> actually calls <see cref="GrpcPeerResolver.Resolve"/> at the
/// top of each (re)connect attempt, and an unresolvable peer actually lands on the existing
/// status-error/backoff path rather than that being an assumption nobody checked. Stops the loop by
/// cancelling from inside the "error" status callback itself - a resolution failure throws before any
/// network I/O, so no real gRPC/REST endpoint is needed to observe it.
///
/// <para>Shares xUnit collection <c>"StreamForge.Discovery.PeerDirectory"</c> (see
/// <see cref="GrpcPeerResolverTests"/>'s class remarks, and track A's own
/// <c>Discovery/PeerProbeTests.cs</c>, which named this hazard first) with every other test class in this
/// assembly that touches the process-wide <see cref="PeerDirectory"/>, so xUnit's default cross-class
/// parallelism cannot interleave a Clear/Configure from this file with one from another.</para></summary>
[Collection("StreamForge.Discovery.PeerDirectory")]
public class GrpcSubscriberCoreReconnectTests
{
    [Fact]
    public async Task RunAsync_ResolvesThePeerAtConnectTime_AnUnresolvablePeerReachesTheErrorStatusPath()
    {
        PeerDirectory.Clear();
        try
        {
            var config = new GrpcSubConfig { Peer = "nonexistent-peer", EntityKey = "source:x" };
            var statuses = new List<(string Status, string? Message)>();
            using var cts = new CancellationTokenSource();

            var core = new GrpcSubscriberCore(
                config,
                onRows: (_, _) => Task.CompletedTask,
                onStatus: (status, message) =>
                {
                    statuses.Add((status, message));
                    if (status == "error")
                    {
                        // Backoff after one failure is 30s (D-E) - cancel here rather than let the loop
                        // sleep through it. The point of this test is the first attempt's wiring, not
                        // the backoff formula (already covered elsewhere).
                        cts.Cancel();
                    }
                });

            await core.RunAsync(cts.Token);

            Assert.Contains(statuses, s => s.Status == "connecting");
            var error = Assert.Single(statuses, s => s.Status == "error");
            Assert.NotNull(error.Message);
            Assert.Contains("nonexistent-peer", error.Message);
            Assert.Contains("not a configured peer", error.Message);
        }
        finally
        {
            PeerDirectory.Clear();
        }
    }

    [Fact]
    public async Task RunAsync_ReResolvesOnEveryAttempt_APeerFixedBetweenAttemptsIsNoLongerAnUnknownPeerError()
    {
        // Not a full reconnect (that would sleep through the real 30s D-E backoff) - calls the same
        // resolution GrpcPeerResolver.Resolve directly twice, exactly as RunAsync does once per loop
        // iteration, to prove nothing between the two calls could have cached the first (failed) outcome.
        PeerDirectory.Clear();
        try
        {
            var config = new GrpcSubConfig { Peer = "prod", EntityKey = "source:x" };

            var firstAttempt = Record.Exception(() => GrpcPeerResolver.Resolve(config));
            Assert.IsType<InvalidOperationException>(firstAttempt);
            Assert.Contains("not a configured peer", firstAttempt!.Message);

            // The fix an operator would make: reconfigure the host with the peer now present. No source
            // restart, no catalog edit - just PeerDirectory.Configure, the same call the host makes.
            PeerDirectory.Configure([new PeerRecord { Name = "prod", GrpcEndpoint = "http://prod-host:5299" }]);

            var resolved = GrpcPeerResolver.Resolve(config);
            Assert.Equal("http://prod-host:5299", resolved.GrpcAddress);
        }
        finally
        {
            PeerDirectory.Clear();
        }
    }
}
