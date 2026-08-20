using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Grpc;
using StreamForge.AppCore.Discovery;
using Xunit;

namespace StreamForge.AppCore.Tests;

/// <summary>Plan 016 wave 5, track B — <see cref="GrpcPeerResolver"/> is the pure decision behind the
/// federated <c>grpc</c> source's headline payoff: no hardcoded address, no GUID. These tests pin the
/// precedence rule pinned on <c>GrpcSubConfig.Peer</c>'s own doc comment (Peer WINS over both address
/// fields, outright, not merely filling blanks) and the two distinct failure sentences an operator has to
/// be able to act on without reading source.
///
/// <para><see cref="PeerDirectory"/> is process-wide, so every test calls <see cref="PeerDirectory.Clear"/>
/// both before configuring its own peers and via a fixture-less try/finally, and none of these tests may
/// assume any ordering relative to another test file that also touches the directory. They share xUnit
/// collection <c>"StreamForge.Discovery.PeerDirectory"</c> with <c>Discovery/PeerProbeTests.cs</c> (track
/// A's own file, whose class doc names this exact hazard: xUnit runs different test classes in this
/// assembly in parallel by default, and two classes each mutating this process-wide static registry would
/// race without sharing one collection) so xUnit's default cross-class parallelism cannot interleave a
/// Clear/Configure from this file with one from that file, or from
/// <see cref="GrpcSubscriberCoreReconnectTests"/>.</para></summary>
[Collection("StreamForge.Discovery.PeerDirectory")]
public class GrpcPeerResolverTests
{
    private static GrpcSubConfig Config(string address = "", string? restAddress = null, string? peer = null) => new()
    {
        Address = address,
        RestAddress = restAddress,
        Peer = peer,
        EntityKey = "source:whatever",
    };

    // ---- Peer unset: byte-identical pre-016 behaviour ---------------------

    [Fact]
    public void PeerUnset_ReturnsTheTwoAddressFieldsAsAuthored()
    {
        PeerDirectory.Clear();
        try
        {
            var config = Config(address: "http://literal-grpc:5299", restAddress: "http://literal-rest:5199");

            var resolved = GrpcPeerResolver.Resolve(config);

            Assert.Equal("http://literal-grpc:5299", resolved.GrpcAddress);
            Assert.Equal("http://literal-rest:5199", resolved.RestAddress);
            Assert.Null(resolved.PeerName);
        }
        finally
        {
            PeerDirectory.Clear();
        }
    }

    [Fact]
    public void PeerUnset_NeverConsultsTheDirectory_EvenWhenItIsEmpty()
    {
        // The directory has NOTHING configured - if Resolve consulted it for an unset Peer it would have
        // no way to succeed. It must not even try.
        PeerDirectory.Clear();
        try
        {
            var config = Config(address: "http://literal-grpc:5299");

            var resolved = GrpcPeerResolver.Resolve(config);

            Assert.Equal("http://literal-grpc:5299", resolved.GrpcAddress);
        }
        finally
        {
            PeerDirectory.Clear();
        }
    }

    // ---- Peer set: wins outright, does not merely fill blanks -------------

    [Fact]
    public void PeerSet_OverridesAStaleLiteralAddress_RatherThanFillingBlanks()
    {
        PeerDirectory.Clear();
        try
        {
            PeerDirectory.Configure([new PeerRecord
            {
                Name = "prod",
                GrpcEndpoint = "http://prod-host:5299",
                RestEndpoint = "http://prod-host:5199",
            }]);

            // Both address fields carry stale literal values - Peer must win over them, not defer to
            // them because they happen to be non-blank.
            var config = Config(address: "http://stale-old-host:5299", restAddress: "http://stale-old-host:5199", peer: "prod");

            var resolved = GrpcPeerResolver.Resolve(config);

            Assert.Equal("http://prod-host:5299", resolved.GrpcAddress);
            Assert.Equal("http://prod-host:5199", resolved.RestAddress);
            Assert.Equal("prod", resolved.PeerName);
        }
        finally
        {
            PeerDirectory.Clear();
        }
    }

    [Fact]
    public void PeerMovedBetweenTwoResolves_ThePickedUpEndpointChangesWithNoCaching()
    {
        // Proves resolution is not cached anywhere it could go stale: a bare re-call after reconfiguring
        // the directory (the effect of "reconfiguring the host, no source restart") sees the new address.
        PeerDirectory.Clear();
        try
        {
            PeerDirectory.Configure([new PeerRecord { Name = "prod", GrpcEndpoint = "http://host-a:5299" }]);
            var config = Config(peer: "prod");

            var first = GrpcPeerResolver.Resolve(config);
            Assert.Equal("http://host-a:5299", first.GrpcAddress);

            PeerDirectory.Configure([new PeerRecord { Name = "prod", GrpcEndpoint = "http://host-b:5299" }]);
            var second = GrpcPeerResolver.Resolve(config);

            Assert.Equal("http://host-b:5299", second.GrpcAddress);
        }
        finally
        {
            PeerDirectory.Clear();
        }
    }

    // ---- Failure paths: name the peer, say what's missing ------------------

    [Fact]
    public void UnknownPeer_NamesThePeerInTheError()
    {
        PeerDirectory.Clear();
        try
        {
            var config = Config(peer: "nonexistent-peer");

            var ex = Assert.Throws<InvalidOperationException>(() => GrpcPeerResolver.Resolve(config));

            Assert.Contains("nonexistent-peer", ex.Message);
            Assert.Contains("not a configured peer", ex.Message);
        }
        finally
        {
            PeerDirectory.Clear();
        }
    }

    [Fact]
    public void PeerWithNoGrpcEndpoint_NamesThePeerAndSaysWhatIsMissing()
    {
        PeerDirectory.Clear();
        try
        {
            PeerDirectory.Configure([new PeerRecord { Name = "rest-only", RestEndpoint = "http://rest-only:5199" }]);
            var config = Config(peer: "rest-only");

            var ex = Assert.Throws<InvalidOperationException>(() => GrpcPeerResolver.Resolve(config));

            Assert.Contains("rest-only", ex.Message);
            Assert.Contains("gRPC endpoint", ex.Message);
        }
        finally
        {
            PeerDirectory.Clear();
        }
    }

    [Fact]
    public void PeerWithNoRestEndpoint_ResolvesWithANullRestAddress_AndDoesNotThrowHere()
    {
        // A peer wired for reflection-only federation ("source:{name}" entity keys, no login) needs no
        // REST endpoint at all - Resolve itself must not require it. Only the callers that actually need
        // REST (login, table/pipeline ident resolution) may throw, and they name the peer themselves.
        PeerDirectory.Clear();
        try
        {
            PeerDirectory.Configure([new PeerRecord { Name = "grpc-only", GrpcEndpoint = "http://grpc-only:5299" }]);
            var config = Config(peer: "grpc-only");

            var resolved = GrpcPeerResolver.Resolve(config);

            Assert.Equal("http://grpc-only:5299", resolved.GrpcAddress);
            Assert.Null(resolved.RestAddress);
            Assert.Equal("grpc-only", resolved.PeerName);
        }
        finally
        {
            PeerDirectory.Clear();
        }
    }
}
