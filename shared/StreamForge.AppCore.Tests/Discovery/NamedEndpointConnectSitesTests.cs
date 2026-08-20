using StreamForge.Abstractions;
using StreamForge.AppCore.Connectors.Grpc;
using StreamForge.AppCore.Discovery;
using StreamForge.AppCore.Nats;
using StreamForge.AppCore.Sinks;
using Xunit;

namespace StreamForge.AppCore.Tests.Discovery;

/// <summary>Plan 016 wave 6, track A — <c>@name</c> actually resolving at the connect sites this track
/// owns: <see cref="NatsConnectionSettings.Build"/> (the NATS source AND sink), <see cref="HttpSinkClient"/>,
/// and <see cref="GrpcPeerResolver"/>'s "Peer unset" branch. The database dialects and the FIX connector
/// have their own test files (<c>DbEndpointResolutionTests</c> in
/// <c>StreamForge.Connectors.Database.Tests</c>; <c>NamedEndpointResolutionTests</c> in
/// <c>StreamForge.Connectors.Fix.Tests</c>) since they live in different assemblies with their own test
/// hosts — no shared static state to race against these.
///
/// <para><see cref="NamedEndpoints"/> is process-wide (its own class doc says so, same as
/// <see cref="PeerDirectory"/>). This file shares xUnit collection <c>"StreamForge.Discovery.PeerDirectory"</c>
/// with <see cref="GrpcPeerResolverTests"/> and <c>Discovery/PeerProbeTests.cs</c> — not because every test
/// below touches <see cref="PeerDirectory"/> (most don't), but because <see cref="PeerSet_IgnoresANamedEndpointReference_EvenWhenUnresolvable"/>
/// below DOES configure <see cref="PeerDirectory"/>, and every class that mutates either process-wide
/// registry in this assembly has to share ONE collection so xUnit's default cross-class parallelism cannot
/// interleave a Clear/Configure from this file with one from those files. Every test calls
/// <see cref="NamedEndpoints.Clear"/> (and, where relevant, <see cref="PeerDirectory.Clear"/>) via a
/// try/finally, mirroring the pattern <c>GrpcPeerResolverTests</c> already established.</para></summary>
[Collection("StreamForge.Discovery.PeerDirectory")]
public class NamedEndpointConnectSitesTests
{
    // ------------------------------------------------------------------
    // NatsConnectionSettings.Build — the one place both the nats SOURCE (NatsClientMessageSource,
    // called fresh per (re)connect) and the nats SINK (NatsSinkClient's constructor, called on every
    // periodic client rebuild) turn NatsSubConfig.Url/NatsPubConfig.Url into a NatsOpts.
    // ------------------------------------------------------------------

    [Fact]
    public void Nats_LiteralUrl_PassesThroughUnchanged()
    {
        NamedEndpoints.Clear();
        try
        {
            var opts = NatsConnectionSettings.Build("nats://literal-host:4222", null, null, null, null, "test");

            Assert.Equal("nats://literal-host:4222", opts.Url);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void Nats_EmbeddedAtSign_IsNotTreatedAsAReference()
    {
        // "nats://user@host:4222" contains an '@' but is not ENTIRELY a reference — NamedEndpoints.IsReference
        // only recognizes a value that starts with '@', and this one starts with "nats://".
        NamedEndpoints.Clear();
        try
        {
            var opts = NatsConnectionSettings.Build("nats://user@host:4222", null, null, null, null, "test");

            Assert.Equal("nats://user@host:4222", opts.Url);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void Nats_KnownReference_ResolvesToTheConfiguredValue()
    {
        NamedEndpoints.Clear();
        try
        {
            NamedEndpoints.Configure([new("primary-broker", "nats://prod-broker:4222")]);

            var opts = NatsConnectionSettings.Build("@primary-broker", null, null, null, null, "test");

            Assert.Equal("nats://prod-broker:4222", opts.Url);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void Nats_UnknownReference_ThrowsTheResolversActionableMessage()
    {
        NamedEndpoints.Clear();
        try
        {
            var ex = Assert.Throws<InvalidOperationException>(
                () => NatsConnectionSettings.Build("@no-such-broker", null, null, null, null, "test"));

            Assert.Contains("no-such-broker", ex.Message);
            Assert.Contains("not configured here", ex.Message);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    // ------------------------------------------------------------------
    // HttpSinkClient — resolved in the constructor, before {name} template expansion. A sink client is
    // rebuilt on every SinkSelection.Signature teardown (config edit) AND on every periodic refresh sweep
    // (NatsPublisherService/NatsSinkPublisherService, ~15-30s), so constructing a fresh client every time
    // this test does is exactly "resolve at connect time, every connect".
    // ------------------------------------------------------------------

    [Fact]
    public void HttpSink_LiteralUrl_PassesThroughUnchanged()
    {
        NamedEndpoints.Clear();
        try
        {
            var client = new HttpSinkClient(new HttpSinkConfig { Url = "http://literal-host/ingest/{name}" }, "table", "trades");

            Assert.Equal("http://literal-host/ingest/trades", client.Url);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void HttpSink_KnownReference_ResolvesBeforeNameExpansion()
    {
        NamedEndpoints.Clear();
        try
        {
            NamedEndpoints.Configure([new("ingest-loop", "http://loop-host/api/sources/{name}/events")]);

            var client = new HttpSinkClient(new HttpSinkConfig { Url = "@ingest-loop" }, "table", "trades");

            Assert.Equal("http://loop-host/api/sources/trades/events", client.Url);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void HttpSink_UnknownReference_ThrowsFromTheConstructor()
    {
        NamedEndpoints.Clear();
        try
        {
            var ex = Assert.Throws<InvalidOperationException>(
                () => new HttpSinkClient(new HttpSinkConfig { Url = "@no-such-endpoint" }, "table", "trades"));

            Assert.Contains("no-such-endpoint", ex.Message);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    // ------------------------------------------------------------------
    // GrpcPeerResolver — "Peer unset" branch (byte-identical-to-pre-016 branch). See
    // GrpcPeerResolverTests for the pre-existing Peer-set-wins-outright coverage this file does not
    // duplicate.
    // ------------------------------------------------------------------

    private static GrpcSubConfig Config(string address = "", string? restAddress = null, string? peer = null) => new()
    {
        Address = address,
        RestAddress = restAddress,
        Peer = peer,
        EntityKey = "source:whatever",
    };

    [Fact]
    public void Grpc_PeerUnset_LiteralAddresses_PassThroughUnchanged()
    {
        NamedEndpoints.Clear();
        try
        {
            var config = Config(address: "http://literal-grpc:5299", restAddress: "http://literal-rest:5199");

            var resolved = GrpcPeerResolver.Resolve(config);

            Assert.Equal("http://literal-grpc:5299", resolved.GrpcAddress);
            Assert.Equal("http://literal-rest:5199", resolved.RestAddress);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void Grpc_PeerUnset_KnownReferencesOnBothAddressFields_Resolve()
    {
        NamedEndpoints.Clear();
        try
        {
            NamedEndpoints.Configure([
                new("prod-grpc", "http://prod-host:5299"),
                new("prod-rest", "http://prod-host:5199"),
            ]);

            var config = Config(address: "@prod-grpc", restAddress: "@prod-rest");

            var resolved = GrpcPeerResolver.Resolve(config);

            Assert.Equal("http://prod-host:5299", resolved.GrpcAddress);
            Assert.Equal("http://prod-host:5199", resolved.RestAddress);
            Assert.Null(resolved.PeerName);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void Grpc_PeerUnset_UnknownReference_ThrowsTheResolversActionableMessage()
    {
        NamedEndpoints.Clear();
        try
        {
            var config = Config(address: "@no-such-grpc-endpoint");

            var ex = Assert.Throws<InvalidOperationException>(() => GrpcPeerResolver.Resolve(config));

            Assert.Contains("no-such-grpc-endpoint", ex.Message);
            Assert.Contains("not configured here", ex.Message);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    [Fact]
    public void Grpc_PeerUnset_EmbeddedAtSignAddress_IsNotTreatedAsAReference()
    {
        NamedEndpoints.Clear();
        try
        {
            var config = Config(address: "http://user@literal-grpc:5299");

            var resolved = GrpcPeerResolver.Resolve(config);

            Assert.Equal("http://user@literal-grpc:5299", resolved.GrpcAddress);
        }
        finally
        {
            NamedEndpoints.Clear();
        }
    }

    /// <summary>The precedence rule this track chose, pinned as a test: <c>Peer</c> wins OUTRIGHT over
    /// both address fields (wave 5's own rule, restated in <c>GrpcSubConfig.Peer</c>'s doc comment) — it
    /// does not merely take priority over a LITERAL address, it means the address fields are never even
    /// looked at, so an <c>@name</c> reference sitting in <see cref="GrpcSubConfig.Address"/> alongside a
    /// set <see cref="GrpcSubConfig.Peer"/> is not resolved, not validated, and would not throw even if it
    /// named an endpoint this instance has no mapping for. Proven here by using a reference name
    /// ("@unmapped-anywhere") that is unmapped in BOTH registries: if <see cref="GrpcPeerResolver.Resolve"/>
    /// ever tried to run it through <see cref="NamedEndpoints.Resolve"/>, this test would throw instead of
    /// returning the peer's address.</summary>
    [Fact]
    public void PeerSet_IgnoresANamedEndpointReference_EvenWhenUnresolvable()
    {
        NamedEndpoints.Clear();
        PeerDirectory.Clear();
        try
        {
            PeerDirectory.Configure([new PeerRecord { Name = "prod", GrpcEndpoint = "http://prod-host:5299", RestEndpoint = "http://prod-host:5199" }]);
            var config = Config(address: "@unmapped-anywhere", restAddress: "@also-unmapped", peer: "prod");

            var resolved = GrpcPeerResolver.Resolve(config);

            Assert.Equal("http://prod-host:5299", resolved.GrpcAddress);
            Assert.Equal("http://prod-host:5199", resolved.RestAddress);
            Assert.Equal("prod", resolved.PeerName);
        }
        finally
        {
            NamedEndpoints.Clear();
            PeerDirectory.Clear();
        }
    }
}
