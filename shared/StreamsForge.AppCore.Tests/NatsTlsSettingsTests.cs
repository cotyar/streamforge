using NATS.Client.Core;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Nats;
using Xunit;

namespace StreamsForge.AppCore.Tests;

/// <summary>NATS TLS options: <see cref="NatsConnectionSettings.Build"/>'s optional trailing
/// <c>tls</c> parameter, turning a <see cref="NatsTlsConfig"/> into the underlying client's
/// <see cref="NatsTlsOpts"/>. See that method's own doc comment for the "additive, not a
/// replacement for the <c>tls://</c> URL scheme" contract these tests pin.</summary>
public class NatsTlsSettingsTests
{
    [Fact]
    public void NullTls_LeavesTlsOptsAtDefault()
    {
        var opts = NatsConnectionSettings.Build("nats://localhost:4222", null, null, null, null, "test", tls: null);

        var defaultTlsOpts = NatsOpts.Default.TlsOpts;
        Assert.Equal(defaultTlsOpts.Mode, opts.TlsOpts.Mode);
        Assert.Null(opts.TlsOpts.CaFile);
        Assert.Null(opts.TlsOpts.CertFile);
        Assert.Null(opts.TlsOpts.KeyFile);
        Assert.False(opts.TlsOpts.InsecureSkipVerify);
    }

    [Fact]
    public void CaFileOnly_SetsCaFileAndRequiresTls()
    {
        var tls = new NatsTlsConfig { CaFile = "/etc/streamsforge/nats-ca.pem" };

        var opts = NatsConnectionSettings.Build("nats://localhost:4222", null, null, null, null, "test", tls);

        Assert.Equal("/etc/streamsforge/nats-ca.pem", opts.TlsOpts.CaFile);
        Assert.Null(opts.TlsOpts.CertFile);
        Assert.Null(opts.TlsOpts.KeyFile);
        Assert.Equal(TlsMode.Require, opts.TlsOpts.Mode);
    }

    [Fact]
    public void AllThreePaths_AreAllSet()
    {
        var tls = new NatsTlsConfig
        {
            CaFile = "/etc/streamsforge/nats-ca.pem",
            CertFile = "/etc/streamsforge/nats-client.pem",
            KeyFile = "/etc/streamsforge/nats-client-key.pem",
        };

        var opts = NatsConnectionSettings.Build("nats://localhost:4222", null, null, null, null, "test", tls);

        Assert.Equal("/etc/streamsforge/nats-ca.pem", opts.TlsOpts.CaFile);
        Assert.Equal("/etc/streamsforge/nats-client.pem", opts.TlsOpts.CertFile);
        Assert.Equal("/etc/streamsforge/nats-client-key.pem", opts.TlsOpts.KeyFile);
        Assert.Equal(TlsMode.Require, opts.TlsOpts.Mode);
    }

    [Fact]
    public void InsecureSkipVerify_SetsFlagAndRequiresTls()
    {
        var tls = new NatsTlsConfig { InsecureSkipVerify = true };

        var opts = NatsConnectionSettings.Build("nats://localhost:4222", null, null, null, null, "test", tls);

        Assert.True(opts.TlsOpts.InsecureSkipVerify);
        Assert.Equal(TlsMode.Require, opts.TlsOpts.Mode);
    }

    [Fact]
    public void BlankStrings_AreNotSet_AndBehaveLikeNull()
    {
        // Every field blank/false — same as passing tls: null. Guards against a form round-tripping ""
        // (rather than omitting the field) silently forcing TLS on.
        var tls = new NatsTlsConfig { CaFile = "", CertFile = "  ", KeyFile = null, InsecureSkipVerify = false };

        var opts = NatsConnectionSettings.Build("nats://localhost:4222", null, null, null, null, "test", tls);

        var defaultTlsOpts = NatsOpts.Default.TlsOpts;
        Assert.Equal(defaultTlsOpts.Mode, opts.TlsOpts.Mode);
        Assert.Null(opts.TlsOpts.CaFile);
        Assert.Null(opts.TlsOpts.CertFile);
        Assert.False(opts.TlsOpts.InsecureSkipVerify);
    }
}
