using StreamsForge.Abstractions;
using StreamsForge.AppCore.Sinks;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 009 B2: unit tests for <see cref="SinkSelection"/> — the "which sinks are eligible" filter and
/// the "did the sink config change" signature both flavors' background publisher services share.
/// </summary>
public class SinkSelectionTests
{
    private static SinkSpec Nats(string url = "nats://localhost:4222", string subject = "sf.out", bool enabled = true) => new()
    {
        Kind = SinkKinds.Nats,
        Enabled = enabled,
        Nats = new NatsPubConfig { Url = url, Subject = subject },
    };

    [Fact]
    public void ActiveNats_ReturnsNothing_ForNullOrEmptyInput()
    {
        Assert.Empty(SinkSelection.ActiveNats(null));
        Assert.Empty(SinkSelection.ActiveNats([]));
    }

    [Fact]
    public void ActiveNats_ExcludesDisabledSinks()
    {
        var sinks = new List<SinkSpec> { Nats(enabled: false) };
        Assert.Empty(SinkSelection.ActiveNats(sinks));
    }

    [Fact]
    public void ActiveNats_ExcludesNonNatsKind()
    {
        var sinks = new List<SinkSpec> { new() { Kind = "not-nats", Enabled = true, Nats = new NatsPubConfig { Url = "x", Subject = "y" } } };
        Assert.Empty(SinkSelection.ActiveNats(sinks));
    }

    [Fact]
    public void ActiveNats_ExcludesHalfConfiguredSinks()
    {
        // A sink with Enabled=true but no Url/Subject yet is "not configured", not "configured and
        // broken" — nothing to attempt a connection to, so it must not show up as active (which would
        // otherwise immediately produce a spurious failure counter).
        var noUrl = new SinkSpec { Kind = SinkKinds.Nats, Enabled = true, Nats = new NatsPubConfig { Url = "", Subject = "sf.out" } };
        var noSubject = new SinkSpec { Kind = SinkKinds.Nats, Enabled = true, Nats = new NatsPubConfig { Url = "nats://x", Subject = "" } };
        var noNats = new SinkSpec { Kind = SinkKinds.Nats, Enabled = true, Nats = null };

        Assert.Empty(SinkSelection.ActiveNats([noUrl]));
        Assert.Empty(SinkSelection.ActiveNats([noSubject]));
        Assert.Empty(SinkSelection.ActiveNats([noNats]));
    }

    [Fact]
    public void ActiveNats_IncludesAFullyConfiguredEnabledSink()
    {
        var sinks = new List<SinkSpec> { Nats() };
        var active = SinkSelection.ActiveNats(sinks);
        Assert.Single(active);
    }

    [Fact]
    public void Signature_IsStableForEquivalentInput()
    {
        var a = SinkSelection.Signature([Nats(subject: "sf.a")]);
        var b = SinkSelection.Signature([Nats(subject: "sf.a")]);
        Assert.Equal(a, b);
    }

    [Fact]
    public void Signature_ChangesWhenSubjectChanges()
    {
        var a = SinkSelection.Signature([Nats(subject: "sf.a")]);
        var b = SinkSelection.Signature([Nats(subject: "sf.b")]);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Signature_ChangesWhenUrlChanges()
    {
        var a = SinkSelection.Signature([Nats(url: "nats://host-a:4222")]);
        var b = SinkSelection.Signature([Nats(url: "nats://host-b:4222")]);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Signature_ChangesWhenSinkCountChanges()
    {
        var one = SinkSelection.Signature([Nats(subject: "sf.a")]);
        var two = SinkSelection.Signature([Nats(subject: "sf.a"), Nats(subject: "sf.b")]);
        Assert.NotEqual(one, two);
    }
}
