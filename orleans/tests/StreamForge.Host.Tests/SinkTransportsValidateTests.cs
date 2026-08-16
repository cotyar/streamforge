using StreamForge.Abstractions;
using StreamForge.AppCore.Sinks;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Wishlist item 13 gap 3 ("ISinkTransport.Validate is not wired to any REST call site"): the fix adds
/// <see cref="SinkTransports.Validate"/> — the missing call site — and wires it into
/// TablesEndpoints.cs/PipelinesEndpoints.cs's create/update handlers. This repo has no HTTP-level test
/// harness (see SourcesEndpointsLogicTests.cs's own class doc), so this file covers
/// <see cref="SinkTransports.Validate"/> itself directly, pure and infrastructure-free — the same
/// "instantiate/call the pure logic directly" pattern HttpSinkTransportTests.cs already uses for
/// <see cref="HttpSinkTransport"/>'s own Validate.
/// </summary>
public class SinkTransportsValidateTests
{
    [Fact]
    public void AnHttpSinkWithABlankUrl_ProducesAnErrorNamingTheField()
    {
        var sinks = new List<SinkSpec> { new() { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "" } } };
        var errors = new List<string>();

        SinkTransports.Validate(sinks, errors);

        var error = Assert.Single(errors);
        Assert.Contains("url", error, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AnHttpSinkWithAUrl_ProducesNoErrors()
    {
        var sinks = new List<SinkSpec> { new() { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "http://x/events" } } };
        var errors = new List<string>();

        SinkTransports.Validate(sinks, errors);

        Assert.Empty(errors);
    }

    [Theory]
    [InlineData(SinkKinds.Nats)]
    [InlineData(SinkKinds.File)]
    public void OlderSinkKindsStillUsingTheDefaultNoOpValidate_NeverProduceAnError(string kind)
    {
        // Nats/File never implemented Validate (they still use ISinkTransport's default no-op) — wiring
        // the call site must not retroactively make either kind stricter. A blank/missing config for
        // either kind produces zero errors, exactly as if this method had never been called.
        var sinks = new List<SinkSpec>
        {
            new() { Kind = kind, Nats = new NatsPubConfig(), File = new FileSinkConfig() },
        };
        var errors = new List<string>();

        SinkTransports.Validate(sinks, errors);

        Assert.Empty(errors);
    }

    [Fact]
    public void AnUnregisteredKind_IsSkippedRatherThanRejected()
    {
        // Find() already answers null for an unregistered kind, and SinkSelection.Active already treats
        // that the same "never selected, never runs" way IsConfigured == false does — this method does
        // not widen that into a NEW rejection (see its own doc comment).
        var sinks = new List<SinkSpec> { new() { Kind = "no-such-kind" } };
        var errors = new List<string>();

        SinkTransports.Validate(sinks, errors);

        Assert.Empty(errors);
    }

    [Fact]
    public void OneBadSinkAmongGoodOnes_OnlyTheBadOneIsReported()
    {
        var sinks = new List<SinkSpec>
        {
            new() { Kind = SinkKinds.Nats, Nats = new NatsPubConfig { Url = "nats://x", Subject = "s" } },
            new() { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "" } },
            new() { Kind = SinkKinds.File, File = new FileSinkConfig { Path = "/tmp/x" } },
        };
        var errors = new List<string>();

        SinkTransports.Validate(sinks, errors);

        var error = Assert.Single(errors);
        Assert.Contains("sinks[1]", error);
        Assert.Contains("http", error);
    }

    [Fact]
    public void ANamedSink_IsLabeledByNameNotIndex()
    {
        var sinks = new List<SinkSpec> { new() { Kind = SinkKinds.Http, Name = "loop-back", Http = new HttpSinkConfig { Url = "" } } };
        var errors = new List<string>();

        SinkTransports.Validate(sinks, errors);

        var error = Assert.Single(errors);
        Assert.StartsWith("sink 'loop-back':", error);
    }

    [Fact]
    public void MultipleBrokenSinks_EachGetsItsOwnLabeledError()
    {
        var sinks = new List<SinkSpec>
        {
            new() { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "" } },
            new() { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "   " } },
        };
        var errors = new List<string>();

        SinkTransports.Validate(sinks, errors);

        Assert.Equal(2, errors.Count);
        Assert.Contains("sinks[0]", errors[0]);
        Assert.Contains("sinks[1]", errors[1]);
    }

    [Fact]
    public void AnEmptySinkList_ProducesNoErrors()
    {
        var errors = new List<string>();

        SinkTransports.Validate([], errors);

        Assert.Empty(errors);
    }
}
