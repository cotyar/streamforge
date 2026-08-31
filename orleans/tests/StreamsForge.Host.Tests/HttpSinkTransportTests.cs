using StreamsForge.Abstractions;
using StreamsForge.AppCore.Config;
using StreamsForge.AppCore.Sinks;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Wishlist item 9(a): <see cref="HttpSinkTransport"/> — registration, eligibility, validation and the
/// console descriptor. <see cref="HttpSinkClientTests"/> covers the client's own publish behavior; this
/// file covers the transport wrapper <c>SinkTransports</c>/<c>SinkSelection</c>/the console actually see.
/// </summary>
public class HttpSinkTransportTests
{
    [Fact]
    public void IsRegisteredUnderTheHttpKind()
    {
        // Proves TRANSPORTS.md's "one line in SinkTransports.Registered" claim held for this kind too —
        // the one-line addition in ISinkTransport.cs is exercised here rather than asserted by reading it.
        var found = StreamsForge.AppCore.Sinks.SinkTransports.Find(SinkKinds.Http);
        Assert.NotNull(found);
        Assert.IsType<HttpSinkTransport>(found);
    }

    [Theory]
    [InlineData(SinkKinds.Http, true)]
    [InlineData("HTTP", true)]
    [InlineData("HtTp", true)]
    public void KindLookupIsCaseInsensitive(string kind, bool expectFound)
    {
        var found = StreamsForge.AppCore.Sinks.SinkTransports.Find(kind);
        Assert.Equal(expectFound, found is not null);
    }

    [Fact]
    public void IsConfigured_TrueOnlyWithANonBlankUrl()
    {
        var transport = new HttpSinkTransport();

        Assert.True(transport.IsConfigured(new SinkSpec { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "http://x/events" } }));
        Assert.False(transport.IsConfigured(new SinkSpec { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "" } }));
        Assert.False(transport.IsConfigured(new SinkSpec { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "   " } }));
        Assert.False(transport.IsConfigured(new SinkSpec { Kind = SinkKinds.Http, Http = null }));
    }

    [Fact]
    public void Active_ExcludesAHalfConfiguredHttpSink()
    {
        // SinkSelection.Active is what NatsPublisherService/NatsSinkPublisherService actually iterate —
        // a blank URL must not produce a client attempt, matching FileSinkTransport/NatsSinkTransport's
        // existing behavior for a half-filled sink (see ISinkTransport.IsConfigured's own doc).
        var sinks = new List<SinkSpec>
        {
            new() { Kind = SinkKinds.Http, Enabled = true, Http = new HttpSinkConfig() },
            new() { Kind = SinkKinds.Http, Enabled = true, Http = new HttpSinkConfig { Url = "http://x/events" } },
        };

        var active = StreamsForge.AppCore.Sinks.SinkSelection.Active(sinks);

        var single = Assert.Single(active);
        Assert.Equal("http://x/events", single.Http!.Url);
    }

    [Fact]
    public void Validate_RequiresTheUrl()
    {
        var transport = new HttpSinkTransport();
        var errors = new List<string>();

        transport.Validate(new SinkSpec { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "" } }, errors);

        Assert.Single(errors);
        Assert.Contains("url", errors[0], StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Validate_AcceptsANonBlankUrl()
    {
        var transport = new HttpSinkTransport();
        var errors = new List<string>();

        transport.Validate(new SinkSpec { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "http://x/events" } }, errors);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task Create_ReturnsAnHttpSinkClientBoundToTheSpec()
    {
        var transport = new HttpSinkTransport();
        var spec = new SinkSpec { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "http://x/{name}/events" } };

        await using var client = (HttpSinkClient)transport.Create(spec, "table", "orders", onFailure: null);

        Assert.Equal("http://x/orders/events", client.Url);
        Assert.Equal("orders", client.EntityName);
    }

    [Fact]
    public void Describe_MatchesTheConfigContainerAndCarriesTheMaxDepthField()
    {
        var descriptor = new HttpSinkTransport().Describe();

        Assert.Equal(SinkKinds.Http, descriptor.Kind);
        Assert.Equal("http", descriptor.ConfigProperty);
        Assert.Contains(descriptor.Fields, f => f is { Key: "url", Required: true });
        Assert.Contains(descriptor.Fields, f => f.Key == "maxDepth");
        Assert.Contains(descriptor.Fields, f => f is { Key: "headerValue", Type: StreamsForge.AppCore.Transports.TransportFieldTypes.Secret });
    }

    // ------------------------------------------------------------------
    // Secrets: [Secret] on HeaderValue is the entire masking story (SecretWalk finds it by reflection —
    // see SecretWalkTests.cs — so this is a NEW test extending coverage to the http kind rather than a
    // change to SecretsMasker, which this wave does not touch).
    // ------------------------------------------------------------------

    [Fact]
    public void SecretWalk_MasksHeaderValue_ButNotUrlOrHeaderName()
    {
        var stored = new List<SinkSpec>
        {
            new()
            {
                Kind = SinkKinds.Http,
                Http = new HttpSinkConfig { Url = "http://x/events", HeaderName = "X-SF-Ingest-Key", HeaderValue = "sfk_live_secret" },
            },
        };

        var masked = SecretsMasker.MaskSinks(stored);

        Assert.Equal(SourceKinds.SecretMask, masked[0].Http!.HeaderValue);
        Assert.Equal("http://x/events", masked[0].Http!.Url);
        Assert.Equal("X-SF-Ingest-Key", masked[0].Http!.HeaderName);
        Assert.True(SecretsMasker.HasMaskedSinkValues(masked));
        Assert.False(SecretsMasker.HasMaskedSinkValues(stored));
    }

    [Fact]
    public void SecretWalk_MergeRestoresTheStoredHeaderValue()
    {
        var stored = new List<SinkSpec>
        {
            new() { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "http://x/events", HeaderValue = "sfk_live_secret" } },
        };
        var incoming = SecretsMasker.MaskSinks(stored);

        var merged = SecretsMasker.MergeSinkSecrets(incoming, stored);

        Assert.Equal("sfk_live_secret", merged[0].Http!.HeaderValue);
    }

    [Fact]
    public void SecretWalk_LeavesAnUnsetHeaderValueAlone()
    {
        var stored = new List<SinkSpec> { new() { Kind = SinkKinds.Http, Http = new HttpSinkConfig { Url = "http://x/events" } } };

        var masked = SecretsMasker.MaskSinks(stored);

        Assert.Null(masked[0].Http!.HeaderValue);
    }
}
