using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Connectors.Fix.Tests;

/// <summary>
/// Plan 019 wave E: registration, <c>Validate</c>/<c>Describe</c>/<c>FormatOf</c>, and the
/// publish-on-open/withdraw-on-dispose contract <see cref="DuplexSessions"/>'s own doc comment states every
/// duplex transport owes — everything <see cref="FixDuplexTransport"/> does that needs no socket at all.
/// The live acceptance test lives in <see cref="FixDuplexAcceptanceTests"/>.
/// </summary>
public class FixDuplexTransportTests
{
    static FixDuplexTransportTests() => FixConnectors.RegisterAll();

    // ------------------------------------------------------------------
    // Registration — fix-duplex lands in BOTH registries; fix itself is untouched
    // ------------------------------------------------------------------

    [Fact]
    public void RegisterAllPutsFixDuplexInInboundTransportsAndDuplexTransports()
    {
        Assert.IsType<FixDuplexTransport>(InboundTransports.Find(SourceKinds.FixDuplex));
        Assert.IsType<FixDuplexTransport>(DuplexTransports.Find(SourceKinds.FixDuplex));
    }

    [Fact]
    public void FixItselfIsUntouchedByThisWave()
    {
        // Plan 018's own frozen assertion (FixConnectorsTests.RegisterAllPutsFixInInboundTransports)
        // already pins InboundTransports.Find(SourceKinds.Fix) as a FixInboundTransport; this test pins
        // the other half of the claim this wave makes: 'fix' never became a duplex kind.
        Assert.IsType<FixInboundTransport>(InboundTransports.Find(SourceKinds.Fix));
        Assert.Null(DuplexTransports.Find(SourceKinds.Fix));
    }

    [Fact]
    public void CallingRegisterAllTwiceStaysANoOp()
    {
        FixConnectors.RegisterAll();
        FixConnectors.RegisterAll();

        Assert.NotNull(DuplexTransports.Find(SourceKinds.FixDuplex));
    }

    [Fact]
    public void TheKindIsTheContractsOwnConstant() => Assert.Equal("fix-duplex", SourceKinds.FixDuplex);

    // ------------------------------------------------------------------
    // FormatOf / Describe
    // ------------------------------------------------------------------

    [Fact]
    public void FormatOfIsFix()
    {
        var transport = new FixDuplexTransport();
        Assert.Equal(FileFormats.Fix, transport.FormatOf(FixDuplexTestSupport.FixDuplexSource()));
    }

    [Fact]
    public void DescribeDeclaresTheDuplexFlag()
    {
        var descriptor = new FixDuplexTransport().Describe();

        Assert.Equal(SourceKinds.FixDuplex, descriptor.Kind);
        Assert.Equal("fix", descriptor.ConfigProperty);
        Assert.True(descriptor.Duplex);
        Assert.False(descriptor.Polled);
        Assert.True(descriptor.Mapping);
    }

    [Fact]
    public void EveryDescriptorFieldNamesARealPropertyOfFixSourceConfig()
    {
        var properties = typeof(FixSourceConfig).GetProperties()
            .Select(p => char.ToLowerInvariant(p.Name[0]) + p.Name[1..])
            .ToHashSet(StringComparer.Ordinal);

        Assert.All(new FixDuplexTransport().Describe().Fields, f => Assert.Contains(f.Key, properties));
    }

    // ------------------------------------------------------------------
    // Validate — same rules as the receive-only kind (wave 019-G adds the mandatory-persistence rule later)
    // ------------------------------------------------------------------

    [Fact]
    public void ValidateAcceptsAGoodConfig()
    {
        // Plan 019 wave G (D5): "a good config" for fix-duplex now means a durable, non-resetting store —
        // FixDuplexTestSupport's own default config (FixTestSupport.ValidConfig(), StorePath empty,
        // ResetOnLogon true) is the market-data-shaped default and is deliberately no longer "good" here;
        // see FixDuplexPersistenceValidationTests for the full coverage of what changed and why.
        var config = FixTestSupport.ValidConfig();
        config.StorePath = "/data/fix-duplex/eurusd.store";
        config.ResetOnLogon = false;

        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(config), errors);
        Assert.Empty(errors);
    }

    [Fact]
    public void ValidateRequiresConnectorFix()
    {
        var def = FixDuplexTestSupport.FixDuplexSource();
        def.Connector!.Fix = null;

        var errors = new List<string>();
        new FixDuplexTransport().Validate(def, errors);

        Assert.Contains("kind 'fix-duplex' requires connector.fix", errors);
    }

    [Fact]
    public void ValidateNamesAMissingHost()
    {
        var config = FixTestSupport.ValidConfig();
        config.Host = "";

        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(config), errors);

        Assert.Contains("connector.fix.host is required", errors);
    }

    [Fact]
    public void ValidateNowRequiresAStorePath()
    {
        // Plan 019 D5's mandatory-persistence rule landed in wave 019-G, which is this wave -- this test
        // used to be named ValidateDoesNotRequireAStorePath and asserted the opposite (Assert.Empty), with
        // a comment that named itself as wave G's job to supersede. It now asserts the rule it was always
        // going to gain: an in-memory store (StorePath empty, the FixSourceConfig default) is refused for
        // fix-duplex. See FixDuplexPersistenceValidationTests for the message wording and full coverage.
        var config = FixTestSupport.ValidConfig();
        config.StorePath = "";
        config.ResetOnLogon = true;

        var errors = new List<string>();
        new FixDuplexTransport().Validate(FixDuplexTestSupport.FixDuplexSource(config), errors);

        Assert.Contains(errors, e => e.Contains("connector.fix.storePath is required", StringComparison.Ordinal));
    }

    // ------------------------------------------------------------------
    // OpenDuplex / Open — publish on open, withdraw on dispose (DuplexSessions' own stated contract)
    // ------------------------------------------------------------------

    [Fact]
    public async Task OpenDuplexPublishesIntoDuplexSessionsAndDisposeWithdraws()
    {
        var transport = new FixDuplexTransport();
        var def = FixDuplexTestSupport.FixDuplexSource();
        def.Name = $"fx-duplex-open-{Guid.NewGuid():N}";

        var session = transport.OpenDuplex(def);
        try
        {
            Assert.Same(session, DuplexSessions.Find(def.Name));
        }
        finally
        {
            await session.DisposeAsync();
        }

        Assert.Null(DuplexSessions.Find(def.Name));
    }

    [Fact]
    public async Task OpenDelegatesToOpenDuplex()
    {
        var transport = new FixDuplexTransport();
        var def = FixDuplexTestSupport.FixDuplexSource();
        def.Name = $"fx-duplex-open2-{Guid.NewGuid():N}";

        var subscription = transport.Open(def);
        try
        {
            var session = Assert.IsAssignableFrom<IDuplexSession>(subscription);
            Assert.Same(session, DuplexSessions.Find(def.Name));
        }
        finally
        {
            await subscription.DisposeAsync();
        }
    }

    [Fact]
    public void OpenDuplexThrowsForAKindMismatchedDefinition()
    {
        var def = FixDuplexTestSupport.FixDuplexSource();
        def.Connector!.Fix = null;

        Assert.Throws<InvalidOperationException>(() => new FixDuplexTransport().OpenDuplex(def));
    }
}
