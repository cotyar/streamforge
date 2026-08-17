using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Connectors.Fix.Tests;

/// <summary>Validate/Describe/FormatOf — everything <see cref="FixInboundTransport"/> does that needs no
/// socket at all. The acceptance test (a real QuickFIX/n acceptor) lives in <c>FixAcceptanceTests</c>.</summary>
public class FixInboundTransportTests
{
    private static readonly FixInboundTransport Transport = new();

    // ------------------------------------------------------------------
    // Validate
    // ------------------------------------------------------------------

    [Fact]
    public void ValidateAcceptsAGoodConfig()
    {
        var errors = new List<string>();
        Transport.Validate(FixTestSupport.FixSource(), errors);
        Assert.Empty(errors);
    }

    [Fact]
    public void ValidateRequiresConnectorFix()
    {
        var def = FixTestSupport.FixSource();
        def.Connector!.Fix = null;

        var errors = new List<string>();
        Transport.Validate(def, errors);

        Assert.Contains("kind 'fix' requires connector.fix", errors);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void ValidateNamesAMissingHost(string host)
    {
        var config = FixTestSupport.ValidConfig();
        config.Host = host;

        var errors = new List<string>();
        Transport.Validate(FixTestSupport.FixSource(config), errors);

        Assert.Contains("connector.fix.host is required", errors);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(65536)]
    public void ValidateNamesAnInvalidPort(int port)
    {
        var config = FixTestSupport.ValidConfig();
        config.Port = port;

        var errors = new List<string>();
        Transport.Validate(FixTestSupport.FixSource(config), errors);

        Assert.Contains("connector.fix.port must be between 1 and 65535", errors);
    }

    [Fact]
    public void ValidateNamesAMissingSenderCompId()
    {
        var config = FixTestSupport.ValidConfig();
        config.SenderCompId = "";

        var errors = new List<string>();
        Transport.Validate(FixTestSupport.FixSource(config), errors);

        Assert.Contains("connector.fix.senderCompId is required", errors);
    }

    [Fact]
    public void ValidateNamesAMissingTargetCompId()
    {
        var config = FixTestSupport.ValidConfig();
        config.TargetCompId = "";

        var errors = new List<string>();
        Transport.Validate(FixTestSupport.FixSource(config), errors);

        Assert.Contains("connector.fix.targetCompId is required", errors);
    }

    [Fact]
    public void ValidateNamesAnUnrecognizedBeginString()
    {
        var config = FixTestSupport.ValidConfig();
        config.BeginString = "FIX.9.9";

        var errors = new List<string>();
        Transport.Validate(FixTestSupport.FixSource(config), errors);

        Assert.Contains(errors, e => e.Contains("connector.fix.beginString 'FIX.9.9' is not recognized", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void ValidateNamesANonPositiveHeartBtInt(int heartBtInt)
    {
        var config = FixTestSupport.ValidConfig();
        config.HeartBtIntSeconds = heartBtInt;

        var errors = new List<string>();
        Transport.Validate(FixTestSupport.FixSource(config), errors);

        Assert.Contains("connector.fix.heartBtIntSeconds must be > 0", errors);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ValidateNamesANonPositiveQueueCapacity(int capacity)
    {
        var config = FixTestSupport.ValidConfig();
        config.QueueCapacity = capacity;

        var errors = new List<string>();
        Transport.Validate(FixTestSupport.FixSource(config), errors);

        Assert.Contains("connector.fix.queueCapacity must be > 0", errors);
    }

    // ------------------------------------------------------------------
    // FormatOf
    // ------------------------------------------------------------------

    [Fact]
    public void FormatOfIsAlwaysFixRegardlessOfConfig()
    {
        Assert.Equal(FileFormats.Fix, Transport.FormatOf(FixTestSupport.FixSource()));

        var oddConfig = FixTestSupport.ValidConfig();
        oddConfig.BeginString = "FIX.4.2";
        Assert.Equal(FileFormats.Fix, Transport.FormatOf(FixTestSupport.FixSource(oddConfig)));
    }

    // ------------------------------------------------------------------
    // Open — construct only, never connect
    // ------------------------------------------------------------------

    [Fact]
    public void OpenNeverInvokesTheSourceFactoryEagerlyBeyondConstruction()
    {
        var invoked = 0;
        var transport = new FixInboundTransport(() =>
        {
            invoked++;
            return new FakeFixMessageSource([]);
        });

        var subscription = transport.Open(FixTestSupport.FixSource());

        Assert.Equal(1, invoked); // the factory itself is invoked to build the seam...
        Assert.NotNull(subscription); // ...but nothing here has connected: FakeFixMessageSource never dials anything.
    }

    [Fact]
    public void OpenThrowsForAKindMismatchedDefinition()
    {
        var def = FixTestSupport.FixSource();
        def.Connector!.Fix = null;

        Assert.Throws<InvalidOperationException>(() => Transport.Open(def));
    }

    // ------------------------------------------------------------------
    // Describe
    // ------------------------------------------------------------------

    [Fact]
    public void DescribeShapeMatchesThePinnedContract()
    {
        var descriptor = Transport.Describe();

        Assert.Equal(SourceKinds.Fix, descriptor.Kind);
        Assert.Equal("fix", descriptor.ConfigProperty);
        Assert.False(descriptor.Polled);
        Assert.True(descriptor.Mapping);
        Assert.False(descriptor.CanProbe);
    }

    [Fact]
    public void DescribeTypesThePasswordFieldAsSecret()
    {
        var field = Assert.Single(Transport.Describe().Fields, f => f.Key == "password");
        Assert.Equal(TransportFieldTypes.Secret, field.Type);
    }

    [Fact]
    public void DescribeTypesOnLogonAsText()
    {
        var field = Assert.Single(Transport.Describe().Fields, f => f.Key == "onLogon");
        Assert.Equal(TransportFieldTypes.Text, field.Type);
    }

    [Fact]
    public void EverySecretFieldMatchesAnActualSecretProperty()
    {
        var declared = typeof(FixSourceConfig).GetProperties()
            .Where(p => p.IsDefined(typeof(SecretAttribute), inherit: true))
            .Select(p => char.ToLowerInvariant(p.Name[0]) + p.Name[1..])
            .ToHashSet(StringComparer.Ordinal);

        var described = Transport.Describe().Fields
            .Where(f => f.Type == TransportFieldTypes.Secret)
            .Select(f => f.Key)
            .ToHashSet(StringComparer.Ordinal);

        Assert.Equal(declared, described);
    }

    [Fact]
    public void EveryDescriptorFieldNamesARealPropertyOfFixSourceConfig()
    {
        var properties = typeof(FixSourceConfig).GetProperties()
            .Select(p => char.ToLowerInvariant(p.Name[0]) + p.Name[1..])
            .ToHashSet(StringComparer.Ordinal);

        Assert.All(Transport.Describe().Fields, f => Assert.Contains(f.Key, properties));
    }

    [Fact]
    public void TheDescriptorMeetsTheSameShapeRulesTheCatalogPinsForEveryOtherTransport()
    {
        var descriptor = Transport.Describe();

        Assert.False(string.IsNullOrWhiteSpace(descriptor.Label));
        Assert.False(string.IsNullOrWhiteSpace(descriptor.Help));
        Assert.NotEmpty(descriptor.Fields);

        var groups = descriptor.Groups.Select(g => g.Key).ToHashSet(StringComparer.Ordinal);
        Assert.All(descriptor.Fields, f => Assert.True(f.Group is null || groups.Contains(f.Group)));
        Assert.All(descriptor.Fields, f => Assert.Equal(f.Type == TransportFieldTypes.Select, f.Options is { Count: > 0 }));
    }

    [Fact]
    public void TheHelpTextStatesTheDropCopyCeilingInPlainWords()
    {
        Assert.Contains("drop-copy", Transport.Describe().Help!, StringComparison.Ordinal);
        Assert.Contains("worthless", Transport.Describe().Help!, StringComparison.Ordinal);
    }

    private sealed class FakeFixMessageSource(IReadOnlyList<FixInboundMessage> messages) : IFixMessageSource
    {
        public async IAsyncEnumerable<FixInboundMessage> SubscribeAsync(
            FixSourceConfig config, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            foreach (var m in messages)
            {
                yield return m;
            }

            await Task.Delay(Timeout.Infinite, ct);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
