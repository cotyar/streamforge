using StreamsForge.Abstractions;
using StreamsForge.AppCore.Sinks;
using StreamsForge.AppCore.Transports;
using Xunit;

namespace StreamsForge.Connectors.Fix.Tests;

/// <summary>
/// Registration. Mirrors <c>DatabaseConnectorsTests</c>'s shape: registration is process-global and
/// permanent, so <see cref="FixConnectors.RegisterAll"/> is called once from the static constructor and
/// its idempotence is tested rather than worked around.
///
/// <para>Plan 018-C's regression requirement: <c>DatabaseConnectorsTests</c> asserts the exact set of
/// registered POLLED kinds, and must stay green UNMODIFIED — FIX registers into
/// <see cref="InboundTransports"/>, not <c>PolledTransports</c>, so that assertion is unaffected. This
/// class confirms that placement rather than assuming it.</para>
/// </summary>
public class FixConnectorsTests
{
    static FixConnectorsTests() => FixConnectors.RegisterAll();

    [Fact]
    public void RegisterAllPutsFixInInboundTransports()
    {
        Assert.NotNull(InboundTransports.Find(SourceKinds.Fix));
        Assert.IsType<FixInboundTransport>(InboundTransports.Find(SourceKinds.Fix));
    }

    [Fact]
    public void FixIsNotMistakenForAPolledOrSinkKind()
    {
        // FIX is a persistent subscription, not a poll schedule, and it is ingress-only — there is no FIX
        // sink at all (order entry is plan 019, a different plan).
        Assert.Null(PolledTransports.Find(SourceKinds.Fix));
        Assert.Null(SinkTransports.Find(SourceKinds.Fix));
    }

    [Fact]
    public void CallingItTwiceIsANoOpRatherThanTheDuplicateKindException()
    {
        // Two hosts in one test process, or a re-entrant startup, must not take the process down for a
        // reason that has nothing to do with the operator.
        FixConnectors.RegisterAll();
        FixConnectors.RegisterAll();

        Assert.NotNull(InboundTransports.Find(SourceKinds.Fix));
    }

    [Fact]
    public void TheKindIsTheContractsOwnConstant() => Assert.Equal("fix", SourceKinds.Fix);
}
