using System.Runtime.CompilerServices;
using System.Text;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Connectors.Nats;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 019 (wave A): the acceptance test for <see cref="IDuplexTransport"/>/<see cref="DuplexTransports"/>,
/// mirroring <see cref="TransportRegistryTests"/>'s static-constructor pattern for the same reason — the
/// registries are process-global and permanent, so the fake kind is named distinctively and registered
/// exactly once.
///
/// <para>The load-bearing claim of the whole seam: a duplex kind's inbound half needs NO duplex-specific
/// code anywhere. <see cref="SubscriberCore_DrivesADuplexSessionIntoRowsWithNoDuplexSpecificPath"/> drives a
/// fake duplex session through the exact same <see cref="SubscriberCore"/> the message-transport family
/// uses, unmodified, and <see cref="Find_ResolvesInBothRegistriesViaCoRegistration"/> pins that
/// <see cref="DuplexTransports.Register"/> makes the kind visible to
/// <see cref="InboundTransports"/>-based consumers (<c>ConnectorGrain.ArmForKind</c>,
/// <c>ConnectorActor</c>, <see cref="SourceValidation"/>, <c>GET /api/transports</c>) with zero code in any
/// of them naming "flux".</para>
/// </summary>
public class DuplexTransportRegistryTests
{
    private const string FluxKind = "flux";

    /// <summary>Registered directly into <see cref="InboundTransports"/> (not through
    /// <see cref="DuplexTransports"/>) so a later test can prove the reverse-order collision: register into
    /// InboundTransports first, then try to register the SAME kind as a duplex transport.</summary>
    private const string CollisionKind = "flux-collision";

    private static readonly FluxTransport Flux = new(FluxKind);
    private static readonly FluxTransport CollisionPlain = new(CollisionKind);

    static DuplexTransportRegistryTests()
    {
        InboundTransports.Register(CollisionPlain);
        DuplexTransports.Register(Flux);
    }

    // ------------------------------------------------------------------
    // The fake duplex transport.
    // ------------------------------------------------------------------

    /// <summary>Rides the existing <c>connector.nats</c> slot for the same reason
    /// <c>TransportRegistryTests.FizzTransport</c> does — a real duplex transport (FIX, wave 019-E) gets its
    /// own contracts property; adding one here for a test fixture would be a contract change this wave does
    /// not make.</summary>
    private sealed class FluxTransport(string kind) : IDuplexTransport
    {
        public List<byte[]> Payloads { get; } = [];
        public List<string> Acked { get; } = [];
        public bool SessionReady { get; set; } = true;

        public string Kind => kind;

        public string FormatOf(SourceDefinition def) => FileFormats.JsonArray;

        public void Validate(SourceDefinition def, List<string> errors)
        {
            if (def.Connector?.Nats is null)
            {
                errors.Add($"kind '{kind}' requires connector.nats");
            }
        }

        // Pinned by IDuplexTransport's own doc comment: Open() delegates to OpenDuplex() so both entry
        // points return the same live object rather than two independently-connected ones.
        public IInboundSubscription Open(SourceDefinition def) => OpenDuplex(def);

        public IDuplexSession OpenDuplex(SourceDefinition def) => new Session(this);

        public TransportDescriptor Describe() => new()
        {
            Kind = kind,
            Label = "Flux",
            ConfigProperty = "nats",
            Duplex = true,
            Fields = [new TransportField { Key = "url", Label = "Server URL", Required = true }],
        };

        private sealed class Session(FluxTransport owner) : IDuplexSession
        {
            public bool IsReady => owner.SessionReady;

            public async IAsyncEnumerable<InboundMessage> SubscribeAsync([EnumeratorCancellation] CancellationToken ct)
            {
                foreach (var payload in owner.Payloads)
                {
                    yield return new InboundMessage("flux.subject", payload, () =>
                    {
                        owner.Acked.Add(Encoding.UTF8.GetString(payload));
                        return Task.CompletedTask;
                    });
                }

                // Same discipline as FizzTransport's Subscription: block rather than complete, so
                // SubscriberCore's clean-disconnect reconnect doesn't re-yield the fixed list forever.
                await Task.Delay(Timeout.Infinite, ct);
            }

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;

            /// <summary>Fakes a partial-failure delivery: every row after the first is reported as failed,
            /// never thrown — pinning IDuplexSession's "MUST NOT throw for an ordinary delivery failure"
            /// contract.</summary>
            public Task<DuplexSendOutcome> SendAsync(IReadOnlyList<Dictionary<string, object?>> rows, CancellationToken ct)
            {
                if (!owner.SessionReady)
                {
                    return Task.FromResult(new DuplexSendOutcome(0, rows.Count,
                        [.. rows.Select(r => new DuplexSendFailure(r.GetValueOrDefault("id")?.ToString(), null, "session not ready"))]));
                }

                var failures = new List<DuplexSendFailure>();
                var sent = 0;
                foreach (var row in rows)
                {
                    if (sent == 0)
                    {
                        sent++;
                    }
                    else
                    {
                        failures.Add(new DuplexSendFailure(row.GetValueOrDefault("id")?.ToString(), sent + failures.Count, "simulated rejection"));
                    }
                }

                return Task.FromResult(new DuplexSendOutcome(sent, failures.Count, failures));
            }
        }
    }

    private static SourceDefinition FluxSource() => new()
    {
        Name = "fx",
        Kind = FluxKind,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("qty", FieldType.Long)],
        Connector = new ConnectorConfig
        {
            Nats = new NatsSubConfig { Url = "flux://localhost", Subject = "flux.subject" },
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                Fields =
                [
                    new FieldMapEntry { Field = new FieldDef("id", FieldType.String) },
                    new FieldMapEntry { Field = new FieldDef("qty", FieldType.Long) },
                ],
            },
        },
    };

    // ------------------------------------------------------------------
    // Registry — co-registration and atomicity.
    // ------------------------------------------------------------------

    [Fact]
    public void Find_ResolvesInBothRegistriesViaCoRegistration()
    {
        Assert.Same(Flux, DuplexTransports.Find(FluxKind));

        // The load-bearing part: registering through DuplexTransports made the SAME object visible to
        // InboundTransports too, with no line anywhere naming "flux".
        Assert.Same(Flux, InboundTransports.Find(FluxKind));

        Assert.Null(DuplexTransports.Find("no-such-kind"));
        Assert.Null(DuplexTransports.Find(null));
    }

    [Fact]
    public void Register_RejectsADuplicateDuplexKind_AndLeavesBothRegistriesUnchanged()
    {
        var duplexBefore = DuplexTransports.Kinds.ToList();
        var inboundBefore = InboundTransports.Kinds.ToList();

        Assert.Throws<InvalidOperationException>(() => DuplexTransports.Register(new FluxTransport(FluxKind)));

        Assert.Equal(duplexBefore, DuplexTransports.Kinds);
        Assert.Equal(inboundBefore, InboundTransports.Kinds);
        Assert.Same(Flux, DuplexTransports.Find(FluxKind)); // still the original instance, not replaced
    }

    [Fact]
    public void Register_ThrowsWhenTheKindIsAlreadyInInboundTransports_AndLeavesDuplexTransportsUntouched()
    {
        // CollisionKind was registered directly into InboundTransports (not via DuplexTransports) by this
        // class's static constructor — the reverse order from the happy path above.
        var duplexBefore = DuplexTransports.Kinds.ToList();

        Assert.Throws<InvalidOperationException>(() => DuplexTransports.Register(new FluxTransport(CollisionKind)));

        Assert.Equal(duplexBefore, DuplexTransports.Kinds);
        Assert.Null(DuplexTransports.Find(CollisionKind));
        Assert.Same(CollisionPlain, InboundTransports.Find(CollisionKind)); // untouched by the failed attempt
    }

    // ------------------------------------------------------------------
    // SourceValidation — reached without SourceValidation naming "flux".
    // ------------------------------------------------------------------

    [Fact]
    public void SourceValidation_AcceptsTheDuplexKindAndDelegatesItsRules()
    {
        Assert.Empty(SourceValidation.Validate(FluxSource()));

        var broken = FluxSource();
        broken.Connector!.Nats = null;
        Assert.Contains($"kind '{FluxKind}' requires connector.nats", SourceValidation.Validate(broken));
    }

    [Fact]
    public void SourceValidation_UnknownKindMessageIsUnchangedForAGenuinelyUnknownKind()
    {
        var def = FluxSource();
        def.Kind = "not-a-real-kind";
        var errors = SourceValidation.Validate(def);
        Assert.Contains(errors, e => e.Contains("is not recognized"));
        // The known-kinds list quoted in the message includes the duplex kind (via the InboundTransports
        // co-registration) exactly once — proof SourceValidation.IsKnownKind needed no wave-019 edit.
        var message = Assert.Single(errors, e => e.Contains("is not recognized"));
        Assert.Contains(FluxKind, message);
    }

    // ------------------------------------------------------------------
    // SubscriberCore — the load-bearing claim: no duplex-specific code path.
    // ------------------------------------------------------------------

    [Fact]
    public async Task SubscriberCore_DrivesADuplexSessionIntoRowsWithNoDuplexSpecificPath()
    {
        var def = FluxSource();
        Flux.Payloads.Clear();
        Flux.Acked.Clear();
        Flux.Payloads.Add(Encoding.UTF8.GetBytes("""[{"id":"a","qty":"7"}]"""));

        var emitted = new List<Dictionary<string, object?>>();
        using var cts = new CancellationTokenSource();

        // Constructed exactly like TransportRegistryTests' SubscriberCore test, with Flux passed as the
        // plain IInboundTransport it also is (IDuplexTransport : IInboundTransport) — SubscriberCore
        // neither knows nor needs to know it is duplex.
        var core = new SubscriberCore(
            def, Flux, new DedupTracker([]),
            onRows: (rows, _) =>
            {
                emitted.AddRange(rows);
                cts.Cancel();
                return Task.CompletedTask;
            },
            onStatus: (_, _) => { });

        await core.RunAsync(cts.Token);

        var row = Assert.Single(emitted);
        Assert.Equal("a", row["id"]);
        Assert.Equal(7L, row["qty"]);          // coerced through the shared path
        Assert.Equal("fx", row["_source"]);    // stamped by the shared path
        Assert.True(row.ContainsKey("_ts"));
        Assert.Equal(["""[{"id":"a","qty":"7"}]"""], Flux.Acked);
    }

    // ------------------------------------------------------------------
    // SendAsync — never throws for an ordinary delivery failure.
    // ------------------------------------------------------------------

    [Fact]
    public async Task SendAsync_ReportsPartialFailureRatherThanThrowing()
    {
        var session = Flux.OpenDuplex(FluxSource());
        Assert.True(session.IsReady);

        var rows = new List<Dictionary<string, object?>>
        {
            new() { ["id"] = "ord-1" },
            new() { ["id"] = "ord-2" },
            new() { ["id"] = "ord-3" },
        };

        var outcome = await session.SendAsync(rows, CancellationToken.None);

        Assert.Equal(1, outcome.Sent);
        Assert.Equal(2, outcome.Failed);
        Assert.Equal(2, outcome.Failures.Count);
        Assert.All(outcome.Failures, f => Assert.False(string.IsNullOrEmpty(f.Reason)));
        Assert.Contains(outcome.Failures, f => f.CorrelationId == "ord-2");
        Assert.Contains(outcome.Failures, f => f.CorrelationId == "ord-3");

        await session.DisposeAsync();
    }

    [Fact]
    public async Task SendAsync_WhenSessionNotReady_FailsEveryRowWithoutThrowing()
    {
        var notReady = new FluxTransport("flux-not-ready") { SessionReady = false };
        var session = notReady.OpenDuplex(FluxSource());
        Assert.False(session.IsReady);

        var outcome = await session.SendAsync([new Dictionary<string, object?> { ["id"] = "ord-1" }], CancellationToken.None);

        Assert.Equal(0, outcome.Sent);
        Assert.Equal(1, outcome.Failed);
        Assert.Equal("session not ready", Assert.Single(outcome.Failures).Reason);

        await session.DisposeAsync();
    }

    // ------------------------------------------------------------------
    // GET /api/transports catalog shape — undisturbed, and the duplex flag round-trips.
    // ------------------------------------------------------------------

    [Fact]
    public void TheCatalogPlacesTheDuplexDescriptorInInboundOnlyAndRoundTripsTheFlag()
    {
        // Same projection TransportsEndpoints.MapTransportsEndpoints builds for GET /api/transports.
        var inbound = InboundTransports.Kinds.Select(k => InboundTransports.Find(k)!.Describe()).ToList();
        var outbound = SinkTransports.Kinds.Select(k => SinkTransports.Find(k)!.Describe()).ToList();

        var fluxDescriptor = Assert.Single(inbound, d => d.Kind == FluxKind);
        Assert.True(fluxDescriptor.Duplex);
        Assert.DoesNotContain(outbound, d => d.Kind == FluxKind);

        // TransportRegistryTests.TheCatalogIncludesEveryRegisteredTransportInBothDirections already pins
        // that nats/fizz/fizz-sink are still present and every entry's shape is well-formed; this test only
        // needs to show the duplex entry did not break that shape or leak into Outbound.
        Assert.Contains(inbound, d => d.Kind == SourceKinds.Nats);
        Assert.False(string.IsNullOrWhiteSpace(fluxDescriptor.Label));
        Assert.False(string.IsNullOrWhiteSpace(fluxDescriptor.ConfigProperty));
        Assert.NotEmpty(fluxDescriptor.Fields);

        // Every OTHER descriptor defaults Duplex to false — additive, no pre-019 descriptor changed shape.
        // (CollisionKind is excluded too: it is a FluxTransport registered directly into InboundTransports
        // by this class's static constructor for the cross-registry collision test above, so its own
        // Describe() also reports Duplex = true — that is a property of the fake transport, not evidence
        // against the "defaults to false" claim, which is about descriptors this wave did not touch.)
        Assert.All(inbound.Concat(outbound).Where(d => d.Kind is not (FluxKind or CollisionKind)), d => Assert.False(d.Duplex));
    }
}
