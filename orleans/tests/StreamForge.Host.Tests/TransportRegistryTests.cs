using System.Runtime.CompilerServices;
using System.Text;
using StreamForge.Abstractions;
using StreamForge.Api;
using StreamForge.AppCore.Config;
using StreamForge.AppCore.Connectors.Polling;
using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Transports;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 010: the acceptance test for pluggable transports — a transport that this repo's own code has never
/// heard of is registered from a TEST, and the platform then validates it, masks its credentials, and drives
/// its messages into rows. Nothing in <c>SourceValidation</c>, <c>SecretsMasker</c>, <c>SubscriberCore</c> or
/// either connector driver mentions "fizz"; if any of them regains a hardcoded per-kind branch, these fail.
///
/// <para>That is the whole claim being made about extensibility, stated as a test rather than as a doc
/// comment: adding a transport costs one <see cref="IInboundTransport"/> implementation (plus one
/// <see cref="ISinkTransport"/> for the outbound direction) and one registration.</para>
///
/// <para>Registration is process-global and permanent, so the fake kinds here are named distinctively and
/// registered exactly once via the static constructor — re-registering the same kind is a programming error
/// the registry deliberately throws on, which <see cref="Register_RejectsADuplicateKind"/> pins.</para>
/// </summary>
public class TransportRegistryTests
{
    private const string FizzKind = "fizz";
    private const string FizzSinkKind = "fizz-sink";

    private static readonly FizzTransport Fizz = new();

    static TransportRegistryTests()
    {
        InboundTransports.Register(Fizz);
        SinkTransports.Register(new FizzSinkTransport());
    }

    // ------------------------------------------------------------------
    // The fake transport — what a real one would look like, minus a network.
    // ------------------------------------------------------------------

    /// <summary>Config for the fake transport. It is NOT a contracts type, which is the point: the walker
    /// and the drivers work off shape and attributes, not off a known list of types. It rides on the
    /// existing nats slot only because adding an [Id] to ConnectorConfig for a test fixture would be a
    /// contract change; a real transport gets its own property.</summary>
    private sealed class FizzTransport : IInboundTransport
    {
        public List<byte[]> Payloads { get; } = [];
        public List<string> Acked { get; } = [];
        public int OpenCount { get; private set; }
        public bool Validated { get; private set; }

        public string Kind => FizzKind;

        public string FormatOf(SourceDefinition def) => FileFormats.JsonArray;

        public void Validate(SourceDefinition def, List<string> errors)
        {
            Validated = true;
            if (def.Connector?.Nats is null)
            {
                errors.Add("kind 'fizz' requires connector.nats");
            }
        }

        public IInboundSubscription Open(SourceDefinition def)
        {
            OpenCount++;
            return new Subscription(this);
        }

        private sealed class Subscription(FizzTransport owner) : IInboundSubscription
        {
            public async IAsyncEnumerable<InboundMessage> SubscribeAsync([EnumeratorCancellation] CancellationToken ct)
            {
                foreach (var payload in owner.Payloads)
                {
                    yield return new InboundMessage("fizz.subject", payload, () =>
                    {
                        owner.Acked.Add(Encoding.UTF8.GetString(payload));
                        return Task.CompletedTask;
                    });
                }

                // Block (honoring ct) rather than completing: a clean end would trigger SubscriberCore's
                // documented immediate no-backoff reconnect and re-yield the same fixed list forever. Same
                // discipline NatsSubscriberCoreTests' fake uses, and for the same reason.
                await Task.Delay(Timeout.Infinite, ct);
            }

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    private sealed class FizzSinkTransport : ISinkTransport
    {
        public string Kind => FizzSinkKind;

        // Reuses the nats config slot for the same reason FizzTransport does — see its doc.
        public bool IsConfigured(SinkSpec spec) => !string.IsNullOrWhiteSpace(spec.Nats?.Url);

        public ISinkClient Create(SinkSpec spec, string entityKind, string entityName, Action<string, Exception>? onFailure) =>
            new FizzSinkClient(entityName);
    }

    private sealed class FizzSinkClient(string entityName) : ISinkClient
    {
        public string EntityName { get; } = entityName;
        public SinkPublishCounters Counters => new(0, 0, null, 0);
        public Task PublishAsync<T>(T payload, CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static SourceDefinition FizzSource() => new()
    {
        Name = "fz",
        Kind = FizzKind,
        Fields = [new FieldDef("id", FieldType.String), new FieldDef("qty", FieldType.Long)],
        Connector = new ConnectorConfig
        {
            Nats = new NatsSubConfig { Url = "fizz://localhost", Subject = "fizz.subject", Token = "s3cret" },
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
    // Registry
    // ------------------------------------------------------------------

    [Fact]
    public void Find_ResolvesARegisteredKindAndIgnoresTheDriverKinds()
    {
        Assert.Same(Fizz, InboundTransports.Find(FizzKind));
        Assert.NotNull(InboundTransports.Find(SourceKinds.Nats));

        // Kinds with drivers of their own must NOT resolve here, or both connector drivers would route
        // them into the subscriber path instead of their timer/grpc/ingest paths.
        Assert.Null(InboundTransports.Find(SourceKinds.Generator));
        Assert.Null(InboundTransports.Find(SourceKinds.Url));
        Assert.Null(InboundTransports.Find(SourceKinds.Grpc));
        Assert.Null(InboundTransports.Find(SourceKinds.Ingest));
        Assert.Null(InboundTransports.Find(null));
        Assert.Null(InboundTransports.Find("no-such-kind"));
    }

    [Fact]
    public void Register_RejectsADuplicateKind() =>
        Assert.Throws<InvalidOperationException>(() => InboundTransports.Register(new FizzTransport()));

    // ------------------------------------------------------------------
    // Validation — reached without SourceValidation naming the kind
    // ------------------------------------------------------------------

    [Fact]
    public void SourceValidation_AcceptsARegisteredKindAndDelegatesItsRules()
    {
        Assert.Empty(SourceValidation.Validate(FizzSource()));
        Assert.True(Fizz.Validated);

        var broken = FizzSource();
        broken.Connector!.Nats = null;
        Assert.Contains("kind 'fizz' requires connector.nats", SourceValidation.Validate(broken));
    }

    [Fact]
    public void SourceValidation_StillRejectsAnUnregisteredKind()
    {
        var def = FizzSource();
        def.Kind = "buzz";
        Assert.Contains(SourceValidation.Validate(def), e => e.Contains("is not recognized"));
    }

    [Fact]
    public void SourceValidation_AppliesMappingRulesToARegisteredTransport()
    {
        // Mapping validation used to be gated on an explicit `or SourceKinds.Nats`; a transport that got
        // registered but forgotten there would silently accept a broken mapping document.
        var def = FizzSource();
        def.Connector!.Mapping!.ItemsPath = "$.items[";
        Assert.NotEmpty(SourceValidation.Validate(def));
    }

    // ------------------------------------------------------------------
    // Secrets — masked without SecretsMasker knowing the kind
    // ------------------------------------------------------------------

    [Fact]
    public void SecretsAreMaskedForARegisteredKind()
    {
        var masked = SecretsMasker.Mask(FizzSource());
        Assert.Equal(SourceKinds.SecretMask, masked.Connector!.Nats!.Token);
        Assert.Equal("fizz://localhost", masked.Connector.Nats.Url);
    }

    // ------------------------------------------------------------------
    // The loop — messages become rows through the shared path
    // ------------------------------------------------------------------

    [Fact]
    public async Task SubscriberCore_DrivesARegisteredTransportIntoRows()
    {
        var def = FizzSource();
        Fizz.Payloads.Clear();
        Fizz.Acked.Clear();
        Fizz.Payloads.Add(Encoding.UTF8.GetBytes("""[{"id":"a","qty":"7"}]"""));

        var emitted = new List<Dictionary<string, object?>>();
        using var cts = new CancellationTokenSource();

        var core = new SubscriberCore(
            def, Fizz, new DedupTracker([]),
            onRows: (rows, _) =>
            {
                emitted.AddRange(rows);
                cts.Cancel(); // stop as soon as the assertion's subject exists — never a wall-clock wait
                return Task.CompletedTask;
            },
            onStatus: (_, _) => { });

        await core.RunAsync(cts.Token);

        var row = Assert.Single(emitted);
        Assert.Equal("a", row["id"]);
        Assert.Equal(7L, row["qty"]);          // coerced through the shared path, not the transport's
        Assert.Equal("fz", row["_source"]);    // stamped by the shared path
        Assert.True(row.ContainsKey("_ts"));
        Assert.Equal(["""[{"id":"a","qty":"7"}]"""], Fizz.Acked);
    }

    // ------------------------------------------------------------------
    // Sinks
    // ------------------------------------------------------------------

    [Fact]
    public void SinkSelection_ActivatesARegisteredSinkKind()
    {
        var spec = new SinkSpec { Kind = FizzSinkKind, Enabled = true, Nats = new NatsPubConfig { Url = "fizz://out" } };

        Assert.Single(SinkSelection.Active([spec]));

        // …and still honors the two rules that are the registry's, not the transport's.
        Assert.Empty(SinkSelection.Active([new SinkSpec { Kind = FizzSinkKind, Enabled = false, Nats = spec.Nats }]));
        Assert.Empty(SinkSelection.Active([new SinkSpec { Kind = "unregistered", Enabled = true, Nats = spec.Nats }]));
        Assert.Empty(SinkSelection.Active([new SinkSpec { Kind = FizzSinkKind, Enabled = true, Nats = new NatsPubConfig() }]));
    }

    [Fact]
    public void SinkTransports_CreateReturnsTheRegisteredKindsClient()
    {
        var spec = new SinkSpec { Kind = FizzSinkKind, Enabled = true, Nats = new NatsPubConfig { Url = "fizz://out" } };
        var client = SinkTransports.Find(spec.Kind)!.Create(spec, "pipeline", "p1", null);

        Assert.IsType<FizzSinkClient>(client);
        Assert.Equal("p1", client.EntityName);
    }
}
