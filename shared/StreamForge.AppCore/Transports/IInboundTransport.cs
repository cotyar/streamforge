using StreamForge.Abstractions;

namespace StreamForge.AppCore.Transports;

/// <summary>One received message: the subject/topic it actually arrived on (which may differ from the
/// configured one when the subscription uses wildcards), the raw payload bytes, and — where the transport
/// supports acknowledgement — the ack callback. A null <see cref="AckAsync"/> means the transport has no
/// redelivery to acknowledge, i.e. at-most-once with nothing left behind on the broker.</summary>
public sealed record InboundMessage(string Subject, byte[] Payload, Func<Task>? AckAsync);

/// <summary>One connection attempt's worth of subscription. Constructed fresh per (re)connect by
/// <see cref="IInboundTransport.Open"/> and disposed when that attempt ends, so a transport implementation
/// never has to make its own connection object re-enterable.</summary>
public interface IInboundSubscription : IAsyncDisposable
{
    /// <summary>Yields messages until <paramref name="ct"/> is cancelled or the underlying subscription ends
    /// on its own. Throwing means "this connection attempt failed" — <see cref="SubscriberCore"/> owns the
    /// backoff and will call <see cref="IInboundTransport.Open"/> again.</summary>
    IAsyncEnumerable<InboundMessage> SubscribeAsync(CancellationToken ct);
}

/// <summary>
/// Plan 010: everything the platform needs to know about a message-transport source kind, in one place.
/// Implementing this plus registering it in <see cref="InboundTransports"/> is the WHOLE cost of a new
/// inbound transport — the connector drivers (Orleans <c>ConnectorGrain</c>, Dapr <c>ConnectorActor</c>),
/// <c>SourceValidation</c> and <c>SecretsMasker</c> all go through the registry and need no edit.
///
/// <para><b>Payload → row is not a transport's business.</b> Bytes go through the one shared
/// format/mapping/coercion/dedup path (<c>ConnectorPollCycle.ExecuteMessage</c>) that a polled HTTP body
/// already uses; a transport supplies bytes and says which <see cref="FormatOf"/> to parse them as, and
/// nothing else. That is the constraint that keeps "add a transport" from meaning "add a second extraction
/// path with its own subtly different NULL handling".</para>
///
/// <para><b>What this deliberately does NOT cover: the grpc kind.</b> A gRPC subscription decodes typed
/// frames against a remote schema and never sees a payload-format question at all, so it stays its own
/// branch in both drivers rather than being bent into this shape. Transports that fit here are the
/// subject/topic + opaque-payload family (NATS today; RV, MQTT, AMQP, Kafka next).</para>
/// </summary>
public interface IInboundTransport
{
    /// <summary>The <see cref="SourceDefinition.Kind"/> value this transport serves, e.g.
    /// <see cref="SourceKinds.Nats"/>. Compared ordinally, and must be unique across the registry.</summary>
    string Kind { get; }

    /// <summary>Appends a human-readable message per problem with this source's transport config — the
    /// per-kind half of <c>SourceValidation.Validate</c>, which owns everything kind-independent (name,
    /// fields, mapping) and calls this for the rest. Never throws; an empty <paramref name="errors"/> on
    /// return means the config is accepted.</summary>
    void Validate(SourceDefinition def, List<string> errors);

    /// <summary>Payload format for the shared parse path: "ndjson" | "json" | "csv"
    /// (<see cref="FileFormats"/>). Usually a field on the transport's own config.</summary>
    string FormatOf(SourceDefinition def);

    /// <summary>Creates (but does not connect — that happens on first enumeration) one subscription attempt.
    /// Throwing here is treated exactly like a failed connection: reported and retried with backoff.</summary>
    IInboundSubscription Open(SourceDefinition def);

    /// <summary>What the console needs to render this transport's config form — see
    /// <see cref="TransportDescriptor"/>. Implementing it is what keeps "add a transport" from also meaning
    /// "add a React component".</summary>
    TransportDescriptor Describe();
}
