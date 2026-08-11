using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.AppCore.Connectors.Nats;

/// <summary>
/// Plan 010: NATS as an <see cref="IInboundTransport"/> — the reference implementation, and the template for
/// every transport after it. Everything NATS-specific about a <c>nats</c>-kind source now lives in this file
/// and <see cref="NatsClientMessageSource"/>: which config to read, what makes that config valid, and how to
/// open a subscription. The reconnect/backoff/mapping/ack loop is <see cref="SubscriberCore"/>'s.
///
/// <para><see cref="NatsSubscriberCore"/> (plan 009 B1) remains as the type its own test suite drives, and is
/// now a wrapper over this pair. The optional <paramref name="sourceFactory"/> is what lets those tests
/// substitute a fake <see cref="INatsMessageSource"/> instead of dialing a broker.</para>
/// </summary>
public sealed class NatsInboundTransport(Func<INatsMessageSource>? sourceFactory = null) : IInboundTransport
{
    private static readonly HashSet<string> KnownFormats = new(StringComparer.Ordinal)
    {
        FileFormats.Ndjson, FileFormats.JsonArray, FileFormats.Csv,
    };

    public string Kind => SourceKinds.Nats;

    public string FormatOf(SourceDefinition def) => ConfigOf(def).Format;

    public IInboundSubscription Open(SourceDefinition def)
    {
        var config = ConfigOf(def);
        var source = sourceFactory?.Invoke() ?? new NatsClientMessageSource($"streamforge-source-{def.Name}");
        return new Subscription(source, config);
    }

    /// <summary>Plan 009 B1's rules, moved here from <c>SourceValidation</c> so the kind's config and the
    /// definition of "valid" for it sit together: url + subject are required (a subscription with no server
    /// to dial or subject to listen on cannot possibly do anything); Format must be a known connector format
    /// (same vocabulary as file/folder); JetStream, when present, needs BOTH Stream and Durable — a durable
    /// consumer with either missing cannot be created (see <see cref="NatsJetStreamConfig"/>'s own doc
    /// comment on why JetStream is opt-in in the first place).</summary>
    public void Validate(SourceDefinition def, List<string> errors)
    {
        var nats = def.Connector?.Nats;
        if (nats is null)
        {
            errors.Add("kind 'nats' requires connector.nats");
            return;
        }

        if (string.IsNullOrWhiteSpace(nats.Url))
        {
            errors.Add("connector.nats.url is required");
        }

        if (string.IsNullOrWhiteSpace(nats.Subject))
        {
            errors.Add("connector.nats.subject is required");
        }

        if (!KnownFormats.Contains(nats.Format))
        {
            errors.Add($"connector.nats.format '{nats.Format}' is not recognized (expected one of: ndjson, json, csv)");
        }

        if (nats.JetStream is { } js)
        {
            if (string.IsNullOrWhiteSpace(js.Stream))
            {
                errors.Add("connector.nats.jetStream.stream is required when jetStream is set");
            }

            if (string.IsNullOrWhiteSpace(js.Durable))
            {
                errors.Add("connector.nats.jetStream.durable is required when jetStream is set");
            }

            if (js.MaxAckPending <= 0)
            {
                errors.Add("connector.nats.jetStream.maxAckPending must be > 0");
            }
        }
    }

    private static NatsSubConfig ConfigOf(SourceDefinition def) =>
        def.Connector?.Nats ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'nats' but no nats config");

    /// <summary>Adapts the plan 009 <see cref="INatsMessageSource"/> seam (config-per-call, NATS-typed
    /// message) to the transport-neutral <see cref="IInboundSubscription"/> (config captured at Open,
    /// neutral message). Kept as an adapter rather than a rewrite so the fake-driven test suite around
    /// <see cref="INatsMessageSource"/> keeps testing the real path.</summary>
    private sealed class Subscription(INatsMessageSource source, NatsSubConfig config) : IInboundSubscription
    {
        public async IAsyncEnumerable<InboundMessage> SubscribeAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            await foreach (var msg in source.SubscribeAsync(config, ct).ConfigureAwait(false))
            {
                yield return new InboundMessage(msg.Subject, msg.Payload, msg.AckAsync);
            }
        }

        public ValueTask DisposeAsync() => source.DisposeAsync();
    }
}
