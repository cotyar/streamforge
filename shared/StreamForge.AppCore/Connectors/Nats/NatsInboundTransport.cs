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
        FileFormats.Ndjson, FileFormats.JsonArray, FileFormats.Csv, FileFormats.Fix,
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
            errors.Add($"connector.nats.format '{nats.Format}' is not recognized (expected one of: ndjson, json, csv, fix)");
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

    /// <summary>The console form, declared rather than coded — this replaced a 261-line hand-written
    /// <c>NatsConfigEditor.tsx</c>. The one visible difference is that the four credential fields are shown
    /// as themselves instead of behind an "authentication mode" picker: the config really does have four
    /// independent slots, and the group's help states the precedence the server actually applies
    /// (<c>NatsConnectionSettings.Build</c>: creds file &gt; token &gt; user+password), which the picker hid.</summary>
    public TransportDescriptor Describe() => new()
    {
        Kind = SourceKinds.Nats,
        // Plan 016 wave 4: every in-tree kind states its contract version explicitly, even at the
        // "1.0.0" default, so a future behavior-changing edit here is a deliberate bump, not a silent one.
        Version = "1.0.0",
        Label = "NATS",
        Help = "A persistent subject subscription — not a poll schedule, so this kind ignores Schedule.",
        ConfigProperty = "nats",
        Groups =
        [
            new TransportGroup
            {
                Key = "auth",
                Label = "Credentials",
                Help = "All optional. If more than one is set the server applies: .creds file, then token, then username+password.",
            },
            new TransportGroup
            {
                Key = "jetstream",
                Label = "JetStream",
                Optional = true,
                ObjectKey = "jetStream",
                Help = "Off is core NATS: at-most-once, no cursor, nothing to clean up server-side. On trades that for a "
                     + "durable consumer — messages are redelivered until acked, at the cost of server-side state this "
                     + "platform then owns and must not leave orphaned.",
            },
        ],
        Fields =
        [
            new TransportField { Key = "url", Label = "Server URL", Required = true, Mono = true, Placeholder = "nats://localhost:4222", Help = "Comma-separate several servers." },
            new TransportField { Key = "subject", Label = "Subject", Required = true, Mono = true, Placeholder = "trades.>", Help = "NATS wildcards (* and >) are the server's to interpret." },
            new TransportField
            {
                Key = "format", Label = "Payload format", Type = TransportFieldTypes.Select,
                Options = [FileFormats.Ndjson, FileFormats.JsonArray, FileFormats.Csv, FileFormats.Fix], Default = FileFormats.JsonArray,
                Help = "How each message body is parsed, before field mapping — same vocabulary as the file/folder connectors.",
            },
            new TransportField
            {
                Key = "queueGroup", Label = "Queue group", Mono = true, Placeholder = "streamforge-ingest",
                Help = "Two replicas sharing a queue group split the subject's messages between them instead of both "
                     + "ingesting every message — set this when this source runs on more than one host.",
            },
            new TransportField { Key = "token", Label = "Token", Type = TransportFieldTypes.Secret, Group = "auth" },
            new TransportField { Key = "username", Label = "Username", Group = "auth" },
            new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret, Group = "auth" },
            new TransportField
            {
                Key = "credentials", Label = ".creds file contents", Type = TransportFieldTypes.Secret, Group = "auth", Mono = true,
                Placeholder = "Paste the contents of a NATS .creds file",
                Help = "The contents, not a path — the catalog has to stay portable across hosts.",
            },
            new TransportField { Key = "stream", Label = "Stream", Group = "jetstream", Required = true, Mono = true },
            new TransportField { Key = "durable", Label = "Durable consumer name", Group = "jetstream", Required = true, Mono = true },
            new TransportField
            {
                Key = "maxAckPending", Label = "Max ack pending", Type = TransportFieldTypes.Number, Group = "jetstream", Default = "1000",
                Help = "In-flight unacked messages — the JetStream-side analogue of an ingress buffer bound.",
            },
        ],
    };

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
