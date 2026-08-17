using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Fix;

/// <summary>
/// Plan 018 wave C: a receive-only FIX session as an <see cref="IInboundTransport"/>, living out of the
/// core in this assembly — <c>StreamForge.AppCore</c> gains nothing, exactly like
/// <c>StreamForge.Connectors.Database</c>'s postgres/mssql kinds. It yields the raw SOH-delimited FIX
/// bytes off the wire and declares <see cref="FormatOf"/> as the constant <see cref="FileFormats.Fix"/>,
/// so it reuses the SAME format/mapping/coercion/dedup path (<c>ConnectorPollCycle.ExecuteMessage</c>,
/// via <c>SubscriberCore</c>) every other message transport uses — nothing about payload-&gt;row is this
/// transport's business, matching <see cref="IInboundTransport"/>'s own class doc.
///
/// <para><b><see cref="FormatOf"/> is a constant, not configurable</b> — unlike NATS, which can carry
/// ndjson/json/csv/fix over the same subject. A FIX session speaks FIX; there is nothing to choose, the
/// way a Postgres row source has no format picker either.</para>
///
/// <para><b>The reconnecting subscribe loop is <c>SubscriberCore</c>'s</b>, shared with every other
/// message transport — <see cref="Open"/> only CONSTRUCTS a subscription; the actual FIX connection
/// happens on first enumeration (<see cref="Subscription.SubscribeAsync"/>), exactly as
/// <see cref="IInboundTransport.Open"/>'s own doc requires, so a bad config surfaces as a normal
/// reconnect-with-backoff cycle rather than a crash at source-start time.</para>
///
/// <para><b>The bounded, drop-oldest bridge from QuickFIX/n's session thread is a stated ceiling</b>,
/// not a bug — see <see cref="QuickFixMessageSource"/>'s class doc for the mechanism and
/// <see cref="Describe"/>'s <c>Help</c> text for the plain-words version: correct for market data (a stale
/// quote is worthless), <b>wrong for drop-copy</b>, where every message must survive.</para>
///
/// <para><b><see cref="StreamForge.AppCore.Transports.InboundMessage.AckAsync"/> is always null.</b>
/// QuickFIX/n's sequence layer acknowledges at the SESSION level (a message is in-sequence, the
/// counterparty need not resend it) — that is not the same thing as this platform having processed the
/// row into a table, and claiming otherwise would be exactly the lie <c>IInboundTransport</c>'s own doc
/// warns a non-null <c>AckAsync</c> must not tell.</para>
///
/// <para><b>A substitutable session seam</b>, mirroring <c>NatsInboundTransport</c>'s optional
/// <c>Func&lt;INatsMessageSource&gt;</c> constructor parameter: <paramref name="sourceFactory"/> lets
/// tests substitute a fake <see cref="IFixMessageSource"/> instead of dialing a counterparty. Only the
/// acceptance test (a real QuickFIX/n acceptor on a 7xxx port) exercises <see cref="QuickFixMessageSource"/>
/// end to end.</para>
/// </summary>
public sealed class FixInboundTransport(Func<IFixMessageSource>? sourceFactory = null) : IInboundTransport
{
    /// <summary>Recognized <see cref="FixSourceConfig.BeginString"/> values — the version headers this
    /// session might claim, never a schema to validate against (see <see cref="FixSourceConfig"/>'s doc
    /// comment on why no dictionary backs this list). FIXT.1.1 is accepted here even though plan 018 defers
    /// FIXT application-version negotiation — the header value alone is harmless to send.</summary>
    private static readonly string[] KnownBeginStrings =
        ["FIX.4.0", "FIX.4.1", "FIX.4.2", "FIX.4.3", "FIX.4.4", "FIXT.1.1"];

    public string Kind => SourceKinds.Fix;

    public string FormatOf(SourceDefinition def) => FileFormats.Fix;

    public IInboundSubscription Open(SourceDefinition def)
    {
        var config = ConfigOf(def);
        var source = sourceFactory?.Invoke() ?? new QuickFixMessageSource();
        return new Subscription(source, config);
    }

    /// <summary>Follows <c>NatsInboundTransport.Validate</c>'s shape and wording exactly: required fields
    /// first, then the enumerated/ranged ones, one message per problem, never throws.</summary>
    public void Validate(SourceDefinition def, List<string> errors)
    {
        var fix = def.Connector?.Fix;
        if (fix is null)
        {
            errors.Add("kind 'fix' requires connector.fix");
            return;
        }

        if (string.IsNullOrWhiteSpace(fix.Host))
        {
            errors.Add("connector.fix.host is required");
        }

        if (fix.Port is < 1 or > 65535)
        {
            errors.Add("connector.fix.port must be between 1 and 65535");
        }

        if (string.IsNullOrWhiteSpace(fix.SenderCompId))
        {
            errors.Add("connector.fix.senderCompId is required");
        }

        if (string.IsNullOrWhiteSpace(fix.TargetCompId))
        {
            errors.Add("connector.fix.targetCompId is required");
        }

        if (!KnownBeginStrings.Contains(fix.BeginString, StringComparer.Ordinal))
        {
            errors.Add($"connector.fix.beginString '{fix.BeginString}' is not recognized (expected one of: {string.Join(", ", KnownBeginStrings)})");
        }

        if (fix.HeartBtIntSeconds <= 0)
        {
            errors.Add("connector.fix.heartBtIntSeconds must be > 0");
        }

        if (fix.QueueCapacity <= 0)
        {
            errors.Add("connector.fix.queueCapacity must be > 0");
        }
    }

    /// <summary>The console form, declared rather than coded — see <c>NatsInboundTransport.Describe</c>
    /// for the pattern this follows. <c>Polled = false</c> (a session, not a schedule); <c>Mapping</c>
    /// defaults true (FIX rows are nested JSON, same as a NATS/url/file payload, so they need a
    /// <c>MappingSpec</c> exactly like those do — unlike a polled SQL row source, where the SELECT list
    /// already IS the mapping).</summary>
    public TransportDescriptor Describe() => new()
    {
        Kind = SourceKinds.Fix,
        Label = "FIX",
        Help = "A persistent FIX session — not a poll schedule, so this kind ignores Schedule. Receive-only: "
             + "there is no order-entry surface here (see plan 019 for that). Inbound messages are bridged "
             + "through a bounded, drop-oldest queue, which is correct for market data (a stale quote is "
             + "worthless) but WRONG for drop-copy, where every message must survive — a drop-copy session "
             + "needs a large queueCapacity and a consumer that keeps up, not a queue that discards for it.",
        ConfigProperty = "fix",
        Groups =
        [
            new TransportGroup
            {
                Key = "auth",
                Label = "Credentials",
                Help = "Both optional. When set, sent as tags 553/554 inside the Logon message — QuickFIX/n "
                     + "has no built-in credential exchange, so this is this platform's own addition.",
            },
        ],
        Fields =
        [
            new TransportField { Key = "host", Label = "Host", Required = true, Mono = true, Placeholder = "fix.venue.example.com" },
            new TransportField { Key = "port", Label = "Port", Type = TransportFieldTypes.Number, Required = true, Placeholder = "9880" },
            new TransportField { Key = "senderCompId", Label = "SenderCompID (this side)", Required = true, Mono = true },
            new TransportField { Key = "targetCompId", Label = "TargetCompID (counterparty)", Required = true, Mono = true },
            new TransportField
            {
                Key = "beginString", Label = "FIX version", Type = TransportFieldTypes.Select,
                Options = KnownBeginStrings, Default = "FIX.4.4",
                Help = "No dictionary ships with this platform (plan 018) — this only selects the version header.",
            },
            new TransportField { Key = "username", Label = "Username", Group = "auth" },
            new TransportField { Key = "password", Label = "Password", Type = TransportFieldTypes.Secret, Group = "auth" },
            new TransportField
            {
                Key = "heartBtIntSeconds", Label = "Heartbeat interval (s)", Type = TransportFieldTypes.Number, Default = "30",
            },
            new TransportField
            {
                Key = "resetOnLogon", Label = "Reset sequence numbers on logon", Type = TransportFieldTypes.Bool, Default = "true",
                Help = "On (default): a clean slate every logon, the market-data-shaped default — resending "
                     + "yesterday's quotes is worse than not resending them. Off for a drop-copy session that "
                     + "must resume its sequence across restarts, together with a non-empty Store path.",
            },
            new TransportField
            {
                Key = "storePath", Label = "Store path", Mono = true,
                Help = "Empty (default) = in-memory sequence-number store, reset every restart. Non-empty = a "
                     + "file-backed store at this path — in a container this MUST be a mounted volume, the "
                     + "same requirement the file sink's Path field carries.",
            },
            new TransportField { Key = "useSsl", Label = "Use TLS", Type = TransportFieldTypes.Bool },
            new TransportField
            {
                Key = "onLogon", Label = "Send after logon", Type = TransportFieldTypes.Text, Mono = true,
                Placeholder = "One raw FIX message per line, e.g. 35=V|262=1|263=1|55=EUR/USD|...",
                Help = "Raw FIX text, one message per line — a market-data session must SEND a request to "
                     + "receive anything. Delimiter (SOH, | or ^) is sniffed the same way the fix format "
                     + "parser sniffs a payload. A send failure here fails the whole connection attempt: "
                     + "silence would be the worst possible symptom.",
            },
            new TransportField
            {
                Key = "msgTypes", Label = "MsgType filter", Mono = true, Placeholder = "W,X",
                Help = "Comma-separated MsgType (tag 35) include-filter. Empty = every application message. "
                     + "Session-level traffic (Logon/Heartbeat/TestRequest/ResendRequest/SequenceReset/Logout) "
                     + "never reaches this filter at all — QuickFIX/n's own session layer consumes it first.",
            },
            new TransportField
            {
                Key = "queueCapacity", Label = "Queue capacity", Type = TransportFieldTypes.Number, Default = "10000",
                Help = "Bound on the drop-oldest bridge queue — see this transport's Help text above.",
            },
        ],
    };

    private static FixSourceConfig ConfigOf(SourceDefinition def) =>
        def.Connector?.Fix ?? throw new InvalidOperationException($"source '{def.Name}' has kind 'fix' but no fix config");

    /// <summary>Adapts <see cref="IFixMessageSource"/> (config-per-call, FIX-typed message) to the
    /// transport-neutral <see cref="IInboundSubscription"/> — same shape as <c>NatsInboundTransport</c>'s
    /// own <c>Subscription</c> adapter, and for the same reason: kept separate from a rewrite so the
    /// fake-driven test suite around <see cref="IFixMessageSource"/> keeps testing the real path.
    /// <see cref="StreamForge.AppCore.Transports.InboundMessage.AckAsync"/> is always null — see this
    /// class's doc comment for why.</summary>
    private sealed class Subscription(IFixMessageSource source, FixSourceConfig config) : IInboundSubscription
    {
        public async IAsyncEnumerable<InboundMessage> SubscribeAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            await foreach (var msg in source.SubscribeAsync(config, ct).ConfigureAwait(false))
            {
                yield return new InboundMessage(msg.MsgType, msg.Payload, AckAsync: null);
            }
        }

        public ValueTask DisposeAsync() => source.DisposeAsync();
    }
}
