using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Fix;

/// <summary>
/// Plan 019 wave E: <c>fix-duplex</c> — the FIRST duplex kind (plan 019's third transport seam, after
/// <see cref="IInboundTransport"/> and <see cref="IPolledTransport"/>). One live FIX session whose inbound
/// half is driven exactly like <see cref="FixInboundTransport"/>'s (same <see cref="FixSourceConfig"/>,
/// same <c>SubscriberCore</c> reconnect/backoff loop, same <c>FormatOf</c> reuse of the <c>fix</c> format's
/// parser/mapping/coercion/dedup path) and whose outbound half accepts sends — see
/// <see cref="FixDuplexSession"/> for the session object itself.
///
/// <para><b>A SEPARATE class from <see cref="FixInboundTransport"/>, not a flag on it</b> — see
/// <see cref="SourceKinds.FixDuplex"/>'s own doc comment for the full reasoning (different validation
/// regime, and <see cref="DuplexTransports.Register"/> could not register the SAME kind string twice
/// anyway). <c>FixConnectorsTests.RegisterAllPutsFixInInboundTransports</c> (frozen, plan 018) pins
/// that <c>InboundTransports.Find(SourceKinds.Fix)</c> is specifically a <see cref="FixInboundTransport"/>
/// instance — this class registers under the DIFFERENT kind <see cref="SourceKinds.FixDuplex"/>, so that
/// assertion is untouched.</para>
///
/// <para><b>Registered via <see cref="DuplexTransports.Register"/>, which ALSO registers this into
/// <see cref="InboundTransports"/></b> (see that registry's own doc comment for why) — so every existing
/// inbound code path (<c>ConnectorGrain.ArmForKind</c>'s <c>InboundTransports.Find</c> arm, the Dapr
/// twin, <c>SourceValidation.IsKnownKind</c>, <c>GET /api/transports</c>'s <c>Inbound</c> list) treats a
/// <c>fix-duplex</c> source exactly like any other message-transport source with NO host wiring beyond
/// this one registration call — confirmed by reading <c>ConnectorGrain.cs</c>: <c>ArmForKind</c> dispatches
/// on <c>InboundTransports.Find(def.Kind)</c> alone, and its separate duplex-status reporting
/// (<c>DuplexStateForCurrentDef</c>) dispatches on <c>DuplexTransports.Find(def.Kind)</c> generically, by
/// kind string, with no per-kind branch anywhere.</para>
/// </summary>
public sealed class FixDuplexTransport : IDuplexTransport
{
    /// <summary>Same recognized <see cref="FixSourceConfig.BeginString"/> values as
    /// <see cref="FixInboundTransport"/> — duplicated rather than shared for the same reason
    /// <see cref="FixRowMapper"/>'s tag table is: a small, static, easily eyeballed table, not worth an
    /// internal-visibility seam across two classes that must otherwise stay fully independent (different
    /// validation regime is coming in wave 019-G — see <see cref="SourceKinds.FixDuplex"/>'s doc
    /// comment).</summary>
    private static readonly string[] KnownBeginStrings =
        ["FIX.4.0", "FIX.4.1", "FIX.4.2", "FIX.4.3", "FIX.4.4", "FIXT.1.1"];

    public string Kind => SourceKinds.FixDuplex;

    public string FormatOf(SourceDefinition def) => FileFormats.Fix;

    /// <summary>Delegates to <see cref="OpenDuplex"/> — <see cref="IDuplexTransport"/>'s own class doc
    /// requires this so both entry points return the SAME live session object rather than two
    /// independently-connected ones.</summary>
    public IInboundSubscription Open(SourceDefinition def) => OpenDuplex(def);

    /// <summary>Constructs the session and PUBLISHES it into <see cref="DuplexSessions"/> immediately —
    /// before any connection attempt, exactly like <see cref="FixInboundTransport.Open"/>'s own contract
    /// ("Open only CONSTRUCTS a subscription; the actual FIX connection happens on first enumeration").
    /// Publishing here rather than after logon means the proxy sink can find this session (and get an
    /// honest <see cref="IDuplexSession.IsReady"/> == false) the moment the source is armed, rather than
    /// racing the first successful logon — see <see cref="DuplexSessions"/>'s own doc comment: "OpenDuplex
    /// publishes, the session's own DisposeAsync withdraws" is the seam's stated contract, not an
    /// implementation detail this class gets to skip.</summary>
    public IDuplexSession OpenDuplex(SourceDefinition def)
    {
        var config = ConfigOf(def);
        var session = new FixDuplexSession(def.Name, config);
        DuplexSessions.Publish(def.Name, session);
        return session;
    }

    /// <summary>Identical rules to <see cref="FixInboundTransport.Validate"/> — see that method's own doc
    /// comment. Deliberately does NOT add wave 019-G's mandatory-persistence rule (StorePath required,
    /// ResetOnLogon defaulting false) — that is a stated NOT-this-wave item; adding it here now would make
    /// this class lie about which wave actually enforces it.</summary>
    public void Validate(SourceDefinition def, List<string> errors)
    {
        var fix = def.Connector?.Fix;
        if (fix is null)
        {
            errors.Add($"kind '{SourceKinds.FixDuplex}' requires connector.fix");
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

    /// <summary>The console form. <see cref="TransportDescriptor.Duplex"/> = true is the one difference
    /// from <see cref="FixInboundTransport.Describe"/>'s shape (see that flag's own doc comment: it means
    /// "this kind implements <see cref="IDuplexTransport"/>", never true for the plain <c>fix</c> kind).
    /// Fields are otherwise the same <see cref="FixSourceConfig"/> surface — order entry needs nothing this
    /// wave adds to the config type itself (row → FIX mapping is code, not configuration).</summary>
    public TransportDescriptor Describe() => new()
    {
        Kind = SourceKinds.FixDuplex,
        Label = "FIX (order entry)",
        Duplex = true,
        Help = "A live FIX session whose outbound half also accepts sends — pair this with a 'duplex' sink "
             + "naming this source to route orders through it. At-most-once at the seam (plan 019 D3): an "
             + "order the socket loses is the venue's resend problem, not something this transport detects. "
             + "No FIX dictionary and no ClOrdID/OrigClOrdID chain yet (plan 019 wave F) — a row this "
             + "transport cannot map to a FIX message is reported as a failed send, never silently dropped.",
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
                Help = "No dictionary ships with this platform for INBOUND parsing (plan 018) — this only selects the version header.",
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
                Help = "For an order session this SHOULD usually be off, together with a non-empty Store "
                     + "path, so sequence numbers survive a restart — plan 019 wave G makes that mandatory; "
                     + "this wave still accepts the market-data-shaped default, so set both deliberately "
                     + "until then.",
            },
            new TransportField
            {
                Key = "storePath", Label = "Store path", Mono = true,
                Help = "Empty (default) = in-memory sequence-number store, reset every restart — an order "
                     + "session that restarts on an empty store risks resending or skipping orders the "
                     + "venue already has a sequence number for. Non-empty = a file-backed store at this "
                     + "path — in a container this MUST be a mounted volume.",
            },
            new TransportField { Key = "useSsl", Label = "Use TLS", Type = TransportFieldTypes.Bool },
            new TransportField
            {
                Key = "onLogon", Label = "Send after logon", Type = TransportFieldTypes.Text, Mono = true,
                Placeholder = "One raw FIX message per line",
                Help = "Optional for an order-entry session (unlike market data, nothing must be sent to "
                     + "start receiving execution reports) — raw FIX text, one message per line, delimiter "
                     + "sniffed the same way the fix format parser sniffs a payload.",
            },
            new TransportField
            {
                Key = "msgTypes", Label = "MsgType filter", Mono = true, Placeholder = "8,9",
                Help = "Comma-separated MsgType (tag 35) include-filter over the INBOUND half (e.g. "
                     + "ExecutionReport 8, OrderCancelReject 9). Empty = every application message. "
                     + "Session-level traffic never reaches this filter at all.",
            },
            new TransportField
            {
                Key = "queueCapacity", Label = "Queue capacity", Type = TransportFieldTypes.Number, Default = "10000",
                Help = "Bound on the inbound drop-oldest bridge queue — see FixInboundTransport's Help text "
                     + "for the mechanism. An order-entry session should size this generously: dropping an "
                     + "ExecutionReport is a worse loss here than for market data.",
            },
        ],
    };

    private static FixSourceConfig ConfigOf(SourceDefinition def) =>
        def.Connector?.Fix ?? throw new InvalidOperationException($"source '{def.Name}' has kind '{SourceKinds.FixDuplex}' but no fix config");
}
