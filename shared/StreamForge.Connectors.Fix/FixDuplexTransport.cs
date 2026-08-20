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

    /// <summary>Shares <see cref="FixInboundTransport.Validate"/>'s baseline rules (host/port/CompIDs/
    /// beginString/heartbeat/queueCapacity), plus wave 019-G's D5 rule, which does NOT apply to that class:
    /// on an order session the store IS the record of what was sent — see the doc comment on the
    /// StorePath/ResetOnLogon checks below and TRANSPORTS.md's FIX section ("fix-duplex — mandatory
    /// sequence-number persistence") for the full reasoning and the recovery procedure. Plan 018's `fix`
    /// kind keeps `ResetOnLogon=true` + in-memory store as its default and this method never runs for
    /// it — <see cref="FixInboundTransport.Validate"/> is untouched by this wave.</summary>
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

        ValidatePersistence(fix, errors);
    }

    /// <summary>Plan 019 D5, the reasoning this method's callers point back to: on a market-data session
    /// (the plain <c>fix</c> kind) losing the sequence-number store costs at worst some re-sent quotes, so
    /// <see cref="FixInboundTransport"/> defaults to an in-memory store and resets on every logon.
    /// <b>On an order session the store IS the record of what was sent.</b> Losing it means a resend
    /// request the platform cannot answer, and a gap the venue resolves by its own rules — see
    /// TRANSPORTS.md's FIX section for exactly what that looks like on the wire and how an operator
    /// recovers. So for <c>fix-duplex</c>, and only for it, persistence stops being an operator choice:
    ///
    /// <list type="bullet">
    /// <item><description><see cref="FixSourceConfig.StorePath"/> must be set — an empty value here would
    /// otherwise silently pick <see cref="QuickFix.Store.MemoryStoreFactory"/>, exactly the choice that is
    /// safe for market data and wrong for orders.</description></item>
    /// <item><description><see cref="FixSourceConfig.ResetOnLogon"/> must be <c>false</c> — a store that
    /// exists but is thrown away every logon buys nothing; the two are refused together rather than one
    /// silently undermining the other.</description></item>
    /// <item><description><see cref="FixSourceConfig.StorePath"/> must be an absolute path — a relative
    /// path resolves against the process's current working directory, which is not a location any
    /// deployment can promise is stable, let alone durable.</description></item>
    /// <item><description>A path that starts under a POSIX temp directory (<c>/tmp</c>, <c>/var/tmp</c>)
    /// is flagged too — <b>stated as plainly as the ceiling allows</b>: this process cannot see the volume
    /// mounted behind a path, so it cannot know whether ANY given path — under <c>/tmp</c> or not —
    /// survives a restart or a reschedule. This one check is a strong, cheap, honest hint for the single
    /// most common way to get this wrong (a path that is unconditionally wiped, everywhere, on every
    /// reboot), not a certificate that any other path is safe.</description></item>
    /// </list></summary>
    private static void ValidatePersistence(FixSourceConfig fix, List<string> errors)
    {
        var hasStorePath = !string.IsNullOrWhiteSpace(fix.StorePath);

        if (!hasStorePath)
        {
            errors.Add(
                "connector.fix.storePath is required for fix-duplex: on an order session the sequence-number " +
                "store IS the record of what this platform has sent and received, not just a resend-avoidance " +
                "optimization. Without it, a restart loses that record — the platform can no longer answer a " +
                "resend request the venue may issue, and the gap is the venue's to resolve by its own rules. " +
                "Set storePath to a file-backed path on a volume that survives a restart; see TRANSPORTS.md's " +
                "FIX section for the recovery procedure if this is ever lost anyway.");
            // Deliberately does NOT return: ResetOnLogon is checked independently below, so an operator who
            // fixes the empty storePath first does not then get a second round-trip to discover this one.
        }

        if (fix.ResetOnLogon)
        {
            errors.Add(
                "connector.fix.resetOnLogon must be false for fix-duplex: resetting sequence numbers on every " +
                "logon throws away exactly the continuity storePath exists to preserve, so a durable store " +
                "combined with a reset-every-logon session is refused rather than left to quietly defeat " +
                "itself — set resetOnLogon to false alongside a non-empty storePath.");
        }

        if (!hasStorePath)
        {
            return; // the path-shape checks below need an actual path to inspect.
        }

        if (!Path.IsPathRooted(fix.StorePath))
        {
            errors.Add(
                $"connector.fix.storePath '{fix.StorePath}' must be an absolute path: a relative path resolves " +
                "against the process's current working directory, which is not a location this platform (or " +
                "its container orchestrator) promises is stable across a restart.");
        }

        if (IsUnderPosixTempDirectory(fix.StorePath))
        {
            errors.Add(
                $"connector.fix.storePath '{fix.StorePath}' is under a POSIX temp directory (/tmp or " +
                "/var/tmp), which is wiped on every reboot on essentially every platform that has one. This " +
                "check cannot see the actual volume behind ANY path — it cannot confirm a different path is " +
                "durable either — but it can say this specific pattern is known-bad, so it does. Point " +
                "storePath at a path you know is backed by a mounted, persistent volume.");
        }
    }

    /// <summary>String-prefix check only — no filesystem access. Deliberately does not attempt to check
    /// writability: doing so would mean I/O (and its failure modes: permissions, a not-yet-created parent
    /// directory, a mount that isn't attached yet) inside source validation, for a property — "is this
    /// path writable right now" — that does not even answer the question this check exists for (a tmpfs
    /// mount is writable AND exactly as ephemeral as /tmp). An unwritable store surfaces immediately and
    /// loudly on the first connection attempt instead (QuickFIX/n's <c>FileStoreFactory</c> throws), which
    /// is not a silent failure mode this validator needs to pre-empt.</summary>
    private static bool IsUnderPosixTempDirectory(string path)
    {
        var normalized = path.Replace('\\', '/');
        return normalized == "/tmp" || normalized.StartsWith("/tmp/", StringComparison.Ordinal)
            || normalized == "/var/tmp" || normalized.StartsWith("/var/tmp/", StringComparison.Ordinal);
    }

    /// <summary>The console form. <see cref="TransportDescriptor.Duplex"/> = true is the one difference
    /// from <see cref="FixInboundTransport.Describe"/>'s shape (see that flag's own doc comment: it means
    /// "this kind implements <see cref="IDuplexTransport"/>", never true for the plain <c>fix</c> kind).
    /// Fields are otherwise the same <see cref="FixSourceConfig"/> surface — order entry needs nothing this
    /// wave adds to the config type itself (row → FIX mapping is code, not configuration).</summary>
    public TransportDescriptor Describe() => new()
    {
        Kind = SourceKinds.FixDuplex,
        Version = "1.0.0", // plan 016 wave 4: explicit contract version — see TransportDescriptor.Version.
        Label = "FIX (order entry)",
        Duplex = true,
        Help = "A live FIX session whose outbound half also accepts sends — pair this with a 'duplex' sink "
             + "naming this source to route orders through it. At-most-once at the seam (plan 019 D3): an "
             + "order the socket loses is the venue's resend problem, not something this transport detects. "
             + "No real FIX dictionary — outbound required-field validation (plan 019 D6) is a curated table "
             + "for NewOrderSingle/OrderCancelRequest/OrderCancelReplaceRequest only, naming the missing tag "
             + "when it refuses a message. A row this transport cannot map, or cannot validate, is reported "
             + "as a failed send, never silently dropped.",
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
                Key = "resetOnLogon", Label = "Reset sequence numbers on logon", Type = TransportFieldTypes.Bool, Default = "false",
                Help = "Must be false for fix-duplex (plan 019 D5, enforced by Validate): a durable Store "
                     + "path combined with resetting on every logon throws away exactly the continuity the "
                     + "store exists to preserve. Unlike the plain 'fix' kind, this default is false, not "
                     + "true — an order session's whole point is that sequence numbers survive a restart.",
            },
            new TransportField
            {
                Key = "storePath", Label = "Store path", Mono = true, Required = true,
                Help = "Required for fix-duplex (plan 019 D5, enforced by Validate) — on an order session "
                     + "this file IS the record of what was sent and received; there is no in-memory option "
                     + "here as there is for the plain 'fix' kind. Must be an absolute path, and in a "
                     + "container it MUST be on a mounted, persistent volume — a path this platform cannot "
                     + "confirm is durable (this process cannot see the volume behind any path) but a path "
                     + "under /tmp or /var/tmp is refused outright as a known-ephemeral pattern. See "
                     + "TRANSPORTS.md's FIX section for the recovery procedure if the store is ever lost.",
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
            new TransportField
            {
                Key = "generateClOrdId", Label = "Generate ClOrdID when missing", Type = TransportFieldTypes.Bool,
                Help = "Off by default (plan 019 D7): a row missing ClOrdID is refused rather than silently "
                     + "completed. Turn this on only if the caller does not need the id before sending — a "
                     + "generated id is learned after the fact, via the venue's ExecutionReport echoing it "
                     + "back on the inbound half, not returned synchronously from the send itself.",
            },
        ],
    };

    private static FixSourceConfig ConfigOf(SourceDefinition def) =>
        def.Connector?.Fix ?? throw new InvalidOperationException($"source '{def.Name}' has kind '{SourceKinds.FixDuplex}' but no fix config");
}
