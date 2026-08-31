using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using QuickFix;
using QuickFix.Fields;
using QuickFix.Logger;
using QuickFix.Store;
using QuickFix.Transport;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Discovery;

namespace StreamsForge.Connectors.Fix;

/// <summary>One application message off a FIX session: the MsgType (tag 35) — the useful discriminator,
/// and what <see cref="StreamsForge.AppCore.Transports.InboundMessage.Subject"/> means for this transport
/// — and the raw SOH-delimited wire bytes exactly as <c>Message.ConstructString()</c> returned them
/// (which is exactly what <c>FixParser</c> parses; see <c>FixInboundTransport</c>'s class doc).</summary>
public sealed record FixInboundMessage(string MsgType, byte[] Payload);

/// <summary>
/// The substitutable seam plan 018-C requires, mirroring
/// <see cref="StreamsForge.AppCore.Connectors.Nats.INatsMessageSource"/>'s role for NATS: everything
/// <see cref="FixInboundTransport"/> needs from an actual FIX session, factored out so the whole test
/// suite except the acceptance test can drive a fake instead of dialing a counterparty.
/// <see cref="QuickFixMessageSource"/> is the one real implementation.
/// </summary>
public interface IFixMessageSource : IAsyncDisposable
{
    /// <summary>Connects (dials <see cref="FixSourceConfig.Host"/>/<see cref="FixSourceConfig.Port"/> as
    /// a FIX initiator) and yields one <see cref="FixInboundMessage"/> per application message that
    /// survives the <see cref="FixSourceConfig.MsgTypes"/> filter and the bounded drop-oldest channel,
    /// until <paramref name="ct"/> is cancelled or the connection ends. Throwing ends this connection
    /// attempt; <c>SubscriberCore</c> reconnects with backoff, exactly like every other
    /// <c>IInboundTransport</c>.</summary>
    IAsyncEnumerable<FixInboundMessage> SubscribeAsync(FixSourceConfig config, CancellationToken ct);
}

/// <summary>
/// Real <see cref="IFixMessageSource"/> over QuickFIX/n: one <see cref="SocketInitiator"/> per connection
/// attempt, built fresh from <see cref="FixSourceConfig"/> in code (no ini file on disk — a
/// <see cref="SessionSettings"/> is constructible from a <see cref="StringReader"/>, and
/// <see cref="BuildSettingsText"/> is that ini text). <c>UseDataDictionary=N</c> is set unconditionally:
/// QuickFIX/n then does the session layer and no message validation, and hands the application message
/// over intact — <c>Message.ConstructString()</c> returns the raw SOH-delimited wire string, which is
/// exactly the <c>byte[]</c> an <c>InboundMessage</c> carries and exactly what <c>FixParser</c> parses.
///
/// <para><b>The bridge from QuickFIX/n's callback thread to this async-enumerable is
/// <see cref="FixBridgeApplication"/></b>, an <see cref="IApplication"/> whose <c>FromApp</c> — a
/// SYNCHRONOUS callback on the session's own thread — writes into a bounded
/// <see cref="System.Threading.Channels.Channel{T}"/> with <see cref="BoundedChannelFullMode.DropOldest"/>.
/// Blocking that callback instead would apply backpressure to the FIX session itself and eventually trip
/// the counterparty's heartbeat timeout — a worse failure than dropping. This is a STATED CEILING, not a
/// bug: correct for market data (a stale quote is worthless), <b>wrong for drop-copy</b>, where every
/// message must survive. The drop count is never swallowed — see <see cref="FixBridgeApplication.Dropped"/>
/// and its logging.</para></summary>
public sealed class QuickFixMessageSource : IFixMessageSource
{
    private SocketInitiator? _initiator;

    public async IAsyncEnumerable<FixInboundMessage> SubscribeAsync(
        FixSourceConfig config, [EnumeratorCancellation] CancellationToken ct)
    {
        var capacity = Math.Max(1, config.QueueCapacity);
        var channel = Channel.CreateBounded<FixInboundMessage>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
        });

        var settings = new SessionSettings(new StringReader(BuildSettingsText(config)));
        var app = new FixBridgeApplication(config, channel, capacity);

        IMessageStoreFactory storeFactory = string.IsNullOrWhiteSpace(config.StorePath)
            ? new MemoryStoreFactory()
            : new FileStoreFactory(settings); // caller's responsibility in a container: StorePath must be a mounted volume.

        var initiator = new SocketInitiator(app, storeFactory, settings, new NullLogFactory());
        _initiator = initiator;
        initiator.Start();

        try
        {
            await foreach (var msg in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                yield return msg;
            }
        }
        finally
        {
            initiator.Stop();
            initiator.Dispose();
            _initiator = null;
        }
    }

    public ValueTask DisposeAsync()
    {
        _initiator?.Stop();
        _initiator?.Dispose();
        _initiator = null;
        return ValueTask.CompletedTask;
    }

    /// <summary>Builds a QuickFIX/n ini-style config string in code — see this class's doc comment for
    /// why no file ever touches disk for this. One [DEFAULT] + one [SESSION] section is enough for a
    /// single always-on (<c>StartTime</c>=<c>EndTime</c>=00:00:00) initiator session.
    ///
    /// <para>Plan 016 wave 6: <see cref="FixSourceConfig.Host"/> is resolved through
    /// <see cref="NamedEndpoints.Resolve"/> right here, before it reaches the ini text — the ONE place
    /// both the receive-only <c>fix</c> kind (<see cref="QuickFixMessageSource.SubscribeAsync"/>) and the
    /// duplex <c>fix-duplex</c> kind (<c>FixDuplexSession</c>, which calls this same method) build their
    /// settings from. Both callers call this fresh at the top of every (re)connect attempt — a brand new
    /// <see cref="SocketInitiator"/> per attempt, never reused — so resolving here IS resolving at connect
    /// time, every connect. An unresolvable <c>@name</c> throws before <c>SessionSettings</c> is even
    /// constructed, which both callers let propagate out of their own <c>SubscribeAsync</c>/connect method
    /// to <c>SubscriberCore</c>'s existing reconnect/backoff/status-error path — the same one a bad literal
    /// host already takes (QuickFIX/n itself would only fail asynchronously, on the socket).</para></summary>
    internal static string BuildSettingsText(FixSourceConfig config)
    {
        var host = NamedEndpoints.Resolve(config.Host);

        var sb = new StringBuilder();
        sb.AppendLine("[DEFAULT]");
        sb.AppendLine("ConnectionType=initiator");
        sb.AppendLine("ReconnectInterval=5");
        sb.AppendLine("StartTime=00:00:00");
        sb.AppendLine("EndTime=00:00:00");
        sb.AppendLine("UseDataDictionary=N"); // plan 018's central decision — see FixSourceConfig's doc.
        if (!string.IsNullOrWhiteSpace(config.StorePath))
        {
            sb.AppendLine($"FileStorePath={config.StorePath}");
        }

        sb.AppendLine();
        sb.AppendLine("[SESSION]");
        sb.AppendLine($"BeginString={config.BeginString}");
        sb.AppendLine($"SenderCompID={config.SenderCompId}");
        sb.AppendLine($"TargetCompID={config.TargetCompId}");
        sb.AppendLine($"SocketConnectHost={host}");
        sb.AppendLine($"SocketConnectPort={config.Port}");
        sb.AppendLine($"HeartBtInt={Math.Max(1, config.HeartBtIntSeconds)}");
        sb.AppendLine($"ResetOnLogon={(config.ResetOnLogon ? "Y" : "N")}");
        if (config.UseSsl)
        {
            sb.AppendLine("SSLEnable=Y");
        }

        return sb.ToString();
    }
}

/// <summary>
/// The <see cref="IApplication"/> that bridges one FIX session to this platform. Every method is called
/// synchronously on QuickFIX/n's own session thread — see <see cref="QuickFixMessageSource"/>'s class doc
/// for why <see cref="FromApp"/> therefore never blocks.
///
/// <para><b>Public, not internal</b> (house style — see <c>DbSource</c>), specifically so
/// <c>FixBridgeApplicationTests</c> can construct one directly and call <see cref="FromApp"/>/
/// <see cref="OnLogon"/> with hand-built <see cref="QuickFix.Message"/> objects — which need no socket at
/// all, since a <see cref="QuickFix.Message"/> is a plain in-memory value once its SOH-delimited text is
/// parsed. That is plan 018-C's "fake seam (no socket)" for the MsgTypes filter and the drop-oldest+
/// counter behaviour; only the acceptance test opens a real one.</para></summary>
public sealed class FixBridgeApplication : IApplication
{
    private readonly FixSourceConfig _config;
    private readonly Channel<FixInboundMessage> _channel;
    private readonly int _capacity;
    private readonly HashSet<string>? _msgTypeFilter;
    private readonly Lock _dropGate = new();
    private long _dropped;

    // Plan 019 wave E: this bridge is shared by both the receive-only `fix` kind and the new `fix-duplex`
    // kind (FixDuplexSession) — the extra state below costs the receive-only path nothing (it is simply
    // never read) and keeps ONE class owning the QuickFIX/n callback surface rather than forking it.
    private readonly Lock _sessionGate = new();
    private SessionID? _activeSessionId;

    public FixBridgeApplication(FixSourceConfig config, Channel<FixInboundMessage> channel, int capacity)
    {
        _config = config;
        _channel = channel;
        _capacity = Math.Max(1, capacity);
        _msgTypeFilter = ParseMsgTypes(config.MsgTypes);
    }

    /// <summary>Cumulative messages dropped by the bounded channel's drop-oldest policy — the "counter the
    /// operator can see" plan 018-C requires. Logged (not swallowed) on every increment; see
    /// <see cref="Enqueue"/>.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>Plan 019 wave E: the SessionID this session logged on as, or null before the first logon /
    /// after a logout — learned in <see cref="OnLogon"/> and refreshed defensively on every outbound
    /// application message in <see cref="ToApp"/> (belt and braces: <see cref="OnLogon"/> is the
    /// authoritative source, <see cref="ToApp"/> just keeps this field from ever going stale while the
    /// session is plainly still up). <see cref="FixDuplexSession.SendAsync"/> reads this to know WHERE to
    /// call <c>Session.SendToTarget</c>, and whether it is null at all is exactly
    /// <see cref="FixDuplexSession.IsReady"/>'s definition of "logged on".</summary>
    public SessionID? ActiveSessionId
    {
        get { lock (_sessionGate) { return _activeSessionId; } }
    }

    /// <summary>The only method with FIX-application logic in it: filter by MsgType (tag 35), then
    /// enqueue. Session-level traffic (Logon/Heartbeat/TestRequest/ResendRequest/SequenceReset/Logout)
    /// never reaches here at all — QuickFIX/n's own session layer consumes it before <c>FromApp</c> is
    /// ever called, which is what makes "receive-only" free to enforce (plan 018's "Decisions").</summary>
    public void FromApp(Message message, SessionID sessionID)
    {
        var msgType = message.Header.GetString(Tags.MsgType);
        if (_msgTypeFilter is not null && !_msgTypeFilter.Contains(msgType))
        {
            return;
        }

        var wire = Encoding.UTF8.GetBytes(message.ConstructString());
        Enqueue(new FixInboundMessage(msgType, wire));
    }

    /// <summary>Bounded, drop-oldest, counted. <see cref="ChannelWriter{T}.TryWrite"/> under
    /// <see cref="BoundedChannelFullMode.DropOldest"/> always succeeds by discarding the oldest queued
    /// item when full — .NET gives no direct signal that it did, so the count is taken from
    /// <see cref="ChannelReader{T}.Count"/> just before the write, under a lock that serializes this
    /// method against itself.
    /// <para><b>Ponytail:</b> that check-then-write pair still races the single background reader
    /// draining the SAME channel — the count can shift by one between the two, so
    /// <see cref="Dropped"/> is a best-effort operator signal ("backpressure is happening, roughly this
    /// much"), not an exact ledger. An exact count would need the reader's dequeue to share this lock
    /// too, which would reintroduce the very backpressure onto the session thread this design exists to
    /// avoid. Upgrade path: a channel implementation that reports drops itself.</para></summary>
    private void Enqueue(FixInboundMessage message)
    {
        lock (_dropGate)
        {
            if (_channel.Reader.Count >= _capacity)
            {
                var n = Interlocked.Increment(ref _dropped);
                Console.Error.WriteLine(
                    $"fix source: dropped {n} message(s) so far — the drop-oldest buffer (queueCapacity="
                    + $"{_capacity}) is full; a slow consumer or a burst is falling behind the session.");
            }

            _channel.Writer.TryWrite(message); // DropOldest: always succeeds, discarding the oldest entry if full.
        }
    }

    public void OnCreate(SessionID sessionID)
    {
    }

    /// <summary>Sends <see cref="FixSourceConfig.OnLogon"/>'s lines, one raw FIX message each, right after
    /// logon succeeds — the only way a market-data session gets anything to receive (plan 018's
    /// "Decisions": no request builder, raw FIX text). Failures are REPORTED, not swallowed: a
    /// <c>MarketDataRequest</c> that never went out means a session that receives nothing, and silence is
    /// the worst possible symptom, so any failure here fails the WHOLE connection attempt by completing
    /// the channel with the exception — <see cref="QuickFixMessageSource.SubscribeAsync"/>'s enumeration
    /// then throws, which the SPI treats exactly like a failed connection (reported, retried with
    /// backoff).</summary>
    public void OnLogon(SessionID sessionID)
    {
        // Plan 019 wave E: the bridge LEARNS its SessionID here, unconditionally — before wave E this was
        // discarded (only used locally to send OnLogon's own lines below); a duplex session's SendAsync
        // needs it kept around for the whole life of this logon, which is exactly this field's scope (see
        // its own doc comment).
        lock (_sessionGate)
        {
            _activeSessionId = sessionID;
        }

        var onLogon = _config.OnLogon;
        if (string.IsNullOrWhiteSpace(onLogon))
        {
            return;
        }

        try
        {
            foreach (var line in SplitOnLogonLines(onLogon))
            {
                var wireLine = ToWireText(line);
                var ok = Session.SendToTarget(new Message(wireLine, false), sessionID);
                if (!ok)
                {
                    throw new InvalidOperationException(
                        $"Session.SendToTarget returned false for an onLogon line: \"{Truncate(line)}\"");
                }
            }
        }
        catch (Exception ex)
        {
            _channel.Writer.TryComplete(new InvalidOperationException($"FIX onLogon request failed: {ex.Message}", ex));
        }
    }

    /// <summary>Completes the channel (no exception) so the enumeration in
    /// <see cref="QuickFixMessageSource.SubscribeAsync"/> ends rather than hanging forever — nothing else
    /// observes a socket disconnect, since <see cref="FromApp"/> simply stops being called. A clean
    /// completion (not a thrown one) is deliberate: it matches <c>SubscriberCore</c>'s "reached here
    /// without throwing -&gt; clean disconnect, reconnect with no backoff" rule, the same treatment every
    /// other transport's ordinary session end gets. <see cref="QuickFixMessageSource.SubscribeAsync"/>'s
    /// <c>finally</c> then stops this attempt's <c>SocketInitiator</c> before <c>SubscriberCore</c> calls
    /// <see cref="FixInboundTransport.Open"/> again for the next attempt.</summary>
    public void OnLogout(SessionID sessionID)
    {
        // Plan 019 wave E: clears the learned SessionID BEFORE completing the channel — a duplex session's
        // IsReady must go false the moment the session is no longer logged on, not one bridge callback
        // later, and SendAsync (FixDuplexSession) reads ActiveSessionId directly with no channel involved.
        lock (_sessionGate)
        {
            _activeSessionId = null;
        }

        _channel.Writer.TryComplete();
    }

    /// <summary>Injects tag 553 (Username) / 554 (Password) into the outgoing Logon(A) message when
    /// configured — QuickFIX/n has no built-in credential exchange, so this is this session project's own
    /// addition, applied only to the Logon message and no other admin traffic.</summary>
    public void ToAdmin(Message message, SessionID sessionID)
    {
        if (message.Header.GetString(Tags.MsgType) != "A")
        {
            return;
        }

        var username = _config.Username;
        if (!string.IsNullOrEmpty(username))
        {
            message.SetField(new StringField(Tags.Username, username));
        }

        var password = _config.Password;
        if (!string.IsNullOrEmpty(password))
        {
            message.SetField(new StringField(Tags.Password, password));
        }
    }

    public void FromAdmin(Message message, SessionID sessionID)
    {
    }

    /// <summary>Plan 019 wave E: no longer a no-op. Fires for every OUTBOUND application message this
    /// session sends — <see cref="OnLogon"/>'s own request lines for the receive-only <c>fix</c> kind, and
    /// (new in this wave) every row <see cref="FixDuplexSession.SendAsync"/> hands to
    /// <c>Session.SendToTarget</c> for the <c>fix-duplex</c> kind. It refreshes
    /// <see cref="ActiveSessionId"/> — a defensive, redundant signal alongside <see cref="OnLogon"/>'s (see
    /// that field's own doc comment) — and deliberately does nothing else: no field mutation, no
    /// validation, no <c>QuickFix.DoNotSend</c>. Required-field validation and message construction are
    /// wave 019-F's job (plan 019 D6); this wave's row → FIX mapping (<see cref="FixRowMapper"/>) already
    /// refuses to build a <see cref="Message"/> it cannot map, so nothing reaches this callback that this
    /// wave could usefully reject anyway.</summary>
    public void ToApp(Message message, SessionID sessionID)
    {
        lock (_sessionGate)
        {
            _activeSessionId = sessionID;
        }
    }

    private static HashSet<string>? ParseMsgTypes(string? msgTypes)
    {
        if (string.IsNullOrWhiteSpace(msgTypes))
        {
            return null;
        }

        var set = new HashSet<string>(StringComparer.Ordinal);
        foreach (var part in msgTypes.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            set.Add(part);
        }

        return set.Count == 0 ? null : set;
    }

    /// <summary>Non-blank lines of <see cref="FixSourceConfig.OnLogon"/>, split on either newline style.</summary>
    private static IEnumerable<string> SplitOnLogonLines(string onLogon) =>
        onLogon.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>Delimiter candidates, precedence order — the SAME doctrine <c>FixParser</c>'s own
    /// (private) sniffer uses: highest count over the sample wins, a tie goes to the earlier candidate,
    /// "nothing counted" falls back to SOH. Duplicated rather than shared because <c>FixParser</c> lives
    /// in <c>StreamsForge.AppCore</c> and keeps this method private; the two are one line of logic each and
    /// diverging would be a worse cost than the duplication.</summary>
    private static readonly char[] DelimiterCandidates = ['\x01', '|', '^'];

    /// <summary>Turns one user-typed FIX line (any of the sniffed delimiters) into real SOH-delimited wire
    /// text a <see cref="Message"/> can parse — a user pastes <c>|</c>-delimited text because SOH doesn't
    /// paste into a text editor (plan 018's own wording for the payload parser applies here verbatim).</summary>
    private static string ToWireText(string line)
    {
        var counts = new int[DelimiterCandidates.Length];
        foreach (var c in line)
        {
            var idx = Array.IndexOf(DelimiterCandidates, c);
            if (idx >= 0) counts[idx]++;
        }

        var best = 0;
        for (var i = 1; i < counts.Length; i++)
        {
            if (counts[i] > counts[best]) best = i;
        }

        var delimiter = counts[best] == 0 ? '\x01' : DelimiterCandidates[best];
        var soh = delimiter == '\x01' ? line : line.Replace(delimiter, '\x01');
        return soh.EndsWith('\x01') ? soh : soh + '\x01';
    }

    private static string Truncate(string s) => s.Length > 60 ? s[..60] + "…" : s;

    /// <summary>The handful of numeric FIX tags this file needs by name, so the callback methods above
    /// read as FIX rather than as magic numbers. Not <c>FixParser</c>'s tag tables (those are private to
    /// that file, and this project has no reference to it in the other direction either).</summary>
    private static class Tags
    {
        public const int MsgType = 35;
        public const int Username = 553;
        public const int Password = 554;
    }
}
