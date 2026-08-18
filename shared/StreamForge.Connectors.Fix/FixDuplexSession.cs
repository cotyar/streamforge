using System.Runtime.CompilerServices;
using System.Threading.Channels;
using QuickFix;
using QuickFix.Logger;
using QuickFix.Store;
using QuickFix.Transport;
using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.Connectors.Fix;

/// <summary>
/// Plan 019 wave E: the <see cref="IDuplexSession"/> for the <c>fix-duplex</c> kind — ONE FIX session
/// covering both an inbound half (<see cref="SubscribeAsync"/>, driven by <c>SubscriberCore</c> exactly
/// like every other message transport) and an outbound half (<see cref="SendAsync"/>, reached by the
/// stateless proxy sink, wave 019-B, through <see cref="DuplexSessions"/>).
///
/// <para><b>Connection lifecycle mirrors <see cref="QuickFixMessageSource"/> exactly</b> — one
/// <see cref="SocketInitiator"/> per connection attempt, built fresh from <see cref="FixSourceConfig"/> via
/// <see cref="QuickFixMessageSource.BuildSettingsText"/> (shared rather than duplicated: unlike the small
/// tag tables this project duplicates elsewhere, this is a whole method with real behaviour, and the two
/// copies WOULD drift). The one difference: this class keeps the <see cref="FixBridgeApplication"/>
/// instance around as <see cref="_app"/> for the life of the connection, because <see cref="SendAsync"/>
/// needs to reach <see cref="FixBridgeApplication.ActiveSessionId"/> — <see cref="QuickFixMessageSource"/>
/// has no outbound half, so it never needed to.</para>
///
/// <para><b><see cref="IsReady"/> and the SessionID <see cref="SendAsync"/> sends to both come from the
/// SAME field</b>, <see cref="FixBridgeApplication.ActiveSessionId"/> — learned in
/// <c>FixBridgeApplication.OnLogon</c>, cleared in <c>OnLogout</c>. There is deliberately no SEPARATE
/// "connected" flag: a session that is connected but not yet logged on cannot accept a send either (there
/// is nothing to address <c>Session.SendToTarget</c> at), so one field answers both questions
/// correctly.</para>
///
/// <para><b>Before <see cref="SubscribeAsync"/> is ever enumerated, <see cref="_app"/> is null and
/// <see cref="IsReady"/> is false</b> — matching <c>FixInboundTransport.Open</c>'s own documented contract
/// ("Open only CONSTRUCTS a subscription; the actual FIX connection happens on first enumeration"). A send
/// attempted before the driver has started reading the inbound half is therefore an ordinary, reported
/// "not ready" failure, not a null-reference crash — see <see cref="SendAsync"/>.</para>
/// </summary>
public sealed class FixDuplexSession : IDuplexSession
{
    private readonly string _sourceName;
    private readonly FixSourceConfig _config;

    private SocketInitiator? _initiator;
    private FixBridgeApplication? _app;

    private long _sentTotal;
    private long _failedTotal;
    private DuplexSendFailure? _lastFailure;

    public FixDuplexSession(string sourceName, FixSourceConfig config)
    {
        _sourceName = sourceName;
        _config = config;
    }

    /// <summary>True once QuickFIX/n's <c>OnLogon</c> has fired for this connection attempt and stays true
    /// until <c>OnLogout</c> or disposal — see this class's doc comment for why one field
    /// (<see cref="FixBridgeApplication.ActiveSessionId"/>) answers both "is there a socket" and "is it
    /// logged on".</summary>
    public bool IsReady => _app?.ActiveSessionId is not null;

    /// <inheritdoc/>
    public long SentTotal => Interlocked.Read(ref _sentTotal);

    /// <inheritdoc/>
    public long FailedTotal => Interlocked.Read(ref _failedTotal);

    /// <inheritdoc/>
    public DuplexSendFailure? LastFailure => Volatile.Read(ref _lastFailure);

    /// <summary>The inbound half — identical to <see cref="QuickFixMessageSource.SubscribeAsync"/>, except
    /// this instance keeps <see cref="_app"/> and <see cref="_initiator"/> as FIELDS (not locals) so
    /// <see cref="SendAsync"/>, called from a completely different call stack (the proxy sink, wave
    /// 019-B), can reach the same live session.</summary>
    public async IAsyncEnumerable<InboundMessage> SubscribeAsync([EnumeratorCancellation] CancellationToken ct)
    {
        var capacity = Math.Max(1, _config.QueueCapacity);
        var channel = Channel.CreateBounded<FixInboundMessage>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
        });

        var settings = new SessionSettings(new StringReader(QuickFixMessageSource.BuildSettingsText(_config)));
        var app = new FixBridgeApplication(_config, channel, capacity);
        _app = app;

        IMessageStoreFactory storeFactory = string.IsNullOrWhiteSpace(_config.StorePath)
            ? new MemoryStoreFactory()
            : new FileStoreFactory(settings); // caller's responsibility in a container: StorePath must be a mounted volume.

        var initiator = new SocketInitiator(app, storeFactory, settings, new NullLogFactory());
        _initiator = initiator;
        initiator.Start();

        try
        {
            await foreach (var msg in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                yield return new InboundMessage(msg.MsgType, msg.Payload, AckAsync: null);
            }
        }
        finally
        {
            initiator.Stop();
            initiator.Dispose();
            _initiator = null;
        }
    }

    /// <summary>The outbound half. NEVER throws — every ordinary failure (a row that cannot be mapped to a
    /// FIX message, the session not being logged on yet, <c>Session.SendToTarget</c> declining the send) is
    /// reported in the returned <see cref="DuplexSendOutcome"/>, per <see cref="IDuplexSession.SendAsync"/>'s
    /// own contract.
    ///
    /// <para><b>Mapping happens BEFORE the readiness check, per row</b> — deliberately, not by accident: a
    /// row this wave cannot map is exactly as much a failure whether or not the session happens to be up
    /// right now, and reporting the REAL reason (a bad column) rather than a generic "not ready" is more
    /// useful to whoever is looking at <see cref="LastFailure"/>. Only rows that DID map are checked
    /// against readiness, and (if ready) actually sent.</para></summary>
    public Task<DuplexSendOutcome> SendAsync(IReadOnlyList<Dictionary<string, object?>> rows, CancellationToken ct)
    {
        var sessionId = _app?.ActiveSessionId;
        var failures = new List<DuplexSendFailure>();
        var sent = 0;

        foreach (var row in rows)
        {
            if (!FixRowMapper.TryBuildMessage(row, out var message, out var mappingFailure))
            {
                failures.Add(new DuplexSendFailure(FixRowMapper.CorrelationIdOf(row), null, mappingFailure!));
                continue;
            }

            if (sessionId is not { } sid)
            {
                failures.Add(new DuplexSendFailure(FixRowMapper.CorrelationIdOf(row), null, "fix-duplex session is not logged on"));
                continue;
            }

            bool accepted;
            try
            {
                accepted = Session.SendToTarget(message, sid);
            }
            catch (Exception ex)
            {
                // Session.SendToTarget's own doc: "true if send was successful, false otherwise" — it is
                // not documented to throw for an ordinary "not found"/"not logged on" case, but this class
                // does not get to assume that forever. Caught here rather than propagated, exactly the
                // ordinary-failure treatment IDuplexSession.SendAsync's own contract requires.
                failures.Add(new DuplexSendFailure(FixRowMapper.CorrelationIdOf(row), null, $"{ex.GetType().Name}: {ex.Message}"));
                continue;
            }

            if (accepted)
            {
                sent++;
            }
            else
            {
                failures.Add(new DuplexSendFailure(FixRowMapper.CorrelationIdOf(row), null, "Session.SendToTarget returned false (session not found or not logged on)"));
            }
        }

        if (sent > 0)
        {
            Interlocked.Add(ref _sentTotal, sent);
        }

        if (failures.Count > 0)
        {
            Interlocked.Add(ref _failedTotal, failures.Count);
            Volatile.Write(ref _lastFailure, failures[^1]);
        }

        return Task.FromResult(new DuplexSendOutcome(sent, failures.Count, failures));
    }

    /// <summary>Stops this connection attempt's socket (if any was ever opened) and withdraws THIS instance
    /// from <see cref="DuplexSessions"/> — the contract <see cref="DuplexSessions"/>'s own doc comment
    /// states every duplex transport owes: "a transport that forgets it produces a source that ingests
    /// happily while its sink can never find the session." <see cref="DuplexSessions.Withdraw"/>'s
    /// reference-identity check means this is safe to call even if a NEWER session has already replaced
    /// this one in the map (the belated-dispose race plan 019 D1/waves C/D name) — it simply does
    /// nothing in that case.</summary>
    public ValueTask DisposeAsync()
    {
        _initiator?.Stop();
        _initiator?.Dispose();
        _initiator = null;
        DuplexSessions.Withdraw(_sourceName, this);
        return ValueTask.CompletedTask;
    }
}
