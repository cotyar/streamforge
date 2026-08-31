using System.Collections.Concurrent;

namespace StreamsForge.AppCore.Transports;

/// <summary>
/// Plan 019 (pre-built between waves B/C/D): the rendezvous between a duplex session's OWNER and its
/// outbound CALLER.
///
/// <para>Plan 019 D1 puts the session in the connector driver — Orleans <c>ConnectorGrain</c> (one
/// activation per grain key), Dapr <c>ConnectorActor</c> (one activation per actor id) — because that is
/// the only place in this platform with a single-instance guarantee at all. The sink publisher services
/// that would otherwise have owned it (<c>NatsPublisherService</c>, <c>NatsSinkPublisherService</c>) are
/// plain <c>BackgroundService</c>s with no such guarantee, and an order session must never be duplicated.
/// So a session is <see cref="Publish"/>ed here when it opens and <see cref="Withdraw"/>n when it is
/// disposed, and the stateless proxy sink client (wave 019-B) <see cref="Find"/>s it by source name
/// instead of opening a connection of its own.</para>
///
/// <para><b>Who publishes, and why it is not the driver directly</b>: <c>SubscriberCore</c> owns the
/// connect/backoff loop and constructs a fresh session per attempt via <c>IInboundTransport.Open</c>, so
/// the driver never holds an individual attempt's session object. The publish/withdraw pair therefore
/// belongs in <see cref="IDuplexTransport.OpenDuplex"/> and the session's own <c>DisposeAsync</c> — which
/// is still driver-scoped in effect, because <c>OpenDuplex</c> is only ever reached through the connector
/// grain/actor that owns the source. <b>Every duplex transport owes this pair</b>; it is part of the seam's
/// contract, not an implementation detail, and a transport that forgets it produces a source that ingests
/// happily while its sink can never find the session.</para>
///
/// <para><b>ponytail: process-local, deliberately.</b> A <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// works because both flavours' documented topology is single-instance — see
/// <c>orleans/ARCHITECTURE.md</c>'s note that the push-stream provider is single-silo by design, and the
/// Dapr flavour's single app. The ceiling is exactly that: on a multi-silo deployment the proxy sink and
/// the grain holding the session can land in different processes, and <see cref="Find"/> returns null in
/// the one that does not hold it. The upgrade path is to route the send through the grain/actor by key
/// (the driver is already addressable by source name from anywhere in the cluster) rather than through
/// this map — at the cost of a serialization hop per batch, which is why it is not the first thing built.
/// A duplex sink whose session is not found reports it as a delivery failure with a stated reason; it must
/// never silently succeed.</para>
///
/// <para>Registration is by SOURCE NAME, ordinal — the same key <c>ConnectorGrain</c> and
/// <c>ConnectorActor</c> are themselves addressed by, so there is no second naming scheme to keep in
/// sync.</para>
/// </summary>
public static class DuplexSessions
{
    private static readonly ConcurrentDictionary<string, IDuplexSession> Live = new(StringComparer.Ordinal);

    /// <summary>Announces a freshly-opened session as the live one for <paramref name="sourceName"/>,
    /// replacing whatever was there. Replacement rather than rejection is deliberate: a reconnect opens a
    /// new session while the old one is being torn down, and the NEW one is always the right answer.</summary>
    public static void Publish(string sourceName, IDuplexSession session)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceName);
        ArgumentNullException.ThrowIfNull(session);
        Live[sourceName] = session;
    }

    /// <summary>Removes <paramref name="session"/> if it is still the live one — and does nothing if it is
    /// not.
    ///
    /// <para>The identity check is the whole point, not defensiveness: a stale generation's dispose can run
    /// AFTER the next connection attempt has already published its session (the exact race
    /// <c>ConnectorGrain</c>'s generation counter exists to guard), and a blind remove would unpublish a
    /// perfectly live session and silently break egress until the next reconnect.</para></summary>
    public static void Withdraw(string sourceName, IDuplexSession session)
    {
        if (string.IsNullOrWhiteSpace(sourceName) || session is null)
        {
            return;
        }

        // ConcurrentDictionary's compare-and-remove overload — reference identity, not equality.
        ((ICollection<KeyValuePair<string, IDuplexSession>>)Live)
            .Remove(new KeyValuePair<string, IDuplexSession>(sourceName, session));
    }

    /// <summary>The live session for <paramref name="sourceName"/>, or null when the source is not running,
    /// is not a duplex kind, or (see the ponytail note above) is held by another process.</summary>
    public static IDuplexSession? Find(string sourceName) =>
        string.IsNullOrWhiteSpace(sourceName) ? null : Live.TryGetValue(sourceName, out var s) ? s : null;

    /// <summary>Source names with a live published session. Diagnostics and tests only — never a
    /// dispatch path.</summary>
    public static IReadOnlyList<string> PublishedNames => Live.Keys.ToList();

    /// <summary>Test seam: drops every published session without touching the sessions themselves.
    /// Production code never calls this.</summary>
    public static void ClearForTests() => Live.Clear();
}
