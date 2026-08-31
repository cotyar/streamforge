using System.Threading;
using StreamsForge.Abstractions;

namespace StreamsForge.AppCore.Discovery;

/// <summary>
/// Plan 016 wave 5: the peers this instance knows about, configured at startup and probed over each
/// peer's own anonymous <c>GET /api/meta/instance</c>.
///
/// <para><b>Static, like <see cref="StreamsForge.AppCore.Transports.InboundTransports"/>, and for the same
/// reason.</b> The consumer that needs a peer most is the federated <c>grpc</c> source, whose driver is an
/// Orleans grain / Dapr actor constructed by runtime machinery whose DI container is NOT the host's.
/// Injecting a directory into a grain has already broken this repo's test cluster once. A static registry
/// is legible, has no startup ordering, and behaves identically in a unit test, a silo and an actor.</para>
///
/// <para><b>A configured list, not a service registry.</b> No heartbeat, no persistence, no leader
/// election, no consensus, no failover: a peer is a name, a REST address and a gRPC address, plus whatever
/// the last probe learned. That is enough to unblock federation-by-name and the admin app, which is the
/// actual need. It is <b>not</b> an HA service registry and nothing here should be read as one.</para>
///
/// <para>Names match <b>ordinally</b>, the rule wave 1 pinned for every other entity lookup in this
/// codebase — a peer whose name resolves differently here than in a config file would be worse than no
/// resolution at all.</para>
/// </summary>
// ponytail: a static class, not IPeerDirectory. The plan named an interface with StaticPeerDirectory as
// its first implementation; the interface earns its keep when a second implementation (the self-hosted
// heartbeat variant) exists, and extracting one from this is a ten-line change then.
public static class PeerDirectory
{
    private static readonly Lock Gate = new();
    private static readonly Dictionary<string, PeerRecord> Peers = new(StringComparer.Ordinal);

    /// <summary>Replaces the whole set — configuration is the source of truth, so a reconfigure must be
    /// able to REMOVE a peer, which a merge could not. Entries with a blank name are dropped; a duplicate
    /// name keeps the last one.</summary>
    public static void Configure(IEnumerable<PeerRecord> peers)
    {
        lock (Gate)
        {
            Peers.Clear();
            foreach (var peer in peers)
            {
                if (!string.IsNullOrWhiteSpace(peer.Name))
                {
                    Peers[peer.Name] = peer;
                }
            }
        }
    }

    /// <summary>The peer named <paramref name="name"/>, or null. Exact ordinal match.</summary>
    public static PeerRecord? Find(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return null;
        }

        lock (Gate)
        {
            return Peers.TryGetValue(name, out var peer) ? peer : null;
        }
    }

    /// <summary>Every configured peer, in name order so a listing is stable between calls.</summary>
    public static IReadOnlyList<PeerRecord> All()
    {
        lock (Gate)
        {
            return [.. Peers.Values.OrderBy(p => p.Name, StringComparer.Ordinal)];
        }
    }

    /// <summary>Records the outcome of a probe. <paramref name="info"/> non-null is a success: it stamps
    /// <see cref="PeerRecord.LastSeenAtMs"/> and clears the error. A failure keeps the last-seen stamp, so
    /// "configured but never reachable" (0) and "was reachable and is not now" stay distinguishable
    /// without reading a log. Unknown names are ignored — configuration decides who exists.</summary>
    public static void RecordProbe(string name, InstanceInfo? info, string? error, long nowMs)
    {
        lock (Gate)
        {
            if (!Peers.TryGetValue(name, out var peer))
            {
                return;
            }

            if (info is not null)
            {
                peer.InstanceId = info.InstanceId;
                peer.Info = info;
                peer.LastSeenAtMs = nowMs;
                peer.LastError = null;
            }
            else
            {
                peer.LastError = error;
            }
        }
    }

    /// <summary>Test hook — the registry is process-wide, so a test that configures peers must be able to
    /// put it back.</summary>
    public static void Clear() => Configure([]);
}
