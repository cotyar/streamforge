using StreamForge.AppCore.Discovery;

namespace StreamForge.AppCore.Connectors.Grpc;

/// <summary>
/// Plan 016 wave 5 — resolves the gRPC + REST addresses a federated <c>grpc</c> source dials for ONE
/// (re)connect attempt. Pure and side-effect-free (no I/O, no caching): <see cref="GrpcSubscriberCore"/>
/// calls <see cref="Resolve"/> fresh at the top of every (re)connect — the exact cadence it already uses
/// for its schema snapshot and login — and never remembers the result across attempts, so a peer whose
/// address moved is fixed by reconfiguring the host, with no catalog edit and no restart of the source.
///
/// <para><b>Precedence, pinned on <c>GrpcSubConfig.Peer</c>'s own doc comment</b>: when
/// <see cref="StreamForge.Abstractions.GrpcSubConfig.Peer"/> is set it WINS over both
/// <see cref="StreamForge.Abstractions.GrpcSubConfig.Address"/> and
/// <see cref="StreamForge.Abstractions.GrpcSubConfig.RestAddress"/> outright — it does not merely fill
/// them in when blank. A source that names a peer and also carries a stale literal address must not
/// silently connect to the stale one. Peer unset is byte-identical pre-016 behaviour: the two address
/// fields are returned exactly as authored, with no call into <see cref="PeerDirectory"/> at all.</para>
///
/// <para>An unresolvable peer name, or a peer whose gRPC endpoint is blank, throws
/// <see cref="InvalidOperationException"/> naming the peer and what is missing — the message that lands
/// on <see cref="GrpcSubscriberCore"/>'s existing status-error path at its existing backoff, which is the
/// whole reason this cadence was chosen. A blank REST endpoint does NOT throw here: only the callers that
/// actually need REST (login, id/name-to-display-name resolution) require it, and they name the peer
/// themselves when it's missing — a peer wired for reflection-only federation (no login, a
/// <c>"source:{name}"</c> entity key) needs no REST endpoint at all.</para>
/// </summary>
public static class GrpcPeerResolver
{
    public static ResolvedGrpcEndpoints Resolve(StreamForge.Abstractions.GrpcSubConfig config)
    {
        if (string.IsNullOrEmpty(config.Peer))
        {
            return new ResolvedGrpcEndpoints(config.Address, config.RestAddress, PeerName: null);
        }

        var peer = PeerDirectory.Find(config.Peer);
        if (peer is null)
        {
            throw new InvalidOperationException(
                $"GrpcSubConfig.Peer '{config.Peer}' is not a configured peer.");
        }

        if (string.IsNullOrEmpty(peer.GrpcEndpoint))
        {
            throw new InvalidOperationException(
                $"GrpcSubConfig.Peer '{config.Peer}' has no gRPC endpoint configured — cannot dial it.");
        }

        var restAddress = string.IsNullOrEmpty(peer.RestEndpoint) ? null : peer.RestEndpoint;
        return new ResolvedGrpcEndpoints(peer.GrpcEndpoint, restAddress, PeerName: config.Peer);
    }
}

/// <summary>The addresses resolved for one (re)connect attempt. <see cref="RestAddress"/> may be null even
/// when <see cref="GrpcAddress"/> is set — see <see cref="GrpcPeerResolver"/>'s class remarks.
/// <see cref="PeerName"/> is null when <see cref="StreamForge.Abstractions.GrpcSubConfig.Peer"/> was
/// unset, so callers that need a peer's name in an error message (when the REST address they need turns
/// out to be missing) can tell the two cases apart without re-reading the config.</summary>
public sealed record ResolvedGrpcEndpoints(string GrpcAddress, string? RestAddress, string? PeerName);
