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
///
/// <para><b>Plan 016 wave 6, precedence with <c>@name</c> named endpoints.</b> <c>Peer</c> is checked
/// FIRST, exactly as wave 5 left it: when it is set it wins outright, and neither address field is even
/// looked at — so an <c>Address</c>/<c>RestAddress</c> of the form <c>@name</c> alongside a set
/// <c>Peer</c> is simply never read, the same as a stale literal would be. Only in the "Peer unset" branch
/// — the byte-identical-to-pre-016 branch — are the two address fields run through
/// <see cref="NamedEndpoints.Resolve"/>, one call each, so a literal (including one with an embedded
/// <c>@</c>, e.g. <c>http://user@host:5299</c>, which <see cref="NamedEndpoints.IsReference"/> does not
/// recognize as a reference) passes through unchanged and a bare <c>@name</c> is replaced. This makes the
/// two mechanisms strict alternatives rather than something that could "fight": a source names EITHER a
/// peer OR (optionally, on each address field independently) a named endpoint, never both at once, because
/// naming a peer makes the address fields unreachable by construction. An unresolvable <c>@name</c> throws
/// here too, landing on the exact same status-error path as an unresolvable peer.</para>
/// </summary>
public static class GrpcPeerResolver
{
    public static ResolvedGrpcEndpoints Resolve(StreamForge.Abstractions.GrpcSubConfig config)
    {
        if (string.IsNullOrEmpty(config.Peer))
        {
            return new ResolvedGrpcEndpoints(
                NamedEndpoints.Resolve(config.Address)!, NamedEndpoints.Resolve(config.RestAddress), PeerName: null);
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
