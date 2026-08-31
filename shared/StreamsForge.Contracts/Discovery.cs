using Orleans;

namespace StreamsForge.Abstractions;

/// <summary>
/// Plan 016 wave 0 — the discovery vocabulary, pre-built by the orchestrator so that wave 5's three
/// concurrent agents (instance identity, the peers/meta endpoints, and the gRPC subscriber's peer
/// resolution) meet on a shape none of them owns.
///
/// <para><b>Discovery is two layers here and layer 1 is most of the value.</b>
/// <c>GET /api/meta/instance</c> is anonymous — like <c>/healthz</c> — and answers "what is this thing,
/// and what can it do"; that alone lets the admin app, the CLI and a human point at an address and learn
/// the rest. Layer 2 is a directory of peers, shipped first as a CONFIGURED list, each entry probed via
/// the same endpoint. A configured list already unblocks the federated <c>grpc</c> source and the admin
/// app, which is the actual need.</para>
///
/// <para><b>What this deliberately is not.</b> Not Consul, etcd, DNS-SD or Redis: a new infrastructure
/// dependency would have to land across two flavours, two compose stacks, two Cloud Run manifests and
/// the admin app, in a repo whose house rule is zero dependencies where avoidable — and the Orleans
/// flavour has no Redis at all, so adding one would break the flavour parity every feature here is held
/// to. Not Orleans membership either: a peer in this plan is a different DEPLOYMENT, not another silo,
/// so it is the wrong scope even under a real clustering provider. When the self-hosted heartbeat variant
/// lands it is in-memory, with no persistence, no leader election and no consensus — it is <b>not</b> an
/// HA service registry and the docs must say so plainly rather than let someone infer otherwise.</para>
/// </summary>
[GenerateSerializer]
public sealed class InstanceInfo
{
    /// <summary>Stable across restarts because it is persisted at <c>{DataDir}/instance.json</c> — a peer
    /// that changed identity every restart would make "is this the same instance I federated from
    /// yesterday" unanswerable, which is the one question a directory exists to answer.</summary>
    [Id(0)] public string InstanceId { get; set; } = "";

    /// <summary>Operator-chosen and NOT unique by construction — the id is the identity. This is what a
    /// peer is addressed by in a `grpc` source, so it is a name a human types.</summary>
    [Id(1)] public string Name { get; set; } = "";

    /// <summary>"orleans" | "dapr".</summary>
    [Id(2)] public string Flavor { get; set; } = "";

    [Id(3)] public string Version { get; set; } = "";

    /// <summary>Where this instance can actually be reached, keyed by protocol ("rest", "grpc"). A map
    /// rather than two properties because the set grows and a peer record that cannot carry a protocol
    /// the reader does not know about is a contract change per protocol.</summary>
    [Id(4)] public Dictionary<string, string> Endpoints { get; set; } = [];

    /// <summary>Feature strings a caller can test for before depending on one — the honest alternative to
    /// inferring capability from <see cref="Version"/>, which requires the caller to know this project's
    /// release history.</summary>
    [Id(5)] public List<string> Capabilities { get; set; } = [];

    /// <summary>Registered connector/transport kinds, so an importer can say "this document needs a kind
    /// this instance does not have" before it applies anything.</summary>
    [Id(6)] public List<string> Plugins { get; set; } = [];

    /// <summary>Entity counts by kind ("sources", "pipelines", "tables") — the cheap "is this the
    /// instance I meant" check that needs no catalog read on the caller's side.</summary>
    [Id(7)] public Dictionary<string, int> CatalogCounts { get; set; } = [];

    /// <summary>Conditions the catalog tolerates but somebody should see: duplicate pipeline names,
    /// broken pins, entities referencing a kind that is not registered here. Surfaced HERE rather than
    /// refused at boot, deliberately — a catalog that was legal when it was written must not become a
    /// host that will not start.</summary>
    [Id(8)] public List<string> CatalogWarnings { get; set; } = [];

    [Id(9)] public long StartedAtMs { get; set; }
}

/// <summary>One known peer. <see cref="RestEndpoint"/> is why this type earns its keep: the federated
/// <c>grpc</c> source needs a REST address to translate an entity id to a name, and requiring the
/// operator to supply it alongside the gRPC address is the friction this removes. Because the consumer
/// already makes that round trip, it can run it in reverse — so an <c>EntityKey</c> can be authored as
/// <c>table:daily_pnl</c> and canonicalised to <c>table:{id}</c>. That is where name resolution and
/// discovery meet, and it is the plan's most user-visible payoff: federation with no hardcoded address
/// and no GUID.</summary>
[GenerateSerializer]
public sealed class PeerRecord
{
    /// <summary>What a `grpc` source names in <c>GrpcSubConfig.Peer</c>.</summary>
    [Id(0)] public string Name { get; set; } = "";

    /// <summary>Empty until the peer has been probed successfully — the field that distinguishes
    /// "configured" from "seen".</summary>
    [Id(1)] public string InstanceId { get; set; } = "";

    [Id(2)] public string RestEndpoint { get; set; } = "";
    [Id(3)] public string GrpcEndpoint { get; set; } = "";

    /// <summary>0 = never reached. Resolution happens at each (re)connect — the cadence
    /// <c>GrpcSubscriberCore</c> already uses for schema snapshots and login — so an unreachable peer
    /// takes the existing status-error path at the existing backoff and becomes fixable without a
    /// restart. No new failure machinery is introduced for it.</summary>
    [Id(4)] public long LastSeenAtMs { get; set; }

    /// <summary>Why the last probe failed, or null. Kept next to <see cref="LastSeenAtMs"/> so
    /// "configured but never reachable" and "was reachable and is not now" are distinguishable without
    /// reading a log.</summary>
    [Id(5)] public string? LastError { get; set; }

    /// <summary>The peer's own answer, when it has been reached. Null before the first successful
    /// probe.</summary>
    [Id(6)] public InstanceInfo? Info { get; set; }
}
