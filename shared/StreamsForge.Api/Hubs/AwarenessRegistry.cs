namespace StreamsForge.Api.Hubs;

/// <summary>
/// Plan 020 wave G — ephemeral presence/liveness for CRDT documents ("who is looking at this document
/// right now"). A host-process singleton modeled directly on
/// <c>StreamsForge.AppCore.Ingest.SourceIngressRegistry</c>, which pins the shape a process-lifetime,
/// in-memory, keyed-by-name registry takes in this codebase: a plain <see cref="Dictionary{TKey,TValue}"/>
/// behind one lock, no persistence, rebuilt from nothing on restart. Nothing here ever reaches
/// <c>CrdtDocGrain</c>, <c>PendingUpdates</c>, the delta journal, or any grain state — a presence entry
/// that touched any of those would have stopped being ephemeral, which is the one line this wave exists
/// not to cross (plan 020's own design point 1).
///
/// <para><b>Per-host, not per-cluster — stated, not discovered.</b> <c>StreamsForge.Api</c> registers
/// SignalR with <c>.AddSignalR().AddJsonProtocol(...)</c> and nothing else — no
/// <c>AddStackExchangeRedis</c>, no backplane of any kind, anywhere in this repository (both flavours'
/// Redis usage is catalog/actor state, not SignalR). A SignalR <c>Group</c> is therefore already scoped
/// to one host process, and this registry inherits exactly that scope: two clients connected to two
/// different host processes (two Orleans silos behind a load balancer, or an Orleans instance and a Dapr
/// one) never see each other's presence, even on the identical document name. That is a real ceiling on
/// what "presence" means in this deployment shape today, not a bug in this class — building a backplane
/// is far outside this wave's scope (see the plan's own "how" note on why wave G is scoped to SignalR and
/// one client and nothing broader).</para>
///
/// <para><b>An entry belongs to one SignalR connection</b>, never to a "user" or a "document" alone — a
/// second browser tab, or a reconnect after a network blip, is a SECOND entry with its own TTL, never a
/// rename of the first. That is the only choice consistent with "an entry expires without a heartbeat":
/// liveness is a property of one live connection, and two tabs from the same person can go stale
/// independently of each other.</para>
///
/// <para><b>The TTL needs no background sweep, and that is deliberate.</b> Every entry point that touches
/// a document's live set (<see cref="Join"/>, <see cref="Heartbeat"/>, <see cref="Leave"/>,
/// <see cref="RemoveConnection"/>) evicts every expired entry in that document FIRST, under the same
/// lock, before doing anything else. So as long as at least one connection on a document keeps
/// heartbeating — which every live participant must do to keep its own entry alive — every OTHER
/// connection's expiry is discovered and reported (via that call's returned snapshot) within one more
/// heartbeat interval of that live participant, with no timer, no hosted service, and no per-document
/// scheduled work sitting idle between calls. The cost is that a document nobody has touched in a while
/// keeps its already-empty entry around until the next call touches it — see
/// <see cref="PruneIfEmptyLocked"/>, which removes it as soon as any call does.</para>
/// </summary>
public sealed class AwarenessRegistry(Func<DateTimeOffset>? clock = null)
{
    private readonly object _gate = new();
    private readonly Dictionary<string, AwarenessDocument> _byDocument = new(StringComparer.Ordinal);

    /// <summary>connectionId -> every documentKey it currently has a live entry under, so a disconnect
    /// (<see cref="RemoveConnection"/>) can find every group to clean up and notify without scanning
    /// every document this registry has ever seen.</summary>
    private readonly Dictionary<string, HashSet<string>> _documentsByConnection = new(StringComparer.Ordinal);

    private readonly Func<DateTimeOffset> _now = clock ?? (() => DateTimeOffset.UtcNow);

    /// <summary>Creates or refreshes this connection's entry on <paramref name="documentKey"/>, subject to
    /// <paramref name="cap"/>. <paramref name="ttl"/>/<paramref name="cap"/> are recorded on the document
    /// and used by every later call (<see cref="Heartbeat"/> in particular needs neither passed back in) —
    /// the freshest join's values win, so a config edit on the source (a new <c>TtlSeconds</c>/
    /// <c>MaxEntries</c>) reaches the document the next time anybody joins it, the same "freshest config
    /// wins, no migration of what's already live" tradeoff <c>SourceIngressRegistry</c>'s own doc comment
    /// makes for its buffers.
    ///
    /// <para>The cap check counts LIVE entries only (expired ones are evicted first) and never counts a
    /// connection that already holds an entry against itself — a heartbeat-via-rejoin from an existing
    /// member must never be refused for a cap it isn't adding to.</para></summary>
    public AwarenessJoinResult Join(
        string documentKey, string connectionId, string clientId, string identity, string? label,
        TimeSpan ttl, int cap)
    {
        lock (_gate)
        {
            var now = _now();
            var doc = GetOrAddDocLocked(documentKey);
            doc.Ttl = ttl;
            doc.Cap = cap;
            EvictExpiredLocked(documentKey, doc, now);

            if (!doc.Entries.ContainsKey(connectionId) && doc.Entries.Count >= cap)
            {
                return AwarenessJoinResult.Rejected(
                    $"awareness cap of {cap} reached for '{documentKey}' ({doc.Entries.Count} active)");
            }

            doc.Entries[connectionId] = new AwarenessEntry(clientId, identity, label, now, now + ttl);
            AddReverseIndexLocked(connectionId, documentKey);
            return AwarenessJoinResult.Accepted(Snapshot(doc));
        }
    }

    /// <summary>Refreshes an EXISTING entry's expiry using the document's own recorded TTL (see
    /// <see cref="Join"/>). Silently a no-op — never creates an entry — when this (documentKey,
    /// connectionId) pair has none, which is what makes it safe for this to stay UNGATED at the hub layer
    /// (<c>StreamHub</c>'s own "Unsubscribe is deliberately ungated" precedent): a caller who never passed
    /// <see cref="Join"/>'s <c>AccessGuard</c> check has no entry to refresh, so calling this cannot become
    /// a back door around that check.
    ///
    /// <para><see cref="AwarenessHeartbeatResult.MembershipChanged"/> is <c>true</c> exactly when this
    /// call's own eviction pass removed at least one OTHER connection's stale entry — the signal
    /// <c>StreamHub</c> uses to decide whether this heartbeat is worth broadcasting to the rest of the
    /// group. An ordinary heartbeat that only refreshes the caller's own expiry changes nothing anyone
    /// else can observe and broadcasts nothing, which is what keeps a live document's steady-state traffic
    /// at "one small message per heartbeat interval per member" rather than "every heartbeat, to every
    /// member" — the flooding this whole wave is written to avoid.</para></summary>
    public AwarenessHeartbeatResult Heartbeat(string documentKey, string connectionId)
    {
        lock (_gate)
        {
            if (!_byDocument.TryGetValue(documentKey, out var doc))
            {
                return new AwarenessHeartbeatResult(ConnectionFound: false, MembershipChanged: false, Peers: []);
            }

            var now = _now();
            var before = doc.Entries.Count;
            EvictExpiredLocked(documentKey, doc, now);
            var changed = doc.Entries.Count != before;

            var found = doc.Entries.TryGetValue(connectionId, out var existing);
            if (found)
            {
                doc.Entries[connectionId] = existing! with { ExpiresAt = now + doc.Ttl };
            }

            var peers = Snapshot(doc);
            PruneIfEmptyLocked(documentKey, doc);
            return new AwarenessHeartbeatResult(found, changed, peers);
        }
    }

    /// <summary>Removes this connection's entry from one document. Returns <c>null</c> when there was
    /// nothing to remove (already expired, or never joined) — <c>StreamHub</c> uses that to skip a
    /// pointless broadcast, not as an error.</summary>
    public IReadOnlyList<AwarenessEntry>? Leave(string documentKey, string connectionId)
    {
        lock (_gate)
        {
            if (!_byDocument.TryGetValue(documentKey, out var doc) || !doc.Entries.Remove(connectionId))
            {
                return null;
            }

            RemoveReverseIndexLocked(connectionId, documentKey);
            var now = _now();
            EvictExpiredLocked(documentKey, doc, now);
            var peers = Snapshot(doc);
            PruneIfEmptyLocked(documentKey, doc);
            return peers;
        }
    }

    /// <summary>Called from <c>StreamHub.OnDisconnectedAsync</c> — a dropped connection never calls
    /// <see cref="Leave"/> for anything it joined, so this is the only place that cleanup happens for it.
    /// Uses the reverse index (<see cref="_documentsByConnection"/>) rather than scanning every document
    /// this registry has ever seen. Returns one row per document the connection actually held a live
    /// entry on, each carrying the bare source name recorded at <see cref="Join"/> time so the hub can
    /// address the right <c>awarenessUpdate</c> payload without re-deriving it from the (environment-
    /// qualified) document key.</summary>
    public IReadOnlyList<(string DocumentKey, string SourceName, IReadOnlyList<AwarenessEntry> Peers)> RemoveConnection(
        string connectionId)
    {
        lock (_gate)
        {
            if (!_documentsByConnection.TryGetValue(connectionId, out var documentKeys))
            {
                return [];
            }

            var results = new List<(string, string, IReadOnlyList<AwarenessEntry>)>();
            foreach (var documentKey in documentKeys.ToArray())
            {
                if (!_byDocument.TryGetValue(documentKey, out var doc) || !doc.Entries.Remove(connectionId))
                {
                    continue;
                }

                var now = _now();
                EvictExpiredLocked(documentKey, doc, now);
                results.Add((documentKey, doc.SourceName, Snapshot(doc)));
                PruneIfEmptyLocked(documentKey, doc);
            }

            _documentsByConnection.Remove(connectionId);
            return results;
        }
    }

    private AwarenessDocument GetOrAddDocLocked(string documentKey)
    {
        if (!_byDocument.TryGetValue(documentKey, out var doc))
        {
            // SourceName is filled in properly by the first Join (the only caller that knows it); a
            // document that somehow only ever sees Heartbeat/Leave calls for a connectionId it doesn't
            // hold — not reachable through StreamHub, which always Joins before either — would keep this
            // placeholder, which is why it is never surfaced anywhere except RemoveConnection's payload.
            doc = new AwarenessDocument { SourceName = "" };
            _byDocument[documentKey] = doc;
        }
        return doc;
    }

    private void AddReverseIndexLocked(string connectionId, string documentKey)
    {
        if (!_documentsByConnection.TryGetValue(connectionId, out var set))
        {
            set = new HashSet<string>(StringComparer.Ordinal);
            _documentsByConnection[connectionId] = set;
        }
        set.Add(documentKey);
    }

    private void RemoveReverseIndexLocked(string connectionId, string documentKey)
    {
        if (!_documentsByConnection.TryGetValue(connectionId, out var set))
        {
            return;
        }
        set.Remove(documentKey);
        if (set.Count == 0)
        {
            _documentsByConnection.Remove(connectionId);
        }
    }

    private void EvictExpiredLocked(string documentKey, AwarenessDocument doc, DateTimeOffset now)
    {
        List<string>? stale = null;
        foreach (var (connectionId, entry) in doc.Entries)
        {
            if (entry.ExpiresAt <= now)
            {
                (stale ??= []).Add(connectionId);
            }
        }
        if (stale is null)
        {
            return;
        }
        foreach (var connectionId in stale)
        {
            doc.Entries.Remove(connectionId);
            RemoveReverseIndexLocked(connectionId, documentKey);
        }
    }

    /// <summary>Bounds this registry's memory to "documents with at least one currently-live entry",
    /// rather than growing one key forever for every distinct document name ever joined over the life of
    /// the process.</summary>
    private void PruneIfEmptyLocked(string documentKey, AwarenessDocument doc)
    {
        if (doc.Entries.Count == 0)
        {
            _byDocument.Remove(documentKey);
        }
    }

    private static IReadOnlyList<AwarenessEntry> Snapshot(AwarenessDocument doc) =>
        doc.Entries.Count == 0
            ? []
            : doc.Entries.Values.OrderBy(e => e.ClientId, StringComparer.Ordinal).ToList();

    private sealed class AwarenessDocument
    {
        public required string SourceName { get; init; }
        public TimeSpan Ttl { get; set; }
        public int Cap { get; set; }
        public Dictionary<string, AwarenessEntry> Entries { get; } = new(StringComparer.Ordinal);
    }
}

/// <summary>One live presence entry. <see cref="Identity"/> is always the AUTHENTICATED caller's own
/// name (<c>Context.User.Identity.Name</c>, resolved by <c>StreamHub</c> — never client-supplied), because
/// presence answers "who is working on this document" and a field trusting arbitrary client input for
/// that would let one connection impersonate another in every other viewer's presence list.
/// <see cref="ClientId"/> and <see cref="Label"/> ARE client-supplied and exist only to distinguish two
/// tabs from the same identity and to carry cosmetic detail (a cursor color, a display name variant) —
/// never used for anything AccessGuard reasons about.</summary>
public sealed record AwarenessEntry(string ClientId, string Identity, string? Label, DateTimeOffset JoinedAt, DateTimeOffset ExpiresAt);

/// <summary>What <see cref="AwarenessRegistry.Join"/> did.</summary>
public sealed class AwarenessJoinResult
{
    public bool Ok { get; private init; }
    public string? Reason { get; private init; }
    public IReadOnlyList<AwarenessEntry> Peers { get; private init; } = [];

    public static AwarenessJoinResult Accepted(IReadOnlyList<AwarenessEntry> peers) => new() { Ok = true, Peers = peers };
    public static AwarenessJoinResult Rejected(string reason) => new() { Ok = false, Reason = reason };
}

/// <summary>What <see cref="AwarenessRegistry.Heartbeat"/> did. See that method's own doc comment for why
/// <see cref="MembershipChanged"/>, not <see cref="ConnectionFound"/>, is the field <c>StreamHub</c> acts
/// on.</summary>
public readonly record struct AwarenessHeartbeatResult(bool ConnectionFound, bool MembershipChanged, IReadOnlyList<AwarenessEntry> Peers);

/// <summary>What <c>StreamHub.SubscribeAwareness</c> returns to the caller that just joined — the two
/// numbers it needs to behave itself (how often to heartbeat, relative to <see cref="TtlSeconds"/>; what
/// <see cref="MaxEntries"/> means for a cap refusal it might see later) plus who else is here right now,
/// so the caller does not need a second round trip to learn its own starting state.</summary>
public sealed record AwarenessSnapshot(int TtlSeconds, int MaxEntries, IReadOnlyList<AwarenessEntry> Peers);
