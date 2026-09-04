namespace StreamsForge.Abstractions;

// Plan 006 — connector configuration + runtime-status contracts. Frozen like everything else in
// this assembly: additive evolution only, next free [Id], set-accessors (ORLEANS0101 forbids init
// under cross-assembly codegen). Secret fields (URL header values, gRPC password/token) follow
// D-H secrets-lite: masked as SecretMask in every read path; a written SecretMask value means
// "keep the stored value".

/// <summary>Well-known source kinds (string constants, not an enum — additive like GeneratorProfile).</summary>
public static class SourceKinds
{
    public const string Generator = "generator";
    public const string Url = "url";
    public const string File = "file";
    public const string Folder = "folder";
    public const string Grpc = "grpc";
    /// <summary>Plan 008 W4: client-push ingress. The only kind that is not pull-based — there is no
    /// connector and no timer; rows arrive through IIngressFacade. See <see cref="IngestConfig"/>.</summary>
    public const string Ingest = "ingest";
    /// <summary>Plan 009 B1: a NATS subject subscription. A persistent subscriber like
    /// <see cref="Grpc"/>, not a polled kind — its Schedule is ignored. See <see cref="NatsSubConfig"/>.</summary>
    public const string Nats = "nats";

    /// <summary>Plan 014: a PostgreSQL table or query, polled with a durable cursor. Unlike every kind
    /// above it is driven by <c>IPolledTransport</c> — the pull-shaped seam — and is implemented in
    /// <c>StreamsForge.Connectors.Database</c>, out of the core. See <see cref="DbSourceConfig"/>.</summary>
    public const string Postgres = "postgres";
    /// <summary>Plan 014: the Microsoft SQL Server twin of <see cref="Postgres"/>. Same config, same
    /// transport, a different dialect.</summary>
    public const string MsSql = "mssql";

    /// <summary>Plan 017: Postgres logical replication (pgoutput) read via <c>Npgsql.Replication</c> off a
    /// replication slot + publication, instead of polling a cursor column. Reuses <see cref="DbSourceConfig"/>
    /// — see <see cref="DbSourceConfig.SlotName"/> / <see cref="DbSourceConfig.PublicationName"/>.</summary>
    public const string PostgresCdc = "postgres-cdc";
    /// <summary>Plan 017: SQL Server's built-in CDC capture tables
    /// (<c>cdc.fn_cdc_get_all_changes_*</c>), cursor is the <c>binary(10)</c> LSN. See
    /// <see cref="DbSourceConfig.CaptureInstance"/>.</summary>
    public const string MsSqlCdc = "mssql-cdc";

    /// <summary>Plan 018 wave C: a receive-only FIX session (market data, drop-copy) — a persistent
    /// subscription like <see cref="Nats"/>, not a polled kind, so its Schedule is ignored. Implemented
    /// out of the core in <c>StreamsForge.Connectors.Fix</c>, on <c>QuickFIXn.Core</c>. See
    /// <see cref="FixSourceConfig"/>. This string collides with <see cref="FileFormats.Fix"/> only in
    /// spelling — a <see cref="SourceDefinition.Kind"/> and a <see cref="FileFormats"/> value are two
    /// different registries (kinds pick a transport; formats pick a parser), so the shared spelling is
    /// a coincidence of both being named after the protocol, not a conflict.</summary>
    public const string Fix = "fix";

    /// <summary>Plan 019 wave E: the FIRST duplex kind — one live FIX session whose outbound half also
    /// accepts sends (<c>NewOrderSingle</c> out, <c>ExecutionReport</c> back, same TCP connection, same
    /// sequence-number streams as <see cref="Fix"/>'s receive-only session). A SEPARATE kind from
    /// <see cref="Fix"/> rather than a widening of it — plan 019's "Decisions" (D-shaped) section states
    /// why: <see cref="Fix"/> defaults to an in-memory sequence store and <c>ResetOnLogon=true</c>, right
    /// for market data and wrong for a session that originates orders, and
    /// <c>StreamsForge.AppCore.Transports.DuplexTransports.Register</c> co-registers into
    /// <c>StreamsForge.AppCore.Transports.InboundTransports</c>, which throws on a duplicate kind — so
    /// <see cref="Fix"/> could not be
    /// registered twice even if the two validation regimes could somehow be reconciled. Follows the
    /// platform's existing <see cref="PostgresCdc"/>/<see cref="MsSqlCdc"/> naming shape: the kind names the
    /// mechanism. Reuses <see cref="FixSourceConfig"/> — same config type as <see cref="Fix"/>, only the
    /// registered kind (and therefore which registry opens it as: <c>IDuplexTransport.OpenDuplex</c> instead
    /// of a plain <c>IInboundTransport.Open</c>) differs. Implemented in
    /// <c>StreamsForge.Connectors.Fix.FixDuplexTransport</c>.</summary>
    public const string FixDuplex = "fix-duplex";

    /// <summary>Plan 020 wave B: a CRDT document — a Yjs <c>YDoc</c> that lives in a grain of its own,
    /// accepts updates from edges that may have been offline, merges them, and projects the result to rows.
    /// The FIRST kind since <see cref="Generator"/> and <see cref="Ingest"/> that is neither a transport nor
    /// polled: plan 020 D3 explains at length why it does not fit <c>IInboundTransport</c>'s
    /// bytes→rows-through-a-named-format seam — a Yjs update is a delta against stateful, durable,
    /// per-document state and yields rows only AFTER being merged into it, so bending it into
    /// <c>FormatOf</c> would produce a transport that secretly owns persistence. It is therefore dispatched
    /// to its own grain, exactly as <see cref="Generator"/> is
    /// (<see cref="SourceKindDispatch.ActorKind.Crdt"/>), and is a built-in kind for
    /// <c>SourceValidation</c> / <c>KindVersions</c> rather than a registered transport. See
    /// <see cref="CrdtSourceConfig"/>. Orleans-only for now (plan 020 D9): the Dapr flavor stores the kind
    /// and refuses to START it, the same shape as <see cref="TableDefinition.ShardBy"/>.</summary>
    public const string Crdt = "crdt";

    /// <summary>The masked placeholder for secrets-lite values (D-H).</summary>
    public const string SecretMask = "***";
}

/// <summary>Per-kind connector config container. Exactly one of Url/File/Folder/Grpc/Nats is set,
/// matching <see cref="SourceDefinition.Kind"/>. Schedule applies to url/file/folder kinds (grpc
/// is a persistent subscription — its Schedule is ignored). Mapping applies to url/file/folder/nats.
///
/// <para>Plan 010: a new message-transport kind adds its config property here (next free
/// <c>[Id(n)]</c>, <c>[Secret]</c> on its credential fields) and one <c>IInboundTransport</c>
/// implementation. Nothing in SecretsMasker, the connector drivers, or the validator needs editing —
/// see <c>InboundTransports</c>.</para></summary>
[GenerateSerializer]
public sealed class ConnectorConfig
{
    [Id(0)] public ScheduleSpec? Schedule { get; set; }
    [Id(1)] public UrlPollConfig? Url { get; set; }
    [Id(2)] public FilePollConfig? File { get; set; }
    [Id(3)] public FolderPollConfig? Folder { get; set; }
    [Id(4)] public GrpcSubConfig? Grpc { get; set; }
    [Id(5)] public MappingSpec? Mapping { get; set; }
    /// <summary>Plan 009 B1; set only for <see cref="SourceKinds.Nats"/>.</summary>
    [Id(6)] public NatsSubConfig? Nats { get; set; }

    /// <summary>Plan 014; set for <see cref="SourceKinds.Postgres"/> and <see cref="SourceKinds.MsSql"/>.
    /// One container for both, because the two differ only in dialect — the config surface is identical.
    /// It lives HERE rather than in the connector project because <c>SecretWalk</c> only recurses into
    /// types declared in this assembly, so a config class outside it would export its password in
    /// plaintext, silently — the exact failure <c>[Secret]</c> was introduced to eliminate.</summary>
    [Id(7)] public DbSourceConfig? Db { get; set; }

    /// <summary>Plan 018 wave C; set for <see cref="SourceKinds.Fix"/> AND (plan 019 wave E)
    /// <see cref="SourceKinds.FixDuplex"/> — one config type for both kinds, since they differ only in
    /// which registry opens the session (see <see cref="SourceKinds.FixDuplex"/>'s own doc comment). Lives
    /// HERE for the same <c>SecretWalk</c> reason <see cref="Db"/>'s doc comment gives — a config class declared in
    /// <c>StreamsForge.Connectors.Fix</c> would export <see cref="FixSourceConfig.Password"/> in
    /// plaintext on every config export, because <c>SecretWalk.IsContractClass</c> only recurses into
    /// types declared in THIS assembly.</summary>
    [Id(8)] public FixSourceConfig? Fix { get; set; }

    /// <summary>Plan 020 wave B; set for <see cref="SourceKinds.Crdt"/>. Carries no credential today —
    /// it lives here anyway, with every other connector config, because that is where <c>SecretWalk</c>
    /// can see it if one is ever added (see <see cref="Db"/>'s doc comment for what happens to a config
    /// class declared outside this assembly).</summary>
    [Id(9)] public CrdtSourceConfig? Crdt { get; set; }

    /// <summary>
    /// The open slot for a kind whose config class cannot live in this assembly at all: an OUT-OF-TREE
    /// transport, registered from host startup (see <c>StreamsForge.AppCore.Plugins.StreamsForgePlugins</c>),
    /// whose fields this repo has never heard of. Every typed property above exists because a config class
    /// declared elsewhere is invisible to <c>SecretWalk</c> — this bag closes that hole from the other side:
    /// a string dictionary IS visible, and which of its keys are secret is read off the kind's own
    /// <c>TransportDescriptor</c> (<c>SecretsMasker.MaskSettings</c>) rather than from an attribute the
    /// walker cannot see.
    ///
    /// <para><b>Flat, string-valued, and that is the ceiling.</b> Descriptor field types (number, bool,
    /// select…) are declared as strings here and parsed by the transport — <c>SettingsBag</c> in AppCore has
    /// the readers. A descriptor group with an <c>ObjectKey</c> (a nested, nullable object — "core NATS vs a
    /// JetStream consumer") is the one form this cannot express; a kind that needs it needs a typed class in
    /// this file, and that is the deliberate trade for not having to edit this file at all.</para>
    ///
    /// <para>Never used by a built-in kind. Empty on every source that predates it, and an out-of-tree kind
    /// that ADDS a field (a fourth environment dimension, say) changes nothing here: a new key in a bag is
    /// not a schema change, so no <c>[Id(n)]</c> is spent and no import of an older document breaks.</para>
    /// </summary>
    [Id(10)] public Dictionary<string, string> Settings { get; set; } = [];
}

/// <summary>
/// Plan 020 wave B — a CRDT document source. Deliberately two fields: everything else this kind could
/// plausibly configure is either a decision the plan already made for good (<c>Gc = true</c>, D8) or a
/// wave that has not landed.
///
/// <para><b>The document's shape is a contract, not a convention.</b> The root <see cref="RootMap"/> is a
/// <c>YMap</c> whose keys are ENTITY KEYS and whose values are that entity's attributes (a nested
/// <c>YMap</c>, or a scalar for a single-column entity). One key projects to one row, carrying its key in
/// <see cref="KeyField"/>. The alternative — a root map that IS one entity's attributes, projecting the
/// whole document to a single row — was rejected because it leaves per-entity deletion inexpressible, and
/// deletion is the half of this feature that goes wrong silently.</para>
///
/// <para><b>Deletion reuses the platform's existing vocabulary rather than inventing a tombstone.</b>
/// Plan 020's projector section calls for "an explicit tombstone convention (a reserved field on the
/// projected row)". That convention already exists and is spoken by two subsystems: a CDC row carries
/// <c>_op</c> (<c>c</c>/<c>u</c>/<c>d</c>) and <c>_weight</c> (<c>+1</c>/<c>-1</c>) — see
/// <c>StreamsForge.Connectors.Database.CdcStamp</c> — and the database sink planner already turns
/// <c>_weight = -1</c> into a <c>DELETE</c>. Removing a key from the root map therefore emits exactly what
/// a Postgres <c>DELETE</c> emits, so one piece of downstream SQL covers a CDC feed and a CRDT document
/// alike. Inventing a third spelling would have meant every consumer learning it.</para>
///
/// <para><b>Those two stamps reach a SINK. They do not, on their own, converge a TABLE</b> — found in
/// wave B-2's live check, not reasoned about. <c>_weight</c> on an inbound row is just a column; the
/// Engine's Z-set weights are computed FROM table SQL, never carried in from ingress, so a tombstone
/// arrives as one more <c>+1</c> assert and the table ends up holding the old row AND an all-null row
/// for the same key. <c>CdcEnvelope</c>'s class doc states this limit for Debezium deletes in as many
/// words — CDC has always had it. The tombstone therefore carries a THIRD stamp, <c>_retract = true</c>
/// (<c>IngressRowAcceptance.RetractField</c>), which the Engine's <c>TableIngestOp</c> honours
/// unconditionally: a <c>LATEST BY</c> table receiving one genuinely frees the key. Three stamps, three
/// readers — <c>_op</c> for SQL, <c>_weight</c> for a sink, <c>_retract</c> for a table.</para>
/// </summary>
[GenerateSerializer]
public sealed class CrdtSourceConfig
{
    /// <summary>Name of the root <c>YMap</c> inside the document — the map whose keys are entity keys.
    /// Must match what the edge writes into; there is no discovery, because a document with no writes yet
    /// is indistinguishable from a document whose root is named something else.</summary>
    [Id(0)] public string RootMap { get; set; } = "root";

    /// <summary>Column the entity's key is projected into. Must be declared in
    /// <see cref="SourceDefinition.Fields"/> like any other column — the projector does not invent
    /// schema.</summary>
    [Id(1)] public string KeyField { get; set; } = "id";

    /// <summary>Plan 020 wave D, finding 2. Off by default — an existing document's behaviour is
    /// unchanged unless an operator opts in. When <c>true</c>, <c>CrdtEndpoints</c> decodes every
    /// update BEFORE merging it (<c>Ycs.UpdateOperations.DecodeUpdate</c>, never applying anything) and
    /// works out which entity key(s) it touches, then checks <see cref="StreamsForge.AppCore.Access.Actions.SourceWrite"/>
    /// against a per-entity scope (<c>"{sourceName}/{entityKey}"</c>) in addition to the coarse
    /// per-document check every route already makes.
    ///
    /// <para><b>The honest boundary, stated once here rather than left for the reader to discover</b>:
    /// which entity (and, one level deep, which field) an update touches is recoverable ONLY when the
    /// item's whole ancestor chain — up to the configured <see cref="RootMap"/> — is present in THAT
    /// SAME update frame. A <c>YMap</c> value's parent, as decoded (not yet applied), is either the
    /// literal name of a root type or the <see cref="Ycs.ID"/> of the item that defined its container;
    /// resolving an ID means finding that exact struct among the ones this same frame decoded.  An edit
    /// nested under a map/entity created in an EARLIER update — the ordinary case, since an edge usually
    /// creates an entity once and edits it many times afterwards — or under one that lives only in the
    /// document's already-applied state, cannot be resolved this way at all: the defining struct simply
    /// is not in the bytes being inspected. The inspector does not guess in that case; it reports the
    /// update as undecidable, and — because this flag is on — the update is REFUSED rather than merged.
    /// A refusal here never aborts the rest of the batch (matches <c>CrdtDocGrain.MergeAsync</c>'s own
    /// corrupt-frame handling) and always names itself in <see cref="CrdtMergeResult.Diagnostics"/>.</para>
    ///
    /// <para><b>Cost.</b> Turning this on for an already-populated document, without ALSO granting
    /// entity-scoped (or <c>*</c>/prefix) permissions, denies every subsequent edit outright — a grant
    /// scoped to the bare source name does not widen to <c>"{name}/{key}"</c> under the platform's own
    /// exact-match scope grammar (015), the same way a <c>prod-*</c> grant does not widen to <c>*</c>.
    /// It also multiplies audit volume: each touched entity key gets its OWN
    /// <see cref="StreamsForge.Api.Auth.AccessGuard"/> decision (and, by that guard's existing policy, its
    /// own audit row when allowed), on top of the one row the coarse per-document check already writes.
    /// See <c>CrdtEndpoints</c>'s own comment at the call site for the exact scope string and the
    /// deliberate choice not to gate <c>/crdt/replay</c> by it (that route re-asserts the WHOLE document
    /// by design, so a per-entity filter there would silently under-deliver a replay rather than refuse
    /// it honestly).</para></summary>
    [Id(2)] public bool RequireEntityAuthorization { get; set; }

    /// <summary>Plan 020 wave D, finding 3. Off by default. When <c>true</c>, a successful
    /// <c>/crdt/updates</c> call maps every Yjs client id that contributed to an accepted update onto
    /// the REST caller's identity, using <c>Ycs.PermanentUserData.SetUserMapping</c> — durable
    /// attribution written INTO the document itself, the same mechanism y-prosemirror's change tracking
    /// is built on.
    ///
    /// <para><b>The tension this creates with D8, stated rather than hidden.</b> D8 chose
    /// <c>Gc = true</c> deliberately, because history for a twin belongs in the delta journal and row
    /// history (both with retention), and an undeletable document would make personal data
    /// undeletable with it. <c>PermanentUserData</c> is the opposite instinct: it writes a permanent,
    /// append-only <c>users</c> map INTO the document, and nothing in this platform ever compacts or
    /// expires it. That is why this is opt-in rather than the default this document already has — an
    /// operator turning it on is choosing to carry that cost for the documents that need "who wrote
    /// this" answered from inside the document itself.</para>
    ///
    /// <para><b>What this buys, precisely, and what it does not.</b> <c>SetUserMapping</c>'s clientId
    /// -&gt; description mapping (<c>GetUserByClientId</c>) is written unconditionally and works for a
    /// remotely-applied update exactly as it would for a local edit — the grain calls
    /// <c>SetUserMapping</c> itself, synchronously, right after a merge, so it does not depend on Ycs's
    /// transaction-local bookkeeping at all. <c>GetUserByDeletedId</c> — "who deleted this" — is a
    /// DIFFERENT half of the same class, and it is NOT populated by this flag: Ycs only records a
    /// deletion into a user's delete-set from an <c>AfterTransaction</c> handler gated on
    /// <c>Transaction.Local</c>, and <c>YDoc.ApplyUpdateV1</c> — the only way this grain ever mutates a
    /// document — defaults <c>local</c> to <c>false</c> and this grain never overrides it (asserting
    /// <c>local: true</c> on a store-and-forward apply would be describing an edge's remote edit as this
    /// grain's own local one, which is not true and would have unknown knock-on effects on whatever else
    /// in Ycs keys off that flag). So "who wrote entity X" is answered by this feature; "who deleted
    /// entity X" is not — see <c>CrdtDocGrain</c>'s own comment at the call site, and
    /// <c>CrdtAttributionTests</c> for the empirical check that pins this exact gap rather than assuming
    /// it.</para></summary>
    [Id(3)] public bool AttributeChanges { get; set; }

    /// <summary>Plan 020 wave F. <c>null</c> (the default) means this document carries no bounded
    /// counter at all — every escrow route/field on this document answers accordingly ("no escrow
    /// counter configured"), and an existing document's behaviour is completely unaffected by this
    /// wave landing. See <see cref="CrdtEscrowConfig"/> for the shape and the four limits it exists to
    /// keep visible rather than let an operator discover the hard way.</summary>
    [Id(4)] public CrdtEscrowConfig? Escrow { get; set; }

    /// <summary>Plan 020 wave G. <c>null</c> (the default) means awareness is OFF for this document —
    /// <c>StreamHub.SubscribeAwareness</c> refuses to join anybody to it, and an existing document's
    /// behaviour is completely unaffected by this wave landing. Awareness never reaches this class's
    /// document at all — it lives entirely in a host-process registry (<c>AwarenessRegistry</c>,
    /// <c>shared/StreamsForge.Api/Hubs/</c>), never in <c>CrdtDocGrainState</c> — this field only
    /// carries the two numbers an operator has to decide before turning it on. See
    /// <see cref="CrdtAwarenessConfig"/> for what they mean.</summary>
    [Id(5)] public CrdtAwarenessConfig? Awareness { get; set; }
}

/// <summary>
/// Plan 020 wave F — a bounded counter (Balegas et al., 2015) over a set of NAMED replicas (a
/// warehouse, a shop floor, a vessel — see the class's own limit below on what this is NOT for),
/// living inside the SAME document as an ordinary <c>YMap</c>. No new Ycs type: the plan's own
/// observation is that under single-writer-per-key discipline (only replica <c>i</c> ever writes
/// <c>d:i</c> or <c>t:i:*</c>) a <c>YMap</c>'s last-writer-wins is never actually exercised, so the
/// counter needs nothing beyond keys on a map real Yjs can already read — see
/// <c>EscrowCounter</c> (<c>StreamsForge.Connectors.Crdt</c>) for the key scheme (<c>d:&lt;replica&gt;</c>,
/// <c>t:&lt;from&gt;:&lt;to&gt;</c>) and the local-allowance formula this class's own doc references.
///
/// <para><b>The four limits, restated here because they are what an operator reads before turning this
/// on, not a footnote:</b></para>
/// <list type="number">
///   <item><b>One-sided numeric bounds only.</b> "Total across these three fields stays consistent" is
///   a DIFFERENT mechanism — plan 020 wave E's reconciliation (<c>orleans/docs/index.html#crdt-reconcile</c>),
///   already shipped. This class answers exactly one question: can replica <paramref name="Replica"/> —
///   see <see cref="EscrowCounter"/> — spend <c>N</c> more without the GLOBAL sum ever breaching
///   <see cref="InitialAllowance"/>'s total.</item>
///   <item><b>Rebalancing is pairwise coordination — an ONLINE operation.</b> A replica that has spent
///   its whole share stops (every further spend is refused, visibly — see <c>EscrowSpendResult</c>)
///   until allowance is moved to it, which can only happen through <c>ICrdtFacade.RebalanceAsync</c> /
///   <c>POST /api/sources/{name}/crdt/escrow/rebalance</c> while the document is reachable. There is no
///   offline rebalance: unlike an ordinary content edit, a transfer needs the CURRENT merged state of
///   both replicas (how much <c>from</c> actually holds right now) to decide honestly, which an edge
///   working from its own last-synced copy cannot answer for a peer it has not seen.</item>
///   <item><b>Key count is O(replicas²)</b> (one <c>t:</c> key per ordered pair that has ever
///   transferred). This is a mechanism for a HANDFUL of named sites, not thousands of browser tabs —
///   <see cref="InitialAllowance"/> is a small, operator-typed dictionary, not something generated per
///   session.</item>
///   <item><b>The allocation policy is domain knowledge and is configured, never inferred.</b>
///   <see cref="InitialAllowance"/> is the whole policy: which replicas exist and what each starts
///   with. A replica name absent from it is not a silent zero — every operation naming an undeclared
///   replica is refused, the same "loud and refused beats plausible and wrong" standard
///   <c>CrdtProjector</c> already holds itself to for an unrecognized Y-type.</item>
/// </list>
/// </summary>
[GenerateSerializer]
public sealed class CrdtEscrowConfig
{
    /// <summary>Name of the top-level <c>YMap</c> holding this counter's <c>d:</c>/<c>t:</c> entries.
    /// Deliberately a SIBLING of <see cref="CrdtSourceConfig.RootMap"/>, never nested inside it: the
    /// counter's bookkeeping keys are not an entity and must never enumerate as a projected row (see
    /// <c>CrdtProjector.Flatten</c>, which only ever reads <see cref="CrdtSourceConfig.RootMap"/>).</summary>
    [Id(0)] public string CounterMap { get; set; } = "escrow";

    /// <summary>Replica name -&gt; the allowance it holds before any transfer. The bound <c>K</c> is the
    /// SUM of these values — not a separate field, because a transfer only ever moves allowance between
    /// replicas (see <c>EscrowCounter.Transfer</c>'s own doc comment for why the sum is transfer-invariant),
    /// it never creates or destroys it, so <c>K</c> is exactly what this dictionary adds up to on day
    /// one and stays that way for the life of the counter. Configured, never inferred (limit 4 above):
    /// an operator names every site this counter will ever cover before the first spend, and the
    /// bound this whole feature exists to protect is nothing more than what they wrote down.</summary>
    [Id(1)] public Dictionary<string, long> InitialAllowance { get; set; } = new();

    /// <summary>Plan 020 wave F — the name of a declared replica that exists ONLY to hold unallocated
    /// allowance and <b>never spends</b>. Empty (the default) means there is none, and the coordinator
    /// rebalance RPC is then unusable: see <c>EscrowCounter.TryCoordinatorTransfer</c> for why that is a
    /// safety rule and not an inconvenience.
    ///
    /// <para><b>The reason this field exists at all.</b> Moving allowance OUT of a replica is only sound
    /// if that replica has already deducted it from its own view — otherwise it can still spend what it
    /// no longer has, and the merged result breaches the bound. A replica that never spends cannot have
    /// an unsynced spend, so the coordinating document's view of the reserve is never stale, and a
    /// transfer out of it is safe for the coordinator to write. Transfers between two SPENDING replicas
    /// are not the coordinator's to make; the giver performs them on its own document (
    /// <c>EscrowCounter.TryTransfer</c>) and ships the result as an ordinary update, which deducts first
    /// and can therefore never oversell.</para></summary>
    [Id(2)] public string ReserveReplica { get; set; } = "";
}

/// <summary>
/// Plan 020 wave G — turns on ephemeral presence/liveness ("who is looking at this document right now")
/// for one CRDT source. Non-null is the opt-in itself; there is no separate boolean, matching
/// <see cref="CrdtEscrowConfig"/>'s own "presence of config IS the flag" idiom two fields up.
///
/// <para><b>What this does NOT configure, on purpose.</b> There is no field here for WHO may join —
/// that is <c>StreamHub.SubscribeAwareness</c> asking <c>AccessGuard</c> for
/// <see cref="Actions.SourceRead"/> at this source's own name/tags, the exact
/// action and scope a REST read of this source already asks for (plan 015's grant model, unmodified —
/// see <c>CrdtEndpoints</c>' own "Finding 1, verified rather than rebuilt" for why a second ACL
/// mechanism is not what a wave like this should add). This class only carries the two numbers that
/// decide what an authorized presence entry looks like once it exists.</para>
///
/// <para><b>Both numbers are mandatory operator decisions, not defaults to leave alone.</b> The plan's
/// own framing for this wave: awareness is "the thing most likely to flood a link", so turning it on at
/// all is meant to be a deliberate act against a measurement, not a config toggle with harmless-looking
/// defaults. <see cref="TtlSeconds"/> and <see cref="MaxEntries"/> both still carry sane defaults (30s,
/// 50) so a minimal `{"ttlSeconds":30,"maxEntries":50}` — or, once the SPA/console grows a form for this,
/// accepting the shown defaults — is a real, working choice, not a landmine; but the class stays
/// non-null-to-opt-in specifically so an operator has to write the two numbers down at least once.</para>
/// </summary>
[GenerateSerializer]
public sealed class CrdtAwarenessConfig
{
    /// <summary>Seconds a presence entry survives without a heartbeat. A connection that stops calling
    /// <c>Heartbeat</c> (tab closed without a clean disconnect, laptop asleep, link dropped) disappears
    /// from every other client's view within roughly one more heartbeat interval of ANY still-live
    /// member of the same document — see <c>AwarenessRegistry</c>'s own doc comment for why that needs
    /// no background timer. Default 30.</summary>
    [Id(0)] public int TtlSeconds { get; set; } = 30;

    /// <summary>Hard cap on LIVE (non-expired) entries per document. The (N+1)th distinct connection to
    /// join a document already at this cap is refused outright — <c>AwarenessRegistry.Join</c> returns a
    /// refusal naming the cap and the current count, and <c>StreamHub.SubscribeAwareness</c> turns that
    /// into a <c>HubException</c> the caller can show a human, rather
    /// than silently dropping the join or silently evicting somebody already present. This is a
    /// mechanism for a bounded set of live editors/viewers on ONE document, not a general presence
    /// service — see the class's own remarks on why the numbers here are operator decisions rather than
    /// defaults to leave alone. Default 50.</summary>
    [Id(1)] public int MaxEntries { get; set; } = 50;
}

/// <summary>Plan 009 B1: NATS subject subscription. Credentials follow the secrets-lite convention
/// plan 006 established — read back as <see cref="SourceKinds.SecretMask"/>, and sending the mask on
/// write means "keep the stored value".</summary>
[GenerateSerializer]
public sealed class NatsSubConfig
{
    /// <summary>Server URL(s), comma-separated, e.g. "nats://localhost:4222".</summary>
    [Id(0)] public string Url { get; set; } = "";
    /// <summary>Subject to subscribe to; NATS wildcards (<c>*</c>, <c>&gt;</c>) are the server's to
    /// interpret, not ours.</summary>
    [Id(1)] public string Subject { get; set; } = "";
    /// <summary>Queue group. Two replicas sharing one group split the subject between them instead of
    /// both ingesting every message — which is this path's answer to the per-replica problem that
    /// IngestStatus.Aggregated documents on the push-ingress side. Empty = every replica gets
    /// everything, which is rarely what you want with more than one instance.</summary>
    [Id(2)] public string QueueGroup { get; set; } = "";
    /// <summary>Payload format, same vocabulary as the file/folder connectors ("ndjson" | "json" | "csv").</summary>
    [Id(3)] public string Format { get; set; } = "json";
    [Id(4)] [Secret] public string? Token { get; set; }
    [Id(5)] public string? Username { get; set; }
    [Id(6)] [Secret] public string? Password { get; set; }
    /// <summary>Contents of a .creds file, not a path — the catalog has to be portable across hosts.</summary>
    [Id(7)] [Secret] public string? Credentials { get; set; }
    /// <summary>Null (default) = core NATS subscribe: at-most-once, no cursor, nothing left behind on
    /// the server. Non-null opts into a JetStream durable consumer, which gets redelivery and acks at
    /// the price of server-side state this platform then owns.</summary>
    [Id(8)] public NatsJetStreamConfig? JetStream { get; set; }
    /// <summary>Null (default) = today's behaviour unchanged: no <c>TlsOpts</c> is set on the
    /// connection, so a plain <c>nats://</c> URL stays plaintext and a <c>tls://</c> URL still gets
    /// system-trust TLS exactly as before this field existed. Non-null opts into client certs and/or a
    /// private CA — see <see cref="NatsTlsConfig"/>.</summary>
    [Id(9)] public NatsTlsConfig? Tls { get; set; }
}

/// <summary>Plan (NATS TLS): client-side TLS material for a NATS connection, layered on top of the
/// <c>tls://</c> URL scheme rather than replacing it — a <c>tls://</c> URL with system trust needs no
/// entry here at all, this only exists for a private CA and/or mutual TLS. <see cref="CaFile"/>,
/// <see cref="CertFile"/> and <see cref="KeyFile"/> are PATHS on the HOST's filesystem, the same
/// convention <c>FileSinkConfig.Path</c> and <see cref="FixSourceConfig.StorePath"/> already use — not
/// secrets themselves (a path reveals nothing), so none of the three carries <see cref="SecretAttribute"/>
/// and none is masked on read. <see cref="InsecureSkipVerify"/> disables server certificate validation
/// entirely and exists for local/dev brokers with a self-signed cert nobody has trusted yet; it must
/// never be set against a broker that matters.</summary>
[GenerateSerializer]
public sealed class NatsTlsConfig
{
    /// <summary>Path to a CA bundle to trust, on the host's filesystem, instead of (or in addition to)
    /// the system trust store.</summary>
    [Id(0)] public string? CaFile { get; set; }
    /// <summary>Path to a client certificate, on the host's filesystem — set together with
    /// <see cref="KeyFile"/> for mutual TLS.</summary>
    [Id(1)] public string? CertFile { get; set; }
    /// <summary>Path to the client certificate's private key, on the host's filesystem.</summary>
    [Id(2)] public string? KeyFile { get; set; }
    /// <summary>Skips server certificate validation entirely. DEV-ONLY — never set this against a
    /// broker that matters.</summary>
    [Id(3)] public bool InsecureSkipVerify { get; set; }
}

/// <summary>Plan 009 B1: opt-in JetStream durable consumer. Deliberately not the default — a durable
/// consumer nobody drains is a server-side resource we would create and never clean up.</summary>
[GenerateSerializer]
public sealed class NatsJetStreamConfig
{
    [Id(0)] public string Stream { get; set; } = "";
    /// <summary>Durable consumer name. Two replicas sharing it share the work; distinct names each get
    /// their own cursor over the whole stream.</summary>
    [Id(1)] public string Durable { get; set; } = "";
    /// <summary>Max in-flight unacked messages, the JetStream-side analogue of an ingress buffer bound.</summary>
    [Id(2)] public int MaxAckPending { get; set; } = 1000;
}

/// <summary>Plan 018 wave C: a receive-only FIX session — market data or drop-copy, never order entry
/// (that is plan 019, a different plan, not a later wave of this one). One session, one connection: this
/// platform is always the FIX INITIATOR, dialing out to <see cref="Host"/>/<see cref="Port"/>; the
/// counterparty is always the acceptor. <see cref="FormatOf"/> on the transport is a constant
/// (<see cref="FileFormats.Fix"/>) — a FIX session speaks FIX, there is nothing to choose here the way
/// url/file/folder/nats sources choose a payload format.
///
/// <para><b>No FIX dictionary ships with this platform</b> (plan 018's "Decisions" section) —
/// <c>UseDataDictionary=N</c> is set unconditionally by the session project, so <see cref="BeginString"/>
/// only selects which version header this session claims, never a schema to validate against.</para>
///
/// <para><b>Session state defaults to in-memory, file-backed on request.</b> <see cref="StorePath"/> empty
/// (the default) means <c>MemoryStoreFactory</c> — no persisted sequence numbers, paired with
/// <see cref="ResetOnLogon"/> defaulting true: a market-data session normally wants a clean slate every
/// logon, because resending yesterday's quotes is worse than not resending them. Setting
/// <see cref="StorePath"/> switches to <c>FileStoreFactory</c> for a drop-copy session that must not lose
/// its place across restarts — in a container that path must be a mounted volume, exactly as
/// <c>FileSinkConfig.Path</c>'s doc comment already says for the file sink.</para>
///
/// <para><b><see cref="OnLogon"/> is raw FIX text, not a request builder.</b> A market-data session must
/// SEND something (a <c>MarketDataRequest</c>, a <c>SecurityListRequest</c>, …) after logon to receive
/// anything at all; this field holds one raw FIX message per line, delimiter-sniffed the same way
/// <c>FixParser</c> sniffs a payload — SOH, <c>|</c> or <c>^</c>, whichever a user's pasted text actually
/// uses. No templating, no request/response correlation, no resubscribe-on-reject: a typed request
/// builder is a plan-019-sized decision, not a field on this class.</para></summary>
[GenerateSerializer]
public sealed class FixSourceConfig
{
    /// <summary>Counterparty host to dial. This platform is always the initiator.</summary>
    [Id(0)] public string Host { get; set; } = "";
    [Id(1)] public int Port { get; set; }
    /// <summary>This side's SenderCompID (tag 49 on outbound, TargetCompID on inbound).</summary>
    [Id(2)] public string SenderCompId { get; set; } = "";
    /// <summary>The counterparty's CompID (tag 56 on outbound).</summary>
    [Id(3)] public string TargetCompId { get; set; } = "";
    /// <summary>FIX version header, e.g. "FIX.4.4". See this class's doc comment for why there is no
    /// dictionary behind it.</summary>
    [Id(4)] public string BeginString { get; set; } = "FIX.4.4";
    /// <summary>Optional: sent as tag 553 (Username) inside the Logon(A) message when non-empty.
    /// QuickFIX/n has no built-in credential exchange — this is the session project's own addition.</summary>
    [Id(5)] public string? Username { get; set; }
    /// <summary>Optional: sent as tag 554 (Password) inside the Logon(A) message when non-empty. See
    /// <see cref="Username"/>.</summary>
    [Id(6)] [Secret] public string? Password { get; set; }
    /// <summary>Session heartbeat interval, seconds. Must be &gt; 0.</summary>
    [Id(7)] public int HeartBtIntSeconds { get; set; } = 30;
    /// <summary>Reset sequence numbers to 1 on every logon. See this class's doc comment for why true is
    /// the market-data-shaped default.</summary>
    [Id(8)] public bool ResetOnLogon { get; set; } = true;
    /// <summary>Empty (default) = in-memory sequence-number store, reset on every process restart. Set to
    /// switch to a file-backed store — see this class's doc comment.</summary>
    [Id(9)] public string StorePath { get; set; } = "";
    /// <summary>Wraps the socket in TLS. Deferred beyond this bare flag: client certificates, CA pinning
    /// (plan 018's "Deferred" list).</summary>
    [Id(10)] public bool UseSsl { get; set; }
    /// <summary>One raw FIX message per line, sent via <c>Session.SendToTarget</c> right after logon
    /// succeeds. See this class's doc comment.</summary>
    [Id(11)] public string? OnLogon { get; set; }
    /// <summary>Comma-separated include-filter over MsgType (tag 35) values, e.g. "W,X". Empty (default)
    /// = every application message that reaches <c>FromApp</c> becomes a row. Session-level traffic
    /// (Logon/Heartbeat/TestRequest/ResendRequest/SequenceReset/Logout) never reaches <c>FromApp</c> at
    /// all — QuickFIX/n's own session layer consumes it — so this filter has nothing to do with those.</summary>
    [Id(12)] public string MsgTypes { get; set; } = "";
    /// <summary>Capacity of the bounded, drop-oldest queue bridging QuickFIX/n's own callback thread to
    /// this platform's async consumption of the subscription. See <c>FixInboundTransport</c>'s class doc
    /// for the backpressure ceiling this trades for: correct for market data (a stale quote is worthless),
    /// wrong for drop-copy (every message must survive).</summary>
    [Id(13)] public int QueueCapacity { get; set; } = 10000;

    /// <summary>Plan 019 wave F (D7): opt-in <c>ClOrdID</c> (tag 11) generation for a row that omits it,
    /// on the <see cref="SourceKinds.FixDuplex"/> outbound half only (this field is inert for the
    /// receive-only <see cref="SourceKinds.Fix"/> kind, which has no outbound half to generate an id
    /// for). <b>Defaults to <c>false</c></b> — deliberately, not an oversight: a <c>ClOrdID</c> this
    /// platform invents is one the caller's own SQL/pipeline never learns synchronously (<c>SendAsync</c>'s
    /// <c>DuplexSendOutcome</c> carries counts and FAILURES only, no per-row success payload), so a caller
    /// that needs to correlate an order it just sent against something it does BEFORE the venue's
    /// <c>ExecutionReport</c> comes back would silently lose that ability the moment generation is
    /// silently on by default. The only way to observe a generated id at all is the round trip: the venue
    /// echoes <c>ClOrdID</c> back on its <c>ExecutionReport</c>, which arrives as an ordinary inbound row
    /// on this same source — plan 019 D7 names exactly this table, keyed on <c>ClOrdID</c>, as the plan's
    /// cheapest large win. Set <see langword="true"/> only for a caller that is fully served by that
    /// after-the-fact correlation and does not need the id before sending.</summary>
    [Id(14)] public bool GenerateClOrdId { get; set; }
}

/// <summary>Plan 009 B2: where a pipeline's rows or a table's deltas are republished. The platform's
/// first outbound concept — everything before this was inbound or read-on-demand. Kind is currently
/// only "nats"; the container shape exists so a second sink kind is additive.</summary>
[GenerateSerializer]
public sealed class SinkSpec
{
    [Id(0)] public string Kind { get; set; } = SinkKinds.Nats;
    [Id(1)] public bool Enabled { get; set; } = true;
    [Id(2)] public NatsPubConfig? Nats { get; set; }
    /// <summary>Plan 012; set only for <see cref="SinkKinds.File"/>.</summary>
    [Id(3)] public FileSinkConfig? File { get; set; }

    /// <summary>Plan 014; set for <see cref="SinkKinds.Postgres"/> and <see cref="SinkKinds.MsSql"/>.</summary>
    [Id(4)] public DbSinkConfig? Db { get; set; }

    /// <summary>Plan 014: an optional name, so <c>INSERT INTO &lt;name&gt; SELECT …</c> has something to
    /// address. Empty for every sink authored before that syntax existed, which is why the sugar reports
    /// an unknown target rather than guessing at a positional one.</summary>
    [Id(5)] public string Name { get; set; } = "";

    /// <summary>Wishlist item 9(a); set only for <see cref="SinkKinds.Http"/>. See <see cref="HttpSinkConfig"/>.</summary>
    [Id(6)] public HttpSinkConfig? Http { get; set; }

    /// <summary>Wishlist item 9(b); set only for <see cref="SinkKinds.Loopback"/>. See
    /// <see cref="LoopbackSinkConfig"/>.</summary>
    [Id(7)] public LoopbackSinkConfig? Loopback { get; set; }

    /// <summary>Plan 019 D2; set only for <see cref="SinkKinds.Duplex"/>. See
    /// <see cref="DuplexSinkConfig"/>.</summary>
    [Id(8)] public DuplexSinkConfig? Duplex { get; set; }

    /// <summary>The sink half of <see cref="ConnectorConfig.Settings"/> — same bag, same rules, same
    /// reason (an out-of-tree sink kind's config class is invisible to <c>SecretWalk</c>). See that
    /// property's doc comment; everything it says applies verbatim here.</summary>
    [Id(9)] public Dictionary<string, string> Settings { get; set; } = [];
}

public static class SinkKinds
{
    public const string Nats = "nats";

    /// <summary>Plan 012: append rows to a local file as CSV or NDJSON — the egress twin of the
    /// <see cref="SourceKinds.File"/> source kind. See <see cref="FileSinkConfig"/>.</summary>
    public const string File = "file";

    /// <summary>Plan 014: write rows into a PostgreSQL table, appending or mirroring. The first sink whose
    /// natural unit is a TRANSACTION rather than a message, which is why <c>IBatchSinkClient</c> exists.
    /// See <see cref="DbSinkConfig"/>.</summary>
    public const string Postgres = "postgres";
    /// <summary>Plan 014: the Microsoft SQL Server twin of <see cref="Postgres"/>.</summary>
    public const string MsSql = "mssql";

    /// <summary>Wishlist item 9(a): POST each row/delta as JSON to an HTTP(S) endpoint — the smaller of
    /// the two "bounded feedback loop" options the wishlist offers, and the one the loop actually uses:
    /// its target is <c>/api/sources/{name}/events</c> on the SAME StreamsForge host. See
    /// <see cref="HttpSinkConfig"/>.</summary>
    public const string Http = "http";

    /// <summary>Wishlist item 9(b): the native in-process loopback pair — feeds a table's deltas directly
    /// into a named generator-kind source's stream, no HTTP hop, no serialize/parse round trip. See
    /// <see cref="LoopbackSinkConfig"/> and <c>StreamsForge.Host.Generators.LoopbackHub</c>
    /// (shared/StreamsForge.AppCore/Generators/LoopbackHub.cs), the in-process "wire" this kind writes to.</summary>
    public const string Loopback = "loopback";

    /// <summary>Plan 019 D2: the stateless proxy for a duplex session's outbound half — <c>fix</c> order
    /// entry is the first duplex kind (wave 019-E). Holds NO connection of its own: publishing resolves the
    /// NAMED SOURCE's live session via <c>StreamsForge.AppCore.Transports.DuplexSessions.Find</c> and
    /// forwards to it, rather than opening a second connection the way every other sink kind does. That is
    /// what makes tearing this client down and rebuilding it (which <c>SinkSelection.Signature</c> does on
    /// any unrelated field edit) harmless — there is nothing here to tear down. See
    /// <see cref="DuplexSinkConfig"/> and <c>StreamsForge.AppCore.Sinks.DuplexSinkTransport</c>.</summary>
    public const string Duplex = "duplex";
}

/// <summary>Plan 009 B2. DELIVERY IS FIRE-AND-FORGET and there is no backpressure from the sink into
/// the pipeline — the same honest limit plan 008's ingress documents, restated rather than pretended
/// away: a slow or absent broker drops, it does not slow the platform down.</summary>
[GenerateSerializer]
public sealed class NatsPubConfig
{
    [Id(0)] public string Url { get; set; } = "";
    /// <summary>Subject to publish to. May contain <c>{name}</c>, replaced with the pipeline/table
    /// name, so one spec can serve a whole catalog.</summary>
    [Id(1)] public string Subject { get; set; } = "";
    [Id(2)] [Secret] public string? Token { get; set; }
    [Id(3)] public string? Username { get; set; }
    [Id(4)] [Secret] public string? Password { get; set; }
    [Id(5)] [Secret] public string? Credentials { get; set; }
    /// <summary>Null (default) = today's behaviour unchanged — see <see cref="NatsSubConfig.Tls"/>'s
    /// doc comment, which applies identically here.</summary>
    [Id(6)] public NatsTlsConfig? Tls { get; set; }
}

/// <summary>Wishlist item 9(a). DELIVERY IS FIRE-AND-FORGET, same honest limit as <see cref="NatsPubConfig"/>
/// (see its doc comment) and the same reason: a slow or unreachable endpoint drops rather than stalling
/// the pipeline/table it is attached to.
///
/// <para><b>The auth shape is one generic header, not a bearer-token field.</b> The wishlist's own worked
/// example — the loop, POSTing back to this SAME host's <c>/api/sources/{name}/events</c> — authenticates
/// with a per-source push key sent as <c>X-SF-Ingest-Key</c> (see <c>SourcesEndpoints.IsAuthorizedToPushAsync</c>),
/// not a bearer token; a <c>BearerToken</c> field would not even cover the one destination this sink was
/// built for. One <see cref="HeaderName"/>/<see cref="HeaderValue"/> pair covers that case (set
/// <c>headerName: "X-SF-Ingest-Key"</c>) and an <c>Authorization: Bearer …</c> receiver equally (set
/// <c>headerName: "Authorization"</c>, <c>headerValue: "Bearer …"</c>), without this config needing to
/// know which scheme any given receiver expects.</para>
///
/// <para><b>The body is shaped for the loop's own endpoint, not a bare row.</b> Every POST body is
/// <c>{ "events": [ &lt;row&gt; ] }</c> — exactly one event, matching <c>IngestEventsRequest</c>
/// (<c>StreamsForge.Api/Dtos.cs</c>) field-for-field on the wire. This sink cannot reference that type
/// directly (<c>StreamsForge.AppCore</c> sits BELOW <c>StreamsForge.Api</c> in the dependency graph — see
/// <c>StreamsForge.AppCore.csproj</c>, which has no reference to it), so <see cref="HttpSinkClient"/> defines
/// its own wire-identical shape rather than growing a back-reference. A one-row batch is not an
/// optimization left on the table: batching would mean holding rows before sending, which is exactly the
/// unbounded buffering this whole family of sinks (see <see cref="NatsPubConfig"/>'s doc) exists to avoid.</para>
///
/// <para><b><see cref="MaxDepth"/> is the loop's cycle-breaker</b> (wishlist item 9's "bounded feedback
/// loop"/"scenario clock" section) — a row whose <see cref="StepField"/> value is <c>&gt;=</c> MaxDepth is
/// DROPPED instead of POSTed, so a table that forgets its own <c>WHERE step &lt; D</c> termination clause
/// cannot turn this sink into an unbounded loop. It is a BACKSTOP, not the primary guard — the wishlist is
/// explicit that termination is normally the user's job via SQL (<c>WHERE step &lt; D</c> on the consuming
/// table); this only catches the case where that guard is missing or wrong. 0 (the default) disables it:
/// most HTTP sinks are not the loop's feedback edge at all, and a guard that fired by default on every
/// ordinary webhook sink (dropping every row once some unrelated field happened to be named "step") would
/// be a worse default than none. A row with no <see cref="StepField"/>, or a non-numeric value there, is
/// NOT dropped — the guard only fires on rows that actually carry a recognizable step counter.</para></summary>
[GenerateSerializer]
public sealed class HttpSinkConfig
{
    /// <summary>Destination URL. May contain <c>{name}</c>, replaced with the pipeline id / table name —
    /// same substitution <see cref="NatsPubConfig.Subject"/> and <see cref="FileSinkConfig.Path"/> use.
    /// REQUIRED: there is no default host. A sink with this blank is "not yet configured"
    /// (<see cref="StreamsForge.AppCore.Sinks.HttpSinkTransport.IsConfigured"/>) rather than one that
    /// silently posts nowhere.</summary>
    [Id(0)] public string Url { get; set; } = "";

    /// <summary>Optional extra header NAME, e.g. <c>"X-SF-Ingest-Key"</c> or <c>"Authorization"</c>. Not a
    /// secret itself — see <see cref="HeaderValue"/> for why they are two properties, only one of which is
    /// masked.</summary>
    [Id(1)] public string? HeaderName { get; set; }

    /// <summary>The header's value — a credential when <see cref="HeaderName"/> names an auth header, hence
    /// <c>[Secret]</c>. Sent only when both this and <see cref="HeaderName"/> are non-empty.</summary>
    [Id(2)] [Secret] public string? HeaderValue { get; set; }

    /// <summary>Upper bound, in milliseconds, on one POST — including connect time. Mirrors
    /// <see cref="StreamsForge.AppCore.Sinks.NatsSinkClient.PublishTimeout"/>'s role (the mechanism behind
    /// "never blocks the caller") but is per-sink here rather than a shared constant, because an HTTP
    /// receiver's acceptable latency varies far more than one broker publish does — a same-host loopback
    /// call (the loop's own use) and a third-party webhook do not belong under the same fixed bound.</summary>
    [Id(3)] public int TimeoutMs { get; set; } = 3000;

    /// <summary>Field name inside the row this sink reads the "scenario clock" step counter from — see
    /// <see cref="MaxDepth"/>. Matches the wishlist's own wording ("carrying <c>step + 1</c>"); change it
    /// only if the looping table's SQL names its own counter column something else.</summary>
    [Id(4)] public string StepField { get; set; } = "step";

    /// <summary>See this class's doc comment. 0 = the guard is off.</summary>
    [Id(5)] public int MaxDepth { get; set; }
}

/// <summary>Wishlist item 9(b): the native in-process loopback sink — the smaller-hop twin of
/// <see cref="HttpSinkConfig"/> (option (a)): instead of POSTing JSON to
/// <c>/api/sources/{name}/events</c>, it writes the row directly into
/// <c>StreamsForge.Host.Generators.LoopbackHub</c> (shared/StreamsForge.AppCore/Generators/LoopbackHub.cs),
/// an in-process, thread-safe hand-off that the target source's own generator (Orleans
/// <c>GeneratorGrain</c> / Dapr <c>GeneratorActor</c>) drains on its own timer and republishes exactly as
/// it would a synthetic tick. No URL, no header, no timeout — there is no network hop to configure.
///
/// <para><b>Same loop-guard semantics as <see cref="HttpSinkConfig"/>, reusing the SAME code.</b>
/// <see cref="StepField"/>/<see cref="MaxDepth"/> mean exactly what they mean there — see that class's
/// doc comment — and both sink kinds share one guard implementation,
/// <c>StreamsForge.AppCore.Sinks.SinkStepGuard</c>, so "the maxDepth guard must work exactly as it does in
/// the HTTP sink" is true by construction, not by parallel maintenance.</para>
///
/// <para><b>The cycle this exists for.</b> A loopback edge means a table's own sink can feed the very
/// source that feeds that table — table T reads source A, T's loopback sink targets A again. Termination
/// is the user's SQL (<c>WHERE step &lt; D</c>); this hub only guarantees delivery/ordering and freedom
/// from deadlock/stack-overflow, never termination — see <c>LoopbackHub</c>'s class doc for exactly why
/// an unbounded cycle cannot corrupt the process (it just runs, and keeps running, until something stops
/// it).</para></summary>
[GenerateSerializer]
public sealed class LoopbackSinkConfig
{
    /// <summary>The target source's name — a generator-kind source that has been started (its
    /// GeneratorGrain/GeneratorActor activation must be Attach'd to <c>LoopbackHub</c>, which happens on
    /// every <c>StartAsync</c> regardless of <c>EventsPerSecond</c>/profile). May contain <c>{name}</c>,
    /// replaced with the OWNING pipeline's id / table's name — same substitution
    /// <see cref="HttpSinkConfig.Url"/> and <see cref="NatsPubConfig.Subject"/> use, so a table named the
    /// same as its own upstream source can loop back with one reusable spec. REQUIRED: there is no
    /// default target.</summary>
    [Id(0)] public string TargetSourceName { get; set; } = "";

    /// <summary>See <see cref="HttpSinkConfig.StepField"/> — identical meaning, shared guard.</summary>
    [Id(1)] public string StepField { get; set; } = "step";

    /// <summary>See <see cref="HttpSinkConfig.MaxDepth"/> — identical meaning, shared guard. 0 = off.</summary>
    [Id(2)] public int MaxDepth { get; set; }
}

/// <summary>Plan 019 D2: the config for the DUPLEX proxy sink — the outbound half of a duplex session
/// (<c>fix</c> order entry is the first duplex kind, wave 019-E), reached by NAMING the source that already
/// holds the live session rather than opening a connection of its own. One FIX session carries
/// <c>NewOrderSingle</c> out and <c>ExecutionReport</c> back over the SAME TCP connection / sequence-number
/// streams, so a sink that opened its own connection would produce a second logon a real counterparty
/// rejects — see <c>StreamsForge.AppCore.Sinks.DuplexSinkTransport</c> and
/// <c>StreamsForge.AppCore.Transports.DuplexSessions</c> for the mechanics.
///
/// <para><b>No <c>[Secret]</c> field here, deliberately.</b> The session's own credentials live on the
/// SOURCE definition <see cref="SourceName"/> points at (e.g. <c>FixSourceConfig.Password</c>) — this sink
/// has no credential of its own to mask.</para></summary>
[GenerateSerializer]
public sealed class DuplexSinkConfig
{
    /// <summary>The name of the duplex-kind SOURCE whose live session this sink forwards to — resolved at
    /// PUBLISH time via <c>DuplexSessions.Find</c>, never held as a connection. May contain <c>{name}</c>,
    /// replaced with the OWNING pipeline's id / table's name, the same substitution every other sink config
    /// in this file uses. REQUIRED: there is no default target. Plan 019 D2: a sink whose named source does
    /// not exist, or is not a duplex kind, must be a validation-time error, not a runtime surprise — see
    /// <c>DuplexSinkTransport.Validate</c>'s doc comment for exactly what this wave's validation can and
    /// cannot enforce without a catalog lookup, and where the remaining check belongs.</summary>
    [Id(0)] public string SourceName { get; set; } = "";

    /// <summary>Plan 019 D3: when true, a pipeline/table whose duplex sink's session is not currently up is
    /// meant to refuse to START rather than run with every send counted as a silent failure. Defaults false
    /// so every sink authored before this flag existed behaves exactly as it did before. <b>This wave adds
    /// the field only</b> — enforcing the refusal is a pipeline/table START-time check needing the same
    /// catalog/runtime context the source-existence validation does (see <see cref="SourceName"/>'s doc
    /// comment), which is out of this wave's ownership; this flag carries the operator's intent forward for
    /// that check to read once it exists.</summary>
    [Id(1)] public bool RequireSession { get; set; }
}

/// <summary>Plan 012: the file egress sink. Rows are APPENDED, never truncated — the file is a log,
/// and a sink that could truncate would silently discard whatever an operator pointed it at by mistake.
/// The writing process is the StreamsForge host, so the path is resolved on the HOST's filesystem with
/// the host process's permissions: this is exactly the same trust the <see cref="SourceKinds.File"/> /
/// <see cref="SourceKinds.Folder"/> source kinds already place in an Editor, in the write direction.
/// In a container or on Cloud Run the target must be a mounted volume, or the output dies with the
/// instance.</summary>
[GenerateSerializer]
public sealed class FileSinkConfig
{
    /// <summary>Destination path. May contain <c>{name}</c>, replaced with the pipeline id / table name,
    /// so one spec can serve a whole catalog (same substitution <see cref="NatsPubConfig.Subject"/> uses).
    /// Missing parent directories are created.</summary>
    [Id(0)] public string Path { get; set; } = "";
    /// <summary><see cref="FileFormats.Csv"/> (default) or <see cref="FileFormats.Ndjson"/>. NOT
    /// <see cref="FileFormats.JsonArray"/>: a JSON array has a closing bracket, and an append-only writer
    /// with no idea when the stream ends can never write one — a half-written array file that no parser
    /// accepts would be worse than refusing the format.</summary>
    [Id(1)] public string Format { get; set; } = FileFormats.Csv;
    /// <summary>CSV only: explicit column order, comma-separated (e.g. "symbol,qty,_weight"). Empty =
    /// the first written row's key order. Either way the header is fixed for the life of the file, so a
    /// column that only appears in a later row is dropped rather than silently shifting every subsequent
    /// row one cell to the left — set this when the rows are not uniform.</summary>
    [Id(2)] public string Columns { get; set; } = "";
}

/// <summary>Cron (5/6-field, UTC, Cronos) XOR fixed interval; IntervalMs floor is 1000 (D-E).</summary>
[GenerateSerializer]
public sealed class ScheduleSpec
{
    [Id(0)] public string? Cron { get; set; }
    [Id(1)] public int? IntervalMs { get; set; }
}

/// <summary>HTTP(S) GET polling. Header VALUES are secrets-lite (D-H).</summary>
[GenerateSerializer]
public sealed class UrlPollConfig
{
    [Id(0)] public string Url { get; set; } = "";
    [Id(1)] public Dictionary<string, string> Headers { get; set; } = [];
    /// <summary>Optional OpenAPI derivation reference (schema was derived from it; kept for re-derive).</summary>
    [Id(2)] public OpenApiRef? OpenApi { get; set; }
    /// <summary>Plan 012: response body format, same vocabulary as the file/folder connectors
    /// ("ndjson" | "json" | "csv"). Defaults to "json", which is what this kind did before the field
    /// existed — an endpoint serving text/csv or NDJSON no longer needs a file in between.</summary>
    [Id(3)] public string Format { get; set; } = FileFormats.JsonArray;
}

/// <summary>Where an OpenAPI-derived schema came from (D-F).</summary>
[GenerateSerializer]
public sealed class OpenApiRef
{
    /// <summary>URL of the OpenAPI v3 document (JSON or YAML). Mutually exclusive with DocInline.</summary>
    [Id(0)] public string? DocUrl { get; set; }
    /// <summary>The document text itself, when supplied inline instead of by URL.</summary>
    [Id(1)] public string? DocInline { get; set; }
    /// <summary>operationId in the doc; response defaults to 200 / first application/json media type.</summary>
    [Id(2)] public string? OperationId { get; set; }
    /// <summary>Explicit JSON pointer to a schema (e.g. "#/components/schemas/Trade"); overrides OperationId.</summary>
    [Id(3)] public string? SchemaPointer { get; set; }
}

// ---- Plan 014: database ingress / egress ----

/// <summary>Plan 014: a database source, Postgres or MS SQL. Structured rather than a single connection
/// string BECAUSE a raw string with an embedded password masks to "***" wholesale, and the operator can
/// then no longer see which host it points at — the escape hatch below exists for the cases the structured
/// fields cannot express, and it pays that price knowingly.
///
/// <para>Plan 017: this one config class now serves FOUR kinds — <see cref="SourceKinds.Postgres"/>,
/// <see cref="SourceKinds.MsSql"/>, <see cref="SourceKinds.PostgresCdc"/> and
/// <see cref="SourceKinds.MsSqlCdc"/> — rather than growing a sibling CDC config, so the connection fields
/// above are shared verbatim and only the CDC-specific fields below are added. Some fields are therefore
/// inert for a given kind (e.g. <see cref="CursorColumn"/> is meaningless for either CDC kind, and
/// <see cref="SlotName"/> is meaningless outside <see cref="SourceKinds.PostgresCdc"/>); the per-kind
/// <c>TransportDescriptor</c> is what hides the irrelevant fields from the console. The cost, paid
/// knowingly: a reader of this raw class cannot tell which fields apply to which kind without also
/// consulting that descriptor.</para></summary>
[GenerateSerializer]
public sealed class DbSourceConfig
{
    [Id(0)] public string Host { get; set; } = "";
    /// <summary>0 = the dialect default (5432 / 1433).</summary>
    [Id(1)] public int Port { get; set; }
    [Id(2)] public string Database { get; set; } = "";
    [Id(3)] public string Username { get; set; } = "";
    [Id(4)] [Secret] public string? Password { get; set; }

    /// <summary>Schema holding <see cref="Table"/>. Empty = the dialect default (public / dbo).</summary>
    [Id(5)] public string Schema { get; set; } = "";
    /// <summary>Table to poll. Ignored when <see cref="Query"/> is set.</summary>
    [Id(6)] public string Table { get; set; } = "";
    /// <summary>Extra predicate ANDed onto the generated WHERE. Table mode only.</summary>
    [Id(7)] public string Where { get; set; } = "";
    /// <summary>Escape hatch: your own SQL, which MUST contain the <c>@cursor</c> placeholder. Bound as a
    /// PARAMETER, never interpolated — injection, and just as importantly type fidelity, since a
    /// timestamptz spliced in as text compares wrong across a DST boundary.</summary>
    [Id(8)] public string Query { get; set; } = "";

    /// <summary>The monotonic column the high-water mark is taken from.
    ///
    /// <para><b>Read this before choosing a timestamp.</b> A surrogate key compared with <c>&gt;</c> is
    /// safe. An <c>updated_at</c> compared with <c>&gt;</c> LOSES every row written in the same millisecond
    /// as the watermark; compared with <c>&gt;=</c> it re-reads them, which is why the recommended shape is
    /// <c>&gt;=</c> plus a <see cref="DedupKeyColumn"/>. Neither variant ever sees a row whose transaction
    /// commits after a later-timestamped one — a polled source is eventually-consistent-with-holes on a
    /// timestamp column, and that is the honest argument for CDC when it matters.</para></summary>
    [Id(9)] public string CursorColumn { get; set; } = "";
    /// <summary>"long" | "timestamp" | "string" (<see cref="CursorKinds"/>) — how the persisted opaque
    /// cursor string is parsed back into a bound parameter.</summary>
    [Id(10)] public string CursorKind { get; set; } = CursorKinds.Long;
    /// <summary>Where to start when no cursor is persisted yet. Empty + <see cref="Snapshot"/> = from the
    /// beginning; empty without it = from MAX(cursorColumn), i.e. new rows only.</summary>
    [Id(11)] public string InitialCursor { get; set; } = "";
    /// <summary>Column whose value dedups re-read rows. The companion to a <c>&gt;=</c> cursor.</summary>
    [Id(12)] public string DedupKeyColumn { get; set; } = "";

    /// <summary>Rows per poll. Also the page size for the initial snapshot, which pages across successive
    /// driver cycles — each persisting its cursor — so a restart mid-snapshot resumes instead of starting
    /// over.</summary>
    [Id(13)] public int BatchSize { get; set; } = 1000;
    /// <summary>Read the whole table first, then tail. See <see cref="InitialCursor"/>.</summary>
    [Id(14)] public bool Snapshot { get; set; }
    [Id(15)] public int CommandTimeoutSeconds { get; set; } = 30;
    /// <summary>Require TLS on the connection.</summary>
    [Id(16)] public bool Tls { get; set; }

    /// <summary>Full connection string. When set it WINS over every structured field above — for the
    /// options this shape does not model. Masked wholesale, with the visibility cost named at the top.</summary>
    [Id(17)] [Secret] public string? ConnectionString { get; set; }

    // ---- Plan 017: CDC-only fields. Inert for the polled postgres/mssql kinds. ----

    /// <summary>Postgres logical replication slot name. Required for <see cref="SourceKinds.PostgresCdc"/>;
    /// unused elsewhere.</summary>
    [Id(18)] public string SlotName { get; set; } = "";
    /// <summary>The Postgres publication the slot streams from. Required for
    /// <see cref="SourceKinds.PostgresCdc"/>; unused elsewhere.</summary>
    [Id(19)] public string PublicationName { get; set; } = "";
    /// <summary>SQL Server CDC capture instance (conventionally <c>&lt;schema&gt;_&lt;table&gt;</c>).
    /// Required for <see cref="SourceKinds.MsSqlCdc"/>; unused elsewhere.</summary>
    [Id(20)] public string CaptureInstance { get; set; } = "";
    /// <summary>Optional CSV of <c>schema.table</c> to keep; empty = everything the publication/capture
    /// instance carries. An informational filter only — on Postgres the publication is the real filter, this
    /// just narrows what the driver surfaces from it. CDC kinds only; unused elsewhere.</summary>
    [Id(21)] public string Tables { get; set; } = "";
    /// <summary>Upper bound, in milliseconds, on how long a single poll cycle drains the replication stream
    /// before returning whatever it has collected so far. CDC kinds only; unused elsewhere.</summary>
    [Id(22)] public int MaxPollMs { get; set; } = 1000;
    /// <summary>Opt-in: create the Postgres replication slot on the first cycle if it does not already
    /// exist. Off by default ON PURPOSE — creating a slot starts pinning WAL on the source database, a
    /// privileged and consequential act on a system this connector does not own, not a convenience to
    /// default to. <see cref="SourceKinds.PostgresCdc"/> only; unused elsewhere.</summary>
    [Id(23)] public bool CreateSlotIfMissing { get; set; }
}

/// <summary>Plan 014: a database sink. Connection fields mirror <see cref="DbSourceConfig"/> deliberately,
/// so an operator configuring both directions types the same form twice rather than two different ones.</summary>
[GenerateSerializer]
public sealed class DbSinkConfig
{
    [Id(0)] public string Host { get; set; } = "";
    [Id(1)] public int Port { get; set; }
    [Id(2)] public string Database { get; set; } = "";
    [Id(3)] public string Username { get; set; } = "";
    [Id(4)] [Secret] public string? Password { get; set; }
    [Id(5)] public string Schema { get; set; } = "";
    /// <summary>Destination table. May contain <c>{name}</c>, replaced with the pipeline id / table name,
    /// the same substitution the NATS and file sinks use. It must ALREADY EXIST — this sink issues no DDL,
    /// because a streaming sink that can CREATE is a trust escalation over one that can only INSERT.</summary>
    [Id(6)] public string Table { get; set; } = "";

    /// <summary>"append" | "upsert" (<see cref="DbSinkModes"/>).</summary>
    [Id(7)] public string Mode { get; set; } = DbSinkModes.Append;
    /// <summary>Comma-separated key columns. REQUIRED for <see cref="DbSinkModes.Upsert"/> and unused by
    /// append. Explicit rather than derived, because a sink client only ever sees the entity NAME — reaching
    /// back for its SQL to guess the identity would couple egress to the catalog. The console prefills it
    /// from the table's declared keys, which is a visible suggestion rather than a silent derivation.</summary>
    [Id(8)] public string KeyColumns { get; set; } = "";
    /// <summary>Write the Z-set weight as a <c>_weight</c> column. Append mode only — in upsert mode the
    /// weight IS the operation (negative = DELETE), so persisting it would store a number already spent.</summary>
    [Id(9)] public bool IncludeWeight { get; set; }

    /// <summary>Explicit column order, comma-separated. Empty = the union of the batch's keys.</summary>
    [Id(10)] public string Columns { get; set; } = "";
    [Id(11)] public int CommandTimeoutSeconds { get; set; } = 30;
    [Id(12)] public bool Tls { get; set; }
    [Id(13)] [Secret] public string? ConnectionString { get; set; }
}

/// <summary>How <see cref="DbSourceConfig.CursorKind"/> parses the persisted opaque cursor.</summary>
public static class CursorKinds
{
    public const string Long = "long";
    public const string Timestamp = "timestamp";
    public const string String = "string";
}

/// <summary>How a database sink applies a batch.</summary>
public static class DbSinkModes
{
    /// <summary>Every delivered row becomes an INSERT. Never deletes; the destination is a log.</summary>
    public const string Append = "append";
    /// <summary>Positive weights UPSERT on <see cref="DbSinkConfig.KeyColumns"/>, negative weights DELETE,
    /// deletes applied last within the one transaction so a delete-then-reinsert of the same key inside a
    /// batch lands the way the caller meant it. Rejected on a PIPELINE sink: a pipeline emits results, not
    /// deltas, so there is no identity and no weight and "mirror current state" means nothing there.</summary>
    public const string Upsert = "upsert";
}

/// <summary>Formats for file/folder sources.</summary>
public static class FileFormats
{
    public const string Ndjson = "ndjson";
    public const string JsonArray = "json";
    public const string Csv = "csv";
    /// <summary>Plan 018: tag=value FIX protocol text — one FIX message per frame (SOH/<c>|</c>/<c>^</c>
    /// delimited, sniffed the way <see cref="Csv"/> sniffs its own delimiter), parsed by the
    /// dependency-free FIX parser in <c>StreamsForge.AppCore.Connectors.Formats.FixParser</c>. Every kind
    /// that names a <see cref="FileFormats"/> gets it for free — a <c>file</c>/<c>folder</c> source
    /// replays a FIX log off disk, a <c>url</c> source reads one over HTTP, a <c>nats</c> source ingests
    /// FIX-over-NATS. <b>Ingress-only</b>: the <c>file</c> SINK's format select deliberately does NOT
    /// offer this constant (see <c>FileSinkTransport.Describe()</c>) — writing FIX without a session to
    /// number the messages produces something no counterparty would accept.</summary>
    public const string Fix = "fix";
}

/// <summary>Poll one file; re-parse on content change (hash+mtime). No tailing guarantees.</summary>
[GenerateSerializer]
public sealed class FilePollConfig
{
    [Id(0)] public string Path { get; set; } = "";
    /// <summary>"ndjson" | "json" | "csv" (<see cref="FileFormats"/>).</summary>
    [Id(1)] public string Format { get; set; } = FileFormats.Ndjson;
}

/// <summary>Poll a directory; each NEW file (name+mtime ledger) is parsed once and remembered.</summary>
[GenerateSerializer]
public sealed class FolderPollConfig
{
    [Id(0)] public string Path { get; set; } = "";
    [Id(1)] public string Format { get; set; } = FileFormats.Ndjson;
    /// <summary>Optional glob over file NAMES within the folder (no recursion), e.g. "*.json".</summary>
    [Id(2)] public string? Glob { get; set; }
}

/// <summary>Subscription to a remote StreamsForge DynamicStreamService (D-G — federation).
/// Password/Token are secrets-lite (D-H).</summary>
[GenerateSerializer]
public sealed class GrpcSubConfig
{
    /// <summary>Target gRPC address, e.g. "http://localhost:5299" (h2c).</summary>
    [Id(0)] public string Address { get; set; } = "";
    /// <summary>"source:{name}" | "pipeline:{id}" | "table:{id}" on the REMOTE instance.</summary>
    [Id(1)] public string EntityKey { get; set; } = "";
    /// <summary>Login for the remote's /api/auth/login (re-login on expiry). XOR Token.</summary>
    [Id(2)] public string? Username { get; set; }
    [Id(3)] [Secret] public string? Password { get; set; }
    /// <summary>Static bearer token alternative (no re-login possible — documented).</summary>
    [Id(4)] [Secret] public string? Token { get; set; }
    /// <summary>"reflection" (default) | "proto". Reflection walks the remote's v1alpha service;
    /// "proto" parses <see cref="ProtoText"/> (StreamsForge-generated files only).</summary>
    [Id(5)] public string SchemaSource { get; set; } = "reflection";
    /// <summary>Pasted/downloaded proto text when SchemaSource == "proto".</summary>
    [Id(6)] public string? ProtoText { get; set; }
    /// <summary>Remote REST base for login when it differs from the gRPC address,
    /// e.g. "http://localhost:5199".</summary>
    [Id(7)] public string? RestAddress { get; set; }

    /// <summary>Plan 016 wave 5: the name of a configured peer to take <see cref="Address"/> and
    /// <see cref="RestAddress"/> from, instead of hardcoding either. Resolved at each (re)connect —
    /// the cadence this subscriber already uses for its schema snapshot and login — so a peer whose
    /// address moved is fixed by reconfiguring the host, with no catalog edit and no restart of the
    /// source. Null/empty = the pre-016 behaviour, byte for byte: the two addresses are used as
    /// authored.
    ///
    /// <para>When set, it WINS over both address fields rather than filling them in when blank: a
    /// source that names a peer and also carries a stale literal address must not silently connect to
    /// the stale one. An unresolvable peer takes the existing status-error path at the existing
    /// backoff.</para></summary>
    [Id(8)] public string? Peer { get; set; }
}

/// <summary>Response-structure mapping (the "mapping document" deserializes into this; JSON or
/// YAML accepted at the API boundary). Paths use the JSONPath-lite subset:
/// $ .name ['name'] [n] [*] — nothing else (documented, closed).</summary>
[GenerateSerializer]
public sealed class MappingSpec
{
    /// <summary>Where the items live, e.g. "$.data.trades[*]". "$" = the root (single item, or
    /// each element when the root is an array).</summary>
    [Id(0)] public string ItemsPath { get; set; } = "$";
    /// <summary>Emitted FIELD name whose value dedups re-polled items. Null = no dedup.</summary>
    [Id(1)] public string? DedupKeyField { get; set; }
    /// <summary>Emitted FIELD name holding the event timestamp (epoch-ms or ISO-8601) → _ts.
    /// Null = arrival time.</summary>
    [Id(2)] public string? TimestampField { get; set; }
    [Id(3)] public List<FieldMapEntry> Fields { get; set; } = [];

    /// <summary>Plan 014: an envelope the payload is wrapped in and should be unwrapped from BEFORE
    /// <see cref="ItemsPath"/> applies. <see cref="CdcEnvelopes.None"/> (the default) is byte-identical
    /// to every mapping authored before this existed. It lives on the MAPPING rather than on a transport
    /// so that one unwrapper serves every transport at once — NATS today, a queue tomorrow, and a
    /// file of captured envelopes in a test.</summary>
    [Id(4)] public string Envelope { get; set; } = CdcEnvelopes.None;
}

/// <summary>Plan 014: recognized payload envelopes for <see cref="MappingSpec.Envelope"/>.</summary>
public static class CdcEnvelopes
{
    public const string None = "none";

    /// <summary>Debezium's change envelope: <c>op</c> c/u/r take the row from <c>after</c>, <c>d</c> takes
    /// it from <c>before</c> and stamps <c>_weight = -1</c>; <c>ts_ms</c> becomes <c>_ts</c>. Accepts both
    /// the raw <c>{schema,payload}</c> form and the already-unwrapped form the <c>ExtractNewRecordState</c>
    /// SMT emits, and passes a message with no <c>op</c> through untouched.
    ///
    /// <para><b>The honest limit:</b> a source is an append-only event stream, so <c>_weight</c> on an
    /// INBOUND row is just a column — the Engine's Z-set weights are computed by table SQL, not carried in
    /// from ingress. A delete therefore arrives as a tombstone row; the working pattern is
    /// <c>LATEST BY key</c> + <c>WHERE _op &lt;&gt; 'd'</c>, which hides the key but does not free it.</para></summary>
    public const string Debezium = "debezium";
}

/// <summary>One output field: where it comes from (path relative to the item) and its FieldDef
/// (name/type/children/isArray — the existing schema model, reused).</summary>
[GenerateSerializer]
public sealed class FieldMapEntry
{
    /// <summary>JSONPath-lite relative to the item, e.g. "price" or "user.tier". Null = same as Field.Name.</summary>
    [Id(0)] public string? SourcePath { get; set; }
    [Id(1)] public FieldDef Field { get; set; } = new("", FieldType.String);
}

/// <summary>Connector runtime status (D-C). Returned by IConnectorStatusFacade; null for
/// generator-kind sources. LastStatus: "never" | "ok" | "error".</summary>
[GenerateSerializer]
public sealed class ConnectorRuntimeStatus
{
    [Id(0)] public string SourceName { get; set; } = "";
    [Id(1)] public long? NextRunMs { get; set; }
    [Id(2)] public long? LastRunMs { get; set; }
    [Id(3)] public string LastStatus { get; set; } = "never";
    [Id(4)] public string? LastError { get; set; }
    [Id(5)] public int ConsecutiveFailures { get; set; }
    [Id(6)] public long EventsEmittedTotal { get; set; }
    [Id(7)] public int LastBatchCount { get; set; }

    /// <summary>Plan 009 C2: cumulative field-level coercion failures on this source's inbound rows —
    /// a value that would not convert to its declared type. A queryable counter rather than only a note
    /// in <see cref="LastError"/>, because under
    /// <see cref="CoercionFailurePolicy.DropRow"/>/<see cref="CoercionFailurePolicy.RejectBatch"/> these
    /// are rows that did not land, and "counted and surfaced" was the whole condition for letting a
    /// policy discard anything. Cumulative since activation, like
    /// <see cref="EventsEmittedTotal"/>.</summary>
    [Id(8)] public long CoercionFailuresTotal { get; set; }

    /// <summary>Plan 014: the polled transport's persisted high-water mark, verbatim and opaque — an
    /// LSN, a composite "(ts,id)", or a plain bigint, whatever the transport minted. Surfaced because an
    /// operator who cannot see the cursor cannot tell a stuck source from an idle one. Null for every
    /// kind that has no cursor.</summary>
    [Id(9)] public string? Cursor { get; set; }

    /// <summary>Plan 014: cumulative messages a <see cref="MappingSpec.Envelope"/> unwrapper could not turn
    /// into a row — a Debezium delete whose <c>before</c> is absent because the table has no
    /// REPLICA IDENTITY FULL, or a tombstone. Counted rather than folded into <see cref="LastError"/>,
    /// because a non-null error drops every row the cycle produced and one unrepresentable change event
    /// must not discard the good rows sitting beside it in the same batch. Same reasoning, and the same
    /// shape, as <see cref="CoercionFailuresTotal"/>: silence would be indistinguishable from a source
    /// that simply has no deletes.</summary>
    [Id(10)] public long EnvelopeSkippedTotal { get; set; }

    /// <summary>Plan 019 D3: true when this source's duplex session is established and can accept a send
    /// right now. Null for every kind that has no outbound half — which is all of them but a duplex kind,
    /// so null and false mean genuinely different things here: "there is no session to be ready" versus
    /// "there is one and it is down".</summary>
    [Id(11)] public bool? DuplexReady { get; set; }

    /// <summary>Plan 019 D3: rows this source's outbound half accepted, cumulative since activation. Same
    /// shape and same reasoning as <see cref="EventsEmittedTotal"/>, for the other direction.</summary>
    [Id(12)] public long DuplexSentTotal { get; set; }

    /// <summary>Plan 019 D3: rows the outbound half could not deliver. A counter is NOT the whole story
    /// for an order — see <see cref="LastDuplexFailure"/> — but it is what makes "some went out and some
    /// did not" visible at a glance, and it must never be the only place a failure lands.</summary>
    [Id(13)] public long DuplexFailedTotal { get; set; }

    /// <summary>Plan 019 D3: the most recent outbound failure, identified rather than merely counted —
    /// for a FIX session that means the order's <c>ClOrdID</c> and the reason. This field exists because
    /// <c>ISinkClient.PublishAsync</c> may never throw, so an order that did not go out has no other way
    /// to reach an operator; "counted in a failure counter" was explicitly rejected in plan 019 §2 as an
    /// acceptable outcome for a <c>NewOrderSingle</c>.</summary>
    [Id(14)] public string? LastDuplexFailure { get; set; }
}

// ---- REST helper DTOs (cross HTTP only, but follow house serialization style anyway) ----

/// <summary>POST /api/sources/schema/mapping-validate request.</summary>
[GenerateSerializer]
public sealed class MappingValidateRequest
{
    /// <summary>Mapping document text (JSON or YAML).</summary>
    [Id(0)] public string Document { get; set; } = "";
    /// <summary>Optional sample response body to dry-run the mapping against.</summary>
    [Id(1)] public string? Sample { get; set; }
}

[GenerateSerializer]
public sealed class MappingValidateResult
{
    [Id(0)] public bool Ok { get; set; }
    [Id(1)] public MappingSpec? Mapping { get; set; }
    [Id(2)] public List<string> Diagnostics { get; set; } = [];
    /// <summary>Rows extracted from Sample (first 10), for UI preview.</summary>
    [Id(3)] public List<Dictionary<string, object?>> PreviewRows { get; set; } = [];
}

/// <summary>POST /api/sources/schema/derive-openapi request.</summary>
[GenerateSerializer]
public sealed class SchemaDeriveRequest
{
    [Id(0)] public OpenApiRef OpenApi { get; set; } = new();
}

[GenerateSerializer]
public sealed class SchemaDeriveResult
{
    [Id(0)] public List<FieldDef> Fields { get; set; } = [];
    [Id(1)] public List<string> Diagnostics { get; set; } = [];
}

/// <summary>POST /api/sources/schema/from-remote request.</summary>
[GenerateSerializer]
public sealed class RemoteSchemaRequest
{
    [Id(0)] public GrpcSubConfig Grpc { get; set; } = new();
}

[GenerateSerializer]
public sealed class RemoteSchemaResult
{
    [Id(0)] public List<FieldDef> Fields { get; set; } = [];
    /// <summary>FieldNumberMap JSON (EntitySchemas.ParseMap format) captured from the remote.</summary>
    [Id(1)] public string FieldNumbersJson { get; set; } = "";
    [Id(2)] public List<string> Diagnostics { get; set; } = [];
}

/// <summary>One entity's outcome in a config import (D-J).</summary>
[GenerateSerializer]
public sealed class ConfigImportReportEntry
{
    /// <summary>"source" | "pipeline" | "table" | "document" (a whole-import gate that names nothing
    /// more specific, e.g. a table dependency cycle) | "requires" (plan 016 wave 4: an unsatisfied
    /// <c>ConfigDocument.Requires</c> entry — Name is the connector KIND, not a catalog entity).</summary>
    [Id(0)] public string Kind { get; set; } = "";
    [Id(1)] public string Name { get; set; } = "";
    /// <summary>"created" | "updated" | "deleted" | "skipped" | "error".</summary>
    [Id(2)] public string Action { get; set; } = "";
    [Id(3)] public List<string> Diagnostics { get; set; } = [];
}

/// <summary>POST /api/config/import response (D-J).</summary>
[GenerateSerializer]
public sealed class ConfigImportReport
{
    /// <summary>"validate" | "merge" | "replace".</summary>
    [Id(0)] public string Mode { get; set; } = "";
    [Id(1)] public List<ConfigImportReportEntry> Entries { get; set; } = [];
    [Id(2)] public bool Ok { get; set; }
}
