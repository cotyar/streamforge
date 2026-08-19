using System.Text.Json.Serialization;
using StreamForge.Abstractions;

namespace StreamForge.Api;

// ============================================================================
// REST DTOs — must match web/src/api/types.ts exactly (camelCase via default
// System.Text.Json naming policy).
// ============================================================================

public sealed record LoginRequest(string Username, string Password);

public sealed record LoginResponse(string Token, string Username, string DisplayName, string Role);

/// <summary>Plan 015 wave 2-C: five ADDITIVE, OPTIONAL entitlement fields, populated by
/// <c>GET /api/auth/me</c> and left null everywhere else.
///
/// <para>They are serialized only when populated. That is not tidiness — it is the rolling-deploy
/// contract: <c>web/src/api/types.ts</c> declares every one of them optional and the SPA treats a
/// MISSING <c>permissions[]</c> as "an old server, fall back to ordinal Viewer &lt; Editor &lt; Admin".
/// A serialized <c>"permissions": null</c> is not missing, so the fallback would misfire; and
/// <c>GET /api/users</c>, which still builds the four-argument form, keeps producing byte-identical
/// JSON to the pre-015 server. The ignore condition lives on the record rather than in a global JSON
/// option because this file does not own the global options and every other DTO's shape would move
/// with them.</para></summary>
public sealed record UserInfo(
    string Username,
    string DisplayName,
    string Role,
    long CreatedAtMs,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] IReadOnlyList<PermissionGrant>? Permissions = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] IReadOnlyList<string>? Roles = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] IReadOnlyList<string>? Groups = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] bool? Disabled = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] long? PolicyVersion = null);

public sealed record CreateUserRequest(string Username, string DisplayName, string Role, string Password);

public sealed record UpdateUserRequest(string? DisplayName, string? Role, string? Password);

/// <summary>Plan 015 wave 2-C: the body of <c>PUT /api/access/users/{username}/disabled</c>. One field,
/// its own record, because disabling a login is the cheap 90% of token revocation and must not require
/// sending the rest of a <c>UserAccessEntry</c> — a caller under pressure would sooner or later send it
/// without the grants they did not know were there. Deliberately NOT a field on
/// <see cref="UpdateUserRequest"/>: "disabled" is policy, and policy lives in the access document, not
/// on the credential record.</summary>
public sealed record SetAccessDisabledRequest(bool Disabled);

/// <summary>Plan 015 wave 5-A: the body of <c>POST /api/approvals</c>. FOUR fields, and the four are
/// exactly what a caller owns — what they want to do (<paramref name="Action"/>), to what
/// (<paramref name="Scope"/>), why (<paramref name="Reason"/>) and the serialized request that would
/// have executed (<paramref name="PayloadJson"/>).
///
/// <para>Deliberately NOT <see cref="ApprovalRequest"/> bound off the wire. That would be safe —
/// <c>ApprovalStateMachine.CreateRequest</c> is a whitelist that discards <c>Votes</c>, <c>State</c>,
/// <c>RequiredApprovals</c> and the rest whatever a caller sends — but publishing those fields as part
/// of a request contract invites a client to fill them in and a future reader to assume they mean
/// something. Above all there is no <c>RequestedBy</c>: the requester is the authenticated principal,
/// always, and the entire self-vote rule rests on a caller being unable to say otherwise.</para></summary>
public sealed record FileApprovalRequest(string Action, string Scope, string? Reason = null, string? PayloadJson = null);

/// <summary>Plan 015 wave 5-A: the (optional) body of the approve/reject/cancel routes. One field,
/// because the vote's identity and timestamp are server-set — a caller-supplied voter is an
/// impersonation and a caller-supplied timestamp is a caller-supplied place in the ordering.</summary>
public sealed record ApprovalDecisionRequest(string? Comment = null);

/// <summary>Plan 015 wave 5-A: <c>GET /api/audit/{day}</c>.
///
/// <para><see cref="Truncated"/> is <see cref="AuditPage.Truncated"/> carried through unchanged and is
/// NOT optional — the day shard's drop-oldest cap counts what it dropped precisely so that silence is
/// never mistaken for absence, and a response shape that let a client omit it would undo that.</para>
///
/// <para><see cref="ChangesIncluded"/>/<see cref="ChangesWithheld"/> do the same job for the
/// before/after payloads, which are off unless the caller asks for them AND is entitled to
/// <c>access.read</c> — see <c>AuditEndpoints.RedactChanges</c> for why. A reader who cannot see a diff
/// is told that one exists.</para></summary>
public sealed record AuditPageResponse(
    string Day,
    IReadOnlyList<AuditEntry> Entries,
    long Truncated,
    int Total,
    bool ChangesIncluded,
    int ChangesWithheld);

public sealed record CreatePipelineRequest(
    string Name,
    string Description,
    string Sql,
    List<string>? Tags = null,
    Dictionary<string, string>? Metadata = null,
    // Plan 009 B2: where this pipeline's result rows are republished. Null/omitted = leave unset (on
    // create) or unchanged (on update, mirroring how Tags/Metadata's null-means-unchanged works below).
    // NatsPubConfig credential fields follow the secrets-lite convention (SourceKinds.SecretMask on read;
    // a written mask means "keep the stored value") — see PipelinesEndpoints' PUT handler.
    List<SinkSpec>? Sinks = null,
    // Plan 016 wave 2-B: what this pipeline was authored against. Null/omitted = leave unset (create) or
    // unchanged (update), the same convention as Tags/Metadata/Sinks above. Validated by
    // EntityPinValidation before it ever reaches the registry — a pin's kind must be "source" or "table",
    // never "pipeline" (EntityPin's own doc comment: nothing reads a pipeline's output by name).
    List<EntityPin>? DependsOn = null);

public sealed record ValidateRequest(string Sql);

public sealed record SqlDiagnosticDto(string Message, int Line, int Column, string Severity);

public sealed record ValidateResponse(bool Ok, IReadOnlyList<SqlDiagnosticDto> Diagnostics, string? PlanSummary, IReadOnlyList<string> SourceNames);

public sealed record ErrorResponse(string Error);

/// <summary>Plan 016 wave 2-B: the 409 body for <c>PUT /api/sources/{name}?allowBreaking=false</c> when
/// the field change IS breaking. <paramref name="BreakingReasons"/> is
/// <c>SchemaCompatibility.Compare(...).BreakingReasons</c> verbatim — one human-readable line per removed
/// or re-typed field, so the operator learns WHICH field, not just "incompatible".</summary>
public sealed record SchemaBreakingChangeResponse(string Error, IReadOnlyList<string> BreakingReasons);

public sealed record CreateTableRequest(
    string Name,
    string Description,
    string Sql,
    bool SearchEnabled = false,
    TableSearchMode SearchMode = TableSearchMode.Exact,
    bool HistoryEnabled = false,
    TableHistoryMode HistoryMode = TableHistoryMode.All,
    int HistoryLimit = 10,
    string? HistoryByField = null,
    long HistoryWindowMs = 0,
    List<string>? Tags = null,
    Dictionary<string, string>? Metadata = null,
    // Plan 003 M2: partitioned execution opt-in. 1 (default) = classic single-grain path. See
    // TableDefinition.Parallelism's doc comment; RegistryGrain validates 1..16.
    int Parallelism = 1,
    // Plan 008: durability policy for the materialized snapshot, and the flush cadence for the two
    // modes that write. Defaults reproduce the pre-008 behavior exactly. See TablePersistenceMode.
    TablePersistenceMode Persistence = TablePersistenceMode.Batched,
    int FlushMs = 0,
    // Plan 009 A2: compaction threshold for TablePersistenceMode.Journaled. 0 = default.
    int JournalMaxEntries = 0,
    // Plan 009 B2: where this table's deltas are republished. Same null-means-unchanged convention as
    // CreatePipelineRequest.Sinks above.
    List<SinkSpec>? Sinks = null,
    // Plan 011 C2: opt-in row retention. 0/0 (the defaults) = off, i.e. the table holds every row its SQL
    // says it should. Non-zero makes the table a BOUNDED VIEW — see TableDefinition.RetentionMaxRows.
    int RetentionMaxRows = 0,
    long RetentionTtlMs = 0,
    // Plan 011 D1: opt-in key sharding. Null/empty (the default) = not sharded, i.e. today's behavior
    // byte for byte. See TableDefinition.ShardBy; RegistryGrain validates the columns against the
    // compiled output schema and refuses the searchEnabled combination.
    List<string>? ShardBy = null,
    // Plan 016 wave 2-B: what this table was authored against. Null/omitted = leave unset (create) or
    // unchanged (update), the same null-means-unchanged convention as Tags/Metadata/Sinks/ShardBy above.
    // Validated by EntityPinValidation before it ever reaches the registry — see CreatePipelineRequest's
    // identical field for why "pipeline" is not a legal kind.
    List<EntityPin>? DependsOn = null);

public sealed record TableSearchResponse(IReadOnlyList<TableRowDto> Rows, string Mode, bool Enabled, int Total);

public sealed record FieldDefDto(string Name, string Kind);

public sealed record ValidateTableResponse(
    bool Ok,
    IReadOnlyList<SqlDiagnosticDto> Diagnostics,
    string? PlanSummary,
    IReadOnlyList<string> StreamInputs,
    IReadOnlyList<string> TableInputs,
    IReadOnlyList<FieldDefDto> OutputSchema);

/// <summary>Plan 003 M4: <paramref name="FrontierEpoch"/> is additive (default null) — non-null only for a
/// Parallelism &gt;= 2 table's coordinator, once it has observed a full round (see TableGrain's class doc
/// and TableMetrics.SnapshotFrontierEpoch, which carries the identical value). CONSISTENCY STATEMENT: when
/// non-null, <paramref name="Rows"/> reflects ALL deltas whose epoch is &lt;= FrontierEpoch and NONE beyond
/// it — see TableGrain.OnOutputBatchAsync's doc comment for exactly why that's true by construction, not
/// just by convention. Null for every Parallelism==1 table (classic mode has no partitioned frontier) and
/// for a Parallelism &gt;= 2 table that hasn't yet completed its first round.</summary>
public sealed record TableRowsResponse(IReadOnlyList<TableRowDto> Rows, int TotalRows, long Seq, long? FrontierEpoch = null, TableRowsShardNote? Shards = null);

/// <summary>Plan 011 D1: present (non-null) only on a SHARDED table's <c>/rows</c> response, and there
/// purely to say honestly where the rows came from.
///
/// THE TRAP THIS ANNOTATION EXISTS TO CLOSE. The console's table page polls <c>/rows</c> every two
/// seconds. If a keyless listing on a sharded table were served by fanning out across the shard
/// directory, every shard would wake on every poll, nothing would ever be swapped out, and the feature
/// would be self-defeating while looking like it worked in every functional test. So it is not: a keyless
/// listing is served from the table itself, and <see cref="ShardsConsulted"/> is <c>false</c> to say so.
/// Not one shard is contacted, so no amount of polling can wake an idle key.
///
/// Plan 011 D2: "from the table itself" now means LIVE, from its executor, because a sharded table no
/// longer keeps a persisted consolidated mirror at all — the shards are the durable per-key copy, and the
/// mirror was a second copy of the same rows (see TableGrain's own D2 paragraph). The rows served here are
/// therefore FRESHER than the up-to-one-flush-interval-stale copy they replace, not staler.
///
/// What that costs in honesty, stated rather than hidden: these rows are the table's view, not a fresh
/// read of the shard tier — and a STOPPED sharded table, having no executor to serve them from, reports
/// zero rows where a stopped unsharded one still reports its last snapshot (the rows are not lost, they
/// are in the shards, and a per-key lookup returns them). For a key, use
/// <c>POST /api/tables/{id}/shard/lookup</c> — one grain, strictly consistent. For every shard, use
/// <c>GET /api/tables/{id}/shards/scan</c>, which is a separate endpoint precisely because it does wake
/// them.</summary>
public sealed record TableRowsShardNote(IReadOnlyList<string> ShardBy, bool ShardsConsulted, string Note);

// Plan 011 D1 — sharded tables. POST (not GET) for the same reason /history/lookup is: the lookup key is
// the row's own content, an arbitrary-shaped object. The client hands back a row (or just an object
// carrying the shardBy columns) and the server derives the shard key with the identical codec the router
// uses on live deltas, so the client never needs to know the key encoding.
public sealed record ShardLookupRequest(Dictionary<string, object?> Row);

/// <summary>Plan 011 D1: <c>GET /api/tables/{id}/shards</c>. Every number here is answered from the
/// router and the directory — NO shard is activated, which is what makes it safe to poll.</summary>
public sealed record TableShardsResponse(
    bool Enabled,
    IReadOnlyList<string> ShardBy,
    int ShardCount,
    int ResidentShardCount,
    long Activations,
    long Deactivations,
    long RouterSeq,
    long RoutedBatches,
    long RoutedDeltas,
    bool RouterActive,
    IReadOnlyList<string> Keys);

/// <summary>Plan 011 D1: <c>GET /api/tables/{id}/shards/scan</c>. <paramref name="Woke"/> is the number of
/// shards this call activated — the honest price tag, reported so a caller can see what it just did.
///
/// Plan 011 D2 adds <c>?fenced=true</c>, and the three fields below are what it answers with.
/// <paramref name="Fenced"/> false is D1's behavior unchanged and remains the default: a set of per-shard
/// observations taken at DIFFERENT sequence numbers while ingest continued, not a cut.
/// <paramref name="Fenced"/> true is a genuine consistent cut at <paramref name="FenceSeq"/> — see
/// <c>TableShardScanResult</c> for how, and for why "each shard waits until it has applied the fence
/// sequence" is the wrong shape (it deadlocks on any idle shard and still admits post-fence deltas).
///
/// <paramref name="RoutedDeltasAtFence"/> is what makes the cut CHECKABLE instead of merely claimed: when
/// the page covers every shard (<paramref name="ShardCount"/> shards returned), the sum of the shards'
/// own <c>deltasApplied</c> must equal it exactly — nothing forwarded at or before the fence missing, and
/// nothing from after it counted. Both are 0/-1 on an unfenced scan, where no such statement can be
/// made.</summary>
public sealed record TableShardScanResponse(
    IReadOnlyList<TableShardStats> Shards,
    int Woke,
    int Offset,
    int Limit,
    bool Fenced = false,
    long FenceSeq = -1,
    long RoutedDeltasAtFence = 0,
    int ShardCount = 0);

// Row history (Feature B). The client hands back the exact row object it already has (from the live grid
// or a search result) rather than pre-computing/round-tripping an opaque key — the server derives the
// row-identity key from it (TableGroupKeyExtractor + RowKeyCodec), so the client never needs to know
// whether/how a table's GROUP BY identity was derived.
public sealed record HistoryLookupRequest(Dictionary<string, object?> Row);

// ============================================================================
// Plan 008 W4: client-push ingress. POST /api/sources/{name}/events (Editor) and
// GET /api/sources/{name}/ingest (Viewer). Shapes pinned verbatim — concurrent agents code the
// backend, the gRPC service and the SPA card against exactly this JSON. Semantics live on the
// contracts side (StreamForge.Contracts/IngestModels.cs); these are the wire shapes only.
// ============================================================================

/// <summary>POST body. Deliberately NOT the frozen SourceEventsEnvelope — that is an internal
/// transport shape and must not become a public request contract. <paramref name="Partial"/> admits
/// the valid rows of a batch that has invalid ones; the default fails the whole batch instead.</summary>
/// <param name="IdempotencyKey">Plan 009 A1. Repeating a push with the same key replays the original
/// result instead of admitting anything again — what makes "retry the identical body after a 429" safe.
/// Also accepted as the <c>Idempotency-Key</c> header; the body wins if both are present.</param>
public sealed record IngestEventsRequest(
    List<Dictionary<string, object?>> Events,
    bool Partial = false,
    string? IdempotencyKey = null);

/// <summary>202 body. Success is "buffered", never "processed" — see IngestModels.cs's header on why
/// no honest 200 exists here. Dropped is non-zero only under DropNewest/DropOldest.</summary>
public sealed record IngestAcceptedResponse(
    int Accepted,
    int Dropped,
    int Invalid,
    int DepthRows,
    int CapacityRows,
    // Plan 009 A1, additive. Duplicate = suppressed by row-level dedup, a distinct reason from Dropped
    // (capacity) and Invalid (coercion). Replayed = this 202 restates an earlier push's counts.
    int Duplicate = 0,
    bool Replayed = false);

// ----- Plan 009 A1: per-source push keys. POST returns the secret ONCE; nothing can read it back. -----

public sealed record CreateIngestKeyRequest(string Label);

/// <summary>The one and only time <paramref name="Secret"/> exists outside the client — it is stored
/// hashed and salted, so a lost key is regenerated, never recovered.</summary>
public sealed record CreatedIngestKeyResponse(string Id, string Label, string Secret, long CreatedAtMs);

/// <summary>Listing shape: identity and usage, never the secret or its hash.</summary>
public sealed record IngestKeyResponse(string Id, string Label, long CreatedAtMs, long LastUsedMs);

/// <summary>Body for 400/409/413/429. <paramref name="RetryAfterMs"/> is 0 except on 429, where it
/// mirrors the Retry-After header (which is whole seconds clamped to [1,30]).</summary>
public sealed record IngestErrorResponse(string Error, int RetryAfterMs, IReadOnlyList<string> RowErrors);

/// <summary>GET /api/sources/{name}/ingest. 404 unknown source, 204 source exists but is not
/// ingest-kind, 200 otherwise — reusing DecideStatusOutcome's existing three-way convention.
/// <paramref name="DownstreamDropped"/> is the second loss point (the transport's own drops), exposed
/// so it is visible rather than discovered later as missing rows.</summary>
public sealed record IngestStatusResponse(
    string Policy,
    int CapacityRows,
    int DepthRows,
    int MaxBatchRows,
    long TotalAccepted,
    long TotalRejected,
    long TotalDropped,
    long TotalInvalid,
    long TotalPublished,
    long DownstreamDropped,
    long LastPushMs,
    // Plan 009 A1, additive. InstanceId/Aggregated exist because plan 008 shipped counters that are
    // per-replica while reading as global — see IngestStatus.InstanceId's doc comment.
    long TotalDuplicate = 0,
    string InstanceId = "",
    bool Aggregated = false);

// ============================================================================
// Plan 008 W5: GET /api/pipelines/{id}/plan and GET /api/tables/{id}/plan — lineage + execution-plan
// DTOs. Shape pinned verbatim for the console's React Flow lineage/plan page (a concurrent agent codes
// against this exact JSON — do not rename fields). See PlanEndpointsLogic for how these are built.
// ============================================================================

/// <summary>One inbound edge reference on a <see cref="PlanStageDto"/> — just enough (edge id + role)
/// for the UI to draw the arrow into this stage; the edge's own endpoints/mode/etc. live on the matching
/// entry in <see cref="ExecutionPlanResponse.Edges"/> (looked up by <see cref="EdgeId"/>).</summary>
public sealed record PlanStageInEdgeDto(int EdgeId, string Role);

/// <summary>One node in a table's partitioned dataflow graph — projected 1:1 from
/// <see cref="StreamForge.Engine.Dataflow.TableStageDescriptor"/> (Kind stringified via ToString(), same
/// PascalCase-enum-as-string convention the rest of this API's DTOs use, e.g. TableSearchMode).</summary>
public sealed record PlanStageDto(int StageId, string Kind, string Alias, IReadOnlyList<PlanStageInEdgeDto> InEdges);

/// <summary>One directed edge in a table's partitioned dataflow graph — projected 1:1 from
/// <see cref="StreamForge.Engine.Dataflow.TableEdgeDescriptor"/>. FromStageId/ToStageId == -1 have the
/// same "external input" / "terminal output" meaning as the engine type. ArrangeKeyFields is null for
/// every non-arrangeable edge.</summary>
public sealed record PlanEdgeDto(
    int EdgeId,
    int FromStageId,
    int ToStageId,
    string Role,
    string Mode,
    IReadOnlyList<string> ExternalInputNames,
    IReadOnlyList<string>? ArrangeKeyFields);

/// <summary>GET /api/pipelines/{id}/plan and GET /api/tables/{id}/plan response. <see cref="Physical"/>
/// is true only when <see cref="Stages"/>/<see cref="Edges"/> carry a real compiled stage/edge graph
/// (a Parallelism &gt;= 2 table on the Orleans flavor whose plan shape supports partitioning); otherwise
/// they're empty and <see cref="UnavailableReason"/> explains why (a pipeline — no partitioned dataflow
/// concept at all; a Parallelism == 1 table — the classic single-grain path; the Dapr flavor —
/// partitioned execution is Orleans-only, decision D-F; or SQL that doesn't currently compile).
/// <see cref="Inputs"/> is always populated when the SQL compiles (pipeline: SourceNames; table:
/// StreamInputs ∪ TableInputs), independent of Physical.</summary>
public sealed record ExecutionPlanResponse(
    string? PlanSummary,
    IReadOnlyList<string> Inputs,
    IReadOnlyList<PlanStageDto> Stages,
    IReadOnlyList<PlanEdgeDto> Edges,
    int Parallelism,
    bool Physical,
    string? UnavailableReason);
