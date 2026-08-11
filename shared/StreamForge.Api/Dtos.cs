using StreamForge.Abstractions;

namespace StreamForge.Api;

// ============================================================================
// REST DTOs — must match web/src/api/types.ts exactly (camelCase via default
// System.Text.Json naming policy).
// ============================================================================

public sealed record LoginRequest(string Username, string Password);

public sealed record LoginResponse(string Token, string Username, string DisplayName, string Role);

public sealed record UserInfo(string Username, string DisplayName, string Role, long CreatedAtMs);

public sealed record CreateUserRequest(string Username, string DisplayName, string Role, string Password);

public sealed record UpdateUserRequest(string? DisplayName, string? Role, string? Password);

public sealed record CreatePipelineRequest(
    string Name,
    string Description,
    string Sql,
    List<string>? Tags = null,
    Dictionary<string, string>? Metadata = null);

public sealed record ValidateRequest(string Sql);

public sealed record SqlDiagnosticDto(string Message, int Line, int Column, string Severity);

public sealed record ValidateResponse(bool Ok, IReadOnlyList<SqlDiagnosticDto> Diagnostics, string? PlanSummary, IReadOnlyList<string> SourceNames);

public sealed record ErrorResponse(string Error);

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
    int JournalMaxEntries = 0);

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
public sealed record TableRowsResponse(IReadOnlyList<TableRowDto> Rows, int TotalRows, long Seq, long? FrontierEpoch = null);

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
