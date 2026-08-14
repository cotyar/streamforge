// ============================================================================
// FROZEN API CONTRACT — mirrors StreamForge.Abstractions models + REST DTOs.
// The SPA track codes against exactly this; the Host serializes to match
// (System.Text.Json camelCase).
// ============================================================================

export type Role = 'Admin' | 'Editor' | 'Viewer'
export type FieldType = 'String' | 'Double' | 'Long' | 'Bool' | 'Timestamp' | 'Json'
export type PipelineStatus = 'Stopped' | 'Running' | 'Failed'

export interface FieldDef {
  name: string
  type: FieldType
  /** Declared nested shape of a `Json` field (drill-down schema). Absent for scalar fields. */
  children?: FieldDef[]
  /** The field holds a JSON array rather than a single value (additive; absent/false = scalar, the
   * pre-existing default). Combined with the other two: isArray + children = a typed list of records
   * (each element shaped like `children`); isArray + no children (type != 'Json') = a repeated scalar;
   * isArray + type 'Json' + no children = a repeated schemaless value. */
  isArray?: boolean
}

// ============================================================================
// Feature A: user-editable metadata (tags + key-values), additive on sources/pipelines/tables.
// ============================================================================
export type Tags = string[]
export type Metadata = Record<string, string>

export interface SourceDefinition {
  name: string
  description: string
  fields: FieldDef[]
  generatorProfile: 'trades' | 'quotes' | 'orders' | 'generic'
  eventsPerSecond: number
  enabled: boolean
  tags: Tags
  metadata: Metadata
  /** Source kind (plan 006, additive). Absent/'generator' = the pre-existing synthetic behavior. */
  kind?: SourceKind
  /** Connector configuration; absent for generator-kind sources (plan 006). */
  connector?: ConnectorConfig
  /** Client-push ingress config (plan 008 W4); present only for 'ingest'-kind sources. */
  ingest?: IngestConfig
  /** Plan 009 C2: coercion-failure policy for this source's inbound rows. Absent = 'Null'. */
  onCoercionFailure?: CoercionFailurePolicy
}

// ============================================================================
// Plan 006: ingestion connectors. Secret values (URL header values, gRPC password/token) read
// back as the mask '***'; sending '***' on write means "keep the stored value".
// ============================================================================
/** Plan 010: the kinds with drivers of their own, plus `(string & {})` for any registered message
 *  transport — the console learns those from GET /api/transports rather than from this union, so a new
 *  transport does not need a line here. The literals stay for autocompletion and for the places that
 *  genuinely branch on a built-in kind. */
export type SourceKind = 'generator' | 'url' | 'file' | 'folder' | 'grpc' | 'ingest' | 'nats' | (string & {})
export const SECRET_MASK = '***'
export type FileFormat = 'ndjson' | 'json' | 'csv'

/** Cron (5/6-field, UTC) XOR fixed interval (min 1000 ms). */
export interface ScheduleSpec {
  cron?: string | null
  intervalMs?: number | null
}

export interface OpenApiRef {
  docUrl?: string | null
  docInline?: string | null
  operationId?: string | null
  schemaPointer?: string | null
}

export interface UrlPollConfig {
  url: string
  headers: Record<string, string>
  openApi?: OpenApiRef | null
  /** Plan 012: response body format. Absent = 'json', which is what this kind did before the field
   *  existed — an endpoint serving text/csv or NDJSON needs no file in between. */
  format?: FileFormat
}

export interface FilePollConfig {
  path: string
  format: FileFormat
}

export interface FolderPollConfig {
  path: string
  format: FileFormat
  /** Glob over file names within the folder (no recursion), e.g. "*.json". */
  glob?: string | null
}

export interface GrpcSubConfig {
  address: string
  /** "source:{name}" | "pipeline:{id}" | "table:{id}" on the REMOTE instance. */
  entityKey: string
  username?: string | null
  password?: string | null
  token?: string | null
  /** 'reflection' (default) | 'proto'. */
  schemaSource: string
  protoText?: string | null
  restAddress?: string | null
}

/** JSONPath-lite subset for paths: $ .name ['name'] [n] [*] — nothing else. */
export interface MappingSpec {
  itemsPath: string
  dedupKeyField?: string | null
  timestampField?: string | null
  fields: FieldMapEntry[]
}

export interface FieldMapEntry {
  /** Path relative to the item; null = same as field.name. */
  sourcePath?: string | null
  field: FieldDef
}

export interface ConnectorConfig {
  schedule?: ScheduleSpec | null
  url?: UrlPollConfig | null
  file?: FilePollConfig | null
  folder?: FolderPollConfig | null
  grpc?: GrpcSubConfig | null
  mapping?: MappingSpec | null
  /** Plan 009 B1; set only for 'nats'-kind sources. */
  nats?: NatsSubConfig | null
}

/** Plan 009 B1. Credentials follow the same secrets-lite convention as the other connectors:
 *  read back as '***', and sending '***' means "keep the stored value". */
export interface NatsSubConfig {
  url: string
  subject: string
  /** Two replicas sharing a queue group split the subject instead of both ingesting everything. */
  queueGroup: string
  format: FileFormat
  token?: string | null
  username?: string | null
  password?: string | null
  /** Contents of a .creds file, not a path. */
  credentials?: string | null
  /** Null = core NATS (at-most-once, no server-side state). Non-null opts into a durable consumer. */
  jetStream?: NatsJetStreamConfig | null
}

export interface NatsJetStreamConfig {
  stream: string
  durable: string
  maxAckPending: number
}

/** Plan 009 B2: the platform's first outbound concept. Delivery is fire-and-forget — a slow or
 *  absent broker drops, it does not slow the pipeline down. */
export interface SinkSpec {
  /** Any registered sink kind — see TransportCatalog.outbound. */
  kind: string
  enabled: boolean
  nats?: NatsPubConfig | null
  file?: FileSinkConfig | null
}

/** Plan 012: the file egress sink — appends to a file on the HOST's filesystem, never truncates. */
export interface FileSinkConfig {
  /** May contain {name}, replaced with the pipeline id / table name. */
  path: string
  /** 'csv' | 'ndjson' — 'json' is absent on purpose (an append-only writer can't close the array). */
  format: FileFormat
  /** CSV only: explicit column order, comma-separated. Empty = the first written row's order. */
  columns: string
}

export interface NatsPubConfig {
  url: string
  /** May contain {name}, replaced with the pipeline/table name. */
  subject: string
  token?: string | null
  username?: string | null
  password?: string | null
  credentials?: string | null
}

/** Plan 010: what the console needs to render a transport's config form, served by GET /api/transports.
 *  Mirrors StreamForge.AppCore.Transports.TransportDescriptor. A transport added to the backend gets a
 *  working editor here with no change to this file — the descriptor IS the form. */
export interface TransportDescriptor {
  kind: string
  label: string
  help?: string | null
  /** Which property of ConnectorConfig / SinkSpec holds this transport's config object (e.g. "nats"). */
  configProperty: string
  fields: TransportField[]
  groups: TransportGroup[]
}

export type TransportFieldType = 'string' | 'secret' | 'number' | 'bool' | 'select'

export interface TransportField {
  key: string
  label: string
  type: TransportFieldType
  group?: string | null
  required: boolean
  mono: boolean
  placeholder?: string | null
  help?: string | null
  options?: string[] | null
  /** Initial value for a NEW entity, as a string coerced by `type`. */
  default?: string | null
}

export interface TransportGroup {
  key: string
  label: string
  help?: string | null
  /** Rendered with an on/off switch; when off, `objectKey` is written as null. */
  optional: boolean
  /** When set, this group's fields live in a nested object under this property. */
  objectKey?: string | null
}

/** GET /api/transports */
export interface TransportCatalog {
  inbound: TransportDescriptor[]
  outbound: TransportDescriptor[]
}

/** Plan 009 C2: what an inbound row does when a value will not coerce to its declared field type.
 *  Whichever is chosen, the failure is counted and surfaced. PascalCase on the wire. */
export type CoercionFailurePolicy = 'Null' | 'DropRow' | 'RejectBatch'

/** GET /api/sources/{name}/status — null body (204) for generator-kind sources. */
export interface ConnectorRuntimeStatus {
  sourceName: string
  nextRunMs?: number | null
  lastRunMs?: number | null
  lastStatus: 'never' | 'ok' | 'error'
  lastError?: string | null
  consecutiveFailures: number
  eventsEmittedTotal: number
  lastBatchCount: number
  /** Plan 009 C2: cumulative field coercion failures — rows or fields that would not convert to their
   *  declared type. Under DropRow/RejectBatch these are rows that did not land, so the counter is the
   *  queryable half of "counted and surfaced". Absent from a pre-009 backend. */
  coercionFailuresTotal?: number
}

/** POST /api/sources/schema/mapping-validate */
export interface MappingValidateRequest {
  document: string
  sample?: string | null
}
export interface MappingValidateResult {
  ok: boolean
  mapping?: MappingSpec | null
  diagnostics: string[]
  previewRows: Record<string, unknown>[]
}

/** POST /api/sources/schema/derive-openapi */
export interface SchemaDeriveRequest {
  openApi: OpenApiRef
}
export interface SchemaDeriveResult {
  fields: FieldDef[]
  diagnostics: string[]
}

/** POST /api/sources/schema/from-remote */
export interface RemoteSchemaRequest {
  grpc: GrpcSubConfig
}
export interface RemoteSchemaResult {
  fields: FieldDef[]
  fieldNumbersJson: string
  diagnostics: string[]
}

/** POST /api/config/import response. */
export interface ConfigImportReportEntry {
  kind: 'source' | 'pipeline' | 'table'
  name: string
  action: 'created' | 'updated' | 'deleted' | 'skipped' | 'error'
  diagnostics: string[]
}
export interface ConfigImportReport {
  mode: 'validate' | 'merge' | 'replace'
  entries: ConfigImportReportEntry[]
  ok: boolean
}

export interface PipelineDefinition {
  id: string
  name: string
  description: string
  sql: string
  status: PipelineStatus
  error: string | null
  createdBy: string
  createdAtMs: number
  updatedAtMs: number
  tags: Tags
  metadata: Metadata
  /** Plan 008 W5: real leaf source names this pipeline reads, from the last successful compile — the
   * pipeline-side counterpart of TableDefinition's streamInputs/tableInputs. Optional/additive — absent
   * on responses from a pre-W5 backend; empty until the SQL compiles. */
  sourceNames?: string[]
  /** Plan 009 B2: where this pipeline's rows are republished. Absent/empty = nowhere. */
  sinks?: SinkSpec[]
}

export interface JsonObject {
  [key: string]: RowValue
}
export type JsonArray = RowValue[]
export type RowValue = string | number | boolean | null | JsonObject | JsonArray
export type ResultRow = Record<string, RowValue>

export interface ResultEnvelope {
  pipelineId: string
  seq: number
  timestampMs: number
  row: ResultRow
}

export interface PipelineMetrics {
  pipelineId: string
  status: PipelineStatus
  eventsInPerSec: number
  rowsOutPerSec: number
  totalEventsIn: number
  totalRowsOut: number
  windowsClosed: number
  lastEventTsMs: number
}

export interface SqlDiagnostic {
  message: string
  line: number
  column: number
  severity: 'Error' | 'Warning'
}

export interface ValidateResponse {
  ok: boolean
  diagnostics: SqlDiagnostic[]
  planSummary: string | null
  sourceNames: string[]
}

export interface LoginRequest {
  username: string
  password: string
}

export interface LoginResponse {
  token: string
  username: string
  displayName: string
  role: Role
}

export interface UserInfo {
  username: string
  displayName: string
  role: Role
  createdAtMs: number
}

export interface CreateUserRequest {
  username: string
  displayName: string
  role: Role
  password: string
}

export interface UpdateUserRequest {
  displayName?: string
  role?: Role
  password?: string
}

// SignalR hub `/hubs/stream` — client→server: subscribePipeline(id), unsubscribePipeline(id),
// subscribeSource(name), unsubscribeSource(name), subscribeMetrics().
// Server→client events:
export interface HubEvents {
  pipelineResult: (pipelineId: string, rows: ResultEnvelope[]) => void
  pipelineMetrics: (metrics: PipelineMetrics) => void
  pipelineStatus: (pipelineId: string, status: PipelineStatus) => void
  sourceEvent: (sourceName: string, event: ResultRow) => void
}

// ============================================================================
// Materialized tables — persistent Z-set incremental views over streams and
// other tables. Shares the pipelines' Stopped/Running/Failed status union.
// Field names below were confirmed empirically against the running backend
// (curl, editor/editor123!) rather than assumed — the validate response in
// particular uses different field names (`outputSchema`/`kind`) than the
// TableDefinition's own `outputFields`/`type`.
// ============================================================================

export type TableStatus = PipelineStatus

// Per-table reverse (inverted) search index. Exact/Fuzzy chosen per table; toggling either field
// via update triggers a backend restart of that table's pipeline.
export type TableSearchMode = 'Exact' | 'Fuzzy'

// Feature B: opt-in per-table ROW HISTORY retention mode — see TableHistoryStats/TableHistoryResponse
// below and TableDetailPage's history config card.
export type TableHistoryMode = 'All' | 'LastN' | 'FirstN' | 'MinBy' | 'MaxBy'

export interface TableDefinition {
  id: string
  name: string
  description: string
  sql: string
  status: TableStatus
  error: string | null
  createdBy: string
  createdAtMs: number
  updatedAtMs: number
  outputFields: FieldDef[]
  streamInputs: string[]
  tableInputs: string[]
  searchEnabled: boolean
  searchMode: TableSearchMode
  historyEnabled: boolean
  historyMode: TableHistoryMode
  historyLimit: number
  historyByField: string | null
  historyWindowMs: number
  tags: Tags
  metadata: Metadata
  /** Plan 003 M5: 1 (default) = classic single-grain execution; 2..16 deploys the partitioned
   * dataflow graph (see TableMetrics.partitions). Changing it restarts the table. */
  parallelism: number
  /** Plan 008: durability policy for the materialized snapshot. Optional — absent means 'Batched',
   * the pre-008 behavior. */
  persistence?: TablePersistenceMode
  /** Flush interval in ms for Batched/FireAndForget; 0 or absent = 2000. */
  flushMs?: number
  /** Plan 009 A2: journal length that triggers compaction, for 'Journaled' only. 0/absent = default. */
  journalMaxEntries?: number
  /** Plan 009 B2: where this table's deltas are republished. Absent/empty = nowhere. */
  sinks?: SinkSpec[]
  /** Plan 011 C2: opt-in row retention — maximum rows the table retains, oldest-first by EVENT
   * timestamp. 0 or absent = unbounded (the default, and the pre-011 behavior). Non-zero makes the
   * table a BOUNDED VIEW of its SQL's relation: rows past the bound are evicted with real retractions,
   * so downstream tables, the live grid, sinks, search and row history all follow along — but the
   * table no longer holds everything its SQL says it should. Rejected (409) for SQL with joins, set
   * operations, derived sources or GROUP BY/aggregates, and for parallelism > 1. */
  retentionMaxRows?: number
  /** Plan 011 C2: maximum age of a retained row, in EVENT-time ms measured back from the highest event
   * timestamp the table has seen — not wall clock, so replay is deterministic and a stalled input ages
   * nothing out. 0 or absent = unbounded. Composes with retentionMaxRows (age first, then count). */
  retentionTtlMs?: number
  /** Plan 011 D1: opt-in KEY SHARDING — the output columns this table's rows are sharded by. Absent or
   * empty (the default) = not sharded, i.e. exactly today's behavior. When set, every delta the table
   * emits is routed to a per-key shard grain holding just that key's rows and version history, and an
   * idle key's shard deactivates so its state lives on disk until the next lookup — which is the point.
   *
   * NOT the same knob as retentionMaxRows/retentionTtlMs, and worth not confusing: retention DELETES
   * rows to bound a table; sharding KEEPS everything and bounds what stays RESIDENT. They compose (a
   * retention eviction reclaims that row's shard history too, since history follows the table).
   *
   * Orleans-only. On the Dapr flavor the field is stored but such a table cannot be started (the error
   * says so). Rejected (409) together with searchEnabled: a table-wide reverse index would keep every
   * row resident and defeat sharding, so the combination is refused rather than half-served. */
  shardBy?: string[]
}

/** Plan 008: how a table's snapshot reaches storage (wire values are PascalCase — confirmed against
 * the running backend's JsonStringEnumConverter, same convention as TableSearchMode). 'Batched' awaits
 * the write inside the grain turn (durable, but the stall grows with the row count); 'FireAndForget'
 * returns the turn immediately and writes in the background (a crash loses the unwritten tail);
 * 'MemoryOnly' never writes, so a restart brings the table back empty; 'Journaled' (plan 009) has
 * Batched's durability but writes only the rows that changed, compacting to a full snapshot when the
 * journal outgrows journalMaxEntries. */
export type TablePersistenceMode = 'Batched' | 'FireAndForget' | 'MemoryOnly' | 'Journaled'

export interface TableRowDto {
  row: ResultRow
  weight: number
}

export interface TableRowsResponse {
  rows: TableRowDto[]
  totalRows: number
  seq: number
  /** Plan 003 M4: non-null only for a parallelism >= 2 table once its coordinator has completed a full
   * round (see TableMetrics.snapshotFrontierEpoch — same value). When present, `rows` reflects ALL deltas
   * whose epoch is <= frontierEpoch and NONE beyond it. */
  frontierEpoch?: number | null
  /** Plan 011 D1: present only for a SHARDED table, and only to say where these rows came from. A
   * keyless listing is served from the table's consolidated snapshot and consults NO shard
   * (`shardsConsulted: false`) — deliberately, because this endpoint is polled every 2s and a fan-out
   * across the shard directory would wake every idle key on every poll and undo the whole feature.
   * For one key use POST /api/tables/{id}/shard/lookup; for all of them, GET .../shards/scan. */
  shards?: TableRowsShardNote | null
}

/** Plan 011 D1 — see TableRowsResponse.shards. */
export interface TableRowsShardNote {
  shardBy: string[]
  shardsConsulted: boolean
  note: string
}

/** Plan 011 D1: POST /api/tables/{id}/shard/lookup?historyLimit= — "give me everything for this key".
 * Body is `{ row }` carrying at least the table's shardBy columns (the whole row is fine); the server
 * derives the shard key with the same codec the router uses on live deltas. One grain, strictly
 * consistent by construction. Activates that one shard — which is the intended cost of asking. */
export interface ShardLookupRequest {
  row: Record<string, unknown>
}

export interface TableShardHistoryEntry {
  rowKey: string
  /** Newest-first, capped by the request's historyLimit (0/absent = all retained). */
  versions: HistoryVersion[]
  retractionCount: number
  /** Retained version count before the limit was applied. */
  totalVersions: number
}

export interface TableShardView {
  /** false = nothing was ever routed to this key (as opposed to a key whose rows were all retracted,
   * which is found with an empty `rows`). */
  found: boolean
  shardKey: string
  rows: TableRowDto[]
  history: TableShardHistoryEntry[]
  /** Highest router sequence this shard has applied; -1 = nothing applied yet. */
  appliedSeq: number
  deltasApplied: number
  historyEnabled: boolean
}

/** Plan 011 D1: GET /api/tables/{id}/shards?limit=&offset= — metrics + the live key set. Wakes NO
 * shard, so it is safe to poll. `residentShardCount` far below `shardCount`, with `activations` far
 * above `residentShardCount`, is the evidence that shards are genuinely being swapped out and
 * reloaded. Both are per-process (single-silo) figures. */
export interface TableShardsResponse {
  enabled: boolean
  shardBy: string[]
  /** Distinct live shard keys, from the shard directory — which is itself resident and O(keys). */
  shardCount: number
  residentShardCount: number
  activations: number
  deactivations: number
  routerSeq: number
  routedBatches: number
  routedDeltas: number
  routerActive: boolean
  keys: string[]
}

export interface TableShardStats {
  shardKey: string
  rowCount: number
  historyKeyCount: number
  totalVersions: number
  appliedSeq: number
  /** Plan 011 D2: deltas this shard has ever applied, persisted across deactivation. Summed over every
   * shard of a table it must equal a fenced scan's `routedDeltasAtFence` — which is what makes the cut
   * checkable rather than merely claimed. */
  deltasApplied: number
}

/** Plan 011 D1: GET /api/tables/{id}/shards/scan?limit=&offset= — the EXPLICIT full scan. This one DOES
 * activate every shard in its page (`woke` says how many); it is a separate endpoint precisely so that
 * no routine poll can reach it. Not a consistent cut: shards are read one after another while ingest
 * continues, and each shard's own appliedSeq says where it was. */
export interface TableShardScanResponse {
  shards: TableShardStats[]
  woke: number
  offset: number
  limit: number
  /** Plan 011 D2 — `?fenced=true`. false (the default) is a set of per-shard observations taken at
   * DIFFERENT sequence numbers while ingest continued; true is a genuine consistent cut at `fenceSeq`,
   * bought by pausing the shard tier's ingest for the duration of the scan. Per-key reads need no fence
   * and get none: one key is one grain and is already strictly consistent. */
  fenced?: boolean
  /** The router sequence the cut was taken at; -1 = nothing has ever been routed. */
  fenceSeq?: number
  /** Deltas the router had forwarded at the fence. When the page covers every shard (`shards.length ===
   * shardCount`), the shards' `deltasApplied` must sum to exactly this. */
  routedDeltasAtFence?: number
  /** Live shard keys at the fence, so a caller can tell "this page is all of them" from "this is a page". */
  shardCount?: number
}

// GET /api/tables/{id}/search?q=&limit= — empty q returns rows: []. A 400 means the table's
// searchEnabled is false (see error.message: "Search is not enabled for this table.").
export interface TableSearchResponse {
  rows: TableRowDto[]
  mode: TableSearchMode
  enabled: boolean
  total: number
}

// Plan 003 M5: one partition's contribution to a parallelized table's aggregate TableMetrics —
// confirmed empirically against a live P=4 table (curl, editor/editor123!). stageId identifies which
// TableStageGrain this is (numbering is plan-internal).
export interface TablePartitionMetrics {
  stageId: number
  partition: number
  deltasIn: number
  deltasOut: number
  /** -1 = never advanced yet. */
  frontierEpoch: number
  lastUpdateMs: number
  /** Plan 003 M4: this stage's real operator name (e.g. "Join" | "SemiAnti" | "Unnest" |
   * "FilterProject" | "Reduce" | "LatestBy" — see StreamForge.Engine.Dataflow.TableStageKindLabel on the
   * backend). "" only in the (never-happens-in-practice) case the producing grain never learned its own
   * stage descriptor. */
  kind: string
}

export interface TableMetrics {
  tableId: string
  status: TableStatus
  rowCount: number
  deltasIn: number
  deltasOut: number
  lastUpdateMs: number
  rebuilding?: boolean
  /** Plan 003 M5: per-partition detail, present only for a parallelism >= 2 table — absent for every
   * parallelism == 1 table. */
  partitions?: TablePartitionMetrics[] | null
  /** Plan 003 M4: the epoch this table's read-side snapshot (rowCount / rows / search) reflects — same
   * value as TableRowsResponse.frontierEpoch. Non-null only for a parallelism >= 2 table once its
   * coordinator has completed a full round. */
  snapshotFrontierEpoch?: number | null
  /** Server-derived (from the table's definition, not measured): a plain-language warning that this
   * table's per-row VERSION TRAIL is degraded, absent/null when it is not — which is the normal case.
   * It rides the metrics object for the same reason `rebuilding` does: this is where the console looks
   * for "this table is not in the state you think it is".
   *
   * Exactly one condition is reported: the SQL declares a GROUP BY / LATEST BY row identity whose keys
   * the server could not match to output columns (an expression key, a CAST, a JSON path that doesn't
   * match the projection character-for-character), so rows fall back to being keyed by their WHOLE
   * content and successive versions of one row never group into a trail. Reported only where it costs
   * something — history enabled, or a sharded table. A table with no GROUP BY / LATEST BY at all is NOT
   * flagged: the whole row genuinely is its identity there, and always was.
   *
   * A warning, not a rejection — the table was accepted and runs. Render it verbatim (it names the keys
   * and the fix); see TableDetailPage's Row history card and ShardingPanel. */
  rowIdentityWarning?: string | null
}

export interface TableOutputField {
  name: string
  kind: FieldType
}

export interface TableValidateResponse {
  ok: boolean
  diagnostics: SqlDiagnostic[]
  planSummary: string | null
  streamInputs: string[]
  tableInputs: string[]
  outputSchema: TableOutputField[]
}

// ============================================================================
// Plan 008 W5: GET /api/pipelines/{id}/plan and GET /api/tables/{id}/plan — lineage + execution-plan
// view for the console's React Flow page. Shape pinned verbatim against the backend
// (shared/StreamForge.Api/Dtos.cs's ExecutionPlanResponse/PlanStageDto/PlanEdgeDto/PlanStageInEdgeDto) —
// do not rename fields without updating both sides. `physical` is true only when `stages`/`edges` carry
// a real compiled stage/edge graph (a parallelism >= 2 table on the Orleans flavor whose plan shape
// supports partitioning); otherwise they're empty arrays and `unavailableReason` explains why. A
// pipeline's plan is ALWAYS the logical view (`physical: false`) — pipelines have no partitioned
// dataflow graph concept at all.
// ============================================================================

export interface PlanStageInEdge {
  edgeId: number
  role: string
}

/** kind mirrors StreamForge.Engine.Dataflow.TableStageKind: 'Ingest' | 'Join' | 'SemiAnti' | 'Unnest' |
 * 'FilterProject' | 'Reduce' | 'LatestBy'. */
export interface PlanStage {
  stageId: number
  kind: string
  alias: string
  inEdges: PlanStageInEdge[]
}

/** mode mirrors StreamForge.Engine.Dataflow.TableEdgeMode: 'Local' | 'HashPartition' | 'Broadcast' |
 * 'Gather'. fromStageId/toStageId == -1 mean "external input" / "terminal output" respectively, same as
 * the engine type. arrangeKeyFields is null for every non-arrangeable edge. */
export interface PlanEdge {
  edgeId: number
  fromStageId: number
  toStageId: number
  role: string
  mode: string
  externalInputNames: string[]
  arrangeKeyFields: string[] | null
}

export interface ExecutionPlanResponse {
  planSummary: string | null
  /** pipeline: sourceNames; table: streamInputs ∪ tableInputs. Populated whenever the SQL compiles,
   * independent of `physical`. */
  inputs: string[]
  stages: PlanStage[]
  edges: PlanEdge[]
  parallelism: number
  physical: boolean
  unavailableReason?: string | null
}

// SignalR hub `/hubs/stream` (tables) — client→server: SubscribeTable(name),
// UnsubscribeTable(name). Server→client `tableDelta` confirmed empirically
// (throwaway @microsoft/signalr script against :5199) to carry 3 args:
// (tableName, deltas, seq) — the backend-wide (name, payload, seq) convention.
export interface TableHubEvents {
  tableDelta: (tableName: string, deltas: TableRowDto[], seq: number) => void
}

// ============================================================================
// Feature B: row history. One recorded ASSERTION version of a row-identity's history — see
// TableHistoryGrain (backend) for the retention semantics per historyMode.
// ============================================================================

export interface HistoryVersion {
  row: ResultRow
  tsMs: number
  seq: number
}

// POST /api/tables/{id}/history/lookup?limit= — body: { row }, the exact row object the client
// already has (from the live grid or a search result); the server derives the row-identity key from
// it. A 400 means the table's historyEnabled is false. keyFound is false when this exact row identity
// has never been observed (as opposed to observed-but-empty, which can't happen — every observation
// is either a version or a retraction bump).
export interface TableHistoryResponse {
  versions: HistoryVersion[]
  retractionCount: number
  mode: TableHistoryMode
  totalVersions: number
  keyFound: boolean
}

// GET /api/tables/{id}/history/stats
export interface TableHistoryStats {
  enabled: boolean
  mode: TableHistoryMode
  keyCount: number
  totalVersions: number
}

// ============================================================================
// Plan 007 (W1C/W2A): AI control chat — POST /api/chat (Editor policy), server-side
// Gemini tool loop over the source/pipeline/table facades. Stateless: the client resends the
// full text history every turn; toolCalls is a per-turn server-side trace only (never sent back).
// Error bodies use the shared ErrorResponse shape ({ error: string }) — client.ts's
// extractErrorMessage already surfaces `error` into ApiError.message, so callers just inspect
// ApiError.status (503 = not configured, 502 = provider failure) and .message.
// ============================================================================

export type ChatRole = 'user' | 'assistant'

export interface ChatMessage {
  role: ChatRole
  content: string
}

export interface ChatRequest {
  messages: ChatMessage[]
}

/** input/result are opaque JSON (tool-specific shape) — rendered pretty-printed, not typed further. */
export interface ChatToolCallDto {
  name: string
  input: unknown
  result: unknown
}

export interface ChatResponse {
  reply: string
  toolCalls: ChatToolCallDto[]
  model: string
  /** Concatenated model thought summaries for the turn (additive; null/absent when none). */
  thinking?: string | null
}

/** Verbatim 503 body when Gemini:ApiKey/GEMINI_API_KEY is unset — match this text, not just the status. */
export const CHAT_NOT_CONFIGURED_MESSAGE =
  'AI chat is not configured — set GEMINI_API_KEY (or Gemini:ApiKey) and restart.'

// ============================================================================
// Plan 008 W4: client-push ingress. Mirrors shared/StreamForge.Api/Dtos.cs's
// IngestEventsRequest/IngestAcceptedResponse/IngestErrorResponse/IngestStatusResponse and
// StreamForge.Contracts/IngestModels.cs's IngestConfig.
//
// NOTE the casing: the backend serializes enums as PascalCase strings (JsonStringEnumConverter with
// no naming policy), exactly like TableSearchMode and TablePersistenceMode. Lowercasing these here
// desynchronizes the UI from the server on every save.
// ============================================================================

export type IngressOverflowPolicy = 'Reject' | 'Block' | 'DropNewest' | 'DropOldest' | 'Inline'

export interface IngestConfig {
  policy: IngressOverflowPolicy
  capacityRows: number
  /** Only meaningful for 'Block'; server-capped at 30s. */
  maxWaitMs: number
  maxBatchRows: number
  rejectUnknownFields: boolean
  /** Plan 009 A1: declared field identifying a row, for at-least-once upstreams. Null/absent = off. */
  dedupKeyField?: string | null
  /** How many recent row keys to remember; 0/absent = DedupTracker's own bound. */
  dedupWindow?: number
  /** Per-source push credentials. Read back masked — the secret exists in the clear exactly once,
   *  in the response to POST /api/sources/{name}/ingest/keys. */
  keys?: IngestKey[]
}

/** Never carries the secret. Generate returns it once; after that a lost key is regenerated. */
export interface IngestKey {
  id: string
  label: string
  createdAtMs: number
  /** 0 = never used. Best-effort, per replica. */
  lastUsedMs: number
}

export interface CreateIngestKeyRequest {
  label: string
}

export interface CreatedIngestKeyResponse {
  id: string
  label: string
  /** The only time this value is ever returned. */
  secret: string
  createdAtMs: number
}

/** POST /api/sources/{name}/events. Success is 202 "buffered", never 200. */
export interface IngestEventsRequest {
  events: Record<string, unknown>[]
  /** Admit the valid rows of a batch that has invalid ones; default fails the whole batch. */
  partial?: boolean
  /** Plan 009 A1: repeating a push with the same key replays the original result and admits nothing. */
  idempotencyKey?: string
}

export interface IngestAcceptedResponse {
  accepted: number
  dropped: number
  invalid: number
  depthRows: number
  capacityRows: number
  /** Plan 009 A1: suppressed by row-level dedup — a different reason from dropped (capacity) and
   *  invalid (coercion). Absent from a pre-009 backend. */
  duplicate?: number
  /** True when this 202 restates an earlier push's counts rather than reporting a new admission. */
  replayed?: boolean
}

/** 400/409/413/429 body. retryAfterMs is 0 except on 429. */
export interface IngestErrorResponse {
  error: string
  retryAfterMs: number
  rowErrors: string[]
}

/** GET /api/sources/{name}/ingest — 404 unknown, 204 not ingest-kind, 200 otherwise.
 *  downstreamDropped is the SECOND loss point (the transport's own drops), surfaced deliberately. */
export interface IngestStatusResponse {
  policy: IngressOverflowPolicy
  capacityRows: number
  depthRows: number
  maxBatchRows: number
  totalAccepted: number
  totalRejected: number
  totalDropped: number
  totalInvalid: number
  totalPublished: number
  downstreamDropped: number
  lastPushMs: number
  totalDuplicate?: number
  /** Which process produced these numbers. Plan 008's counters were per-replica while reading as
   *  global; when aggregated is false they describe this instance alone. */
  instanceId?: string
  aggregated?: boolean
}
