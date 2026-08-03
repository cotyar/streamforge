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
}

// ============================================================================
// Plan 006: ingestion connectors. Secret values (URL header values, gRPC password/token) read
// back as the mask '***'; sending '***' on write means "keep the stored value".
// ============================================================================
export type SourceKind = 'generator' | 'url' | 'file' | 'folder' | 'grpc'
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
}

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
}

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
}

/** Verbatim 503 body when Gemini:ApiKey/GEMINI_API_KEY is unset — match this text, not just the status. */
export const CHAT_NOT_CONFIGURED_MESSAGE =
  'AI chat is not configured — set GEMINI_API_KEY (or Gemini:ApiKey) and restart.'
