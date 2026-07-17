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
}

export interface TableRowDto {
  row: ResultRow
  weight: number
}

export interface TableRowsResponse {
  rows: TableRowDto[]
  totalRows: number
  seq: number
}

// GET /api/tables/{id}/search?q=&limit= — empty q returns rows: []. A 400 means the table's
// searchEnabled is false (see error.message: "Search is not enabled for this table.").
export interface TableSearchResponse {
  rows: TableRowDto[]
  mode: TableSearchMode
  enabled: boolean
  total: number
}

export interface TableMetrics {
  tableId: string
  status: TableStatus
  rowCount: number
  deltasIn: number
  deltasOut: number
  lastUpdateMs: number
  rebuilding?: boolean
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
