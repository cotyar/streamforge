// ============================================================================
// FROZEN API CONTRACT — mirrors StreamsForge.Abstractions models + REST DTOs.
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

  // Plan 016, all optional: absent = a pre-016 server, and the console must render the entity exactly
  // as it does today rather than showing a badge for a revision nobody assigned.
  /** Registry-assigned, monotonic; bumps only when the stored definition actually changed. */
  revision?: number
  /** Registry-assigned; bumps ONLY on a field-shape change, which is what makes a pin useful. */
  schemaRevision?: number
}

// ============================================================================
// Plan 006: ingestion connectors. Secret values (URL header values, gRPC password/token) read
// back as the mask '***'; sending '***' on write means "keep the stored value".
// ============================================================================
/** Plan 010: the kinds with drivers of their own, plus `(string & {})` for any registered message
 *  transport — the console learns those from GET /api/transports rather than from this union, so a new
 *  transport does not need a line here. The literals stay for autocompletion and for the places that
 *  genuinely branch on a built-in kind. */
export type SourceKind = 'generator' | 'url' | 'file' | 'folder' | 'grpc' | 'ingest' | 'nats' | 'crdt' | (string & {})
export const SECRET_MASK = '***'
export type FileFormat = 'ndjson' | 'json' | 'csv' | 'fix'

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
  /** Name of a configured peer to take `address`/`restAddress` from. When set it WINS over both —
   *  a source naming a peer must never silently fall back to a stale literal address. */
  peer?: string | null
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
  /** The platform's open, string-valued config bag (ConnectorConfig.Settings on the server): where an
   *  OUT-OF-TREE source kind — one whose config class cannot live in StreamsForge.Contracts — keeps its
   *  fields. The console never reads a key of it by name; it renders whatever the kind's descriptor
   *  declares, and writes every value as a string (see TransportConfigEditor's SETTINGS_BAG). */
  settings?: Record<string, string> | null
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
  /** The sink half of ConnectorConfig.settings — same bag, same rules. */
  settings?: Record<string, string> | null
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
 *  Mirrors StreamsForge.AppCore.Transports.TransportDescriptor. A transport added to the backend gets a
 *  working editor here with no change to this file — the descriptor IS the form. */
export interface TransportDescriptor {
  kind: string
  label: string
  help?: string | null
  /** Which property of ConnectorConfig / SinkSpec holds this transport's config object (e.g. "nats"). */
  configProperty: string
  fields: TransportField[]
  groups: TransportGroup[]
  /** Plan 014: this kind is driven by IPolledTransport — it runs on the source's Schedule, so the console
   *  renders the schedule editor for it. False for the message family, whose Schedule is ignored. Always
   *  present on the wire (a plain bool on the backend record), not optional. */
  polled: boolean
  /** Plan 014: this kind's rows go through a MappingSpec, so the console offers the mapping editor.
   *  Defaults true SERVER-SIDE for every pre-014 transport; always present on the wire here too. */
  mapping: boolean
  /** Plan 014: the transport also implements ISchemaProbe, so the console renders "Discover schema" and
   *  posts to POST /api/transports/{kind}/probe. */
  canProbe: boolean
  /** Plan 019: this kind's source half and sink half are two views of one live session. Optional — absent
   *  on a pre-019 backend and on every non-duplex descriptor's wire shape until it opts in. */
  duplex?: boolean
}

export type TransportFieldType = 'string' | 'secret' | 'number' | 'bool' | 'select' | 'text'

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
  /** Plan 019 D3: `null` (or absent, from a pre-019 backend) means this source has no outbound half at
   *  all — an ordinary source, nothing to show. `false` means it has one and it is down right now. `true`
   *  means the session is up and can accept a send. Deliberately three-valued: collapsing null and false
   *  would paint every non-duplex source as a broken session. See `ConnectorRuntimeStatus.DuplexReady`
   *  (shared/StreamsForge.Contracts/ConnectorModels.cs) for the server-side doc this mirrors. */
  duplexReady?: boolean | null
  /** Plan 019 D3: rows the outbound half accepted, scoped to the CURRENT session instance only — it
   *  resets to 0 on every reconnect (see `IDuplexSession.SentTotal`'s own doc). Not a lifetime total. */
  duplexSentTotal?: number
  /** Plan 019 D3: rows the outbound half could not deliver, same per-session reset-on-reconnect scope as
   *  `duplexSentTotal`. */
  duplexFailedTotal?: number
  /** Plan 019 D3: the most recent outbound failure, already formatted server-side with its correlation id
   *  (a FIX order's `ClOrdID`) first — render verbatim, do not re-parse or truncate it away. `null`/absent
   *  means this session (if any) has not failed a send. */
  lastDuplexFailure?: string | null
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

/** POST /api/transports/{kind}/probe — plan 014. Body is a SourceDefinition (the draft being edited);
 *  200 always, even when the probe itself failed to reach the source — `diagnostics` carries the failure
 *  message and `fields` is empty in that case (not an error status; see TransportDescriptor.canProbe). */
export interface SchemaProbeResult {
  fields: FieldDef[]
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
  /** The pipeline's compiled OUTPUT schema — the same shape TableDefinition.outputFields has, and the
   * reason a table can now name a pipeline as one of its relations: without a declared output shape
   * there is nothing for the table's SQL (or the visual builder's column pickers) to bind to.
   * Optional/additive — absent on responses from a server that predates it, and empty until the SQL
   * compiles, so a consumer must treat "absent or empty" as "this pipeline is not offerable yet"
   * rather than as "a pipeline with no columns". */
  outputFields?: FieldDef[]
  /** Plan 009 B2: where this pipeline's rows are republished. Absent/empty = nowhere. */
  sinks?: SinkSpec[]

  // Plan 016, all optional: absent = a pre-016 server, and the console must render the entity exactly
  // as it does today rather than showing a badge for a revision nobody assigned.
  /** Registry-assigned, monotonic; bumps only when the stored definition actually changed. */
  revision?: number
  /** Author-declared pins, checked at import and at start — never continuously. */
  dependsOn?: EntityPin[]
  /** Why this entity's pins no longer hold, or absent when they do. Renders as the stale badge. */
  staleReason?: string | null
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
  /** Plan 025: events the Engine discarded as late (behind the watermark). Optional: older hosts omit it. */
  lateEvents?: number
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
  /** Plan 015, all optional: absent = a pre-015 server, and the client falls back to `role` ordering. */
  permissions?: PermissionGrant[]
  roles?: string[]
  groups?: string[]
  disabled?: boolean
  policyVersion?: number
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

// ----------------------------------------------------------------------------------------------
// Plan 015 — entitlements, groups, approvals, audit. Every field added to a PRE-EXISTING interface
// below is optional, and the client treats its absence as "an old server": UserInfo without
// `permissions` falls back to today's ordinal Viewer < Editor < Admin semantics, so a rolling deploy
// is safe in both directions.
// ----------------------------------------------------------------------------------------------

export type PermissionEffect = 'Allow' | 'Deny'
/** Tri-state on purpose — `RequiresApproval` reaches the button label ("Request approval…"). */
export type AccessDecision = 'Denied' | 'Allowed' | 'RequiresApproval'

export interface PermissionGrant {
  /** Flat dotted string with `*` wildcards: `pipeline.write`, `source.*`, `*`. */
  action: string
  /** `*` | exact id/name | prefix `prod-*` | `tag:finance`. */
  scope: string
  effect: PermissionEffect
  requiresApproval: boolean
  note?: string | null
}

export interface RoleDefinition {
  name: string
  description: string
  grants: PermissionGrant[]
  builtIn: boolean
  updatedAtMs: number
  updatedBy: string
}

export interface GroupDefinition {
  name: string
  description: string
  members: string[]
  roles: string[]
  grants: PermissionGrant[]
  externalClaimValues: string[]
  createdAtMs: number
  updatedAtMs: number
  updatedBy: string
}

export interface UserAccessEntry {
  username: string
  disabled: boolean
  roles: string[]
  grants: PermissionGrant[]
  updatedAtMs: number
  updatedBy: string
}

export interface ApprovalTemplate {
  name: string
  actionPattern: string
  scopePattern: string
  requiredApprovals: number
  approverGroups: string[]
  expiresAfterSeconds: number
  /** 0 = never escalate. */
  escalateAfterSeconds: number
  escalationGroups: string[]
  enabled: boolean
}

/** Body of `PUT /api/access/users/{username}/disabled` — the cheap 90% of token revocation, as one
 *  field, so disabling a login never means re-sending grants the caller may not know about. */
export interface SetAccessDisabledRequest {
  disabled: boolean
}

/** What `GET /api/access/effective/{username}` answers: one user's flattened, version-stamped view,
 *  built the same way every authorization decision builds it. `version` is the policy snapshot that
 *  produced it. */
export interface EffectivePermissions {
  username: string
  disabled: boolean
  roles: string[]
  groups: string[]
  grants: PermissionGrant[]
  version: number
}

export interface AccessPolicyDocument {
  roles: RoleDefinition[]
  groups: GroupDefinition[]
  users: UserAccessEntry[]
  approvalTemplates: ApprovalTemplate[]
  version: number
  updatedAtMs: number
}

export type ApprovalState =
  | 'Pending'
  | 'Approved'
  | 'Rejected'
  | 'Expired'
  | 'Executed'
  | 'Failed'
  | 'Cancelled'

export interface ApprovalVote {
  username: string
  approve: boolean
  atMs: number
  comment?: string | null
}

export interface ApprovalRequest {
  id: string
  requestedBy: string
  requestedAtMs: number
  action: string
  scope: string
  reason: string
  templateName: string
  requiredApprovals: number
  votes: ApprovalVote[]
  state: ApprovalState
  expiresAtMs: number
  escalatedAtMs?: number | null
  payloadJson?: string | null
  outcome?: string | null
  decidedAtMs?: number | null
  /** "rest" | "chat" | "grpc" — a chat-proposed action must be visible as such in the inbox. */
  origin: string
  approverGroups: string[]
}

export interface AuditEntry {
  id: string
  atMs: number
  actor: string
  action: string
  scope: string
  /** "allowed" | "denied" | "requires-approval" | "executed" | "failed". */
  outcome: string
  detail?: string | null
  /** Set when the chat acted: `actor` is the model, this is the human whose token it carried. */
  onBehalfOf?: string | null
  approvalId?: string | null
  beforeJson?: string | null
  afterJson?: string | null
  origin: string
}

export interface AuditPage {
  entries: AuditEntry[]
  /** Persisted and never reset — drop-oldest silence must not read as absence. */
  truncated: number
  total: number
}

/** Body of `POST /api/approvals`. Four fields, and there is deliberately no `requestedBy`: the server
 *  stamps the authenticated principal, which is what the "you cannot approve your own request" rule
 *  rests on. */
export interface FileApprovalRequest {
  action: string
  scope: string
  reason?: string | null
  /** The request that would have executed, serialized. Replaying from it is the only replay
   *  mechanism — nothing about the original HTTP request is retained. */
  payloadJson?: string | null
}

/** Optional body of `POST /api/approvals/{id}/approve|reject|cancel`. The vote's username and
 *  timestamp are server-set; only the comment is the caller's. */
export interface ApprovalDecisionRequest {
  comment?: string | null
}

/** What `GET /api/audit/{day}` answers. `truncated` is REQUIRED, not optional: the day shard drops
 *  oldest-first under `Audit:MaxEntriesPerDay` and counts what it dropped so that silence is never
 *  mistaken for absence — a client that could omit it would undo that.
 *
 *  `beforeJson`/`afterJson` on the entries are withheld unless the request passed
 *  `?includeChanges=true` AND the caller is entitled to `access.read` (the same opt-in shape
 *  `GET /api/config/export?includeSecrets` uses, because those payloads can carry stored secrets).
 *  `changesIncluded` says whether this response carries them; `changesWithheld` counts the rows that
 *  had something to carry. */
export interface AuditPageResponse {
  day: string
  entries: AuditEntry[]
  truncated: number
  total: number
  changesIncluded: boolean
  changesWithheld: number
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
  /** Pipeline NAMES this table reads — the third input list beside streamInputs (sources) and
   * tableInputs (other tables), populated from the last successful compile. Optional/additive: absent
   * on a server that predates pipeline-into-table, which must render exactly as it does today. */
  pipelineInputs?: string[]
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
  /** Wishlist #18: this table's row-identity key, recomputed on the same compile as outputFields —
   * server-owned, never client-writable. THE THREE STATES ARE NOT INTERCHANGEABLE: a non-empty array is
   * the resolved GROUP BY / LATEST BY key columns (supersede rows that agree on all of them); an empty
   * array ([]) is an UNKEYED GLOBAL AGGREGATE — e.g. 'SELECT COUNT(*) FROM x' with no GROUP BY — the
   * table always has exactly one row, so any new row simply replaces it; null/absent is WHOLE-ROW
   * identity — either no GROUP BY/LATEST BY was declared at all (a plain per-event passthrough, where
   * the whole row always was the identity), or one was declared but couldn't be confidently mapped to an
   * output column (the same degraded fallback the /metrics rowIdentityWarning reports) — either way the
   * engine keys those rows by their whole content, so null is the answer that matches actual dedup
   * behavior, not merely the conservative default. Absent (older engine builds) should be treated the
   * same as null. */
  keyFields?: string[] | null

  // Plan 016, all optional: absent = a pre-016 server, and the console must render the entity exactly
  // as it does today rather than showing a badge for a revision nobody assigned.
  /** Registry-assigned, monotonic; bumps only when the stored definition actually changed. */
  revision?: number
  /** Registry-assigned; bumps ONLY on a field-shape change, which is what makes a pin useful. */
  schemaRevision?: number
  /** Author-declared pins, checked at import and at start — never continuously. */
  dependsOn?: EntityPin[]
  /** Why this entity's pins no longer hold, or absent when they do. Renders as the stale badge. */
  staleReason?: string | null
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
   * "FilterProject" | "Reduce" | "LatestBy" — see StreamsForge.Engine.Dataflow.TableStageKindLabel on the
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
  /** Plan 014: output columns this table's declared row identity resolves to; empty when it declares
   *  none, or when it declares one that could not be mapped (in which case rowIdentityWarning is set and
   *  a suggestion would be a guess at the thing the operator most needs to get right). Used to prefill a
   *  database sink's key columns in upsert mode. */
  declaredKeyColumns?: string[]
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
// (shared/StreamsForge.Api/Dtos.cs's ExecutionPlanResponse/PlanStageDto/PlanEdgeDto/PlanStageInEdgeDto) —
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

/** kind mirrors StreamsForge.Engine.Dataflow.TableStageKind: 'Ingest' | 'Join' | 'SemiAnti' | 'Unnest' |
 * 'FilterProject' | 'Reduce' | 'LatestBy'. */
export interface PlanStage {
  stageId: number
  kind: string
  alias: string
  inEdges: PlanStageInEdge[]
}

/** mode mirrors StreamsForge.Engine.Dataflow.TableEdgeMode: 'Local' | 'HashPartition' | 'Broadcast' |
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
// Plan 008 W4: client-push ingress. Mirrors shared/StreamsForge.Api/Dtos.cs's
// IngestEventsRequest/IngestAcceptedResponse/IngestErrorResponse/IngestStatusResponse and
// StreamsForge.Contracts/IngestModels.cs's IngestConfig.
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

// ----------------------------------------------------------------------------------------------
// Plan 016 — pins, and the discovery payloads. Everything a pre-016 server omits is optional above;
// these two types are only ever read from routes a pre-016 server does not serve at all, so they are
// not optional-per-field — a caller either got an answer or got a 404.
// ----------------------------------------------------------------------------------------------

/** One entry in `dependsOn`: "I was authored against THIS shape of THAT entity". Pinned on
 *  `schemaRevision`, never the plain revision — a pin that fires on an unrelated knob edit is a pin
 *  people learn to ignore. By NAME, because sources have no id at all. */
export interface EntityPin {
  /** 'source' | 'table'. Nothing pins a pipeline — nothing reads a pipeline's output by name. */
  kind: string
  name: string
  /** 0 = a declared edge with no compatibility claim, which import ordering still needs. */
  schemaRevision: number
}

/** What `GET /api/meta/instance` answers. Anonymous, like `/healthz`. */
export interface InstanceInfo {
  /** Stable across restarts — persisted, so "is this the same instance as yesterday" is answerable. */
  instanceId: string
  /** Operator-chosen and NOT unique; the id is the identity. */
  name: string
  /** 'orleans' | 'dapr'. */
  flavor: string
  version: string
  /** Keyed by protocol ('rest', 'grpc'). */
  endpoints: Record<string, string>
  /** Test for a feature rather than inferring it from `version`. */
  capabilities: string[]
  plugins: string[]
  catalogCounts: Record<string, number>
  /** Conditions the catalog tolerates but somebody should see. Surfaced here rather than refused at
   *  boot: a catalog that was legal when written must not become a host that will not start. */
  catalogWarnings: string[]
  startedAtMs: number
}

/** One known peer. `restEndpoint` is why this type earns its keep — the federated `grpc` source needs
 *  it to translate an entity id to a name, and can run the same round trip in reverse so a peer's
 *  entity may be named rather than GUID'd. */
export interface PeerRecord {
  name: string
  /** Empty until the peer has been probed successfully: 'configured' vs 'seen'. */
  instanceId: string
  restEndpoint: string
  grpcEndpoint: string
  /** 0 = never reached. */
  lastSeenAtMs: number
  /** Kept next to `lastSeenAtMs` so "never reachable" and "was reachable" are distinguishable. */
  lastError?: string | null
  info?: InstanceInfo | null
}

/** Plan 016 wave 6 — one row of `GET /api/meta/endpoints`: a name this instance's `Endpoints:<name>`
 *  configuration declares, and the literal host/URL a `@name` reference in a connector config resolves
 *  to at connect time. `value` is not a secret in itself (same class of thing a source's own config
 *  already shows — see MetaEndpoints.cs's note on this route), but the route sits behind Viewer +
 *  catalog.read, unlike `/api/meta/instance`. */
export interface NamedEndpoint {
  name: string
  value: string
}

/** Plan 021 wave 2 (021-F) — mirrors shared/StreamsForge.Contracts/EnvironmentModels.cs's
 *  EnvironmentRecord exactly. One row of `GET /api/environments`; the default environment is always
 *  present, always first, and always spelled `"default"` here (never the server's internal empty-string
 *  key). `entityCount` is -1 when the server did not count it. */
export interface EnvironmentRecord {
  name: string
  description: string
  createdAtMs: number
  createdBy: string
  entityCount: number
}

/** Body of `POST /api/environments` — matches shared/StreamsForge.Api/Endpoints/EnvironmentsEndpoints.cs's
 *  CreateEnvironmentRequest. */
export interface CreateEnvironmentRequest {
  name: string
  description?: string
}
