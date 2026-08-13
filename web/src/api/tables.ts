import { api } from './client'
import type {
  ExecutionPlanResponse,
  Metadata,
  ResultRow,
  SinkSpec,
  Tags,
  TableDefinition,
  TableHistoryMode,
  TableHistoryResponse,
  TableHistoryStats,
  TableMetrics,
  TablePersistenceMode,
  TableRowsResponse,
  TableSearchMode,
  TableSearchResponse,
  TableShardScanResponse,
  TableShardsResponse,
  TableShardView,
  TableValidateResponse,
} from './types'

export interface CreateTableRequest {
  name: string
  description: string
  sql: string
  searchEnabled?: boolean
  searchMode?: TableSearchMode
  historyEnabled?: boolean
  historyMode?: TableHistoryMode
  historyLimit?: number
  historyByField?: string | null
  historyWindowMs?: number
  tags?: Tags
  metadata?: Metadata
  /** Plan 003 M5: 1 (default) = classic single-grain execution; 2..16 = partitioned dataflow.
   * Changing it on an existing table restarts it. */
  parallelism?: number
  /** Plan 008: durability policy for the materialized snapshot. Absent = 'batched'. Changing it on
   * an existing table restarts it. */
  persistence?: TablePersistenceMode
  /** Flush interval in ms for batched/fireAndForget; 0 or absent = 2000. */
  flushMs?: number
  /** Plan 009 A2: compaction threshold for persistence 'Journaled'. 0/absent = server default. */
  journalMaxEntries?: number
  /** Plan 009 B2: where this table's deltas are republished. Absent on create = none; absent on
   * update = leave unchanged, same convention as CreatePipelineRequest.sinks. */
  sinks?: SinkSpec[]
  /** Plan 011 C2: opt-in row retention. 0/absent = unbounded (default). See TableDefinition's own
   * doc comment — a non-zero bound makes the table a bounded view, and the server rejects it (409)
   * for SQL shapes whose per-row state it could not reclaim. */
  retentionMaxRows?: number
  /** Plan 011 C2: event-time TTL in ms; 0/absent = unbounded. */
  retentionTtlMs?: number
  /** Plan 011 D1/D2: opt-in key sharding — the output columns rows are sharded by. Absent on update
   * means "leave as-is" (so an older client cannot un-shard a table by omitting it); [] clears it, which
   * DELETES the shards. Rejected (409) together with searchEnabled and with persistence 'MemoryOnly',
   * and a sharded table cannot be renamed — see TableDefinition.shardBy. */
  shardBy?: string[]
}

export type UpdateTableRequest = CreateTableRequest

export interface TableValidateRequest {
  sql: string
}

export const tablesApi = {
  list: () => api.get<TableDefinition[]>('/api/tables'),
  get: (id: string) => api.get<TableDefinition>(`/api/tables/${encodeURIComponent(id)}`),
  create: (body: CreateTableRequest) => api.post<TableDefinition>('/api/tables', body),
  update: (id: string, body: UpdateTableRequest) =>
    api.put<TableDefinition>(`/api/tables/${encodeURIComponent(id)}`, body),
  remove: (id: string) => api.del<void>(`/api/tables/${encodeURIComponent(id)}`),
  start: (id: string) => api.post<TableDefinition>(`/api/tables/${encodeURIComponent(id)}/start`),
  stop: (id: string) => api.post<TableDefinition>(`/api/tables/${encodeURIComponent(id)}/stop`),
  validate: (body: TableValidateRequest) => api.post<TableValidateResponse>('/api/tables/validate', body),
  rows: (id: string, limit = 500, offset = 0) =>
    api.get<TableRowsResponse>(`/api/tables/${encodeURIComponent(id)}/rows?limit=${limit}&offset=${offset}`),
  metrics: (id: string) => api.get<TableMetrics>(`/api/tables/${encodeURIComponent(id)}/metrics`),
  search: (id: string, q: string, limit = 100) =>
    api.get<TableSearchResponse>(`/api/tables/${encodeURIComponent(id)}/search?q=${encodeURIComponent(q)}&limit=${limit}`),
  // Feature B: row history. historyLookup hands the server the exact row object the caller already
  // has — the server derives the row-identity key from it (see TablesEndpoints/TableGroupKeyExtractor).
  historyLookup: (id: string, row: ResultRow, limit = 0) =>
    api.post<TableHistoryResponse>(`/api/tables/${encodeURIComponent(id)}/history/lookup?limit=${limit}`, { row }),
  historyStats: (id: string) => api.get<TableHistoryStats>(`/api/tables/${encodeURIComponent(id)}/history/stats`),
  // Plan 011 D1/D2 — sharded tables. The split between these three IS the design, so read it before
  // adding a fourth: `shards` wakes NOTHING (router + directory only) and is the one safe to poll;
  // `shardLookup` wakes exactly one shard, which is the point of the tier; `shardsScan` wakes every
  // shard in its page, which is why it is a separate call nothing reaches by accident.
  shards: (id: string, limit = 0, offset = 0) =>
    api.get<TableShardsResponse>(`/api/tables/${encodeURIComponent(id)}/shards?limit=${limit}&offset=${offset}`),
  shardLookup: (id: string, row: ResultRow, historyLimit = 0) =>
    api.post<TableShardView>(`/api/tables/${encodeURIComponent(id)}/shard/lookup?historyLimit=${historyLimit}`, { row }),
  /** fenced=true costs latency (the shard tier's ingest pauses for the scan) and buys a real cut. */
  shardsScan: (id: string, limit = 100, offset = 0, fenced = false) =>
    api.get<TableShardScanResponse>(
      `/api/tables/${encodeURIComponent(id)}/shards/scan?limit=${limit}&offset=${offset}&fenced=${fenced}`,
    ),
  // Plan 008 W5: lineage + execution-plan view (see ExecutionPlanResponse's doc comment in ./types).
  plan: (id: string) => api.get<ExecutionPlanResponse>(`/api/tables/${encodeURIComponent(id)}/plan`),
}
