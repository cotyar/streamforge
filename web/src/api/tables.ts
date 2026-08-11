import { api } from './client'
import type {
  ExecutionPlanResponse,
  Metadata,
  ResultRow,
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
  // Plan 008 W5: lineage + execution-plan view (see ExecutionPlanResponse's doc comment in ./types).
  plan: (id: string) => api.get<ExecutionPlanResponse>(`/api/tables/${encodeURIComponent(id)}/plan`),
}
