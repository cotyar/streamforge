import { api } from './client'
import type {
  TableDefinition,
  TableMetrics,
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
}

export interface UpdateTableRequest {
  name: string
  description: string
  sql: string
  searchEnabled?: boolean
  searchMode?: TableSearchMode
}

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
}
