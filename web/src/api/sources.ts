import { api } from './client'
import type {
  ConnectorConfig,
  ConnectorRuntimeStatus,
  FieldDef,
  MappingValidateRequest,
  MappingValidateResult,
  Metadata,
  RemoteSchemaRequest,
  RemoteSchemaResult,
  SchemaDeriveRequest,
  SchemaDeriveResult,
  SourceDefinition,
  SourceKind,
  Tags,
} from './types'

export interface CreateSourceRequest {
  name: string
  description: string
  fields: FieldDef[]
  generatorProfile: SourceDefinition['generatorProfile']
  eventsPerSecond: number
  enabled: boolean
  tags?: Tags
  metadata?: Metadata
  /** Plan 006, additive: absent/'generator' preserves the pre-existing synthetic-source behavior. */
  kind?: SourceKind
  connector?: ConnectorConfig
}

// The Host's PUT /api/sources/{name} replaces the whole SourceDefinition (name is fixed by the
// route, the rest is a full overwrite) — there is no partial-update support, so callers must
// always send the complete, merged object.
export type UpdateSourceRequest = SourceDefinition

export const sourcesApi = {
  list: () => api.get<SourceDefinition[]>('/api/sources'),
  create: (body: CreateSourceRequest) => api.post<SourceDefinition>('/api/sources', body),
  update: (name: string, body: UpdateSourceRequest) =>
    api.put<SourceDefinition>(`/api/sources/${encodeURIComponent(name)}`, body),
  remove: (name: string) => api.del<void>(`/api/sources/${encodeURIComponent(name)}`),

  // ---- Plan 006: connector status + schema helper endpoints (additive). ----

  /** GET /api/sources/{name}/status — 200 status | 204 (generator-kind, or no cycle run yet) |
   * 404 (source gone). Both the 204 and 404 cases resolve here as `undefined` (client.ts maps
   * 204 to undefined; a 404 throws an ApiError for the caller to catch) — callers that want a
   * uniform "hide the status line" behavior should catch and treat both as absent. */
  status: (name: string) => api.get<ConnectorRuntimeStatus | undefined>(`/api/sources/${encodeURIComponent(name)}/status`),
  validateMapping: (body: MappingValidateRequest) =>
    api.post<MappingValidateResult>('/api/sources/schema/mapping-validate', body),
  deriveOpenApi: (body: SchemaDeriveRequest) => api.post<SchemaDeriveResult>('/api/sources/schema/derive-openapi', body),
  fetchRemoteSchema: (body: RemoteSchemaRequest) => api.post<RemoteSchemaResult>('/api/sources/schema/from-remote', body),
}
