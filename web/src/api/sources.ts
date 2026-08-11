import { api } from './client'
import type {
  CoercionFailurePolicy,
  ConnectorConfig,
  ConnectorRuntimeStatus,
  CreatedIngestKeyResponse,
  CreateIngestKeyRequest,
  FieldDef,
  IngestConfig,
  IngestKey,
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
  /** Plan 008 W4, additive: present only when kind is 'ingest'. */
  ingest?: IngestConfig
  /** Plan 009 C2, additive: meaningful only for connector-kind sources (url/file/folder/grpc/nats).
   * Absent = 'Null'. */
  onCoercionFailure?: CoercionFailurePolicy
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

  // ---- Plan 009 A1.2: per-source ingest keys (Editor). POST returns the secret exactly once —
  // nothing can ever read it back after this call returns. ----
  generateIngestKey: (name: string, body: CreateIngestKeyRequest) =>
    api.post<CreatedIngestKeyResponse>(`/api/sources/${encodeURIComponent(name)}/ingest/keys`, body),
  listIngestKeys: (name: string) => api.get<IngestKey[]>(`/api/sources/${encodeURIComponent(name)}/ingest/keys`),
  revokeIngestKey: (name: string, id: string) =>
    api.del<void>(`/api/sources/${encodeURIComponent(name)}/ingest/keys/${encodeURIComponent(id)}`),
}
