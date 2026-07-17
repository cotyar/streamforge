import { api } from './client'
import type { FieldDef, Metadata, SourceDefinition, Tags } from './types'

export interface CreateSourceRequest {
  name: string
  description: string
  fields: FieldDef[]
  generatorProfile: SourceDefinition['generatorProfile']
  eventsPerSecond: number
  enabled: boolean
  tags?: Tags
  metadata?: Metadata
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
}
