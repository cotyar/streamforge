import { api } from './client'
import type { Metadata, PipelineDefinition, ResultEnvelope, Tags, ValidateResponse } from './types'

export interface CreatePipelineRequest {
  name: string
  description: string
  sql: string
  tags?: Tags
  metadata?: Metadata
}

export type UpdatePipelineRequest = CreatePipelineRequest

export interface ValidateRequest {
  sql: string
}

export const pipelinesApi = {
  list: () => api.get<PipelineDefinition[]>('/api/pipelines'),
  get: (id: string) => api.get<PipelineDefinition>(`/api/pipelines/${encodeURIComponent(id)}`),
  create: (body: CreatePipelineRequest) => api.post<PipelineDefinition>('/api/pipelines', body),
  update: (id: string, body: UpdatePipelineRequest) =>
    api.put<PipelineDefinition>(`/api/pipelines/${encodeURIComponent(id)}`, body),
  remove: (id: string) => api.del<void>(`/api/pipelines/${encodeURIComponent(id)}`),
  start: (id: string) => api.post<PipelineDefinition>(`/api/pipelines/${encodeURIComponent(id)}/start`),
  stop: (id: string) => api.post<PipelineDefinition>(`/api/pipelines/${encodeURIComponent(id)}/stop`),
  validate: (body: ValidateRequest) => api.post<ValidateResponse>('/api/pipelines/validate', body),
  results: (id: string, limit = 50) =>
    api.get<ResultEnvelope[]>(`/api/pipelines/${encodeURIComponent(id)}/results?limit=${limit}`),
}
