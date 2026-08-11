import { api } from './client'
import type { ExecutionPlanResponse, Metadata, PipelineDefinition, ResultEnvelope, SinkSpec, Tags, ValidateResponse } from './types'

export interface CreatePipelineRequest {
  name: string
  description: string
  sql: string
  tags?: Tags
  metadata?: Metadata
  /** Plan 009 B2: where this pipeline's result rows are republished. Absent on create = none;
   * absent on update = leave unchanged (mirrors tags/metadata's null-means-unchanged convention). A
   * "***" credential round-tripped from a masked GET is restored server-side from the stored value. */
  sinks?: SinkSpec[]
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
  // Plan 008 W5: always the logical view (physical: false — pipelines have no partitioned dataflow
  // graph concept) — see ExecutionPlanResponse's doc comment in ./types.
  plan: (id: string) => api.get<ExecutionPlanResponse>(`/api/pipelines/${encodeURIComponent(id)}/plan`),
}
