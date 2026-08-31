// Plan 021 wave 2 (021-F) — client for /api/environments (shared/StreamsForge.Api/Endpoints/EnvironmentsEndpoints.cs).
// Plain JSON in and out, so this would ordinarily just reuse `api.get/post/del` — but every call here
// goes through the `*Global` variants instead. Found live: EnvironmentSelectionMiddleware does not
// exclude /api/environments from environment selection (unlike /api/auth/* and /api/meta/instance), so
// a stale/deleted environment still named by X-StreamsForge-Environment 404s this route exactly like any
// other — including the `list()` call EnvironmentPicker.tsx's recovery path depends on to discover that
// very fact. The endpoint handlers never consult EnvironmentAmbient anyway (the directory is not part
// of any one environment's catalog), so omitting the header changes nothing about what they return and
// fixes the deadlock. See client.ts's `api.getGlobal` doc comment for the full story.
import { api } from './client'
import type { CreateEnvironmentRequest, EnvironmentRecord } from './types'

export const environmentsApi = {
  /** GET /api/environments — Viewer. `default` first, the rest name-ordered. */
  list: () => api.getGlobal<EnvironmentRecord[]>('/api/environments'),

  /** POST /api/environments — Admin. 400 on an invalid/reserved name, 409 on a duplicate (both surface
   *  as ApiError via client.ts). */
  create: (req: CreateEnvironmentRequest) => api.postGlobal<EnvironmentRecord>('/api/environments', req),

  /** DELETE /api/environments/{name}?force=true — Admin. Without `force`, 409s on a non-empty
   *  environment; with it, deletes the catalog AND the runtime state of everything in it — the one
   *  genuinely destructive operation this plan adds. `force` defaults to false so a caller has to name
   *  the intent explicitly rather than pass a bare boolean by habit. */
  remove: (name: string, force = false) =>
    api.delGlobal<void>(`/api/environments/${encodeURIComponent(name)}${force ? '?force=true' : ''}`),
}
