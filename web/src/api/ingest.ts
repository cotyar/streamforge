// Client for the client-push ingress surface (plan 008 W4): POST /api/sources/{name}/events and
// GET /api/sources/{name}/ingest. Both fall outside the shared JSON-only `api` helper (client.ts)
// for the push endpoint specifically — a non-2xx response there carries a structured
// IngestErrorResponse body (retryAfterMs, rowErrors) that api.post's generic ApiError would flatten
// to a single message string, and the console's test-push affordance needs those fields to render
// an honest outcome. Mirrors config.ts's raw-fetch approach. The status GET has no such need (its
// only non-200 outcomes are 404/204, both "hide the card"), so it reuses `api.get` directly.
import { api, ApiError, getStoredToken } from './client'
import type { IngestAcceptedResponse, IngestErrorResponse, IngestEventsRequest, IngestStatusResponse } from './types'

function authHeaders(extra?: Record<string, string>): Record<string, string> {
  const headers: Record<string, string> = { Accept: 'application/json', ...extra }
  const token = getStoredToken()
  if (token) headers.Authorization = `Bearer ${token}`
  return headers
}

export type IngestPushResult =
  | { accepted: true; body: IngestAcceptedResponse }
  | { accepted: false; status: number; body: IngestErrorResponse }

/** POST /api/sources/{name}/events — 202 IngestAcceptedResponse on success; 400/409/413/429
 * IngestErrorResponse on failure. Both are returned (not thrown) so the caller can render the
 * outcome honestly; only network failures or an unparsable body become an ApiError. */
async function pushEvents(name: string, body: IngestEventsRequest): Promise<IngestPushResult> {
  const res = await fetch(`/api/sources/${encodeURIComponent(name)}/events`, {
    method: 'POST',
    headers: authHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(body),
  })

  const text = await res.text()
  let data: unknown
  try {
    data = text ? JSON.parse(text) : undefined
  } catch {
    data = undefined
  }

  if (res.status === 202 && data && typeof data === 'object') {
    return { accepted: true, body: data as IngestAcceptedResponse }
  }
  if (!res.ok && data && typeof data === 'object' && typeof (data as Record<string, unknown>).error === 'string') {
    return { accepted: false, status: res.status, body: data as IngestErrorResponse }
  }

  const message = (data && typeof data === 'object' && typeof (data as Record<string, unknown>).error === 'string'
    ? (data as Record<string, unknown>).error
    : undefined) as string | undefined
  throw new ApiError(res.status, message ?? text ?? res.statusText ?? `Request failed with status ${res.status}`)
}

export const ingestApi = {
  pushEvents,
  /** GET /api/sources/{name}/ingest — 404 unknown source (ApiError), 204 not ingest-kind (undefined
   * — client.ts maps 204 to undefined), 200 IngestStatusResponse. */
  status: (name: string) => api.get<IngestStatusResponse | undefined>(`/api/sources/${encodeURIComponent(name)}/ingest`),
}
