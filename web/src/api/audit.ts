import { api } from './client'
import type { AuditPageResponse } from './types'

/** Filters `GET /api/audit/{day}` accepts, and deliberately no more: `actor` is an EXACT match and
 *  `action` a PREFIX match. That is the whole grammar the server implements — a day's rows are a
 *  stream somebody can point platform SQL at one layer up, which is where a real query language
 *  belongs. Do not grow a client-side one on top of this. */
export interface AuditQuery {
  actor?: string
  /** Prefix, not substring: `source` matches `source.create`, `create` matches nothing. */
  action?: string
  limit?: number
  offset?: number
  /** Opt-in for `beforeJson`/`afterJson`. The server ALSO requires the caller to hold `access.read`,
   *  so `changesIncluded: false` can come back on a request that asked for them. */
  includeChanges?: boolean
}

function qs(query: AuditQuery): string {
  const p = new URLSearchParams()
  if (query.actor) p.set('actor', query.actor)
  if (query.action) p.set('action', query.action)
  if (query.limit !== undefined) p.set('limit', String(query.limit))
  if (query.offset !== undefined) p.set('offset', String(query.offset))
  if (query.includeChanges) p.set('includeChanges', 'true')
  const s = p.toString()
  return s ? `?${s}` : ''
}

export const auditApi = {
  /** Which days hold entries, newest-first as the server orders them. Reads an index and wakes no
   *  day shard, which is what makes it the cheap first call. */
  days: () => api.get<string[]>('/api/audit/days'),

  /** One day's page. `day` is `yyyyMMdd` (UTC); anything else is a 400 with a sentence, because the
   *  day is a storage key rather than a filter. */
  page: (day: string, query: AuditQuery = {}) =>
    api.get<AuditPageResponse>(`/api/audit/${encodeURIComponent(day)}${qs(query)}`),
}
