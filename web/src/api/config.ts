// Client for the config export/import surface (plan 006 W5B — decisions D-I/D-J).
// GET /api/config/export and POST /api/config/import both fall outside the shared JSON-only `api`
// helper (client.ts): export returns a file (blob + Content-Disposition filename), import accepts
// application/json, text/yaml, or multipart/form-data depending on the source the user picked — so
// this file mirrors explorerTypes.ts's fetchProtoText raw-fetch approach rather than reusing `api`.
import { ApiError, getStoredToken } from './client'
import type { ConfigImportReport } from './types'

export type ConfigFormat = 'json' | 'yaml'
export type ImportMode = 'validate' | 'merge' | 'replace'

export interface ConfigExportResult {
  blob: Blob
  filename: string
}

function authHeaders(extra?: Record<string, string>): Record<string, string> {
  const headers: Record<string, string> = { ...extra }
  const token = getStoredToken()
  if (token) headers.Authorization = `Bearer ${token}`
  return headers
}

/** Server sets `Content-Disposition: attachment; filename="streamforge-config.json"; filename*=...`
 * (ASP.NET's Results.File(fileDownloadName:) format) — pull the plain filename= param, falling back
 * to the format-derived default if the header is ever missing (e.g. a proxy strips it). */
function filenameFromContentDisposition(header: string | null, fallback: string): string {
  if (!header) return fallback
  const match = /filename="?([^";]+)"?/i.exec(header)
  return match ? match[1] : fallback
}

async function errorMessageFromResponse(res: Response): Promise<string> {
  try {
    const data: unknown = await res.clone().json()
    if (data && typeof data === 'object' && typeof (data as Record<string, unknown>).error === 'string') {
      return (data as Record<string, unknown>).error as string
    }
  } catch {
    // not JSON — fall through
  }
  try {
    const text = await res.text()
    if (text) return text
  } catch {
    // ignore
  }
  return res.statusText || `Request failed with status ${res.status}`
}

/** GET /api/config/export?format=json|yaml[&includeSecrets=true] (Viewer; includeSecrets needs Admin). */
export async function exportConfig(format: ConfigFormat, includeSecrets: boolean): Promise<ConfigExportResult> {
  const params = new URLSearchParams({ format })
  if (includeSecrets) params.set('includeSecrets', 'true')

  const res = await fetch(`/api/config/export?${params.toString()}`, { headers: authHeaders() })
  if (!res.ok) {
    throw new ApiError(res.status, await errorMessageFromResponse(res))
  }

  const blob = await res.blob()
  const filename = filenameFromContentDisposition(res.headers.get('Content-Disposition'), `streamforge-config.${format}`)
  return { blob, filename }
}

/** A 400 from /import can be either a document-level ConfigImportReport (Kind="document" entries —
 * render like any other report) or a plain {error} (e.g. an unknown mode, or the replace/Admin
 * gate) — distinguished by the presence of an `entries` array. Anything else (network failure,
 * unparsable body) becomes an ApiError for the caller to toast. */
async function parseImportResponse(res: Response): Promise<ConfigImportReport> {
  const text = await res.text()
  let data: unknown
  try {
    data = text ? JSON.parse(text) : undefined
  } catch {
    data = undefined
  }

  if (data && typeof data === 'object' && Array.isArray((data as Record<string, unknown>).entries)) {
    return data as ConfigImportReport
  }

  if (res.ok) {
    throw new ApiError(res.status, 'Import succeeded but returned an unrecognized response body.')
  }

  let message = res.statusText
  if (data && typeof data === 'object' && typeof (data as Record<string, unknown>).error === 'string') {
    message = (data as Record<string, unknown>).error as string
  } else if (text) {
    message = text
  }
  throw new ApiError(res.status, message || `Request failed with status ${res.status}`)
}

/** POST /api/config/import?mode=... with a multipart file set — FIRST file is the root document;
 * includes among the set resolve by file name (D-I). File order/names are preserved via FormData's
 * natural append order. */
export async function importConfigFiles(mode: ImportMode, files: File[]): Promise<ConfigImportReport> {
  const form = new FormData()
  for (const file of files) form.append('files', file, file.name)

  const res = await fetch(`/api/config/import?mode=${encodeURIComponent(mode)}`, {
    method: 'POST',
    headers: authHeaders(),
    body: form,
  })
  return parseImportResponse(res)
}

/** POST /api/config/import?mode=... with a pasted document — sent as application/json when the text
 * parses as JSON (single doc object or an ordered array of docs), else as a raw text/yaml body. */
export async function importConfigText(mode: ImportMode, text: string): Promise<ConfigImportReport> {
  let isJson = true
  try {
    JSON.parse(text)
  } catch {
    isJson = false
  }

  const res = await fetch(`/api/config/import?mode=${encodeURIComponent(mode)}`, {
    method: 'POST',
    headers: authHeaders({ 'Content-Type': isJson ? 'application/json' : 'text/yaml' }),
    body: text,
  })
  return parseImportResponse(res)
}
