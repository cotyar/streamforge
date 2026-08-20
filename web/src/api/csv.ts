// Plan 012: CSV downloads (GET /api/tables/{id}/rows.csv, /api/pipelines/{id}/results.csv). Outside the
// shared `api` helper (client.ts) for the same reason config export is: those endpoints return a file
// with a Content-Disposition filename, not JSON.
import { ApiError, environmentHeader, getStoredToken } from './client'

function filenameFrom(header: string | null, fallback: string): string {
  if (!header) return fallback
  const match = /filename="?([^";]+)"?/i.exec(header)
  return match ? match[1] : fallback
}

/** Fetches a text/csv endpoint with the session's bearer token and hands the result to the browser as a
 * download. A plain <a href> can't be used: these routes require the Authorization header. */
export async function downloadCsv(path: string, fallbackFilename: string): Promise<void> {
  const token = getStoredToken()
  const headers: Record<string, string> = { ...environmentHeader() }
  if (token) headers.Authorization = `Bearer ${token}`
  const res = await fetch(path, { headers })
  if (!res.ok) {
    throw new ApiError(res.status, res.statusText || `Request failed with status ${res.status}`)
  }

  const blob = await res.blob()
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filenameFrom(res.headers.get('Content-Disposition'), fallbackFilename)
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}
