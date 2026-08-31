// Tiny typed fetch wrapper against the StreamsForge REST API.
// Base URL is empty — Vite dev server proxies /api to the backend host.
import { getStoredEnvironment, needsEnvironmentSelector } from '../lib/environment'

export const AUTH_STORAGE_KEY = 'sf.auth'

/** The header EnvironmentSelectionMiddleware.cs reads (shared/StreamsForge.Api/Environments/). Exported
 * so the five raw-fetch call sites that bypass `request()` below (csv.ts, config.ts, explorerTypes.ts,
 * ingest.ts) inject the identical header rather than each hardcoding the string. */
export const ENVIRONMENT_HEADER = 'X-StreamsForge-Environment'

/** `{}` for the default environment (D2: costs nothing — no header at all), else the one header that
 * selects a non-default environment on every route the middleware does not exclude. */
export function environmentHeader(): Record<string, string> {
  const env = getStoredEnvironment()
  return needsEnvironmentSelector(env) ? { [ENVIRONMENT_HEADER]: env } : {}
}

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

interface StoredAuthShape {
  token: string
}

export function getStoredToken(): string | null {
  try {
    const raw = localStorage.getItem(AUTH_STORAGE_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw) as StoredAuthShape
    return parsed.token ?? null
  } catch {
    return null
  }
}

let onUnauthorized: (() => void) | null = null

/** The one route where a 403 is a session kill rather than a refusal — see `request`. */
const IDENTITY_PATH = '/api/auth/me'

/** Registered once by the auth module so 401s clear session state and redirect. */
export function setUnauthorizedHandler(handler: () => void) {
  onUnauthorized = handler
}

/** Registered once by EnvironmentPicker.tsx. Fired on every 404 seen WHILE a non-default environment
 * is selected — see `request` below. Deliberately not fired on the message text: `{env}' does not
 * exist"` is one possible cause of a 404 in that state, but so is an ordinary missing entity inside a
 * still-valid environment, and the two are indistinguishable from the status code alone. The handler
 * is expected to re-validate against GET /api/environments before doing anything user-visible — see
 * EnvironmentPicker's validateSelection(). */
let onEnvironment404: ((env: string) => void) | null = null

export function setEnvironment404Handler(handler: ((env: string) => void) | null) {
  onEnvironment404 = handler
}

async function extractErrorMessage(res: Response): Promise<string> {
  try {
    const data: unknown = await res.clone().json()
    if (data && typeof data === 'object') {
      const obj = data as Record<string, unknown>
      if (typeof obj.message === 'string') return obj.message
      if (typeof obj.title === 'string') return obj.title
      if (typeof obj.error === 'string') return obj.error
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

/** `global: true` (see `api.getGlobal`/`postGlobal`/`delGlobal` below) skips both the environment
 * header AND the 404 detection — see their doc comment for why /api/environments itself needs this. */
async function request<T>(method: string, path: string, body?: unknown, opts?: { global?: boolean }): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json', ...(opts?.global ? {} : environmentHeader()) }
  const token = getStoredToken()
  if (token) headers.Authorization = `Bearer ${token}`
  if (body !== undefined) headers['Content-Type'] = 'application/json'

  const res = await fetch(path, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })

  if (res.status === 401) {
    onUnauthorized?.()
    throw new ApiError(401, 'Session expired — please sign in again.')
  }

  if (res.status === 404 && !opts?.global) {
    const env = getStoredEnvironment()
    if (needsEnvironmentSelector(env)) onEnvironment404?.(env)
  }

  // A 403 anywhere else means "this account may not do that", and the screen says so. On /api/auth/me
  // it cannot mean that: the route asks only who the caller is, and the sole thing that refuses it is
  // Auth:StrictViewer deciding the account is disabled or its every role has been deleted. Plan 015
  // wave 6 found the consequence of treating it like any other refusal — a disabled user keeps the last
  // permission snapshot they successfully fetched and goes on seeing a working console until they click
  // something. Nothing is over-granted (the server refuses every request), but the session has to end
  // where it actually ended.
  if (res.status === 403 && path === IDENTITY_PATH) {
    onUnauthorized?.()
    throw new ApiError(403, 'This account is no longer permitted to sign in.')
  }

  if (!res.ok) {
    throw new ApiError(res.status, await extractErrorMessage(res))
  }

  if (res.status === 204) return undefined as T
  const text = await res.text()
  if (!text) return undefined as T
  return JSON.parse(text) as T
}

export const api = {
  get: <T>(path: string) => request<T>('GET', path),
  post: <T>(path: string, body?: unknown) => request<T>('POST', path, body),
  put: <T>(path: string, body?: unknown) => request<T>('PUT', path, body),
  del: <T>(path: string) => request<T>('DELETE', path),

  // Plan 021 wave 2 (021-F) — found live: the environment DIRECTORY is not part of any one
  // environment's catalog (EnvironmentsEndpoints.cs's handlers never read EnvironmentAmbient — see
  // shared/StreamsForge.Api/Endpoints/EnvironmentsEndpoints.cs), but EnvironmentSelectionMiddleware does
  // not exclude /api/environments the way it excludes /api/auth/* and /api/meta/instance, so it 404s
  // that route too whenever the CURRENTLY SELECTED environment is invalid. That is exactly the moment
  // EnvironmentPicker's recovery path (validate() in EnvironmentPicker.tsx) calls this route to find out
  // what to fall back to — sending the header would make the recovery call fail with the very condition
  // it exists to detect, and the console would stay wedged on a dead environment forever. `global: true`
  // omits the header (and skips 404 detection, since a 404 from a route the middleware never blocks on
  // environment validity is never an unknown-environment signal in the first place).
  getGlobal: <T>(path: string) => request<T>('GET', path, undefined, { global: true }),
  postGlobal: <T>(path: string, body?: unknown) => request<T>('POST', path, body, { global: true }),
  delGlobal: <T>(path: string) => request<T>('DELETE', path, undefined, { global: true }),
}
