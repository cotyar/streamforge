// Tiny typed fetch wrapper against the StreamForge REST API.
// Base URL is empty — Vite dev server proxies /api to the backend host.

export const AUTH_STORAGE_KEY = 'sf.auth'

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

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json' }
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
}
