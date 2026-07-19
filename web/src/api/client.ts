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
