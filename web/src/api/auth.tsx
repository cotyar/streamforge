import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import type { LoginResponse, PermissionGrant, Role } from './types'
import { api, AUTH_STORAGE_KEY, setUnauthorizedHandler } from './client'
import { disconnectHub } from '../realtime/hub'

export interface AuthUser {
  username: string
  displayName: string
  role: Role
}

interface StoredAuth extends AuthUser {
  token: string
}

const ROLE_ORDER: Record<Role, number> = { Viewer: 0, Editor: 1, Admin: 2 }

function readStoredAuth(): StoredAuth | null {
  try {
    const raw = localStorage.getItem(AUTH_STORAGE_KEY)
    if (!raw) return null
    return JSON.parse(raw) as StoredAuth
  } catch {
    return null
  }
}

export interface AuthContextValue {
  user: AuthUser | null
  role: Role | null
  token: string | null
  login: (username: string, password: string) => Promise<void>
  logout: () => void
  hasRole: (min: Role) => boolean
  /** The caller's own entitlements as `/api/auth/me` reported them, or null against a pre-015 server
   *  (see the `permissions?` note in types.ts) — which is exactly what makes `can` fall back. */
  permissions: PermissionGrant[] | null
  /** May this principal do `action` to `scope`? Scope defaults to `*`. */
  can: (action: string, scope?: string) => boolean
}

// SEAM — plan 015 wave 6. The SIGNATURE of `can` above is final and is what the access, approvals and
// audit surfaces are written against; the BODY below is not. Wave 6-A replaces it with the client-side
// twin of PermissionEvaluator fed by `/api/auth/me`'s `permissions[]`, and keeps this ordinal answer as
// the fallback for a server that sends no `permissions[]` at all. Until then every caller gets today's
// role semantics — which is the correct answer for every surface this wave adds, and a safe one for the
// rest.
const ACTION_ROLE_FLOOR: ReadonlyArray<readonly [string, Role]> = [
  ['approval.request', 'Viewer'],
  ['approval.', 'Admin'],
  ['access.', 'Admin'],
  ['audit.', 'Admin'],
  ['user.', 'Admin'],
  ['config.replace', 'Admin'],
] as const

function minRoleFor(action: string): Role {
  return ACTION_ROLE_FLOOR.find(([prefix]) => action.startsWith(prefix))?.[1] ?? 'Editor'
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [auth, setAuth] = useState<StoredAuth | null>(() => readStoredAuth())

  // Plan 011 wave G: signing in also sets an httpOnly `sf_docs` cookie, which is the ONLY way the
  // per-entity Scalar pages can authenticate (a page navigation and Scalar's own document fetch can
  // neither of them send an Authorization header — see DocsAuthCookie). Clearing localStorage cannot
  // clear an httpOnly cookie, so signing out has to ask the server to: without this call the cookie
  // would outlive an in-app sign-out by its full 12h lifetime, on a shared browser.
  const clearDocsCookie = () => {
    void fetch('/api/auth/logout', { method: 'POST' }).catch(() => {
      /* best-effort: the cookie expiring on its own is the fallback, and a failed sign-out must never
         block clearing the client-side session below */
    })
  }

  const logout = useCallback(() => {
    clearDocsCookie()
    localStorage.removeItem(AUTH_STORAGE_KEY)
    setAuth(null)
    void disconnectHub()
  }, [])

  useEffect(() => {
    setUnauthorizedHandler(() => {
      clearDocsCookie()
      localStorage.removeItem(AUTH_STORAGE_KEY)
      setAuth(null)
      void disconnectHub()
      if (window.location.pathname !== '/login') {
        window.location.href = '/login'
      }
    })
  }, [])

  const login = useCallback(async (username: string, password: string) => {
    const res = await api.post<LoginResponse>('/api/auth/login', { username, password })
    const stored: StoredAuth = {
      username: res.username,
      displayName: res.displayName,
      role: res.role,
      token: res.token,
    }
    localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(stored))
    setAuth(stored)
  }, [])

  const hasRole = useCallback(
    (min: Role) => {
      if (!auth) return false
      return ROLE_ORDER[auth.role] >= ROLE_ORDER[min]
    },
    [auth],
  )

  const can = useCallback(
    (action: string, _scope?: string) => hasRole(minRoleFor(action)),
    [hasRole],
  )

  const value = useMemo<AuthContextValue>(
    () => ({
      user: auth ? { username: auth.username, displayName: auth.displayName, role: auth.role } : null,
      role: auth?.role ?? null,
      token: auth?.token ?? null,
      login,
      logout,
      hasRole,
      permissions: null,
      can,
    }),
    [auth, login, logout, hasRole, can],
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within an AuthProvider')
  return ctx
}
