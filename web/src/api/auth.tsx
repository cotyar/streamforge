import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import type { LoginResponse, Role } from './types'
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
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [auth, setAuth] = useState<StoredAuth | null>(() => readStoredAuth())

  const logout = useCallback(() => {
    localStorage.removeItem(AUTH_STORAGE_KEY)
    setAuth(null)
    void disconnectHub()
  }, [])

  useEffect(() => {
    setUnauthorizedHandler(() => {
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

  const value = useMemo<AuthContextValue>(
    () => ({
      user: auth ? { username: auth.username, displayName: auth.displayName, role: auth.role } : null,
      role: auth?.role ?? null,
      token: auth?.token ?? null,
      login,
      logout,
      hasRole,
    }),
    [auth, login, logout, hasRole],
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within an AuthProvider')
  return ctx
}
