import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import type { AccessDecision, LoginResponse, PermissionGrant, Role, UserInfo } from './types'
import { api, AUTH_STORAGE_KEY, setUnauthorizedHandler } from './client'
import { decide as decideWith } from './permissions'
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
  /** The tri-state behind `can`, for the surfaces that want to say "Request approval…" rather than
   *  hide a control. Same arguments, same fallback; `can` is `decide(...) === 'Allowed'`. */
  decide: (action: string, scope?: string) => AccessDecision
  /** The role names `/api/auth/me` resolved (store roles + group roles), empty against an old server.
   *  Not the same thing as `user.role`, which is the single legacy role on the credential record. */
  roles: string[]
  /** Group memberships, empty against an old server. */
  groups: string[]
  /** The access-document version the snapshot was computed from — a client that caches permissions can
   *  tell that they moved, and a bug report that quotes it says which document decided. */
  policyVersion: number | null
}

// Plan 015 wave 6-A. The permission snapshot from `/api/auth/me` decides everything below; this table
// is only the FALLBACK, for a server that sends no `permissions[]` at all — a pre-015 build, or one
// rolled back to `Auth:Mode=legacy`. Against such a server the SPA must reproduce today's answer
// exactly, so the floors are transcribed from the built-in role seeds in
// shared/StreamForge.AppCore/Access/BuiltInRoleCatalog.cs, which are themselves the legacy policies
// written out as grants (and pinned route-by-route by LegacyEquivalenceMatrixTests). Read it as: "the
// lowest legacy role whose policy admits the route this action stands for."
//
// Two entries are worth a sentence:
//   - `approval.request` is a Viewer action on purpose. Asking for a second pair of eyes is not a
//     privilege; deciding is. A build where the only people who could file a request were the people
//     who did not need one would have shipped dead.
//   - `config.replace` is Editor, not Admin as the wave-6 seam guessed: `POST /api/config/import` is
//     `RequireAuthorization("Editor")` today. Flooring it at Admin would hide, from an Editor talking
//     to an old server, a button whose request would have succeeded.
const ACTION_ROLE_FLOOR: Readonly<Record<string, Role>> = {
  'source.read': 'Viewer',
  'pipeline.read': 'Viewer',
  'table.read': 'Viewer',
  'config.export': 'Viewer',
  'catalog.read': 'Viewer',
  'approval.request': 'Viewer',

  'source.write': 'Editor',
  'source.delete': 'Editor',
  'source.ingest': 'Editor',
  'source.run': 'Editor',
  'pipeline.write': 'Editor',
  'pipeline.delete': 'Editor',
  'pipeline.control': 'Editor',
  'table.write': 'Editor',
  'table.delete': 'Editor',
  'table.control': 'Editor',
  'config.replace': 'Editor',
  'catalog.write': 'Editor',
  'chat.use': 'Editor',

  'user.read': 'Admin',
  'user.write': 'Admin',
  'access.read': 'Admin',
  'access.write': 'Admin',
  'audit.read': 'Admin',
  'approval.decide': 'Admin',
  'approval.bypass': 'Admin',
}

// Actions the table above does not name — a future one, or a wildcard somebody typed. The privileged
// families floor at Admin, because everything under them today does and an unlisted sibling is far
// likelier to be another privileged operation than a read. Everything else floors at Editor: an
// unnamed action is most likely a new catalog operation, and Editor is what the legacy server gated
// those with. `approval.request` is already Viewer above, so the `approval.` prefix here only ever
// catches the deciding half.
const ACTION_PREFIX_FLOOR: ReadonlyArray<readonly [string, Role]> = [
  ['access.', 'Admin'],
  ['audit.', 'Admin'],
  ['user.', 'Admin'],
  ['approval.', 'Admin'],
] as const

function minRoleFor(action: string): Role {
  return (
    ACTION_ROLE_FLOOR[action] ??
    ACTION_PREFIX_FLOOR.find(([prefix]) => action.startsWith(prefix))?.[1] ??
    'Editor'
  )
}

// How often the permission snapshot is re-fetched while a tab is open.
//
// The server caches the same snapshot for `Auth:PolicyCacheSeconds` (default 10), so it cannot answer
// fresher than that anyway, and polling per render — the obvious wrong answer — puts a request on the
// critical path of every re-render of every screen. 60s while the tab is VISIBLE is the cheap middle:
// one request a minute per open tab (next to a permanently open SignalR connection, nothing), a
// revoked entitlement leaves a stale button for at most ~70s, and clicking that stale button gets a
// 403 from the server, which is the enforcement point regardless. A hidden tab polls not at all and
// refetches the moment it becomes visible again, so the laptop-lid case resolves on the first glance
// rather than on the first click.
const PERMISSION_POLL_MS = 60_000

interface PermissionSnapshot {
  /** null means the server sent no `permissions[]` — a pre-015 build or `Auth:Mode=legacy`. That is
   *  NOT the same as `[]`, which is an entitlements server saying "you hold nothing". */
  permissions: PermissionGrant[] | null
  roles: string[]
  groups: string[]
  disabled: boolean
  policyVersion: number | null
}

const NO_SNAPSHOT: PermissionSnapshot = {
  permissions: null,
  roles: [],
  groups: [],
  disabled: false,
  policyVersion: null,
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [auth, setAuth] = useState<StoredAuth | null>(() => readStoredAuth())
  const [snapshot, setSnapshot] = useState<PermissionSnapshot>(NO_SNAPSHOT)

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
    setSnapshot(NO_SNAPSHOT)
    void disconnectHub()
  }, [])

  useEffect(() => {
    setUnauthorizedHandler(() => {
      clearDocsCookie()
      localStorage.removeItem(AUTH_STORAGE_KEY)
      setAuth(null)
      setSnapshot(NO_SNAPSHOT)
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
    // The effect below refetches on the token change; nothing here has to await it. The screen the
    // user lands on renders once with the fallback answer and once with the real one, and the fallback
    // is today's role semantics — never a locked-out UI.
  }, [])

  // Fetch the snapshot: on mount when a stored token exists, after a login, and on the poll below.
  const token = auth?.token ?? null

  useEffect(() => {
    if (!token) {
      setSnapshot(NO_SNAPSHOT)
      return
    }

    let cancelled = false

    const refresh = () => {
      void api
        .get<UserInfo>('/api/auth/me')
        .then((me) => {
          if (cancelled) return
          setSnapshot({
            permissions: me.permissions ?? null,
            roles: me.roles ?? [],
            groups: me.groups ?? [],
            disabled: me.disabled ?? false,
            policyVersion: me.policyVersion ?? null,
          })
        })
        .catch(() => {
          // Deliberately silent, and deliberately NOT clearing the snapshot. A failed /me is a network
          // hiccup or an old server; either way the last known answer (or the role fallback, if there
          // never was one) is a better UI than every control vanishing.
          //
          // The two answers that are NOT hiccups are handled globally before this catch ever runs: a
          // 401, and — since the orchestrator's wave-6 fix — a 403 from this route specifically, which
          // can only mean the account was disabled or its every role deleted. Both fire the unauthorized
          // handler, which signs the session out; the throw arriving here is then just the tail of it.
        })
    }

    refresh()

    // Visible tabs only. An interval that keeps firing in a background tab is how a laptop wakes up to
    // a burst of stale requests, and a hidden tab has no buttons anybody is looking at.
    const tick = () => {
      if (document.visibilityState === 'visible') refresh()
    }
    const timer = window.setInterval(tick, PERMISSION_POLL_MS)
    const onVisible = () => {
      if (document.visibilityState === 'visible') refresh()
    }
    document.addEventListener('visibilitychange', onVisible)

    return () => {
      cancelled = true
      window.clearInterval(timer)
      document.removeEventListener('visibilitychange', onVisible)
    }
  }, [token])

  const hasRole = useCallback(
    (min: Role) => {
      if (!auth) return false
      return ROLE_ORDER[auth.role] >= ROLE_ORDER[min]
    },
    [auth],
  )

  const decide = useCallback(
    (action: string, scope?: string): AccessDecision => {
      if (!auth) return 'Denied'
      // No `permissions[]` at all = a pre-015 server or `Auth:Mode=legacy`: fall back to the ordinal
      // answer, which is what that server actually enforces. An EMPTY array is an entitlements server
      // saying "you hold nothing", and must deny — which is why the two are kept apart all the way
      // from the JSON (UserInfo omits the field rather than sending null) to here.
      if (snapshot.permissions === null) {
        return hasRole(minRoleFor(action)) ? 'Allowed' : 'Denied'
      }
      return decideWith(
        { grants: snapshot.permissions, disabled: snapshot.disabled },
        action,
        scope ?? '*',
      )
    },
    [auth, hasRole, snapshot],
  )

  const can = useCallback(
    (action: string, scope?: string) => decide(action, scope) === 'Allowed',
    [decide],
  )

  const value = useMemo<AuthContextValue>(
    () => ({
      user: auth ? { username: auth.username, displayName: auth.displayName, role: auth.role } : null,
      role: auth?.role ?? null,
      token: auth?.token ?? null,
      login,
      logout,
      hasRole,
      permissions: snapshot.permissions,
      can,
      decide,
      roles: snapshot.roles,
      groups: snapshot.groups,
      policyVersion: snapshot.policyVersion,
    }),
    [auth, login, logout, hasRole, can, decide, snapshot],
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within an AuthProvider')
  return ctx
}

// Exported for web/test/permissions.test.ts, which pins the fallback against the same three legacy
// roles LegacyEquivalenceMatrixTests pins the server against. Not part of the context value: a screen
// that wants to know what a caller may do asks `can`/`decide`, never the floor table.
export const __testing = { minRoleFor, ROLE_ORDER }
