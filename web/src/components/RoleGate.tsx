import type { ReactNode } from 'react'
import type { Role } from '../api/types'
import { useAuth } from '../api/auth'

// Plan 015 wave 6-A — the no-flag-day shim.
//
// `min` keeps working with byte-identical semantics, so not one of this component's 57 call sites had
// to change at cut-over: an entitlement system that required a 57-file rename in the same commit is an
// entitlement system that gets reviewed by nobody. `action` (and optionally `scope`) is the new way to
// ask, and a site migrates when someone has a reason to migrate it.
//
// Both props together mean BOTH must pass — the conjunction, not the disjunction. A caller that has
// written `min="Admin"` and then adds `action` is narrowing on purpose; reading it as "or" would let
// the new prop silently WIDEN an existing gate, and a gate that widens when you add a condition to it
// is the kind of surprise this shim exists to avoid.
export function RoleGate({
  min,
  action,
  scope,
  children,
}: {
  min?: Role
  action?: string
  scope?: string
  children: ReactNode
}) {
  const { hasRole, can } = useAuth()
  if (min !== undefined && !hasRole(min)) return null
  if (action !== undefined && !can(action, scope)) return null
  // Neither prop given renders the children: the only way to reach that is a call site that asked for
  // no condition at all, and refusing there would hide content for a reason nobody wrote down.
  return <>{children}</>
}
