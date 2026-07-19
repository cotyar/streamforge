import type { ReactNode } from 'react'
import type { Role } from '../api/types'
import { useAuth } from '../api/auth'

export function RoleGate({ min, children }: { min: Role; children: ReactNode }) {
  const { hasRole } = useAuth()
  if (!hasRole(min)) return null
  return <>{children}</>
}
