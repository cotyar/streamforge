import { NavLink, Navigate, Outlet } from 'react-router-dom'
import { useAuth } from '../api/auth'
import { DashboardIcon, LogoutIcon, PipelineIcon, SourcesIcon, UsersIcon } from './icons'

const NAV_ITEMS = [
  { to: '/', label: 'Dashboard', icon: DashboardIcon, end: true },
  { to: '/pipelines', label: 'Pipelines', icon: PipelineIcon, end: false },
  { to: '/sources', label: 'Sources', icon: SourcesIcon, end: false },
] as const

const ROLE_BADGE_STYLE: Record<string, string> = {
  Admin: 'bg-[var(--sf-accent-2)]/15 text-[var(--sf-accent-2)] border-[var(--sf-accent-2)]/30',
  Editor: 'bg-[var(--sf-accent)]/15 text-[var(--sf-accent)] border-[var(--sf-accent)]/30',
  Viewer: 'border-[var(--sf-border)] text-gray-400',
}

export function RequireAuth() {
  const { user } = useAuth()
  if (!user) return <Navigate to="/login" replace />
  return <Layout />
}

function Layout() {
  const { user, hasRole, logout } = useAuth()

  return (
    <div className="flex h-full min-h-screen bg-[var(--sf-bg)]">
      <aside className="flex w-60 shrink-0 flex-col border-r border-[var(--sf-border)] bg-[var(--sf-panel)]/60">
        <div className="flex items-center gap-2 px-5 py-5">
          <div className="h-7 w-7 rounded-md bg-gradient-to-br from-sky-400 to-violet-500" />
          <span className="text-lg font-bold tracking-tight text-white">
            Stream<span className="text-[var(--sf-accent)]">Forge</span>
          </span>
        </div>

        <nav className="flex flex-1 flex-col gap-1 px-3">
          {NAV_ITEMS.map(({ to, label, icon: Icon, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                `flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-[var(--sf-accent)]/12 text-[var(--sf-accent)]'
                    : 'text-gray-400 hover:bg-white/5 hover:text-gray-200'
                }`
              }
            >
              <Icon className="h-5 w-5" />
              {label}
            </NavLink>
          ))}
          {hasRole('Admin') && (
            <NavLink
              to="/users"
              className={({ isActive }) =>
                `flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-[var(--sf-accent)]/12 text-[var(--sf-accent)]'
                    : 'text-gray-400 hover:bg-white/5 hover:text-gray-200'
                }`
              }
            >
              <UsersIcon className="h-5 w-5" />
              Users
            </NavLink>
          )}
        </nav>

        {user && (
          <div className="border-t border-[var(--sf-border)] p-3">
            <div className="flex items-center gap-3 rounded-lg px-2 py-2">
              <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-white/10 text-sm font-semibold text-gray-200">
                {user.displayName.slice(0, 1).toUpperCase()}
              </div>
              <div className="min-w-0 flex-1">
                <div className="truncate text-sm font-medium text-gray-200">{user.displayName}</div>
                <span
                  className={`inline-block rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${ROLE_BADGE_STYLE[user.role]}`}
                >
                  {user.role}
                </span>
              </div>
              <button
                onClick={logout}
                title="Log out"
                className="rounded-md p-1.5 text-gray-500 transition-colors hover:bg-white/5 hover:text-gray-200 focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--sf-accent)]"
              >
                <LogoutIcon className="h-4 w-4" />
              </button>
            </div>
          </div>
        )}
      </aside>

      <main className="min-w-0 flex-1 overflow-y-auto">
        <Outlet />
      </main>
    </div>
  )
}
