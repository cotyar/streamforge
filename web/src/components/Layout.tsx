import { NavLink, Navigate, Outlet } from 'react-router-dom'
import { ArrowLeftRight, Bot, BookOpen, Braces, ClipboardCheck, LayoutDashboard, LogOut, Moon, ScrollText, ShieldCheck, Sun, Users as UsersIconLucide, Workflow, Database, Table2, Waypoints } from 'lucide-react'
import { useAuth } from '../api/auth'
import { cn } from '@/lib/utils'
import { useTheme } from '@/lib/theme'
import { Avatar, AvatarFallback } from '@/components/ui/avatar'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Toaster } from '@/components/ui/sonner'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

const NAV_ITEMS = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard, end: true },
  { to: '/pipelines', label: 'Pipelines', icon: Workflow, end: false },
  { to: '/tables', label: 'Tables', icon: Table2, end: false },
  { to: '/sources', label: 'Sources', icon: Database, end: false },
  { to: '/lineage', label: 'Lineage', icon: Waypoints, end: false },
  { to: '/explorer', label: 'API Explorer', icon: Braces, end: false },
  { to: '/config', label: 'Configuration', icon: ArrowLeftRight, end: false },
] as const

const ROLE_BADGE_VARIANT: Record<string, 'default' | 'secondary' | 'outline'> = {
  Admin: 'default',
  Editor: 'secondary',
  Viewer: 'outline',
}

export function RequireAuth() {
  const { user } = useAuth()
  if (!user) return <Navigate to="/login" replace />
  return <Layout />
}

function ThemeToggle() {
  const { theme, toggleTheme } = useTheme()
  const isDark = theme === 'dark'
  const label = isDark ? 'Switch to light theme' : 'Switch to dark theme'

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Button variant="ghost" size="icon-sm" onClick={toggleTheme} aria-label={label}>
            {isDark ? <Sun /> : <Moon />}
          </Button>
        </TooltipTrigger>
        <TooltipContent side="top">{label}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

function Layout() {
  const { user, hasRole, can, logout } = useAuth()

  return (
    <div className="flex h-full min-h-screen bg-background">
      <aside className="flex w-60 shrink-0 flex-col border-r border-sidebar-border bg-sidebar">
        <div className="flex flex-col gap-0.5 px-5 py-5">
          <span className="text-lg font-bold tracking-tight text-sidebar-foreground">
            Stream<span className="text-primary">Forge</span>
          </span>
        </div>

        <nav className="flex flex-1 flex-col gap-1 px-3">
          {NAV_ITEMS.map(({ to, label, icon: Icon, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-sidebar-accent text-sidebar-primary'
                    : 'text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-foreground',
                )
              }
            >
              <Icon className="size-5" />
              {label}
            </NavLink>
          ))}
          {hasRole('Editor') && (
            <NavLink
              to="/chat"
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-sidebar-accent text-sidebar-primary'
                    : 'text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-foreground',
                )
              }
            >
              <Bot className="size-5" />
              AI Control
            </NavLink>
          )}
          {/* Plan 015 wave 6. Approvals is ungated on purpose: /api/approvals is a Viewer route and the
              inbox is where a requester watches their OWN request — gating it on approval.decide would
              hide the page from exactly the person who filed the thing. */}
          <NavLink
            to="/approvals"
            className={({ isActive }) =>
              cn(
                'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                isActive
                  ? 'bg-sidebar-accent text-sidebar-primary'
                  : 'text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-foreground',
              )
            }
          >
            <ClipboardCheck className="size-5" />
            Approvals
          </NavLink>
          {can('access.read') && (
            <NavLink
              to="/access"
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-sidebar-accent text-sidebar-primary'
                    : 'text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-foreground',
                )
              }
            >
              <ShieldCheck className="size-5" />
              Access
            </NavLink>
          )}
          {can('audit.read') && (
            <NavLink
              to="/audit"
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-sidebar-accent text-sidebar-primary'
                    : 'text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-foreground',
                )
              }
            >
              <ScrollText className="size-5" />
              Audit
            </NavLink>
          )}
          {hasRole('Admin') && (
            <NavLink
              to="/users"
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-sidebar-accent text-sidebar-primary'
                    : 'text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-foreground',
                )
              }
            >
              <UsersIconLucide className="size-5" />
              Users
            </NavLink>
          )}

          <div className="mt-4 border-t border-sidebar-border pt-3">
            <div className="px-3 pb-1 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
              Resources
            </div>
            <a
              href="/docs"
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-sidebar-foreground"
            >
              <BookOpen className="size-5" />
              Documentation
            </a>
          </div>
        </nav>

        {user && (
          <div className="border-t border-sidebar-border p-3">
            <div className="flex items-center gap-3 rounded-lg px-2 py-2">
              <Avatar className="shrink-0">
                <AvatarFallback>{user.displayName.slice(0, 1).toUpperCase()}</AvatarFallback>
              </Avatar>
              <div className="min-w-0 flex-1">
                <div className="truncate text-sm font-medium text-sidebar-foreground">{user.displayName}</div>
                <Badge variant={ROLE_BADGE_VARIANT[user.role]} className="mt-0.5">
                  {user.role}
                </Badge>
              </div>
              <ThemeToggle />
              <Button variant="ghost" size="icon-sm" onClick={logout} title="Log out">
                <LogOut />
              </Button>
            </div>
          </div>
        )}
      </aside>

      <main className="min-w-0 flex-1 overflow-y-auto">
        <Outlet />
      </main>
      <Toaster />
    </div>
  )
}
