import type { ReactNode } from 'react'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { AuthProvider, useAuth } from './api/auth'
import { RequireAuth } from './components/Layout'
import { LoginPage } from './pages/LoginPage'
import { DashboardPage } from './pages/DashboardPage'
import { PipelinesPage } from './pages/PipelinesPage'
import { PipelineDetailPage } from './pages/PipelineDetailPage'
import { TablesPage } from './pages/TablesPage'
import { TableDetailPage } from './pages/TableDetailPage'
import { SourcesPage } from './pages/SourcesPage'
import { LineagePage } from './pages/LineagePage'
import { UsersPage } from './pages/UsersPage'
import { ApiExplorerPage } from './pages/ApiExplorerPage'
import { ConfigPage } from './pages/ConfigPage'
import { ChatPage } from './pages/ChatPage'
import { AccessPage } from './pages/AccessPage'
import { ApprovalsPage } from './pages/ApprovalsPage'
import { AuditPage } from './pages/AuditPage'

function AdminRoute({ children }: { children: ReactNode }) {
  const { hasRole } = useAuth()
  if (!hasRole('Admin')) return <Navigate to="/" replace />
  return <>{children}</>
}

/** Plan 015 wave 6: a route gate keyed on an ACTION rather than a role. `AdminRoute`/`EditorRoute`
 *  stay exactly as they are — `can` answers ordinally until wave 6-A gives it real grants, so this is
 *  a strictly additive gate, not a migration of the two above. */
function CanRoute({ action, children }: { action: string; children: ReactNode }) {
  const { can } = useAuth()
  if (!can(action)) return <Navigate to="/" replace />
  return <>{children}</>
}

function EditorRoute({ children }: { children: ReactNode }) {
  const { hasRole } = useAuth()
  if (!hasRole('Editor')) return <Navigate to="/" replace />
  return <>{children}</>
}

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route element={<RequireAuth />}>
            <Route path="/" element={<DashboardPage />} />
            <Route path="/pipelines" element={<PipelinesPage />} />
            <Route path="/pipelines/:id" element={<PipelineDetailPage />} />
            <Route path="/tables" element={<TablesPage />} />
            <Route path="/tables/:id" element={<TableDetailPage />} />
            <Route path="/sources" element={<SourcesPage />} />
            <Route path="/lineage" element={<LineagePage />} />
            <Route path="/explorer" element={<ApiExplorerPage />} />
            <Route path="/config" element={<ConfigPage />} />
            <Route
              path="/chat"
              element={
                <EditorRoute>
                  <ChatPage />
                </EditorRoute>
              }
            />
            <Route path="/approvals" element={<ApprovalsPage />} />
            <Route
              path="/access"
              element={
                <CanRoute action="access.read">
                  <AccessPage />
                </CanRoute>
              }
            />
            <Route
              path="/audit"
              element={
                <CanRoute action="audit.read">
                  <AuditPage />
                </CanRoute>
              }
            />
            <Route
              path="/users"
              element={
                <AdminRoute>
                  <UsersPage />
                </AdminRoute>
              }
            />
          </Route>
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AuthProvider>
    </BrowserRouter>
  )
}
