import { useState } from 'react'
import type { FormEvent } from 'react'
import { Navigate, useLocation, useNavigate } from 'react-router-dom'
import { useAuth } from '../api/auth'
import { ApiError } from '../api/client'

const DEMO_CREDS = [
  { user: 'admin', pass: 'admin123!', role: 'Admin' },
  { user: 'editor', pass: 'editor123!', role: 'Editor' },
  { user: 'viewer', pass: 'viewer123!', role: 'Viewer' },
]

export function LoginPage() {
  const { user, login } = useAuth()
  const navigate = useNavigate()
  const location = useLocation()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)

  if (user) {
    const redirectTo = (location.state as { from?: string } | null)?.from ?? '/'
    return <Navigate to={redirectTo} replace />
  }

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      await login(username, password)
      navigate('/', { replace: true })
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Unable to sign in — check your credentials.')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-[var(--sf-bg)] px-4">
      <div className="w-full max-w-sm">
        <div className="mb-8 flex flex-col items-center gap-3">
          <div className="h-11 w-11 rounded-xl bg-gradient-to-br from-sky-400 to-violet-500" />
          <h1 className="text-2xl font-bold tracking-tight text-white">
            Stream<span className="text-[var(--sf-accent)]">Forge</span>
          </h1>
          <p className="text-sm text-gray-500">Streaming SQL, live from your event fabric.</p>
        </div>

        <form
          onSubmit={handleSubmit}
          className="flex flex-col gap-4 rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-6 shadow-xl shadow-black/20"
        >
          <div>
            <label htmlFor="username" className="mb-1.5 block text-xs font-medium uppercase tracking-wide text-gray-500">
              Username
            </label>
            <input
              id="username"
              autoComplete="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full rounded-lg border border-[var(--sf-border)] bg-[var(--sf-bg)] px-3 py-2 text-sm text-gray-100 outline-none transition-colors focus:border-[var(--sf-accent)] focus:ring-1 focus:ring-[var(--sf-accent)]"
              placeholder="admin"
              required
            />
          </div>
          <div>
            <label htmlFor="password" className="mb-1.5 block text-xs font-medium uppercase tracking-wide text-gray-500">
              Password
            </label>
            <input
              id="password"
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full rounded-lg border border-[var(--sf-border)] bg-[var(--sf-bg)] px-3 py-2 text-sm text-gray-100 outline-none transition-colors focus:border-[var(--sf-accent)] focus:ring-1 focus:ring-[var(--sf-accent)]"
              placeholder="••••••••"
              required
            />
          </div>

          {error && (
            <p className="rounded-md border border-[var(--sf-bad)]/30 bg-[var(--sf-bad)]/10 px-3 py-2 text-sm text-[var(--sf-bad)]">
              {error}
            </p>
          )}

          <button
            type="submit"
            disabled={submitting}
            className="mt-1 rounded-lg bg-gradient-to-r from-sky-400 to-violet-500 px-4 py-2.5 text-sm font-semibold text-slate-950 transition-opacity hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {submitting ? 'Signing in…' : 'Sign in'}
          </button>
        </form>

        <div className="mt-5 rounded-lg border border-[var(--sf-border)] bg-[var(--sf-panel)]/50 p-3.5 text-xs text-gray-500">
          <p className="mb-1.5 font-medium text-gray-400">Demo credentials</p>
          <ul className="space-y-1 font-mono">
            {DEMO_CREDS.map((c) => (
              <li key={c.user} className="flex justify-between">
                <span>
                  {c.user} / {c.pass}
                </span>
                <span className="text-gray-600">{c.role}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  )
}
