import { useState } from 'react'
import type { FormEvent } from 'react'
import { Navigate, useLocation, useNavigate } from 'react-router-dom'
import { useAuth } from '../api/auth'
import { ApiError } from '../api/client'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Spinner } from '@/components/ui/spinner'

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
    <div className="flex min-h-screen items-center justify-center bg-background px-4">
      <div className="w-full max-w-sm">
        <div className="mb-8 flex flex-col items-center gap-1 text-center">
          <h1 className="text-2xl font-bold tracking-tight text-foreground">
            Stream<span className="text-primary">Forge</span>
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">Streaming SQL, live from your event fabric.</p>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Sign in</CardTitle>
            <CardDescription>Enter your StreamForge credentials to continue.</CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleSubmit}>
              <FieldGroup>
                <Field>
                  <FieldLabel htmlFor="username">Username</FieldLabel>
                  <Input
                    id="username"
                    autoComplete="username"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    placeholder="admin"
                    required
                  />
                </Field>
                <Field>
                  <FieldLabel htmlFor="password">Password</FieldLabel>
                  <Input
                    id="password"
                    type="password"
                    autoComplete="current-password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="••••••••"
                    required
                  />
                </Field>

                {error && (
                  <Alert variant="destructive">
                    <AlertDescription>{error}</AlertDescription>
                  </Alert>
                )}

                <Button type="submit" disabled={submitting} className="w-full">
                  {submitting && <Spinner data-icon="inline-start" />}
                  {submitting ? 'Signing in…' : 'Sign in'}
                </Button>
              </FieldGroup>
            </form>
          </CardContent>
          <CardFooter className="flex-col items-start gap-2">
            <p className="text-xs font-medium text-muted-foreground">Demo credentials</p>
            <ul className="flex w-full flex-col gap-1 font-mono text-xs text-muted-foreground">
              {DEMO_CREDS.map((c) => (
                <li key={c.user} className="flex justify-between">
                  <span>
                    {c.user} / {c.pass}
                  </span>
                  <span className="text-muted-foreground/70">{c.role}</span>
                </li>
              ))}
            </ul>
          </CardFooter>
        </Card>
      </div>
    </div>
  )
}
