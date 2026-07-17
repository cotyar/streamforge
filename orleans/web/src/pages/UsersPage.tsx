import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { usersApi } from '../api/users'
import type { Role, UserInfo } from '../api/types'
import { useAuth } from '../api/auth'
import { Topbar } from '../components/Topbar'
import { EmptyState } from '../components/EmptyState'
import { Skeleton } from '../components/Skeleton'
import { EditIcon, PlusIcon, TrashIcon } from '../components/icons'

const ROLES: Role[] = ['Admin', 'Editor', 'Viewer']

const inputCls =
  'w-full rounded-md border border-[var(--sf-border)] bg-[var(--sf-bg)] px-2.5 py-1.5 text-sm text-gray-200 focus:border-[var(--sf-accent)] focus:outline-none'
const labelCls = 'mb-1 block text-xs font-medium uppercase tracking-wide text-gray-500'

function formatDate(ms: number): string {
  return new Date(ms).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' })
}

function UserModal({
  initial,
  onClose,
  onSaved,
}: {
  initial?: UserInfo
  onClose: () => void
  onSaved: () => void
}) {
  const isEdit = Boolean(initial)
  const [username, setUsername] = useState(initial?.username ?? '')
  const [displayName, setDisplayName] = useState(initial?.displayName ?? '')
  const [role, setRole] = useState<Role>(initial?.role ?? 'Viewer')
  const [password, setPassword] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    if (!isEdit && (!username.trim() || !password)) {
      setError('Username and password are required.')
      return
    }
    setSaving(true)
    try {
      if (isEdit && initial) {
        await usersApi.update(initial.username, {
          displayName: displayName.trim() || undefined,
          role,
          password: password || undefined,
        })
      } else {
        await usersApi.create({ username: username.trim(), displayName: displayName.trim() || username.trim(), role, password })
      }
      onSaved()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save user.')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 px-4">
      <form onSubmit={handleSubmit} className="flex w-full max-w-sm flex-col gap-4 rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-5">
        <h3 className="text-sm font-semibold text-gray-100">{isEdit ? `Edit ${initial?.username}` : 'New user'}</h3>

        <div>
          <label className={labelCls}>Username</label>
          <input className={inputCls} value={username} disabled={isEdit} onChange={(e) => setUsername(e.target.value)} placeholder="jdoe" />
        </div>
        <div>
          <label className={labelCls}>Display name</label>
          <input className={inputCls} value={displayName} onChange={(e) => setDisplayName(e.target.value)} placeholder="Jane Doe" />
        </div>
        <div>
          <label className={labelCls}>Role</label>
          <select className={inputCls} value={role} onChange={(e) => setRole(e.target.value as Role)}>
            {ROLES.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className={labelCls}>{isEdit ? 'New password (optional)' : 'Password'}</label>
          <input
            type="password"
            className={inputCls}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder={isEdit ? 'Leave blank to keep current' : '••••••••'}
          />
        </div>

        {error && (
          <p className="rounded-md border border-[var(--sf-bad)]/30 bg-[var(--sf-bad)]/10 px-3 py-2 text-sm text-[var(--sf-bad)]">{error}</p>
        )}

        <div className="mt-1 flex justify-end gap-2">
          <button type="button" onClick={onClose} className="rounded-md border border-[var(--sf-border)] px-3 py-1.5 text-sm text-gray-300 hover:bg-white/5">
            Cancel
          </button>
          <button
            type="submit"
            disabled={saving}
            className="rounded-md bg-gradient-to-r from-sky-400 to-violet-500 px-4 py-1.5 text-sm font-semibold text-slate-950 hover:opacity-90 disabled:opacity-50"
          >
            {saving ? 'Saving…' : 'Save'}
          </button>
        </div>
      </form>
    </div>
  )
}

export function UsersPage() {
  const { user: currentUser } = useAuth()
  const [users, setUsers] = useState<UserInfo[] | null>(null)
  const [modal, setModal] = useState<{ mode: 'create' } | { mode: 'edit'; user: UserInfo } | null>(null)
  const [pendingDelete, setPendingDelete] = useState<UserInfo | null>(null)

  const load = useCallback(() => {
    usersApi.list().then(setUsers).catch(() => setUsers([]))
  }, [])

  useEffect(() => {
    load()
  }, [load])

  async function confirmDelete() {
    if (!pendingDelete) return
    const username = pendingDelete.username
    setPendingDelete(null)
    await usersApi.remove(username)
    load()
  }

  return (
    <div>
      <Topbar
        title="Users"
        subtitle="Manage StreamForge accounts and roles"
        action={
          <button
            onClick={() => setModal({ mode: 'create' })}
            className="flex items-center gap-1.5 rounded-lg bg-gradient-to-r from-sky-400 to-violet-500 px-4 py-2 text-sm font-semibold text-slate-950 transition-opacity hover:opacity-90"
          >
            <PlusIcon className="h-4 w-4" /> New user
          </button>
        }
      />

      <div className="p-8">
        {users === null ? (
          <div className="flex flex-col gap-2">
            {Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : users.length === 0 ? (
          <EmptyState title="No users found" />
        ) : (
          <div className="overflow-hidden rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)]">
            <table className="w-full border-collapse text-left text-sm">
              <thead>
                <tr className="border-b border-[var(--sf-border)] text-xs uppercase tracking-wide text-gray-500">
                  <th className="px-4 py-3 font-medium">Username</th>
                  <th className="px-4 py-3 font-medium">Display name</th>
                  <th className="px-4 py-3 font-medium">Role</th>
                  <th className="px-4 py-3 font-medium">Created</th>
                  <th className="px-4 py-3 font-medium text-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {users.map((u) => {
                  const isSelf = u.username === currentUser?.username
                  return (
                    <tr key={u.username} className="border-b border-[var(--sf-border)]/60 last:border-0 hover:bg-white/[0.02]">
                      <td className="px-4 py-3 font-medium text-gray-100">
                        {u.username}
                        {isSelf && <span className="ml-2 text-xs text-gray-500">(you)</span>}
                      </td>
                      <td className="px-4 py-3 text-gray-300">{u.displayName}</td>
                      <td className="px-4 py-3 text-gray-400">{u.role}</td>
                      <td className="px-4 py-3 text-xs text-gray-500">{formatDate(u.createdAtMs)}</td>
                      <td className="px-4 py-3">
                        <div className="flex items-center justify-end gap-1">
                          <button
                            title="Edit"
                            onClick={() => setModal({ mode: 'edit', user: u })}
                            className="rounded-md p-1.5 text-gray-400 transition-colors hover:bg-white/5 hover:text-gray-200"
                          >
                            <EditIcon className="h-4 w-4" />
                          </button>
                          <button
                            title={isSelf ? "You can't delete your own account" : 'Delete'}
                            disabled={isSelf}
                            onClick={() => setPendingDelete(u)}
                            className="rounded-md p-1.5 text-gray-400 transition-colors hover:bg-white/5 hover:text-[var(--sf-bad)] disabled:cursor-not-allowed disabled:opacity-30"
                          >
                            <TrashIcon className="h-4 w-4" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {modal && (
        <UserModal
          initial={modal.mode === 'edit' ? modal.user : undefined}
          onClose={() => setModal(null)}
          onSaved={() => {
            setModal(null)
            load()
          }}
        />
      )}

      {pendingDelete && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 px-4">
          <div className="w-full max-w-sm rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-5">
            <h3 className="text-sm font-semibold text-gray-100">Delete user?</h3>
            <p className="mt-2 text-sm text-gray-400">
              This permanently removes <span className="font-medium text-gray-200">{pendingDelete.username}</span>.
            </p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                onClick={() => setPendingDelete(null)}
                className="rounded-md border border-[var(--sf-border)] px-3 py-1.5 text-sm text-gray-300 hover:bg-white/5"
              >
                Cancel
              </button>
              <button
                onClick={() => void confirmDelete()}
                className="rounded-md bg-[var(--sf-bad)]/20 px-3 py-1.5 text-sm font-medium text-[var(--sf-bad)] hover:bg-[var(--sf-bad)]/30"
              >
                Delete
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
