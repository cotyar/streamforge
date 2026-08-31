import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { Pencil, Plus, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { usersApi } from '../api/users'
import type { Role, UserInfo } from '../api/types'
import { useAuth } from '../api/auth'
import { Topbar } from '../components/Topbar'
import { Card } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Dialog, DialogClose, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Users as UsersIcon } from 'lucide-react'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'

const ROLES: Role[] = ['Admin', 'Editor', 'Viewer']

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
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-sm">
        <form onSubmit={handleSubmit} className="flex flex-col gap-4">
          <DialogHeader>
            <DialogTitle>{isEdit ? `Edit ${initial?.username}` : 'New user'}</DialogTitle>
          </DialogHeader>

          <FieldGroup className="gap-3">
            <Field>
              <FieldLabel htmlFor="user-username">Username</FieldLabel>
              <Input id="user-username" value={username} disabled={isEdit} onChange={(e) => setUsername(e.target.value)} placeholder="jdoe" />
            </Field>
            <Field>
              <FieldLabel htmlFor="user-display">Display name</FieldLabel>
              <Input id="user-display" value={displayName} onChange={(e) => setDisplayName(e.target.value)} placeholder="Jane Doe" />
            </Field>
            <Field>
              <FieldLabel htmlFor="user-role">Role</FieldLabel>
              <Select value={role} onValueChange={(v) => setRole(v as Role)}>
                <SelectTrigger id="user-role" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    {ROLES.map((r) => (
                      <SelectItem key={r} value={r}>
                        {r}
                      </SelectItem>
                    ))}
                  </SelectGroup>
                </SelectContent>
              </Select>
            </Field>
            <Field>
              <FieldLabel htmlFor="user-password">{isEdit ? 'New password (optional)' : 'Password'}</FieldLabel>
              <Input
                id="user-password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder={isEdit ? 'Leave blank to keep current' : '••••••••'}
              />
            </Field>
          </FieldGroup>

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">
                Cancel
              </Button>
            </DialogClose>
            <Button type="submit" disabled={saving}>
              {saving ? 'Saving…' : 'Save'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
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
    try {
      await usersApi.remove(username)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to delete user.')
    } finally {
      load()
    }
  }

  return (
    <div>
      <Topbar
        title="Users"
        subtitle="Manage StreamsForge accounts and roles"
        action={
          <Button onClick={() => setModal({ mode: 'create' })}>
            <Plus data-icon="inline-start" /> New user
          </Button>
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
          <Empty className="border border-dashed">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <UsersIcon />
              </EmptyMedia>
              <EmptyTitle>No users found</EmptyTitle>
              <EmptyDescription>Add StreamsForge accounts to grant access.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <Card className="overflow-hidden py-0">
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead>Username</TableHead>
                  <TableHead>Display name</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {users.map((u) => {
                  const isSelf = u.username === currentUser?.username
                  return (
                    <TableRow key={u.username}>
                      <TableCell className="font-medium text-foreground">
                        {u.username}
                        {isSelf && <span className="ml-2 text-xs text-muted-foreground">(you)</span>}
                      </TableCell>
                      <TableCell className="text-foreground/80">{u.displayName}</TableCell>
                      <TableCell className="text-muted-foreground">{u.role}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{formatDate(u.createdAtMs)}</TableCell>
                      <TableCell>
                        <div className="flex items-center justify-end gap-1">
                          <Button variant="ghost" size="icon-sm" title="Edit" onClick={() => setModal({ mode: 'edit', user: u })}>
                            <Pencil />
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon-sm"
                            title={isSelf ? "You can't delete your own account" : 'Delete'}
                            disabled={isSelf}
                            onClick={() => setPendingDelete(u)}
                            className="hover:text-destructive"
                          >
                            <Trash2 />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </Card>
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

      <AlertDialog open={pendingDelete !== null} onOpenChange={(open) => !open && setPendingDelete(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete user?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently removes <span className="font-medium text-foreground">{pendingDelete?.username}</span>.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={() => void confirmDelete()}>
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
