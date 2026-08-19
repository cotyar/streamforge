import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { KeyRound, Pencil, Plus, RefreshCw, ScanEye, ShieldCheck, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { accessApi } from '../api/access'
import { usersApi } from '../api/users'
import type {
  AccessPolicyDocument,
  ApprovalTemplate,
  EffectivePermissions,
  GroupDefinition,
  PermissionGrant,
  RoleDefinition,
  UserAccessEntry,
  UserInfo,
} from '../api/types'
import { useAuth } from '../api/auth'
import { Topbar } from '../components/Topbar'
import { ActionsDatalist, GrantEditor, GrantSummary } from '../components/access/GrantEditor'
import { StringListField } from '../components/access/StringListField'
import { Card } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Field, FieldDescription, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Dialog, DialogClose, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
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

// ------------------------------------------------------------------------------------------------
// helpers
// ------------------------------------------------------------------------------------------------

function formatWhen(ms: number): string {
  if (!ms) return '—'
  return new Date(ms).toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function seconds(s: number): string {
  if (s <= 0) return 'never'
  if (s % 86400 === 0) return `${s / 86400}d`
  if (s % 3600 === 0) return `${s / 3600}h`
  if (s % 60 === 0) return `${s / 60}m`
  return `${s}s`
}

function message(err: unknown, fallback: string): string {
  return err instanceof Error && err.message ? err.message : fallback
}

/** Every mutating call routes through here so the server's own sentence reaches the operator verbatim.
 *  Several handlers answer 409 with a reason meant to be read ("'Viewer' is a built-in role and cannot
 *  be deleted — …"); replacing it with "Something went wrong" would throw away the whole point. */
async function run(work: () => Promise<unknown>, success: string, onDone: () => void) {
  try {
    await work()
    toast.success(success)
    onDone()
  } catch (err) {
    toast.error(message(err, 'The server refused the change.'))
  }
}

// ------------------------------------------------------------------------------------------------
// Role editor
// ------------------------------------------------------------------------------------------------

function RoleDialog({
  initial,
  existingNames,
  onClose,
  onSaved,
}: {
  initial?: RoleDefinition
  existingNames: string[]
  onClose: () => void
  onSaved: () => void
}) {
  const isEdit = Boolean(initial)
  const [name, setName] = useState(initial?.name ?? '')
  const [description, setDescription] = useState(initial?.description ?? '')
  const [grants, setGrants] = useState<PermissionGrant[]>(initial?.grants ?? [])
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    const trimmed = name.trim()
    if (!trimmed) return setError('A role needs a name.')
    if (!isEdit && existingNames.includes(trimmed)) return setError(`Role '${trimmed}' already exists.`)
    if (grants.some((g) => !g.action.trim())) return setError('Every grant needs an action.')

    setSaving(true)
    try {
      // BuiltIn / UpdatedAt / UpdatedBy are derived server-side and whatever is sent here is discarded.
      await accessApi.upsertRole(trimmed, {
        name: trimmed,
        description: description.trim(),
        grants: grants.map((g) => ({ ...g, action: g.action.trim(), scope: g.scope.trim() || '*' })),
        builtIn: initial?.builtIn ?? false,
        updatedAtMs: 0,
        updatedBy: '',
      })
      onSaved()
    } catch (err) {
      setError(message(err, 'Failed to save the role.'))
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-2xl">
        <form onSubmit={submit} className="flex flex-col gap-4">
          <DialogHeader>
            <DialogTitle>{isEdit ? `Edit role ${initial?.name}` : 'New role'}</DialogTitle>
          </DialogHeader>

          <FieldGroup className="gap-3">
            <Field>
              <FieldLabel htmlFor="role-name">Name</FieldLabel>
              <Input id="role-name" value={name} disabled={isEdit} onChange={(e) => setName(e.target.value)} placeholder="release-manager" />
            </Field>
            <Field>
              <FieldLabel htmlFor="role-desc">Description</FieldLabel>
              <Input id="role-desc" value={description} onChange={(e) => setDescription(e.target.value)} />
            </Field>
          </FieldGroup>

          {initial?.builtIn && (
            <Alert>
              <AlertDescription>
                {initial.name} is a built-in role. Its grants are editable — carve it back if you need to — but it cannot
                be deleted, because deleting it would strand every pre-upgrade token.
              </AlertDescription>
            </Alert>
          )}

          <GrantEditor grants={grants} onChange={setGrants} />

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">Cancel</Button>
            </DialogClose>
            <Button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save role'}</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

// ------------------------------------------------------------------------------------------------
// Group editor
// ------------------------------------------------------------------------------------------------

function GroupDialog({
  initial,
  onClose,
  onSaved,
}: {
  initial?: GroupDefinition
  onClose: () => void
  onSaved: () => void
}) {
  const isEdit = Boolean(initial)
  const [name, setName] = useState(initial?.name ?? '')
  const [description, setDescription] = useState(initial?.description ?? '')
  const [members, setMembers] = useState<string[]>(initial?.members ?? [])
  const [roles, setRoles] = useState<string[]>(initial?.roles ?? [])
  const [claims, setClaims] = useState<string[]>(initial?.externalClaimValues ?? [])
  const [grants, setGrants] = useState<PermissionGrant[]>(initial?.grants ?? [])
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    const trimmed = name.trim()
    if (!trimmed) return setError('A group needs a name.')
    if (grants.some((g) => !g.action.trim())) return setError('Every grant needs an action.')

    setSaving(true)
    try {
      await accessApi.upsertGroup(trimmed, {
        name: trimmed,
        description: description.trim(),
        members,
        roles,
        grants: grants.map((g) => ({ ...g, action: g.action.trim(), scope: g.scope.trim() || '*' })),
        externalClaimValues: claims,
        // The store carries CreatedAtMs forward on an update and ignores what is sent.
        createdAtMs: initial?.createdAtMs ?? 0,
        updatedAtMs: 0,
        updatedBy: '',
      })
      onSaved()
    } catch (err) {
      setError(message(err, 'Failed to save the group.'))
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-2xl">
        <form onSubmit={submit} className="flex flex-col gap-4">
          <DialogHeader>
            <DialogTitle>{isEdit ? `Edit group ${initial?.name}` : 'New group'}</DialogTitle>
          </DialogHeader>

          <FieldGroup className="gap-3">
            <Field>
              <FieldLabel htmlFor="group-name">Name</FieldLabel>
              <Input id="group-name" value={name} disabled={isEdit} onChange={(e) => setName(e.target.value)} placeholder="reviewers" />
            </Field>
            <Field>
              <FieldLabel htmlFor="group-desc">Description</FieldLabel>
              <Input id="group-desc" value={description} onChange={(e) => setDescription(e.target.value)} />
            </Field>
            <StringListField
              id="group-members"
              label="Members"
              description="Usernames, comma-separated. Membership lives on the group, not on the user."
              value={members}
              onChange={setMembers}
              placeholder="alice, bob"
            />
            <StringListField
              id="group-roles"
              label="Roles"
              description="Every member inherits these roles' grants."
              value={roles}
              onChange={setRoles}
              placeholder="Editor"
            />
            <StringListField
              id="group-claims"
              label="External claim values"
              description="Identity-provider group claims that map into this group."
              value={claims}
              onChange={setClaims}
              placeholder="sf-reviewers"
            />
          </FieldGroup>

          <GrantEditor grants={grants} onChange={setGrants} />

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">Cancel</Button>
            </DialogClose>
            <Button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save group'}</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

// ------------------------------------------------------------------------------------------------
// Per-user entry editor — grants and roles ONLY. Disabling is its own control; see the table below.
// ------------------------------------------------------------------------------------------------

function UserEntryDialog({
  initial,
  onClose,
  onSaved,
}: {
  initial?: UserAccessEntry
  onClose: () => void
  onSaved: () => void
}) {
  const isEdit = Boolean(initial)
  const [username, setUsername] = useState(initial?.username ?? '')
  const [roles, setRoles] = useState<string[]>(initial?.roles ?? [])
  const [grants, setGrants] = useState<PermissionGrant[]>(initial?.grants ?? [])
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    const trimmed = username.trim()
    if (!trimmed) return setError('An entry needs a username.')
    if (grants.some((g) => !g.action.trim())) return setError('Every grant needs an action.')

    setSaving(true)
    try {
      await accessApi.upsertUser(trimmed, {
        username: trimmed,
        // Carried forward verbatim: this dialog never changes it, and the disable toggle never
        // changes grants. Mirrors the server's split of the two into separate routes.
        disabled: initial?.disabled ?? false,
        roles,
        grants: grants.map((g) => ({ ...g, action: g.action.trim(), scope: g.scope.trim() || '*' })),
        updatedAtMs: 0,
        updatedBy: '',
      })
      onSaved()
    } catch (err) {
      setError(message(err, 'Failed to save the entry.'))
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-2xl">
        <form onSubmit={submit} className="flex flex-col gap-4">
          <DialogHeader>
            <DialogTitle>{isEdit ? `Entitlements for ${initial?.username}` : 'New access entry'}</DialogTitle>
          </DialogHeader>

          <FieldGroup className="gap-3">
            <Field>
              <FieldLabel htmlFor="entry-username">Username</FieldLabel>
              <Input id="entry-username" value={username} disabled={isEdit} onChange={(e) => setUsername(e.target.value)} placeholder="alice" />
              <FieldDescription>
                An access entry is not an account. Deleting it leaves the login working — it just stops granting
                anything beyond the token's own role.
              </FieldDescription>
            </Field>
            <StringListField
              id="entry-roles"
              label="Roles"
              description="Mirrored from the account's role on every create and update; extra roles added here stack on top."
              value={roles}
              onChange={setRoles}
              placeholder="Editor"
            />
          </FieldGroup>

          <GrantEditor grants={grants} onChange={setGrants} />

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">Cancel</Button>
            </DialogClose>
            <Button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save entry'}</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

// ------------------------------------------------------------------------------------------------
// Approval template editor
// ------------------------------------------------------------------------------------------------

function TemplateDialog({
  initial,
  onClose,
  onSaved,
}: {
  initial?: ApprovalTemplate
  onClose: () => void
  onSaved: () => void
}) {
  const isEdit = Boolean(initial)
  const [t, setT] = useState<ApprovalTemplate>(
    initial ?? {
      name: '',
      actionPattern: '*',
      scopePattern: '*',
      requiredApprovals: 1,
      approverGroups: [],
      expiresAfterSeconds: 86400,
      escalateAfterSeconds: 0,
      escalationGroups: [],
      enabled: true,
    },
  )
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const patch = (fields: Partial<ApprovalTemplate>) => setT((prev) => ({ ...prev, ...fields }))

  async function submit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    const trimmed = t.name.trim()
    if (!trimmed) return setError('A template needs a name.')
    if (t.requiredApprovals < 1) return setError('At least one approval is required.')

    setSaving(true)
    try {
      await accessApi.upsertTemplate(trimmed, { ...t, name: trimmed })
      onSaved()
    } catch (err) {
      setError(message(err, 'Failed to save the template.'))
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-xl">
        <form onSubmit={submit} className="flex flex-col gap-4">
          <DialogHeader>
            <DialogTitle>{isEdit ? `Edit template ${initial?.name}` : 'New approval template'}</DialogTitle>
          </DialogHeader>

          <FieldGroup className="gap-3">
            <Field>
              <FieldLabel htmlFor="tpl-name">Name</FieldLabel>
              <Input id="tpl-name" value={t.name} disabled={isEdit} onChange={(e) => patch({ name: e.target.value })} placeholder="prod-pipeline-change" />
            </Field>
            <div className="grid grid-cols-2 gap-3">
              <Field>
                <FieldLabel htmlFor="tpl-action">Action pattern</FieldLabel>
                <Input id="tpl-action" value={t.actionPattern} onChange={(e) => patch({ actionPattern: e.target.value })} placeholder="pipeline.*" />
              </Field>
              <Field>
                <FieldLabel htmlFor="tpl-scope">Scope pattern</FieldLabel>
                <Input id="tpl-scope" value={t.scopePattern} onChange={(e) => patch({ scopePattern: e.target.value })} placeholder="prod-*" />
              </Field>
            </div>
            <Field>
              <FieldLabel htmlFor="tpl-required">Required approvals</FieldLabel>
              <Input
                id="tpl-required"
                type="number"
                min={1}
                value={t.requiredApprovals}
                onChange={(e) => patch({ requiredApprovals: Number(e.target.value) || 1 })}
              />
              <FieldDescription>
                One rejection decides the whole request — requiring several would let a requester shop for approvers.
              </FieldDescription>
            </Field>
            <StringListField
              id="tpl-approvers"
              label="Approver groups"
              description="A request with no approver group can only expire — nobody can approve it."
              value={t.approverGroups}
              onChange={(v) => patch({ approverGroups: v })}
              placeholder="reviewers"
            />
            <div className="grid grid-cols-2 gap-3">
              <Field>
                <FieldLabel htmlFor="tpl-expires">Expires after (seconds)</FieldLabel>
                <Input
                  id="tpl-expires"
                  type="number"
                  min={0}
                  value={t.expiresAfterSeconds}
                  onChange={(e) => patch({ expiresAfterSeconds: Number(e.target.value) || 0 })}
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="tpl-escalate">Escalate after (seconds)</FieldLabel>
                <Input
                  id="tpl-escalate"
                  type="number"
                  min={0}
                  value={t.escalateAfterSeconds}
                  onChange={(e) => patch({ escalateAfterSeconds: Number(e.target.value) || 0 })}
                />
                <FieldDescription>0 = never escalate.</FieldDescription>
              </Field>
            </div>
            <StringListField
              id="tpl-escalation"
              label="Escalation groups"
              description="Added to the approvers on escalation — the original approvers keep their say."
              value={t.escalationGroups}
              onChange={(v) => patch({ escalationGroups: v })}
              placeholder="oncall"
            />
            <Field orientation="horizontal">
              <Switch id="tpl-enabled" checked={t.enabled} onCheckedChange={(v) => patch({ enabled: v })} />
              <FieldLabel htmlFor="tpl-enabled">Enabled</FieldLabel>
            </Field>
          </FieldGroup>

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">Cancel</Button>
            </DialogClose>
            <Button type="submit" disabled={saving}>{saving ? 'Saving…' : 'Save template'}</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

// ------------------------------------------------------------------------------------------------
// Effective permissions — what the RESOLVER believes right now, version stamp and all
// ------------------------------------------------------------------------------------------------

function EffectiveDialog({ username, onClose }: { username: string; onClose: () => void }) {
  const [data, setData] = useState<EffectivePermissions | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let live = true
    accessApi
      .effective(username)
      .then((d) => live && setData(d))
      .catch((err) => live && setError(message(err, 'Failed to resolve permissions.')))
    return () => {
      live = false
    }
  }, [username])

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Effective permissions — {username}</DialogTitle>
        </DialogHeader>

        {error ? (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : !data ? (
          <Skeleton className="h-40 w-full" />
        ) : (
          <div className="flex flex-col gap-4">
            <div className="flex flex-wrap items-center gap-2 text-sm">
              {data.disabled ? (
                <Badge variant="destructive">disabled</Badge>
              ) : (
                <Badge variant="secondary">active</Badge>
              )}
              <span className="text-muted-foreground">policy version {data.version}</span>
            </div>

            {data.disabled && (
              <Alert variant="destructive">
                <AlertDescription>
                  Disabled, so the answer below is empty by short-circuit, not by configuration — the resolver hands a
                  disabled user no roles, no groups and no grants, which is what kills their live token without a
                  revocation list. Re-enable them to see what they would actually hold.
                </AlertDescription>
              </Alert>
            )}

            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <p className="mb-1 text-xs font-medium text-muted-foreground">Roles</p>
                <p className="text-foreground">{data.roles.length ? data.roles.join(', ') : '—'}</p>
              </div>
              <div>
                <p className="mb-1 text-xs font-medium text-muted-foreground">Groups</p>
                <p className="text-foreground">{data.groups.length ? data.groups.join(', ') : '—'}</p>
              </div>
            </div>

            <div>
              <p className="mb-2 text-xs font-medium text-muted-foreground">
                Flattened grants ({data.grants.length}) — read-only. Edit the role, group or entry they came from.
              </p>
              {data.grants.length === 0 ? (
                <p className="rounded-md border border-dashed border-border px-3 py-4 text-center text-sm text-muted-foreground">
                  This document grants {username} nothing. Against a catalog whose legacy roles were never mirrored,
                  their capability still comes from the token's role claim.
                </p>
              ) : (
                <Card className="max-h-64 overflow-auto py-0">
                  <Table>
                    <TableHeader>
                      <TableRow className="hover:bg-transparent">
                        <TableHead>Action</TableHead>
                        <TableHead>Scope</TableHead>
                        <TableHead>Effect</TableHead>
                        <TableHead>Approval</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {data.grants.map((g, i) => (
                        <TableRow key={i}>
                          <TableCell className="font-mono text-xs text-foreground">{g.action}</TableCell>
                          <TableCell className="font-mono text-xs text-muted-foreground">{g.scope}</TableCell>
                          <TableCell>
                            <Badge variant={g.effect === 'Deny' ? 'destructive' : 'secondary'}>{g.effect}</Badge>
                          </TableCell>
                          <TableCell className="text-xs text-muted-foreground">
                            {g.requiresApproval ? 'required' : '—'}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </Card>
              )}
            </div>
          </div>
        )}

        <DialogFooter>
          <DialogClose asChild>
            <Button type="button" variant="outline">Close</Button>
          </DialogClose>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ------------------------------------------------------------------------------------------------
// The page
// ------------------------------------------------------------------------------------------------

type Editing =
  | { kind: 'role'; value?: RoleDefinition }
  | { kind: 'group'; value?: GroupDefinition }
  | { kind: 'user'; value?: UserAccessEntry }
  | { kind: 'template'; value?: ApprovalTemplate }

type PendingDelete = { kind: Editing['kind']; name: string; note?: string }

export function AccessPage() {
  const { can } = useAuth()
  const canWrite = can('access.write')

  const [doc, setDoc] = useState<AccessPolicyDocument | null>(null)
  // The access document knows nothing about which accounts EXIST — it only carries the entries somebody
  // wrote. On a fresh install that list is empty while three accounts are perfectly able to sign in, so
  // without this the Users tab would show nothing and there would be no way to disable a login that has
  // no entry yet. Best-effort: a caller holding access.read but not user.read still gets the entries.
  const [accounts, setAccounts] = useState<UserInfo[]>([])
  const [loadError, setLoadError] = useState<string | null>(null)
  const [editing, setEditing] = useState<Editing | null>(null)
  const [pendingDelete, setPendingDelete] = useState<PendingDelete | null>(null)
  const [effectiveFor, setEffectiveFor] = useState<string | null>(null)
  const [lookup, setLookup] = useState('')

  const load = useCallback(() => {
    setLoadError(null)
    accessApi
      .get()
      .then(setDoc)
      .catch((err) => {
        setDoc(null)
        setLoadError(message(err, 'Failed to load the access policy.'))
      })
    usersApi.list().then(setAccounts).catch(() => setAccounts([]))
  }, [])

  useEffect(() => load(), [load])

  async function confirmDelete() {
    const target = pendingDelete
    setPendingDelete(null)
    if (!target) return
    const remove =
      target.kind === 'role'
        ? accessApi.deleteRole
        : target.kind === 'group'
          ? accessApi.deleteGroup
          : target.kind === 'user'
            ? accessApi.deleteUser
            : accessApi.deleteTemplate
    await run(() => remove(target.name), `Deleted ${target.name}.`, load)
  }

  async function toggleDisabled(entry: UserAccessEntry, disabled: boolean) {
    // The dedicated route, always. Flipping this must never re-send grants this page happens to hold,
    // which is why the server gave it its own route in the first place.
    //
    // This page briefly carried a workaround here: for a user with NO entry the route used to create
    // one carrying only `disabled` and an empty `roles`, and an empty entry is not the same as no entry
    // — the evaluator consults the token's role claim only while no entry EXISTS. Disable+enable
    // therefore turned an Editor into a 403. The route now seeds the roles from the credential record
    // itself, so every caller is fixed rather than just this page, and the client is back to one call.
    await run(
      () => accessApi.setDisabled(entry.username, { disabled }),
      disabled
        ? `${entry.username} is disabled — their live token stops working within the policy TTL.`
        : `${entry.username} is enabled again.`,
      load,
    )
  }

  const roles = doc?.roles ?? []
  const groups = doc?.groups ?? []
  const templates = doc?.approvalTemplates ?? []

  // One row per ACCOUNT, plus any access entry whose account is gone (an entry outlives a deleted user
  // and would otherwise be invisible — the only place it shows up is here). An account without an entry
  // is not a problem to fix: it is the normal state, and its capability comes from the role on its
  // token. What it must not be is unreachable — disabling it has to work from this table.
  const entriesByName = new Map((doc?.users ?? []).map((u) => [u.username, u]))
  const userRows: { username: string; entry?: UserAccessEntry; accountRole?: string }[] = [
    ...accounts.map((a) => ({ username: a.username, entry: entriesByName.get(a.username), accountRole: a.role })),
    ...(doc?.users ?? [])
      .filter((u) => !accounts.some((a) => a.username === u.username))
      .map((u) => ({ username: u.username, entry: u, accountRole: undefined })),
  ]

  return (
    <div>
      <ActionsDatalist />
      <datalist id="sf-access-usernames">
        {accounts.map((a) => (
          <option key={a.username} value={a.username} />
        ))}
      </datalist>
      <Topbar
        title="Access"
        subtitle="Roles, groups, per-user entitlements and approval templates"
        action={
          <div className="flex items-center gap-2">
            {doc && (
              <Badge variant="outline" title={`Last change ${formatWhen(doc.updatedAtMs)}`}>
                policy v{doc.version}
              </Badge>
            )}
            <Button variant="outline" size="sm" onClick={load}>
              <RefreshCw data-icon="inline-start" /> Refresh
            </Button>
          </div>
        }
      />

      <div className="p-8">
        {loadError ? (
          <Alert variant="destructive">
            <AlertDescription>{loadError}</AlertDescription>
          </Alert>
        ) : doc === null ? (
          <div className="flex flex-col gap-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : (
          <Tabs defaultValue="roles" className="gap-4">
            <TabsList>
              <TabsTrigger value="roles">Roles ({roles.length})</TabsTrigger>
              <TabsTrigger value="groups">Groups ({groups.length})</TabsTrigger>
              <TabsTrigger value="users">Users ({userRows.length})</TabsTrigger>
              <TabsTrigger value="templates">Approval templates ({templates.length})</TabsTrigger>
            </TabsList>

            {/* ---------------------------------------------------------------- roles */}
            <TabsContent value="roles" className="flex flex-col gap-3">
              {canWrite && (
                <div>
                  <Button size="sm" onClick={() => setEditing({ kind: 'role' })}>
                    <Plus data-icon="inline-start" /> New role
                  </Button>
                </div>
              )}
              <Card className="overflow-hidden py-0">
                <Table>
                  <TableHeader>
                    <TableRow className="hover:bg-transparent">
                      <TableHead>Role</TableHead>
                      <TableHead>Description</TableHead>
                      <TableHead>Grants</TableHead>
                      <TableHead>Updated</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {roles.map((r) => (
                      <TableRow key={r.name}>
                        <TableCell className="font-medium text-foreground">
                          {r.name}
                          {r.builtIn && (
                            <Badge variant="outline" className="ml-2">built-in</Badge>
                          )}
                        </TableCell>
                        <TableCell className="max-w-xs truncate text-foreground/80" title={r.description}>
                          {r.description}
                        </TableCell>
                        <TableCell><GrantSummary grants={r.grants} /></TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {formatWhen(r.updatedAtMs)}
                          {r.updatedBy && <div className="opacity-70">by {r.updatedBy}</div>}
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center justify-end gap-1">
                            <Button
                              variant="ghost"
                              size="icon-sm"
                              title={canWrite ? 'Edit grants' : 'View grants'}
                              onClick={() => setEditing({ kind: 'role', value: r })}
                            >
                              <Pencil />
                            </Button>
                            <Button
                              variant="ghost"
                              size="icon-sm"
                              className="hover:text-destructive"
                              disabled={!canWrite || r.builtIn}
                              title={
                                r.builtIn
                                  ? 'Built-in roles cannot be deleted — deleting one would strand every pre-upgrade token. Edit its grants instead.'
                                  : 'Delete role'
                              }
                              onClick={() => setPendingDelete({ kind: 'role', name: r.name })}
                            >
                              <Trash2 />
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </Card>
            </TabsContent>

            {/* --------------------------------------------------------------- groups */}
            <TabsContent value="groups" className="flex flex-col gap-3">
              {canWrite && (
                <div>
                  <Button size="sm" onClick={() => setEditing({ kind: 'group' })}>
                    <Plus data-icon="inline-start" /> New group
                  </Button>
                </div>
              )}
              {groups.length === 0 ? (
                <Empty className="border border-dashed">
                  <EmptyHeader>
                    <EmptyMedia variant="icon"><ShieldCheck /></EmptyMedia>
                    <EmptyTitle>No groups</EmptyTitle>
                    <EmptyDescription>
                      Groups carry roles and grants, and membership lives on the group. Approval templates route
                      requests to groups, so approvals need at least one.
                    </EmptyDescription>
                  </EmptyHeader>
                </Empty>
              ) : (
                <Card className="overflow-hidden py-0">
                  <Table>
                    <TableHeader>
                      <TableRow className="hover:bg-transparent">
                        <TableHead>Group</TableHead>
                        <TableHead>Members</TableHead>
                        <TableHead>Roles</TableHead>
                        <TableHead>Grants</TableHead>
                        <TableHead>Updated</TableHead>
                        <TableHead className="text-right">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {groups.map((g) => (
                        <TableRow key={g.name}>
                          <TableCell className="font-medium text-foreground">
                            {g.name}
                            {g.description && (
                              <div className="text-xs font-normal text-muted-foreground">{g.description}</div>
                            )}
                          </TableCell>
                          <TableCell className="text-sm text-foreground/80">
                            {g.members.length ? g.members.join(', ') : <span className="text-muted-foreground">none</span>}
                          </TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {g.roles.length ? g.roles.join(', ') : '—'}
                          </TableCell>
                          <TableCell><GrantSummary grants={g.grants} /></TableCell>
                          <TableCell className="text-xs text-muted-foreground">{formatWhen(g.updatedAtMs)}</TableCell>
                          <TableCell>
                            <div className="flex items-center justify-end gap-1">
                              <Button variant="ghost" size="icon-sm" title="Edit" onClick={() => setEditing({ kind: 'group', value: g })}>
                                <Pencil />
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon-sm"
                                className="hover:text-destructive"
                                disabled={!canWrite}
                                title="Delete group"
                                onClick={() => setPendingDelete({ kind: 'group', name: g.name })}
                              >
                                <Trash2 />
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </Card>
              )}
            </TabsContent>

            {/* ---------------------------------------------------------------- users */}
            <TabsContent value="users" className="flex flex-col gap-3">
              <div className="flex flex-wrap items-center gap-2">
                {canWrite && (
                  <Button size="sm" onClick={() => setEditing({ kind: 'user' })}>
                    <Plus data-icon="inline-start" /> New entry
                  </Button>
                )}
                <div className="ml-auto flex items-center gap-2">
                  <Input
                    value={lookup}
                    onChange={(e) => setLookup(e.target.value)}
                    placeholder="Resolve any username…"
                    list="sf-access-usernames"
                    className="w-56"
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' && lookup.trim()) setEffectiveFor(lookup.trim())
                    }}
                  />
                  <Button variant="outline" size="sm" disabled={!lookup.trim()} onClick={() => setEffectiveFor(lookup.trim())}>
                    <ScanEye data-icon="inline-start" /> Effective
                  </Button>
                </div>
              </div>

              {userRows.length === 0 ? (
                <Empty className="border border-dashed">
                  <EmptyHeader>
                    <EmptyMedia variant="icon"><KeyRound /></EmptyMedia>
                    <EmptyTitle>No accounts and no entries</EmptyTitle>
                    <EmptyDescription>
                      Users get their capability from their roles. Add an entry only to grant or deny something to one
                      person — or to disable a login.
                    </EmptyDescription>
                  </EmptyHeader>
                </Empty>
              ) : (
                <Card className="overflow-hidden py-0">
                  <Table>
                    <TableHeader>
                      <TableRow className="hover:bg-transparent">
                        <TableHead>Username</TableHead>
                        <TableHead>Roles</TableHead>
                        <TableHead>Direct grants</TableHead>
                        <TableHead>Enabled</TableHead>
                        <TableHead>Updated</TableHead>
                        <TableHead className="text-right">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {userRows.map((row) => {
                        // A row with no entry still needs every control: the disable route creates the entry it
                        // needs, and the editor opens on a blank one carrying only the username.
                        const entry: UserAccessEntry = row.entry ?? {
                          username: row.username,
                          disabled: false,
                          roles: [],
                          grants: [],
                          updatedAtMs: 0,
                          updatedBy: '',
                        }
                        return (
                          <TableRow key={row.username}>
                            <TableCell className="font-medium text-foreground">
                              {row.username}
                              {entry.disabled && <Badge variant="destructive" className="ml-2">disabled</Badge>}
                              {!row.accountRole && (
                                <Badge
                                  variant="outline"
                                  className="ml-2"
                                  title="An access entry with no account — the login was deleted, this entry was not"
                                >
                                  orphaned
                                </Badge>
                              )}
                            </TableCell>
                            <TableCell className="text-sm text-muted-foreground">
                              {entry.roles.length ? (
                                entry.roles.join(', ')
                              ) : row.accountRole ? (
                                <span title="No access entry — the capability comes from the role on this account's token">
                                  {row.accountRole} <span className="opacity-60">(account)</span>
                                </span>
                              ) : (
                                '—'
                              )}
                            </TableCell>
                            <TableCell><GrantSummary grants={entry.grants} /></TableCell>
                            <TableCell>
                              <Switch
                                checked={!entry.disabled}
                                disabled={!canWrite}
                                aria-label={`${row.username} enabled`}
                                title={
                                  entry.disabled
                                    ? 'Disabled — the resolver hands this user an empty grant set, so their live token is already dead'
                                    : 'Enabled. Turning this off revokes their live token within the policy TTL and touches nothing else on the entry.'
                                }
                                onCheckedChange={(v) => void toggleDisabled(entry, !v)}
                              />
                            </TableCell>
                            <TableCell className="text-xs text-muted-foreground">
                              {row.entry ? formatWhen(entry.updatedAtMs) : <span className="opacity-70">no entry</span>}
                              {row.entry && entry.updatedBy && <div className="opacity-70">by {entry.updatedBy}</div>}
                            </TableCell>
                            <TableCell>
                              <div className="flex items-center justify-end gap-1">
                                <Button variant="ghost" size="icon-sm" title="Effective permissions" onClick={() => setEffectiveFor(row.username)}>
                                  <ScanEye />
                                </Button>
                                <Button variant="ghost" size="icon-sm" title="Edit entitlements" onClick={() => setEditing({ kind: 'user', value: entry })}>
                                  <Pencil />
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="icon-sm"
                                  className="hover:text-destructive"
                                  disabled={!canWrite || !row.entry}
                                  title={
                                    row.entry
                                      ? 'Delete the access entry (the account itself is untouched)'
                                      : 'No access entry to delete'
                                  }
                                  onClick={() =>
                                    setPendingDelete({
                                      kind: 'user',
                                      name: row.username,
                                      note: 'The account and its password are untouched — the login keeps working and falls back to the role on its token.',
                                    })
                                  }
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
            </TabsContent>

            {/* ------------------------------------------------------------ templates */}
            <TabsContent value="templates" className="flex flex-col gap-3">
              {canWrite && (
                <div>
                  <Button size="sm" onClick={() => setEditing({ kind: 'template' })}>
                    <Plus data-icon="inline-start" /> New template
                  </Button>
                </div>
              )}
              {templates.length === 0 ? (
                <Empty className="border border-dashed">
                  <EmptyHeader>
                    <EmptyMedia variant="icon"><ShieldCheck /></EmptyMedia>
                    <EmptyTitle>No approval templates</EmptyTitle>
                    <EmptyDescription>
                      A template says which actions need a second pair of eyes and who provides it. Without one matching
                      it, filing an approval request is refused — nobody would be configured to answer it.
                    </EmptyDescription>
                  </EmptyHeader>
                </Empty>
              ) : (
                <Card className="overflow-hidden py-0">
                  <Table>
                    <TableHeader>
                      <TableRow className="hover:bg-transparent">
                        <TableHead>Template</TableHead>
                        <TableHead>Matches</TableHead>
                        <TableHead>Approvals</TableHead>
                        <TableHead>Approvers</TableHead>
                        <TableHead>Expiry / escalation</TableHead>
                        <TableHead className="text-right">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {templates.map((t) => (
                        <TableRow key={t.name}>
                          <TableCell className="font-medium text-foreground">
                            {t.name}
                            {!t.enabled && <Badge variant="outline" className="ml-2">disabled</Badge>}
                          </TableCell>
                          <TableCell className="font-mono text-xs text-muted-foreground">
                            {t.actionPattern} @ {t.scopePattern}
                          </TableCell>
                          <TableCell className="text-sm text-foreground/80">{t.requiredApprovals}</TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {t.approverGroups.length ? (
                              t.approverGroups.join(', ')
                            ) : (
                              <span className="text-destructive">none — can only expire</span>
                            )}
                          </TableCell>
                          <TableCell className="text-xs text-muted-foreground">
                            expires {seconds(t.expiresAfterSeconds)} · escalates {seconds(t.escalateAfterSeconds)}
                            {t.escalationGroups.length > 0 && ` → ${t.escalationGroups.join(', ')}`}
                          </TableCell>
                          <TableCell>
                            <div className="flex items-center justify-end gap-1">
                              <Button variant="ghost" size="icon-sm" title="Edit" onClick={() => setEditing({ kind: 'template', value: t })}>
                                <Pencil />
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon-sm"
                                className="hover:text-destructive"
                                disabled={!canWrite}
                                title="Delete template"
                                onClick={() => setPendingDelete({ kind: 'template', name: t.name })}
                              >
                                <Trash2 />
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </Card>
              )}
            </TabsContent>
          </Tabs>
        )}
      </div>

      {editing?.kind === 'role' && (
        <RoleDialog
          initial={editing.value}
          existingNames={roles.map((r) => r.name)}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null)
            toast.success('Role saved.')
            load()
          }}
        />
      )}
      {editing?.kind === 'group' && (
        <GroupDialog
          initial={editing.value}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null)
            toast.success('Group saved.')
            load()
          }}
        />
      )}
      {editing?.kind === 'user' && (
        <UserEntryDialog
          initial={editing.value}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null)
            toast.success('Access entry saved.')
            load()
          }}
        />
      )}
      {editing?.kind === 'template' && (
        <TemplateDialog
          initial={editing.value}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null)
            toast.success('Approval template saved.')
            load()
          }}
        />
      )}

      {effectiveFor && <EffectiveDialog username={effectiveFor} onClose={() => setEffectiveFor(null)} />}

      <AlertDialog open={pendingDelete !== null} onOpenChange={(open) => !open && setPendingDelete(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete {pendingDelete?.name}?</AlertDialogTitle>
            <AlertDialogDescription>
              {pendingDelete?.note ?? 'This takes effect for every signed-in user within the policy cache TTL.'}
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
