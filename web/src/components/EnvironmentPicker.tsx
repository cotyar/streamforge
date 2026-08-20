import { useCallback, useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { Check, ChevronsUpDown, Layers, Loader2, Plus, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { environmentsApi } from '../api/environments'
import { ApiError, setEnvironment404Handler } from '../api/client'
import { useAuth } from '../api/auth'
import { disconnectHub } from '../realtime/hub'
import { DEFAULT_ENVIRONMENT, getStoredEnvironment, isValidEnvironmentName, useEnvironment } from '../lib/environment'
import type { EnvironmentRecord } from '../api/types'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
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

/** Reads the environment straight out of localStorage rather than closing over the `environment` value
 * a hook render captured — the 404 handler below is registered once (empty deps) and must see whatever
 * is CURRENTLY selected at the moment a request 404s, not whatever was selected when it mounted. */
function currentSelection(): string {
  return getStoredEnvironment()
}

/**
 * The dialog body only — deliberately NOT the thing that owns whether it is open. It is rendered as a
 * sibling of `<DropdownMenu>` in EnvironmentPicker (like the two AlertDialogs below it), not nested
 * inside `<DropdownMenuContent>`.
 *
 * Found live: nesting a `<Dialog>` inside `DropdownMenuContent` looked reasonable — click "New
 * environment…", the dialog opens over the menu — but Radix unmounts a closed menu's content
 * subtree, and selecting a `DropdownMenuItem` closes the menu by default. The two happen in the same
 * tick, so the freshly-opened Dialog's own subtree was torn down before it ever painted: the menu
 * would close and nothing would appear. Lifting `open` up to EnvironmentPicker (state that survives
 * the menu closing) and rendering `DialogContent` outside the menu's subtree — exactly how the delete
 * confirmations already had to be structured — is what makes it actually show up.
 */
function CreateEnvironmentDialog({
  open,
  onOpenChange,
  onCreated,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  onCreated: (env: EnvironmentRecord) => void
}) {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  function reset() {
    setName('')
    setDescription('')
    setError(null)
  }

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    const trimmed = name.trim()
    if (!isValidEnvironmentName(trimmed)) {
      setError('Lowercase letters, digits and hyphens only, starting with a letter or digit, 1–32 characters.')
      return
    }
    setSaving(true)
    setError(null)
    try {
      const created = await environmentsApi.create({ name: trimmed, description: description.trim() || undefined })
      toast.success(`Environment "${created.name}" created.`)
      onCreated(created)
      onOpenChange(false)
      reset()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create environment.')
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        onOpenChange(next)
        if (!next) reset()
      }}
    >
      <DialogContent className="sm:max-w-sm">
        <form onSubmit={(e) => void handleSubmit(e)}>
          <DialogHeader>
            <DialogTitle>New environment</DialogTitle>
          </DialogHeader>
          <FieldGroup className="gap-3">
            <Field>
              <FieldLabel htmlFor="env-name">Name</FieldLabel>
              <Input
                id="env-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="staging"
                autoFocus
              />
            </Field>
            <Field>
              <FieldLabel htmlFor="env-description">Description (optional)</FieldLabel>
              <Input
                id="env-description"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="Pre-production catalog"
              />
            </Field>
            {error && <p className="text-sm text-destructive">{error}</p>}
          </FieldGroup>
          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">
                Cancel
              </Button>
            </DialogClose>
            <Button type="submit" disabled={saving}>
              {saving && <Loader2 data-icon="inline-start" className="animate-spin" />}
              {saving ? 'Creating…' : 'Create'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

/**
 * Beside the auth/user control (Layout.tsx's sidebar footer) — lists environments from
 * GET /api/environments, switches on click, and (Admin-only) creates/deletes.
 *
 * Three behaviours beyond a plain dropdown, per plan 021's 021-F brief:
 *  - A selection that no longer exists (someone force-deleted it from another tab/session) falls back
 *    to default with a toast, both on mount and reactively on any 404 seen while non-default is
 *    selected (see the 404 handler below and client.ts's setEnvironment404Handler).
 *  - Switching tears the SignalR hub connection down (disconnectHub) BEFORE persisting the new
 *    selection, so the next lazy reconnect (realtime/hub.ts's hubUrl()) picks it up.
 *  - Layout.tsx keys its routed content on the environment, so switching remounts every page — the
 *    console's only "cache" (each page's own useEffect-on-mount fetch) is invalidated by construction
 *    rather than by a bespoke invalidation call this component would otherwise have to know about.
 */
export function EnvironmentPicker() {
  const { environment, setEnvironment } = useEnvironment()
  const { can } = useAuth()
  const canManage = can('access.write')

  const [environments, setEnvironments] = useState<EnvironmentRecord[] | null>(null)
  const [open, setOpen] = useState(false)
  const [switching, setSwitching] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<EnvironmentRecord | null>(null)
  const [forceTarget, setForceTarget] = useState<EnvironmentRecord | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [createOpen, setCreateOpen] = useState(false)

  const checkingRef = useRef(false)

  const validate = useCallback((list: EnvironmentRecord[]) => {
    const current = currentSelection()
    if (current === DEFAULT_ENVIRONMENT) return
    if (!list.some((e) => e.name === current)) {
      void disconnectHub()
      setEnvironment(DEFAULT_ENVIRONMENT)
      toast.warning(`Environment "${current}" no longer exists — switched back to default.`)
    }
  }, [setEnvironment])

  const load = useCallback(() => {
    return environmentsApi
      .list()
      .then((list) => {
        setEnvironments(list)
        validate(list)
        return list
      })
      .catch(() => {
        setEnvironments(null)
      })
  }, [validate])

  useEffect(() => {
    void load()
  }, [load])

  // Plan 021 wave 2 (021-F): every 404 seen while a non-default environment is selected re-validates
  // against the live list — see client.ts's ENVIRONMENT_HEADER machinery and setEnvironment404Handler's
  // doc comment for why this does not just trust the status code. checkingRef dedupes a burst of 404s
  // (e.g. a page firing several requests at once right after the environment was deleted elsewhere)
  // into a single /api/environments round trip.
  useEffect(() => {
    const handler = () => {
      if (checkingRef.current) return
      checkingRef.current = true
      void load().finally(() => {
        checkingRef.current = false
      })
    }
    setEnvironment404Handler(handler)
    return () => setEnvironment404Handler(null)
  }, [load])

  const switchTo = useCallback(
    async (name: string) => {
      if (name === environment) return
      setSwitching(true)
      try {
        await disconnectHub()
        setEnvironment(name)
      } finally {
        setSwitching(false)
      }
    },
    [environment, setEnvironment],
  )

  const handleCreated = useCallback(
    (created: EnvironmentRecord) => {
      setEnvironments((prev) => (prev ? [...prev, created].sort((a, b) => a.name.localeCompare(b.name)) : [created]))
    },
    [],
  )

  async function afterDelete(name: string) {
    setEnvironments((prev) => (prev ? prev.filter((e) => e.name !== name) : prev))
    if (name === environment) {
      await disconnectHub()
      setEnvironment(DEFAULT_ENVIRONMENT)
    }
  }

  async function confirmDelete() {
    if (!deleteTarget) return
    const target = deleteTarget
    setDeleting(true)
    try {
      await environmentsApi.remove(target.name, false)
      toast.success(`Environment "${target.name}" deleted.`)
      setDeleteTarget(null)
      await afterDelete(target.name)
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        // Non-empty — escalate to the explicit force step rather than silently retrying with
        // force=true. See EnvironmentsEndpoints.cs: 409 means "refuses a non-empty environment
        // without force".
        setDeleteTarget(null)
        setForceTarget(target)
      } else {
        toast.error(err instanceof Error ? err.message : 'Failed to delete environment.')
      }
    } finally {
      setDeleting(false)
    }
  }

  async function confirmForceDelete() {
    if (!forceTarget) return
    const target = forceTarget
    setDeleting(true)
    try {
      await environmentsApi.remove(target.name, true)
      toast.success(`Environment "${target.name}" force-deleted.`)
      setForceTarget(null)
      await afterDelete(target.name)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to force-delete environment.')
    } finally {
      setDeleting(false)
    }
  }

  const list = environments ?? [{ name: DEFAULT_ENVIRONMENT, description: '', createdAtMs: 0, createdBy: '', entityCount: -1 }]

  return (
    <>
      <DropdownMenu
        open={open}
        onOpenChange={(next) => {
          setOpen(next)
          // Refresh on every open, not just at mount — an environment another operator created or
          // deleted since this tab loaded (found live: a directory-mutating action from a second
          // session/tab is otherwise invisible here until the next full page load) should show up the
          // next time this menu is actually looked at, without polling in the background for it.
          if (next) void load()
        }}
      >
        <DropdownMenuTrigger asChild>
          <Button
            variant="outline"
            size="sm"
            disabled={switching}
            className="w-full justify-between gap-2 text-xs"
            title="Current environment"
          >
            <span className="flex min-w-0 items-center gap-1.5">
              {switching ? <Loader2 className="size-3.5 shrink-0 animate-spin" /> : <Layers className="size-3.5 shrink-0" />}
              <span className="truncate">{environment}</span>
            </span>
            <ChevronsUpDown className="size-3.5 shrink-0 text-muted-foreground" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start" className="w-56">
          <DropdownMenuLabel>Environment</DropdownMenuLabel>
          {list.map((env) => (
            <div key={env.name}>
              <DropdownMenuItem onSelect={() => void switchTo(env.name)}>
                {env.name === environment ? <Check className="size-3.5" /> : <span className="size-3.5" />}
                <span className="flex-1 truncate">{env.name}</span>
                {env.entityCount >= 0 && (
                  <span className="text-xs text-muted-foreground">{env.entityCount}</span>
                )}
              </DropdownMenuItem>
              {canManage && env.name !== DEFAULT_ENVIRONMENT && (
                <DropdownMenuItem
                  variant="destructive"
                  className="pl-7 text-xs"
                  onSelect={() => setDeleteTarget(env)}
                >
                  <Trash2 className="size-3.5" /> Delete
                </DropdownMenuItem>
              )}
            </div>
          ))}
          {canManage && (
            <>
              <DropdownMenuSeparator />
              <DropdownMenuItem onSelect={() => setCreateOpen(true)}>
                <Plus className="size-3.5" /> New environment…
              </DropdownMenuItem>
            </>
          )}
        </DropdownMenuContent>
      </DropdownMenu>

      <CreateEnvironmentDialog open={createOpen} onOpenChange={setCreateOpen} onCreated={handleCreated} />

      {/* Stage 1: refuses a non-empty environment without force. */}
      <AlertDialog open={deleteTarget !== null} onOpenChange={(o) => !o && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete environment "{deleteTarget?.name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              This removes the environment from the directory. It only succeeds while the environment holds no
              sources, pipelines or tables.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleting}>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="destructive" disabled={deleting} onClick={() => void confirmDelete()}>
              {deleting ? 'Deleting…' : 'Delete'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Stage 2: only reached after the server has confirmed the environment is non-empty (409 above) —
          a deliberate second, more severe step rather than a checkbox next to stage 1's button, matching
          D7's description of this as the one genuinely destructive operation the plan adds. */}
      <AlertDialog open={forceTarget !== null} onOpenChange={(o) => !o && setForceTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Force-delete "{forceTarget?.name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              {forceTarget && forceTarget.entityCount >= 0
                ? `This environment still holds ${forceTarget.entityCount} ${forceTarget.entityCount === 1 ? 'entity' : 'entities'}. `
                : 'This environment is not empty. '}
              Force-deleting erases its catalog <span className="font-medium text-foreground">and</span> the runtime
              state of every source, pipeline and table in it. This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleting}>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="destructive" disabled={deleting} onClick={() => void confirmForceDelete()}>
              {deleting ? 'Force-deleting…' : 'Force delete'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}

/** Small, always-rendered environment indicator for Topbar.tsx — present on every authenticated page
 *  regardless of environment, so "which environment am I in" never requires opening the picker. Shown
 *  even for "default": the alternative (rendering nothing there) would make "the badge is missing"
 *  indistinguishable from "the badge is broken", which defeats the point of an always-on indicator.
 *  Non-default gets the louder treatment on purpose — it is the state someone is more likely to forget
 *  they are in. Reads the same localStorage-backed hook as the picker, so it stays in sync with zero
 *  coupling between the two components. */
export function EnvironmentBadge() {
  const { environment } = useEnvironment()
  const isDefault = environment === DEFAULT_ENVIRONMENT
  return (
    <span
      className={
        isDefault
          ? 'inline-flex items-center gap-1 rounded-full border border-border px-2.5 py-0.5 text-xs font-medium text-muted-foreground'
          : 'inline-flex items-center gap-1 rounded-full border border-border bg-secondary px-2.5 py-0.5 text-xs font-medium text-secondary-foreground'
      }
      title="Current environment"
    >
      <Layers className="size-3" />
      {environment}
    </span>
  )
}
