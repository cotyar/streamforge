import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { Database, Pencil, Plus, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { sourcesApi } from '../api/sources'
import type { CreateSourceRequest } from '../api/sources'
import type { FieldDef, FieldType, SourceDefinition } from '../api/types'
import { useSourceTape } from '../hooks/useSourceTape'
import { Topbar } from '../components/Topbar'
import { RoleGate } from '../components/RoleGate'
import { cn } from '@/lib/utils'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
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

const FIELD_TYPES: FieldType[] = ['String', 'Double', 'Long', 'Bool', 'Timestamp', 'Json']
const PROFILES: SourceDefinition['generatorProfile'][] = ['trades', 'quotes', 'orders', 'generic']

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'number') return Number.isInteger(v) ? v.toString() : v.toFixed(3)
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  return String(v)
}

function SourceTape({ name }: { name: string }) {
  const events = useSourceTape(name)
  return (
    <ScrollArea className="h-32 rounded-lg border border-border bg-background">
      <div className="p-2 font-mono text-[11px] leading-5 text-muted-foreground">
        {events.length === 0 ? (
          <p className="text-muted-foreground/70">Waiting for live events…</p>
        ) : (
          events.map((row, i) => (
            <div key={i} className={cn('truncate', i === 0 && 'text-foreground')}>
              {Object.entries(row)
                .map(([k, v]) => `${k}=${formatCell(v)}`)
                .join('  ')}
            </div>
          ))
        )}
      </div>
    </ScrollArea>
  )
}

interface SourceFormState {
  name: string
  description: string
  generatorProfile: SourceDefinition['generatorProfile']
  eventsPerSecond: number
  enabled: boolean
  fields: FieldDef[]
}

function toFormState(s?: SourceDefinition): SourceFormState {
  return {
    name: s?.name ?? '',
    description: s?.description ?? '',
    generatorProfile: s?.generatorProfile ?? 'generic',
    eventsPerSecond: s?.eventsPerSecond ?? 5,
    enabled: s?.enabled ?? true,
    fields: s?.fields ?? [{ name: '', type: 'String' }],
  }
}

function SourceModal({
  initial,
  isEdit,
  onClose,
  onSaved,
}: {
  initial?: SourceDefinition
  isEdit: boolean
  onClose: () => void
  onSaved: () => void
}) {
  const [form, setForm] = useState<SourceFormState>(() => toFormState(initial))
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    if (!form.name.trim()) {
      setError('Name is required.')
      return
    }
    const fields = form.fields.filter((f) => f.name.trim())
    setSaving(true)
    try {
      if (isEdit) {
        await sourcesApi.update(form.name, {
          name: form.name,
          description: form.description,
          fields,
          generatorProfile: form.generatorProfile,
          eventsPerSecond: form.eventsPerSecond,
          enabled: form.enabled,
        })
      } else {
        const body: CreateSourceRequest = {
          name: form.name.trim(),
          description: form.description,
          fields,
          generatorProfile: form.generatorProfile,
          eventsPerSecond: form.eventsPerSecond,
          enabled: form.enabled,
        }
        await sourcesApi.create(body)
      }
      onSaved()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save source.')
    } finally {
      setSaving(false)
    }
  }

  function updateField(i: number, patch: Partial<FieldDef>) {
    setForm((f) => ({ ...f, fields: f.fields.map((fld, idx) => (idx === i ? { ...fld, ...patch } : fld)) }))
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-lg">
        <form onSubmit={handleSubmit} className="flex flex-col gap-4">
          <DialogHeader>
            <DialogTitle>{isEdit ? `Edit ${form.name}` : 'New source'}</DialogTitle>
          </DialogHeader>

          <FieldGroup className="gap-3">
            <div className="grid grid-cols-2 gap-3">
              <Field>
                <FieldLabel htmlFor="src-name">Name</FieldLabel>
                <Input
                  id="src-name"
                  value={form.name}
                  disabled={isEdit}
                  onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                  placeholder="trades"
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="src-profile">Profile</FieldLabel>
                <Select
                  value={form.generatorProfile}
                  onValueChange={(v) => setForm((f) => ({ ...f, generatorProfile: v as SourceDefinition['generatorProfile'] }))}
                >
                  <SelectTrigger id="src-profile" className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      {PROFILES.map((p) => (
                        <SelectItem key={p} value={p}>
                          {p}
                        </SelectItem>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
              </Field>
            </div>

            <Field>
              <FieldLabel htmlFor="src-description">Description</FieldLabel>
              <Input
                id="src-description"
                value={form.description}
                onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
              />
            </Field>

            <div className="grid grid-cols-2 gap-3">
              <Field>
                <FieldLabel htmlFor="src-rate">Events / sec</FieldLabel>
                <Input
                  id="src-rate"
                  type="number"
                  min={0}
                  step="0.1"
                  value={form.eventsPerSecond}
                  onChange={(e) => setForm((f) => ({ ...f, eventsPerSecond: Number(e.target.value) || 0 }))}
                />
              </Field>
              <Field orientation="horizontal" className="items-center pb-1.5">
                <Switch
                  id="src-enabled"
                  checked={form.enabled}
                  onCheckedChange={(checked) => setForm((f) => ({ ...f, enabled: checked }))}
                />
                <FieldLabel htmlFor="src-enabled" className="font-normal">
                  Enabled
                </FieldLabel>
              </Field>
            </div>

            <Field>
              <div className="mb-1 flex items-center justify-between">
                <FieldLabel className="mb-0">Fields</FieldLabel>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={() => setForm((f) => ({ ...f, fields: [...f.fields, { name: '', type: 'String' }] }))}
                >
                  <Plus data-icon="inline-start" /> Add field
                </Button>
              </div>
              <div className="flex flex-col gap-2">
                {form.fields.map((f, i) => (
                  <div key={i} className="flex items-center gap-2">
                    <Input placeholder="field name" value={f.name} onChange={(e) => updateField(i, { name: e.target.value })} />
                    <Select value={f.type} onValueChange={(v) => updateField(i, { type: v as FieldType })}>
                      <SelectTrigger className="w-32 shrink-0">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectGroup>
                          {FIELD_TYPES.map((t) => (
                            <SelectItem key={t} value={t}>
                              {t}
                            </SelectItem>
                          ))}
                        </SelectGroup>
                      </SelectContent>
                    </Select>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon-sm"
                      className="hover:text-destructive"
                      onClick={() => setForm((f2) => ({ ...f2, fields: f2.fields.filter((_, idx) => idx !== i) }))}
                    >
                      <Trash2 />
                    </Button>
                  </div>
                ))}
              </div>
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

export function SourcesPage() {
  const [sources, setSources] = useState<SourceDefinition[] | null>(null)
  const [modal, setModal] = useState<{ mode: 'create' } | { mode: 'edit'; source: SourceDefinition } | null>(null)
  const [pendingDelete, setPendingDelete] = useState<SourceDefinition | null>(null)

  const load = useCallback(() => {
    sourcesApi.list().then(setSources).catch(() => setSources([]))
  }, [])

  useEffect(() => {
    load()
  }, [load])

  async function toggleEnabled(s: SourceDefinition) {
    setSources((prev) => (prev ? prev.map((row) => (row.name === s.name ? { ...row, enabled: !row.enabled } : row)) : prev))
    try {
      await sourcesApi.update(s.name, { ...s, enabled: !s.enabled })
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to update source.')
    } finally {
      load()
    }
  }

  async function confirmDelete() {
    if (!pendingDelete) return
    const name = pendingDelete.name
    setPendingDelete(null)
    try {
      await sourcesApi.remove(name)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to delete source.')
    } finally {
      load()
    }
  }

  return (
    <div>
      <Topbar
        title="Sources"
        subtitle="Synthetic event generators feeding your pipelines"
        action={
          <RoleGate min="Editor">
            <Button onClick={() => setModal({ mode: 'create' })}>
              <Plus data-icon="inline-start" /> New source
            </Button>
          </RoleGate>
        }
      />

      <div className="p-8">
        {sources === null ? (
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            {Array.from({ length: 4 }).map((_, i) => (
              <Card key={i}>
                <CardContent className="flex flex-col gap-3">
                  <div className="h-4 w-2/3 animate-pulse rounded-md bg-muted" />
                  <div className="h-3 w-1/3 animate-pulse rounded-md bg-muted" />
                  <div className="h-24 w-full animate-pulse rounded-md bg-muted" />
                </CardContent>
              </Card>
            ))}
          </div>
        ) : sources.length === 0 ? (
          <Empty className="border border-dashed">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <Database />
              </EmptyMedia>
              <EmptyTitle>No sources configured</EmptyTitle>
              <EmptyDescription>Add a source to start generating live events for your pipelines.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            {sources.map((s) => (
              <Card key={s.name}>
                <CardHeader>
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <CardTitle>{s.name}</CardTitle>
                      <p className="mt-0.5 text-xs text-muted-foreground">{s.description || 'No description'}</p>
                    </div>
                    <Badge variant="outline">{s.generatorProfile}</Badge>
                  </div>
                </CardHeader>
                <CardContent className="flex flex-col gap-3">
                  <div className="flex items-center justify-between text-xs text-muted-foreground">
                    <span>
                      rate <span className="font-mono text-foreground">{s.eventsPerSecond}</span>/s
                    </span>
                    <RoleGate min="Editor">
                      <label className="flex items-center gap-1.5">
                        <Switch checked={s.enabled} onCheckedChange={() => void toggleEnabled(s)} />
                        Enabled
                      </label>
                    </RoleGate>
                  </div>

                  <div className="overflow-hidden rounded-lg border border-border">
                    <Table>
                      <TableHeader>
                        <TableRow className="hover:bg-transparent">
                          <TableHead>Field</TableHead>
                          <TableHead>Type</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {s.fields.map((f) => (
                          <TableRow key={f.name}>
                            <TableCell className="font-mono text-foreground">{f.name}</TableCell>
                            <TableCell className="text-muted-foreground">{f.type}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>

                  <div>
                    <p className="mb-1 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Live tape</p>
                    <SourceTape name={s.name} />
                  </div>

                  <RoleGate min="Editor">
                    <div className="flex justify-end gap-1">
                      <Button variant="ghost" size="sm" onClick={() => setModal({ mode: 'edit', source: s })}>
                        <Pencil data-icon="inline-start" /> Edit
                      </Button>
                      <Button variant="ghost" size="sm" className="hover:text-destructive" onClick={() => setPendingDelete(s)}>
                        <Trash2 data-icon="inline-start" /> Delete
                      </Button>
                    </div>
                  </RoleGate>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </div>

      {modal && (
        <SourceModal
          isEdit={modal.mode === 'edit'}
          initial={modal.mode === 'edit' ? modal.source : undefined}
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
            <AlertDialogTitle>Delete source?</AlertDialogTitle>
            <AlertDialogDescription>
              This removes <span className="font-medium text-foreground">{pendingDelete?.name}</span>. Pipelines referencing it will
              fail.
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
