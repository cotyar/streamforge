import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { ChevronRight, Database, Pencil, Plus, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { sourcesApi } from '../api/sources'
import type { CreateSourceRequest } from '../api/sources'
import type { ConnectorConfig, FieldDef, FieldType, Metadata, ScheduleSpec, SourceDefinition, SourceKind, Tags } from '../api/types'
import { useSourceTape } from '../hooks/useSourceTape'
import { Topbar } from '../components/Topbar'
import { RoleGate } from '../components/RoleGate'
import { MetadataEditor } from '../components/MetadataEditor'
import { TagList } from '../components/TagList'
import { ConnectorStatusBadge } from '../components/sources/ConnectorStatusBadge'
import { ScheduleEditor } from '../components/sources/ScheduleEditor'
import {
  UrlConfigEditor,
  buildUrlConfig,
  toUrlFormState,
  type UrlFormState,
} from '../components/sources/UrlConfigEditor'
import {
  FileFolderConfigEditor,
  buildFileConfig,
  buildFolderConfig,
  toFileFormState,
  toFolderFormState,
  type FileFolderFormState,
} from '../components/sources/FileFolderConfigEditor'
import {
  GrpcConfigEditor,
  buildGrpcConfig,
  toGrpcFormState,
  type GrpcFormState,
} from '../components/sources/GrpcConfigEditor'
import {
  MappingEditor,
  buildMappingSpec,
  resyncSourcePaths,
  toMappingFormState,
  type MappingFormState,
} from '../components/sources/MappingEditor'
import { cn } from '@/lib/utils'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import { Badge } from '@/components/ui/badge'
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area'
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
const KINDS: SourceKind[] = ['generator', 'url', 'file', 'folder', 'grpc']
/** Kinds driven by a poll schedule + optional mapping (grpc is a persistent subscription instead). */
const POLL_KINDS: SourceKind[] = ['url', 'file', 'folder']

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'number') return Number.isInteger(v) ? v.toString() : v.toFixed(3)
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

function SourceTape({ name }: { name: string }) {
  const events = useSourceTape(name)
  return (
    <ScrollArea className="h-32 rounded-lg border border-border bg-background">
      <div className="min-w-max p-2 font-mono text-[11px] leading-5 text-muted-foreground">
        {events.length === 0 ? (
          <p className="text-muted-foreground/70">Waiting for live events…</p>
        ) : (
          events.map((row, i) => (
            <div key={i} className={cn('whitespace-nowrap', i === 0 && 'text-foreground')}>
              {Object.entries(row)
                .map(([k, v]) => `${k}=${formatCell(v)}`)
                .join('  ')}
            </div>
          ))
        )}
      </div>
      <ScrollBar orientation="horizontal" />
    </ScrollArea>
  )
}

/** Recursive field editor: Json fields drill into nested sub-fields (which may themselves be Json).
 * In mapping mode (`sourcePaths` supplied), top-level (depth 0) rows grow an extra sourcePath input
 * — the mapping's Fields are literally this same array (D-B: "one schema, sourcePath decorates it"),
 * so nested Json children never get their own sourcePath (they're parsed out of the extracted value). */
function FieldEditor({
  fields,
  onChange,
  depth = 0,
  sourcePaths,
  onSourcePathChange,
}: {
  fields: FieldDef[]
  onChange: (fields: FieldDef[]) => void
  depth?: number
  sourcePaths?: string[]
  onSourcePathChange?: (i: number, path: string) => void
}) {
  function update(i: number, patch: Partial<FieldDef>) {
    onChange(fields.map((f, idx) => (idx === i ? { ...f, ...patch } : f)))
  }
  return (
    <div className="flex flex-col gap-2">
      {fields.map((f, i) => (
        <div key={i} className="flex flex-col gap-2">
          <div className="flex items-center gap-2">
            <Input placeholder="field name" value={f.name} onChange={(e) => update(i, { name: e.target.value })} />
            <Select
              value={f.type}
              onValueChange={(v) => {
                const type = v as FieldType
                // Keep children only while the field stays Json.
                update(i, { type, children: type === 'Json' ? (f.children ?? []) : undefined })
              }}
            >
              <SelectTrigger className="w-28 shrink-0">
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
              variant={f.isArray ? 'secondary' : 'ghost'}
              size="icon-sm"
              className="shrink-0 font-mono text-[11px]"
              title={f.isArray ? 'List field (repeated) — click to make it scalar' : 'Make this a list (repeated) field'}
              aria-pressed={!!f.isArray}
              onClick={() => update(i, { isArray: !f.isArray })}
            >
              []
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon-sm"
              className="hover:text-destructive"
              onClick={() => onChange(fields.filter((_, idx) => idx !== i))}
            >
              <Trash2 />
            </Button>
          </div>
          {depth === 0 && sourcePaths && onSourcePathChange && (
            <Input
              value={sourcePaths[i] ?? ''}
              onChange={(e) => onSourcePathChange(i, e.target.value)}
              placeholder={`source path (default: ${f.name || 'field name'})`}
              className="font-mono text-xs"
            />
          )}
          {f.type === 'Json' && (
            <div className="ml-3 flex flex-col gap-2 border-l border-border pl-3">
              <FieldEditor fields={f.children ?? []} onChange={(children) => update(i, { children })} depth={depth + 1} />
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="self-start text-muted-foreground"
                onClick={() => update(i, { children: [...(f.children ?? []), { name: '', type: 'String' }] })}
              >
                <Plus data-icon="inline-start" /> Add nested field
              </Button>
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

/** Read-only drill-down view of a source's schema; Json fields expand to reveal their nested shape. */
function SchemaNode({ field, depth }: { field: FieldDef; depth: number }) {
  const hasChildren = field.type === 'Json' && !!field.children && field.children.length > 0
  const [open, setOpen] = useState(depth === 0)
  return (
    <>
      <div
        className="flex items-center justify-between px-3 py-1.5 text-xs"
        style={{ paddingLeft: `${12 + depth * 16}px` }}
      >
        <span className="flex items-center gap-1 font-mono text-foreground">
          {hasChildren ? (
            <button type="button" onClick={() => setOpen((o) => !o)} className="text-muted-foreground">
              <ChevronRight className={cn('size-3 transition-transform', open && 'rotate-90')} />
            </button>
          ) : (
            <span className="inline-block size-3" />
          )}
          {field.name}
          {field.isArray && <span className="text-muted-foreground/70">[]</span>}
        </span>
        <span className="text-muted-foreground">{field.type}</span>
      </div>
      {hasChildren && open && field.children!.map((c) => <SchemaNode key={c.name} field={c} depth={depth + 1} />)}
    </>
  )
}

interface SourceFormState {
  name: string
  description: string
  kind: SourceKind
  generatorProfile: SourceDefinition['generatorProfile']
  eventsPerSecond: number
  enabled: boolean
  fields: FieldDef[]
  tags: Tags
  metadata: Metadata
  // Connector config (plan 006) — present regardless of kind so switching the Kind select on a
  // NEW source doesn't discard whatever the user already typed into another kind's panel.
  schedule: ScheduleSpec | null
  mappingEnabled: boolean
  mapping: MappingFormState
  url: UrlFormState
  file: FileFolderFormState
  folder: FileFolderFormState
  grpc: GrpcFormState
}

function toFormState(s?: SourceDefinition): SourceFormState {
  const fields = s?.fields ?? [{ name: '', type: 'String' }]
  return {
    name: s?.name ?? '',
    description: s?.description ?? '',
    kind: s?.kind ?? 'generator',
    generatorProfile: s?.generatorProfile ?? 'generic',
    eventsPerSecond: s?.eventsPerSecond ?? 5,
    enabled: s?.enabled ?? true,
    fields,
    tags: s?.tags ?? [],
    metadata: s?.metadata ?? {},
    schedule: s?.connector?.schedule ?? null,
    mappingEnabled: !!s?.connector?.mapping,
    mapping: toMappingFormState(fields, s?.connector?.mapping),
    url: toUrlFormState(s?.connector?.url),
    file: toFileFormState(s?.connector?.file),
    folder: toFolderFormState(s?.connector?.folder),
    grpc: toGrpcFormState(s?.connector?.grpc),
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

  /** Fields and mapping.sourcePaths must stay index-aligned (D-B) — every place fields can change
   * (manual edit, add/remove row, OpenAPI derive, remote-schema fetch) routes through this. */
  function updateFields(fields: FieldDef[]) {
    setForm((f) => ({ ...f, fields, mapping: { ...f.mapping, sourcePaths: resyncSourcePaths(fields, f.mapping.sourcePaths) } }))
  }

  const isPollKind = POLL_KINDS.includes(form.kind)

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    if (!form.name.trim()) {
      setError('Name is required.')
      return
    }
    const fields = form.fields.filter((f) => f.name.trim())
    if (fields.length === 0) {
      setError('At least one field is required.')
      return
    }

    let connector: ConnectorConfig | undefined
    if (form.kind !== 'generator') {
      connector = {
        schedule: isPollKind ? form.schedule : null,
        url: form.kind === 'url' ? buildUrlConfig(form.url) : null,
        file: form.kind === 'file' ? buildFileConfig(form.file) : null,
        folder: form.kind === 'folder' ? buildFolderConfig(form.folder) : null,
        grpc: form.kind === 'grpc' ? buildGrpcConfig(form.grpc) : null,
        mapping:
          isPollKind && form.mappingEnabled
            ? buildMappingSpec(form.fields, { ...form.mapping, sourcePaths: resyncSourcePaths(form.fields, form.mapping.sourcePaths) })
            : null,
      }
    }

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
          tags: form.tags,
          metadata: form.metadata,
          kind: form.kind,
          connector,
        })
      } else {
        const body: CreateSourceRequest = {
          name: form.name.trim(),
          description: form.description,
          fields,
          generatorProfile: form.generatorProfile,
          eventsPerSecond: form.eventsPerSecond,
          enabled: form.enabled,
          tags: form.tags,
          metadata: form.metadata,
          kind: form.kind,
          connector,
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
                <FieldLabel htmlFor="src-kind">Kind</FieldLabel>
                <Select value={form.kind} onValueChange={(v) => setForm((f) => ({ ...f, kind: v as SourceKind }))} disabled={isEdit}>
                  <SelectTrigger id="src-kind" className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      {KINDS.map((k) => (
                        <SelectItem key={k} value={k}>
                          {k}
                        </SelectItem>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
              </Field>
            </div>
            {isEdit && (
              <p className="-mt-2 text-[11px] text-muted-foreground">Kind can't be changed here — delete and recreate to switch kinds.</p>
            )}

            <Field>
              <FieldLabel htmlFor="src-description">Description</FieldLabel>
              <Input
                id="src-description"
                value={form.description}
                onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
              />
            </Field>

            {form.kind === 'generator' && (
              <div className="grid grid-cols-2 gap-3">
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
              </div>
            )}

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

            {isPollKind && (
              <ScheduleEditor initial={form.schedule} onChange={(schedule) => setForm((f) => ({ ...f, schedule }))} disabled={saving} />
            )}

            {form.kind === 'url' && (
              <UrlConfigEditor
                value={form.url}
                onChange={(patch) => setForm((f) => ({ ...f, url: { ...f.url, ...patch } }))}
                isEdit={isEdit}
                disabled={saving}
                onFieldsDerived={updateFields}
              />
            )}
            {form.kind === 'file' && (
              <FileFolderConfigEditor
                variant="file"
                value={form.file}
                onChange={(patch) => setForm((f) => ({ ...f, file: { ...f.file, ...patch } }))}
                disabled={saving}
              />
            )}
            {form.kind === 'folder' && (
              <FileFolderConfigEditor
                variant="folder"
                value={form.folder}
                onChange={(patch) => setForm((f) => ({ ...f, folder: { ...f.folder, ...patch } }))}
                disabled={saving}
              />
            )}
            {form.kind === 'grpc' && (
              <GrpcConfigEditor
                value={form.grpc}
                onChange={(patch) => setForm((f) => ({ ...f, grpc: { ...f.grpc, ...patch } }))}
                isEdit={isEdit}
                disabled={saving}
                onFieldsFetched={updateFields}
              />
            )}

            <Field>
              <div className="mb-1 flex items-center justify-between">
                <FieldLabel className="mb-0">Fields</FieldLabel>
                <Button type="button" variant="ghost" size="sm" onClick={() => updateFields([...form.fields, { name: '', type: 'String' }])}>
                  <Plus data-icon="inline-start" /> Add field
                </Button>
              </div>
              <FieldEditor
                fields={form.fields}
                onChange={updateFields}
                sourcePaths={isPollKind && form.mappingEnabled ? form.mapping.sourcePaths : undefined}
                onSourcePathChange={
                  isPollKind && form.mappingEnabled
                    ? (i, path) =>
                        setForm((f) => ({
                          ...f,
                          mapping: { ...f.mapping, sourcePaths: f.mapping.sourcePaths.map((p, idx) => (idx === i ? path : p)) },
                        }))
                    : undefined
                }
              />
              <p className="mt-1 text-[11px] text-muted-foreground">
                Set a field to <span className="font-mono">Json</span> to drill in and declare its nested shape.
              </p>
            </Field>

            {isPollKind && (
              <Field orientation="horizontal" className="items-center pb-1.5">
                <Switch
                  id="src-mapping-enabled"
                  checked={form.mappingEnabled}
                  onCheckedChange={(checked) => setForm((f) => ({ ...f, mappingEnabled: checked }))}
                />
                <FieldLabel htmlFor="src-mapping-enabled" className="font-normal">
                  Enable field mapping (extract rows from a nested response)
                </FieldLabel>
              </Field>
            )}
            {isPollKind && form.mappingEnabled && (
              <MappingEditor
                fields={form.fields}
                state={form.mapping}
                onChange={(patch) => setForm((f) => ({ ...f, mapping: { ...f.mapping, ...patch } }))}
                disabled={saving}
              />
            )}
          </FieldGroup>

          <MetadataEditor
            initialTags={form.tags}
            initialMetadata={form.metadata}
            onChange={(tags, metadata) => setForm((f) => ({ ...f, tags, metadata }))}
          />

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
        subtitle="Synthetic generators and ingestion connectors feeding your pipelines"
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
            {sources.map((s) => {
              const isConnector = !!s.kind && s.kind !== 'generator'
              return (
                <Card key={s.name}>
                  <CardHeader>
                    <div className="flex items-start justify-between gap-2">
                      <div>
                        <CardTitle>{s.name}</CardTitle>
                        <p className="mt-0.5 text-xs text-muted-foreground">{s.description || 'No description'}</p>
                      </div>
                      <Badge variant="outline">{isConnector ? s.kind : s.generatorProfile}</Badge>
                    </div>
                    <TagList tags={s.tags} className="mt-1" />
                  </CardHeader>
                  <CardContent className="flex flex-col gap-3">
                    <div className="flex items-center justify-between text-xs text-muted-foreground">
                      {isConnector ? (
                        <span>
                          <span className="font-mono text-foreground">{s.kind}</span>-kind connector
                        </span>
                      ) : (
                        <span>
                          rate <span className="font-mono text-foreground">{s.eventsPerSecond}</span>/s
                        </span>
                      )}
                      <RoleGate min="Editor">
                        <label className="flex items-center gap-1.5">
                          <Switch checked={s.enabled} onCheckedChange={() => void toggleEnabled(s)} />
                          Enabled
                        </label>
                      </RoleGate>
                    </div>

                    {isConnector && (
                      <div className="rounded-lg border border-border bg-muted/30 px-3 py-2">
                        <ConnectorStatusBadge name={s.name} />
                      </div>
                    )}

                    <div className="overflow-hidden rounded-lg border border-border">
                      <div className="flex items-center justify-between border-b border-border px-3 py-1.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
                        <span>Field</span>
                        <span>Type</span>
                      </div>
                      <div className="flex flex-col py-1">
                        {s.fields.map((f) => (
                          <SchemaNode key={f.name} field={f} depth={0} />
                        ))}
                      </div>
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
              )
            })}
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
