import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { sourcesApi } from '../api/sources'
import type { CreateSourceRequest } from '../api/sources'
import type { FieldDef, FieldType, SourceDefinition } from '../api/types'
import { useSourceTape } from '../hooks/useSourceTape'
import { Topbar } from '../components/Topbar'
import { RoleGate } from '../components/RoleGate'
import { EmptyState } from '../components/EmptyState'
import { SkeletonGrid } from '../components/Skeleton'
import { EditIcon, PlusIcon, TrashIcon } from '../components/icons'

const FIELD_TYPES: FieldType[] = ['String', 'Double', 'Long', 'Bool', 'Timestamp']
const PROFILES: SourceDefinition['generatorProfile'][] = ['trades', 'quotes', 'orders', 'generic']

const inputCls =
  'w-full rounded-md border border-[var(--sf-border)] bg-[var(--sf-bg)] px-2.5 py-1.5 text-sm text-gray-200 focus:border-[var(--sf-accent)] focus:outline-none'
const labelCls = 'mb-1 block text-xs font-medium uppercase tracking-wide text-gray-500'

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'number') return Number.isInteger(v) ? v.toString() : v.toFixed(3)
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  return String(v)
}

function SourceTape({ name }: { name: string }) {
  const events = useSourceTape(name)
  return (
    <div className="h-32 overflow-hidden rounded-lg border border-[var(--sf-border)] bg-[var(--sf-bg)] p-2 font-mono text-[11px] leading-5 text-gray-500">
      {events.length === 0 ? (
        <p className="text-gray-600">Waiting for live events…</p>
      ) : (
        events.map((row, i) => (
          <div key={i} className={`truncate ${i === 0 ? 'text-gray-300' : ''}`}>
            {Object.entries(row)
              .map(([k, v]) => `${k}=${formatCell(v)}`)
              .join('  ')}
          </div>
        ))
      )}
    </div>
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
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 px-4">
      <form
        onSubmit={handleSubmit}
        className="flex max-h-[90vh] w-full max-w-lg flex-col gap-4 overflow-y-auto rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-5"
      >
        <h3 className="text-sm font-semibold text-gray-100">{isEdit ? `Edit ${form.name}` : 'New source'}</h3>

        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className={labelCls}>Name</label>
            <input
              className={inputCls}
              value={form.name}
              disabled={isEdit}
              onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
              placeholder="trades"
            />
          </div>
          <div>
            <label className={labelCls}>Profile</label>
            <select
              className={inputCls}
              value={form.generatorProfile}
              onChange={(e) => setForm((f) => ({ ...f, generatorProfile: e.target.value as SourceDefinition['generatorProfile'] }))}
            >
              {PROFILES.map((p) => (
                <option key={p} value={p}>
                  {p}
                </option>
              ))}
            </select>
          </div>
        </div>

        <div>
          <label className={labelCls}>Description</label>
          <input
            className={inputCls}
            value={form.description}
            onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
          />
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className={labelCls}>Events / sec</label>
            <input
              type="number"
              min={0}
              step="0.1"
              className={inputCls}
              value={form.eventsPerSecond}
              onChange={(e) => setForm((f) => ({ ...f, eventsPerSecond: Number(e.target.value) || 0 }))}
            />
          </div>
          <div className="flex items-end pb-1.5">
            <label className="flex items-center gap-2 text-sm text-gray-300">
              <input
                type="checkbox"
                checked={form.enabled}
                onChange={(e) => setForm((f) => ({ ...f, enabled: e.target.checked }))}
                className="h-4 w-4 rounded border-[var(--sf-border)] bg-[var(--sf-bg)] accent-[var(--sf-accent)]"
              />
              Enabled
            </label>
          </div>
        </div>

        <div>
          <div className="mb-2 flex items-center justify-between">
            <label className={labelCls}>Fields</label>
            <button
              type="button"
              onClick={() => setForm((f) => ({ ...f, fields: [...f.fields, { name: '', type: 'String' }] }))}
              className="inline-flex items-center gap-1 text-xs font-medium text-[var(--sf-accent)] hover:opacity-80"
            >
              <PlusIcon className="h-3.5 w-3.5" /> Add field
            </button>
          </div>
          <div className="flex flex-col gap-2">
            {form.fields.map((f, i) => (
              <div key={i} className="flex items-center gap-2">
                <input
                  className={inputCls}
                  placeholder="field name"
                  value={f.name}
                  onChange={(e) => updateField(i, { name: e.target.value })}
                />
                <select className={`${inputCls} w-32 shrink-0`} value={f.type} onChange={(e) => updateField(i, { type: e.target.value as FieldType })}>
                  {FIELD_TYPES.map((t) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </select>
                <button
                  type="button"
                  onClick={() => setForm((f2) => ({ ...f2, fields: f2.fields.filter((_, idx) => idx !== i) }))}
                  className="rounded-md p-1.5 text-gray-500 hover:bg-white/5 hover:text-[var(--sf-bad)]"
                >
                  <TrashIcon className="h-4 w-4" />
                </button>
              </div>
            ))}
          </div>
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
    } finally {
      load()
    }
  }

  async function confirmDelete() {
    if (!pendingDelete) return
    const name = pendingDelete.name
    setPendingDelete(null)
    await sourcesApi.remove(name)
    load()
  }

  return (
    <div>
      <Topbar
        title="Sources"
        subtitle="Synthetic event generators feeding your pipelines"
        action={
          <RoleGate min="Editor">
            <button
              onClick={() => setModal({ mode: 'create' })}
              className="flex items-center gap-1.5 rounded-lg bg-gradient-to-r from-sky-400 to-violet-500 px-4 py-2 text-sm font-semibold text-slate-950 transition-opacity hover:opacity-90"
            >
              <PlusIcon className="h-4 w-4" /> New source
            </button>
          </RoleGate>
        }
      />

      <div className="p-8">
        {sources === null ? (
          <SkeletonGrid />
        ) : sources.length === 0 ? (
          <EmptyState title="No sources configured" hint="Add a source to start generating live events for your pipelines." />
        ) : (
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            {sources.map((s) => (
              <div key={s.name} className="flex flex-col gap-3 rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-5">
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <h3 className="text-sm font-semibold text-gray-100">{s.name}</h3>
                    <p className="mt-0.5 text-xs text-gray-500">{s.description || 'No description'}</p>
                  </div>
                  <span className="rounded-full border border-[var(--sf-border)] px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-gray-400">
                    {s.generatorProfile}
                  </span>
                </div>

                <div className="flex items-center justify-between text-xs text-gray-400">
                  <span>
                    rate <span className="font-mono text-gray-200">{s.eventsPerSecond}</span>/s
                  </span>
                  <RoleGate min="Editor">
                    <label className="flex items-center gap-1.5">
                      <input
                        type="checkbox"
                        checked={s.enabled}
                        onChange={() => void toggleEnabled(s)}
                        className="h-3.5 w-3.5 rounded border-[var(--sf-border)] bg-[var(--sf-bg)] accent-[var(--sf-accent)]"
                      />
                      Enabled
                    </label>
                  </RoleGate>
                </div>

                <div className="overflow-hidden rounded-lg border border-[var(--sf-border)]">
                  <table className="w-full text-left text-xs">
                    <thead className="bg-[var(--sf-bg)]/60">
                      <tr>
                        <th className="px-3 py-1.5 font-medium text-gray-500">Field</th>
                        <th className="px-3 py-1.5 font-medium text-gray-500">Type</th>
                      </tr>
                    </thead>
                    <tbody>
                      {s.fields.map((f) => (
                        <tr key={f.name} className="border-t border-[var(--sf-border)]/60">
                          <td className="px-3 py-1 font-mono text-gray-300">{f.name}</td>
                          <td className="px-3 py-1 text-gray-500">{f.type}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div>
                  <p className="mb-1 text-[10px] font-medium uppercase tracking-wide text-gray-500">Live tape</p>
                  <SourceTape name={s.name} />
                </div>

                <RoleGate min="Editor">
                  <div className="flex justify-end gap-1">
                    <button
                      onClick={() => setModal({ mode: 'edit', source: s })}
                      className="flex items-center gap-1 rounded-md px-2 py-1 text-xs text-gray-400 hover:bg-white/5 hover:text-gray-200"
                    >
                      <EditIcon className="h-3.5 w-3.5" /> Edit
                    </button>
                    <button
                      onClick={() => setPendingDelete(s)}
                      className="flex items-center gap-1 rounded-md px-2 py-1 text-xs text-gray-400 hover:bg-white/5 hover:text-[var(--sf-bad)]"
                    >
                      <TrashIcon className="h-3.5 w-3.5" /> Delete
                    </button>
                  </div>
                </RoleGate>
              </div>
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

      {pendingDelete && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 px-4">
          <div className="w-full max-w-sm rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-5">
            <h3 className="text-sm font-semibold text-gray-100">Delete source?</h3>
            <p className="mt-2 text-sm text-gray-400">
              This removes <span className="font-medium text-gray-200">{pendingDelete.name}</span>. Pipelines referencing it will fail.
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
