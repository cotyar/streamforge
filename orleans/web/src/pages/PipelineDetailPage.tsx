import { useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { pipelinesApi } from '../api/pipelines'
import type { PipelineDefinition, SqlDiagnostic } from '../api/types'
import { useAuth } from '../api/auth'
import { usePipelineResults } from '../hooks/usePipelineResults'
import { useMetricsStream } from '../hooks/useMetricsStream'
import { Topbar } from '../components/Topbar'
import { StatusBadge } from '../components/StatusBadge'
import { SqlEditor } from '../components/SqlEditor'
import { PipelineBuilder } from '../components/PipelineBuilder'
import { ResultsTable } from '../components/ResultsTable'
import { MetricsBar } from '../components/MetricsBar'
import { LiveChart } from '../components/LiveChart'
import { RoleGate } from '../components/RoleGate'
import { Skeleton } from '../components/Skeleton'
import { CheckIcon, ErrorIcon, PlayIcon, TrashIcon, WarnIcon } from '../components/icons'
import type { BuilderState } from '../builder/types'
import { emptyBuilderState } from '../builder/types'
import { builderStateToSql } from '../builder/sqlgen'

type Mode = 'sql' | 'builder'

export function PipelineDetailPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const { hasRole } = useAuth()
  const isNew = id === 'new' || !id
  const canEdit = hasRole('Editor')

  const [pipeline, setPipeline] = useState<PipelineDefinition | null>(null)
  const [loading, setLoading] = useState(!isNew)
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [sql, setSql] = useState('')
  const [mode, setMode] = useState<Mode>('sql')
  const [builderState, setBuilderState] = useState<BuilderState>(emptyBuilderState())

  const [diagnostics, setDiagnostics] = useState<SqlDiagnostic[] | null>(null)
  const [planSummary, setPlanSummary] = useState<string | null>(null)
  const [validating, setValidating] = useState(false)
  const [saving, setSaving] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)

  useEffect(() => {
    if (isNew) {
      setPipeline(null)
      setName('')
      setDescription('')
      setSql('')
      setBuilderState(emptyBuilderState())
      setLoading(false)
      return
    }
    setLoading(true)
    pipelinesApi
      .get(id!)
      .then((p) => {
        setPipeline(p)
        setName(p.name)
        setDescription(p.description)
        setSql(p.sql)
      })
      .finally(() => setLoading(false))
  }, [id, isNew])

  const effectiveSql = mode === 'builder' ? builderStateToSql(builderState) : sql

  useEffect(() => {
    if (!effectiveSql.trim()) {
      setDiagnostics(null)
      setPlanSummary(null)
      return
    }
    setValidating(true)
    const timer = setTimeout(() => {
      pipelinesApi
        .validate({ sql: effectiveSql })
        .then((res) => {
          setDiagnostics(res.diagnostics)
          setPlanSummary(res.ok ? res.planSummary : null)
        })
        .catch(() => {
          setDiagnostics(null)
          setPlanSummary(null)
        })
        .finally(() => setValidating(false))
    }, 500)
    return () => clearTimeout(timer)
  }, [effectiveSql])

  function switchToSql() {
    if (mode === 'builder') setSql(builderStateToSql(builderState))
    setMode('sql')
  }

  async function handleSave(startAfter: boolean) {
    setFormError(null)
    if (!name.trim()) {
      setFormError('Name is required.')
      return
    }
    setSaving(true)
    try {
      let saved: PipelineDefinition
      if (isNew) {
        saved = await pipelinesApi.create({ name: name.trim(), description, sql: effectiveSql })
      } else {
        saved = await pipelinesApi.update(id!, { name: name.trim(), description, sql: effectiveSql })
      }
      if (startAfter && saved.status !== 'Running') {
        saved = await pipelinesApi.start(saved.id)
      }
      setPipeline(saved)
      if (isNew) navigate(`/pipelines/${saved.id}`, { replace: true })
    } catch (err) {
      setFormError(err instanceof Error ? err.message : 'Failed to save pipeline.')
    } finally {
      setSaving(false)
    }
  }

  async function handleDelete() {
    if (!pipeline) return
    setSaving(true)
    try {
      await pipelinesApi.remove(pipeline.id)
      navigate('/pipelines', { replace: true })
    } catch (err) {
      setFormError(err instanceof Error ? err.message : 'Failed to delete pipeline.')
      setSaving(false)
    }
  }

  const { rows, status: liveStatus } = usePipelineResults(isNew ? undefined : id)
  const metrics = useMetricsStream()
  const currentMetrics = pipeline ? (metrics[pipeline.id] ?? null) : null
  const currentStatus = liveStatus ?? pipeline?.status ?? 'Stopped'

  if (loading) {
    return (
      <div className="p-8">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="mt-4 h-96 w-full" />
      </div>
    )
  }

  return (
    <div>
      <Topbar
        title={isNew ? 'New pipeline' : name || 'Pipeline'}
        subtitle={isNew ? 'Define a streaming SQL job' : pipeline?.id}
        action={!isNew && <StatusBadge status={currentStatus} />}
      />

      <div className="grid grid-cols-1 gap-6 p-8 xl:grid-cols-2">
        {/* LEFT */}
        <div className="flex flex-col gap-4">
          <div className="rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-5">
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div>
                <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-gray-500">Name</label>
                <input
                  value={name}
                  disabled={!canEdit}
                  onChange={(e) => setName(e.target.value)}
                  className="w-full rounded-md border border-[var(--sf-border)] bg-[var(--sf-bg)] px-3 py-2 text-sm text-gray-100 outline-none focus:border-[var(--sf-accent)] disabled:opacity-60"
                  placeholder="vwap-by-symbol"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-gray-500">Description</label>
                <input
                  value={description}
                  disabled={!canEdit}
                  onChange={(e) => setDescription(e.target.value)}
                  className="w-full rounded-md border border-[var(--sf-border)] bg-[var(--sf-bg)] px-3 py-2 text-sm text-gray-100 outline-none focus:border-[var(--sf-accent)] disabled:opacity-60"
                  placeholder="Volume-weighted average price per symbol"
                />
              </div>
            </div>
          </div>

          <div className="flex w-fit gap-1 rounded-lg border border-[var(--sf-border)] bg-[var(--sf-panel)] p-1">
            <button
              onClick={switchToSql}
              className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                mode === 'sql' ? 'bg-[var(--sf-accent)]/15 text-[var(--sf-accent)]' : 'text-gray-400 hover:text-gray-200'
              }`}
            >
              SQL
            </button>
            <button
              onClick={() => setMode('builder')}
              className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                mode === 'builder' ? 'bg-[var(--sf-accent)]/15 text-[var(--sf-accent)]' : 'text-gray-400 hover:text-gray-200'
              }`}
            >
              Builder
            </button>
          </div>

          {mode === 'sql' ? (
            <SqlEditor value={sql} onChange={setSql} diagnostics={diagnostics ?? []} readOnly={!canEdit} />
          ) : (
            <PipelineBuilder state={builderState} onChange={setBuilderState} />
          )}

          <div className="rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-4">
            <h3 className="mb-2 text-xs font-medium uppercase tracking-wide text-gray-500">Validation</h3>
            {validating ? (
              <p className="text-sm text-gray-500">Validating…</p>
            ) : diagnostics === null ? (
              <p className="text-sm text-gray-500">Start typing SQL to validate.</p>
            ) : diagnostics.length === 0 ? (
              <p className="flex items-center gap-1.5 text-sm text-[var(--sf-good)]">
                <CheckIcon className="h-4 w-4" /> Valid{planSummary ? ` — ${planSummary}` : ''}
              </p>
            ) : (
              <ul className="flex flex-col gap-1.5">
                {diagnostics.map((d, i) => (
                  <li
                    key={i}
                    title={d.message}
                    className={`flex items-start gap-2 text-xs ${d.severity === 'Error' ? 'text-[var(--sf-bad)]' : 'text-[var(--sf-warn)]'}`}
                  >
                    {d.severity === 'Error' ? (
                      <ErrorIcon className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                    ) : (
                      <WarnIcon className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                    )}
                    <span>
                      <span className="font-mono text-gray-500">
                        {d.line}:{d.column}
                      </span>{' '}
                      {d.message}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {formError && (
            <p className="rounded-md border border-[var(--sf-bad)]/30 bg-[var(--sf-bad)]/10 px-3 py-2 text-sm text-[var(--sf-bad)]">
              {formError}
            </p>
          )}

          <RoleGate min="Editor">
            <div className="flex flex-wrap gap-2">
              <button
                onClick={() => void handleSave(false)}
                disabled={saving}
                className="rounded-lg bg-gradient-to-r from-sky-400 to-violet-500 px-4 py-2 text-sm font-semibold text-slate-950 transition-opacity hover:opacity-90 disabled:opacity-50"
              >
                {saving ? 'Saving…' : 'Save'}
              </button>
              <button
                onClick={() => void handleSave(true)}
                disabled={saving}
                className="flex items-center gap-1.5 rounded-lg border border-[var(--sf-accent)]/40 px-4 py-2 text-sm font-semibold text-[var(--sf-accent)] transition-colors hover:bg-[var(--sf-accent)]/10 disabled:opacity-50"
              >
                <PlayIcon className="h-4 w-4" /> Save & start
              </button>
              {!isNew && (
                <button
                  onClick={() => setConfirmDelete(true)}
                  disabled={saving}
                  className="ml-auto flex items-center gap-1.5 rounded-lg border border-[var(--sf-border)] px-4 py-2 text-sm font-medium text-gray-400 transition-colors hover:border-[var(--sf-bad)]/40 hover:text-[var(--sf-bad)]"
                >
                  <TrashIcon className="h-4 w-4" /> Delete
                </button>
              )}
            </div>
          </RoleGate>
        </div>

        {/* RIGHT */}
        <div className="flex flex-col gap-4">
          {isNew ? (
            <div className="flex h-full items-center justify-center rounded-xl border border-dashed border-[var(--sf-border)] p-10 text-center text-sm text-gray-500">
              Save the pipeline to see live results here.
            </div>
          ) : (
            <>
              <div className="rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-4">
                <MetricsBar metrics={currentMetrics} />
              </div>
              <div className="rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-4">
                <LiveChart rows={rows} />
              </div>
              <div className="min-h-[20rem] flex-1 overflow-hidden rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)]">
                <ResultsTable rows={rows} />
              </div>
            </>
          )}
        </div>
      </div>

      {confirmDelete && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 px-4">
          <div className="w-full max-w-sm rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-5">
            <h3 className="text-sm font-semibold text-gray-100">Delete pipeline?</h3>
            <p className="mt-2 text-sm text-gray-400">
              This permanently removes <span className="font-medium text-gray-200">{name}</span> and its results.
            </p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                onClick={() => setConfirmDelete(false)}
                className="rounded-md border border-[var(--sf-border)] px-3 py-1.5 text-sm text-gray-300 hover:bg-white/5"
              >
                Cancel
              </button>
              <button
                onClick={() => {
                  setConfirmDelete(false)
                  void handleDelete()
                }}
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
