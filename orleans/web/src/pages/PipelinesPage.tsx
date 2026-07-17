import { useCallback, useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { pipelinesApi } from '../api/pipelines'
import type { PipelineDefinition } from '../api/types'
import { extractSourcesFromSql } from '../lib/sqlSources'
import { Topbar } from '../components/Topbar'
import { StatusBadge } from '../components/StatusBadge'
import { RoleGate } from '../components/RoleGate'
import { EmptyState } from '../components/EmptyState'
import { Skeleton } from '../components/Skeleton'
import { EditIcon, EyeIcon, PlayIcon, PlusIcon, StopIcon, TrashIcon } from '../components/icons'

function formatDate(ms: number): string {
  return new Date(ms).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export function PipelinesPage() {
  const navigate = useNavigate()
  const [pipelines, setPipelines] = useState<PipelineDefinition[] | null>(null)
  const [busyIds, setBusyIds] = useState<Set<string>>(new Set())
  const [pendingDelete, setPendingDelete] = useState<PipelineDefinition | null>(null)

  const load = useCallback(() => {
    pipelinesApi.list().then(setPipelines).catch(() => setPipelines([]))
  }, [])

  useEffect(() => {
    load()
  }, [load])

  function withBusy(id: string, fn: () => Promise<unknown>) {
    setBusyIds((prev) => new Set(prev).add(id))
    fn()
      .then(load)
      .finally(() =>
        setBusyIds((prev) => {
          const next = new Set(prev)
          next.delete(id)
          return next
        }),
      )
  }

  async function confirmDelete() {
    if (!pendingDelete) return
    const id = pendingDelete.id
    setPendingDelete(null)
    setBusyIds((prev) => new Set(prev).add(id))
    try {
      await pipelinesApi.remove(id)
      load()
    } finally {
      setBusyIds((prev) => {
        const next = new Set(prev)
        next.delete(id)
        return next
      })
    }
  }

  return (
    <div>
      <Topbar
        title="Pipelines"
        subtitle="Streaming SQL jobs running against your live sources"
        action={
          <RoleGate min="Editor">
            <button
              onClick={() => navigate('/pipelines/new')}
              className="flex items-center gap-1.5 rounded-lg bg-gradient-to-r from-sky-400 to-violet-500 px-4 py-2 text-sm font-semibold text-slate-950 transition-opacity hover:opacity-90"
            >
              <PlusIcon className="h-4 w-4" /> New pipeline
            </button>
          </RoleGate>
        }
      />

      <div className="p-8">
        {pipelines === null ? (
          <div className="flex flex-col gap-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : pipelines.length === 0 ? (
          <EmptyState
            title="Create your first streaming pipeline"
            hint="Write streaming SQL or use the visual builder to join, filter, and window your live sources."
            action={
              <RoleGate min="Editor">
                <button
                  onClick={() => navigate('/pipelines/new')}
                  className="rounded-lg bg-[var(--sf-accent)]/15 px-4 py-2 text-sm font-medium text-[var(--sf-accent)] transition-colors hover:bg-[var(--sf-accent)]/25"
                >
                  New pipeline
                </button>
              </RoleGate>
            }
          />
        ) : (
          <div className="overflow-hidden rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)]">
            <table className="w-full border-collapse text-left text-sm">
              <thead>
                <tr className="border-b border-[var(--sf-border)] text-xs uppercase tracking-wide text-gray-500">
                  <th className="px-4 py-3 font-medium">Name</th>
                  <th className="px-4 py-3 font-medium">Status</th>
                  <th className="px-4 py-3 font-medium">Sources</th>
                  <th className="px-4 py-3 font-medium">Description</th>
                  <th className="px-4 py-3 font-medium">Updated</th>
                  <th className="px-4 py-3 font-medium text-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {pipelines.map((p) => {
                  const busy = busyIds.has(p.id)
                  const sources = extractSourcesFromSql(p.sql)
                  return (
                    <tr key={p.id} className="border-b border-[var(--sf-border)]/60 last:border-0 hover:bg-white/[0.02]">
                      <td className="px-4 py-3">
                        <Link to={`/pipelines/${p.id}`} className="font-medium text-gray-100 hover:text-[var(--sf-accent)]">
                          {p.name}
                        </Link>
                        {p.error && <p className="mt-0.5 max-w-xs truncate text-xs text-[var(--sf-bad)]">{p.error}</p>}
                      </td>
                      <td className="px-4 py-3">
                        <StatusBadge status={p.status} />
                      </td>
                      <td className="px-4 py-3 text-xs text-gray-400">
                        {sources.length > 0 ? sources.join(', ') : '—'}
                      </td>
                      <td className="max-w-xs truncate px-4 py-3 text-gray-400">{p.description || '—'}</td>
                      <td className="px-4 py-3 text-xs text-gray-500">{formatDate(p.updatedAtMs)}</td>
                      <td className="px-4 py-3">
                        <div className="flex items-center justify-end gap-1">
                          <Link
                            to={`/pipelines/${p.id}`}
                            title="View"
                            className="rounded-md p-1.5 text-gray-400 transition-colors hover:bg-white/5 hover:text-gray-200"
                          >
                            <EyeIcon className="h-4 w-4" />
                          </Link>
                          <RoleGate min="Editor">
                            <>
                              <button
                                title={p.status === 'Running' ? 'Stop' : 'Start'}
                                disabled={busy}
                                onClick={() =>
                                  withBusy(p.id, () => (p.status === 'Running' ? pipelinesApi.stop(p.id) : pipelinesApi.start(p.id)))
                                }
                                className="rounded-md p-1.5 text-gray-400 transition-colors hover:bg-white/5 hover:text-[var(--sf-accent)] disabled:opacity-40"
                              >
                                {p.status === 'Running' ? <StopIcon className="h-4 w-4" /> : <PlayIcon className="h-4 w-4" />}
                              </button>
                              <Link
                                to={`/pipelines/${p.id}`}
                                title="Edit"
                                className="rounded-md p-1.5 text-gray-400 transition-colors hover:bg-white/5 hover:text-gray-200"
                              >
                                <EditIcon className="h-4 w-4" />
                              </Link>
                              <button
                                title="Delete"
                                disabled={busy}
                                onClick={() => setPendingDelete(p)}
                                className="rounded-md p-1.5 text-gray-400 transition-colors hover:bg-white/5 hover:text-[var(--sf-bad)] disabled:opacity-40"
                              >
                                <TrashIcon className="h-4 w-4" />
                              </button>
                            </>
                          </RoleGate>
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

      {pendingDelete && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 px-4">
          <div className="w-full max-w-sm rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-5">
            <h3 className="text-sm font-semibold text-gray-100">Delete pipeline?</h3>
            <p className="mt-2 text-sm text-gray-400">
              This permanently removes <span className="font-medium text-gray-200">{pendingDelete.name}</span> and its results.
            </p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                onClick={() => setPendingDelete(null)}
                className="rounded-md border border-[var(--sf-border)] px-3 py-1.5 text-sm text-gray-300 hover:bg-white/5"
              >
                Cancel
              </button>
              <button
                onClick={confirmDelete}
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
