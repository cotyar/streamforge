import { useCallback, useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { pipelinesApi } from '../api/pipelines'
import { sourcesApi } from '../api/sources'
import type { PipelineDefinition, SourceDefinition } from '../api/types'
import { useMetricsStream } from '../hooks/useMetricsStream'
import { Topbar } from '../components/Topbar'
import { StatusBadge } from '../components/StatusBadge'
import { Sparkline } from '../components/Sparkline'
import { RoleGate } from '../components/RoleGate'
import { EmptyState } from '../components/EmptyState'
import { SkeletonGrid, Skeleton } from '../components/Skeleton'
import { PlayIcon, StopIcon } from '../components/icons'

function StatTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-gray-500">{label}</p>
      <p className="mt-1.5 text-2xl font-semibold text-white">{value}</p>
    </div>
  )
}

export function DashboardPage() {
  const navigate = useNavigate()
  const [pipelines, setPipelines] = useState<PipelineDefinition[] | null>(null)
  const [sources, setSources] = useState<SourceDefinition[] | null>(null)
  const [busyIds, setBusyIds] = useState<Set<string>>(new Set())
  const metrics = useMetricsStream()
  const [history, setHistory] = useState<Record<string, number[]>>({})

  const load = useCallback(() => {
    pipelinesApi.list().then(setPipelines).catch(() => setPipelines([]))
    sourcesApi.list().then(setSources).catch(() => setSources([]))
  }, [])

  useEffect(() => {
    load()
  }, [load])

  useEffect(() => {
    setHistory((prev) => {
      let changed = false
      const next = { ...prev }
      for (const m of Object.values(metrics)) {
        const arr = next[m.pipelineId] ?? []
        if (arr[arr.length - 1] !== m.rowsOutPerSec) {
          next[m.pipelineId] = [...arr, m.rowsOutPerSec].slice(-30)
          changed = true
        }
      }
      return changed ? next : prev
    })
  }, [metrics])

  async function toggle(p: PipelineDefinition) {
    setBusyIds((prev) => new Set(prev).add(p.id))
    const goingToStart = p.status !== 'Running'
    setPipelines((prev) =>
      prev
        ? prev.map((row) => (row.id === p.id ? { ...row, status: goingToStart ? 'Running' : 'Stopped' } : row))
        : prev,
    )
    try {
      if (goingToStart) await pipelinesApi.start(p.id)
      else await pipelinesApi.stop(p.id)
      load()
    } catch {
      load()
    } finally {
      setBusyIds((prev) => {
        const next = new Set(prev)
        next.delete(p.id)
        return next
      })
    }
  }

  const totalPipelines = pipelines?.length ?? 0
  const runningCount = pipelines?.filter((p) => p.status === 'Running').length ?? 0
  const totalSources = sources?.length ?? 0
  const totalRowsPerSec = Object.values(metrics).reduce((sum, m) => sum + m.rowsOutPerSec, 0)

  return (
    <div>
      <Topbar title="Dashboard" subtitle="Live overview of your streaming pipelines" />
      <div className="flex flex-col gap-6 p-8">
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
          {pipelines === null ? (
            Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="h-20" />)
          ) : (
            <>
              <StatTile label="Total pipelines" value={totalPipelines.toString()} />
              <StatTile label="Running" value={runningCount.toString()} />
              <StatTile label="Sources" value={totalSources.toString()} />
              <StatTile label="Rows/s (live)" value={totalRowsPerSec.toFixed(1)} />
            </>
          )}
        </div>

        <div>
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-gray-500">Pipelines</h2>
          {pipelines === null ? (
            <SkeletonGrid />
          ) : pipelines.length === 0 ? (
            <EmptyState
              title="No pipelines yet"
              hint="Create your first streaming pipeline to see live metrics here."
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
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {pipelines.map((p) => {
                const m = metrics[p.id]
                const rowHistory = history[p.id] ?? []
                const busy = busyIds.has(p.id)
                return (
                  <div
                    key={p.id}
                    onClick={() => navigate(`/pipelines/${p.id}`)}
                    className="group flex cursor-pointer flex-col gap-3 rounded-xl border border-[var(--sf-border)] bg-[var(--sf-panel)] p-5 transition-colors hover:border-[var(--sf-accent)]/40"
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="min-w-0">
                        <h3 className="truncate text-sm font-semibold text-gray-100 group-hover:text-white">{p.name}</h3>
                        <p className="mt-0.5 truncate text-xs text-gray-500">{p.description || 'No description'}</p>
                      </div>
                      <StatusBadge status={p.status} />
                    </div>

                    <div className="flex items-center justify-between gap-3">
                      <div className="flex gap-4 text-xs text-gray-400">
                        <span>
                          in <span className="font-mono text-gray-200">{(m?.eventsInPerSec ?? 0).toFixed(1)}</span>/s
                        </span>
                        <span>
                          out <span className="font-mono text-gray-200">{(m?.rowsOutPerSec ?? 0).toFixed(1)}</span>/s
                        </span>
                      </div>
                      <Sparkline values={rowHistory.length ? rowHistory : [0, 0]} width={90} height={28} />
                    </div>

                    <RoleGate min="Editor">
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          void toggle(p)
                        }}
                        disabled={busy}
                        className="mt-1 flex items-center justify-center gap-1.5 self-start rounded-md border border-[var(--sf-border)] px-3 py-1.5 text-xs font-medium text-gray-300 transition-colors hover:border-[var(--sf-accent)] hover:text-[var(--sf-accent)] disabled:cursor-wait disabled:opacity-50"
                      >
                        {p.status === 'Running' ? (
                          <>
                            <StopIcon className="h-3.5 w-3.5" /> Stop
                          </>
                        ) : (
                          <>
                            <PlayIcon className="h-3.5 w-3.5" /> Start
                          </>
                        )}
                      </button>
                    </RoleGate>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
