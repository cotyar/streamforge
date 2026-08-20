import { useCallback, useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Play, Square, Rocket } from 'lucide-react'
import { toast } from 'sonner'
import { pipelinesApi } from '../api/pipelines'
import { sourcesApi } from '../api/sources'
import { tablesApi } from '../api/tables'
import type { PipelineDefinition, SourceDefinition, TableDefinition } from '../api/types'
import { useMetricsStream } from '../hooks/useMetricsStream'
import { Topbar } from '../components/Topbar'
import { StatusBadge } from '../components/StatusBadge'
import { Sparkline } from '../components/Sparkline'
import { RoleGate } from '../components/RoleGate'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'

function StatTile({ label, value, onClick }: { label: string; value: string; onClick?: () => void }) {
  return (
    <Card
      onClick={onClick}
      className={onClick ? 'cursor-pointer transition-colors hover:ring-primary/40' : undefined}
    >
      <CardContent>
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{label}</p>
        <p className="mt-1.5 text-2xl font-semibold text-foreground">{value}</p>
      </CardContent>
    </Card>
  )
}

export function DashboardPage() {
  const navigate = useNavigate()
  const [pipelines, setPipelines] = useState<PipelineDefinition[] | null>(null)
  const [sources, setSources] = useState<SourceDefinition[] | null>(null)
  const [tables, setTables] = useState<TableDefinition[] | null>(null)
  const [busyIds, setBusyIds] = useState<Set<string>>(new Set())
  const metrics = useMetricsStream()
  const [history, setHistory] = useState<Record<string, number[]>>({})

  const load = useCallback(() => {
    pipelinesApi.list().then(setPipelines).catch(() => setPipelines([]))
    sourcesApi.list().then(setSources).catch(() => setSources([]))
    tablesApi.list().then(setTables).catch(() => setTables([]))
  }, [])

  useEffect(() => {
    load()
  }, [load])

  // Plan 021 wave 2 (021-F): the hub's "metrics" SignalR group is deliberately CLUSTER-WIDE, not
  // qualified by environment (shared/StreamForge.Api/Hubs/StreamHub.cs's class remarks — it names no
  // entity, so there is nothing to qualify it with). That means `metrics` here can and does carry
  // pipelineIds that belong to a DIFFERENT environment than the one this page is currently showing —
  // confirmed live: switching to a freshly created, empty "staging" environment still pushed a nonzero
  // rowsOutPerSec from "default"'s running pipelines. Every OTHER read of `metrics` in this file already
  // indexes by an id drawn from `pipelines` (the environment-scoped REST list), so it self-filters; only
  // this set exists to keep the two aggregate computations below (totalRowsPerSec, and the sparkline
  // history feeding it) from summing/retaining rows for pipelines this environment cannot even list.
  const pipelineIds = useMemo(() => new Set((pipelines ?? []).map((p) => p.id)), [pipelines])

  useEffect(() => {
    setHistory((prev) => {
      let changed = false
      const next = { ...prev }
      for (const m of Object.values(metrics)) {
        if (!pipelineIds.has(m.pipelineId)) continue
        const arr = next[m.pipelineId] ?? []
        if (arr[arr.length - 1] !== m.rowsOutPerSec) {
          next[m.pipelineId] = [...arr, m.rowsOutPerSec].slice(-30)
          changed = true
        }
      }
      return changed ? next : prev
    })
  }, [metrics, pipelineIds])

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
    } catch (err) {
      toast.error(err instanceof Error ? err.message : `Failed to ${goingToStart ? 'start' : 'stop'} pipeline.`)
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
  const totalRowsPerSec = Object.values(metrics)
    .filter((m) => pipelineIds.has(m.pipelineId))
    .reduce((sum, m) => sum + m.rowsOutPerSec, 0)
  const totalTables = tables?.length ?? 0
  const runningTables = tables?.filter((t) => t.status === 'Running').length ?? 0

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
              <StatTile label="Tables" value={`${totalTables} (${runningTables} running)`} onClick={() => navigate('/tables')} />
            </>
          )}
        </div>

        <div>
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-muted-foreground">Pipelines</h2>
          {pipelines === null ? (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {Array.from({ length: 6 }).map((_, i) => (
                <Card key={i}>
                  <CardContent className="flex flex-col gap-3">
                    <Skeleton className="h-4 w-2/3" />
                    <Skeleton className="h-3 w-1/3" />
                    <Skeleton className="h-8 w-full" />
                  </CardContent>
                </Card>
              ))}
            </div>
          ) : pipelines.length === 0 ? (
            <Empty className="border border-dashed">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <Rocket />
                </EmptyMedia>
                <EmptyTitle>No pipelines yet</EmptyTitle>
                <EmptyDescription>Create your first streaming pipeline to see live metrics here.</EmptyDescription>
              </EmptyHeader>
              <EmptyContent>
                <RoleGate min="Editor">
                  <Button onClick={() => navigate('/pipelines/new')}>New pipeline</Button>
                </RoleGate>
              </EmptyContent>
            </Empty>
          ) : (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {pipelines.map((p) => {
                const m = metrics[p.id]
                const rowHistory = history[p.id] ?? []
                const busy = busyIds.has(p.id)
                return (
                  <Card
                    key={p.id}
                    onClick={() => navigate(`/pipelines/${p.id}`)}
                    className="group cursor-pointer transition-colors hover:ring-primary/40"
                  >
                    <CardHeader>
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0">
                          <CardTitle className="truncate group-hover:text-foreground">{p.name}</CardTitle>
                          <p className="mt-0.5 truncate text-xs text-muted-foreground">{p.description || 'No description'}</p>
                        </div>
                        <StatusBadge status={p.status} />
                      </div>
                    </CardHeader>

                    <CardContent className="flex flex-col gap-3">
                      <div className="flex items-center justify-between gap-3">
                        <div className="flex gap-4 text-xs text-muted-foreground">
                          <span>
                            in <span className="font-mono text-foreground">{(m?.eventsInPerSec ?? 0).toFixed(1)}</span>/s
                          </span>
                          <span>
                            out <span className="font-mono text-foreground">{(m?.rowsOutPerSec ?? 0).toFixed(1)}</span>/s
                          </span>
                        </div>
                        <Sparkline values={rowHistory.length ? rowHistory : [0, 0]} width={90} height={28} />
                      </div>

                      <RoleGate min="Editor">
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={(e) => {
                            e.stopPropagation()
                            void toggle(p)
                          }}
                          disabled={busy}
                          className="self-start"
                        >
                          {p.status === 'Running' ? (
                            <>
                              <Square data-icon="inline-start" /> Stop
                            </>
                          ) : (
                            <>
                              <Play data-icon="inline-start" /> Start
                            </>
                          )}
                        </Button>
                      </RoleGate>
                    </CardContent>
                  </Card>
                )
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
