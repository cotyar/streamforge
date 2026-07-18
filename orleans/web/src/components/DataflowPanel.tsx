import { useEffect, useMemo, useRef } from 'react'
import { ChevronRight } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { Empty, EmptyDescription, EmptyHeader } from '@/components/ui/empty'
import type { TableDefinition, TableMetrics, TablePartitionMetrics } from '../api/types'

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5 rounded-lg border border-border bg-background/60 px-3 py-2">
      <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className="font-mono text-sm font-semibold text-foreground">{value}</span>
    </div>
  )
}

function cellKey(stageId: number, partition: number): string {
  return `${stageId}:${partition}`
}

/** Frontier-lag styling for one partition cell, relative to the fastest partition anywhere in the
 * graph (maxFrontier — every stage shares the same epoch clock, so this is comparable table-wide,
 * not just within a stage column). 0 lag = caught up (primary/fresh tones); a small lag reads
 * neutral; a bigger lag or "never advanced" (-1) reads as warning — existing status-token classes
 * only, no raw hex. */
function lagClasses(frontierEpoch: number, maxFrontier: number): string {
  if (frontierEpoch < 0) return 'border-warning/40 bg-warning/10 text-warning'
  const lag = maxFrontier - frontierEpoch
  if (lag <= 0) return 'border-primary/30 bg-primary/10 text-primary'
  if (lag <= 2) return 'border-border bg-background/60 text-foreground'
  return 'border-warning/40 bg-warning/10 text-warning'
}

/**
 * Plan 003 M5: compact stage-graph visualization for a partitioned table's dataflow — stages as
 * columns, partitions as rows of cells, colored by frontier lag. Shown only once the table is
 * parallelized (parallelism >= 2); a "Single-partition execution" placeholder covers the common
 * parallelism == 1 case, and a lighter loading placeholder covers the gap between a table going
 * Running at P>=2 and its first metrics poll actually carrying `partitions`.
 *
 * Stage columns are labeled `Stage {id} · {kind}` — TablePartitionMetrics.kind (plan 003 M4) carries the
 * real operator name (Join/SemiAnti/Unnest/FilterProject/Reduce/LatestBy — see
 * StreamForge.Engine.Dataflow.TableStageKindLabel on the backend). "Ingest" and "Output" are still drawn
 * as structural end-caps (the graph's known entry/exit points) rather than data-bearing columns, since
 * neither is represented in the partition metrics array (Ingest runs at partition count 1 and isn't
 * tracked per-partition; the terminal gather isn't a TableStageGrain at all).
 *
 * Delta rate is derived client-side the same way useTableMetrics derives its aggregate deltasIn/s —
 * diffing cumulative per-cell counters between successive polls — off the SAME `metrics` object the
 * page already polls on its existing 2s cadence (see useTableMetrics); no second poll loop.
 */
export function DataflowPanel({ table, metrics }: { table: TableDefinition; metrics: TableMetrics | null }) {
  const prevRef = useRef<Map<string, { deltasIn: number; atMs: number }>>(new Map())
  const rateRef = useRef<Map<string, number>>(new Map())

  const partitions = metrics?.partitions ?? null

  useEffect(() => {
    if (!partitions) return
    const now = performance.now()
    const nextRates = new Map<string, number>()
    for (const p of partitions) {
      const key = cellKey(p.stageId, p.partition)
      const prev = prevRef.current.get(key)
      if (prev) {
        const dtSec = (now - prev.atMs) / 1000
        if (dtSec > 0) nextRates.set(key, Math.max(0, (p.deltasIn - prev.deltasIn) / dtSec))
      }
      prevRef.current.set(key, { deltasIn: p.deltasIn, atMs: now })
    }
    rateRef.current = nextRates
  }, [partitions])

  const stageIds = useMemo(() => {
    if (!partitions) return []
    return Array.from(new Set(partitions.map((p) => p.stageId))).sort((a, b) => a - b)
  }, [partitions])

  const cellByKey = useMemo(() => {
    const m = new Map<string, TablePartitionMetrics>()
    for (const p of partitions ?? []) m.set(cellKey(p.stageId, p.partition), p)
    return m
  }, [partitions])

  // Plan 003 M4: every partition of a given stageId shares the same operator kind — take it from
  // whichever partition happened to report first.
  const kindByStageId = useMemo(() => {
    const m = new Map<number, string>()
    for (const p of partitions ?? []) if (p.kind && !m.has(p.stageId)) m.set(p.stageId, p.kind)
    return m
  }, [partitions])

  if (table.parallelism < 2) {
    return (
      <Card>
        <CardContent>
          <Empty className="border-0 p-0">
            <EmptyHeader>
              <EmptyDescription>Single-partition execution — the whole table runs on one grain.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        </CardContent>
      </Card>
    )
  }

  if (!partitions || partitions.length === 0) {
    return (
      <Card>
        <CardContent>
          <Empty className="border-0 p-0">
            <EmptyHeader>
              <EmptyDescription>Waiting for partition metrics…</EmptyDescription>
            </EmptyHeader>
          </Empty>
        </CardContent>
      </Card>
    )
  }

  const frontierValues = partitions.map((p) => p.frontierEpoch).filter((f) => f >= 0)
  const maxFrontier = frontierValues.length > 0 ? Math.max(...frontierValues) : -1
  const minFrontier = frontierValues.length > 0 ? Math.min(...frontierValues) : -1
  const partitionCount = table.parallelism

  return (
    <Card>
      <CardContent className="flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Dataflow</h3>
          <Badge variant="outline">{partitionCount}-way parallel</Badge>
        </div>

        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          <Stat label="Deltas in (total)" value={(metrics?.deltasIn ?? 0).toLocaleString()} />
          <Stat label="Deltas out (total)" value={(metrics?.deltasOut ?? 0).toLocaleString()} />
          <Stat label="Frontier min / max" value={minFrontier < 0 ? '—' : `${minFrontier} / ${maxFrontier}`} />
          <Stat label="Partitions" value={String(partitionCount)} />
        </div>

        <TooltipProvider>
          <div className="flex items-stretch gap-2 overflow-x-auto pb-1">
            <div className="flex flex-col items-center justify-center gap-1 self-stretch rounded-lg border border-dashed border-border px-3 py-2 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
              <span className="[writing-mode:vertical-rl]">Ingest</span>
            </div>
            <ChevronRight className="my-auto size-4 shrink-0 text-muted-foreground" />

            {stageIds.map((stageId) => (
              <div key={stageId} className="flex items-stretch gap-2">
                <div className="flex flex-col gap-1">
                  <div className="text-center text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
                    Stage {stageId}
                    {kindByStageId.get(stageId) ? ` · ${kindByStageId.get(stageId)}` : ''}
                  </div>
                  <div className="flex flex-col gap-1">
                    {Array.from({ length: partitionCount }, (_, partition) => {
                      const cell = cellByKey.get(cellKey(stageId, partition))
                      const rate = rateRef.current.get(cellKey(stageId, partition))
                      return (
                        <Tooltip key={partition}>
                          <TooltipTrigger asChild>
                            <div
                              className={cn(
                                'flex w-24 flex-col gap-0.5 rounded-md border px-2 py-1 font-mono text-[11px]',
                                cell ? lagClasses(cell.frontierEpoch, maxFrontier) : 'border-dashed border-border text-muted-foreground',
                              )}
                            >
                              <span className="text-[9px] uppercase tracking-wide opacity-70">p{partition}</span>
                              <span>{cell ? `E${cell.frontierEpoch}` : '—'}</span>
                              <span className="opacity-70">
                                {rate !== undefined ? `${rate.toFixed(1)}/s` : cell ? `${cell.deltasIn.toLocaleString()} in` : '—'}
                              </span>
                            </div>
                          </TooltipTrigger>
                          <TooltipContent side="top" className="text-xs">
                            {cell ? (
                              <div className="flex flex-col gap-0.5">
                                <span>
                                  Stage {cell.stageId} ({cell.kind || 'unknown'}) · partition {cell.partition}
                                </span>
                                <span>Frontier epoch {cell.frontierEpoch}</span>
                                <span>
                                  Deltas in {cell.deltasIn.toLocaleString()} · out {cell.deltasOut.toLocaleString()}
                                </span>
                                <span>Updated {new Date(cell.lastUpdateMs).toLocaleTimeString()}</span>
                              </div>
                            ) : (
                              'No data yet'
                            )}
                          </TooltipContent>
                        </Tooltip>
                      )
                    })}
                  </div>
                </div>
                <ChevronRight className="my-auto size-4 shrink-0 text-muted-foreground" />
              </div>
            ))}

            <div className="flex flex-col items-center justify-center gap-1 self-stretch rounded-lg border border-dashed border-border px-3 py-2 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
              <span className="[writing-mode:vertical-rl]">Output</span>
            </div>
          </div>
        </TooltipProvider>
      </CardContent>
    </Card>
  )
}
