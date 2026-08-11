import { useMemo } from 'react'
import { ChevronRight } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Empty, EmptyDescription, EmptyHeader } from '@/components/ui/empty'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import type { ExecutionPlanResponse, PlanEdge, PlanStage } from '../../api/types'

/** Ranks each stage by longest path from an external input — fromStageId === -1 edges don't count
 * as a real predecessor, so a stage fed only by inputs sits at rank 0. Cycle-guarded (a malformed
 * plan degrades to rank 0 for the offending stage instead of recursing forever); real plans are
 * DAGs so this never actually triggers. */
function rankStages(plan: ExecutionPlanResponse, edgeById: Map<number, PlanEdge>): Map<number, number> {
  const stageById = new Map(plan.stages.map((s) => [s.stageId, s]))
  const rank = new Map<number, number>()

  function rankOf(stageId: number, visiting: Set<number>): number {
    const cached = rank.get(stageId)
    if (cached !== undefined) return cached
    if (visiting.has(stageId)) return 0
    visiting.add(stageId)
    const stage = stageById.get(stageId)
    let maxDep = -1
    for (const inEdge of stage?.inEdges ?? []) {
      const edge = edgeById.get(inEdge.edgeId)
      if (!edge || edge.fromStageId === -1) continue
      maxDep = Math.max(maxDep, rankOf(edge.fromStageId, visiting))
    }
    visiting.delete(stageId)
    const r = maxDep + 1
    rank.set(stageId, r)
    return r
  }

  for (const s of plan.stages) rankOf(s.stageId, new Set())
  return rank
}

function edgeLabel(edge: PlanEdge): string {
  const base = edge.role ? `${edge.role} · ${edge.mode}` : edge.mode
  return edge.arrangeKeyFields ? `${base} on [${edge.arrangeKeyFields.join(', ')}]` : base
}

function StageCard({ stage, inbound, terminal }: { stage: PlanStage; inbound: PlanEdge[]; terminal: boolean }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="flex w-36 flex-col gap-1 rounded-lg border border-border bg-background/60 px-3 py-2">
            <div className="flex items-center justify-between gap-1">
              <span className="text-xs font-medium text-foreground">Stage {stage.stageId}</span>
              {terminal && (
                <Badge variant="outline" className="px-1 text-[9px]">
                  out
                </Badge>
              )}
            </div>
            <span className="truncate font-mono text-[11px] text-muted-foreground">{stage.kind}</span>
            <span className="truncate text-[10px] text-muted-foreground/80">{stage.alias}</span>
          </div>
        </TooltipTrigger>
        <TooltipContent side="top" className="max-w-xs text-xs">
          <div className="flex flex-col gap-1">
            <span className="font-medium">
              Stage {stage.stageId} · {stage.kind}
            </span>
            <span className="text-muted-foreground">alias: {stage.alias}</span>
            {inbound.length === 0 ? (
              <span className="text-muted-foreground">No inbound edges</span>
            ) : (
              inbound.map((e) => (
                <span key={e.edgeId}>
                  ← {e.fromStageId === -1 ? e.externalInputNames.join(', ') || 'external' : `Stage ${e.fromStageId}`} ({edgeLabel(e)})
                </span>
              ))
            )}
          </div>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

/** Compact box-and-arrow rendering of a table/pipeline's `/plan` response — physical stage graph
 * when `physical` is true, otherwise the `unavailableReason` (always a 200, never an error state).
 * `inputs` is shown regardless of `physical` since it's populated whenever the SQL compiles. */
export function PlanStageGraph({ plan }: { plan: ExecutionPlanResponse }) {
  const edgeById = useMemo(() => new Map(plan.edges.map((e) => [e.edgeId, e])), [plan])
  const rank = useMemo(() => rankStages(plan, edgeById), [plan, edgeById])
  const byRank = useMemo(() => {
    const maxRank = plan.stages.length > 0 ? Math.max(...plan.stages.map((s) => rank.get(s.stageId) ?? 0)) : -1
    const buckets: PlanStage[][] = Array.from({ length: maxRank + 1 }, () => [])
    for (const s of plan.stages) buckets[rank.get(s.stageId) ?? 0].push(s)
    return buckets
  }, [plan, rank])
  const externalInputNames = useMemo(
    () => Array.from(new Set(plan.edges.filter((e) => e.fromStageId === -1).flatMap((e) => e.externalInputNames))),
    [plan],
  )
  const hasTerminalOutput = plan.edges.some((e) => e.toStageId === -1)

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-2">
        {plan.planSummary && <span className="text-xs text-muted-foreground">{plan.planSummary}</span>}
        <Badge variant="outline">{plan.parallelism}-way parallel</Badge>
      </div>

      {plan.inputs.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {plan.inputs.map((name) => (
            <Badge key={name} variant="outline" className="text-muted-foreground">
              {name}
            </Badge>
          ))}
        </div>
      )}

      {!plan.physical ? (
        <Empty className="border border-dashed">
          <EmptyHeader>
            <EmptyDescription>{plan.unavailableReason ?? 'No physical execution plan for this entity.'}</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : plan.stages.length === 0 ? (
        <Empty className="border border-dashed">
          <EmptyHeader>
            <EmptyDescription>The plan carries no stages.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <div className="flex items-stretch gap-2 overflow-x-auto pb-1">
          {externalInputNames.length > 0 && (
            <>
              <div className="flex flex-col items-center justify-center gap-1 self-stretch rounded-lg border border-dashed border-border px-3 py-2 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
                <span className="[writing-mode:vertical-rl]" title={externalInputNames.join(', ')}>
                  In
                </span>
              </div>
              <ChevronRight className="my-auto size-4 shrink-0 text-muted-foreground" />
            </>
          )}

          {byRank.map((stages, i) => (
            <div key={i} className="flex items-stretch gap-2">
              <div className="flex flex-col gap-1.5">
                {stages.map((s) => (
                  <StageCard
                    key={s.stageId}
                    stage={s}
                    inbound={s.inEdges.map((ie) => edgeById.get(ie.edgeId)).filter((e): e is PlanEdge => !!e)}
                    terminal={plan.edges.some((e) => e.fromStageId === s.stageId && e.toStageId === -1)}
                  />
                ))}
              </div>
              {i < byRank.length - 1 && <ChevronRight className="my-auto size-4 shrink-0 text-muted-foreground" />}
            </div>
          ))}

          {hasTerminalOutput && (
            <>
              <ChevronRight className="my-auto size-4 shrink-0 text-muted-foreground" />
              <div className="flex flex-col items-center justify-center gap-1 self-stretch rounded-lg border border-dashed border-border px-3 py-2 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
                <span className="[writing-mode:vertical-rl]">Out</span>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
