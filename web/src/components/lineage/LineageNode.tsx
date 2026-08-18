import { Handle, Position } from '@xyflow/react'
import type { Node, NodeProps } from '@xyflow/react'
import { Database, Table2, Workflow } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { PipelineStatus } from '../../api/types'

/** Node payload for the lineage canvas — one shape for all three kinds (source/pipeline/table) so a
 * single custom node type covers the graph, distinguished at render time by `kind`. `status` applies
 * to pipeline/table only, `enabled` to source only, `parallelism` to table only (elided when 1). */
export interface LineageNodeData extends Record<string, unknown> {
  kind: 'source' | 'pipeline' | 'table'
  label: string
  status?: PipelineStatus
  enabled?: boolean
  parallelism?: number
  /** Plan 019 D9: this pipeline/table has an outbound edge (a duplex sink naming a live source
   * elsewhere in the catalog) — renders the extra source-type connector so that edge has somewhere on
   * the node to leave from. A pipeline with no duplex sink renders no such connector, same as before
   * this wave. */
  hasSinkOut?: boolean
  /** Plan 019 D9: this SOURCE is the named target of another entity's duplex sink — renders the extra
   * target-type connector so that (from this graph's usual left-to-right flow, backward-pointing)
   * edge has somewhere to land. A source nobody writes back to renders no such connector. */
  hasSinkIn?: boolean
}

export type LineageFlowNode = Node<LineageNodeData, 'lineage'>

const KIND_ICON = { source: Database, pipeline: Workflow, table: Table2 } as const

/** Sanctioned chart-* tokens only, one per kind — same palette the rest of the SPA already reads
 * through --color-chart-N (see index.css's @theme block). */
const KIND_ACCENT = {
  source: 'border-l-chart-1',
  pipeline: 'border-l-chart-4',
  table: 'border-l-chart-2',
} as const

const STATUS_DOT: Record<PipelineStatus, string> = {
  Running: 'bg-primary',
  Stopped: 'bg-muted-foreground',
  Failed: 'bg-destructive',
}

export function LineageNode({ data, selected }: NodeProps<LineageFlowNode>) {
  const Icon = KIND_ICON[data.kind]
  return (
    <div
      className={cn(
        'flex w-44 max-w-44 items-center gap-2 rounded-lg border border-l-4 bg-card px-3 py-2 text-left shadow-sm',
        KIND_ACCENT[data.kind],
        selected ? 'border-primary ring-1 ring-primary' : 'border-border',
      )}
    >
      {(data.kind !== 'source' || data.hasSinkIn) && <Handle type="target" position={Position.Left} className="!bg-muted-foreground" />}
      <Icon className="size-4 shrink-0 text-muted-foreground" />
      <div className="flex min-w-0 flex-1 flex-col">
        <span className="truncate text-xs font-medium text-foreground" title={data.label}>{data.label}</span>
        <span className="flex items-center gap-1 text-[10px] text-muted-foreground">
          {data.kind === 'source' ? (
            data.enabled ? 'enabled' : 'disabled'
          ) : (
            <>
              <span className={cn('inline-block size-1.5 shrink-0 rounded-full', STATUS_DOT[data.status ?? 'Stopped'])} />
              {data.status}
            </>
          )}
          {data.kind === 'table' && data.parallelism && data.parallelism > 1 ? ` · P${data.parallelism}` : ''}
        </span>
      </div>
      {(data.kind !== 'pipeline' || data.hasSinkOut) && <Handle type="source" position={Position.Right} className="!bg-muted-foreground" />}
    </div>
  )
}

export const lineageNodeTypes = { lineage: LineageNode }
