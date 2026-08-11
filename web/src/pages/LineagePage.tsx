import { useEffect, useMemo, useState } from 'react'
import { Background, Controls, MarkerType, Panel, ReactFlow, ReactFlowProvider } from '@xyflow/react'
import type { Edge } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Database, Table2, Workflow } from 'lucide-react'
import { sourcesApi } from '../api/sources'
import { pipelinesApi } from '../api/pipelines'
import { tablesApi } from '../api/tables'
import type { PipelineDefinition, SourceDefinition, TableDefinition } from '../api/types'
import { Topbar } from '../components/Topbar'
import { lineageNodeTypes } from '../components/lineage/LineageNode'
import type { LineageFlowNode } from '../components/lineage/LineageNode'
import { LineageDetailPanel } from '../components/lineage/LineageDetailPanel'
import type { LineageTarget } from '../components/lineage/LineageDetailPanel'
import { Skeleton } from '@/components/ui/skeleton'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'

const POLL_MS = 2000
const COLUMN_WIDTH = 240
const ROW_HEIGHT = 84

/** One catalog entity as a lineage-graph vertex: what it depends on (by node id), so edges and rank
 * (column position) both fall out of the same `deps` list — "consumers" are never stored anywhere,
 * they're just every entity whose `deps` names this one (see buildEntries below). */
interface LineageEntry {
  id: string
  kind: 'source' | 'pipeline' | 'table'
  refId: string
  name: string
  deps: string[]
}

function sourceNodeId(name: string): string {
  return `source:${name}`
}
function pipelineNodeId(id: string): string {
  return `pipeline:${id}`
}
function tableNodeId(id: string): string {
  return `table:${id}`
}

/** Pipelines only ever read sources (leaf-level `sourceNames`); tables read streams (sources) AND/OR
 * other tables (`streamInputs`/`tableInputs`) — there is no pipeline-into-table edge in this SQL
 * dialect. A dependency name that doesn't resolve to a known entity (e.g. mid-edit SQL referencing a
 * not-yet-created source) is silently dropped rather than drawn as a dangling edge. */
function buildEntries(sources: SourceDefinition[], pipelines: PipelineDefinition[], tables: TableDefinition[]): Map<string, LineageEntry> {
  const entries = new Map<string, LineageEntry>()
  for (const s of sources) {
    entries.set(sourceNodeId(s.name), { id: sourceNodeId(s.name), kind: 'source', refId: s.name, name: s.name, deps: [] })
  }
  for (const p of pipelines) {
    const deps = (p.sourceNames ?? []).map(sourceNodeId).filter((id) => entries.has(id))
    entries.set(pipelineNodeId(p.id), { id: pipelineNodeId(p.id), kind: 'pipeline', refId: p.id, name: p.name, deps })
  }
  const tableIdByName = new Map(tables.map((t) => [t.name, t.id]))
  for (const t of tables) {
    const deps = [
      ...t.streamInputs.map(sourceNodeId).filter((id) => entries.has(id)),
      ...t.tableInputs
        .map((n) => tableIdByName.get(n))
        .filter((id): id is string => !!id)
        .map(tableNodeId),
    ]
    entries.set(tableNodeId(t.id), { id: tableNodeId(t.id), kind: 'table', refId: t.id, name: t.name, deps })
  }
  return entries
}

/** Column index = longest dependency chain from a root (a source, or an input-less entity). Cycle-
 * guarded defensively; `tableInputs` chains are backend-validated acyclic, so this never actually
 * recurses into a cycle in practice. */
function rankOf(id: string, entries: Map<string, LineageEntry>, cache: Map<string, number>, visiting: Set<string>): number {
  const cached = cache.get(id)
  if (cached !== undefined) return cached
  const entry = entries.get(id)
  if (!entry || entry.deps.length === 0) {
    const r = entry?.kind === 'source' ? 0 : 1
    cache.set(id, r)
    return r
  }
  if (visiting.has(id)) return 0
  visiting.add(id)
  let maxDep = -1
  for (const dep of entry.deps) maxDep = Math.max(maxDep, rankOf(dep, entries, cache, visiting))
  visiting.delete(id)
  const r = maxDep + 1
  cache.set(id, r)
  return r
}

const KIND_ORDER = { source: 0, pipeline: 1, table: 2 }

function buildGraph(sources: SourceDefinition[], pipelines: PipelineDefinition[], tables: TableDefinition[]) {
  const entries = buildEntries(sources, pipelines, tables)
  const rankCache = new Map<string, number>()
  for (const id of entries.keys()) rankOf(id, entries, rankCache, new Set())

  const byRank = new Map<number, LineageEntry[]>()
  for (const entry of entries.values()) {
    const r = rankCache.get(entry.id) ?? 0
    const bucket = byRank.get(r)
    if (bucket) bucket.push(entry)
    else byRank.set(r, [entry])
  }

  const nodes: LineageFlowNode[] = []
  for (const r of Array.from(byRank.keys()).sort((a, b) => a - b)) {
    const bucket = byRank.get(r)!.sort((a, b) => KIND_ORDER[a.kind] - KIND_ORDER[b.kind] || a.name.localeCompare(b.name))
    bucket.forEach((entry, i) => {
      nodes.push({
        id: entry.id,
        type: 'lineage',
        position: { x: r * COLUMN_WIDTH, y: i * ROW_HEIGHT },
        data: {
          kind: entry.kind,
          label: entry.name,
          status: entry.kind === 'pipeline' ? pipelines.find((p) => p.id === entry.refId)?.status
            : entry.kind === 'table' ? tables.find((t) => t.id === entry.refId)?.status
            : undefined,
          enabled: entry.kind === 'source' ? sources.find((s) => s.name === entry.refId)?.enabled : undefined,
          parallelism: entry.kind === 'table' ? tables.find((t) => t.id === entry.refId)?.parallelism : undefined,
        },
      })
    })
  }

  const edges: Edge[] = []
  for (const entry of entries.values()) {
    for (const dep of entry.deps) {
      edges.push({
        id: `${dep}->${entry.id}`,
        source: dep,
        target: entry.id,
        type: 'smoothstep',
        markerEnd: { type: MarkerType.ArrowClosed, color: 'var(--color-muted-foreground)' },
        style: { stroke: 'var(--color-muted-foreground)' },
      })
    }
  }

  return { nodes, edges }
}

function LineageLegend() {
  return (
    <Panel position="top-left" className="flex flex-col gap-1 rounded-lg border border-border bg-card/90 px-3 py-2 text-xs text-muted-foreground backdrop-blur">
      <span className="flex items-center gap-1.5">
        <Database className="size-3.5 text-chart-1" /> Source
      </span>
      <span className="flex items-center gap-1.5">
        <Workflow className="size-3.5 text-chart-4" /> Pipeline
      </span>
      <span className="flex items-center gap-1.5">
        <Table2 className="size-3.5 text-chart-2" /> Table
      </span>
    </Panel>
  )
}

/** Sources → pipelines → tables lineage graph (React Flow), with a per-node detail sheet (SQL,
 * schema, live metrics, sample rows, and — for pipelines/tables — the /plan execution graph). List
 * data is polled on the same 2s cadence useTableMetrics already established elsewhere in the app, so
 * node status/parallelism track start/stop actions taken from the dedicated list pages. */
export function LineagePage() {
  const [sources, setSources] = useState<SourceDefinition[] | null>(null)
  const [pipelines, setPipelines] = useState<PipelineDefinition[] | null>(null)
  const [tables, setTables] = useState<TableDefinition[] | null>(null)
  const [selectedId, setSelectedId] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    function poll() {
      sourcesApi.list().then((l) => !cancelled && setSources(l)).catch(() => !cancelled && setSources((s) => s ?? []))
      pipelinesApi.list().then((l) => !cancelled && setPipelines(l)).catch(() => !cancelled && setPipelines((p) => p ?? []))
      tablesApi.list().then((l) => !cancelled && setTables(l)).catch(() => !cancelled && setTables((t) => t ?? []))
    }
    poll()
    const timer = setInterval(poll, POLL_MS)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [])

  const loaded = sources !== null && pipelines !== null && tables !== null

  const { nodes, edges } = useMemo(() => {
    if (!loaded) return { nodes: [], edges: [] }
    return buildGraph(sources!, pipelines!, tables!)
  }, [loaded, sources, pipelines, tables])

  const displayNodes = useMemo(() => nodes.map((n) => ({ ...n, selected: n.id === selectedId })), [nodes, selectedId])

  const selectedTarget = useMemo<LineageTarget | null>(() => {
    if (!selectedId || !loaded) return null
    if (selectedId.startsWith('source:')) {
      const source = sources!.find((s) => sourceNodeId(s.name) === selectedId)
      return source ? { kind: 'source', source } : null
    }
    if (selectedId.startsWith('pipeline:')) {
      const pipeline = pipelines!.find((p) => pipelineNodeId(p.id) === selectedId)
      return pipeline ? { kind: 'pipeline', pipeline } : null
    }
    const table = tables!.find((t) => tableNodeId(t.id) === selectedId)
    return table ? { kind: 'table', table } : null
  }, [selectedId, loaded, sources, pipelines, tables])

  const isEmpty = loaded && sources!.length === 0 && pipelines!.length === 0 && tables!.length === 0

  return (
    <div>
      <Topbar title="Lineage" subtitle="Sources → pipelines → tables, derived from each entity's declared inputs" />

      <div className="p-8 pt-4">
        {!loaded ? (
          <Skeleton className="h-[75vh] min-h-[480px] w-full" />
        ) : isEmpty ? (
          <Empty className="border border-dashed">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <Workflow />
              </EmptyMedia>
              <EmptyTitle>Nothing to show yet</EmptyTitle>
              <EmptyDescription>Add a source, pipeline, or table to see how data flows between them.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <div className="h-[75vh] min-h-[480px] w-full overflow-hidden rounded-xl border border-border">
            <ReactFlowProvider>
              <ReactFlow
                nodes={displayNodes}
                edges={edges}
                nodeTypes={lineageNodeTypes}
                onNodeClick={(_, node) => setSelectedId(node.id)}
                onPaneClick={() => setSelectedId(null)}
                fitView
                nodesDraggable={false}
                nodesConnectable={false}
                elementsSelectable
              >
                <Background />
                <Controls showInteractive={false} />
                <LineageLegend />
              </ReactFlow>
            </ReactFlowProvider>
          </div>
        )}
      </div>

      <LineageDetailPanel target={selectedTarget} onOpenChange={(open) => !open && setSelectedId(null)} />
    </div>
  )
}
