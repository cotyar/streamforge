import { useEffect, useMemo, useState } from 'react'
import { Background, Controls, MarkerType, Panel, ReactFlow, ReactFlowProvider } from '@xyflow/react'
import type { Edge } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Database, Table2, Workflow } from 'lucide-react'
import { sourcesApi } from '../api/sources'
import { pipelinesApi } from '../api/pipelines'
import { tablesApi } from '../api/tables'
import type { PipelineDefinition, SinkSpec, SourceDefinition, TableDefinition } from '../api/types'
import { Topbar } from '../components/Topbar'
import { lineageNodeTypes } from '../components/lineage/LineageNode'
import type { LineageFlowNode } from '../components/lineage/LineageNode'
import { LineageDetailPanel } from '../components/lineage/LineageDetailPanel'
import type { LineageTarget } from '../components/lineage/LineageDetailPanel'
import { Skeleton } from '@/components/ui/skeleton'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'

const POLL_MS = 2000
// Node width is capped at 176px (LineageNode.tsx's `w-44`); the extra headroom here keeps a gap
// between one column's nodes and the next column's incoming edges even at that cap, so long-range
// edges routed behind a column are never fully occluded by an opaque node (React Flow renders
// edges beneath the node layer).
const COLUMN_WIDTH = 280
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
  /** Plan 019 D9: the first OUTBOUND edges this graph draws — node ids of duplex-kind sinks' named
   * source, i.e. pipeline/table -> source. Deliberately NOT merged into `deps`: `rankOf` (below) walks
   * `deps` alone, and a pipeline that both reads a source (a dep) and writes back to it via a duplex
   * sink — the order-entry topology D9 calls out (orders out, execution reports back on the same FIX
   * session) — must not turn into a cycle `rankOf` has to guard against. Keeping the two lists separate
   * makes that structurally impossible rather than merely handled. */
  sinkTargets: string[]
}

/** Plan 019 D9: only a `duplex` sink names another catalog entity (`DuplexSinkConfig.sourceName`) —
 * every other sink kind (nats/file/http/loopback) points at something outside the catalog (a subject, a
 * path, a URL, a generator) with no node on this graph to draw an edge to, so they draw nothing this
 * wave. `SinkSpec` in types.ts is a deliberately partial mirror (plan 019 D8: SinksEditor.tsx already
 * reads a sink's kind-specific config dynamically off the transport descriptor rather than a typed
 * field), so this reads `duplex` the same loosely-typed way instead of widening the frozen contract.
 * Only an ENABLED sink is drawn — matches SinkSelection.Active's own eligibility rule on the backend, so
 * a disabled duplex sink (never actually published through) draws no edge either. `{name}` is substituted
 * with the owning pipeline's id / table's name, mirroring DuplexSinkClient's own substitution
 * (DuplexSinkTransport.cs:151), the same convention every other sink config's templated field uses. */
function duplexSinkTarget(sink: SinkSpec, ownerName: string): string | null {
  if (!sink.enabled || sink.kind !== 'duplex') return null
  const raw = (sink as unknown as { duplex?: { sourceName?: string } }).duplex?.sourceName
  if (!raw) return null
  return raw.replaceAll('{name}', ownerName)
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
    entries.set(sourceNodeId(s.name), { id: sourceNodeId(s.name), kind: 'source', refId: s.name, name: s.name, deps: [], sinkTargets: [] })
  }
  for (const p of pipelines) {
    const deps = (p.sourceNames ?? []).map(sourceNodeId).filter((id) => entries.has(id))
    const sinkTargets = (p.sinks ?? [])
      .map((s) => duplexSinkTarget(s, p.id))
      .filter((n): n is string => !!n)
      .map(sourceNodeId)
      .filter((id) => entries.has(id))
    entries.set(pipelineNodeId(p.id), { id: pipelineNodeId(p.id), kind: 'pipeline', refId: p.id, name: p.name, deps, sinkTargets })
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
    const sinkTargets = (t.sinks ?? [])
      .map((s) => duplexSinkTarget(s, t.name))
      .filter((n): n is string => !!n)
      .map(sourceNodeId)
      .filter((id) => entries.has(id))
    entries.set(tableNodeId(t.id), { id: tableNodeId(t.id), kind: 'table', refId: t.id, name: t.name, deps, sinkTargets })
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

  // Plan 019 D9: every source targeted by at least one duplex sink, so those (and only those) source
  // nodes render the extra inbound connector below — a source with no duplex sink writing to it looks
  // exactly as it always has.
  const sinkTargetIds = new Set<string>()
  for (const entry of entries.values()) for (const t of entry.sinkTargets) sinkTargetIds.add(t)

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
          hasSinkOut: entry.sinkTargets.length > 0,
          hasSinkIn: sinkTargetIds.has(entry.id),
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
  // Plan 019 D9: the outbound half, drawn as a visually distinct (dashed, --color-chart-5) edge so it
  // never reads as an input — the deps loop above is the only thing that can produce a plain solid
  // muted-foreground edge. Pushed after the deps loop and keyed with a "sink:" id prefix so a pipeline
  // that both reads and writes the same source (the legitimate order-entry cycle D9 names) gets two
  // separate edges between the same pair of nodes instead of one id collision silently dropping one.
  for (const entry of entries.values()) {
    for (const sourceId of entry.sinkTargets) {
      edges.push({
        id: `sink:${entry.id}->${sourceId}`,
        source: entry.id,
        target: sourceId,
        type: 'smoothstep',
        label: 'duplex sink',
        labelStyle: { fill: 'var(--color-chart-5)', fontSize: 10 },
        labelBgStyle: { fill: 'var(--color-card)', fillOpacity: 0.85 },
        markerEnd: { type: MarkerType.ArrowClosed, color: 'var(--color-chart-5)' },
        style: { stroke: 'var(--color-chart-5)', strokeDasharray: '5 4' },
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
      <span className="mt-0.5 flex items-center gap-1.5 border-t border-border/60 pt-1">
        <svg width="14" height="8" viewBox="0 0 14 8" className="shrink-0 text-chart-5" aria-hidden="true">
          <line x1="0" y1="4" x2="14" y2="4" stroke="currentColor" strokeWidth="1.5" strokeDasharray="3 2" />
        </svg>
        Duplex sink (writes back)
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
