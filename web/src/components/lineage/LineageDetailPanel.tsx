import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Database, ExternalLink, Table2, Workflow } from 'lucide-react'
import { pipelinesApi } from '../../api/pipelines'
import { tablesApi } from '../../api/tables'
import type { ExecutionPlanResponse, FieldDef, PipelineDefinition, SourceDefinition, TableDefinition } from '../../api/types'
import { useSourceTape } from '../../hooks/useSourceTape'
import { usePipelineResults } from '../../hooks/usePipelineResults'
import { useMetricsStream } from '../../hooks/useMetricsStream'
import { useTableMetrics } from '../../hooks/useTableMetrics'
import { useTableRows } from '../../hooks/useTableRows'
import { StatusBadge } from '../StatusBadge'
import { TagList } from '../TagList'
import { SqlEditor } from '../SqlEditor'
import { ResultsTable } from '../ResultsTable'
import { MetricsBar } from '../MetricsBar'
import { DataflowPanel } from '../DataflowPanel'
import { PlanStageGraph } from './PlanStageGraph'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Badge } from '@/components/ui/badge'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import type { ResultRow, RowValue } from '../../api/types'

export type LineageTarget =
  | { kind: 'source'; source: SourceDefinition }
  | { kind: 'pipeline'; pipeline: PipelineDefinition }
  | { kind: 'table'; table: TableDefinition }

function targetId(target: LineageTarget): string {
  if (target.kind === 'source') return target.source.name
  if (target.kind === 'pipeline') return target.pipeline.id
  return target.table.id
}

const KIND_ICON = { source: Database, pipeline: Workflow, table: Table2 } as const

function formatCell(v: RowValue): string {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'number') return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(4)
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  if (typeof v === 'object') return JSON.stringify(v)
  return v
}

/** Compact "key=value" live-sample rendering, shared by the source live-tape and table rows tabs
 * (the pipeline results tab reuses the fuller ResultsTable instead, since pipeline output tends to
 * be small numeric rows well suited to a real column grid). */
function SampleLines({ rows, emptyMessage }: { rows: ResultRow[]; emptyMessage: string }) {
  return (
    <ScrollArea className="h-64 rounded-lg border border-border bg-background">
      <div className="min-w-max p-2 font-mono text-[11px] leading-5 text-muted-foreground">
        {rows.length === 0 ? (
          <p className="text-muted-foreground/70">{emptyMessage}</p>
        ) : (
          rows.map((row, i) => (
            <div key={i} className={cn('whitespace-nowrap', i === 0 && 'text-foreground')}>
              {Object.entries(row)
                .map(([k, v]) => `${k}=${formatCell(v)}`)
                .join('  ')}
            </div>
          ))
        )}
      </div>
      <ScrollBar orientation="horizontal" />
    </ScrollArea>
  )
}

/** Read-only field-tree view, shared by a source's declared fields and a table's outputFields (both
 * FieldDef[]) — Json fields drill into their nested shape, always expanded (no collapse state, this
 * is a glance-only panel, not the editable SourcesPage form). */
function SchemaTree({ fields, depth = 0 }: { fields: FieldDef[]; depth?: number }) {
  return (
    <>
      {fields.map((f) => (
        <div key={f.name}>
          <div className="flex items-center justify-between px-3 py-1.5 text-xs" style={{ paddingLeft: `${12 + depth * 16}px` }}>
            <span className="font-mono text-foreground">
              {f.name}
              {f.isArray && <span className="text-muted-foreground/70">[]</span>}
            </span>
            <span className="text-muted-foreground">{f.type}</span>
          </div>
          {f.type === 'Json' && f.children && f.children.length > 0 && <SchemaTree fields={f.children} depth={depth + 1} />}
        </div>
      ))}
    </>
  )
}

function PlanTab({ kind, id }: { kind: 'pipeline' | 'table'; id: string }) {
  const [plan, setPlan] = useState<ExecutionPlanResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setPlan(null)
    setError(null)
    const request = kind === 'pipeline' ? pipelinesApi.plan(id) : tablesApi.plan(id)
    request
      .then((p) => {
        if (!cancelled) setPlan(p)
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Failed to load the execution plan.')
      })
    return () => {
      cancelled = true
    }
  }, [kind, id])

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    )
  }
  if (!plan) return <p className="text-sm text-muted-foreground">Loading…</p>
  return <PlanStageGraph plan={plan} />
}

function SourceTabs({ source }: { source: SourceDefinition }) {
  const events = useSourceTape(source.name)
  const isConnector = !!source.kind && source.kind !== 'generator'
  return (
    <Tabs defaultValue="overview">
      <TabsList>
        <TabsTrigger value="overview">Overview</TabsTrigger>
        <TabsTrigger value="schema">Schema</TabsTrigger>
        <TabsTrigger value="tape">Live tape</TabsTrigger>
      </TabsList>
      <TabsContent value="overview" className="flex flex-col gap-3 px-1 py-3">
        {source.description && <p className="text-sm text-muted-foreground">{source.description}</p>}
        <TagList tags={source.tags} />
        <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
          <Badge variant="outline">{isConnector ? source.kind : source.generatorProfile}</Badge>
          {!isConnector && (
            <span>
              rate <span className="font-mono text-foreground">{source.eventsPerSecond}</span>/s
            </span>
          )}
          <Badge variant={source.enabled ? 'default' : 'secondary'}>{source.enabled ? 'Enabled' : 'Disabled'}</Badge>
        </div>
      </TabsContent>
      <TabsContent value="schema" className="px-1 py-3">
        <div className="overflow-hidden rounded-lg border border-border py-1">
          <SchemaTree fields={source.fields} />
        </div>
      </TabsContent>
      <TabsContent value="tape" className="px-1 py-3">
        <SampleLines rows={events} emptyMessage="Waiting for live events…" />
      </TabsContent>
    </Tabs>
  )
}

function PipelineTabs({ pipeline }: { pipeline: PipelineDefinition }) {
  const { rows } = usePipelineResults(pipeline.id)
  const metricsStream = useMetricsStream()
  const metrics = metricsStream[pipeline.id] ?? null

  return (
    <Tabs defaultValue="overview">
      <TabsList>
        <TabsTrigger value="overview">Overview</TabsTrigger>
        <TabsTrigger value="metrics">Metrics</TabsTrigger>
        <TabsTrigger value="results">Results</TabsTrigger>
        <TabsTrigger value="plan">Plan</TabsTrigger>
      </TabsList>
      <TabsContent value="overview" className="flex flex-col gap-3 px-1 py-3">
        {pipeline.description && <p className="text-sm text-muted-foreground">{pipeline.description}</p>}
        <TagList tags={pipeline.tags} />
        {pipeline.error && (
          <Alert variant="destructive">
            <AlertDescription>{pipeline.error}</AlertDescription>
          </Alert>
        )}
        <SqlEditor value={pipeline.sql} onChange={() => {}} readOnly diagnostics={[]} />
      </TabsContent>
      <TabsContent value="metrics" className="px-1 py-3">
        <MetricsBar metrics={metrics} />
      </TabsContent>
      <TabsContent value="results" className="px-1 py-3">
        <div className="max-h-80 overflow-hidden rounded-lg border border-border">
          <ResultsTable rows={rows} />
        </div>
      </TabsContent>
      <TabsContent value="plan" className="px-1 py-3">
        <PlanTab kind="pipeline" id={pipeline.id} />
      </TabsContent>
    </Tabs>
  )
}

function TableTabs({ table }: { table: TableDefinition }) {
  const { metrics } = useTableMetrics(table.id)
  const { rows } = useTableRows(table.id, table.name)

  return (
    <Tabs defaultValue="overview">
      <TabsList>
        <TabsTrigger value="overview">Overview</TabsTrigger>
        <TabsTrigger value="schema">Schema</TabsTrigger>
        <TabsTrigger value="metrics">Metrics</TabsTrigger>
        <TabsTrigger value="rows">Rows</TabsTrigger>
        <TabsTrigger value="plan">Plan</TabsTrigger>
      </TabsList>
      <TabsContent value="overview" className="flex flex-col gap-3 px-1 py-3">
        {table.description && <p className="text-sm text-muted-foreground">{table.description}</p>}
        <TagList tags={table.tags} />
        {table.error && (
          <Alert variant="destructive">
            <AlertDescription>{table.error}</AlertDescription>
          </Alert>
        )}
        <SqlEditor value={table.sql} onChange={() => {}} readOnly diagnostics={[]} />
      </TabsContent>
      <TabsContent value="schema" className="px-1 py-3">
        <div className="overflow-hidden rounded-lg border border-border py-1">
          <SchemaTree fields={table.outputFields} />
        </div>
      </TabsContent>
      <TabsContent value="metrics" className="flex flex-col gap-3 px-1 py-3">
        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-0.5 rounded-lg border border-border bg-background/60 px-3 py-2">
            <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Rows</span>
            <span className="font-mono text-sm font-semibold text-foreground">{(metrics?.rowCount ?? 0).toLocaleString()}</span>
          </div>
          <div className="flex flex-col gap-0.5 rounded-lg border border-border bg-background/60 px-3 py-2">
            <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Deltas in / out</span>
            <span className="font-mono text-sm font-semibold text-foreground">
              {(metrics?.deltasIn ?? 0).toLocaleString()} / {(metrics?.deltasOut ?? 0).toLocaleString()}
            </span>
          </div>
        </div>
        <DataflowPanel table={table} metrics={metrics} />
      </TabsContent>
      <TabsContent value="rows" className="px-1 py-3">
        <SampleLines rows={rows.slice(0, 50).map((r) => r.row)} emptyMessage="Waiting for rows…" />
      </TabsContent>
      <TabsContent value="plan" className="px-1 py-3">
        <PlanTab kind="table" id={table.id} />
      </TabsContent>
    </Tabs>
  )
}

/** Right-hand slide-over for a clicked lineage node — SQL/schema/live metrics/sample rows, plus a
 * Plan tab (pipeline/table only) rendering `/plan`'s stage graph. Keyed by targetId so switching the
 * selected node resets every tab back to Overview instead of keeping whatever tab was open before. */
export function LineageDetailPanel({ target, onOpenChange }: { target: LineageTarget | null; onOpenChange: (open: boolean) => void }) {
  const Icon = target ? KIND_ICON[target.kind] : Database
  const name = target ? (target.kind === 'source' ? target.source.name : target.kind === 'pipeline' ? target.pipeline.name : target.table.name) : ''
  const status = target?.kind === 'pipeline' ? target.pipeline.status : target?.kind === 'table' ? target.table.status : null
  const linkTo = target?.kind === 'pipeline' ? `/pipelines/${target.pipeline.id}` : target?.kind === 'table' ? `/tables/${target.table.id}` : null

  return (
    <Sheet open={!!target} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-2xl">
        {target && (
          <>
            <SheetHeader>
              <SheetTitle className="flex items-center gap-1.5">
                <Icon className="size-4" /> {name}
                {status && <StatusBadge status={status} />}
              </SheetTitle>
              <SheetDescription className="flex items-center justify-between gap-2">
                <span className="capitalize">{target.kind}</span>
                {linkTo && (
                  <Link to={linkTo} className="inline-flex items-center gap-1 text-primary hover:underline">
                    Open <ExternalLink className="size-3" />
                  </Link>
                )}
              </SheetDescription>
            </SheetHeader>

            <div className="px-4 pb-4" key={targetId(target)}>
              {target.kind === 'source' && <SourceTabs source={target.source} />}
              {target.kind === 'pipeline' && <PipelineTabs pipeline={target.pipeline} />}
              {target.kind === 'table' && <TableTabs table={target.table} />}
            </div>
          </>
        )}
      </SheetContent>
    </Sheet>
  )
}
