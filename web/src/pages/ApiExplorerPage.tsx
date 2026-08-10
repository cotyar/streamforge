import { useEffect, useMemo, useRef, useState } from 'react'
import { Braces, Check, Copy, Download } from 'lucide-react'
import { toast } from 'sonner'
import type { PipelineDefinition, ResultRow, RowValue, TableDefinition } from '../api/types'
import { pipelinesApi } from '../api/pipelines'
import { tablesApi } from '../api/tables'
import { metaApi, fetchProtoText } from '../api/explorerTypes'
import type { DynamicEntityMetaDto, GrpcMetaResponse, StaticProtoDto } from '../api/explorerTypes'
import { subscribePipeline, subscribeSource, subscribeTable } from '../realtime/hub'
import { Topbar } from '../components/Topbar'
import { cn } from '@/lib/utils'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

// ============================================================================
// .proto syntax highlighting — a read-only sibling of SqlEditor's tokenizer:
// same regex-based approach and the same --sql-* CSS variables for colors,
// but no textarea/autocomplete since this view is never edited.
// ============================================================================

const PROTO_KEYWORDS = new Set([
  'syntax', 'package', 'import', 'option', 'message', 'service', 'rpc', 'returns', 'stream',
  'repeated', 'optional', 'reserved', 'enum', 'oneof', 'map', 'to', 'public',
])

const PROTO_TYPES = new Set([
  'string', 'double', 'float', 'int32', 'int64', 'uint32', 'uint64', 'sint32', 'sint64',
  'fixed32', 'fixed64', 'sfixed32', 'sfixed64', 'bool', 'bytes',
])

type ProtoTokenKind = 'comment' | 'string' | 'number' | 'keyword' | 'type' | 'identifier' | 'whitespace' | 'punct'

interface ProtoToken {
  text: string
  kind: ProtoTokenKind
}

const PROTO_TOKEN_REGEX = /(\/\/[^\n]*)|("(?:[^"\\]|\\.)*")|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_.]*)|(\s+)|([^\sA-Za-z0-9_]+)/g

function tokenizeProto(text: string): ProtoToken[] {
  const tokens: ProtoToken[] = []
  const re = new RegExp(PROTO_TOKEN_REGEX)
  let m: RegExpExecArray | null
  while ((m = re.exec(text))) {
    const raw = m[0]
    let kind: ProtoTokenKind
    if (m[1] !== undefined) kind = 'comment'
    else if (m[2] !== undefined) kind = 'string'
    else if (m[3] !== undefined) kind = 'number'
    else if (m[4] !== undefined) {
      kind = PROTO_KEYWORDS.has(raw) ? 'keyword' : PROTO_TYPES.has(raw) ? 'type' : 'identifier'
    } else if (m[5] !== undefined) kind = 'whitespace'
    else kind = 'punct'
    tokens.push({ text: raw, kind })
  }
  return tokens
}

const PROTO_KIND_CLASS: Record<ProtoTokenKind, string> = {
  comment: 'text-muted-foreground italic',
  string: 'text-[var(--sql-string)]',
  number: 'text-[var(--sql-number)]',
  keyword: 'text-[var(--sql-keyword)]',
  type: 'text-[var(--sql-function)]',
  identifier: 'text-foreground',
  whitespace: '',
  punct: 'text-muted-foreground',
}

function ProtoView({ text }: { text: string }) {
  const tokens = useMemo(() => tokenizeProto(text), [text])
  return (
    // Native scroll container, not Radix ScrollArea: max-h on ScrollArea's root doesn't bound the
    // Radix viewport, so vertical scroll never engaged and long protos clipped at the boundary.
    <div className="max-h-[28rem] overflow-auto rounded-lg border border-border bg-input/20">
      <pre className="min-w-max whitespace-pre p-3 font-mono text-[12px] leading-5">
        {tokens.map((t, i) => (
          <span key={i} className={PROTO_KIND_CLASS[t.kind]}>
            {t.text}
          </span>
        ))}
      </pre>
    </div>
  )
}

// ============================================================================
// Small shared bits: copy button, blob download, a copyable command/snippet row.
// ============================================================================

function CopyButton({ text, label = 'Copy' }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false)

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      toast.error('Failed to copy to clipboard.')
    }
  }

  return (
    <Button type="button" variant="ghost" size="icon-sm" onClick={() => void handleCopy()} aria-label={label} title={label}>
      {copied ? <Check className="text-primary" /> : <Copy />}
    </Button>
  )
}

function downloadText(filename: string, text: string) {
  const blob = new Blob([text], { type: 'text/plain;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}

function SnippetRow({ text }: { text: string }) {
  return (
    <div className="flex items-center gap-1 rounded-lg border border-border bg-input/20 px-2.5 py-1.5">
      <code className="min-w-0 flex-1 overflow-x-auto whitespace-pre font-mono text-[11px] text-foreground">{text}</code>
      <CopyButton text={text} label="Copy command" />
    </div>
  )
}

// ============================================================================
// Entity detail: Definition / Connect / Live data cards.
// ============================================================================

function EntityDefinitionCard({ entity }: { entity: DynamicEntityMetaDto }) {
  const [text, setText] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    setText(null)
    fetchProtoText(entity.protoPath)
      .then((t) => {
        if (!cancelled) setText(t)
      })
      .catch((err: unknown) => {
        if (cancelled) return
        const message = err instanceof Error ? err.message : 'Failed to load .proto definition.'
        setError(message)
        toast.error(message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [entity.protoPath])

  const filename = `${entity.name}.proto`

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-2">
          <CardTitle>Definition</CardTitle>
          <div className="flex items-center gap-1">
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={!text}
              onClick={() => text && downloadText(filename, text)}
            >
              <Download data-icon="inline-start" /> Download
            </Button>
            {text && <CopyButton text={text} label="Copy .proto" />}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="flex flex-col gap-2">
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-5/6" />
            <Skeleton className="h-4 w-4/6" />
            <Skeleton className="h-36 w-full" />
          </div>
        ) : error ? (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : (
          <ProtoView text={text ?? ''} />
        )}
      </CardContent>
    </Card>
  )
}

function ConnectCard({ entity, grpcPort }: { entity: DynamicEntityMetaDto; grpcPort: number }) {
  const grpcurlCmd = `grpcurl -plaintext localhost:${grpcPort} describe streamforge.dynamic.v1.${entity.messageName}`
  const genClientCmd = `./tools/generate-client.sh ${entity.kind} ${entity.id} --user editor --pass 'editor123!'`

  return (
    <Card>
      <CardHeader>
        <CardTitle>Connect</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <div>
          <p className="mb-1 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Entity key</p>
          <SnippetRow text={entity.entityKey} />
        </div>
        <div>
          <p className="mb-1 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">grpcurl describe</p>
          <SnippetRow text={grpcurlCmd} />
        </div>
        <div>
          <p className="mb-1 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Generate a typed client</p>
          <SnippetRow text={genClientCmd} />
        </div>
      </CardContent>
    </Card>
  )
}

// REST counterpart of ConnectCard: the plain-HTTP routes for this entity, all bearer-token
// authenticated off the same /api/auth/login the SPA itself uses. Base URL is window.location.origin
// (not grpcPort/a hardcoded port) since the host serves both the SPA and the REST API from one origin,
// on dev ports, container ports, and Cloud Run alike.
function RestCard({ entity }: { entity: DynamicEntityMetaDto }) {
  const origin = window.location.origin
  const auth = 'Authorization: Bearer $TOKEN'
  const loginCmd = `curl -s -X POST ${origin}/api/auth/login -H 'content-type: application/json' -d '{"username":"admin","password":"admin123!"}'`

  let routes: { label: string; text: string }[]
  let note: string | null = null

  if (entity.kind === 'source') {
    const n = entity.name
    routes = [
      { label: 'List sources', text: `curl -s ${origin}/api/sources -H "${auth}"` },
      { label: 'Get source', text: `curl -s ${origin}/api/sources/${n} -H "${auth}"` },
      { label: 'Get .proto', text: `curl -s ${origin}/api/sources/${n}/proto -H "${auth}"` },
      { label: 'Get status', text: `curl -s ${origin}/api/sources/${n}/status -H "${auth}"` },
    ]
    note = "A source's live events are SignalR-only — there is deliberately no REST rows endpoint for sources."
  } else if (entity.kind === 'pipeline') {
    const id = entity.id
    routes = [
      { label: 'Get pipeline', text: `curl -s ${origin}/api/pipelines/${id} -H "${auth}"` },
      { label: 'Get results', text: `curl -s "${origin}/api/pipelines/${id}/results?limit=20" -H "${auth}"` },
      { label: 'Get metrics', text: `curl -s ${origin}/api/pipelines/${id}/metrics -H "${auth}"` },
      { label: 'Get .proto', text: `curl -s ${origin}/api/pipelines/${id}/proto -H "${auth}"` },
    ]
  } else {
    const id = entity.id
    routes = [
      { label: 'Get table', text: `curl -s ${origin}/api/tables/${id} -H "${auth}"` },
      { label: 'Get rows', text: `curl -s "${origin}/api/tables/${id}/rows?limit=20&offset=0" -H "${auth}"` },
      { label: 'Search', text: `curl -s "${origin}/api/tables/${id}/search?q=AAPL&limit=20" -H "${auth}"` },
      { label: 'Get metrics', text: `curl -s ${origin}/api/tables/${id}/metrics -H "${auth}"` },
      { label: 'Get .proto', text: `curl -s ${origin}/api/tables/${id}/proto -H "${auth}"` },
      { label: 'History stats', text: `curl -s ${origin}/api/tables/${id}/history/stats -H "${auth}"` },
      {
        label: 'History lookup (read-only POST)',
        text: `curl -s -X POST "${origin}/api/tables/${id}/history/lookup?limit=20" -H "${auth}" -H 'content-type: application/json' -d '{"row":{...}}'`,
      },
    ]
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>REST</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <div>
          <p className="mb-1 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Log in (get a token)</p>
          <SnippetRow text={loginCmd} />
        </div>
        {routes.map((r) => (
          <div key={r.label}>
            <p className="mb-1 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">{r.label}</p>
            <SnippetRow text={r.text} />
          </div>
        ))}
        {note && <p className="text-[11px] text-muted-foreground">{note}</p>}
        <div>
          <p className="mb-1 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Interactive reference</p>
          <SnippetRow text={`${origin}/scalar`} />
          <p className="mt-1 text-[11px] text-muted-foreground">Same Bearer token as above.</p>
        </div>
      </CardContent>
    </Card>
  )
}

function formatLiveCell(v: RowValue): string {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'number') return Number.isInteger(v) ? v.toString() : v.toFixed(3)
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

interface LiveSample {
  key: string
  cells: ResultRow
  weight?: number
}

const MAX_LIVE_SAMPLES = 8

/** Subscribes this entity to its live stream via the same hub helpers the rest of the console uses
 * (subscribeSource by name / subscribePipeline by id / subscribeTable by name) and keeps the last
 * MAX_LIVE_SAMPLES rows/deltas, newest first. Always unsubscribes on entity switch or unmount. */
function useEntityLiveSamples(entity: DynamicEntityMetaDto): LiveSample[] {
  const [samples, setSamples] = useState<LiveSample[]>([])
  const seqRef = useRef(0)

  useEffect(() => {
    setSamples([])
    seqRef.current = 0

    if (entity.kind === 'source') {
      return subscribeSource(entity.name, (row) => {
        seqRef.current += 1
        setSamples((prev) => [{ key: `s-${seqRef.current}`, cells: row }, ...prev].slice(0, MAX_LIVE_SAMPLES))
      })
    }

    if (entity.kind === 'pipeline') {
      return subscribePipeline(entity.id, (rows) => {
        setSamples((prev) => {
          const incoming = rows.map((r) => ({ key: `p-${r.seq}`, cells: r.row })).reverse()
          return [...incoming, ...prev].slice(0, MAX_LIVE_SAMPLES)
        })
      })
    }

    return subscribeTable(entity.name, (deltas, seq) => {
      setSamples((prev) => {
        const incoming = deltas.map((d, i) => ({ key: `t-${seq}-${i}`, cells: d.row, weight: d.weight })).reverse()
        return [...incoming, ...prev].slice(0, MAX_LIVE_SAMPLES)
      })
    })
  }, [entity.kind, entity.name, entity.id])

  return samples
}

function LiveDataCard({ entity }: { entity: DynamicEntityMetaDto }) {
  const samples = useEntityLiveSamples(entity)

  return (
    <Card>
      <CardHeader>
        <CardTitle>Live data</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-2">
        <p className="text-[11px] text-muted-foreground">Live sample — the same rows the gRPC stream carries.</p>
        <ScrollArea className="h-40 rounded-lg border border-border bg-background">
          <div className="min-w-max p-2 font-mono text-[11px] leading-5">
            {samples.length === 0 ? (
              <p className="text-muted-foreground/70">Waiting for live events…</p>
            ) : (
              samples.map((s, i) => (
                <div key={s.key} className={cn('whitespace-nowrap text-muted-foreground', i === 0 && 'text-foreground')}>
                  {s.weight !== undefined && (
                    <span className={cn('mr-1.5 font-semibold', s.weight < 0 ? 'text-destructive' : 'text-primary')}>
                      {s.weight > 0 ? `+${s.weight}` : s.weight}
                    </span>
                  )}
                  {Object.entries(s.cells)
                    .map(([k, v]) => `${k}=${formatLiveCell(v)}`)
                    .join('  ')}
                </div>
              ))
            )}
          </div>
          <ScrollBar orientation="horizontal" />
        </ScrollArea>
      </CardContent>
    </Card>
  )
}

// ============================================================================
// Static service detail (right pane when a Services row is selected).
// ============================================================================

const STATIC_SERVICE_FILE: Record<string, string> = {
  SourceService: 'streamforge.proto',
  PipelineService: 'streamforge.proto',
  TableService: 'streamforge.proto',
  StreamService: 'streamforge.proto',
  DynamicStreamService: 'streamforge_dynamic.proto',
}

function ServiceDetail({ name, staticProtos, grpcPort }: { name: string; staticProtos: StaticProtoDto[]; grpcPort: number }) {
  if (name === 'ServerReflection') {
    return (
      <Card>
        <CardHeader>
          <CardTitle>ServerReflection</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <p className="text-sm text-muted-foreground">
            Standard <span className="font-mono text-foreground">grpc.reflection.v1alpha.ServerReflection</span> — hand-implemented
            against the reference descriptors (see <span className="font-mono text-foreground">Grpc/Dynamic/DynamicReflectionService.cs</span>),
            not defined in one of this repo's own .proto files. Any standard reflection client (grpcurl, Kreya, grpcui) discovers
            everything below with zero setup and no credentials — reflection is allow-anonymous.
          </p>
          <div>
            <p className="mb-1 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">List every service</p>
            <SnippetRow text={`grpcurl -plaintext localhost:${grpcPort} list`} />
          </div>
          <div>
            <p className="mb-1 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Describe a message</p>
            <SnippetRow text={`grpcurl -plaintext localhost:${grpcPort} describe streamforge.dynamic.v1.<MessageName>`} />
          </div>
        </CardContent>
      </Card>
    )
  }

  const fileName = STATIC_SERVICE_FILE[name]
  const proto = staticProtos.find((p) => p.name === fileName)

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-2">
          <CardTitle>{name}</CardTitle>
          <div className="flex items-center gap-2">
            {fileName && <Badge variant="outline">{fileName}</Badge>}
            {proto && <CopyButton text={proto.text} label="Copy .proto" />}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {proto ? (
          <ProtoView text={proto.text} />
        ) : (
          <Alert variant="destructive">
            <AlertDescription>Could not load {fileName ?? 'the .proto for this service'}.</AlertDescription>
          </Alert>
        )}
      </CardContent>
    </Card>
  )
}

// ============================================================================
// Sidebar rows.
// ============================================================================

const STATUS_DOT_CLASS: Record<string, string> = {
  Running: 'bg-primary',
  Enabled: 'bg-primary',
  Stopped: 'bg-muted-foreground',
  Disabled: 'bg-muted-foreground',
  Failed: 'bg-destructive',
}

interface EntityRowItem {
  key: string
  kind: 'source' | 'table' | 'pipeline'
  name: string
  disabled: boolean
  entity?: DynamicEntityMetaDto
  reason?: string
}

type Selection = { type: 'service'; name: string } | { type: 'entity'; entity: DynamicEntityMetaDto } | null

function ServiceRow({ name, selected, onSelect }: { name: string; selected: boolean; onSelect: () => void }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={cn(
        'flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm font-medium transition-colors',
        selected ? 'bg-muted text-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground',
      )}
    >
      <span className="truncate">{name}</span>
    </button>
  )
}

function EntityRow({ item, selected, onSelect }: { item: EntityRowItem; selected: boolean; onSelect: () => void }) {
  const button = (
    <button
      type="button"
      disabled={item.disabled}
      onClick={onSelect}
      className={cn(
        'flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-sm transition-colors',
        item.disabled ? 'cursor-not-allowed opacity-50' : 'hover:bg-muted',
        selected && 'bg-muted',
      )}
    >
      <span className={cn('inline-block size-1.5 shrink-0 rounded-full', STATUS_DOT_CLASS[item.entity?.status ?? ''] ?? 'bg-muted-foreground')} />
      <span className="min-w-0 flex-1">
        <span className="block truncate font-medium text-foreground">{item.name}</span>
        <span className="block truncate text-[11px] text-muted-foreground">
          {item.entity ? item.entity.messageName : item.reason}
        </span>
      </span>
      <Badge variant="outline" className="h-4 shrink-0 px-1.5 text-[9px] uppercase">
        {item.kind}
      </Badge>
    </button>
  )

  if (!item.disabled) return button

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>{button}</TooltipTrigger>
        <TooltipContent side="right" className="max-w-64">
          {item.reason}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

// ============================================================================
// Page
// ============================================================================

const STATIC_SERVICE_NAMES = ['SourceService', 'PipelineService', 'TableService', 'StreamService', 'DynamicStreamService', 'ServerReflection']

export function ApiExplorerPage() {
  const [grpcMeta, setGrpcMeta] = useState<GrpcMetaResponse | null>(null)
  const [staticProtos, setStaticProtos] = useState<StaticProtoDto[] | null>(null)
  const [allPipelines, setAllPipelines] = useState<PipelineDefinition[] | null>(null)
  const [allTables, setAllTables] = useState<TableDefinition[] | null>(null)
  const [selection, setSelection] = useState<Selection>(null)

  useEffect(() => {
    metaApi
      .grpc()
      .then(setGrpcMeta)
      .catch((err: unknown) => {
        toast.error(err instanceof Error ? err.message : 'Failed to load the gRPC surface.')
        setGrpcMeta({ grpcPort: 5299, services: STATIC_SERVICE_NAMES, dynamicEntities: [] })
      })
    metaApi
      .staticProtos()
      .then(setStaticProtos)
      .catch((err: unknown) => {
        toast.error(err instanceof Error ? err.message : 'Failed to load the static .proto files.')
        setStaticProtos([])
      })
    pipelinesApi.list().then(setAllPipelines).catch(() => setAllPipelines([]))
    tablesApi.list().then(setAllTables).catch(() => setAllTables([]))
  }, [])

  // Pipelines/tables whose SQL doesn't currently compile are excluded from grpcMeta.dynamicEntities
  // (mirrors what a real reflection client would see) — surfaced here as disabled rows instead of
  // silently vanishing from the list, so it's clear *why* they're not reflectable.
  const brokenPipelines = useMemo(() => {
    if (!allPipelines || !grpcMeta) return []
    const known = new Set(grpcMeta.dynamicEntities.filter((e) => e.kind === 'pipeline').map((e) => e.id))
    return allPipelines.filter((p) => !known.has(p.id))
  }, [allPipelines, grpcMeta])

  const brokenTables = useMemo(() => {
    if (!allTables || !grpcMeta) return []
    const known = new Set(grpcMeta.dynamicEntities.filter((e) => e.kind === 'table').map((e) => e.id))
    return allTables.filter((t) => !known.has(t.id))
  }, [allTables, grpcMeta])

  const entityRows = useMemo<EntityRowItem[]>(() => {
    if (!grpcMeta) return []
    const rows: EntityRowItem[] = grpcMeta.dynamicEntities.map((e) => ({
      key: e.entityKey,
      kind: e.kind,
      name: e.name,
      disabled: false,
      entity: e,
    }))
    for (const p of brokenPipelines) {
      rows.push({
        key: `pipeline:${p.id}:unreflectable`,
        kind: 'pipeline',
        name: p.name,
        disabled: true,
        reason:
          p.error?.trim() ||
          "This pipeline's SQL does not currently compile — reflection is unavailable until it's fixed.",
      })
    }
    for (const t of brokenTables) {
      rows.push({
        key: `table:${t.id}:unreflectable`,
        kind: 'table',
        name: t.name,
        disabled: true,
        reason:
          t.error?.trim() ||
          'This table has no compiled output schema yet — reflection is unavailable until it compiles.',
      })
    }
    const kindOrder: Record<EntityRowItem['kind'], number> = { source: 0, table: 1, pipeline: 2 }
    return rows.sort((a, b) => kindOrder[a.kind] - kindOrder[b.kind] || a.name.localeCompare(b.name))
  }, [grpcMeta, brokenPipelines, brokenTables])

  // Auto-select the first available row once data loads, so the page isn't blank on first visit.
  useEffect(() => {
    if (selection || !grpcMeta) return
    const firstEntity = grpcMeta.dynamicEntities[0]
    if (firstEntity) {
      setSelection({ type: 'entity', entity: firstEntity })
    } else if (grpcMeta.services[0]) {
      setSelection({ type: 'service', name: grpcMeta.services[0] })
    }
  }, [grpcMeta, selection])

  const loading = grpcMeta === null || staticProtos === null
  const grpcPort = grpcMeta?.grpcPort ?? 5299

  return (
    <div>
      <Topbar
        title="API Explorer"
        subtitle="Browse StreamForge's gRPC reflection surface: services, protobuf definitions, and live decoded data."
      />

      <div className="flex">
        <aside className="w-80 shrink-0 border-r border-border">
          <div className="sticky top-0 max-h-screen overflow-y-auto p-3">
            <div className="flex flex-col gap-4">
              <div>
                <div className="px-3 pb-1 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Services</div>
                <div className="flex flex-col gap-0.5">
                  {loading
                    ? Array.from({ length: 6 }).map((_, i) => <Skeleton key={i} className="mx-3 h-7 rounded-md" />)
                    : (grpcMeta?.services ?? []).map((name) => (
                        <ServiceRow
                          key={name}
                          name={name}
                          selected={selection?.type === 'service' && selection.name === name}
                          onSelect={() => setSelection({ type: 'service', name })}
                        />
                      ))}
                </div>
              </div>

              <div>
                <div className="px-3 pb-1 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Entities</div>
                <div className="flex flex-col gap-0.5">
                  {loading ? (
                    Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="mx-3 h-10 rounded-md" />)
                  ) : entityRows.length === 0 ? (
                    <p className="px-3 py-2 text-xs text-muted-foreground">No sources, tables, or pipelines yet.</p>
                  ) : (
                    entityRows.map((item) => (
                      <EntityRow
                        key={item.key}
                        item={item}
                        selected={selection?.type === 'entity' && !!item.entity && selection.entity.entityKey === item.entity.entityKey}
                        onSelect={() => item.entity && setSelection({ type: 'entity', entity: item.entity })}
                      />
                    ))
                  )}
                </div>
              </div>
            </div>
          </div>
        </aside>

        <div className="min-w-0 flex-1 p-6">
          {loading ? (
            <div className="flex flex-col gap-4">
              <Skeleton className="h-8 w-64" />
              <Skeleton className="h-48 w-full" />
              <Skeleton className="h-32 w-full" />
            </div>
          ) : selection === null ? (
            <Empty className="border border-dashed">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <Braces />
                </EmptyMedia>
                <EmptyTitle>Pick a service or entity</EmptyTitle>
                <EmptyDescription>Browse the list on the left to inspect its protobuf definition and live data.</EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : selection.type === 'service' ? (
            <ServiceDetail name={selection.name} staticProtos={staticProtos ?? []} grpcPort={grpcPort} />
          ) : (
            <div className="flex flex-col gap-4">
              <EntityDefinitionCard entity={selection.entity} />
              <ConnectCard entity={selection.entity} grpcPort={grpcPort} />
              <RestCard entity={selection.entity} />
              <LiveDataCard entity={selection.entity} />
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
