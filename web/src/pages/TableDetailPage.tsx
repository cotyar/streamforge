import { useEffect, useMemo, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { Check, CircleAlert, Download, Play, Search, Trash2, TriangleAlert, Undo2, X } from 'lucide-react'
import { toast } from 'sonner'
import { tablesApi } from '../api/tables'
import { downloadCsv } from '../api/csv'
import type { UpdateTableRequest } from '../api/tables'
import { sourcesApi } from '../api/sources'
import { ApiError } from '../api/client'
import type {
  FieldDef,
  Metadata,
  ResultRow,
  RowValue,
  SinkSpec,
  SourceDefinition,
  SqlDiagnostic,
  Tags,
  TableDefinition,
  TableHistoryMode,
  TableMetrics,
  TableOutputField,
  TablePersistenceMode,
  TableSearchMode,
  TableSearchResponse,
} from '../api/types'
import { useAuth } from '../api/auth'
import { useTableRows } from '../hooks/useTableRows'
import { formatSql } from '../lib/sqlFormat'
import { useTableMetrics } from '../hooks/useTableMetrics'
import { Topbar } from '../components/Topbar'
import { StatusBadge } from '../components/StatusBadge'
import { SqlEditor } from '../components/SqlEditor'
import { RoleGate } from '../components/RoleGate'
import { MetadataEditor } from '../components/MetadataEditor'
import { RowHistorySheet } from '../components/RowHistorySheet'
import { DataflowPanel } from '../components/DataflowPanel'
import { SinksEditor } from '../components/SinksEditor'
import { ShardingPanel, shardByErrorToast } from '../components/ShardingPanel'
import { cn } from '@/lib/utils'
import { formatEpochMs, isEpochMsColumn } from '@/lib/format'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Field, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupInput } from '@/components/ui/input-group'
import { Switch } from '@/components/ui/switch'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Skeleton } from '@/components/ui/skeleton'
import { Spinner } from '@/components/ui/spinner'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Empty, EmptyDescription, EmptyHeader } from '@/components/ui/empty'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'

function isJsonValue(v: RowValue): v is Record<string, RowValue> | RowValue[] {
  return typeof v === 'object' && v !== null
}

function formatCell(v: RowValue): string {
  if (v === undefined || v === null) return '—'
  if (typeof v === 'number') return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(4)
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  if (isJsonValue(v)) return JSON.stringify(v)
  return v
}

function formatClock(ms: number): string {
  return new Date(ms).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5 rounded-lg border border-border bg-background/60 px-3 py-2">
      <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className="font-mono text-sm font-semibold text-foreground">{value}</span>
    </div>
  )
}

interface DisplayRow {
  key: string
  row: ResultRow
  weight: number
}

/** Row grid shared by the live materialized view and search results: mono numerics, sf-flash on
 * rows touched by the latest delta batch (live view only), and a weight column that only appears
 * when some row's weight is above 1 (rare — only visible mid-transition or on malformed dedupe). */
function RowsTable({
  outputFields,
  rows,
  flashKeys,
  emptyMessage,
  onRowClick,
}: {
  outputFields: FieldDef[]
  rows: DisplayRow[]
  flashKeys?: Set<string>
  emptyMessage: string
  /** When set, rows are clickable — used to open the row-history sheet (see RowHistorySheet /
   * SearchAndView). Absent (undefined) whenever the table's historyEnabled is false, so plain
   * tables never show a misleading pointer cursor. */
  onRowClick?: (row: ResultRow) => void
}) {
  const displayRows = rows.slice(0, 500)
  const showWeightColumn = rows.some((r) => r.weight > 1)

  return (
    <Card className="min-h-[16rem] flex-1 overflow-hidden py-0">
      {displayRows.length === 0 ? (
        <p className="px-4 py-10 text-center text-sm text-muted-foreground">{emptyMessage}</p>
      ) : (
        <div className="max-h-[28rem] overflow-auto">
          <Table className="min-w-max text-xs">
            <TableHeader className="sticky top-0 z-10 bg-card">
              <TableRow className="hover:bg-transparent">
                {outputFields.map((f) => (
                  <TableHead key={f.name} className="uppercase tracking-wide text-muted-foreground">
                    {f.name}
                  </TableHead>
                ))}
                {showWeightColumn && (
                  <TableHead className="text-right uppercase tracking-wide text-muted-foreground">Weight</TableHead>
                )}
              </TableRow>
            </TableHeader>
            <TableBody className="font-mono">
              {displayRows.map((r) => (
                <TableRow
                  key={r.key}
                  className={cn(flashKeys?.has(r.key) && 'sf-row-flash', onRowClick && 'cursor-pointer hover:bg-muted/40')}
                  onClick={onRowClick ? () => onRowClick(r.row) : undefined}
                >
                  {outputFields.map((f) => {
                    const v: RowValue | undefined = r.row[f.name]
                    const json = v !== undefined && isJsonValue(v)
                    const ts = typeof v === 'number' && (f.type === 'Timestamp' || isEpochMsColumn(f.name, v))
                    return (
                      <TableCell
                        key={f.name}
                        title={json ? formatCell(v) : ts ? String(v) : undefined}
                        className={cn(
                          typeof v === 'number' && !ts ? 'text-right text-foreground' : 'text-foreground/80',
                          json && 'max-w-56 truncate font-mono',
                        )}
                      >
                        {ts ? formatEpochMs(v as number) : formatCell(v)}
                      </TableCell>
                    )
                  })}
                  {showWeightColumn && <TableCell className="text-right text-foreground">{r.weight}</TableCell>}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </Card>
  )
}

/** Live materialized-view grid: sorted by the first output column, plus the live/metrics header. */
function MaterializedView({
  table,
  metrics,
  deltasInPerSec,
  onRowClick,
}: {
  table: TableDefinition
  /** Polled by SearchAndView, not here: the same numbers now also drive the config cards' warnings, and
   * this grid unmounts while a search is running — one poll shared beats two that come and go. */
  metrics: TableMetrics | null
  deltasInPerSec: number
  onRowClick?: (row: ResultRow) => void
}) {
  const { rows, live, flashKeys } = useTableRows(table.id, table.name)

  const sortedRows = useMemo(() => {
    const firstField = table.outputFields[0]
    if (!firstField) return rows
    const numeric = firstField.type === 'Double' || firstField.type === 'Long'
    return [...rows].sort((a, b) => {
      const av = a.row[firstField.name]
      const bv = b.row[firstField.name]
      if (numeric) return (Number(av) || 0) - (Number(bv) || 0)
      return String(av ?? '').localeCompare(String(bv ?? ''))
    })
  }, [rows, table.outputFields])

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span className="relative flex size-2">
            {live && <span className="absolute inline-flex size-full animate-ping rounded-full bg-primary opacity-60" />}
            <span className={cn('relative inline-flex size-2 rounded-full', live ? 'bg-primary' : 'bg-muted-foreground')} />
          </span>
          <span className="font-medium text-foreground">{live ? 'Live' : 'Connecting…'}</span>
          <span>·</span>
          <span>
            {sortedRows.length.toLocaleString()} row{sortedRows.length === 1 ? '' : 's'}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {metrics?.rebuilding && (
            <Badge variant="outline" className="border-warning/40 text-warning">
              Rebuilding
            </Badge>
          )}
          {/* Plan 012: the server renders the CSV (GET /rows.csv) rather than this grid exporting what it
              happens to hold — the grid is a capped live view, the download is the table. */}
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() => {
              void downloadCsv(`/api/tables/${table.id}/rows.csv`, `${table.name}.csv`).catch((err: unknown) =>
                toast.error(err instanceof Error ? err.message : 'Download failed.'),
              )
            }}
          >
            <Download data-icon="inline-start" /> CSV
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
        <Stat label="Rows" value={(metrics?.rowCount ?? sortedRows.length).toLocaleString()} />
        <Stat label="Deltas in/s" value={deltasInPerSec.toFixed(1)} />
        <Stat label="Deltas out (total)" value={(metrics?.deltasOut ?? 0).toLocaleString()} />
        <Stat label="Last update" value={metrics ? formatClock(metrics.lastUpdateMs) : '—'} />
      </div>

      <DataflowPanel table={table} metrics={metrics} />

      {sortedRows.length > 500 && (
        <p className="text-xs text-muted-foreground">
          Showing 500 of {sortedRows.length.toLocaleString()} rows.
        </p>
      )}

      <RowsTable
        outputFields={table.outputFields}
        rows={sortedRows}
        flashKeys={flashKeys}
        emptyMessage="Waiting for rows…"
        onRowClick={onRowClick}
      />
    </div>
  )
}

/** Debounces a fast-changing value (keystrokes) so effects depending on it don't fire on every one. */
function useDebounced<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value)
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs)
    return () => clearTimeout(timer)
  }, [value, delayMs])
  return debounced
}

const SEARCH_MODE_HINT: Record<TableSearchMode, string> = {
  Exact: 'Exact — matches full field values',
  Fuzzy: 'Fuzzy — typo-tolerant matching',
}

const PERSISTENCE_LABEL: Record<TablePersistenceMode, string> = {
  Batched: 'Batched',
  FireAndForget: 'Fire-and-forget',
  MemoryOnly: 'Memory-only',
  Journaled: 'Journaled',
}

const PERSISTENCE_HINT: Record<TablePersistenceMode, string> = {
  Batched: 'Written periodically; the write is awaited, so a flush briefly stalls the table and the stall grows with the row count.',
  FireAndForget: 'The write happens in the background; a crash loses whatever had not reached disk.',
  MemoryOnly: 'Never written — a restart brings this table back empty.',
  Journaled: 'Same durability as batched, but a flush writes only the rows that changed, compacting to a full snapshot once the journal grows past its limit.',
}

/** Search box above the materialized view: swaps the grid to search results while a query is
 * active, and (for Editors) exposes the per-table search-index config that the backend restarts
 * the table's pipeline to apply. */
function SearchAndView({
  table,
  canEdit,
  onTableChange,
}: {
  table: TableDefinition
  canEdit: boolean
  onTableChange: (t: TableDefinition) => void
}) {
  const [query, setQuery] = useState('')
  const debouncedQuery = useDebounced(query.trim(), 250)
  const isSearching = query.trim().length > 0

  // One metrics poll for this whole panel: the grid's counters AND the row-identity warning the history
  // / sharding cards render come from the same 2 s read (see TableMetrics.rowIdentityWarning — it is
  // derived from the definition server-side, and it rides the metrics object because that is where this
  // page already looks for "this table is not in the state you think it is", e.g. rebuilding).
  const { metrics, deltasInPerSec } = useTableMetrics(table.id)
  const rowIdentityWarning = metrics?.rowIdentityWarning ?? null

  const [searchResult, setSearchResult] = useState<TableSearchResponse | null>(null)
  const [searching, setSearching] = useState(false)
  const [searchNotEnabled, setSearchNotEnabled] = useState(false)

  // Config draft mirrors the persisted table so a failed update reverts cleanly; resynced whenever
  // a fresh table definition arrives (initial load, or after a successful config change elsewhere).
  const [configEnabled, setConfigEnabled] = useState(table.searchEnabled)
  const [configMode, setConfigMode] = useState<TableSearchMode>(table.searchMode)
  const [reindexing, setReindexing] = useState(false)

  useEffect(() => {
    setConfigEnabled(table.searchEnabled)
    setConfigMode(table.searchMode)
  }, [table.searchEnabled, table.searchMode])

  // History config draft — same resync-on-fresh-definition pattern as the search draft above.
  const [historyDraftEnabled, setHistoryDraftEnabled] = useState(table.historyEnabled)
  const [historyDraftMode, setHistoryDraftMode] = useState<TableHistoryMode>(table.historyMode)
  const [historyDraftLimit, setHistoryDraftLimit] = useState(table.historyLimit)
  const [historyDraftByField, setHistoryDraftByField] = useState<string | null>(table.historyByField)
  const [historyDraftWindowMs, setHistoryDraftWindowMs] = useState(table.historyWindowMs)
  const [historyApplying, setHistoryApplying] = useState(false)
  const [historyRow, setHistoryRow] = useState<ResultRow | null>(null)

  // Execution (parallelism) config draft — same resync-on-fresh-definition pattern as the search/
  // history drafts above.
  const [parallelismDraft, setParallelismDraft] = useState(table.parallelism)
  const [parallelismApplying, setParallelismApplying] = useState(false)

  useEffect(() => {
    setParallelismDraft(table.parallelism)
  }, [table.parallelism])

  // Persistence (durability) config draft — plan 008 W2.5, same resync-on-fresh-definition pattern
  // as the other quick-toggle drafts above. Absent on the wire means the pre-008 default.
  const [persistenceDraft, setPersistenceDraft] = useState<TablePersistenceMode>(table.persistence ?? 'Batched')
  const [flushMsDraft, setFlushMsDraft] = useState(table.flushMs ?? 0)
  // Plan 009 A2: compaction threshold, meaningful only for persistence 'Journaled'.
  const [journalMaxEntriesDraft, setJournalMaxEntriesDraft] = useState(table.journalMaxEntries ?? 0)
  const [persistenceApplying, setPersistenceApplying] = useState(false)

  useEffect(() => {
    setPersistenceDraft(table.persistence ?? 'Batched')
    setFlushMsDraft(table.flushMs ?? 0)
    setJournalMaxEntriesDraft(table.journalMaxEntries ?? 0)
  }, [table.persistence, table.flushMs, table.journalMaxEntries])

  // Plan 011 C2: row-retention draft — same resync-on-fresh-definition pattern as the drafts above.
  // Both bounds default to 0 = off, which is also what an older backend's absent field means.
  const [retentionMaxRowsDraft, setRetentionMaxRowsDraft] = useState(table.retentionMaxRows ?? 0)
  const [retentionTtlMsDraft, setRetentionTtlMsDraft] = useState(table.retentionTtlMs ?? 0)
  const [retentionApplying, setRetentionApplying] = useState(false)
  const retentionEnabled = retentionMaxRowsDraft > 0 || retentionTtlMsDraft > 0

  useEffect(() => {
    setRetentionMaxRowsDraft(table.retentionMaxRows ?? 0)
    setRetentionTtlMsDraft(table.retentionTtlMs ?? 0)
  }, [table.retentionMaxRows, table.retentionTtlMs])

  // Plan 009 B2: outbound sinks draft — same resync-on-fresh-definition pattern as the other
  // quick-toggle drafts above.
  const [sinksDraft, setSinksDraft] = useState<SinkSpec[]>(table.sinks ?? [])
  const [sinksApplying, setSinksApplying] = useState(false)

  useEffect(() => {
    setSinksDraft(table.sinks ?? [])
  }, [table.sinks])

  useEffect(() => {
    setHistoryDraftEnabled(table.historyEnabled)
    setHistoryDraftMode(table.historyMode)
    setHistoryDraftLimit(table.historyLimit)
    setHistoryDraftByField(table.historyByField)
    setHistoryDraftWindowMs(table.historyWindowMs)
  }, [table.historyEnabled, table.historyMode, table.historyLimit, table.historyByField, table.historyWindowMs])

  const numericOrTimestampFields = table.outputFields.filter(
    (f) => f.type === 'Double' || f.type === 'Long' || f.type === 'Timestamp',
  )

  useEffect(() => {
    if (!debouncedQuery) {
      setSearchResult(null)
      setSearchNotEnabled(false)
      setSearching(false)
      return
    }
    if (!table.searchEnabled) {
      setSearchResult(null)
      setSearchNotEnabled(true)
      setSearching(false)
      return
    }
    let cancelled = false
    setSearching(true)
    setSearchNotEnabled(false)
    tablesApi
      .search(table.id, debouncedQuery, 100)
      .then((res) => {
        if (cancelled) return
        setSearchResult(res)
      })
      .catch((err: unknown) => {
        if (cancelled) return
        // "Search is not enabled" is an expected state (e.g. a race with the toggle below), not a
        // real failure — surface it inline instead of a scary toast.
        if (err instanceof ApiError && err.status === 400) {
          setSearchResult(null)
          setSearchNotEnabled(true)
        } else {
          toast.error(err instanceof Error ? err.message : 'Search failed.')
        }
      })
      .finally(() => {
        if (!cancelled) setSearching(false)
      })
    return () => {
      cancelled = true
    }
    // Re-runs on table.searchMode too (not just searchEnabled): switching Exact/Fuzzy while a
    // query is active must re-query immediately, since the same text can match differently.
  }, [debouncedQuery, table.id, table.searchEnabled, table.searchMode])

  // The PUT endpoint replaces the whole table definition (see UpdateTableRequest's doc comment in
  // ../api/tables), so every quick-toggle here — search config, history config — must resend the
  // table's *entire* current config, not just the field being changed. Sending a partial body would
  // silently reset every omitted field to its request-DTO default (e.g. flipping this table's
  // history back off the moment someone toggles search), which is exactly the class of bug this
  // helper exists to prevent.
  function fullUpdateBody(overrides: Partial<UpdateTableRequest>): UpdateTableRequest {
    return {
      name: table.name,
      description: table.description,
      sql: table.sql,
      searchEnabled: table.searchEnabled,
      searchMode: table.searchMode,
      historyEnabled: table.historyEnabled,
      historyMode: table.historyMode,
      historyLimit: table.historyLimit,
      historyByField: table.historyByField,
      historyWindowMs: table.historyWindowMs,
      tags: table.tags,
      metadata: table.metadata,
      parallelism: table.parallelism,
      persistence: table.persistence ?? 'Batched',
      flushMs: table.flushMs ?? 0,
      journalMaxEntries: table.journalMaxEntries ?? 0,
      sinks: table.sinks ?? [],
      retentionMaxRows: table.retentionMaxRows ?? 0,
      retentionTtlMs: table.retentionTtlMs ?? 0,
      // Plan 011 D1/D2: carried like every other field here. Omitting it would be read by the server as
      // "leave as-is", which is survivable — but sending the table's real value keeps this helper's own
      // contract ("resend the ENTIRE current config") true, and it is what makes the sharding card's
      // toggle able to send a CLEARED list at all.
      shardBy: table.shardBy ?? [],
      ...overrides,
    }
  }

  /** Plan 011 D — see ShardingPanel. One PUT, and the server's own refusal (bad column, searchEnabled,
   * MemoryOnly, a rename that would strand the shards) is surfaced verbatim rather than paraphrased:
   * the reason a combination is refused is exactly what the reader needs. */
  async function applyShardBy(next: string[]) {
    try {
      onTableChange(await tablesApi.update(table.id, fullUpdateBody({ shardBy: next })))
    } catch (err) {
      shardByErrorToast(err)
    }
  }

  async function applyParallelism(next: number) {
    setParallelismDraft(next)
    setParallelismApplying(true)
    try {
      const saved = await tablesApi.update(table.id, fullUpdateBody({ parallelism: next }))
      onTableChange(saved)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to update execution settings.')
      setParallelismDraft(table.parallelism)
    } finally {
      setParallelismApplying(false)
    }
  }

  async function applyPersistence(nextMode: TablePersistenceMode, nextFlushMs: number, nextJournalMaxEntries: number) {
    const clampedFlushMs = Math.max(0, nextFlushMs)
    const clampedJournalMaxEntries = Math.max(0, nextJournalMaxEntries)
    setPersistenceDraft(nextMode)
    setFlushMsDraft(clampedFlushMs)
    setJournalMaxEntriesDraft(clampedJournalMaxEntries)
    setPersistenceApplying(true)
    try {
      const saved = await tablesApi.update(
        table.id,
        fullUpdateBody({ persistence: nextMode, flushMs: clampedFlushMs, journalMaxEntries: clampedJournalMaxEntries }),
      )
      onTableChange(saved)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to update persistence settings.')
      setPersistenceDraft(table.persistence ?? 'Batched')
      setFlushMsDraft(table.flushMs ?? 0)
      setJournalMaxEntriesDraft(table.journalMaxEntries ?? 0)
    } finally {
      setPersistenceApplying(false)
    }
  }

  /** Plan 011 C2. Both bounds go up together (one PUT, one restart) so a user setting a max-rows AND
   * a TTL does not restart the table twice — and so an invalid combination comes back as ONE 409 with
   * the server's own explanation (unsupported SQL shape, parallelism > 1), which is surfaced verbatim
   * rather than paraphrased: the reason retention is refused is exactly what the user needs to read. */
  async function applyRetention(nextMaxRows: number, nextTtlMs: number) {
    const clampedMaxRows = Math.max(0, nextMaxRows)
    const clampedTtlMs = Math.max(0, nextTtlMs)
    setRetentionMaxRowsDraft(clampedMaxRows)
    setRetentionTtlMsDraft(clampedTtlMs)
    setRetentionApplying(true)
    try {
      const saved = await tablesApi.update(
        table.id,
        fullUpdateBody({ retentionMaxRows: clampedMaxRows, retentionTtlMs: clampedTtlMs }),
      )
      onTableChange(saved)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to update retention settings.')
      setRetentionMaxRowsDraft(table.retentionMaxRows ?? 0)
      setRetentionTtlMsDraft(table.retentionTtlMs ?? 0)
    } finally {
      setRetentionApplying(false)
    }
  }

  async function applySinks(next: SinkSpec[]) {
    setSinksDraft(next)
    setSinksApplying(true)
    try {
      const saved = await tablesApi.update(table.id, fullUpdateBody({ sinks: next }))
      onTableChange(saved)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to update sinks.')
      setSinksDraft(table.sinks ?? [])
    } finally {
      setSinksApplying(false)
    }
  }

  async function applyConfig(nextEnabled: boolean, nextMode: TableSearchMode) {
    setConfigEnabled(nextEnabled)
    setConfigMode(nextMode)
    setReindexing(true)
    try {
      const saved = await tablesApi.update(table.id, fullUpdateBody({ searchEnabled: nextEnabled, searchMode: nextMode }))
      onTableChange(saved)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to update search settings.')
      setConfigEnabled(table.searchEnabled)
      setConfigMode(table.searchMode)
    } finally {
      setReindexing(false)
    }
  }

  async function applyHistoryConfig(overrides: {
    historyEnabled?: boolean
    historyMode?: TableHistoryMode
    historyLimit?: number
    historyByField?: string | null
    historyWindowMs?: number
  }) {
    const nextEnabled = overrides.historyEnabled ?? historyDraftEnabled
    const nextMode = overrides.historyMode ?? historyDraftMode
    const nextLimit = overrides.historyLimit ?? historyDraftLimit
    const nextByField = 'historyByField' in overrides ? (overrides.historyByField ?? null) : historyDraftByField
    const nextWindowMs = overrides.historyWindowMs ?? historyDraftWindowMs

    setHistoryDraftEnabled(nextEnabled)
    setHistoryDraftMode(nextMode)
    setHistoryDraftLimit(nextLimit)
    setHistoryDraftByField(nextByField)
    setHistoryDraftWindowMs(nextWindowMs)
    setHistoryApplying(true)
    try {
      const saved = await tablesApi.update(
        table.id,
        fullUpdateBody({
          historyEnabled: nextEnabled,
          historyMode: nextMode,
          historyLimit: nextLimit,
          historyByField: nextByField,
          historyWindowMs: nextWindowMs,
        }),
      )
      onTableChange(saved)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to update history settings.')
      setHistoryDraftEnabled(table.historyEnabled)
      setHistoryDraftMode(table.historyMode)
      setHistoryDraftLimit(table.historyLimit)
      setHistoryDraftByField(table.historyByField)
      setHistoryDraftWindowMs(table.historyWindowMs)
    } finally {
      setHistoryApplying(false)
    }
  }

  const searchRows = useMemo<DisplayRow[]>(
    () => (searchResult?.rows ?? []).map((r, i) => ({ key: String(i), row: r.row, weight: r.weight })),
    [searchResult],
  )

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <CardContent className="flex flex-col gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <InputGroup className="max-w-sm">
              <InputGroupAddon>
                <Search className="size-4" />
              </InputGroupAddon>
              <InputGroupInput
                id="tbl-search"
                aria-label="Search rows"
                placeholder="Search rows…"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
              />
              {query && (
                <InputGroupAddon align="inline-end">
                  <InputGroupButton type="button" aria-label="Clear search" onClick={() => setQuery('')}>
                    <X />
                  </InputGroupButton>
                </InputGroupAddon>
              )}
            </InputGroup>

            {table.searchEnabled ? (
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Badge variant="outline">{table.searchMode}</Badge>
                  </TooltipTrigger>
                  <TooltipContent side="top">{SEARCH_MODE_HINT[table.searchMode]}</TooltipContent>
                </Tooltip>
              </TooltipProvider>
            ) : (
              !canEdit && <span className="text-xs text-muted-foreground">Search is not enabled for this table.</span>
            )}

            {isSearching && !searchNotEnabled && (
              <span className="text-xs text-muted-foreground" aria-live="polite">
                {searching
                  ? 'Searching…'
                  : searchResult
                    ? `${searchResult.total.toLocaleString()} match${searchResult.total === 1 ? '' : 'es'}`
                    : null}
              </span>
            )}
          </div>

          <RoleGate min="Editor">
            <div className="flex flex-wrap items-center gap-3 border-t border-border pt-3">
              <label htmlFor="tbl-search-enabled" className="flex items-center gap-2 text-sm text-foreground">
                <Switch
                  id="tbl-search-enabled"
                  checked={configEnabled}
                  disabled={reindexing}
                  onCheckedChange={(checked) => void applyConfig(checked, configMode)}
                />
                Search index
              </label>
              <ToggleGroup
                type="single"
                variant="outline"
                size="sm"
                value={configMode}
                disabled={!configEnabled || reindexing}
                onValueChange={(v) => v && void applyConfig(configEnabled, v as TableSearchMode)}
                aria-label="Search mode"
              >
                <ToggleGroupItem value="Exact">Exact</ToggleGroupItem>
                <ToggleGroupItem value="Fuzzy">Fuzzy</ToggleGroupItem>
              </ToggleGroup>
              {reindexing ? (
                <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Spinner className="size-3.5" /> Reindexing…
                </span>
              ) : (
                !configEnabled && <span className="text-xs text-muted-foreground">Typo-tolerant fuzzy matching available.</span>
              )}
            </div>
          </RoleGate>
        </CardContent>
      </Card>

      <Card>
        <CardContent className="flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Row history</h3>
            <div className="flex items-center gap-1.5">
              {table.historyEnabled && rowIdentityWarning && <Badge variant="destructive">Degraded</Badge>}
              {table.historyEnabled && <Badge variant="outline">{table.historyMode}</Badge>}
            </div>
          </div>

          {!canEdit && !table.historyEnabled && (
            <span className="text-xs text-muted-foreground">Row history is not enabled for this table.</span>
          )}

          {/* The row-identity warning. History is on, the server accepted it, everything LOOKS fine — and
              the trail silently never forms because the GROUP BY / LATEST BY key could not be matched to
              an output column, so each version is keyed by the whole row and sits alone. That is the one
              failure this card cannot let a reader discover by themselves, so the server's own sentence
              (which names the keys and the fix) is shown verbatim rather than paraphrased. */}
          {table.historyEnabled && rowIdentityWarning && (
            <Alert variant="destructive">
              <TriangleAlert />
              <AlertDescription>{rowIdentityWarning}</AlertDescription>
            </Alert>
          )}

          <RoleGate min="Editor">
            <div className="flex flex-col gap-2 border-t border-border pt-3">
              <div className="flex flex-wrap items-center gap-3">
                <label htmlFor="tbl-history-enabled" className="flex items-center gap-2 text-sm text-foreground">
                  <Switch
                    id="tbl-history-enabled"
                    checked={historyDraftEnabled}
                    disabled={historyApplying}
                    onCheckedChange={(checked) => void applyHistoryConfig({ historyEnabled: checked })}
                  />
                  Enabled
                </label>

                <Select
                  value={historyDraftMode}
                  disabled={!historyDraftEnabled || historyApplying}
                  onValueChange={(v) => void applyHistoryConfig({ historyMode: v as TableHistoryMode })}
                >
                  <SelectTrigger className="w-32" aria-label="History mode">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      <SelectItem value="All">All</SelectItem>
                      <SelectItem value="LastN">Last N</SelectItem>
                      <SelectItem value="FirstN">First N</SelectItem>
                      <SelectItem value="MinBy">Min by</SelectItem>
                      <SelectItem value="MaxBy">Max by</SelectItem>
                    </SelectGroup>
                  </SelectContent>
                </Select>

                {(historyDraftMode === 'LastN' || historyDraftMode === 'FirstN') && (
                  <Input
                    type="number"
                    min={1}
                    aria-label="History version limit"
                    className="w-20"
                    value={historyDraftLimit}
                    disabled={!historyDraftEnabled || historyApplying}
                    onChange={(e) => setHistoryDraftLimit(Number(e.target.value) || 1)}
                    onBlur={() => void applyHistoryConfig({ historyLimit: historyDraftLimit })}
                  />
                )}

                {(historyDraftMode === 'MinBy' || historyDraftMode === 'MaxBy') && (
                  <Select
                    value={historyDraftByField ?? undefined}
                    disabled={!historyDraftEnabled || historyApplying || numericOrTimestampFields.length === 0}
                    onValueChange={(v) => void applyHistoryConfig({ historyByField: v })}
                  >
                    <SelectTrigger className="w-36" aria-label="History extremum field">
                      <SelectValue placeholder="Field…" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectGroup>
                        {numericOrTimestampFields.map((f) => (
                          <SelectItem key={f.name} value={f.name}>
                            {f.name}
                          </SelectItem>
                        ))}
                      </SelectGroup>
                    </SelectContent>
                  </Select>
                )}

                <Field orientation="horizontal" className="items-center gap-1.5">
                  <FieldLabel htmlFor="tbl-history-window" className="text-xs font-normal text-muted-foreground">
                    Window (ms)
                  </FieldLabel>
                  <Input
                    id="tbl-history-window"
                    type="number"
                    min={0}
                    step={1000}
                    className="w-28"
                    value={historyDraftWindowMs}
                    disabled={!historyDraftEnabled || historyApplying}
                    onChange={(e) => setHistoryDraftWindowMs(Number(e.target.value) || 0)}
                    onBlur={() => void applyHistoryConfig({ historyWindowMs: historyDraftWindowMs })}
                  />
                </Field>

                {historyApplying && (
                  <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                    <Spinner className="size-3.5" /> Applying…
                  </span>
                )}
              </div>

              <p className="text-[11px] text-muted-foreground">
                {historyDraftMode === 'All' && 'Retains every assertion version per row identity.'}
                {(historyDraftMode === 'LastN' || historyDraftMode === 'FirstN') &&
                  `Retains the ${historyDraftMode === 'LastN' ? 'most' : 'least'} recent ${historyDraftLimit} version${historyDraftLimit === 1 ? '' : 's'} per row identity.`}
                {(historyDraftMode === 'MinBy' || historyDraftMode === 'MaxBy') &&
                  `Retains the ${historyDraftMode === 'MinBy' ? 'minimum' : 'maximum'} and latest version per row identity, by ${historyDraftByField ?? 'the selected field'}.`}
                {' Window 0 = unbounded.'}
                {historyDraftEnabled && ' Applying resets the accumulated history.'}
              </p>
            </div>
          </RoleGate>
        </CardContent>
      </Card>

      <Card>
        <CardContent className="flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Execution</h3>
            <Badge variant="outline">{table.parallelism === 1 ? 'Single' : `${table.parallelism}-way parallel`}</Badge>
          </div>

          {!canEdit && (
            <span className="text-xs text-muted-foreground">
              {table.parallelism === 1
                ? 'This table runs single-partitioned.'
                : `This table runs partitioned across ${table.parallelism} partitions.`}
            </span>
          )}

          <RoleGate min="Editor">
            <div className="flex flex-wrap items-center gap-3 border-t border-border pt-3">
              <ToggleGroup
                type="single"
                variant="outline"
                size="sm"
                value={parallelismDraft === 1 ? 'single' : 'parallel'}
                disabled={parallelismApplying}
                onValueChange={(v) => {
                  if (!v) return
                  if (v === 'single') void applyParallelism(1)
                  else if (parallelismDraft === 1) void applyParallelism(2)
                }}
                aria-label="Execution mode"
              >
                <ToggleGroupItem value="single">Single (1)</ToggleGroupItem>
                <ToggleGroupItem value="parallel">Parallel</ToggleGroupItem>
              </ToggleGroup>

              {parallelismDraft > 1 && (
                <Select
                  value={String(parallelismDraft)}
                  disabled={parallelismApplying}
                  onValueChange={(v) => void applyParallelism(Number(v))}
                >
                  <SelectTrigger className="w-20" aria-label="Partition count">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      {Array.from({ length: 15 }, (_, i) => i + 2).map((n) => (
                        <SelectItem key={n} value={String(n)}>
                          {n}
                        </SelectItem>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
              )}

              {parallelismApplying && (
                <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Spinner className="size-3.5" /> Restarting…
                </span>
              )}
            </div>

            <p className="text-[11px] text-muted-foreground">
              Single runs the whole table on one grain. Parallel deploys a partitioned dataflow graph (2–16
              partitions) so a hot stage no longer blocks the rest of the table. Changing this restarts the table.
            </p>
          </RoleGate>
        </CardContent>
      </Card>

      <Card>
        <CardContent className="flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Persistence</h3>
            <Badge variant={persistenceDraft === 'MemoryOnly' ? 'destructive' : 'outline'}>
              {PERSISTENCE_LABEL[persistenceDraft]}
            </Badge>
          </div>

          {!canEdit && (
            <span className="text-xs text-muted-foreground">{PERSISTENCE_HINT[table.persistence ?? 'Batched']}</span>
          )}

          <RoleGate min="Editor">
            <div className="flex flex-wrap items-center gap-3 border-t border-border pt-3">
              <ToggleGroup
                type="single"
                variant="outline"
                size="sm"
                value={persistenceDraft}
                disabled={persistenceApplying}
                onValueChange={(v) => v && void applyPersistence(v as TablePersistenceMode, flushMsDraft, journalMaxEntriesDraft)}
                aria-label="Persistence mode"
              >
                <ToggleGroupItem value="Batched">Batched</ToggleGroupItem>
                <ToggleGroupItem value="FireAndForget">Fire-and-forget</ToggleGroupItem>
                <ToggleGroupItem value="MemoryOnly">Memory-only</ToggleGroupItem>
                <ToggleGroupItem value="Journaled">Journaled</ToggleGroupItem>
              </ToggleGroup>

              {persistenceDraft !== 'MemoryOnly' && (
                <Field orientation="horizontal" className="items-center gap-1.5">
                  <FieldLabel htmlFor="tbl-flush-ms" className="text-xs font-normal text-muted-foreground">
                    Flush (ms)
                  </FieldLabel>
                  <Input
                    id="tbl-flush-ms"
                    type="number"
                    min={0}
                    step={500}
                    className="w-24"
                    value={flushMsDraft}
                    disabled={persistenceApplying}
                    onChange={(e) => setFlushMsDraft(Math.max(0, Number(e.target.value) || 0))}
                    onBlur={() => void applyPersistence(persistenceDraft, flushMsDraft, journalMaxEntriesDraft)}
                  />
                </Field>
              )}

              {persistenceDraft === 'Journaled' && (
                <Field orientation="horizontal" className="items-center gap-1.5">
                  <FieldLabel htmlFor="tbl-journal-max-entries" className="text-xs font-normal text-muted-foreground">
                    Compact past (entries)
                  </FieldLabel>
                  <Input
                    id="tbl-journal-max-entries"
                    type="number"
                    min={0}
                    step={100}
                    className="w-24"
                    value={journalMaxEntriesDraft}
                    disabled={persistenceApplying}
                    onChange={(e) => setJournalMaxEntriesDraft(Math.max(0, Number(e.target.value) || 0))}
                    onBlur={() => void applyPersistence(persistenceDraft, flushMsDraft, journalMaxEntriesDraft)}
                  />
                </Field>
              )}

              {persistenceApplying && (
                <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Spinner className="size-3.5" /> Restarting…
                </span>
              )}
            </div>

            <p className="text-[11px] text-muted-foreground">
              {PERSISTENCE_HINT[persistenceDraft]}
              {persistenceDraft !== 'MemoryOnly' && ' Flush 0 = 2000ms default.'}
              {persistenceDraft === 'Journaled' && ' Compaction threshold 0 = a sensible default.'}
              {' Changing this restarts the table.'}
            </p>

            {persistenceDraft === 'MemoryOnly' && (
              <Alert variant="destructive">
                <TriangleAlert />
                <AlertDescription>
                  Memory-only rows are never written to storage — a restart or crash brings this table back empty.
                </AlertDescription>
              </Alert>
            )}
          </RoleGate>
        </CardContent>
      </Card>

      {/* Plan 011 C2 — row retention. Deliberately its own card, next to Persistence rather than inside
          it: persistence decides how the rows REACH storage, retention decides which rows EXIST at all.
          The alert is not decoration — a table with a bound is a bounded view of its SQL's relation, and
          that is a change in results the person turning it on has to see. */}
      <Card>
        <CardContent className="flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Row retention</h3>
            <Badge variant={retentionEnabled ? 'destructive' : 'outline'}>
              {retentionEnabled ? 'Bounded view' : 'Unbounded'}
            </Badge>
          </div>

          {!canEdit && (
            <span className="text-xs text-muted-foreground">
              {retentionEnabled
                ? `Keeps ${retentionMaxRowsDraft > 0 ? `${retentionMaxRowsDraft} rows` : 'every row'}${
                    retentionTtlMsDraft > 0 ? ` newer than ${retentionTtlMsDraft} ms of event time` : ''
                  }; older rows are retracted.`
                : 'This table holds every row its SQL produces — no bound.'}
            </span>
          )}

          <RoleGate min="Editor">
            <div className="flex flex-wrap items-center gap-3 border-t border-border pt-3">
              <Field orientation="horizontal" className="items-center gap-1.5">
                <FieldLabel htmlFor="tbl-retention-max-rows" className="text-xs font-normal text-muted-foreground">
                  Max rows
                </FieldLabel>
                <Input
                  id="tbl-retention-max-rows"
                  type="number"
                  min={0}
                  step={100}
                  className="w-28"
                  value={retentionMaxRowsDraft}
                  disabled={retentionApplying}
                  onChange={(e) => setRetentionMaxRowsDraft(Math.max(0, Number(e.target.value) || 0))}
                  onBlur={() => void applyRetention(retentionMaxRowsDraft, retentionTtlMsDraft)}
                />
              </Field>

              <Field orientation="horizontal" className="items-center gap-1.5">
                <FieldLabel htmlFor="tbl-retention-ttl-ms" className="text-xs font-normal text-muted-foreground">
                  Max age (ms, event time)
                </FieldLabel>
                <Input
                  id="tbl-retention-ttl-ms"
                  type="number"
                  min={0}
                  step={1000}
                  className="w-32"
                  value={retentionTtlMsDraft}
                  disabled={retentionApplying}
                  onChange={(e) => setRetentionTtlMsDraft(Math.max(0, Number(e.target.value) || 0))}
                  onBlur={() => void applyRetention(retentionMaxRowsDraft, retentionTtlMsDraft)}
                />
              </Field>

              {retentionApplying && (
                <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Spinner className="size-3.5" /> Restarting…
                </span>
              )}
            </div>

            <p className="text-[11px] text-muted-foreground">
              0 = unbounded (the default). Eviction is oldest-first by the row&apos;s event timestamp, and age is
              measured against the newest event this table has seen — not the wall clock, so a stalled input ages
              nothing out. Unavailable for SQL with joins, set operations, derived sources or GROUP BY/aggregates,
              and for parallelism &gt; 1. Changing this restarts the table.
            </p>

            {retentionEnabled && (
              <Alert variant="destructive">
                <TriangleAlert />
                <AlertDescription>
                  This table is a bounded view, not the full relation its SQL describes: rows past the bound are
                  evicted with real retractions, so downstream tables, sinks, search and row history stay
                  consistent — but the evicted rows, and their history, are gone.
                </AlertDescription>
              </Alert>
            )}
          </RoleGate>
        </CardContent>
      </Card>

      {/* Plan 011 wave D — key sharding, plus the per-key lookup it exists for. Its own component
          because it is two related surfaces (a config control and a live read) rather than one card,
          and because the metrics it polls have a rule attached: they must never wake a shard. */}
      <ShardingPanel
        table={table}
        canEdit={canEdit}
        rowIdentityWarning={rowIdentityWarning}
        onApplyShardBy={applyShardBy}
      />

      <Card>
        <CardContent className="flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Sinks</h3>
            {sinksApplying && (
              <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <Spinner className="size-3.5" /> Applying…
              </span>
            )}
          </div>

          {!canEdit && sinksDraft.length === 0 && <span className="text-xs text-muted-foreground">Nothing is republished.</span>}

          {canEdit ? (
            <SinksEditor value={sinksDraft} onChange={(next) => void applySinks(next)} isEdit disabled={sinksApplying} />
          ) : (
            sinksDraft.map((s, i) => (
              <div key={i} className="flex items-center justify-between rounded-lg border border-border px-3 py-2 text-xs">
                <span>
                  <Badge variant="outline" className="mr-2">
                    {s.kind}
                  </Badge>
                  {s.nats?.subject}
                </span>
                <Badge variant={s.enabled ? 'default' : 'secondary'}>{s.enabled ? 'enabled' : 'disabled'}</Badge>
              </div>
            ))
          )}
        </CardContent>
      </Card>

      {isSearching ? (
        searchNotEnabled ? (
          <Empty className="border border-dashed">
            <EmptyHeader>
              <EmptyDescription>
                {canEdit
                  ? "Search is not enabled for this table. Turn on the search index above, then search again."
                  : 'Search is not enabled for this table.'}
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <RowsTable
            outputFields={table.outputFields}
            rows={searchRows}
            emptyMessage={searching ? 'Searching…' : 'No matches.'}
            onRowClick={table.historyEnabled ? setHistoryRow : undefined}
          />
        )
      ) : (
        <MaterializedView
          table={table}
          metrics={metrics}
          deltasInPerSec={deltasInPerSec}
          onRowClick={table.historyEnabled ? setHistoryRow : undefined}
        />
      )}

      <RowHistorySheet
        open={historyRow !== null}
        onOpenChange={(open) => !open && setHistoryRow(null)}
        tableId={table.id}
        row={historyRow}
      />
    </div>
  )
}

export function TableDetailPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const { hasRole } = useAuth()
  const isNew = id === 'new' || !id
  const canEdit = hasRole('Editor')

  const [table, setTable] = useState<TableDefinition | null>(null)
  const [loading, setLoading] = useState(!isNew)
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [sql, setSql] = useState('')
  const [searchEnabled, setSearchEnabled] = useState(false)
  const [searchMode, setSearchMode] = useState<TableSearchMode>('Exact')
  const [tags, setTags] = useState<Tags>([])
  const [metadata, setMetadata] = useState<Metadata>({})

  const [diagnostics, setDiagnostics] = useState<SqlDiagnostic[] | null>(null)
  const [planSummary, setPlanSummary] = useState<string | null>(null)
  const [outputSchema, setOutputSchema] = useState<TableOutputField[]>([])
  const [validatedStreamInputs, setValidatedStreamInputs] = useState<string[]>([])
  const [validatedTableInputs, setValidatedTableInputs] = useState<string[]>([])
  const [validating, setValidating] = useState(false)
  const [saving, setSaving] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)

  const [sources, setSources] = useState<SourceDefinition[]>([])
  const [otherTables, setOtherTables] = useState<TableDefinition[]>([])

  // Real sources plus every other table exposed as a pseudo-source (its output schema mapped to
  // SourceDefinition-shaped fields) so the SQL editor's FROM/JOIN autocomplete suggests tables the
  // same way it suggests streams. The table currently being edited is excluded to avoid offering a
  // self-reference. Fetched once here so both the editor and any future consumer share one request.
  useEffect(() => {
    let cancelled = false
    sourcesApi
      .list()
      .then((list) => {
        if (!cancelled) setSources(list)
      })
      .catch(() => {
        if (!cancelled) setSources([])
      })
    tablesApi
      .list()
      .then((list) => {
        if (!cancelled) setOtherTables(list)
      })
      .catch(() => {
        if (!cancelled) setOtherTables([])
      })
    return () => {
      cancelled = true
    }
  }, [])

  const editorSources = useMemo<SourceDefinition[]>(() => {
    const pseudo: SourceDefinition[] = otherTables
      .filter((t) => t.id !== id)
      .map((t) => ({
        name: t.name,
        description: t.description,
        fields: t.outputFields,
        generatorProfile: 'generic',
        eventsPerSecond: 0,
        enabled: true,
        tags: [],
        metadata: {},
      }))
    return [...sources, ...pseudo]
  }, [sources, otherTables, id])

  useEffect(() => {
    if (isNew) {
      setTable(null)
      setName('')
      setDescription('')
      setSql('')
      setSearchEnabled(false)
      setSearchMode('Exact')
      setTags([])
      setMetadata({})
      setLoading(false)
      return
    }
    setLoading(true)
    tablesApi
      .get(id!)
      .then((t) => {
        setTable(t)
        setName(t.name)
        setDescription(t.description)
        setSql(t.sql)
        setTags(t.tags)
        setMetadata(t.metadata)
      })
      .finally(() => setLoading(false))
  }, [id, isNew])

  // Keep the left-form search draft in sync with whatever the backend last confirmed — including
  // updates made via the right panel's quick toggle (SearchAndView calls setTable directly, not
  // through handleSave), so a subsequent Save never silently reverts a change made there.
  useEffect(() => {
    if (table) {
      setSearchEnabled(table.searchEnabled)
      setSearchMode(table.searchMode)
    }
  }, [table])

  useEffect(() => {
    if (!sql.trim()) {
      setDiagnostics(null)
      setPlanSummary(null)
      setOutputSchema([])
      setValidatedStreamInputs([])
      setValidatedTableInputs([])
      return
    }
    setValidating(true)
    const timer = setTimeout(() => {
      tablesApi
        .validate({ sql })
        .then((res) => {
          setDiagnostics(res.diagnostics)
          if (res.ok) {
            setPlanSummary(res.planSummary)
            setOutputSchema(res.outputSchema)
            setValidatedStreamInputs(res.streamInputs)
            setValidatedTableInputs(res.tableInputs)
          } else {
            setPlanSummary(null)
            setOutputSchema([])
            setValidatedStreamInputs([])
            setValidatedTableInputs([])
          }
        })
        .catch(() => {
          setDiagnostics(null)
          setPlanSummary(null)
          setOutputSchema([])
          setValidatedStreamInputs([])
          setValidatedTableInputs([])
        })
        .finally(() => setValidating(false))
    }, 500)
    return () => clearTimeout(timer)
  }, [sql])

  // Baseline for both "is the editor dirty" and Revert: the persisted copy for an existing table,
  // or '' for a new/unsaved one — no new persisted state, per plan.
  const baselineSql = table?.sql ?? ''
  const editorDirty = sql !== baselineSql

  function handleRevert() {
    setSql(baselineSql)
  }

  function handleFormat() {
    setSql((prev) => formatSql(prev))
  }

  async function handleSave(startAfter: boolean) {
    setFormError(null)
    if (!name.trim()) {
      setFormError('Name is required.')
      return
    }
    setSaving(true)
    try {
      // History config isn't editable from this form (see the right-panel "Row history" card, which
      // applies its own changes immediately) — carry the currently-persisted values through so a
      // plain Save never resets them back to the request DTO's defaults (history-off). Same for
      // parallelism, persistence/flushMs/journalMaxEntries, sinks and retention — editable only via
      // the right-panel "Execution", "Persistence", "Sinks" and "Row retention" cards.
      const body = {
        name: name.trim(),
        description,
        sql,
        searchEnabled,
        searchMode,
        historyEnabled: table?.historyEnabled ?? false,
        historyMode: table?.historyMode ?? ('All' as TableHistoryMode),
        historyLimit: table?.historyLimit ?? 10,
        historyByField: table?.historyByField ?? null,
        historyWindowMs: table?.historyWindowMs ?? 0,
        tags,
        metadata,
        parallelism: table?.parallelism ?? 1,
        persistence: table?.persistence ?? ('Batched' as TablePersistenceMode),
        flushMs: table?.flushMs ?? 0,
        journalMaxEntries: table?.journalMaxEntries ?? 0,
        retentionMaxRows: table?.retentionMaxRows ?? 0,
        retentionTtlMs: table?.retentionTtlMs ?? 0,
        shardBy: table?.shardBy ?? [],
        sinks: table?.sinks ?? [],
      }
      let saved: TableDefinition
      if (isNew) {
        saved = await tablesApi.create(body)
      } else {
        saved = await tablesApi.update(id!, body)
      }
      if (startAfter && saved.status !== 'Running') {
        saved = await tablesApi.start(saved.id)
        if (saved.status === 'Failed' && saved.error) toast.error(saved.error)
      }
      setTable(saved)
      if (isNew) navigate(`/tables/${saved.id}`, { replace: true })
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to save table.')
    } finally {
      setSaving(false)
    }
  }

  async function handleDelete() {
    if (!table) return
    setSaving(true)
    try {
      await tablesApi.remove(table.id)
      navigate('/tables', { replace: true })
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to delete table.')
      setSaving(false)
    }
  }

  const currentStatus = table?.status ?? 'Stopped'

  if (loading) {
    return (
      <div className="p-8">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="mt-4 h-96 w-full" />
      </div>
    )
  }

  return (
    <div>
      <Topbar
        title={isNew ? 'New table' : name || 'Table'}
        subtitle={isNew ? 'Define a windowless materialized view' : table?.id}
        action={!isNew && <StatusBadge status={currentStatus} />}
      />

      <div className="grid grid-cols-1 gap-6 p-8 xl:grid-cols-2">
        {/* LEFT */}
        <div className="flex flex-col gap-4">
          <Card>
            <CardContent className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <Field>
                <FieldLabel htmlFor="tbl-name">Name</FieldLabel>
                <Input
                  id="tbl-name"
                  value={name}
                  disabled={!canEdit}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="buy_volume"
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="tbl-description">Description</FieldLabel>
                <Input
                  id="tbl-description"
                  value={description}
                  disabled={!canEdit}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="Running buy-side volume per symbol"
                />
              </Field>

              <RoleGate min="Editor">
                <div className="flex flex-wrap items-center gap-3 sm:col-span-2">
                  <label htmlFor="tbl-form-search-enabled" className="flex items-center gap-2 text-sm text-foreground">
                    <Switch
                      id="tbl-form-search-enabled"
                      checked={searchEnabled}
                      onCheckedChange={setSearchEnabled}
                    />
                    Search index
                  </label>
                  <ToggleGroup
                    type="single"
                    variant="outline"
                    size="sm"
                    value={searchMode}
                    disabled={!searchEnabled}
                    onValueChange={(v) => v && setSearchMode(v as TableSearchMode)}
                    aria-label="Search mode"
                  >
                    <ToggleGroupItem value="Exact">Exact</ToggleGroupItem>
                    <ToggleGroupItem value="Fuzzy">Fuzzy</ToggleGroupItem>
                  </ToggleGroup>
                  <span className="text-xs text-muted-foreground">Applied on Save{isNew ? '' : ' (restarts the table)'}.</span>
                </div>
              </RoleGate>
            </CardContent>
          </Card>

          <MetadataEditor
            key={table?.id ?? 'new'}
            initialTags={tags}
            initialMetadata={metadata}
            onChange={(t, m) => {
              setTags(t)
              setMetadata(m)
            }}
            readOnly={!canEdit}
          />

          <SqlEditor
            value={sql}
            onChange={setSql}
            diagnostics={diagnostics ?? []}
            readOnly={!canEdit}
            sources={editorSources}
            onFormat={canEdit ? handleFormat : undefined}
            toolbarEnd={
              canEdit && (
                <Button type="button" variant="outline" size="sm" onClick={handleRevert} disabled={!editorDirty}>
                  <Undo2 data-icon="inline-start" /> Revert
                </Button>
              )
            }
          />

          <Card>
            <CardContent>
              <h3 className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">Validation</h3>
              {validating ? (
                <p className="text-sm text-muted-foreground">Validating…</p>
              ) : diagnostics === null ? (
                <p className="text-sm text-muted-foreground">Start typing SQL to validate.</p>
              ) : diagnostics.length === 0 ? (
                <div className="flex flex-col gap-3">
                  <Alert>
                    <Check className="text-primary" />
                    <AlertDescription>Valid{planSummary ? ` — ${planSummary}` : ''}</AlertDescription>
                  </Alert>

                  {outputSchema.length > 0 && (
                    <div>
                      <h4 className="mb-1.5 text-xs font-medium uppercase tracking-wide text-muted-foreground">Output schema</h4>
                      <div className="overflow-hidden rounded-lg border border-border">
                        <Table className="text-xs">
                          <TableHeader>
                            <TableRow className="hover:bg-transparent">
                              <TableHead>Field</TableHead>
                              <TableHead>Type</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody className="font-mono">
                            {outputSchema.map((f, i) => (
                              <TableRow key={`${f.name}-${i}`}>
                                <TableCell className="text-foreground">{f.name}</TableCell>
                                <TableCell className="text-muted-foreground">{f.kind}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    </div>
                  )}

                  {(validatedStreamInputs.length > 0 || validatedTableInputs.length > 0) && (
                    <div className="flex flex-wrap gap-1.5">
                      {validatedStreamInputs.map((s) => (
                        <Badge key={`s-${s}`} variant="outline" className="text-muted-foreground">
                          {s}
                        </Badge>
                      ))}
                      {validatedTableInputs.map((t) => (
                        <Badge key={`t-${t}`} variant="secondary">
                          {t}
                        </Badge>
                      ))}
                    </div>
                  )}
                </div>
              ) : (
                <Alert variant="destructive">
                  <CircleAlert />
                  <AlertDescription>
                    <ul className="flex flex-col gap-1.5">
                      {diagnostics.map((d, i) => (
                        <li key={i} className="flex items-start gap-2 text-xs">
                          {d.severity === 'Error' ? (
                            <CircleAlert className="mt-0.5 size-3.5 shrink-0" />
                          ) : (
                            <TriangleAlert className="mt-0.5 size-3.5 shrink-0 text-warning" />
                          )}
                          <span>
                            <span className="font-mono text-muted-foreground">
                              {d.line}:{d.column}
                            </span>{' '}
                            {d.message}
                          </span>
                        </li>
                      ))}
                    </ul>
                  </AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>

          {formError && (
            <Alert variant="destructive">
              <AlertDescription>{formError}</AlertDescription>
            </Alert>
          )}

          <RoleGate min="Editor">
            <div className="flex flex-wrap gap-2">
              <Button onClick={() => void handleSave(false)} disabled={saving}>
                {saving && <Spinner data-icon="inline-start" />}
                {saving ? 'Saving…' : 'Save'}
              </Button>
              <Button variant="outline" onClick={() => void handleSave(true)} disabled={saving}>
                <Play data-icon="inline-start" /> Save & start
              </Button>
              {!isNew && (
                <Button
                  variant="outline"
                  onClick={() => setConfirmDelete(true)}
                  disabled={saving}
                  className="ml-auto hover:border-destructive/40 hover:text-destructive"
                >
                  <Trash2 data-icon="inline-start" /> Delete
                </Button>
              )}
            </div>
          </RoleGate>
        </div>

        {/* RIGHT */}
        <div className="flex flex-col gap-4">
          {isNew || !table ? (
            <Empty className="h-full border border-dashed">
              <EmptyHeader>
                <EmptyDescription>Save the table to see its materialized view here.</EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <SearchAndView table={table} canEdit={canEdit} onTableChange={setTable} />
          )}
        </div>
      </div>

      <AlertDialog open={confirmDelete} onOpenChange={setConfirmDelete}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete table?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently removes <span className="font-medium text-foreground">{name}</span> and its materialized rows. Tables
              that depend on it must be stopped first.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => {
                setConfirmDelete(false)
                void handleDelete()
              }}
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
