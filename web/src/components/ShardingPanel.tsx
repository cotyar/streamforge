import { useCallback, useEffect, useState } from 'react'
import { TriangleAlert } from 'lucide-react'
import { toast } from 'sonner'
import { tablesApi } from '../api/tables'
import type { ResultRow, RowValue, TableDefinition, TableShardView, TableShardsResponse } from '../api/types'
import { RoleGate } from './RoleGate'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Field, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Spinner } from '@/components/ui/spinner'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

/**
 * Plan 011 wave D — the console surface for SHARDED TABLES.
 *
 * Two things live here, and they are deliberately different in kind:
 *
 *  * **The ShardBy control** — which output columns a table's rows are keyed by. Off (empty) is the
 *    default and means today's behavior exactly. Turning it ON is not a display preference: it creates a
 *    per-key grain tier, and turning it OFF again DELETES that tier, which is why the toggle is a
 *    destructive-styled confirmation rather than a switch you brush past. The server owns every rule
 *    (columns must be compiled output fields; searchEnabled and MemoryOnly are refused; a sharded table
 *    cannot be renamed) and its message is surfaced verbatim — the reason a combination is refused is
 *    exactly what the reader needs, and paraphrasing it would only lose information.
 *
 *  * **The per-key lookup** — the query the whole feature exists for ("give me everything for this
 *    instrument"): that key's rows and its full version trail, from one grain, strictly consistent by
 *    construction. It ACTIVATES that one shard, which is the intended cost of asking about a key.
 *
 * WHY THE METRICS POLL IS SAFE, and why that is worth saying out loud: `GET /shards?limit=0` is answered
 * by the router and the directory and touches no shard at all. A console poll that fanned out across the
 * key set would wake every idle shard every few seconds, nothing would ever be swapped out, and the
 * feature would quietly stop working while every screen still looked right. Resident-vs-known is exactly
 * the pair of numbers that shows it working, so it is the pair shown biggest.
 */
export function ShardingPanel({
  table,
  canEdit,
  onApplyShardBy,
}: {
  table: TableDefinition
  canEdit: boolean
  /** Sends a full table update with the new shardBy (the PUT replaces the whole definition). */
  onApplyShardBy: (next: string[]) => Promise<void>
}) {
  const shardBy = table.shardBy ?? []
  const sharded = shardBy.length > 0

  const [applying, setApplying] = useState(false)
  const [info, setInfo] = useState<TableShardsResponse | null>(null)

  // Metrics poll. Only while sharded, and only the numbers — limit=0 skips serialising the key list,
  // which is O(distinct keys) and which nothing on this card needs.
  useEffect(() => {
    if (!sharded) {
      setInfo(null)
      return
    }
    let cancelled = false
    const tick = () => {
      tablesApi
        .shards(table.id, 0)
        .then((res) => {
          if (!cancelled) setInfo(res)
        })
        .catch(() => {
          /* transient — the next tick retries, and a failed poll must not raise a toast every 3s */
        })
    }
    tick()
    const timer = setInterval(tick, 3000)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [table.id, sharded])

  const toggleColumn = useCallback(
    async (column: string) => {
      const next = shardBy.includes(column) ? shardBy.filter((c) => c !== column) : [...shardBy, column]
      setApplying(true)
      try {
        await onApplyShardBy(next)
      } finally {
        setApplying(false)
      }
    },
    [shardBy, onApplyShardBy],
  )

  const residentPct =
    info && info.shardCount > 0 ? Math.round((info.residentShardCount / info.shardCount) * 100) : null

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <CardContent className="flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Key sharding</h3>
            <Badge variant={sharded ? 'default' : 'outline'}>{sharded ? `by ${shardBy.join(', ')}` : 'Off'}</Badge>
          </div>

          <span className="text-xs text-muted-foreground">
            {sharded
              ? 'Each key’s rows and version trail live in their own grain, which deactivates when idle — so history stops being resident without anything being deleted.'
              : 'Every row and every version trail stays resident in one grain, as usual.'}
          </span>

          <RoleGate min="Editor">
            <div className="flex flex-col gap-2 border-t border-border pt-3">
              <span className="text-[11px] text-muted-foreground">
                Shard by output column(s) — click to add or remove. Order matters: it is the key’s composition.
              </span>
              <div className="flex flex-wrap gap-1.5">
                {table.outputFields.length === 0 && (
                  <span className="text-xs text-muted-foreground">
                    This table has no compiled output columns yet — fix the SQL first.
                  </span>
                )}
                {table.outputFields.map((f) => {
                  const on = shardBy.includes(f.name)
                  return (
                    <Button
                      key={f.name}
                      type="button"
                      size="sm"
                      variant={on ? 'default' : 'outline'}
                      disabled={applying || !canEdit}
                      onClick={() => void toggleColumn(f.name)}
                    >
                      {f.name}
                    </Button>
                  )
                })}
              </div>
              {applying && (
                <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Spinner className="size-3.5" /> Rebuilding the shard tier…
                </span>
              )}
              <p className="text-[11px] text-muted-foreground">
                Changing this re-keys the tier: the existing shards are discarded and rebuilt from live traffic.
                Cannot be combined with the search index or with memory-only persistence, and a sharded table
                cannot be renamed — clear this first if you need to. Orleans only.
              </p>
            </div>
          </RoleGate>

          {sharded && info && (
            <div className="grid grid-cols-2 gap-x-4 gap-y-2 border-t border-border pt-3 sm:grid-cols-3">
              <ShardStat
                label="Shards resident"
                value={`${info.residentShardCount.toLocaleString()} / ${info.shardCount.toLocaleString()}`}
                hint={residentPct === null ? undefined : `${residentPct}% of the key space is in memory`}
                emphasis
              />
              <ShardStat label="Activations" value={info.activations.toLocaleString()} />
              <ShardStat label="Deactivations" value={info.deactivations.toLocaleString()} />
              <ShardStat label="Routed batches" value={info.routedBatches.toLocaleString()} />
              <ShardStat label="Routed deltas" value={info.routedDeltas.toLocaleString()} />
              <ShardStat label="Router sequence" value={info.routerSeq < 0 ? '—' : info.routerSeq.toLocaleString()} />
            </div>
          )}

          {sharded && info && !info.routerActive && (
            <Alert variant="destructive">
              <TriangleAlert />
              <AlertDescription>
                The shard router is not subscribed to this table’s delta stream, so nothing is being routed. Restart
                the table, or re-apply the shard columns.
              </AlertDescription>
            </Alert>
          )}
        </CardContent>
      </Card>

      {sharded && <ShardLookup table={table} shardBy={shardBy} />}
    </div>
  )
}

function ShardStat({
  label,
  value,
  hint,
  emphasis,
}: {
  label: string
  value: string
  hint?: string
  emphasis?: boolean
}) {
  return (
    <div className="flex flex-col">
      <span className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className={emphasis ? 'text-base font-medium text-foreground' : 'text-sm text-foreground'}>{value}</span>
      {hint && <span className="text-[11px] text-muted-foreground">{hint}</span>}
    </div>
  )
}

/**
 * The per-key read. One field per shard column, because the server refuses a lookup that omits one
 * rather than defaulting it to null — a missing column would silently address the "all nulls" shard,
 * which exists and would answer, so a typo would come back as a confident empty result.
 */
function ShardLookup({ table, shardBy }: { table: TableDefinition; shardBy: string[] }) {
  const [values, setValues] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState(false)
  const [view, setView] = useState<TableShardView | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setValues({})
    setView(null)
    setError(null)
  }, [table.id, shardBy.join(',')])

  const columns = view?.rows.length ? Object.keys(view.rows[0].row) : []

  async function lookup() {
    setLoading(true)
    setError(null)
    try {
      // Values are sent as typed as they were entered; the server derives the shard key with the same
      // codec the router uses on live deltas, and a numeric key column needs a number, not "12".
      const row: ResultRow = {}
      for (const column of shardBy) {
        const raw = values[column] ?? ''
        const asNumber = Number(raw)
        row[column] = raw !== '' && !Number.isNaN(asNumber) && String(asNumber) === raw.trim() ? asNumber : raw
      }
      setView(await tablesApi.shardLookup(table.id, row, 0))
    } catch (err) {
      setView(null)
      setError(err instanceof Error ? err.message : 'Lookup failed.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <Card>
      <CardContent className="flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Look up one key</h3>
          {view && <Badge variant="outline">applied seq {view.appliedSeq}</Badge>}
        </div>

        <span className="text-[11px] text-muted-foreground">
          Everything for this key — its rows and its full version trail — from one grain. Strictly consistent, and it
          wakes exactly this key’s shard and no other.
        </span>

        <div className="flex flex-wrap items-end gap-2">
          {shardBy.map((column) => (
            <Field key={column} className="w-40">
              <FieldLabel htmlFor={`shard-key-${column}`} className="text-xs font-normal text-muted-foreground">
                {column}
              </FieldLabel>
              <Input
                id={`shard-key-${column}`}
                value={values[column] ?? ''}
                disabled={loading}
                onChange={(e) => setValues((v) => ({ ...v, [column]: e.target.value }))}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') void lookup()
                }}
              />
            </Field>
          ))}
          <Button type="button" onClick={() => void lookup()} disabled={loading}>
            {loading && <Spinner data-icon="inline-start" />}
            Look up
          </Button>
        </div>

        {error && (
          <Alert variant="destructive">
            <TriangleAlert />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {view && !view.found && (
          <span className="text-xs text-muted-foreground">
            No shard for that key — nothing has ever been routed to it.
          </span>
        )}

        {view?.found && (
          <div className="flex flex-col gap-3">
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    {columns.map((c) => (
                      <TableHead key={c}>{c}</TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {view.rows.map((r, i) => (
                    <TableRow key={i}>
                      {columns.map((c) => (
                        <TableCell key={c} className="font-mono text-xs">
                          {formatValue(r.row[c])}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>

            {view.historyEnabled ? (
              view.history.map((entry) => (
                <div key={entry.rowKey} className="flex flex-col gap-1 border-t border-border pt-2">
                  <span className="text-[11px] text-muted-foreground">
                    {entry.totalVersions.toLocaleString()} version{entry.totalVersions === 1 ? '' : 's'}
                    {entry.retractionCount > 0 && `, ${entry.retractionCount.toLocaleString()} retraction(s)`}
                  </span>
                  <ol className="flex flex-col gap-1">
                    {entry.versions.map((v) => (
                      <li key={v.seq} className="flex gap-2 font-mono text-[11px] text-muted-foreground">
                        <span className="shrink-0 text-foreground">#{v.seq}</span>
                        <span className="shrink-0">{new Date(v.tsMs).toISOString().slice(11, 23)}</span>
                        <span className="truncate">{JSON.stringify(v.row)}</span>
                      </li>
                    ))}
                  </ol>
                </div>
              ))
            ) : (
              <span className="text-[11px] text-muted-foreground">
                Row history is off for this table, so the shard keeps rows but no version trail.
              </span>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function formatValue(v: RowValue): string {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

/** Re-exported for the page, which owns the toast on failure so the message the server wrote is the
 * message the reader sees. */
export function shardByErrorToast(err: unknown) {
  toast.error(err instanceof Error ? err.message : 'Failed to update shard columns.')
}
