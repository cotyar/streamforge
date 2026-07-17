import { useEffect, useState } from 'react'
import { History } from 'lucide-react'
import { tablesApi } from '../api/tables'
import type { HistoryVersion, ResultRow, RowValue, TableHistoryResponse } from '../api/types'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Spinner } from '@/components/ui/spinner'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia } from '@/components/ui/empty'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'

function formatValue(v: RowValue): string {
  if (v === undefined || v === null) return '—'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

function formatTimestamp(ms: number): string {
  return new Date(ms).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

/** One version's row values, with fields that changed vs. the previous (older) version subtly
 * highlighted — `previous` is the chronologically-earlier neighbor, i.e. versions[i + 1] in the
 * newest-first array this component receives. */
function VersionRow({ version, previous }: { version: HistoryVersion; previous: ResultRow | null }) {
  const fields = Object.keys(version.row)
  return (
    <div className="rounded-lg border border-border p-3">
      <div className="mb-2 flex items-center justify-between">
        <span className="font-mono text-xs text-foreground">{formatTimestamp(version.tsMs)}</span>
        <Badge variant="outline" className="font-mono text-[10px] text-muted-foreground">
          seq {version.seq}
        </Badge>
      </div>
      <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs">
        {fields.map((f) => {
          const changed = previous !== null && !Object.is(previous[f], version.row[f]) && f in previous
          return (
            <div key={f} className="contents">
              <dt className="text-muted-foreground">{f}</dt>
              <dd
                className={cn(
                  'truncate font-mono',
                  changed ? 'rounded bg-primary/10 px-1 text-primary' : 'text-foreground',
                )}
              >
                {formatValue(version.row[f])}
              </dd>
            </div>
          )
        })}
      </dl>
    </div>
  )
}

export function RowHistorySheet({
  open,
  onOpenChange,
  tableId,
  row,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  tableId: string
  row: ResultRow | null
}) {
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<TableHistoryResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open || !row) {
      setResult(null)
      setError(null)
      return
    }
    let cancelled = false
    setLoading(true)
    setError(null)
    tablesApi
      .historyLookup(tableId, row, 0)
      .then((res) => {
        if (!cancelled) setResult(res)
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Failed to load history.')
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [open, row, tableId])

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-md">
        <SheetHeader>
          <SheetTitle className="flex items-center gap-1.5">
            <History className="size-4" /> Row history
          </SheetTitle>
          <SheetDescription>Version timeline for this row identity, newest first.</SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-3 px-4 pb-4">
          {loading ? (
            <div className="flex items-center gap-2 py-8 text-sm text-muted-foreground">
              <Spinner className="size-4" /> Loading…
            </div>
          ) : error ? (
            <Empty className="border border-dashed">
              <EmptyHeader>
                <EmptyDescription>{error}</EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : !result?.keyFound ? (
            <Empty className="border border-dashed">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <History />
                </EmptyMedia>
                <EmptyDescription>No history recorded yet for this row.</EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <>
              <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                <Badge variant="outline">{result.mode}</Badge>
                <span>
                  {result.versions.length} version{result.versions.length === 1 ? '' : 's'} retained
                  {result.totalVersions !== result.versions.length ? ` (of ${result.totalVersions})` : ''}
                </span>
                {result.retractionCount > 0 && (
                  <span>
                    · {result.retractionCount} retraction{result.retractionCount === 1 ? '' : 's'}
                  </span>
                )}
              </div>

              {result.versions.length === 0 ? (
                <p className="text-xs text-muted-foreground">
                  This row identity was observed but has no retained assertion versions (all retracted).
                </p>
              ) : (
                <div className="flex flex-col gap-2">
                  {result.versions.map((v, i) => (
                    <VersionRow key={`${v.seq}`} version={v} previous={result.versions[i + 1]?.row ?? null} />
                  ))}
                </div>
              )}
            </>
          )}
        </div>
      </SheetContent>
    </Sheet>
  )
}
