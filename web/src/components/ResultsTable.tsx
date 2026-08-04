import { useEffect, useMemo, useRef, useState } from 'react'
import type { ResultEnvelope, RowValue } from '../api/types'
import { cn } from '@/lib/utils'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { formatEpochMs, isEpochMsColumn } from '@/lib/format'

function isJsonValue(v: RowValue): v is Record<string, RowValue> | RowValue[] {
  return typeof v === 'object' && v !== null
}

function formatValue(v: RowValue, key?: string): string {
  if (v === null) return '—'
  if (key !== undefined && isEpochMsColumn(key, v)) return formatEpochMs(v)
  if (typeof v === 'number') return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(4)
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  if (isJsonValue(v)) return JSON.stringify(v)
  return v
}

export function ResultsTable({ rows }: { rows: ResultEnvelope[] }) {
  const columns = useMemo(() => {
    const set = new Set<string>()
    for (const r of rows) for (const k of Object.keys(r.row)) set.add(k)
    return Array.from(set)
  }, [rows])

  const seenSeqs = useRef<Set<number>>(new Set())
  const [flashSeqs, setFlashSeqs] = useState<Set<number>>(new Set())

  useEffect(() => {
    const fresh = rows.filter((r) => !seenSeqs.current.has(r.seq))
    if (fresh.length === 0) return
    fresh.forEach((r) => seenSeqs.current.add(r.seq))
    setFlashSeqs((prev) => {
      const next = new Set(prev)
      fresh.forEach((r) => next.add(r.seq))
      return next
    })
    const timer = setTimeout(() => {
      setFlashSeqs((prev) => {
        const next = new Set(prev)
        fresh.forEach((r) => next.delete(r.seq))
        return next
      })
    }, 900)
    return () => clearTimeout(timer)
  }, [rows])

  if (rows.length === 0) {
    return <p className="px-4 py-10 text-center text-sm text-muted-foreground">Waiting for results…</p>
  }

  return (
    <div className="max-h-full overflow-auto">
      <Table className="min-w-max text-xs">
        <TableHeader className="sticky top-0 z-10 bg-card">
          <TableRow className="hover:bg-transparent">
            {columns.map((c) => (
              <TableHead key={c} className="uppercase tracking-wide text-muted-foreground">
                {c}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody className="font-mono">
          {rows.map((r) => (
            <TableRow key={r.seq} className={cn(flashSeqs.has(r.seq) && 'sf-row-flash')}>
              {columns.map((c) => {
                const has = c in r.row
                const v = r.row[c]
                const json = has && isJsonValue(v)
                const ts = has && isEpochMsColumn(c, v)
                return (
                  <TableCell
                    key={c}
                    title={json ? formatValue(v) : ts ? String(v) : undefined}
                    className={cn(
                      typeof v === 'number' && !ts ? 'text-right text-foreground' : 'text-foreground/80',
                      json && 'max-w-56 truncate font-mono',
                    )}
                  >
                    {has ? formatValue(v, c) : ''}
                  </TableCell>
                )
              })}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}
