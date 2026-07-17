import { useEffect, useMemo, useRef, useState } from 'react'
import type { ResultEnvelope, RowValue } from '../api/types'

function formatValue(v: RowValue): string {
  if (v === null) return '—'
  if (typeof v === 'number') return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(4)
  if (typeof v === 'boolean') return v ? 'true' : 'false'
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
    return <p className="px-4 py-10 text-center text-sm text-gray-500">Waiting for results…</p>
  }

  return (
    <div className="max-h-full overflow-auto">
      <table className="w-full min-w-max border-collapse text-left text-xs">
        <thead className="sticky top-0 z-10 bg-[var(--sf-panel)]">
          <tr>
            {columns.map((c) => (
              <th
                key={c}
                className="whitespace-nowrap border-b border-[var(--sf-border)] px-3 py-2 font-medium uppercase tracking-wide text-gray-500"
              >
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="font-mono">
          {rows.map((r) => (
            <tr
              key={r.seq}
              className={`border-b border-[var(--sf-border)]/60 ${flashSeqs.has(r.seq) ? 'animate-[sf-flash_0.9s_ease-out]' : ''}`}
            >
              {columns.map((c) => {
                const has = c in r.row
                const v = r.row[c]
                return (
                  <td
                    key={c}
                    className={`whitespace-nowrap px-3 py-1.5 ${typeof v === 'number' ? 'text-right text-gray-200' : 'text-gray-300'}`}
                  >
                    {has ? formatValue(v) : ''}
                  </td>
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
