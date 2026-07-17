import { useEffect, useMemo, useState } from 'react'
import type { ResultEnvelope } from '../api/types'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

const WIDTH = 640
const HEIGHT = 220
const PAD = { top: 16, right: 16, bottom: 24, left: 52 }
const MAX_POINTS = 100

export function LiveChart({ rows }: { rows: ResultEnvelope[] }) {
  const numericColumns = useMemo(() => {
    const cols = new Set<string>()
    for (const r of rows.slice(0, 50)) {
      for (const [k, v] of Object.entries(r.row)) {
        if (typeof v === 'number') cols.add(k)
      }
    }
    return Array.from(cols)
  }, [rows])

  const [column, setColumn] = useState('')

  useEffect(() => {
    if (numericColumns.length === 0) return
    if (!column || !numericColumns.includes(column)) setColumn(numericColumns[0])
  }, [numericColumns, column])

  const points = useMemo(() => {
    if (!column) return []
    const recentChronological = rows.slice(0, MAX_POINTS).slice().reverse()
    return recentChronological
      .map((r) => ({ seq: r.seq, value: r.row[column] }))
      .filter((p): p is { seq: number; value: number } => typeof p.value === 'number')
  }, [rows, column])

  if (numericColumns.length === 0) {
    return <p className="px-4 py-10 text-center text-sm text-muted-foreground">No numeric columns yet.</p>
  }

  const plotW = WIDTH - PAD.left - PAD.right
  const plotH = HEIGHT - PAD.top - PAD.bottom
  const values = points.map((p) => p.value)
  const max = values.length ? Math.max(...values) : 1
  const min = values.length ? Math.min(...values) : 0
  const range = max - min || 1

  const coords = points.map((p, i) => ({
    x: PAD.left + (points.length > 1 ? (i / (points.length - 1)) * plotW : plotW / 2),
    y: PAD.top + plotH - ((p.value - min) / range) * plotH,
  }))

  const linePath = coords.map((c, i) => `${i === 0 ? 'M' : 'L'}${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(' ')
  const areaPath =
    coords.length > 0
      ? `${linePath} L${coords[coords.length - 1].x.toFixed(1)},${PAD.top + plotH} L${coords[0].x.toFixed(1)},${PAD.top + plotH} Z`
      : ''

  const gridRows = 4
  const gridYs = Array.from({ length: gridRows + 1 }, (_, i) => PAD.top + (i / gridRows) * plotH)

  return (
    <div>
      <div className="mb-2 flex items-center justify-between">
        <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Live chart</span>
        <Select value={column} onValueChange={setColumn}>
          <SelectTrigger size="sm" className="text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              {numericColumns.map((c) => (
                <SelectItem key={c} value={c}>
                  {c}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>
      </div>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="w-full" preserveAspectRatio="none">
        {gridYs.map((y, i) => (
          <g key={i}>
            <line x1={PAD.left} y1={y} x2={WIDTH - PAD.right} y2={y} stroke="var(--border)" strokeWidth={1} />
            <text x={PAD.left - 8} y={y + 3} textAnchor="end" fontSize={9} fill="var(--muted-foreground)">
              {(max - (i / gridRows) * range).toFixed(1)}
            </text>
          </g>
        ))}
        {coords.length > 1 && (
          <>
            <path d={areaPath} fill="var(--chart-fill)" opacity={0.15} />
            <path d={linePath} fill="none" stroke="var(--chart-stroke)" strokeWidth={1.75} strokeLinejoin="round" strokeLinecap="round" />
          </>
        )}
        {coords.length > 0 && <circle cx={coords[coords.length - 1].x} cy={coords[coords.length - 1].y} r={3} fill="var(--chart-stroke)" />}
        <text x={PAD.left} y={HEIGHT - 6} fontSize={9} fill="var(--muted-foreground)">
          older
        </text>
        <text x={WIDTH - PAD.right} y={HEIGHT - 6} textAnchor="end" fontSize={9} fill="var(--muted-foreground)">
          now
        </text>
      </svg>
    </div>
  )
}
