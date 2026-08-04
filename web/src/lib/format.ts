import type { RowValue } from '../api/types'

/** Timestamp-ish column names: `_ts`, `stage_ts`, `expiry_ts`, aliased `quotes__ts`, `event_time`,
 * `updated_at`, `window_start`/`window_end`. */
const TS_COLUMN = /(^|_)ts$|time$|timestamp$|_at$|_(start|end)$/i

/** Epoch-ms longs travel as plain numbers; only render them as dates when the column name says
 * "timestamp" AND the value lands in a plausible epoch-ms range (2001…2096). */
export function isEpochMsColumn(name: string, v: RowValue): v is number {
  return typeof v === 'number' && Number.isInteger(v) && v >= 1e12 && v < 4e12 && TS_COLUMN.test(name)
}

/** Local time with milliseconds — the date part only when it isn't today (expiries, backfills). */
export function formatEpochMs(ms: number): string {
  const d = new Date(ms)
  const clock = `${d.toLocaleTimeString(undefined, { hour12: false })}.${String(d.getMilliseconds()).padStart(3, '0')}`
  return d.toDateString() === new Date().toDateString() ? clock : `${d.toLocaleDateString()} ${clock}`
}
