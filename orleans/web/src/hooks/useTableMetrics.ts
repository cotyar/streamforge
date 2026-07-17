import { useEffect, useRef, useState } from 'react'
import type { TableMetrics } from '../api/types'
import { tablesApi } from '../api/tables'

const POLL_MS = 2000

/** Tables have no dedicated metrics push over the hub (unlike PipelineMetrics/subscribeMetrics) —
 * polled on an interval instead. Also derives a client-side deltasIn/s rate from successive reads
 * since the backend only reports cumulative totals (deltasIn/deltasOut). */
export function useTableMetrics(tableId: string | undefined) {
  const [metrics, setMetrics] = useState<TableMetrics | null>(null)
  const [deltasInPerSec, setDeltasInPerSec] = useState(0)
  const prevRef = useRef<{ deltasIn: number; atMs: number } | null>(null)

  useEffect(() => {
    setMetrics(null)
    setDeltasInPerSec(0)
    prevRef.current = null

    if (!tableId || tableId === 'new') return

    let cancelled = false

    function poll() {
      tablesApi
        .metrics(tableId!)
        .then((m) => {
          if (cancelled) return
          const now = performance.now()
          const prev = prevRef.current
          if (prev) {
            const dtSec = (now - prev.atMs) / 1000
            if (dtSec > 0) setDeltasInPerSec(Math.max(0, (m.deltasIn - prev.deltasIn) / dtSec))
          }
          prevRef.current = { deltasIn: m.deltasIn, atMs: now }
          setMetrics(m)
        })
        .catch(() => {
          // best-effort — keep showing the last known metrics on a transient failure
        })
    }

    poll()
    const timer = setInterval(poll, POLL_MS)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [tableId])

  return { metrics, deltasInPerSec }
}
