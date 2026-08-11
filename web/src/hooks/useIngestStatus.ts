import { useEffect, useState } from 'react'
import type { IngestStatusResponse } from '../api/types'
import { ingestApi } from '../api/ingest'

const POLL_MS = 2000

/**
 * Ingress buffer/counters status for one ingest-kind source — 2 s polled (the page's existing
 * convention, mirroring useConnectorStatus/useTableMetrics; there's no push channel for this data
 * and the brief calls for no new SignalR group). `GET /api/sources/{name}/ingest` returns 204 for
 * a source that isn't ingest-kind and 404 if the source no longer exists; both collapse to `null`
 * here so callers can mount this unconditionally.
 */
export function useIngestStatus(name: string | undefined): IngestStatusResponse | null {
  const [status, setStatus] = useState<IngestStatusResponse | null>(null)

  useEffect(() => {
    setStatus(null)
    if (!name) return

    let cancelled = false

    function poll() {
      ingestApi
        .status(name!)
        .then((s) => {
          if (!cancelled) setStatus(s ?? null)
        })
        .catch(() => {
          // 404 (deleted mid-poll) or a transient network failure — hide rather than error.
          if (!cancelled) setStatus(null)
        })
    }

    poll()
    const timer = setInterval(poll, POLL_MS)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [name])

  return status
}
