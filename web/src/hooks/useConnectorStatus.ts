import { useEffect, useState } from 'react'
import type { ConnectorRuntimeStatus } from '../api/types'
import { sourcesApi } from '../api/sources'

const POLL_MS = 2000

/**
 * Connector runtime status for one source — 2 s polled, mirroring `useTableMetrics` (there's no
 * push channel for this data). `GET /api/sources/{name}/status` returns 204 for generator-kind
 * sources (or a connector that hasn't run its first cycle yet) and 404 if the source no longer
 * exists; both collapse to `null` here so callers can hide the status line unconditionally rather
 * than branching on kind or transient fetch failures.
 */
export function useConnectorStatus(name: string | undefined): ConnectorRuntimeStatus | null {
  const [status, setStatus] = useState<ConnectorRuntimeStatus | null>(null)

  useEffect(() => {
    setStatus(null)
    if (!name) return

    let cancelled = false

    function poll() {
      sourcesApi
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
