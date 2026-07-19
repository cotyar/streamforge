import { useEffect, useRef, useState } from 'react'
import type { PipelineStatus, ResultEnvelope } from '../api/types'
import { pipelinesApi } from '../api/pipelines'
import { subscribePipeline } from '../realtime/hub'

const MAX_ROWS = 200

/** Fetches initial results and layers the live hub stream on top. Newest rows first, capped at MAX_ROWS. */
export function usePipelineResults(pipelineId: string | undefined) {
  const [rows, setRows] = useState<ResultEnvelope[]>([])
  const [status, setStatus] = useState<PipelineStatus | null>(null)
  const [loading, setLoading] = useState(true)
  const seenSeqs = useRef<Set<number>>(new Set())

  useEffect(() => {
    seenSeqs.current = new Set()
    setRows([])
    setStatus(null)

    if (!pipelineId || pipelineId === 'new') {
      setLoading(false)
      return
    }

    setLoading(true)
    let cancelled = false

    pipelinesApi
      .results(pipelineId, 50)
      .then((initial) => {
        if (cancelled) return
        const ordered = [...initial].sort((a, b) => b.seq - a.seq)
        for (const r of ordered) seenSeqs.current.add(r.seq)
        setRows(ordered)
      })
      .catch(() => {
        // best-effort — the live stream still populates rows going forward
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    const unsub = subscribePipeline(
      pipelineId,
      (incoming) => {
        const fresh = incoming.filter((r) => !seenSeqs.current.has(r.seq))
        if (fresh.length === 0) return
        fresh.forEach((r) => seenSeqs.current.add(r.seq))
        setRows((prev) => [...fresh].reverse().concat(prev).slice(0, MAX_ROWS))
      },
      (s) => setStatus(s),
    )

    return () => {
      cancelled = true
      unsub()
    }
  }, [pipelineId])

  return { rows, status, loading }
}
