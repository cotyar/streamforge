import { useEffect, useState } from 'react'
import type { PipelineMetrics } from '../api/types'
import { subscribeMetrics } from '../realtime/hub'

/** Subscribes once to the metrics stream and keeps the latest PipelineMetrics per pipeline id. */
export function useMetricsStream(): Record<string, PipelineMetrics> {
  const [metrics, setMetrics] = useState<Record<string, PipelineMetrics>>({})

  useEffect(() => {
    const unsub = subscribeMetrics((m) => {
      setMetrics((prev) => ({ ...prev, [m.pipelineId]: m }))
    })
    return unsub
  }, [])

  return metrics
}
