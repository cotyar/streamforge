import { useEffect, useState } from 'react'
import type { ResultRow } from '../api/types'
import { subscribeSource } from '../realtime/hub'

const MAX_EVENTS = 8

/** Keeps the last MAX_EVENTS live rows for a source, newest first. */
export function useSourceTape(sourceName: string): ResultRow[] {
  const [events, setEvents] = useState<ResultRow[]>([])

  useEffect(() => {
    setEvents([])
    const unsub = subscribeSource(sourceName, (row) => {
      setEvents((prev) => [row, ...prev].slice(0, MAX_EVENTS))
    })
    return unsub
  }, [sourceName])

  return events
}
