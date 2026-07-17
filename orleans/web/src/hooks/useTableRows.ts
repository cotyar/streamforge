import { useEffect, useRef, useState } from 'react'
import type { ResultRow, TableRowDto } from '../api/types'
import { tablesApi } from '../api/tables'
import { subscribeTable } from '../realtime/hub'

export interface LiveRow {
  /** Canonical Z-set tuple identity — see canonicalKey() below. */
  key: string
  row: ResultRow
  weight: number
}

/** Canonical row identity: JSON.stringify of the row's own entries, sorted by key, independent of
 * field insertion order. This matches the backend's Z-set tuple identity: DBSP has no separate
 * primary key, the full tuple content *is* the identity, so an aggregate's old and new values are
 * naturally different tuples — a GROUP BY update arrives as a retraction (-1) of the old full row
 * plus an assertion (+1) of the new one, which is exactly what the reducer below expects. */
function canonicalKey(row: ResultRow): string {
  const keys = Object.keys(row).sort()
  return JSON.stringify(keys.map((k) => [k, row[k]]))
}

/** The table's leading column value, used as a "group key" safety net (see the hook's doc comment
 * below). `Object.keys()` on a JSON.parse()'d object preserves source field order, which mirrors
 * the SELECT list — so this is reliably the table's first/grouping column (e.g. `symbol`) without
 * needing the caller to pass output-schema metadata into the hook. */
function groupKeyOf(row: ResultRow): string | null {
  const firstField = Object.keys(row)[0]
  if (firstField === undefined) return null
  return `${firstField}=${JSON.stringify(row[firstField])}`
}

/**
 * Live view of a materialized table's rows.
 *
 * Reconciliation strategy (to avoid the snapshot/stream race — a delta landing between the REST
 * read and the hub subscribe ack must be neither dropped nor double-applied):
 *   1. Subscribe via subscribeTable() BEFORE issuing the GET /rows request, buffering every
 *      incoming delta batch instead of applying it.
 *   2. Once GET /rows resolves, build the initial Map from the snapshot.
 *   3. Replay the buffered batches, oldest first, against that Map.
 *   4. Flip to "live" — every subsequent delta is applied directly and flushed to state.
 *
 * Weights are summed per canonical identity; entries whose weight drops to <=0 are removed.
 *
 * Filtering already-reflected batches during replay (step 3): the obvious approach — compare each
 * buffered batch's `seq` against the snapshot response's own `seq` field and only replay batches
 * numbered higher — turned out to be unsound. Empirically (a synchronized curl + signalR probe
 * against the running backend), GET /rows's `seq` and the hub's per-batch `seq` are *different
 * counters* on entirely different scales (observed ~860 vs. ~15,000 at the same instant), so that
 * comparison is close to `true` for every buffered batch regardless of whether it actually
 * predates the snapshot. Replay filtering here is content-based instead: a buffered batch is
 * treated as already reflected — and skipped whole — when every one of its retracting
 * (negative-weight) deltas fails to match a row currently in the map.
 *
 * Group-key supersession (the second, independent gap this closes): even with the above, the
 * REST snapshot and the hub push stream turned out — empirically, same probe — to not be
 * perfectly consistent with each other. The very first live delta after a snapshot can represent
 * several trades coalesced into one push, so its retraction targets a row that never exactly
 * matches what the snapshot stored; that snapshot row then never gets retracted and sits in the
 * map forever as an orphaned duplicate. Pure full-tuple identity (the DBSP-correct approach) is
 * too fragile against that gap. As a safety net, every assertion also evicts any *other* row
 * currently sharing the same leading-column value (the table's natural group key for the
 * windowless-GROUP-BY tables this UI targets) — i.e. "a fresher row for this group has arrived,
 * whatever was there before for this group is now stale." This trades strict Z-set multiplicity
 * semantics for a live grid that reliably shows one current row per group, which is what the
 * materialized-view UI is for.
 */
export function useTableRows(tableId: string | undefined, tableName: string | undefined) {
  const [rows, setRows] = useState<LiveRow[]>([])
  const [snapshotTotal, setSnapshotTotal] = useState(0)
  const [live, setLive] = useState(false)
  const [loading, setLoading] = useState(true)
  const [flashKeys, setFlashKeys] = useState<Set<string>>(new Set())

  const mapRef = useRef<Map<string, LiveRow>>(new Map())
  // groupKey -> the canonical row key currently representing that group, for supersession lookups.
  const groupIndexRef = useRef<Map<string, string>>(new Map())

  useEffect(() => {
    mapRef.current = new Map()
    groupIndexRef.current = new Map()
    setRows([])
    setSnapshotTotal(0)
    setLive(false)
    setFlashKeys(new Set())

    if (!tableId || !tableName || tableId === 'new') {
      setLoading(false)
      return
    }

    setLoading(true)
    let cancelled = false
    let buffering = true
    const buffered: { deltas: TableRowDto[]; seq: number }[] = []
    let flashTimer: ReturnType<typeof setTimeout> | null = null

    function applyBatch(deltas: TableRowDto[], trackFlash: boolean) {
      const map = mapRef.current
      const groupIndex = groupIndexRef.current
      const touched: string[] = []
      for (const d of deltas) {
        const key = canonicalKey(d.row)
        const groupKey = groupKeyOf(d.row)
        const nextWeight = (map.get(key)?.weight ?? 0) + d.weight
        if (nextWeight <= 0) {
          map.delete(key)
          if (groupKey && groupIndex.get(groupKey) === key) groupIndex.delete(groupKey)
        } else {
          if (groupKey) {
            const staleKey = groupIndex.get(groupKey)
            if (staleKey && staleKey !== key) map.delete(staleKey)
            groupIndex.set(groupKey, key)
          }
          map.set(key, { key, row: d.row, weight: nextWeight })
          touched.push(key)
        }
      }
      if (trackFlash && touched.length > 0) {
        setFlashKeys((prev) => {
          const next = new Set(prev)
          touched.forEach((k) => next.add(k))
          return next
        })
        if (flashTimer) clearTimeout(flashTimer)
        flashTimer = setTimeout(() => {
          if (!cancelled) setFlashKeys(new Set())
        }, 900)
      }
    }

    function flushToState() {
      setRows(Array.from(mapRef.current.values()))
    }

    const unsub = subscribeTable(tableName, (deltas, seq) => {
      if (buffering) {
        buffered.push({ deltas, seq })
        return
      }
      applyBatch(deltas, true)
      flushToState()
    })

    /** A buffered batch is already reflected in the snapshot when none of its retracting
     * (negative-weight) deltas match a row currently in the map — see the reconciliation-strategy
     * comment above the hook for why this replaces a seq-number comparison. Pure-insert batches
     * (no retractions at all, e.g. a brand-new group appearing for the first time) have nothing to
     * match against and are always applied. */
    function isBatchAlreadyReflected(deltas: TableRowDto[]): boolean {
      const retractions = deltas.filter((d) => d.weight < 0)
      if (retractions.length === 0) return false
      return retractions.every((d) => (mapRef.current.get(canonicalKey(d.row))?.weight ?? 0) <= 0)
    }

    tablesApi
      .rows(tableId, 500)
      .then((res) => {
        if (cancelled) return
        const map = new Map<string, LiveRow>()
        const groupIndex = new Map<string, string>()
        for (const r of res.rows) {
          const key = canonicalKey(r.row)
          map.set(key, { key, row: r.row, weight: r.weight })
          const groupKey = groupKeyOf(r.row)
          if (groupKey) groupIndex.set(groupKey, key)
        }
        mapRef.current = map
        groupIndexRef.current = groupIndex
        setSnapshotTotal(res.totalRows)

        // Replay in arrival order (reliably chronological — single SignalR connection), skipping
        // batches whose transition already happened before the snapshot was taken.
        for (const b of buffered) {
          if (isBatchAlreadyReflected(b.deltas)) continue
          applyBatch(b.deltas, false)
        }

        buffering = false
        flushToState()
        setLive(true)
      })
      .catch(() => {
        // best-effort — once buffering flips off, live deltas still populate the view
        buffering = false
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
      if (flashTimer) clearTimeout(flashTimer)
      unsub()
    }
  }, [tableId, tableName])

  return { rows, snapshotTotal, live, loading, flashKeys }
}
