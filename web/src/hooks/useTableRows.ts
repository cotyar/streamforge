import { useEffect, useRef, useState } from 'react'
import { ZSet } from '@streamforge/client'
import type { Row as LiveTableRow } from '@streamforge/client'
import type { ResultRow, TableRowDto } from '../api/types'
import { tablesApi } from '../api/tables'
import { subscribeTable } from '../realtime/hub'

export interface LiveRow {
  /** Canonical Z-set tuple identity -- see @streamforge/client's canonicalKey(). */
  key: string
  row: ResultRow
  weight: number
}

/** This console has no per-table catalog of key columns (unlike the OTC-terms demo `keyFields`
 * map @streamforge/client's own `keyfields.ts` ports), so it groups by a table's first/leading
 * column as a heuristic safety net against orphaned duplicates -- `Object.keys()` on a
 * JSON.parse()'d object preserves source field order, which mirrors the SELECT list, so this is
 * reliably the table's first/grouping column (e.g. `symbol`) without needing output-schema
 * metadata. @streamforge/client's own default policy deliberately does NOT guess a column (see
 * its zset.ts docstring), so this is passed to `ZSet` as an explicit `groupKeyFn` override --
 * console-specific policy layered on the package's shared bookkeeping, not a fork of it. */
function firstColumnGroupKey(row: LiveTableRow): string | null {
  const firstField = Object.keys(row)[0]
  if (firstField === undefined) return null
  return `${firstField}=${JSON.stringify(row[firstField])}`
}

/**
 * Live view of a materialized table's rows.
 *
 * The Z-set reducer itself (canonical-row identity, weight summation, group supersession, the
 * content-based "already reflected" replay heuristic) lives in `@streamforge/client`'s `zset.ts`
 * now -- extracted from this hook, which used to hand-roll all of it inline. Read that module's
 * doc comment for the hazards it defends against (arrival order isn't guaranteed, `GET /rows`'s
 * `seq` and the hub's per-batch `seq` are different counters on different scales -- measured
 * ~860 vs ~15,000 at the same instant). What stays HERE, because it's genuinely this hook's own
 * concern rather than the reducer's: subscribing to the hub BEFORE the snapshot read so no delta
 * is lost to the race, buffering until the snapshot lands, replaying, and the 900ms flash-key
 * timer for the UI's "this row just changed" highlight.
 */
export function useTableRows(tableId: string | undefined, tableName: string | undefined) {
  const [rows, setRows] = useState<LiveRow[]>([])
  const [snapshotTotal, setSnapshotTotal] = useState(0)
  const [live, setLive] = useState(false)
  const [loading, setLoading] = useState(true)
  const [flashKeys, setFlashKeys] = useState<Set<string>>(new Set())

  const zsetRef = useRef<ZSet>(new ZSet(null, firstColumnGroupKey))

  useEffect(() => {
    zsetRef.current = new ZSet(null, firstColumnGroupKey)
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

    function toDeltaTuples(deltas: TableRowDto[]): Array<readonly [ResultRow, number]> {
      return deltas.map((d) => [d.row, d.weight] as const)
    }

    function flushToState() {
      // @streamforge/client's Entry.row is `Row` (Record<string, unknown>); ResultRow is the
      // narrower RowValue-typed shape this console's components expect. Every row on the wire is
      // JSON already, so this is a type-level narrowing, not a runtime conversion.
      setRows(zsetRef.current.entries() as unknown as LiveRow[])
    }

    function applyBatch(deltas: TableRowDto[], trackFlash: boolean) {
      const touched = zsetRef.current.apply(toDeltaTuples(deltas))
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

    // Subscribe via subscribeTable() BEFORE issuing the GET /rows request, buffering every
    // incoming delta batch instead of applying it -- see the reconciliation strategy in
    // @streamforge/client's zset.ts docstring for why (a delta landing between the REST read and
    // the hub subscribe ack must be neither dropped nor double-applied).
    const unsub = subscribeTable(tableName, (deltas, seq) => {
      if (buffering) {
        buffered.push({ deltas, seq })
        return
      }
      applyBatch(deltas, true)
      flushToState()
    })

    tablesApi
      .rows(tableId, 500)
      .then((res) => {
        if (cancelled) return
        zsetRef.current.seed(toDeltaTuples(res.rows))
        setSnapshotTotal(res.totalRows)

        // Replay in arrival order (reliably chronological -- single SignalR connection), skipping
        // batches whose transition already happened before the snapshot was taken.
        for (const b of buffered) {
          if (!zsetRef.current.alreadyReflected(toDeltaTuples(b.deltas))) {
            applyBatch(b.deltas, false)
          }
        }

        buffering = false
        flushToState()
        setLive(true)
      })
      .catch(() => {
        // best-effort -- once buffering flips off, live deltas still populate the view
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
