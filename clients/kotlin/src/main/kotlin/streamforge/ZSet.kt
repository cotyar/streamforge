package streamforge

import com.google.gson.Gson

/** One live row: an arbitrary bag of columns. Stays a Map -- there is no pandas equivalent here,
 * and typing it would fight the fact that ad-hoc tables (`adhoc_*`) exist created at runtime. */
typealias Row = Map<String, Any?>

/** One Z-set delta: a row entering (+1) or leaving (-1) a table's output. Weight is int64 on the
 * wire (TableDelta.weight / TableRow.weight in streamforge.proto). */
data class RowDelta(val row: Row, val weight: Long)

/** One batch of deltas as either transport frames it: `TableDeltaBatch` on gRPC,
 * `tableDelta(name, deltas, seq)` on SignalR. `seq` is a per-subscription batch counter, not
 * comparable across transports or to a snapshot's own `seq` -- see [ZSet]'s docstring. */
data class DeltaBatch(val deltas: List<RowDelta>, val seq: Long)

private val canonicalGson = Gson()

/** Stable identity for a row's exact content: sorted (key, value) pairs, JSON-serialized. Two
 * rows are the "same" delta target iff this string matches -- not any single column. */
fun canonicalKey(row: Row): String = canonicalGson.toJson(row.toSortedMap(compareBy { it }))

/**
 * The row's logical-identity ("group") key, or null when supersession does not apply.
 *
 * - `keyFields` non-null and non-empty: joins each field's value. Two canonical rows sharing the
 *   same group key are the same logical entity at different times (an updated MTM tick, a
 *   LATEST BY row) -- asserting a new one deletes the group's previous row even though nothing on
 *   the wire explicitly retracted it.
 * - `keyFields == emptyList()`: a global aggregate -- exactly one row, one constant group `"*"`.
 * - `keyFields == null`: the table's key is unknown and none was given. Deliberately NOT "guess
 *   the first column" (the browser console's fallback, wrong for a composite key or a global
 *   aggregate) -- this falls back to whole-row identity instead. Never guess a key.
 */
fun groupKeyOf(row: Row, keyFields: List<String>?): String? {
    if (keyFields == null) return null
    if (keyFields.isEmpty()) return "*"
    return keyFields.joinToString("|") { f ->
        if (row.containsKey(f)) "$f=${canonicalGson.toJson(row[f])}" else "$f=undefined"
    }
}

/**
 * Live reduced state for one table: canonicalKey -> (row, weight), plus a groupKey ->
 * canonicalKey index for supersession. Ported literally from `lib/streamforge/live-table.ts`
 * (via the Python client's `_zset.py`, whose module docstring is the fullest account of why this
 * looks the way it does) -- pure state, no I/O, so it is testable with handcrafted fixtures alone
 * (see the conformance suite in `../conformance/zset-cases.json`).
 *
 * Two hazards this defends against:
 *
 * 1. Weight is SUMMED per canonical identity across every delta seen; a summed weight <= 0
 *    removes the row outright rather than going negative. This is what makes retract-then-assert
 *    and assert-then-retract (arrival order is not guaranteed) both converge to the same state.
 * 2. Subscribe races the initial snapshot. The caller ([LiveTable]) buffers everything until the
 *    snapshot is in hand, seeds from it (dropping weight<=0 rows and resolving any supersession
 *    the snapshot itself straddled), then replays buffered batches -- except ones the snapshot
 *    already reflects. There is no shared sequence counter between a snapshot read and the delta
 *    stream (measured on different scales), so [alreadyReflected] is a CONTENT heuristic, not a
 *    seq comparison: a buffered batch is skipped only when EVERY one of its retractions targets a
 *    row the snapshot does not contain.
 */
class ZSet(private val keyFields: List<String>?) {
    private val map = LinkedHashMap<String, Pair<Row, Long>>() // canonicalKey -> (row, weight)
    private val groupIndex = HashMap<String, String>() // groupKey -> canonicalKey

    /** Current live rows. Collapses to one row per group when `keyFields` is known -- apply()/
     * seed() already maintain the one-canonical-key-per-group invariant, but a consumer must
     * never see a group surface twice regardless of that. */
    fun rows(): List<Row> {
        if (keyFields == null) return map.values.map { it.first }
        val byGroup = LinkedHashMap<String, Row>()
        for ((_, entry) in map) {
            val (row, _) = entry
            val gk = groupKeyOf(row, keyFields) ?: canonicalKey(row)
            byGroup[gk] = row
        }
        return byGroup.values.toList()
    }

    /** Apply one batch of deltas in place. Returns the canonical keys newly asserted (weight
     * summed > 0 after this batch) -- for on_change/flash tracking. */
    fun apply(deltas: List<RowDelta>): Set<String> {
        val touched = LinkedHashSet<String>()
        for ((row, weight) in deltas) {
            val key = canonicalKey(row)
            val groupKey = groupKeyOf(row, keyFields)
            val prevWeight = map[key]?.second ?: 0L
            val nextWeight = prevWeight + weight
            if (nextWeight <= 0) {
                map.remove(key)
                if (groupKey != null && groupIndex[groupKey] == key) groupIndex.remove(groupKey)
            } else {
                if (groupKey != null) {
                    val staleKey = groupIndex[groupKey]
                    if (staleKey != null && staleKey != key) map.remove(staleKey)
                    groupIndex[groupKey] = key
                }
                map[key] = row to nextWeight
                touched.add(key)
            }
        }
        return touched
    }

    /** Reset state and seed from a snapshot read (GET /rows or TableService.Rows). Mirrors
     * apply()'s rules: a weight<=0 row is not part of the snapshot at all, and a group keeps only
     * its newest row -- a snapshot read mid-update can carry both sides of a supersession. */
    fun seed(snapshotRows: List<RowDelta>) {
        map.clear()
        groupIndex.clear()
        for ((row, weight) in snapshotRows) {
            if (weight <= 0) continue
            val key = canonicalKey(row)
            val groupKey = groupKeyOf(row, keyFields)
            if (groupKey != null) {
                val staleKey = groupIndex[groupKey]
                if (staleKey != null && staleKey != key) map.remove(staleKey)
                groupIndex[groupKey] = key
            }
            map[key] = row to weight
        }
    }

    /** True when this buffered batch's effect is already visible in the (just-seeded) current
     * state. A batch with no retractions is never considered reflected -- an assert-only batch is
     * always safe, and possibly necessary, to replay. */
    fun alreadyReflected(deltas: List<RowDelta>): Boolean {
        val retractions = deltas.filter { it.weight < 0 }
        if (retractions.isEmpty()) return false
        for ((row, _) in retractions) {
            val weight = map[canonicalKey(row)]?.second ?: 0L
            if (weight > 0) return false
        }
        return true
    }
}
