/**
 * The Z-set reducer -- ported literally from web/src/hooks/useTableRows.ts (this repo's own
 * console), itself the reference implementation the Python client's `_zset.py` also ported from.
 * Pure functions/state, no I/O: this module never touches a socket, so it is testable with
 * handcrafted fixtures alone (see ../test/conformance.test.ts, which runs it against
 * ../conformance/zset-cases.json -- the cross-language conformance suite every StreamForge
 * client must pass).
 *
 * Hazards, and why the code below looks the way it does (mirrors _zset.py's module docstring):
 *
 * - A row's identity for retract/assert matching is the WHOLE row's content, not any one column:
 *   `canonicalKey` is a stable serialization of every (key, value) pair, sorted by key. Weight is
 *   SUMMED per canonical identity across every delta seen; a summed weight <= 0 removes the row
 *   outright rather than going negative. This is what makes retract-then-assert and
 *   assert-then-retract (arrival order is not guaranteed) both converge to the same state.
 *
 * - A logical key ("group", `keyFields`) can SUPERSEDE: two different canonical rows sharing the
 *   same keyFields values are the same logical entity at different times (an updated MTM tick, a
 *   LATEST BY row). When a new row for a group is asserted, the group's PREVIOUS canonical row is
 *   deleted even though its own weight was never explicitly retracted on the wire -- the
 *   retraction is implied by the new assert superseding it. `groupKeyOf` returns `"*"` for
 *   `keyFields=[]` (a global aggregate: one row, one group), and `null` for `keyFields=null`
 *   (unknown table, no override given) -- deliberately NOT the browser console's own fallback
 *   (useTableRows.ts's `groupKeyOf` uses the row's first column, which is right for a LATEST BY
 *   table that leads with its key and silently wrong for a composite key or a global aggregate).
 *   This client instead falls back to whole-row identity -- no column is ever guessed.
 *
 * - Subscribe races the initial snapshot: deltas can arrive before, during or after the snapshot
 *   read lands. The caller (live-table.ts) buffers everything until the snapshot is in hand,
 *   seeds state from it (dropping weight<=0 rows and resolving any supersession the snapshot
 *   itself straddled -- a snapshot read mid-update can carry both the old and new row of a
 *   group), and then replays the buffered batches -- except ones the snapshot has ALREADY
 *   reflected. There is no shared sequence counter between the snapshot read (`/rows`' `seq`) and
 *   the delta stream (`TableDeltaBatch.seq` is a per-subscription batch counter on a completely
 *   different scale -- measured ~860 vs ~15,000 at the same instant in useTableRows.ts) so
 *   "already reflected" cannot be seq-based. Instead: `alreadyReflected` is a CONTENT heuristic --
 *   a buffered batch is skipped only when EVERY one of its retractions targets a row the snapshot
 *   does not contain (i.e. the snapshot already dropped it). Replaying it anyway would
 *   double-apply a retraction the snapshot already reflects, which for a plain reduce-by-weight
 *   is harmless, but for a LATEST BY group it can delete the WRONG (newer) row out from under the
 *   group index. Wishlist #20 (a shared epoch on both the snapshot and the delta stream) is the
 *   fix that would make this exact instead of a heuristic.
 */

/** A table row -- dynamic shape, so consumers narrow. Mirrors the engine's dynamic Struct rows. */
export type Row = Record<string, unknown>;

/** One live Z-set tuple: its canonical identity, its row, and its currently summed weight. Most
 * consumers want `.rows()` (plain rows, the common case); a caller that needs to render
 * multiplicity itself -- e.g. a raw stream/pipeline view with no dedup, where weight can
 * genuinely exceed 1 -- wants `.entries()` instead. */
export interface Entry {
  key: string;
  row: Row;
  weight: number;
}

/** One Z-set delta: a row entering (+1) or leaving (-1) a table's output. */
export type Delta = readonly [row: Row, weight: number];

const MISSING = Symbol("missing");

/** Stable identity for a row's exact content: sorted (key, value) pairs, JSON-serialized. */
export function canonicalKey(row: Row): string {
  const keys = Object.keys(row).sort();
  return JSON.stringify(keys.map((k) => [k, row[k]]));
}

/**
 * The row's logical-identity ("group") key, or `null` when supersession does not apply.
 * - `keyFields` a real (possibly empty) array: `[]` is a global aggregate -- exactly one row, one
 *   constant group `"*"`. A non-empty array joins each field's value.
 * - `keyFields === null`: the table's key is unknown and none was given -- callers fall back to
 *   whole-row identity (canonicalKey alone), never a guessed column.
 */
export function groupKeyOf(row: Row, keyFields: readonly string[] | null): string | null {
  if (keyFields === null) return null;
  if (keyFields.length === 0) return "*";
  const parts = keyFields.map((f) => {
    const has = Object.hasOwn(row, f);
    const value = has ? row[f] : MISSING;
    return `${f}=${value === MISSING ? "undefined" : JSON.stringify(value)}`;
  });
  return parts.join("|");
}

/**
 * Live reduced state for one table: canonicalKey -> [row, weight], plus a groupKey -> canonicalKey
 * index for supersession. `keyFields=null` disables supersession entirely (whole-row identity);
 * `keyFields=[]` is a global aggregate.
 *
 * `groupKeyFn`, if given, REPLACES the `keyFields`-based policy above with a caller-supplied one --
 * an explicit escape hatch, not a second default. Wishlist #18 put a table's real key fields on
 * the wire (`TableDefinitionDto.keyFields`, read via `tables.ts#resolveKeyFields`), so both this
 * package's own `Client.table()` and web/'s console now pass the engine-reported `keyFields`
 * straight to the constructor for a current engine. `groupKeyFn` survives as web/'s fallback for
 * an OLDER engine that doesn't report the field at all: `useTableRows.ts` groups by a table's
 * first/leading column as a heuristic safety net against orphaned duplicates in that one case --
 * a policy this package deliberately does NOT apply by default (see the module docstring: "no
 * column is ever guessed"). The weight-summation, supersession bookkeeping and replay-heuristic
 * logic below is identical either way; only "what counts as the same logical row" is pluggable.
 */
export class ZSet {
  private map = new Map<string, [Row, number]>();
  private groupIndex = new Map<string, string>();

  constructor(
    private readonly keyFields: readonly string[] | null,
    private readonly groupKeyFn?: (row: Row) => string | null,
  ) {}

  private groupKeyForRow(row: Row): string | null {
    return this.groupKeyFn ? this.groupKeyFn(row) : groupKeyOf(row, this.keyFields);
  }

  /**
   * Current live rows. Collapses to one row per group when keyFields is known -- a defensive
   * step mirroring useTableRows.ts's flushToState: apply()/seed() already maintain the
   * one-canonical-key-per-group invariant, but a consumer must never see a group surface twice
   * regardless.
   */
  rows(): Row[] {
    return this.entries().map((e) => e.row);
  }

  /** Same rows as `.rows()`, plus each one's canonical key and current summed weight -- for a
   * consumer that renders multiplicity (a raw stream/pipeline table with no dedup can have
   * weight > 1 for a genuinely-repeated row) or wants a stable React-list key without
   * recomputing canonicalKey() itself. */
  entries(): Entry[] {
    if (this.keyFields === null && !this.groupKeyFn) {
      return Array.from(this.map.entries(), ([key, [row, weight]]) => ({ key, row, weight }));
    }
    const byGroup = new Map<string, Entry>();
    for (const [key, [row, weight]] of this.map.entries()) {
      const gk = this.groupKeyForRow(row) ?? key;
      byGroup.set(gk, { key, row, weight });
    }
    return Array.from(byGroup.values());
  }

  /** The current row for one canonical key, or `undefined` if that key isn't (or is no longer)
   * present -- reads the same `map` `.rows()`/`.entries()` project from, so a caller holding a
   * `touched` key (see `apply()`) can resolve it to its row in O(1) instead of scanning
   * `.rows()`. A touched key that resolves to `undefined` here means the tuple was retracted --
   * that is exactly how a consumer distinguishes upsert (key present) from delete (key absent). */
  get(key: string): Row | undefined {
    return this.map.get(key)?.[0];
  }

  /**
   * Apply one batch of deltas in place. Returns every canonical key whose PRESENCE OR CONTENT in
   * `map` actually changed as a result of this batch:
   *
   *  - an ASSERT: weight summed > 0 after this delta, whether that's a brand-new key entering the
   *    map or an existing key's weight being updated;
   *  - a RETRACTION: weight summed <= 0 for a key that WAS present beforehand, so it is deleted
   *    from the map -- this is the tuple leaving the live set, and is exactly as much a "change"
   *    as an assert is (`ZSet.get()`'s doc comment already promises this: "a touched key that
   *    resolves to undefined here means the tuple was retracted");
   *  - a SUPERSESSION: asserting a new row for a group (`keyFields`) whose previous canonical row
   *    is still resident deletes that OLD row from the map too, even though no explicit retraction
   *    for it arrived on the wire -- the old key is reported alongside the new one.
   *
   * A retraction for a key that was never present (prevWeight already 0, so there is nothing to
   * delete) changes nothing and is NOT reported -- there is no state transition to observe. A key
   * touched more than once within the same batch (e.g. retracted then re-asserted, or asserted
   * then immediately superseded by a later delta in the same batch) is reported exactly once.
   */
  apply(deltas: readonly Delta[]): string[] {
    const touched = new Set<string>();
    for (const [row, weight] of deltas) {
      const key = canonicalKey(row);
      const groupKey = this.groupKeyForRow(row);
      const prevWeight = this.map.get(key)?.[1] ?? 0;
      const nextWeight = prevWeight + weight;
      if (nextWeight <= 0) {
        const existed = this.map.has(key);
        this.map.delete(key);
        if (groupKey !== null && this.groupIndex.get(groupKey) === key) {
          this.groupIndex.delete(groupKey);
        }
        if (existed) touched.add(key);
      } else {
        if (groupKey !== null) {
          const staleKey = this.groupIndex.get(groupKey);
          if (staleKey !== undefined && staleKey !== key) {
            this.map.delete(staleKey);
            touched.add(staleKey);
          }
          this.groupIndex.set(groupKey, key);
        }
        this.map.set(key, [row, nextWeight]);
        touched.add(key);
      }
    }
    return Array.from(touched);
  }

  /**
   * Reset state and seed from a snapshot read (GET /rows). Mirrors apply()'s rules: a weight<=0
   * row is not part of the snapshot at all, and a group keeps only its newest row -- a snapshot
   * read mid-update can carry both sides of a supersession.
   */
  seed(snapshotRows: readonly Delta[]): void {
    this.map = new Map();
    this.groupIndex = new Map();
    for (const [row, weight] of snapshotRows) {
      if (weight <= 0) continue;
      const key = canonicalKey(row);
      const groupKey = this.groupKeyForRow(row);
      if (groupKey !== null) {
        const staleKey = this.groupIndex.get(groupKey);
        if (staleKey !== undefined && staleKey !== key) this.map.delete(staleKey);
        this.groupIndex.set(groupKey, key);
      }
      this.map.set(key, [row, weight]);
    }
  }

  /**
   * True when this buffered batch's effect is already visible in the (just-seeded) current
   * state. A batch with no retractions is never considered reflected (an assert-only batch is
   * always safe, and possibly necessary, to replay).
   */
  alreadyReflected(deltas: readonly Delta[]): boolean {
    const retractions = deltas.filter(([, w]) => w < 0);
    if (retractions.length === 0) return false;
    for (const [row] of retractions) {
      const key = canonicalKey(row);
      const weight = this.map.get(key)?.[1] ?? 0;
      if (weight > 0) return false;
    }
    return true;
  }
}
