/**
 * Live-table hooks over `@streamforge/client`. Each hook owns exactly one `LiveTable` (via
 * `client.table()`/`client.sql()`) for as long as it's mounted with a given `name`/`sql` -- the
 * subscribe -> snapshot -> replay dance, the Z-set reduction, and the ~120ms onChange coalescing
 * all live in `LiveTable` itself (see clients/typescript/src/live-table.ts); this file's only job
 * is mirroring that class's state into React state and getting the connect/cleanup races right.
 *
 * Shared shape across `useLiveTable` and `useLiveSql`, both effect bodies below:
 *  - `loading` goes true the instant a connect is kicked off and false only once it resolves or
 *    rejects (never left stuck true -- see LiveTable.connect's NotReady doc comment in
 *    live-table.ts for the "brand-new table, nobody has pushed to it yet" case that times out).
 *  - a `cancelled` flag closes over a LiveTable that resolves after the effect has already been
 *    superseded (name/sql changed, or the component unmounted) instead of adopting it or calling
 *    setState on a torn-down effect.
 *  - `table.onChange()` reseeds state on every coalesced batch; `table.rows` seeds it once
 *    immediately on connect so the first paint isn't stuck waiting for the first delta.
 *  - `flashKeys` mirrors web/src/hooks/useTableRows.ts's own flash-highlight timer, now that
 *    `LiveTable.onChange()`'s second argument hands out the touched-key set directly (it didn't
 *    used to -- see live-table.ts's `ChangeListener` doc comment). Touched keys accumulate across
 *    every batch that lands inside the FLASH_WINDOW_MS window and clear together, same as that
 *    hook; see `scheduleFlashClear` below for the "cancelled"-style closure this reuses.
 */

import { useEffect, useState } from "react";
import type { LiveTable, Row, TableDefinitionDto } from "@streamforge/client";
import { useStreamForge } from "./provider.js";

/** How long touched keys stay in `flashKeys` after the most recent batch that touched them --
 * matches web/src/hooks/useTableRows.ts's own flash-highlight window so both surfaces feel the
 * same. A second batch landing inside the window extends the highlight rather than resetting it
 * (see `scheduleFlashClear`'s callers). */
const FLASH_WINDOW_MS = 900;

/** One shared empty set for every idle/loading/error/cleared state -- avoids handing out a fresh
 * `Set` (and thus a fresh reference) on every render that has nothing to flash. */
const EMPTY_FLASH_KEYS: ReadonlySet<string> = new Set();

export interface LiveTableState {
  rows: readonly Row[];
  loading: boolean;
  error: Error | null;
  /** The underlying LiveTable once connected -- escape hatch for waitFor()/seq/reconnects. */
  table: LiveTable | null;
  /** Canonical Z-set keys touched by the most recent batch, cleared ~900ms after the last one.
   *  Empty set when nothing changed recently. */
  flashKeys: ReadonlySet<string>;
}

export interface UseLiveTableOptions {
  key?: string[];
  timeoutMs?: number;
}

export interface UseLiveSqlOptions {
  name: string;
  key?: string[];
  timeoutMs?: number;
}

export interface TablesState {
  tables: TableDefinitionDto[];
  loading: boolean;
  error: Error | null;
}

const IDLE_STATE: LiveTableState = { rows: [], loading: false, error: null, table: null, flashKeys: EMPTY_FLASH_KEYS };

function errorOf(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

/** Live view of one materialized table. `name` undefined => idle (no rows, not loading). */
export function useLiveTable(name: string | undefined, opts: UseLiveTableOptions = {}): LiveTableState {
  const client = useStreamForge();
  const [state, setState] = useState<LiveTableState>(IDLE_STATE);
  // opts.key is an array -- a caller passing `key={['symbol']}` inline builds a fresh array every
  // render, so comparing it by reference in the effect's deps would reconnect on every render.
  // JSON.stringify gives a stable value-equal string instead (key lists are short and flat).
  const keyDep = JSON.stringify(opts.key ?? null);

  useEffect(() => {
    if (name === undefined) {
      setState(IDLE_STATE);
      return;
    }
    if (!client) {
      // Waiting for the provider's client is NOT an error -- this effect re-runs once
      // useStreamForge() stops returning null, since `client` is a dependency below.
      setState({ rows: [], loading: true, error: null, table: null, flashKeys: EMPTY_FLASH_KEYS });
      return;
    }

    let cancelled = false;
    let connected: LiveTable | null = null;
    let unsubscribe: (() => void) | null = null;
    // Accumulates keys touched since the last clear; a mutable Set the closure below owns for the
    // life of this effect (mirrors `cancelled`'s pattern -- one instance per effect run, torn down
    // on cleanup). Handed to setState only via a fresh clone (see scheduleFlashClear/onChange
    // below) so a consumer holding an old `flashKeys` never sees it mutate out from under them.
    let touchedAccum = new Set<string>();
    let flashTimer: ReturnType<typeof setTimeout> | null = null;
    setState({ rows: [], loading: true, error: null, table: null, flashKeys: EMPTY_FLASH_KEYS });

    // (Re)arms the clear-after-FLASH_WINDOW_MS timer; called on every touch-bearing batch so a
    // second batch landing inside the window pushes the clear out rather than firing on schedule
    // for the first batch alone -- same "extend, don't reset the count but do reset the clock"
    // behaviour as web/src/hooks/useTableRows.ts's flashTimer.
    function scheduleFlashClear(): void {
      if (flashTimer) clearTimeout(flashTimer);
      flashTimer = setTimeout(() => {
        flashTimer = null;
        touchedAccum = new Set();
        if (cancelled) return;
        setState((prev) => ({ ...prev, flashKeys: EMPTY_FLASH_KEYS }));
      }, FLASH_WINDOW_MS);
    }

    client.table(name, { key: opts.key, timeoutMs: opts.timeoutMs }).then(
      (t) => {
        if (cancelled) {
          t.close(); // effect was superseded (name/key/client changed, or unmount) before this landed
          return;
        }
        connected = t;
        // NOTE: on a reconnect, `touched` is every current key (LiveTable marks the whole reseed
        // as changed since deltas missed while disconnected can't be replayed -- see live-table.ts's
        // subscribeSnapshotReplay doc comment). Flashing the entire table then is correct and
        // intentional, not a bug to "fix" by special-casing a large touched set here.
        unsubscribe = t.onChange((rows, touched) => {
          for (const k of touched) touchedAccum.add(k);
          if (touched.size > 0) scheduleFlashClear();
          setState({ rows, loading: false, error: null, table: t, flashKeys: new Set(touchedAccum) });
        });
        setState({ rows: t.rows, loading: false, error: null, table: t, flashKeys: EMPTY_FLASH_KEYS });
      },
      (err: unknown) => {
        if (cancelled) return;
        setState({ rows: [], loading: false, error: errorOf(err), table: null, flashKeys: EMPTY_FLASH_KEYS });
      },
    );

    return () => {
      cancelled = true;
      if (flashTimer) clearTimeout(flashTimer);
      if (unsubscribe) unsubscribe();
      if (connected) connected.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- keyDep stands in for opts.key
  }, [name, client, keyDep, opts.timeoutMs]);

  return state;
}

/** Ad-hoc SQL: validate -> import -> LiveTable, via client.sql(). `sql` undefined => idle. */
export function useLiveSql(sql: string | undefined, opts: UseLiveSqlOptions): LiveTableState {
  const client = useStreamForge();
  const [state, setState] = useState<LiveTableState>(IDLE_STATE);
  const { name, timeoutMs } = opts;
  // Same reasoning as useLiveTable's keyDep -- see the comment there.
  const keyDep = JSON.stringify(opts.key ?? null);

  useEffect(() => {
    if (sql === undefined) {
      setState(IDLE_STATE);
      return;
    }
    if (!client) {
      setState({ rows: [], loading: true, error: null, table: null, flashKeys: EMPTY_FLASH_KEYS });
      return;
    }

    let cancelled = false;
    let connected: LiveTable | null = null;
    let unsubscribe: (() => void) | null = null;
    // See useLiveTable's identical block above for why this is a plain mutable closure variable
    // rather than a ref: it lives and dies with this one effect run, exactly like `cancelled`.
    let touchedAccum = new Set<string>();
    let flashTimer: ReturnType<typeof setTimeout> | null = null;
    setState({ rows: [], loading: true, error: null, table: null, flashKeys: EMPTY_FLASH_KEYS });

    function scheduleFlashClear(): void {
      if (flashTimer) clearTimeout(flashTimer);
      flashTimer = setTimeout(() => {
        flashTimer = null;
        touchedAccum = new Set();
        if (cancelled) return;
        setState((prev) => ({ ...prev, flashKeys: EMPTY_FLASH_KEYS }));
      }, FLASH_WINDOW_MS);
    }

    client.sql(sql, { name, key: opts.key, timeoutMs }).then(
      (t) => {
        if (cancelled) {
          t.close();
          return;
        }
        connected = t;
        // Reconnect note: see useLiveTable's identical onChange above -- a whole-table touched set
        // right after a reconnect is intentional, not special-cased here.
        unsubscribe = t.onChange((rows, touched) => {
          for (const k of touched) touchedAccum.add(k);
          if (touched.size > 0) scheduleFlashClear();
          setState({ rows, loading: false, error: null, table: t, flashKeys: new Set(touchedAccum) });
        });
        setState({ rows: t.rows, loading: false, error: null, table: t, flashKeys: EMPTY_FLASH_KEYS });
      },
      (err: unknown) => {
        if (cancelled) return;
        setState({ rows: [], loading: false, error: errorOf(err), table: null, flashKeys: EMPTY_FLASH_KEYS });
      },
    );

    return () => {
      cancelled = true;
      if (flashTimer) clearTimeout(flashTimer);
      if (unsubscribe) unsubscribe();
      if (connected) connected.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- keyDep stands in for opts.key
  }, [sql, client, name, keyDep, timeoutMs]);

  return state;
}

/** The catalog's table definitions, one-shot per client. */
export function useTables(): TablesState {
  const client = useStreamForge();
  const [state, setState] = useState<TablesState>({ tables: [], loading: true, error: null });

  useEffect(() => {
    if (!client) {
      // Same as above: no client yet is a wait, not a failure.
      setState({ tables: [], loading: true, error: null });
      return;
    }

    let cancelled = false;
    setState({ tables: [], loading: true, error: null });

    client.tables().then(
      (tables) => {
        if (cancelled) return;
        setState({ tables, loading: false, error: null });
      },
      (err: unknown) => {
        if (cancelled) return;
        setState({ tables: [], loading: false, error: errorOf(err) });
      },
    );

    return () => {
      cancelled = true;
    };
  }, [client]);

  return state;
}
