/**
 * @streamforge/tanstack-db -- a TanStack DB collection backed by a StreamForge live table.
 *
 * Layering, spelled out because it's easy to conflate the two: the StreamForge SERVER is the
 * incremental-view-maintenance (IVM) engine -- it runs the differential dataflow and ships Z-set
 * deltas over the wire. `@streamforge/client`'s `LiveTable` is the CLIENT-side reducer that folds
 * those deltas into current rows (see its zset.ts doc comment for the reducer itself). This
 * package adds NO reduction logic of its own: it is a thin bridge that feeds an already-reduced
 * `LiveTable` into a TanStack DB `Collection` via `write()`/`begin()`/`commit()`, so a host app
 * gets TanStack DB's OWN value on top -- cross-collection live queries/joins evaluated in the
 * browser, and (if the app wires it up itself) optimistic local mutations. Three layers, three
 * jobs: server reduces the stream, LiveTable reduces the deltas, this package relays the result.
 *
 * Written against the ACTUAL `@tanstack/db` 0.1.12 typings (`node_modules/@tanstack/db/dist/esm/
 * types.d.ts`), not assumed from memory -- see the deviations noted inline below.
 */

import { canonicalKey } from "@streamforge/client";
import type { Client, LiveTable, Row } from "@streamforge/client";
import type { CollectionConfig } from "@tanstack/db";

export interface StreamForgeCollectionConfig {
  client: Client;
  /** Materialized table name, as passed to client.table(). */
  table: string;
  /** Key columns, forwarded straight through to client.table() -- the Z-set's own identity, NOT
   * this collection's getKey (see the identity note on `getKey` below). Omitted: `client.table()`
   * resolves it itself from the server's own table definition; this module has no need to
   * re-resolve or otherwise know that value (see the `upsert`/`retract` doc comment below for why
   * `touched` alone is now enough). */
  key?: string[];
  timeoutMs?: number;
}

/**
 * Spread into TanStack DB's `createCollection()`:
 *
 *   const table = createCollection(streamForgeCollectionOptions({ client, table: "orders" }));
 *
 * No `startSync` is set here, so this follows TanStack DB's own lazy default: nothing connects to
 * StreamForge until something actually reads from the collection (a `.preload()`, a live query
 * that references it, etc.) -- the same "only sync what's queried" composition TanStack DB expects
 * of every collection, not something this package should override.
 */
export function streamForgeCollectionOptions(config: StreamForgeCollectionConfig): CollectionConfig<Row, string> {
  const { client, table, timeoutMs } = config;

  return {
    id: `streamforge:${table}`,

    // IDENTITY: this MUST be LiveTable's own canonical key (zset.ts's canonicalKey, re-exported
    // by @streamforge/client), not a column we pick or a hash we invent. LiveTable already computes
    // this same string as the map key underlying `.entries()`'s `key` and `.row(key)` -- reusing it
    // verbatim is what keeps this collection's identity and the Z-set's identity the same scheme.
    // Two identity schemes over one dataset (e.g. a guessed `row.id`) is exactly the failure mode
    // this package exists to avoid: a row update that changes its own id-like field would then
    // read as an unrelated insert-and-orphan instead of an update.
    getKey: (row) => canonicalKey(row),

    // Every write() below carries the row's full, current content (LiveTable never hands out
    // partial rows), so "update" must REPLACE the synced value wholesale rather than the default
    // `partial` shallow-merge -- a column a new row no longer has would otherwise survive forever,
    // merged in from the stale previous value.
    sync: {
      rowUpdateMode: "full",
      sync: ({ begin, write, commit, markReady }) => {
        let liveTable: LiveTable | null = null;
        let cleanedUp = false;

        // `written` mirrors exactly the keys/rows this sync loop has told TanStack DB about --
        // NOT a second Z-set reducer (no weights, no group tracking). It exists because TanStack
        // DB's `write()` for a delete still needs a full `value` -- confirmed against the actual
        // installed `@tanstack/db` 0.1.12 typings, not assumed: `SyncConfig.sync`'s `write` param
        // is typed `(message: Omit<ChangeMessage<T>, "key">) => void`, and `ChangeMessage.value` is
        // required (not optional) regardless of `type`, so a delete carries a `value` just like an
        // insert or update does; the collection derives the key itself via `getKey(value)`. And by
        // the time a key is retracted, `LiveTable.row(key)` already returns `undefined` (that
        // absence IS the delete signal -- see live-table.ts's `row()` doc comment), so the last row
        // we wrote for a key is the only place left to get a `value` a delete can point back at.
        const written = new Map<string, Row>();

        let lastReconnects = 0;

        // `touched` alone is now enough to drive insert/update/delete: `ZSet.apply()` (the
        // reducer inside LiveTable) reports every key whose presence or content actually changed --
        // asserts, retractions, AND a group's superseded stale key alike (see @streamforge/client's
        // zset.ts `apply()` doc comment). That closed the gap this module used to paper over with
        // its own `groupIndex` side-index and a duplicate `client.tables()` call to re-resolve
        // `keyFields` (which `Client.table()` already resolves internally): a superseded row's OLD
        // key now shows up in `touched` with `LiveTable.row(key) === undefined`, exactly like any
        // other retraction, so no group-aware bookkeeping is needed in this package at all.
        const upsert = (key: string, row: Row): void => {
          write({ type: written.has(key) ? "update" : "insert", value: row });
          written.set(key, row);
        };

        const retract = (key: string): void => {
          const row = written.get(key);
          if (row === undefined) return; // already gone (or never written) -- nothing to delete
          write({ type: "delete", value: row });
          written.delete(key);
        };

        (async () => {
          const lt = await client.table(table, { key: config.key, timeoutMs });

          // The sync was torn down (host unmounted, collection GC'd) before connect() resolved --
          // adopting it now would leak a live subscription nothing will ever close.
          if (cleanedUp) {
            lt.close();
            return;
          }
          liveTable = lt;
          lastReconnects = lt.reconnects;

          // Seed once from LiveTable's already-reduced state -- entries() is O(current size), not
          // a diff, and this is the only place we ever iterate the whole table.
          begin();
          for (const entry of lt.entries()) upsert(entry.key, entry.row);
          commit();
          markReady();

          lt.onChange((_rows, touched) => {
            // Reconnect detection: LiveTable re-emits after a reconnect reseed with `touched` =
            // every CURRENTLY present key (see live-table.ts's "BUG FIX" comment on
            // subscribeSnapshotReplay) -- it is not a diff against the pre-drop state, because
            // deltas emitted while the connection was down are gone, not buffered anywhere to
            // replay. That means a row can vanish across the gap WITHOUT its key ever appearing in
            // any `touched` set: it is simply absent from the reconnect's full-current-keys set.
            // Ordinary per-delta emissions can't tell us this (a key that's simply not touched
            // might just be unchanged, not deleted) -- only "this is the reconnect emission, and
            // here is EVERY key that still exists" lets us tell the two apart. So we reconcile
            // `written` against `touched` ONLY on the emission where `reconnects` just changed, not
            // on every emission -- a full pass over `written` is fine there (reconnects are rare),
            // and skipping it the rest of the time is what keeps this loop touched-only, not a
            // rescan, in the steady state.
            const reconnected = lt.reconnects !== lastReconnects;
            lastReconnects = lt.reconnects;

            begin();
            for (const k of touched) {
              const row = lt.row(k);
              if (row !== undefined) upsert(k, row);
              else retract(k); // touched-but-absent: retracted (a plain retraction, or a
              // superseded stale key -- both now report the same way, see the doc comment above)
            }
            if (reconnected) {
              for (const k of Array.from(written.keys())) {
                if (!touched.has(k)) retract(k);
              }
            }
            commit();
          });
        })().catch((err: unknown) => {
          if (cleanedUp) return;
          // ponytail: @tanstack/db 0.1.12's SyncConfig has no async-error channel -- `sync()`'s
          // return type is effectively void (a cleanup function is the only thing it's allowed to
          // hand back), and the ONE place that would flip `collection.status` to `"error"`
          // (CollectionImpl#setStatus) is private and only ever called from the synchronous
          // try/catch around the `sync()` call itself, which has already returned by the time an
          // async `client.table()` call rejects. Reaching into that private method at runtime would
          // work today but is exactly the kind of internal-shape guess this package's brief said
          // not to make. So: don't swallow it (rethrow, so it surfaces as an unhandled rejection --
          // the same visibility any other un-awaited async failure gets), and leave the collection
          // parked at "loading" rather than pretending it's ready. A future @tanstack/db version
          // that adds a real async-error hook to SyncConfig should replace this.
          console.error(`streamforge: table '${table}' failed to sync into a TanStack DB collection`, err);
          throw err;
        });

        return () => {
          cleanedUp = true;
          liveTable?.close();
        };
      },
    },
  };
}
