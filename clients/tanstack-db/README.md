# @streamforge/tanstack-db

A [TanStack DB](https://tanstack.com/db) collection backed by a StreamForge live table.

**Layering** -- read this before anything else, because it's easy to conflate the pieces:

| Layer | Does |
| --- | --- |
| StreamForge **server** | Runs the actual incremental-view-maintenance (IVM) dataflow and ships Z-set deltas over the wire. |
| `@streamforge/client`'s **`LiveTable`** | The client-side reducer: folds those deltas into current rows (`zset.ts`). |
| `@streamforge/tanstack-db` (**this package**) | A thin bridge, nothing more: relays `LiveTable`'s already-reduced rows into a TanStack DB `Collection`. Adds no reduction logic of its own. |
| **TanStack DB** | Its own value on top: cross-collection live queries/joins evaluated in the browser, and (if the host app wires it up) optimistic local mutations. |

Three layers reduce something; this package reduces nothing -- it relays.

## Install

```bash
bun add @streamforge/tanstack-db @tanstack/db @streamforge/client
bun run build
```

`@tanstack/db` and `@streamforge/client` are peer dependencies -- the host app owns their versions.

## Quick start

```ts
import { createCollection } from "@tanstack/db";
import { connect } from "@streamforge/client";
import { streamForgeCollectionOptions } from "@streamforge/tanstack-db";

const sf = await connect({ url: "http://localhost:5199" });

const orders = createCollection(
  streamForgeCollectionOptions({ client: sf, table: "orders" }),
);

orders.get(someKey); // reads TanStack DB's own derived state
```

No `startSync` is set, so this follows TanStack DB's lazy default: nothing connects to
StreamForge until something actually reads from the collection (`.preload()`, a live query that
references it, a subscriber). That's the same "only sync what's queried" composition every other
TanStack DB collection follows.

### A live query joining two StreamForge tables

This is the payoff: once two StreamForge tables are each their own collection, TanStack DB's own
query layer joins and filters them client-side, live, with no extra server round trip per query.

```ts
import { createCollection, createLiveQueryCollection, eq } from "@tanstack/db";
import { streamForgeCollectionOptions } from "@streamforge/tanstack-db";

const orders = createCollection(streamForgeCollectionOptions({ client: sf, table: "orders" }));
const desks = createCollection(streamForgeCollectionOptions({ client: sf, table: "desk_exposure" }));

const ordersWithDesk = createLiveQueryCollection((q) =>
  q
    .from({ order: orders })
    .join({ desk: desks }, ({ order, desk }) => eq(order.deskId, desk.id))
    .where(({ desk }) => eq(desk.breached, true))
    .select(({ order, desk }) => ({ orderId: order.id, desk: desk.name, qty: order.qty })),
);
```

`ordersWithDesk` re-derives incrementally as either source collection changes -- StreamForge did
the heavy IVM lifting server-side per table; this join runs entirely in the browser over the two
already-reduced tables.

## Configuration

```ts
streamForgeCollectionOptions({
  client,     // a connected @streamforge/client Client
  table,      // materialized table name, as passed to client.table()
  key,        // optional key columns -- omit to resolve from the server's own table definition
  timeoutMs,  // optional connect timeout, forwarded to client.table()
})
```

## Identity: the collection's key is the Z-set's key

The collection's `getKey` is `canonicalKey(row)` -- the exact same canonical-key string
`LiveTable`'s own `touched` set reports and `LiveTable.row(key)` resolves (`zset.ts`, re-exported
by `@streamforge/client`). This is deliberate and non-negotiable: it is NOT a column this package
picks, and NOT a hash it invents. Two identity schemes over one dataset is exactly the failure
mode this package exists to avoid -- e.g. guessing `row.id` as the key would make a row update
that changes its own id-like field read as an unrelated insert-and-orphan instead of an update.

A second, smaller identity wrinkle: when a table has key columns (a `LATEST BY` / grouped table),
`LiveTable`'s Z-set reducer supersedes the OLD canonical row for a group when a NEW one for the
same group arrives. `ZSet.apply()` reports the superseded OLD key in `touched` alongside the new
one (see `zset.ts`'s `apply()` doc comment), resolving to `undefined` via `LiveTable.row(key)` just
like any other retraction -- so this package's own `touched`-driven insert/update/delete loop
(`src/index.ts`'s `upsert`/`retract`) handles it with no group-aware logic of its own; the stale row
never lingers as a duplicate in the TanStack DB collection.

## Not included, and why

**No mutation handlers.** This package does not implement `onInsert`/`onUpdate`/`onDelete` for the
collection it returns -- it is deliberately read-only.

```ts
// ponytail: StreamForge writes go through the INGEST path (client.push(source, rows)), which
// targets a SOURCE -- a different entity from the materialized TABLE this collection reads. Wiring
// TanStack DB's optimistic-mutation handlers here would silently conflate the two and be actively
// wrong, not merely incomplete. A future optimistic-write layer needs its own collection (or its
// own mutation handlers) that calls client.push() against the SOURCE feeding this table, not this
// module.
```

If a host app wants optimistic writes on top of a StreamForge-backed collection, that layer has to
target the source, and reconcile against whatever this collection converges to once the write's
effect flows back through the server's IVM pipeline and down to this table -- a genuinely separate
piece of work from what this package does.

## The LiveTable-level gap this package used to paper over -- now closed upstream

Earlier versions of `@streamforge/client`'s `ZSet.apply()` only ever pushed *asserted* keys into
the `touched` set it returns -- a pure retraction, or a group's superseded stale key (a LATEST BY
tick, an updated MTM price), never appeared in `touched` at all, so a row whose only server-side
event was a lone retraction could persist in `LiveTable.rows` (and, transitively, in this
package's collection) until some unrelated later batch happened to carry it along. This package
used to compensate for exactly that gap with its own `groupIndex` side-index plus a duplicate
`client.tables()` call to re-resolve `keyFields`.

That gap is now closed in `zset.ts`/`live-table.ts` itself: `ZSet.apply()` reports every key whose
presence or content actually changed -- asserts, retractions, and superseded stale keys alike (see
`@streamforge/client`'s `zset.ts` `apply()` doc comment for the exact contract). This package no
longer needs, and no longer carries, any compensating logic of its own: `touched` is the single
source of truth for insert/update vs. delete (`src/index.ts`'s `upsert`/`retract`), and the only
remaining special case is the reconnect reconciliation described above, which is a genuinely
different situation (deltas emitted while disconnected are gone, not merely unreported).

## Testing

```bash
bun test
```

`test/streamforge-collection.test.ts` drives a real `Client`/`LiveTable` against a hand-rolled fake
`Transport` -- no server involved, same technique `clients/typescript/test/live-table.test.ts` uses
for `LiveTable` on its own.
