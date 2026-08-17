import type { Delta } from "./zset.js";

/**
 * The one interface every live transport implements: gRPC (grpc-transport.ts, Node-only) and
 * SignalR in its three wire modes (signalr-transport.ts, over @microsoft/signalr). live-table.ts,
 * the reducer and the contract-test suite are written against THIS and do not know which
 * concrete transport is underneath -- that is the whole point of running the suite once per
 * transport (design doc §3.6/§8): implementations that agree on every assertion are
 * interchangeable, and one that drifts fails on the same line the others pass.
 *
 * Unlike the Python client (which needs a `.cancel` side-channel because a generator's own
 * `.close()` isn't safe to call across threads), JS has no such hazard: `subscribe()` takes an
 * `AbortSignal` and every implementation tears itself down when it fires -- ordinary async/await,
 * no worker thread involved.
 *
 * `subscribe()` returns a PROMISE of an iterable, not a bare iterable -- this is load-bearing,
 * not stylistic. live-table.ts's contract is subscribe -> buffer -> snapshot -> replay, and the
 * buffering only helps if the subscription is actually registered with the server before the
 * snapshot read is issued; if establishment is still in flight when the snapshot fires, the
 * server has no subscription yet and any delta emitted in that window is never sent at all (not
 * buffered -- gone), while a fresh subscription gets no backfill to cover for it. A `Transport`
 * implementation MUST complete the real handshake (gRPC: create the call, which
 * `@grpc/grpc-js` puts on the wire immediately -- confirmed empirically, matching Python's grpc;
 * SignalR: `await connection.invoke("SubscribeTable", …)`, which resolves only once the server
 * has completed the Hub method) before this promise resolves. Returning an `async function*`
 * directly here would silently reintroduce the bug: an `async function*`'s body -- including
 * that handshake -- does not run at all until the caller's first `.next()`, so "subscribe, then
 * snapshot" would actually execute as "create a generator object, then race an unstarted
 * subscription against the snapshot," which is exactly backwards. (This is the same class of bug
 * this project's Kotlin client had via a cold `Flow`, and the .NET client's own
 * `IAsyncEnumerable` was checked for.)
 */
export interface Transport {
  readonly name: string;

  /**
   * Establish a live subscription for `tableName` and resolve once it is actually registered
   * with the server (see the interface doc comment above for why that distinction matters), then
   * yield (deltas, seq) batches until the subscription ends (error, a clean server-initiated
   * close, or `signal` aborts). No backfill: the first yielded item is whatever arrives after the
   * subscription is live, not the table's current contents -- callers pair this with snapshot()
   * and buffer/replay (live-table.ts), never rely on subscribe() alone.
   */
  subscribe(tableName: string, signal: AbortSignal): Promise<AsyncIterable<readonly [Delta[], number]>>;

  /**
   * One-shot read of the table's current consolidated rows (weight already summed server-side)
   * plus the read's own sequence number. Not comparable to subscribe()'s seq -- see zset.ts's
   * module docstring.
   */
  snapshot(tableName: string, limit?: number): Promise<readonly [Delta[], number]>;

  close(): Promise<void> | void;
}
