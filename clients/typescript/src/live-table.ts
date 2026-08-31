/**
 * LiveTable: one table's Z-set state, kept current by an async reader loop (JS has no threads, so
 * unlike the Python client's dedicated reader thread this is plain non-blocking async/await --
 * see subscribeSnapshotReplay() below for how that simplifies the subscribe/snapshot race).
 *
 * Framework-free: `web/src/hooks/useTableRows.ts` is a React hook wrapping this same logic
 * inline; this class is that logic extracted so any consumer (a React hook, a Node script, a
 * plain <script> tag) can use it without React. `onChange`/the AsyncIterable are the two ways to
 * observe it; `rows` is a frozen snapshot array, never mutated in place -- a consumer holding a
 * stale reference never sees it change out from under it (same reasoning as the Python client's
 * ".df is a projection, not a mirror").
 *
 * onChange emissions are coalesced with a LEADING edge + TRAILING coalesce window (`flushMs`,
 * default 16ms -- one frame at 60Hz, the natural ceiling for a UI consumer that cannot display
 * more than one frame per 16ms anyway; `flushMs: 0` disables coalescing entirely and emits
 * synchronously per applied batch). If at least `flushMs` has elapsed since the last emit, a batch
 * is delivered immediately -- no timer, no wait -- so a lone update on an otherwise-quiet table is
 * never held back. Only a batch that lands INSIDE the window opened by the previous emit gets
 * merged into a single pending emit, fired at `lastEmit + flushMs`; further batches inside that
 * same window merge into the same pending emit, so at most one emit is ever pending. The window
 * exists at all because a firehose of tens of thousands of deltas/sec would otherwise fire one
 * callback per delta and melt the consumer -- NOT, as an earlier version of this comment claimed,
 * because this repo's own hub-driven UI does anything similar: `web/src/hooks/useTableRows.ts`
 * calls its flush on every batch with no coalescing window of its own (its 900ms timer is the
 * flash-highlight effect, an unrelated concern).
 */

import { NotReady, StreamsForgeError } from "./errors.js";
import type { Transport } from "./transport.js";
import { ZSet, type Delta, type Entry, type Row } from "./zset.js";

const DEFAULT_FLUSH_MS = 16; // one frame at 60Hz -- see the module doc comment above
const MAX_BACKOFF_MS = 15_000;

/** `touched` is the set of canonical keys (zset.ts's `canonicalKey`) that changed in the batch(es)
 * folded into this emit -- additive alongside `rows` so an existing one-arg listener keeps working
 * unchanged (JS ignores the extra argument). Resolve a key to its row with `LiveTable.row()`; a
 * key absent there was retracted, not upserted -- see that method's doc comment. */
export type ChangeListener = (rows: readonly Row[], touched: ReadonlySet<string>) => void;

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const t = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(t);
      resolve();
    });
  });
}

export class LiveTable implements AsyncIterable<readonly Row[]> {
  private zset: ZSet;
  private rowsFrozen: readonly Row[] = Object.freeze([]);
  private listeners = new Set<ChangeListener>();
  private iterWaiters: Array<(v: IteratorResult<readonly Row[]>) => void> = [];
  /** Latest-wins, capacity 1 -- NOT a queue. These are state snapshots (each carries the full
   * current `rows`, not a delta), so a consumer that stops calling `next()` for a while must catch
   * up to the LATEST state on its next call, not replay every intermediate one it missed -- an
   * unbounded array here would just be a silent memory leak under a slow `for await` body. See the
   * README's "change-notification latency and backpressure" section. */
  private iterBuffer: readonly Row[] | null = null;
  private iterDone = false;

  private readyFlag = false;
  private readyWaiters: Array<{ resolve: () => void; reject: (err: Error) => void }> = [];
  private closed = false;
  private reconnectsCount = 0;
  private seqValue = 0;

  private pendingTouched: Set<string> | null = null;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  /** -Infinity so the very first batch on a freshly-connected table always takes the leading-edge
   * immediate path -- there has never been a prior emit to be "inside the window" of. */
  private lastEmitAt = -Infinity;

  private readerAbort = new AbortController();
  private readonly readerLoopPromise: Promise<void>;

  private constructor(
    private readonly transport: Transport,
    private readonly tableName: string,
    private readonly keyFields: readonly string[] | null,
    private readonly flushMs: number,
  ) {
    this.zset = new ZSet(keyFields);
    this.readerLoopPromise = this.runReaderLoop();
  }

  /** Subscribes, snapshots, replays -- resolves once the table has filled (or rejects with
   * NotReady past `timeoutMs`). Async factory rather than a blocking constructor: JS has no
   * synchronous "wait on a background thread" primitive, and pretending otherwise would just be
   * a Promise wearing a constructor's clothes. */
  static async connect(
    transport: Transport,
    tableName: string,
    keyFields: readonly string[] | null,
    timeoutMs = 30_000,
    flushMs = DEFAULT_FLUSH_MS,
  ): Promise<LiveTable> {
    const table = new LiveTable(transport, tableName, keyFields, flushMs);
    await table.waitUntilReady(timeoutMs);
    return table;
  }

  private waitUntilReady(timeoutMs: number): Promise<void> {
    if (this.readyFlag) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const entry = {
        resolve: () => {
          clearTimeout(timer);
          resolve();
        },
        reject: (err: Error) => {
          clearTimeout(timer);
          reject(err);
        },
      };
      const timer = setTimeout(() => {
        this.readyWaiters = this.readyWaiters.filter((w) => w !== entry);
        this.close();
        reject(
          new NotReady(
            `table '${this.tableName}' did not fill within ${timeoutMs}ms -- a brand-new table gets ` +
              "no backfill, so this is expected until something pushes to it",
          ),
        );
      }, timeoutMs);
      this.readyWaiters.push(entry);
    });
  }

  // ---- public surface ----

  /** Current rows, frozen -- a fresh array each time state changes, never mutated in place. */
  get rows(): readonly Row[] {
    return this.rowsFrozen;
  }

  /** Current rows plus each one's canonical key and summed weight -- delegates to the underlying
   * ZSet's own `.entries()` (see zset.ts). */
  entries(): Entry[] {
    return this.zset.entries();
  }

  /** The current row for one canonical key (e.g. one drawn from a `ChangeListener`'s `touched`
   * set), or `undefined` if that key isn't present. A touched key absent here means the tuple was
   * retracted, not upserted -- that absence IS the delete signal, since ZSet never keeps a
   * tombstone around to say so explicitly. Delegates to ZSet.get(). */
  row(key: string): Row | undefined {
    return this.zset.get(key);
  }

  get ready(): boolean {
    return this.readyFlag;
  }

  get reconnects(): number {
    return this.reconnectsCount;
  }

  get seq(): number {
    return this.seqValue;
  }

  value(col: string, keys: Record<string, unknown>): unknown {
    for (const row of this.rowsFrozen) {
      let match = true;
      for (const [k, v] of Object.entries(keys)) {
        if (row[k] !== v) {
          match = false;
          break;
        }
      }
      if (match) return row[col];
    }
    return undefined;
  }

  /** Poll `pred(rows)` until it's true, or throw NotReady past `timeoutMs`. Registered as an
   * onChange listener internally, so it costs no polling interval of its own (no periodic
   * re-check, no lag or wasted work between checks) -- but it does NOT resolve any faster than
   * `onChange` itself fires: `waitFor` only sees what `onChange` decides to deliver, so it
   * inherits that emission's leading-edge/trailing-coalesce window (immediate if the table has
   * been quiet for `flushMs`, otherwise merged into the next scheduled flush) just like any other
   * listener. */
  waitFor(pred: (rows: readonly Row[]) => boolean, timeoutMs = 30_000): Promise<readonly Row[]> {
    if (pred(this.rowsFrozen)) return Promise.resolve(this.rowsFrozen);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        unsubscribe();
        reject(new NotReady(`waitFor on '${this.tableName}' timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      const unsubscribe = this.onChange((rows) => {
        if (pred(rows)) {
          clearTimeout(timer);
          unsubscribe();
          resolve(rows);
        }
      });
    });
  }

  /** Subscribe to change notifications; returns an unsubscribe function. Callbacks receive the
   * same frozen array `.rows` would return at that instant. */
  onChange(cb: ChangeListener): () => void {
    this.listeners.add(cb);
    return () => {
      this.listeners.delete(cb);
    };
  }

  [Symbol.asyncIterator](): AsyncIterator<readonly Row[]> {
    return {
      next: (): Promise<IteratorResult<readonly Row[]>> => {
        if (this.iterBuffer !== null) {
          const value = this.iterBuffer;
          this.iterBuffer = null;
          return Promise.resolve({ value, done: false });
        }
        if (this.iterDone) return Promise.resolve({ value: undefined, done: true });
        return new Promise((resolve) => this.iterWaiters.push(resolve));
      },
    };
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.readerAbort.abort();
    // A pending trailing-coalesce timer must not fire after close (it would call listeners/touch
    // iterBuffer on a table nobody can observe results from anymore), and the touched keys it was
    // about to merge are not a resource that needs draining -- just discard both.
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    this.pendingTouched = null;
    this.iterDone = true;
    for (const w of this.iterWaiters.splice(0)) w({ value: undefined, done: true });
    for (const w of this.readyWaiters.splice(0)) {
      w.reject(new StreamsForgeError(`'${this.tableName}' was closed before it became ready`));
    }
  }

  /** Waits for the reader loop to actually unwind (subscription torn down, etc), not just the
   * `closed` flag being set -- useful in tests that want a clean process exit. */
  async closeAndWait(): Promise<void> {
    this.close();
    await this.readerLoopPromise;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.closeAndWait();
  }

  // ---- reader loop ----

  private async runReaderLoop(): Promise<void> {
    let backoff = 1000;
    let firstAttempt = true;
    while (!this.closed) {
      try {
        await this.subscribeSnapshotReplay(firstAttempt);
        backoff = 1000;
      } catch (err) {
        if (this.closed) return;
        this.readyFlag = false;
        this.reconnectsCount += 1;
        console.warn(
          `streamsforge: ${this.tableName} reader error (reconnect #${this.reconnectsCount} in ${backoff}ms): ${String(err)}`,
        );
        await sleep(backoff, this.readerAbort.signal);
        if (this.closed) return;
        backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
      }
      firstAttempt = false;
    }
  }

  private async subscribeSnapshotReplay(_firstAttempt: boolean): Promise<void> {
    // A resumed connection without a fresh snapshot silently corrupts the Z-set (deltas emitted
    // while it was down are gone), so every (re)connect starts from a clean reducer.
    this.zset = new ZSet(this.keyFields);

    // A fresh AbortController per attempt: closing an earlier attempt must never cancel a later
    // reconnect's subscription.
    this.readerAbort = new AbortController();
    const signal = this.readerAbort.signal;

    // MUST be awaited before the snapshot read is issued, not raced against it: Transport.subscribe()
    // only resolves once the subscription is actually registered with the server (see
    // transport.ts's doc comment). Racing the two here -- e.g. calling snapshot() first and only
    // then awaiting subscribe() -- would reopen the exact bug that contract exists to close: a
    // delta emitted after the server computes the snapshot but before it finishes registering us
    // is never sent at all (not buffered -- the server didn't know we existed yet), and a fresh
    // subscription gets no backfill to cover for it. Once this resolves, though, the subscription
    // and the snapshot read genuinely race each other (both can now proceed concurrently), which
    // is what the buffering below is for.
    const subscription = await this.transport.subscribe(this.tableName, signal);
    const iter = subscription[Symbol.asyncIterator]();

    // Race the initial snapshot read against incoming delta batches, buffering the latter --
    // this is the JS analogue of live.py's "drain the queue non-blocking, then seed, then
    // replay": no separate thread is needed because `Promise.race` between an in-flight
    // `snapshot()` and the next `iter.next()` naturally interleaves the two without ever
    // blocking the event loop.
    const buffered: Array<readonly [Delta[], number]> = [];
    const snapshotPromise = this.transport.snapshot(this.tableName);
    let pendingNext = iter.next();

    let snapshotResult: readonly [Delta[], number] | null = null;
    while (snapshotResult === null) {
      const winner = await Promise.race([
        snapshotPromise.then((v) => ({ kind: "snapshot" as const, v })),
        pendingNext.then((v) => ({ kind: "delta" as const, v })),
      ]);
      if (winner.kind === "snapshot") {
        snapshotResult = winner.v;
      } else {
        if (winner.v.done) {
          throw new StreamsForgeError(`'${this.tableName}' subscription ended before the initial snapshot`);
        }
        buffered.push(winner.v.value);
        pendingNext = iter.next();
      }
    }

    const [snapRows, snapSeq] = snapshotResult;
    this.zset.seed(snapRows);
    this.seqValue = snapSeq;
    for (const [deltas, seq] of buffered) {
      if (!this.zset.alreadyReflected(deltas)) {
        this.zset.apply(deltas);
        this.seqValue = seq;
      }
    }
    this.flushRowsSnapshot();

    if (this.closed) return;
    this.readyFlag = true;
    for (const w of this.readyWaiters.splice(0)) w.resolve();

    // BUG FIX: on a RECONNECT (not the first attempt), the reseed above just silently changed
    // `.rows` out from under every listener with no notification. `readyWaiters` is empty by now
    // regardless -- it was already drained on the first successful attempt, so the resolve loop
    // just above is a no-op here -- and nothing else calls emit() until the next LIVE delta
    // arrives, which on a quiet table can be never (this method's own top comment: deltas emitted
    // while the connection was down are gone, not buffered anywhere to replay later). So a
    // listener would keep showing pre-drop state indefinitely even though the table has already
    // silently moved on. Fix: emit right here, once, for every reconnect but the very first
    // connection (where nobody could be listening yet -- LiveTable.connect() hasn't returned a
    // handle to subscribe onChange() on, so emitting there would be a no-op at best and a
    // misleading "changed" notification for the initial state at worst -- hence `_firstAttempt`
    // gates it, guaranteeing exactly one emit per reconnect and none on first connect). `touched`
    // is every current key rather than a guess at what moved: everything may have changed across
    // the gap, and that is the honest signal to send.
    if (!_firstAttempt) {
      this.doEmit(new Set(this.zset.entries().map((e) => e.key)));
    }

    // Live loop: continue from `pendingNext`, which is already in flight and represents
    // whatever arrives next after the snapshot race settled.
    while (!this.closed) {
      const { value, done } = await pendingNext;
      if (done) {
        if (this.closed) return;
        throw new StreamsForgeError(`'${this.tableName}' subscription stream ended`);
      }
      const [deltas, seq] = value;
      const touched = this.zset.apply(deltas);
      this.seqValue = seq;
      this.scheduleEmit(touched);
      pendingNext = iter.next();
    }
  }

  private flushRowsSnapshot(): void {
    this.rowsFrozen = Object.freeze(this.zset.rows());
  }

  private scheduleEmit(touched: string[]): void {
    // ZSet.apply() reports every key whose presence or content actually changed (asserts,
    // retractions, and superseded stale keys alike -- see zset.ts's apply() doc comment), so an
    // empty `touched` here genuinely means this batch changed nothing: safe to skip the flush.
    if (touched.length === 0) return;
    if (this.pendingTouched) {
      for (const k of touched) this.pendingTouched.add(k);
      return; // a trailing flush is already scheduled; it will pick up this batch too
    }
    const now = Date.now();
    const elapsed = now - this.lastEmitAt;
    if (elapsed >= this.flushMs) {
      // Leading edge: at least `flushMs` has passed since the last emit (or there has never been
      // one), so tell the consumer right away -- no timer, no wait. This is exactly the case a
      // pure trailing window always got wrong: a lone update on an otherwise-quiet table. Also
      // covers `flushMs: 0` for free -- `elapsed >= 0` is always true, so every batch takes this
      // branch and nothing is ever coalesced.
      this.flushRowsSnapshot();
      this.doEmit(new Set(touched));
      return;
    }
    // Trailing coalesce: still inside the window opened by the last emit. Merge into ONE pending
    // emit fired at `lastEmitAt + flushMs` (i.e. `elapsed` less than a full `flushMs` from now,
    // not a fresh `flushMs` from now) -- a batch that lands late in the window must not push the
    // deadline back out.
    this.pendingTouched = new Set(touched);
    this.flushTimer = setTimeout(() => {
      const flushed = this.pendingTouched!;
      this.pendingTouched = null;
      this.flushTimer = null;
      this.flushRowsSnapshot();
      this.doEmit(flushed);
    }, this.flushMs - elapsed);
  }

  /** Every actual emission -- leading-edge immediate, trailing-coalesce timer, or the reconnect
   * fix's own emit -- funnels through here so `lastEmitAt` (what the leading-edge check measures
   * against) is always up to date. */
  private doEmit(touched: ReadonlySet<string>): void {
    this.lastEmitAt = Date.now();
    this.emit(touched);
  }

  private emit(touched: ReadonlySet<string>): void {
    const rows = this.rowsFrozen;
    for (const cb of this.listeners) {
      try {
        cb(rows, touched);
      } catch (err) {
        console.error(`streamsforge: onChange callback for '${this.tableName}' raised`, err);
      }
    }
    if (this.iterWaiters.length > 0) {
      for (const w of this.iterWaiters.splice(0)) w({ value: rows, done: false });
    } else {
      this.iterBuffer = rows; // latest-wins, capacity 1 -- see the field's own doc comment
    }
  }
}
