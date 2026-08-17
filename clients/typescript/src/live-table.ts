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
 * onChange callbacks are coalesced to roughly one per 120ms regardless of how fast deltas arrive
 * -- the same window live.py (Python client) and this repo's own hub-driven UI use, and for the
 * same reason: firing one callback per delta melts the consumer under a Monte-Carlo firehose
 * (tens of thousands of deltas/sec), while at most one frame of staleness is free.
 */

import { NotReady, StreamForgeError } from "./errors.js";
import type { Transport } from "./transport.js";
import { ZSet, type Delta, type Row } from "./zset.js";

const FLUSH_MS = 120; // coalesce window for onChange callbacks -- mirrors live.py's FLUSH_S
const MAX_BACKOFF_MS = 15_000;

export type ChangeListener = (rows: readonly Row[]) => void;

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
  private iterBuffer: Array<readonly Row[]> = [];
  private iterDone = false;

  private readyFlag = false;
  private readyWaiters: Array<{ resolve: () => void; reject: (err: Error) => void }> = [];
  private closed = false;
  private reconnectsCount = 0;
  private seqValue = 0;

  private pendingTouched: Set<string> | null = null;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;

  private readerAbort = new AbortController();
  private readonly readerLoopPromise: Promise<void>;

  private constructor(
    private readonly transport: Transport,
    private readonly tableName: string,
    private readonly keyFields: readonly string[] | null,
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
  ): Promise<LiveTable> {
    const table = new LiveTable(transport, tableName, keyFields);
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
   * onChange listener internally (no polling loop) so it resolves the instant a matching batch
   * lands rather than up to FLUSH_MS late. */
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
        if (this.iterBuffer.length > 0) {
          return Promise.resolve({ value: this.iterBuffer.shift()!, done: false });
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
    this.iterDone = true;
    for (const w of this.iterWaiters.splice(0)) w({ value: undefined, done: true });
    for (const w of this.readyWaiters.splice(0)) {
      w.reject(new StreamForgeError(`'${this.tableName}' was closed before it became ready`));
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
          `streamforge: ${this.tableName} reader error (reconnect #${this.reconnectsCount} in ${backoff}ms): ${String(err)}`,
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
          throw new StreamForgeError(`'${this.tableName}' subscription ended before the initial snapshot`);
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

    // Live loop: continue from `pendingNext`, which is already in flight and represents
    // whatever arrives next after the snapshot race settled.
    while (!this.closed) {
      const { value, done } = await pendingNext;
      if (done) {
        if (this.closed) return;
        throw new StreamForgeError(`'${this.tableName}' subscription stream ended`);
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
    if (touched.length === 0) return;
    if (this.pendingTouched) {
      for (const k of touched) this.pendingTouched.add(k);
      return; // a flush is already scheduled; it will pick up this batch too
    }
    this.pendingTouched = new Set(touched);
    this.flushTimer = setTimeout(() => {
      this.pendingTouched = null;
      this.flushTimer = null;
      this.flushRowsSnapshot();
      this.emit();
    }, FLUSH_MS);
  }

  private emit(): void {
    const rows = this.rowsFrozen;
    for (const cb of this.listeners) {
      try {
        cb(rows);
      } catch (err) {
        console.error(`streamforge: onChange callback for '${this.tableName}' raised`, err);
      }
    }
    if (this.iterWaiters.length > 0) {
      for (const w of this.iterWaiters.splice(0)) w({ value: rows, done: false });
    } else {
      this.iterBuffer.push(rows);
    }
  }
}
