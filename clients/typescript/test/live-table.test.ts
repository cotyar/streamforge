/**
 * Unit tests for LiveTable's change-tracking surface (onChange's `touched` set, `.entries()`,
 * `.row()`) and for the reconnect-emit bug fix -- all driven against a hand-rolled fake Transport
 * rather than a real engine, since none of this needs a server: it is pure LiveTable/ZSet wiring.
 * See engine-fixture.ts/contract.test.ts for the (separate) real-engine contract suite.
 */

import { describe, expect, test } from "bun:test";
import { LiveTable, type ChangeListener } from "../src/live-table.js";
import { canonicalKey, type Delta, type Row } from "../src/zset.js";
import type { Transport } from "../src/transport.js";

/** A controllable async iterable of delta batches: tests push into it on demand, and can end it
 * to simulate a dropped subscription -- same queue/waiters shape as LiveTable's own AsyncIterable
 * side, deliberately, so it's obviously equivalent to "a real transport would eventually do this". */
class PushableIter implements AsyncIterable<readonly [Delta[], number]> {
  private queue: Array<readonly [Delta[], number]> = [];
  private waiters: Array<(v: IteratorResult<readonly [Delta[], number]>) => void> = [];
  private ended = false;

  push(deltas: Delta[], seq: number): void {
    const item = [deltas, seq] as const;
    const w = this.waiters.shift();
    if (w) w({ value: item, done: false });
    else this.queue.push(item);
  }

  /** Ends the subscription after `afterMs` -- delayed so it never wins the initial
   * subscribe-vs-snapshot race in subscribeSnapshotReplay() (that race is not what these tests
   * are about; the snapshot must win it, exactly like a real transport). */
  endAfter(afterMs: number): void {
    setTimeout(() => {
      this.ended = true;
      for (const w of this.waiters.splice(0)) w({ value: undefined, done: true });
    }, afterMs);
  }

  [Symbol.asyncIterator](): AsyncIterator<readonly [Delta[], number]> {
    return {
      next: (): Promise<IteratorResult<readonly [Delta[], number]>> => {
        if (this.queue.length > 0) return Promise.resolve({ value: this.queue.shift()!, done: false });
        if (this.ended) return Promise.resolve({ value: undefined, done: true });
        return new Promise((resolve) => this.waiters.push(resolve));
      },
    };
  }
}

/** A never-resolving async iterable -- for an attempt where "no further delta is ever pushed"
 * needs to be literally true, not just "none happened to arrive during the test". Resolves
 * `done: true` on abort (see `withAbort` below) so it never hangs test cleanup. */
class HangingIter implements AsyncIterable<readonly [Delta[], number]> {
  [Symbol.asyncIterator](): AsyncIterator<readonly [Delta[], number]> {
    return { next: (): Promise<IteratorResult<readonly [Delta[], number]>> => new Promise(() => {}) };
  }
}

/** Wraps a fake iterable so a pending `next()` resolves `done: true` the instant `signal` aborts
 * -- exactly what every real Transport implementation does (transport.ts's own doc comment: "every
 * implementation tears itself down when [the AbortSignal] fires"). Without this, `LiveTable.close()`
 * aborts the signal but a fake iterable that ignores it leaves the reader loop stuck forever on
 * `await pendingNext`, which hangs `closeAndWait()` -- and therefore every test's own cleanup --
 * indefinitely regardless of what the test body itself asserted. */
function withAbort<T>(source: AsyncIterable<T>, signal: AbortSignal): AsyncIterable<T> {
  const inner = source[Symbol.asyncIterator]();
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      return {
        next: (): Promise<IteratorResult<T>> => {
          if (signal.aborted) return Promise.resolve({ value: undefined as unknown as T, done: true });
          return new Promise((resolve) => {
            const onAbort = () => resolve({ value: undefined as unknown as T, done: true });
            signal.addEventListener("abort", onAbort, { once: true });
            inner.next().then((r) => {
              signal.removeEventListener("abort", onAbort);
              resolve(r);
            });
          });
        },
      };
    },
  };
}

/** Fake Transport: one subscription-iterable and one snapshot per call to subscribe(), taken in
 * order from `attempts`. The last entry repeats if subscribe() is called more times than provided
 * (not exercised by these tests, but keeps a stray extra reconnect from throwing instead of
 * failing the assertion that actually matters). */
class FakeTransport implements Transport {
  readonly name = "fake";
  private calls = 0;
  constructor(
    private readonly attempts: Array<{
      iter: AsyncIterable<readonly [Delta[], number]>;
      snapshot: readonly [Delta[], number];
    }>,
  ) {}

  async subscribe(_tableName: string, signal: AbortSignal): Promise<AsyncIterable<readonly [Delta[], number]>> {
    const idx = Math.min(this.calls, this.attempts.length - 1);
    this.calls += 1;
    return withAbort(this.attempts[idx]!.iter, signal);
  }

  async snapshot(_tableName: string): Promise<readonly [Delta[], number]> {
    const idx = Math.min(this.calls - 1, this.attempts.length - 1);
    return this.attempts[idx]!.snapshot;
  }

  close(): void {}
}

function collect(): { listener: ChangeListener; calls: Array<{ rows: readonly Row[]; touched: ReadonlySet<string> }> } {
  const calls: Array<{ rows: readonly Row[]; touched: ReadonlySet<string> }> = [];
  const listener: ChangeListener = (rows, touched) => calls.push({ rows, touched });
  return { listener, calls };
}

describe("LiveTable.onChange touched set", () => {
  test("touched contains exactly the keys that changed in the batch, not the whole table", async () => {
    const baseline: Delta[] = [
      [{ id: "r1", val: 1 }, 1],
      [{ id: "r2", val: 1 }, 1],
      [{ id: "r3", val: 1 }, 1],
    ];
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [baseline, 1] }]);

    const table = await LiveTable.connect(transport, "t", ["id"], 5000);
    try {
      expect(table.rows.length).toBe(3);

      const { listener, calls } = collect();
      table.onChange(listener);

      // Update r1 (retract old identity, assert new) and insert r4 -- r2/r3 untouched.
      iter.push(
        [
          [{ id: "r1", val: 1 }, -1],
          [{ id: "r1", val: 2 }, 1],
          [{ id: "r4", val: 1 }, 1],
        ],
        2,
      );

      await new Promise((r) => setTimeout(r, 250)); // generous settle margin, well past the default 16ms flushMs window

      expect(calls.length).toBe(1);
      const { rows, touched } = calls[0]!;
      expect(rows.length).toBe(4); // r1(new), r2, r3, r4

      // Three keys genuinely changed: r1's OLD identity (val:1) was retracted -- it went from
      // present to absent, exactly as real a change as an assert -- plus r1's NEW identity
      // (val:2) and r4 were both asserted. r2/r3 are the ones that must NOT appear: this is the
      // "not the whole table" the test title is actually about.
      const expectedTouched = new Set([
        canonicalKey({ id: "r1", val: 1 }),
        canonicalKey({ id: "r1", val: 2 }),
        canonicalKey({ id: "r4", val: 1 }),
      ]);
      expect(touched.size).toBe(3);
      expect(new Set(touched)).toEqual(expectedTouched);
    } finally {
      await table.closeAndWait();
    }
  }, 10_000);

  test("a retraction reports its OWN key in touched, resolving to undefined via row() -- not a stranded assert", async () => {
    // Rewritten: this test used to assert BUGGY semantics -- ZSet.apply() only ever pushed
    // ASSERTED keys into its returned `touched`, so X's key only showed up in `touched` because an
    // earlier assert in the SAME 120ms coalesce window happened to strand it there (nothing ever
    // removed a key from `pendingTouched` once added). That coincidence made a pure retraction
    // LOOK like it reported its own key without the reducer actually doing so. Now that
    // ZSet.apply() reports retractions directly (see zset.ts's apply() doc comment), this asserts
    // the real contract: X's assert and its later retraction happen in SEPARATE, non-overlapping
    // coalesce windows, so the second emit's `touched` can only contain X's key if the retraction
    // itself was reported -- there is no stranded-assert coincidence left to lean on.
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [[], 1] }]);

    const table = await LiveTable.connect(transport, "t", ["id"], 5000);
    try {
      const xKey = canonicalKey({ id: "x", val: 1 });
      const yKey = canonicalKey({ id: "y", val: 1 });

      // First, separate window: assert X and Y, then let it flush and drain.
      iter.push(
        [
          [{ id: "x", val: 1 }, 1],
          [{ id: "y", val: 1 }, 1],
        ],
        2,
      );
      await new Promise((r) => setTimeout(r, 250));
      expect(table.row(xKey)).toEqual({ id: "x", val: 1 });

      const { listener, calls } = collect();
      table.onChange(listener);

      // Second, separate window: retract X only -- no co-occurring assert anywhere in this batch.
      iter.push([[{ id: "x", val: 1 }, -1]], 3);

      await new Promise((r) => setTimeout(r, 250));

      expect(calls.length).toBe(1); // a pure-retraction batch must still flush and notify
      const { touched } = calls[0]!;
      expect(touched).toEqual(new Set([xKey])); // X's own key, reported by the retraction itself -- not Y's

      expect(table.row(xKey)).toBeUndefined(); // retracted
      expect(table.row(yKey)).toEqual({ id: "y", val: 1 }); // untouched, still resolves to its row
    } finally {
      await table.closeAndWait();
    }
  }, 10_000);

  test("a batch of ONLY retractions notifies listeners and removes the row from LiveTable.rows", async () => {
    const baseline: Delta[] = [
      [{ id: "r1", val: 1 }, 1],
      [{ id: "r2", val: 1 }, 1],
    ];
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [baseline, 1] }]);

    const table = await LiveTable.connect(transport, "t", ["id"], 5000);
    try {
      expect(table.rows.map((r) => r.id).sort()).toEqual(["r1", "r2"]);

      const { listener, calls } = collect();
      table.onChange(listener);

      const r1Key = canonicalKey({ id: "r1", val: 1 });

      // Pure retraction: no assert anywhere in this batch. Before the fix, ZSet.apply() reported
      // no touched keys for this at all, so LiveTable.scheduleEmit()'s `touched.length === 0`
      // early-return skipped the flush entirely -- the retracted row would keep serving out of
      // `.rows` forever (until some unrelated assert happened to arrive).
      iter.push([[{ id: "r1", val: 1 }, -1]], 2);

      await new Promise((r) => setTimeout(r, 250));

      expect(calls.length).toBe(1); // must actually flush and notify listeners
      const { rows, touched } = calls[0]!;
      expect(touched).toEqual(new Set([r1Key]));
      expect(rows.map((r) => r.id)).toEqual(["r2"]);
      expect(table.row(r1Key)).toBeUndefined();

      // `.rows` itself (not just the callback argument) must reflect the retraction.
      expect(table.rows.map((r) => r.id)).toEqual(["r2"]);
    } finally {
      await table.closeAndWait();
    }
  }, 10_000);

  test("a group supersession reports the superseded stale key in touched, not just the new one", async () => {
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [[], 1] }]);

    // keyFields = ["id"] enables group supersession: two canonical rows sharing "id" are the same
    // logical entity at different times (a LATEST BY-style update).
    const table = await LiveTable.connect(transport, "t", ["id"], 5000);
    try {
      const aKey = canonicalKey({ id: "g1", val: 1 });
      const bKey = canonicalKey({ id: "g1", val: 2 });

      // First, separate window: assert the group's first row.
      iter.push([[{ id: "g1", val: 1 }, 1]], 2);
      await new Promise((r) => setTimeout(r, 250));
      expect(table.row(aKey)).toEqual({ id: "g1", val: 1 });

      const { listener, calls } = collect();
      table.onChange(listener);

      // Second, separate window: a NEW row for the SAME group ("id"="g1") -- supersedes the old
      // canonical row without any explicit retraction ever arriving on the wire for it.
      iter.push([[{ id: "g1", val: 2 }, 1]], 3);

      await new Promise((r) => setTimeout(r, 250));

      expect(calls.length).toBe(1);
      const { touched } = calls[0]!;
      expect(touched).toEqual(new Set([aKey, bKey])); // both the superseded key AND the new one

      expect(table.row(aKey)).toBeUndefined(); // the old row is gone
      expect(table.row(bKey)).toEqual({ id: "g1", val: 2 });
      expect(table.rows.length).toBe(1); // one row per group, as always
    } finally {
      await table.closeAndWait();
    }
  }, 10_000);
});

describe("LiveTable reconnect emit (bug fix)", () => {
  test("a listener is notified with post-reconnect rows without any further delta being pushed", async () => {
    // Attempt 1: connects and becomes ready normally, then its subscription silently ends
    // shortly after (simulating a dropped connection) -- this is what drives runReaderLoop into
    // a RECONNECT, not the first connection attempt.
    const iter1 = new PushableIter();
    iter1.endAfter(30);
    const snapshot1: Delta[] = [[{ id: "a", val: 1 }, 1]];

    // Attempt 2 (the reconnect): a DIFFERENT snapshot, and a subscription that never yields a
    // single delta -- if the listener sees the new rows at all, it can only be from the reconnect
    // fix's own emit(), not from a live delta arriving.
    const iter2 = new HangingIter();
    const snapshot2: Delta[] = [
      [{ id: "b", val: 2 }, 1],
      [{ id: "c", val: 3 }, 1],
    ];

    const transport = new FakeTransport([
      { iter: iter1, snapshot: [snapshot1, 1] },
      { iter: iter2, snapshot: [snapshot2, 2] },
    ]);

    const table = await LiveTable.connect(transport, "t", ["id"], 5000);
    try {
      expect(table.rows.map((r) => r.id)).toEqual(["a"]);

      const { listener, calls } = collect();
      table.onChange(listener);

      // Wait past iter1's drop (30ms) + runReaderLoop's reconnect backoff (starts at 1000ms) +
      // the reconnect's own subscribe/snapshot race -- generous margin, no delta is ever pushed
      // on either side for this test to pass.
      await new Promise((r) => setTimeout(r, 1500));

      expect(table.reconnects).toBeGreaterThanOrEqual(1);
      expect(table.rows.map((r) => r.id).sort()).toEqual(["b", "c"]);

      expect(calls.length).toBe(1); // exactly one emit for the reconnect -- not zero, not more
      const { rows, touched } = calls[0]!;
      expect(rows.map((r) => r.id).sort()).toEqual(["b", "c"]);
      expect(touched).toEqual(new Set([canonicalKey({ id: "b", val: 2 }), canonicalKey({ id: "c", val: 3 })]));
    } finally {
      await table.closeAndWait();
    }
  }, 10_000);

  test("the first connection attempt does not itself emit (no listener could exist yet anyway)", async () => {
    const iter = new HangingIter();
    const transport = new FakeTransport([{ iter, snapshot: [[[{ id: "a", val: 1 }, 1]], 1] }]);

    const table = await LiveTable.connect(transport, "t", ["id"], 5000);
    try {
      const { listener, calls } = collect();
      table.onChange(listener); // registered only after connect() resolves -- the realistic order
      await new Promise((r) => setTimeout(r, 200));
      expect(calls.length).toBe(0);
    } finally {
      await table.closeAndWait();
    }
  }, 10_000);
});

describe("LiveTable flushMs semantics (leading edge + trailing coalesce)", () => {
  test("a lone batch on an otherwise-quiet table emits with no artificial delay", async () => {
    // Default flushMs (16ms) applies -- no explicit 5th arg. A table fresh off connect() has never
    // emitted (lastEmitAt starts at -Infinity), so this first live delta must take the
    // leading-edge immediate path: no timer, no wait.
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [[], 1] }]);
    const table = await LiveTable.connect(transport, "t", ["id"], 5000);
    try {
      const emitted = new Promise<number>((resolve) => {
        table.onChange(() => resolve(Date.now()));
      });
      const t0 = Date.now();
      iter.push([[{ id: "x", val: 1 }, 1]], 2);
      const t1 = await emitted;
      // Well under the OLD unconditional 120ms trailing window -- this is the whole point of the
      // change: a lone update is no longer held back by a timer at all.
      expect(t1 - t0).toBeLessThan(100);
    } finally {
      await table.closeAndWait();
    }
  }, 10_000);

  test("a burst inside one window produces exactly one emit carrying the merged touched keys", async () => {
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [[], 1] }]);
    const table = await LiveTable.connect(transport, "t", ["id"], 5000);
    try {
      // Prime one leading-edge emit first (lastEmitAt starts at -Infinity, so this lands
      // immediately) so the burst below lands INSIDE the window it opens, not as its own
      // leading-edge emits.
      iter.push([[{ id: "prime", val: 0 }, 1]], 2);
      await new Promise((r) => setTimeout(r, 5)); // stay well inside the 16ms default window

      const { listener, calls } = collect();
      table.onChange(listener);

      iter.push([[{ id: "b", val: 1 }, 1]], 3);
      iter.push([[{ id: "c", val: 1 }, 1]], 4);
      iter.push([[{ id: "d", val: 1 }, 1]], 5);

      await new Promise((r) => setTimeout(r, 100)); // past the trailing flush deadline

      expect(calls.length).toBe(1); // three batches, ONE emit
      const { touched } = calls[0]!;
      expect(touched).toEqual(
        new Set([canonicalKey({ id: "b", val: 1 }), canonicalKey({ id: "c", val: 1 }), canonicalKey({ id: "d", val: 1 })]),
      );
    } finally {
      await table.closeAndWait();
    }
  }, 10_000);

  test("flushMs: 0 emits synchronously per batch -- no coalescing at all", async () => {
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [[], 1] }]);
    const table = await LiveTable.connect(transport, "t", ["id"], 5000, 0);
    try {
      const { listener, calls } = collect();
      table.onChange(listener);

      iter.push([[{ id: "a", val: 1 }, 1]], 2);
      iter.push([[{ id: "b", val: 1 }, 1]], 3);

      await new Promise((r) => setTimeout(r, 100));

      expect(calls.length).toBe(2); // one emit per batch, never merged
      expect(calls[0]!.touched).toEqual(new Set([canonicalKey({ id: "a", val: 1 })]));
      expect(calls[1]!.touched).toEqual(new Set([canonicalKey({ id: "b", val: 1 })]));
    } finally {
      await table.closeAndWait();
    }
  }, 10_000);

  test("the AsyncIterable buffer is latest-wins with capacity 1, not an unbounded queue", async () => {
    // flushMs: 0 so both batches below emit distinctly (not merged into one), proving the
    // SECOND emit's buffered snapshot overwrites the first rather than queuing behind it.
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [[], 1] }]);
    const table = await LiveTable.connect(transport, "t", ["id"], 5000, 0);
    try {
      // Nobody is draining the AsyncIterable yet, so both emits below land in `iterBuffer`.
      iter.push([[{ id: "a", val: 1 }, 1]], 2);
      await new Promise((r) => setTimeout(r, 20)); // let the first emit land and buffer
      iter.push([[{ id: "b", val: 1 }, 1]], 3);
      await new Promise((r) => setTimeout(r, 20)); // let the second emit land, overwriting the buffer

      const it = table[Symbol.asyncIterator]();
      const first = await it.next();
      expect(first.done).toBe(false);
      // Latest-wins: the one buffered snapshot reflects BOTH updates (a slow consumer sees current
      // state), not the stale intermediate one from the first emit.
      expect((first.value as Row[]).map((r) => r.id).sort()).toEqual(["a", "b"]);

      // And there is nothing else queued behind it -- an unbounded buffer would have a second,
      // stale entry waiting here; a capacity-1 buffer has none, so this next() call hangs until a
      // THIRD batch arrives (which never comes), proven by racing it against a short timeout.
      const secondOrTimeout = await Promise.race([
        it.next().then(() => "resolved"),
        new Promise<string>((resolve) => setTimeout(() => resolve("timed-out"), 100)),
      ]);
      expect(secondOrTimeout).toBe("timed-out");
    } finally {
      await table.closeAndWait();
    }
  }, 10_000);
});
