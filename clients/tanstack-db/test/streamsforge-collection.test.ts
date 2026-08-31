/**
 * Drives `streamsForgeCollectionOptions` with a real `Client`/`LiveTable` but a hand-rolled fake
 * `Transport` -- no server involved, same technique clients/typescript/test/live-table.test.ts
 * uses for LiveTable itself (this file does not copy it; it rebuilds the same small fixtures for
 * this package's own needs, which are slightly different: it needs a fully-constructed `Client`,
 * not just a bare `LiveTable`).
 */

import { describe, expect, test } from "bun:test";
import { createCollection } from "@tanstack/db";
import { Client, canonicalKey, type Delta } from "@streamsforge/client";
import { streamsForgeCollectionOptions } from "../src/index.js";
import type { Transport } from "@streamsforge/client";

/** Same controllable async iterable shape as live-table.test.ts's PushableIter. */
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

/** Honours the AbortSignal `LiveTable.close()` fires -- without this, closing the LiveTable never
 * unblocks a pending `iter.next()` and `closeAndWait()` (which our sync's cleanup indirectly
 * relies on tearing down) hangs, per live-table.test.ts's own doc comment on this exact helper. */
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

class FakeTransport implements Transport {
  readonly name = "fake";
  private calls = 0;
  closed = false;
  abortSeen = false;
  constructor(
    private readonly attempts: Array<{
      iter: AsyncIterable<readonly [Delta[], number]>;
      snapshot: readonly [Delta[], number];
    }>,
  ) {}

  async subscribe(_tableName: string, signal: AbortSignal): Promise<AsyncIterable<readonly [Delta[], number]>> {
    signal.addEventListener("abort", () => {
      this.abortSeen = true;
    });
    const idx = Math.min(this.calls, this.attempts.length - 1);
    this.calls += 1;
    return withAbort(this.attempts[idx]!.iter, signal);
  }

  async snapshot(_tableName: string): Promise<readonly [Delta[], number]> {
    const idx = Math.min(this.calls - 1, this.attempts.length - 1);
    return this.attempts[idx]!.snapshot;
  }

  close(): void {
    this.closed = true;
  }
}

/** `RestClient` isn't part of `@streamsforge/client`'s public surface (its `package.json` `exports`
 * only opens up ".", not internal paths), so this stands in an untouched `{}`: every test below
 * always passes `key` to the collection config, which makes `Client.table()` skip its one path
 * that would ever call into `http` (`resolveKeyFields`) -- see typescript/src/index.ts's
 * `Client.table()`. */
function makeClient(transport: Transport): Client {
  return new Client({} as never, null, transport, undefined, "fake");
}

async function waitUntil(pred: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitUntil timed out");
    await new Promise((r) => setTimeout(r, 10));
  }
}

describe("streamsForgeCollectionOptions", () => {
  test("seeds the collection from the initial snapshot as inserts, and reports ready only after", async () => {
    const baseline: Delta[] = [
      [{ id: "r1", val: 1 }, 1],
      [{ id: "r2", val: 1 }, 1],
    ];
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [baseline, 1] }]);
    const client = makeClient(transport);

    const collection = createCollection(streamsForgeCollectionOptions({ client, table: "t", key: ["id"] }));

    expect(collection.status).not.toBe("ready");
    await collection.preload();
    expect(collection.status).toBe("ready");
    expect(collection.size).toBe(2);
    expect(collection.get(canonicalKey({ id: "r1", val: 1 }))).toEqual({ id: "r1", val: 1 });
    expect(collection.get(canonicalKey({ id: "r2", val: 1 }))).toEqual({ id: "r2", val: 1 });

    await collection.cleanup();
  });

  test("a delta upsert updates the existing key without creating a duplicate", async () => {
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [[[{ id: "r1", val: 1 }, 1]], 1] }]);
    const client = makeClient(transport);

    const collection = createCollection(streamsForgeCollectionOptions({ client, table: "t", key: ["id"] }));
    await collection.preload();
    expect(collection.size).toBe(1);

    // Same logical row (key field "id"), new value -- a LATEST BY-style supersession: retract the
    // old canonical row, assert the new one. The OLD canonical key must not linger as a duplicate.
    iter.push(
      [
        [{ id: "r1", val: 1 }, -1],
        [{ id: "r1", val: 2 }, 1],
      ],
      2,
    );

    await waitUntil(() => collection.get(canonicalKey({ id: "r1", val: 2 })) !== undefined);
    expect(collection.size).toBe(1);
    expect(collection.get(canonicalKey({ id: "r1", val: 1 }))).toBeUndefined();
    expect(collection.get(canonicalKey({ id: "r1", val: 2 }))).toEqual({ id: "r1", val: 2 });

    await collection.cleanup();
  });

  test("a retraction deletes the row from the collection", async () => {
    const iter = new PushableIter();
    const transport = new FakeTransport([
      { iter, snapshot: [[[{ id: "x", val: 1 }, 1], [{ id: "y", val: 1 }, 1]], 1] },
    ]);
    const client = makeClient(transport);

    const collection = createCollection(streamsForgeCollectionOptions({ client, table: "t", key: ["id"] }));
    await collection.preload();
    expect(collection.size).toBe(2);

    const xKey = canonicalKey({ id: "x", val: 1 });
    const yKey = canonicalKey({ id: "y", val: 1 });

    // A PURE retraction of x -- no co-occurring assert anywhere in this batch, and y is untouched
    // throughout. `ZSet.apply()` now reports a retraction's own key in `touched` directly (see
    // @streamsforge/client's zset.ts `apply()` doc comment), so this needs no same-window coincidence
    // with an assert to exercise: `touched` alone drives this package's delete via `LiveTable.row(key)
    // === undefined` (see src/index.ts's `retract`).
    iter.push([[{ id: "x", val: 1 }, -1]], 2);

    await waitUntil(() => collection.size === 1 && collection.get(xKey) === undefined);
    expect(collection.get(yKey)).toEqual({ id: "y", val: 1 });

    await collection.cleanup();
  });

  test("a row that vanishes silently across a reconnect is deleted once the reconnect reseed lands", async () => {
    // Attempt 1: connects normally with two rows, then its subscription drops.
    const iter1 = new PushableIter();
    iter1.endAfter(30);
    const snapshot1: Delta[] = [
      [{ id: "a", val: 1 }, 1],
      [{ id: "b", val: 1 }, 1],
    ];

    // Attempt 2 (the reconnect): only "a" survives -- "b" is gone with no explicit retraction ever
    // sent for it (exactly the gap live-table.ts's reconnect-emit fix documents).
    const iter2 = new PushableIter();
    const snapshot2: Delta[] = [[{ id: "a", val: 1 }, 1]];

    const transport = new FakeTransport([
      { iter: iter1, snapshot: [snapshot1, 1] },
      { iter: iter2, snapshot: [snapshot2, 2] },
    ]);
    const client = makeClient(transport);

    const collection = createCollection(streamsForgeCollectionOptions({ client, table: "t", key: ["id"] }));
    await collection.preload();
    expect(collection.size).toBe(2);

    const bKey = canonicalKey({ id: "b", val: 1 });
    await waitUntil(() => collection.get(bKey) === undefined, 4000);
    expect(collection.size).toBe(1);
    expect(collection.get(canonicalKey({ id: "a", val: 1 }))).toEqual({ id: "a", val: 1 });

    await collection.cleanup();
  }, 10_000);

  test("cleanup closes the underlying LiveTable (the transport observes the abort)", async () => {
    const iter = new PushableIter();
    const transport = new FakeTransport([{ iter, snapshot: [[[{ id: "a", val: 1 }, 1]], 1] }]);
    const client = makeClient(transport);

    const collection = createCollection(streamsForgeCollectionOptions({ client, table: "t", key: ["id"] }));
    await collection.preload();

    await collection.cleanup();
    await waitUntil(() => transport.abortSeen, 2000);
    expect(transport.abortSeen).toBe(true);
  });
});
