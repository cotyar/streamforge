/**
 * Tests for hooks.ts's `flashKeys`: the "which rows just changed" accumulate-then-clear behaviour
 * added to `useLiveTable`/`useLiveSql`. Mirrors test/react.test.tsx's fake-Transport-plus-real-
 * LiveTable technique (a hand-rolled in-memory `Transport` driving a real `LiveTable`, rendered
 * under `<StreamForgeProvider client={...}>`) rather than importing from that file -- it's owned
 * by a concurrent agent this wave and must not be touched or depended on.
 *
 * Deliberately does NOT render through `LiveTableView`/`LiveTablePanel` (another agent is
 * rewriting those components right now): a tiny probe component reads `flashKeys` straight off
 * the hook and renders it as a sorted, comma-joined string, so assertions only ever touch this
 * file's own markup.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { LiveTable, canonicalKey } from "@streamforge/client";
import type { Client, Delta, Transport } from "@streamforge/client";
import { StreamForgeProvider, useLiveTable } from "../src/index.js";

afterEach(cleanup);

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---- FakeTransport: same minimal in-memory Transport as test/react.test.tsx (duplicated, not
// imported -- see file header). subscribe() resolves synchronously, standing in for "the
// handshake completed", and push() delivers a delta batch to whichever reader loop is currently
// awaiting iter.next() (or queues it if none is waiting yet).
class FakeTransport implements Transport {
  readonly name = "fake";
  private queue: Array<readonly [Delta[], number]> = [];
  private waiters: Array<(v: IteratorResult<readonly [Delta[], number]>) => void> = [];
  private seq = 0;

  constructor(
    private readonly snapshotRows: Delta[] = [],
    private readonly snapshotSeq = 0,
  ) {}

  async subscribe(_tableName: string, signal: AbortSignal): Promise<AsyncIterable<readonly [Delta[], number]>> {
    const waiters = this.waiters;
    const queue = this.queue;
    return {
      [Symbol.asyncIterator]() {
        return {
          next(): Promise<IteratorResult<readonly [Delta[], number]>> {
            if (signal.aborted) return Promise.resolve({ value: undefined, done: true });
            const next = queue.shift();
            if (next) return Promise.resolve({ value: next, done: false });
            return new Promise((resolve) => {
              const onAbort = () => {
                const idx = waiters.indexOf(resolve);
                if (idx !== -1) waiters.splice(idx, 1);
                resolve({ value: undefined, done: true });
              };
              waiters.push(resolve);
              signal.addEventListener("abort", onAbort, { once: true });
            });
          },
        };
      },
    };
  }

  async snapshot(_tableName: string): Promise<readonly [Delta[], number]> {
    return [this.snapshotRows, this.snapshotSeq];
  }

  close(): void {
    // no-op: nothing held open besides in-memory arrays.
  }

  /** Test-only: deliver a delta batch as if it just arrived over the wire. */
  push(deltas: Delta[]): void {
    this.seq += 1;
    const batch: readonly [Delta[], number] = [deltas, this.seq];
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: batch, done: false });
    else this.queue.push(batch);
  }
}

function fakeClient(table: LiveTable): Client {
  return {
    table: async () => table,
    sql: async () => {
      throw new Error("FakeTransport-backed test client does not implement sql()");
    },
    tables: async () => [],
    close: () => {},
  } as unknown as Client;
}

/** Test-only probe: renders useLiveTable's flashKeys as a sorted, comma-joined string of
 * canonical keys, so assertions can compare plain text instead of reaching into Set internals. */
function FlashProbe({ name }: { name: string }) {
  const { flashKeys } = useLiveTable(name);
  return <div data-testid="flash">{[...flashKeys].sort().join(",")}</div>;
}

const ROW_A1: Delta = [{ id: "a", price: 1 }, 1];
const ROW_B1: Delta = [{ id: "b", price: 1 }, 1];
const ROW_A2: Delta = [{ id: "a", price: 2 }, 1]; // supersedes ROW_A1's group ("id"="a")
const ROW_B2: Delta = [{ id: "b", price: 2 }, 1]; // supersedes ROW_B1's group ("id"="b")

/** A supersession changes TWO canonical keys, not one: the new tuple is asserted and the old one
 * stops existing. `ZSet.apply()` reports both (see its doc comment -- reporting only the assert was
 * a bug: a batch of pure retractions then looked like "nothing happened" and never flushed rows at
 * all), so `flashKeys` carries both here. Only the surviving row renders, so a host's flash CSS
 * lands on exactly one <tr>; the retired key is simply not on screen to match anything. */
function supersededPair(...deltas: Delta[]): string {
  return deltas.map((d) => canonicalKey(d[0])).sort().join(",");
}

describe("useLiveTable flashKeys", () => {
  test("a delta puts exactly the changed key(s) into flashKeys, not untouched keys", async () => {
    const transport = new FakeTransport([ROW_A1, ROW_B1]);
    const table = await LiveTable.connect(transport, "prices", ["id"], 2_000);

    render(
      <StreamForgeProvider client={fakeClient(table)}>
        <FlashProbe name="prices" />
      </StreamForgeProvider>,
    );

    // First connection (not a reconnect) never synthesizes a touched set -- starts empty.
    await waitFor(() => expect(screen.getByTestId("flash").textContent).toBe(""));

    // Only "a" changes; "b" must not show up in flashKeys.
    transport.push([ROW_A2]);
    const expectedKeyA = supersededPair(ROW_A1, ROW_A2);
    const untouchedKeyB = canonicalKey(ROW_B1[0]);

    await waitFor(() => expect(screen.getByTestId("flash").textContent).toBe(expectedKeyA), { timeout: 2_000 });
    expect(screen.getByTestId("flash").textContent).not.toContain(untouchedKeyB);

    table.close();
  });

  test("flashKeys clears ~900ms after the last touching batch", async () => {
    const transport = new FakeTransport([ROW_A1]);
    const table = await LiveTable.connect(transport, "prices", ["id"], 2_000);

    render(
      <StreamForgeProvider client={fakeClient(table)}>
        <FlashProbe name="prices" />
      </StreamForgeProvider>,
    );
    await waitFor(() => expect(screen.getByTestId("flash").textContent).toBe(""));

    transport.push([ROW_A2]);
    const expectedKeyA = supersededPair(ROW_A1, ROW_A2);
    await waitFor(() => expect(screen.getByTestId("flash").textContent).toBe(expectedKeyA), { timeout: 2_000 });

    // The window (900ms) runs from this batch's flush, which itself lands ~120ms after the push
    // (LiveTable's own onChange coalescing) -- 3s comfortably covers both without asserting on
    // exact timing.
    await waitFor(() => expect(screen.getByTestId("flash").textContent).toBe(""), { timeout: 3_000 });

    table.close();
  });

  test("a second delta inside the window extends rather than resets flashKeys", async () => {
    const transport = new FakeTransport([ROW_A1, ROW_B1]);
    const table = await LiveTable.connect(transport, "prices", ["id"], 2_000);

    render(
      <StreamForgeProvider client={fakeClient(table)}>
        <FlashProbe name="prices" />
      </StreamForgeProvider>,
    );
    await waitFor(() => expect(screen.getByTestId("flash").textContent).toBe(""));

    transport.push([ROW_A2]);
    const keyA = supersededPair(ROW_A1, ROW_A2);
    await waitFor(() => expect(screen.getByTestId("flash").textContent).toBe(keyA), { timeout: 2_000 });

    // Land a second batch well inside the 900ms window (but past LiveTable's own ~120ms
    // coalescing) -- both keys must accumulate, not replace one another.
    await sleep(300);
    transport.push([ROW_B2]);
    const expectedBoth = supersededPair(ROW_A1, ROW_A2, ROW_B1, ROW_B2);
    await waitFor(() => expect(screen.getByTestId("flash").textContent).toBe(expectedBoth), { timeout: 2_000 });

    // At the point the FIRST batch's window would have expired on its own (~1020ms after the
    // first push), the set must still hold both keys -- the second batch pushed the clear out
    // rather than merely adding to a set that was about to be wiped on the old schedule.
    await sleep(700); // ~1000ms total since the first push, still short of the second batch's own clear
    expect(screen.getByTestId("flash").textContent).toBe(expectedBoth);

    // Eventually clears, ~900ms after the LATEST touching batch.
    await waitFor(() => expect(screen.getByTestId("flash").textContent).toBe(""), { timeout: 3_000 });

    table.close();
  });
});
