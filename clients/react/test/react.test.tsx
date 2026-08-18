/**
 * The smallest set of checks that actually fails if this package's logic breaks -- one runnable
 * check per piece of non-trivial behavior, not a per-function suite (this is a ponytail repo).
 *
 * Owns:
 *  1. useLiveTable, wired through a REAL LiveTable (from clients/typescript) over a hand-rolled
 *     FakeTransport, rendered via <StreamForgeProvider client={fakeClient}> + <LiveTablePanel>.
 *     Deltas pushed AFTER the initial render must reach the DOM -- that's the whole point of the
 *     package, so it gets the most scrutiny of anything here.
 *  2. The wait state: a client whose table() promise never resolves must leave the panel showing
 *     "loading", never "error".
 *  3. LiveTableView's pure rendering: column derivation order, empty state, error state, a
 *     formatCell override, and the epoch-ms column heuristic (name-gated, not just value-range).
 *  4. Sparkline: empty/single/all-equal values must never emit "NaN" into the SVG markup -- that
 *     is exactly the regression worth catching, not the geometry itself.
 *
 * No fake data reaches these tests through StreamForgeProvider's own connect() path -- that would
 * mean a real network handshake per test. Everything here uses `client` (the pre-built-client
 * escape hatch documented on StreamForgeProviderProps) instead, which is the supported way to test
 * against this package without a live engine.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { LiveTable } from "@streamforge/client";
import type { Client, Delta, Row, Transport } from "@streamforge/client";
import { LiveTablePanel, LiveTableView, Sparkline, StreamForgeProvider } from "../src/index.js";

afterEach(cleanup);

// ---- FakeTransport: the minimal, in-memory implementation of clients/typescript's Transport
// interface needed to drive a real LiveTable. subscribe() resolves synchronously -- standing in
// for "the handshake completed" -- so it honors the contract's ordering requirement (subscribe
// registers before the snapshot race begins) without any real I/O. push() delivers a delta batch
// to whichever reader loop is currently awaiting iter.next(), or queues it if none is waiting yet.

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

describe("useLiveTable via StreamForgeProvider + LiveTablePanel", () => {
  test("shows the initial snapshot immediately, and a delta pushed after render reaches the DOM", async () => {
    const transport = new FakeTransport([[{ id: "a", price: 1 }, 1]]);
    const table = await LiveTable.connect(transport, "prices", ["id"], 2_000);

    render(
      <StreamForgeProvider client={fakeClient(table)}>
        <LiveTablePanel name="prices" />
      </StreamForgeProvider>,
    );

    // Initial snapshot row: LiveTable.connect() has already subscribed/snapshotted/replayed by
    // the time client.table() resolves, so this should appear without needing to wait on a flush.
    await waitFor(() => expect(screen.getByText("a")).toBeTruthy());
    expect(screen.getByText("1")).toBeTruthy();

    // The point of the whole package: a delta arriving AFTER the component is already mounted and
    // painted must still show up (onChange coalesces at ~120ms -- waitFor's polling absorbs that).
    transport.push([[{ id: "b", price: 2 }, 1]]);
    await waitFor(() => expect(screen.getByText("b")).toBeTruthy(), { timeout: 2_000 });
    expect(screen.getByText("2")).toBeTruthy();

    table.close();
  });
});

describe("useLiveTable wait state", () => {
  test("a client whose table() never resolves leaves the panel loading, never errored", async () => {
    const client = {
      table: () => new Promise<LiveTable>(() => {}), // never settles
      sql: async () => {
        throw new Error("unused");
      },
      tables: async () => [],
      close: () => {},
    } as unknown as Client;

    render(
      <StreamForgeProvider client={client}>
        <LiveTablePanel name="prices" />
      </StreamForgeProvider>,
    );

    await waitFor(() => expect(screen.getByRole("status").textContent).toBe("Loading…"));
    expect(screen.queryByRole("alert")).toBeNull(); // error stays null
  });
});

describe("LiveTableView pure rendering", () => {
  test("derives column order from first-seen key order across rows", () => {
    const { container } = render(
      <LiveTableView
        rows={[
          { b: 1, a: 2 },
          { c: 3, a: 9 },
        ]}
      />,
    );
    const headers = Array.from(container.querySelectorAll("th")).map((th) => th.textContent);
    expect(headers).toEqual(["b", "a", "c"]);
  });

  test("shows the default empty-state text for zero rows, not loading", () => {
    render(<LiveTableView rows={[]} />);
    expect(screen.getByRole("status").textContent).toBe("No rows yet.");
  });

  test("honors a custom emptyText", () => {
    render(<LiveTableView rows={[]} emptyText="Nothing here." />);
    expect(screen.getByText("Nothing here.")).toBeTruthy();
  });

  test("renders the error message with role=alert instead of any data rows", () => {
    const { container } = render(<LiveTableView rows={[{ a: 1 }]} error={new Error("boom")} />);
    expect(screen.getByRole("alert").textContent).toBe("boom");
    expect(container.querySelectorAll("td.sf-table__cell").length).toBe(0);
  });

  test("a formatCell override replaces the default cell formatting", () => {
    const { container } = render(<LiveTableView rows={[{ name: "abc" }]} formatCell={(v) => `<${String(v)}>`} />);
    expect(container.querySelector("td")?.textContent).toBe("<abc>");
  });

  test("formats an epoch-ms column by name, but leaves a same-ranged number alone under a non-timestamp name", () => {
    const ts = 1_700_000_000_000; // 2023-11-14T... -- not "today" under any timezone
    const { container } = render(<LiveTableView rows={[{ id: 1, created_at: ts, count: ts }]} />);
    const cells = Array.from(container.querySelectorAll("td"));
    expect(cells[0]?.textContent).toBe("1"); // id: plain number
    expect(cells[1]?.textContent).not.toBe(String(ts)); // created_at: name matches TS_COLUMN -- reformatted
    expect(cells[1]?.textContent).toMatch(/:/); // the clock format always has colons; the raw epoch number never does
    expect(cells[2]?.textContent).toBe(String(ts)); // count: same value, but the name doesn't match -- left as a plain number
  });
});

describe("Sparkline", () => {
  test("renders an empty svg with no path/line for an empty array", () => {
    const { container } = render(<Sparkline values={[]} />);
    expect(container.querySelector("svg")).toBeTruthy();
    expect(container.querySelector("path")).toBeNull();
    expect(container.querySelector("line")).toBeNull();
  });

  test("renders a flat <line> for a single value, with no NaN coordinates", () => {
    const { container } = render(<Sparkline values={[42]} width={100} height={20} />);
    const line = container.querySelector("line");
    expect(line).toBeTruthy();
    for (const attr of ["x1", "y1", "x2", "y2"]) {
      expect(line?.getAttribute(attr)).not.toBeNull();
      expect(line?.getAttribute(attr)).not.toContain("NaN");
    }
  });

  test("renders a flat <path> with no NaN when every value is equal (range=0 divide-by-zero regression)", () => {
    const { container } = render(<Sparkline values={[5, 5, 5, 5]} width={100} height={20} />);
    const path = container.querySelector("path");
    expect(path).toBeTruthy();
    const d = path?.getAttribute("d") ?? "";
    expect(d).not.toContain("NaN");
    expect(d.startsWith("M0.0,")).toBe(true); // sanity: real coordinates got emitted, not an empty path
  });
});
