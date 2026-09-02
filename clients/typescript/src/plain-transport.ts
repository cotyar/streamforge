/**
 * Plain WebSocket / SSE live transport -- the SAME `tableDelta` contract SignalR carries
 * (`name`, `[{row, weight}]`, `seq`), but over a bare socket with no hub protocol, so a non-.NET
 * server (`@streamsforge/server`, or anything else that speaks this) can feed live-table.ts.
 *
 * Wire contract (also documented in server/README.md):
 *
 *   SSE   GET {base}/api/tables/{name}/live        one connection per subscription
 *         event: subscribed   data: {"name":"orders"}
 *         event: tableDelta   data: {"name":"orders","deltas":[{"row":{},"weight":1}],"seq":12}
 *   WS    GET {base}/api/live?access_token=…        one connection, multiplexed
 *         -> {"type":"subscribe","table":"orders"} / {"type":"unsubscribe","table":"orders"}
 *         <- {"type":"subscribed","table":"orders"}
 *         <- {"type":"tableDelta","name":"orders","deltas":[…],"seq":12}
 *         <- {"type":"error","table":"orders","message":"…"}
 *
 * SSE is fetch+ReadableStream rather than `EventSource` because EventSource cannot send an
 * Authorization header. `subscribe()` resolves only on the server's `subscribed` ack, per
 * transport.ts's contract. No reconnect here -- live-table.ts owns that policy.
 */

import { StreamsForgeError } from "./errors.js";
import type { RestClient } from "./http.js";
import * as tablesModule from "./tables.js";
import type { Transport } from "./transport.js";
import type { Delta, Row } from "./zset.js";

export type PlainMode = "ws" | "sse";

type WireDelta = { row: Row; weight: number };
type Batch = readonly [Delta[], number];

/** Pull-side of a push queue: an AsyncIterable fed by `push`, ended by `end`. */
function channel(signal: AbortSignal, tableName: string) {
  const queue: Batch[] = [];
  let waiter: (() => void) | null = null;
  let ended = false;
  let endError: Error | undefined;
  const wake = () => {
    const w = waiter;
    waiter = null;
    w?.();
  };
  const push = (deltas: WireDelta[], seq: number) => {
    queue.push([deltas.map((d) => [d.row, d.weight] as const), seq]);
    wake();
  };
  const end = (err?: Error) => {
    ended = true;
    endError = err;
    wake();
  };
  signal.addEventListener("abort", wake, { once: true });
  const iterable = (onFinally: () => void): AsyncIterable<Batch> => ({
    [Symbol.asyncIterator]: () =>
      (async function* () {
        try {
          while (true) {
            if (queue.length > 0) {
              yield queue.shift()!;
              continue;
            }
            if (signal.aborted) return;
            if (ended) {
              if (endError) throw new StreamsForgeError(`${tableName} subscription ended: ${endError.message}`);
              return;
            }
            await new Promise<void>((r) => (waiter = r));
          }
        } finally {
          onFinally();
        }
      })(),
  });
  return { push, end, iterable };
}

export class PlainTransport implements Transport {
  readonly name: string;
  private ws: WebSocket | null = null;
  private wsOpening: Promise<WebSocket> | null = null;
  private wsSubs = new Map<string, { push: (d: WireDelta[], s: number) => void; end: (e?: Error) => void; ack: (e?: Error) => void }>();

  constructor(
    private readonly http: RestClient,
    private readonly mode: PlainMode,
  ) {
    this.name = mode;
  }

  subscribe(tableName: string, signal: AbortSignal): Promise<AsyncIterable<Batch>> {
    return this.mode === "ws" ? this.subscribeWs(tableName, signal) : this.subscribeSse(tableName, signal);
  }

  snapshot(tableName: string, limit = 500): Promise<Batch> {
    return tablesModule.snapshotDeltas(this.http, tableName, limit);
  }

  async close(): Promise<void> {
    const ws = this.ws;
    this.ws = null;
    ws?.close();
  }

  // ---- SSE ----

  private async subscribeSse(tableName: string, signal: AbortSignal): Promise<AsyncIterable<Batch>> {
    const ch = channel(signal, tableName);
    const res = await this.http.get(`/api/tables/${encodeURIComponent(tableName)}/live`, { headers: { accept: "text/event-stream" } });
    if (!res.ok || !res.body) throw new StreamsForgeError(`SSE live '${tableName}' failed: ${res.status} ${await res.text()}`);
    const reader = res.body.getReader();
    signal.addEventListener("abort", () => reader.cancel().catch(() => {}), { once: true });

    const ackBox: { fn?: (err?: Error) => void } = {};
    const acked = new Promise<void>((resolve, reject) => (ackBox.fn = (err) => (err ? reject(err) : resolve())));

    (async () => {
      const dec = new TextDecoder();
      let buf = "";
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          let idx: number;
          while ((idx = buf.indexOf("\n\n")) >= 0) {
            const frame = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            let event = "message";
            const data: string[] = [];
            for (const line of frame.split("\n")) {
              if (line.startsWith("event:")) event = line.slice(6).trim();
              else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
            }
            if (data.length === 0) continue; // comment / keepalive
            const msg = JSON.parse(data.join("\n")) as { deltas?: WireDelta[]; seq?: number; message?: string };
            if (event === "subscribed") ackBox.fn?.();
            else if (event === "tableDelta") ch.push(msg.deltas ?? [], msg.seq ?? 0);
            else if (event === "error") throw new StreamsForgeError(msg.message ?? "server error");
          }
        }
        ch.end();
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        ackBox.fn?.(e);
        ch.end(signal.aborted ? undefined : e);
      }
    })();

    await acked;
    return ch.iterable(() => reader.cancel().catch(() => {}));
  }

  // ---- WebSocket ----

  private async socket(): Promise<WebSocket> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return this.ws;
    if (this.wsOpening) return this.wsOpening;
    this.wsOpening = (async () => {
      const url = new URL(`${this.http.baseUrl}/api/live`);
      url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
      url.searchParams.set("access_token", await this.http.token());
      const ws = new WebSocket(url);
      await new Promise<void>((resolve, reject) => {
        ws.onopen = () => resolve();
        ws.onerror = () => reject(new StreamsForgeError(`WebSocket to ${url.host} failed`));
      });
      ws.onmessage = (ev) => {
        const msg = JSON.parse(String(ev.data)) as { type: string; table?: string; name?: string; deltas?: WireDelta[]; seq?: number; message?: string };
        const sub = this.wsSubs.get(msg.table ?? msg.name ?? "");
        if (!sub) return;
        if (msg.type === "subscribed") sub.ack();
        else if (msg.type === "tableDelta") sub.push(msg.deltas ?? [], msg.seq ?? 0);
        else if (msg.type === "error") sub.ack(new StreamsForgeError(msg.message ?? "server error"));
      };
      ws.onclose = () => {
        if (this.ws === ws) this.ws = null;
        const subs = this.wsSubs;
        this.wsSubs = new Map();
        for (const s of subs.values()) s.end(new Error("connection closed"));
      };
      this.ws = ws;
      return ws;
    })().finally(() => (this.wsOpening = null));
    return this.wsOpening;
  }

  private async subscribeWs(tableName: string, signal: AbortSignal): Promise<AsyncIterable<Batch>> {
    const ws = await this.socket();
    const ch = channel(signal, tableName);
    if (this.wsSubs.has(tableName)) throw new StreamsForgeError(`already subscribed to '${tableName}' on this connection`);
    const acked = new Promise<void>((resolve, reject) => {
      this.wsSubs.set(tableName, { push: ch.push, end: ch.end, ack: (err) => (err ? reject(err) : resolve()) });
    });
    ws.send(JSON.stringify({ type: "subscribe", table: tableName }));
    try {
      await acked;
    } catch (err) {
      this.wsSubs.delete(tableName);
      throw err;
    }
    return ch.iterable(() => {
      this.wsSubs.delete(tableName);
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "unsubscribe", table: tableName }));
    });
  }
}
