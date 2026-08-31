/**
 * SignalR live transport, over @microsoft/signalr itself -- never hand-rolled (unlike the Python
 * client's _hub.py, which reimplements the wire protocol because Python has no first-party
 * SignalR client). `app.MapHub<StreamHub>("/hubs/stream")` restricts no transports, so the engine
 * serves WebSockets, Server-Sent Events and Long Polling on that one URL/port -- this class just
 * picks one via `HttpConnectionBuilder.withUrl`'s `transport` option, mirroring
 * web/src/realtime/hub.ts's `resolveTransport()` (its `localStorage['sf.transport']` override is
 * the operator-facing reason all three modes are exposed here rather than just the default).
 *
 * One HubConnection is shared across every table a Client subscribes to (same multiplexing
 * web/src/realtime/hub.ts does with its module-level singleton) -- `getConnection()` lazily
 * (re)connects, and a dropped connection ends every in-flight subscribe() generator so
 * live-table.ts's own reconnect-with-backoff loop re-subscribes AND re-snapshots (§3.6: resuming
 * a stream without a fresh snapshot silently corrupts the Z-set). Deliberately no
 * `withAutomaticReconnect()` here -- that would resume the SAME logical connection behind
 * live-table.ts's back, which is exactly the silent-gap failure mode the design doc calls out;
 * one reconnect policy (live-table.ts's) beats two disagreeing ones.
 */

import * as signalR from "@microsoft/signalr";
import { StreamsForgeError } from "./errors.js";
import type { RestClient } from "./http.js";
import * as tablesModule from "./tables.js";
import type { Transport } from "./transport.js";
import type { Delta, Row } from "./zset.js";

export type SignalRMode = "ws" | "sse" | "lp";

function transportTypeFor(mode: SignalRMode): signalR.HttpTransportType {
  switch (mode) {
    case "ws":
      return signalR.HttpTransportType.WebSockets;
    case "sse":
      return signalR.HttpTransportType.ServerSentEvents;
    case "lp":
      return signalR.HttpTransportType.LongPolling;
  }
}

type DeltaPush = (deltas: Delta[], seq: number) => void;
type CloseListener = (err?: Error) => void;

export class SignalRTransport implements Transport {
  readonly name: string;
  private connection: signalR.HubConnection | null = null;
  private connectingPromise: Promise<signalR.HubConnection> | null = null;
  private deltaListeners = new Map<string, Set<DeltaPush>>();
  private closeListeners = new Set<CloseListener>();

  constructor(
    private readonly http: RestClient,
    private readonly mode: SignalRMode,
  ) {
    this.name = `signalr:${mode}`;
  }

  /** Public so connect()'s "auto" probing (probeSignalRMode below) can prove a mode actually
   * connects without duplicating the connection-setup logic. */
  async getConnection(): Promise<signalR.HubConnection> {
    if (this.connection && this.connection.state === signalR.HubConnectionState.Connected) {
      return this.connection;
    }
    if (this.connectingPromise) return this.connectingPromise;
    this.connectingPromise = this.connectNow();
    try {
      return await this.connectingPromise;
    } finally {
      this.connectingPromise = null;
    }
  }

  private async connectNow(): Promise<signalR.HubConnection> {
    const conn = new signalR.HubConnectionBuilder()
      .withUrl(`${this.http.baseUrl}/hubs/stream`, {
        transport: transportTypeFor(this.mode),
        accessTokenFactory: () => this.http.token(),
      })
      .build();

    conn.on("tableDelta", (name: string, deltas: Array<{ row: Row; weight: number }>, seq: number) => {
      const listeners = this.deltaListeners.get(name);
      if (!listeners || listeners.size === 0) return;
      const converted: Delta[] = deltas.map((d) => [d.row, d.weight] as const);
      for (const fn of listeners) fn(converted, seq);
    });
    conn.onclose((err) => {
      this.connection = null;
      const listeners = this.closeListeners;
      this.closeListeners = new Set();
      for (const fn of listeners) fn(err);
    });

    await conn.start();
    this.connection = conn;
    return conn;
  }

  /**
   * Establishes the subscription and only THEN resolves, per Transport.subscribe()'s contract
   * (transport.ts's doc comment): connects (or reuses) the shared HubConnection, registers the
   * delta listener, and -- the real handshake -- `await`s `invoke("SubscribeTable", …)`, whose
   * promise SignalR resolves only once the server's completion message for that invocation comes
   * back. The listener is wired up BEFORE that await, so nothing the server sends between the ack
   * and the caller's first read of the returned iterable is lost; it just queues. This function
   * must NOT be an `async function*` itself -- a generator's body, including this handshake,
   * would not run until the caller's first `.next()`, silently deferring establishment past
   * whatever the caller does next (live-table.ts's snapshot read) and reopening the race this
   * contract exists to close.
   */
  async subscribe(tableName: string, signal: AbortSignal): Promise<AsyncIterable<readonly [Delta[], number]>> {
    const self = this;
    const conn = await this.getConnection();

    const queue: Array<readonly [Delta[], number]> = [];
    let waiter: (() => void) | null = null;
    let ended = false;
    let endError: Error | undefined;

    const push: DeltaPush = (deltas, seq) => {
      queue.push([deltas, seq]);
      const w = waiter;
      waiter = null;
      w?.();
    };
    let listeners = this.deltaListeners.get(tableName);
    if (!listeners) {
      listeners = new Set();
      this.deltaListeners.set(tableName, listeners);
    }
    listeners.add(push);

    const onClose: CloseListener = (err) => {
      ended = true;
      endError = err;
      const w = waiter;
      waiter = null;
      w?.();
    };
    this.closeListeners.add(onClose);

    const onAbort = () => {
      ended = true;
      const w = waiter;
      waiter = null;
      w?.();
    };

    const cleanupListeners = () => {
      self.deltaListeners.get(tableName)?.delete(push);
      self.closeListeners.delete(onClose);
      signal.removeEventListener("abort", onAbort);
    };

    if (signal.aborted) {
      cleanupListeners();
      return { [Symbol.asyncIterator]: () => (async function* () {})() };
    }
    signal.addEventListener("abort", onAbort);

    try {
      await conn.invoke("SubscribeTable", tableName);
    } catch (err) {
      // Failed establishment must not leave a half-registered subscription (a listener with
      // nobody ever reading it, or a server-side SubscribeTable that landed anyway) -- unwind
      // both before propagating, rather than leaking either.
      cleanupListeners();
      try {
        await conn.invoke("UnsubscribeTable", tableName);
      } catch {
        // best-effort: if SubscribeTable itself failed, the server likely never registered us
      }
      throw new StreamsForgeError(`SignalR SubscribeTable('${tableName}') failed to establish: ${String(err)}`);
    }

    const iterate = async function* (): AsyncGenerator<readonly [Delta[], number]> {
      try {
        while (true) {
          if (queue.length > 0) {
            yield queue.shift()!;
            continue;
          }
          if (signal.aborted) return;
          if (ended) {
            if (endError) throw new StreamsForgeError(`SignalR subscription for '${tableName}' ended: ${endError.message}`);
            return;
          }
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      } finally {
        cleanupListeners();
        if (!signal.aborted && self.connection) {
          try {
            await self.connection.invoke("UnsubscribeTable", tableName);
          } catch {
            // best-effort: the connection may already be on its way down
          }
        }
      }
    };
    return { [Symbol.asyncIterator]: () => iterate() };
  }

  async snapshot(tableName: string, limit = 500): Promise<readonly [Delta[], number]> {
    // Snapshot is REST for every wire mode -- there is no "SSE version" of GET /rows.
    return tablesModule.snapshotDeltas(this.http, tableName, limit);
  }

  async close(): Promise<void> {
    const conn = this.connection;
    this.connection = null;
    if (conn) await conn.stop();
  }
}

/** auto: try ws, then sse, then give up on lp (it needs no upgrade and no long-lived probe
 * connection, so it's the mode that "always works" if REST does) -- mirrors __init__.py's
 * `_probe_signalr_mode` on the Python side. */
export async function probeSignalRMode(http: RestClient): Promise<SignalRMode> {
  for (const mode of ["ws", "sse"] as const) {
    try {
      const probe = new SignalRTransport(http, mode);
      await probe.getConnection();
      await probe.close();
      return mode;
    } catch (err) {
      console.warn(`streamsforge: signalr:${mode} unavailable (${String(err)}), trying next mode`);
    }
  }
  return "lp";
}
