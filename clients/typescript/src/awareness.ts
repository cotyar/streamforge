/**
 * Plan 020 wave G -- ephemeral presence/liveness for a CRDT document ("who is looking at this
 * document right now"), off by default server-side (`CrdtSourceConfig.Awareness` must be set) and
 * scoped to the SignalR hub only -- see `shared/StreamForge.Api/Hubs/StreamHub.cs` and
 * `AwarenessRegistry.cs` for the server side this speaks to.
 *
 * This is deliberately its OWN `HubConnection`, independent of whichever transport `connect()`
 * chose for table deltas (`signalr-transport.ts`'s shared, multiplexed connection). A `Client`
 * that picked `grpc` for tables still has no SignalR connection at all otherwise, and awareness --
 * unlike table deltas -- has no gRPC equivalent in this platform (plan 020's own cut list: "An
 * online REST/gRPC sync endpoint for live peers" is explicitly out of scope). One extra WebSocket
 * for the rare caller that actually turns awareness on is the honest cost of that, not an
 * oversight.
 *
 * The heartbeat loop lives here, client-side, entirely for the caller's convenience -- the server
 * TTL/cap mechanics (`AwarenessRegistry`) work identically against ANY caller that keeps invoking
 * `Heartbeat`, hand-rolled or not.
 */

import * as signalR from "@microsoft/signalr";
import { StreamForgeError } from "./errors.js";
import type { RestClient } from "./http.js";

/** Mirrors `AwarenessEntry` (shared/StreamForge.Api/Hubs/AwarenessRegistry.cs) as the SignalR JSON
 * protocol serializes it -- camelCase, ISO-8601 timestamps (`DateTimeOffset`'s default
 * `System.Text.Json` shape). `identity` is always the OTHER caller's authenticated name, never
 * something they chose -- see that record's own doc comment for why. */
export interface AwarenessPeer {
  clientId: string;
  identity: string;
  label: string | null;
  joinedAt: string;
  expiresAt: string;
}

export type AwarenessListener = (peers: readonly AwarenessPeer[]) => void;

export interface AwarenessOptions {
  /** Distinguishes two tabs/connections under the same identity. Default: a random id, stable for
   * this session only -- not a durable identifier and not meant to be one. */
  clientId?: string;
  /** Arbitrary cosmetic detail (a cursor color, a display variant) shown alongside `identity` --
   * never used for anything the server's AccessGuard reasons about. */
  label?: string;
}

/**
 * One joined presence session on one document. `AwarenessSession.join()` is the only way to get
 * one -- construction alone does not connect, matching every other transport's "the handshake
 * completes before you get an object back" contract in this client (see `transport.ts`'s own
 * doc comment on why `subscribe()` cannot return before `SubscribeTable` completes; the same
 * argument applies to `SubscribeAwareness`).
 */
export class AwarenessSession {
  private connection: signalR.HubConnection | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private readonly listeners = new Set<AwarenessListener>();
  private _peers: readonly AwarenessPeer[] = [];
  private closed = false;

  private constructor(
    private readonly sourceName: string,
    private readonly clientId: string,
  ) {}

  /** Current membership, including this session's own entry. Updated in place on every
   * `awarenessUpdate` push -- read it after `onUpdate` fires, or right after `join()` resolves for
   * the starting snapshot. */
  get peers(): readonly AwarenessPeer[] {
    return this._peers;
  }

  /** Fires on every membership change: a peer joining, leaving, or expiring. Returns an
   * unsubscribe function. Does NOT fire for the initial snapshot -- read `.peers` (or `join()`'s
   * own return value carries nothing extra; construct off `.peers` right after `join()`
   * resolves) for that, the same "await resolves with current state, listener covers what
   * happens after" split `signalr-transport.ts` uses for table snapshots vs. deltas. */
  onUpdate(listener: AwarenessListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /**
   * Connects, joins `sourceName`'s awareness group, and starts an internal heartbeat loop. Throws
   * a `StreamForgeError` when the server refuses the join -- no source by that name, the source is
   * not crdt-kind, the source has no `CrdtAwarenessConfig` (awareness is off, the default), or the
   * document is already at its configured cap. The refusal message is the server's own
   * `HubException.Message` (`AccessGuard`'s reason, or the specific config/cap complaint) --
   * `StreamHub`'s own doc comment on why that type specifically is what SignalR relays verbatim.
   */
  static async join(http: RestClient, sourceName: string, opts: AwarenessOptions = {}): Promise<AwarenessSession> {
    const clientId = opts.clientId ?? randomClientId();
    const session = new AwarenessSession(sourceName, clientId);
    await session.connectAndJoin(http, opts.label ?? null);
    return session;
  }

  private async connectAndJoin(http: RestClient, label: string | null): Promise<void> {
    // An explicit `transport` -- WebSockets, matching `signalr-transport.ts`'s default "ws" mode
    // -- not the SignalR client's own negotiate-then-auto-pick default: found live while building
    // this class (isolated instance, plan 020 wave G verification) that leaving `transport`
    // unset here hangs `conn.start()` indefinitely against this platform's `StreamHub`, while
    // every OTHER connection in this client always passes one explicitly
    // (`signalr-transport.ts`'s `transportTypeFor`, `probeSignalRMode`'s own probes). Root cause
    // not chased further -- the fix already matches this client's own established convention, and
    // awareness is one extra connection per caller, not a hot path worth a client picking its
    // transport dynamically the way table deltas do.
    const conn = new signalR.HubConnectionBuilder()
      .withUrl(`${http.baseUrl}/hubs/stream`, {
        transport: signalR.HttpTransportType.WebSockets,
        accessTokenFactory: () => http.token(),
      })
      .build();

    conn.on("awarenessUpdate", (name: string, peers: AwarenessPeer[]) => {
      if (name !== this.sourceName) return;
      this._peers = peers;
      for (const fn of this.listeners) fn(peers);
    });
    conn.onclose(() => this.stopHeartbeat());

    try {
      await conn.start();
    } catch (err) {
      throw new StreamForgeError(`SignalR connection for awareness on '${this.sourceName}' failed to start: ${String(err)}`);
    }

    let snapshot: { ttlSeconds: number; maxEntries: number; peers: AwarenessPeer[] };
    try {
      snapshot = await conn.invoke("SubscribeAwareness", this.sourceName, this.clientId, label);
    } catch (err) {
      await conn.stop();
      throw new StreamForgeError(`SignalR SubscribeAwareness('${this.sourceName}') refused: ${String(err)}`);
    }

    this.connection = conn;
    this._peers = snapshot.peers;

    // Heartbeat at roughly a third of the server's own TTL (floor 5s) -- a couple of missed beats
    // (a slow tick, a backgrounded tab throttling timers) should not read as "gone" to every other
    // viewer the instant one is missed. The server enforces the real deadline; this is only about
    // giving it margin.
    const intervalMs = Math.max(5_000, Math.floor((snapshot.ttlSeconds * 1000) / 3));
    this.heartbeatTimer = setInterval(() => {
      this.connection?.invoke("Heartbeat", this.sourceName).catch(() => {
        // Best-effort: a skipped beat just means this tick did not refresh the entry -- the next
        // one, or the server-side TTL if the connection is actually gone, settles it either way.
      });
    }, intervalMs);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  /** Leaves the group, stops the heartbeat loop, and closes this session's own connection.
   * Idempotent -- safe to call more than once or after the connection already dropped on its
   * own. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.stopHeartbeat();

    const conn = this.connection;
    this.connection = null;
    if (conn) {
      try {
        await conn.invoke("UnsubscribeAwareness", this.sourceName);
      } catch {
        // best-effort: the connection may already be on its way down
      }
      await conn.stop();
    }
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

function randomClientId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `client-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}
