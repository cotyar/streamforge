/**
 * @streamsforge/server -- an embeddable JS/TS StreamsForge-compatible server: the dataset layer
 * (a registry of Z-set tables, fan-out of `tableDelta` batches) plus the REST + live routes
 * `@streamsforge/client` needs, over plain SSE (any runtime) and WebSocket (Bun).
 *
 * Everything is a Web-standard `(Request) => Response` so ONE object drops into Bun.serve,
 * Hono, or a Next.js route handler -- see README.md. The "executor" is your code: a source
 * handler that turns pushed rows into `table.upsert()` / `table.remove()` / `table.apply()`.
 *
 * Wire contract (mirrors clients/typescript/src/plain-transport.ts; same shape SignalR's
 * `tableDelta(name, [{row, weight}], seq)` carries):
 *   POST {prefix}/api/auth/login                    -> {token, username, displayName, role}
 *   GET  {prefix}/api/tables                        -> [{id, name, keyFields}]
 *   GET  {prefix}/api/tables/{name}/rows?limit=     -> {rows:[{row,weight}], totalRows, seq}
 *   GET  {prefix}/api/tables/{name}/live            -> SSE: `subscribed`, then `tableDelta` events
 *   GET  {prefix}/api/live?access_token=            -> WebSocket (Bun): subscribe/unsubscribe JSON
 *   POST {prefix}/api/sources/{name}/events         -> 202 {accepted, dropped, invalid, ...}
 */

import { ZSet, canonicalKey, groupKeyOf, type Delta, type Row } from "@streamsforge/client";

export type { Delta, Row };
export type WireDelta = { row: Row; weight: number };
type Push = (deltas: WireDelta[], seq: number) => void;

export interface TableOptions {
  /** Row-identity key: `["id"]` = LATEST BY id (upsert supersedes), `[]` = single global row,
   * `null`/omitted = whole-row identity (plain multiset). Reported to clients via /api/tables. */
  keyFields?: string[] | null;
}

export class Table {
  readonly id: string;
  private readonly zset: ZSet;
  private readonly groups = new Map<string, Row>(); // keyed tables only: group -> current row
  private readonly subscribers = new Set<Push>();
  seq = 0;

  constructor(
    readonly name: string,
    readonly keyFields: string[] | null,
  ) {
    this.id = name; // ponytail: id == name; the client resolves either, and a rename is not a feature here
    this.zset = new ZSet(keyFields);
  }

  rows(): Row[] {
    return this.zset.rows();
  }

  /** Raw Z-set deltas -- apply to local state and broadcast as one batch. */
  apply(deltas: readonly Delta[]): void {
    if (deltas.length === 0) return;
    this.zset.apply(deltas);
    if (this.keyFields !== null) {
      for (const [row, w] of deltas) {
        const gk = groupKeyOf(row, this.keyFields)!;
        if (w > 0) this.groups.set(gk, row);
        else if (this.groups.get(gk) !== undefined && canonicalKey(this.groups.get(gk)!) === canonicalKey(row)) this.groups.delete(gk);
      }
    }
    const seq = ++this.seq;
    const wire = deltas.map(([row, weight]) => ({ row, weight }));
    for (const fn of this.subscribers) fn(wire, seq);
  }

  /** Keyed: retract the group's previous row (if different) and assert this one. Unkeyed: assert. */
  upsert(row: Row): void {
    if (this.keyFields === null) return this.apply([[row, 1]]);
    const old = this.groups.get(groupKeyOf(row, this.keyFields)!);
    if (old && canonicalKey(old) === canonicalKey(row)) return; // identical: no-op, weight stays 1
    this.apply(old ? [[old, -1], [row, 1]] : [[row, 1]]);
  }

  /** Keyed: retract whatever row the group currently holds (only key columns need to be present).
   * Unkeyed: retract exactly this row. */
  remove(row: Row): void {
    if (this.keyFields === null) return this.apply([[row, -1]]);
    const old = this.groups.get(groupKeyOf(row, this.keyFields)!);
    if (old) this.apply([[old, -1]]);
  }

  snapshot(limit: number) {
    const entries = this.zset.entries();
    return { rows: entries.slice(0, limit).map((e) => ({ row: e.row, weight: e.weight })), totalRows: entries.length, seq: this.seq };
  }

  subscribe(fn: Push): () => void {
    this.subscribers.add(fn);
    return () => this.subscribers.delete(fn);
  }
}

export interface AuthUser {
  token: string;
  displayName?: string;
  role?: "Admin" | "Editor" | "Viewer";
}

export interface ServerOptions {
  /** Path prefix before `/api/...`, e.g. "/sf" when mounted under a sub-path. Default "". */
  prefix?: string;
  /** Omitted = open: login hands out a dummy token and nothing is checked. Provided = every
   * route requires a Bearer token (or `?access_token=` for WebSocket) that `verify` accepts. */
  auth?: {
    login(username: string, password: string): Promise<AuthUser | null> | AuthUser | null;
    verify(token: string): Promise<boolean> | boolean;
  };
  /** SSE keepalive comment interval (ms); proxies drop idle streams. Default 15000. */
  keepaliveMs?: number;
}

export type SourceHandler = (rows: Row[], meta: { source: string; idempotencyKey?: string; partial: boolean }) => void | Promise<void>;

interface WsData {
  subs: Map<string, () => void>;
}
/** Structural subset of Bun's ServerWebSocket -- avoids a hard type dependency on bun-types. */
interface WsLike {
  data: WsData;
  send(msg: string): unknown;
}

const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

export class StreamsForgeServer {
  private readonly tables = new Map<string, Table>();
  private readonly sources = new Map<string, SourceHandler>();
  private readonly prefix: string;
  private readonly auth: ServerOptions["auth"];
  private readonly keepaliveMs: number;

  constructor(opts: ServerOptions = {}) {
    this.prefix = (opts.prefix ?? "").replace(/\/+$/, "");
    this.auth = opts.auth;
    this.keepaliveMs = opts.keepaliveMs ?? 15_000;
    // Bound so `Bun.serve({ fetch: sf.fetch })` / `export const GET = sf.fetch` work unwrapped.
    this.fetch = this.fetch.bind(this);
  }

  /** Create-or-get. keyFields is fixed on first creation. */
  table(name: string, opts: TableOptions = {}): Table {
    let t = this.tables.get(name);
    if (!t) {
      if (name.includes(".")) throw new Error(`table name '${name}' may not contain '.'`);
      t = new Table(name, opts.keyFields ?? null);
      this.tables.set(name, t);
    }
    return t;
  }

  /** Register the executor for `POST /api/sources/{name}/events` -- your code decides which
   * tables a pushed row lands in. */
  source(name: string, handler: SourceHandler): this {
    this.sources.set(name, handler);
    return this;
  }

  /** True when this request is one of ours -- lets a host router hand only `/api/*` over. */
  matches(req: Request): boolean {
    return new URL(req.url).pathname.startsWith(`${this.prefix}/api/`);
  }

  /**
   * The one entry point. `server` is Bun's Server (for `server.upgrade`); pass it to get
   * WebSocket, omit it (Hono on Node, Next.js) and only SSE is live. Returns `undefined` after a
   * successful upgrade, as Bun requires.
   */
  async fetch(req: Request, server?: { upgrade(req: Request, opts: { data: WsData }): boolean }): Promise<Response | undefined> {
    const url = new URL(req.url);
    const path = url.pathname.startsWith(this.prefix) ? url.pathname.slice(this.prefix.length) : null;
    if (path === null) return json({ error: "not found" }, 404);

    if (req.method === "POST" && path === "/api/auth/login") {
      const body = (await req.json().catch(() => ({}))) as { username?: string; password?: string };
      const user = this.auth
        ? await this.auth.login(body.username ?? "", body.password ?? "")
        : { token: "anonymous", role: "Admin" as const };
      if (!user) return json({ error: "invalid credentials" }, 401);
      return json({ token: user.token, username: body.username ?? "anonymous", displayName: user.displayName ?? body.username ?? "anonymous", role: user.role ?? "Editor" });
    }

    if (this.auth) {
      const token = req.headers.get("authorization")?.replace(/^Bearer\s+/i, "") ?? url.searchParams.get("access_token") ?? "";
      if (!token || !(await this.auth.verify(token))) return json({ error: "unauthorized" }, 401);
    }

    if (req.method === "GET" && path === "/api/tables") {
      return json([...this.tables.values()].map((t) => ({ id: t.id, name: t.name, keyFields: t.keyFields, running: true, status: "Running" })));
    }

    if (req.method === "GET" && path === "/api/live") {
      if (!server?.upgrade) return json({ error: "WebSocket needs Bun.serve; use SSE (/api/tables/{name}/live) here" }, 426);
      return server.upgrade(req, { data: { subs: new Map() } }) ? undefined : json({ error: "upgrade failed" }, 400);
    }

    let m = /^\/api\/tables\/([^/]+)\/(rows|live)$/.exec(path);
    if (m && req.method === "GET") {
      const t = this.tables.get(decodeURIComponent(m[1]!));
      if (!t) return json({ error: `no such table '${m[1]}'` }, 404);
      if (m[2] === "rows") return json(t.snapshot(Number(url.searchParams.get("limit") ?? 500)));
      return this.sse(t, req.signal);
    }

    m = /^\/api\/sources\/([^/]+)\/events$/.exec(path);
    if (m && req.method === "POST") {
      const name = decodeURIComponent(m[1]!);
      const handler = this.sources.get(name);
      if (!handler) return json({ error: `no such source '${name}'`, retryAfterMs: 0, rowErrors: [] }, 404);
      const body = (await req.json().catch(() => null)) as { events?: unknown; partial?: boolean; idempotencyKey?: string } | null;
      if (!body || !Array.isArray(body.events)) return json({ error: "body must be {events: Row[]}", retryAfterMs: 0, rowErrors: [] }, 400);
      try {
        await handler(body.events as Row[], { source: name, idempotencyKey: body.idempotencyKey, partial: body.partial ?? false });
      } catch (err) {
        return json({ error: String(err), retryAfterMs: 0, rowErrors: [] }, 422);
      }
      return json({ accepted: body.events.length, dropped: 0, invalid: 0, depthRows: 0, capacityRows: 0 }, 202);
    }

    return json({ error: "not found" }, 404);
  }

  private sse(t: Table, signal: AbortSignal): Response {
    const enc = new TextEncoder();
    let unsubscribe = () => {};
    let timer: ReturnType<typeof setInterval> | undefined;
    const stream = new ReadableStream<Uint8Array>({
      start: (ctl) => {
        const send = (event: string, data: unknown) => {
          try {
            ctl.enqueue(enc.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
          } catch {
            stop();
          }
        };
        const stop = () => {
          unsubscribe();
          clearInterval(timer);
          try {
            ctl.close();
          } catch {}
        };
        unsubscribe = t.subscribe((deltas, seq) => send("tableDelta", { name: t.name, deltas, seq }));
        timer = setInterval(() => {
          try {
            ctl.enqueue(enc.encode(": ping\n\n"));
          } catch {
            stop();
          }
        }, this.keepaliveMs);
        signal.addEventListener("abort", stop, { once: true });
        send("subscribed", { name: t.name });
      },
      cancel: () => {
        unsubscribe();
        clearInterval(timer);
      },
    });
    return new Response(stream, { headers: { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" } });
  }

  /** Bun.serve's `websocket` handler: `Bun.serve({ fetch: sf.fetch, websocket: sf.websocket })`. */
  readonly websocket = {
    message: (ws: WsLike, raw: string | Buffer) => {
      let msg: { type?: string; table?: string };
      try {
        msg = JSON.parse(String(raw));
      } catch {
        return ws.send(JSON.stringify({ type: "error", message: "invalid JSON" }));
      }
      const name = msg.table ?? "";
      if (msg.type === "subscribe") {
        const t = this.tables.get(name);
        if (!t) return ws.send(JSON.stringify({ type: "error", table: name, message: `no such table '${name}'` }));
        ws.data.subs.get(name)?.();
        ws.data.subs.set(name, t.subscribe((deltas, seq) => ws.send(JSON.stringify({ type: "tableDelta", name, deltas, seq }))));
        ws.send(JSON.stringify({ type: "subscribed", table: name }));
      } else if (msg.type === "unsubscribe") {
        ws.data.subs.get(name)?.();
        ws.data.subs.delete(name);
      }
    },
    close: (ws: WsLike) => {
      for (const off of ws.data.subs.values()) off();
      ws.data.subs.clear();
    },
  };
}

export function createStreamsForge(opts?: ServerOptions): StreamsForgeServer {
  return new StreamsForgeServer(opts);
}
