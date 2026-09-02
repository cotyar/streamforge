/**
 * @streamsforge/client -- TypeScript client for StreamsForge live tables.
 *
 *   import { connect } from '@streamsforge/client'
 *   const sf = await connect()                 // env, or explicit url/user/password
 *   const t = await sf.table('trigger_monitor') // subscribes, snapshots, replays
 *   t.rows                                       // Row[], frozen, current state
 *   await t.waitFor((rows) => rows.length > 0, 30_000)
 *   const stop = t.onChange((rows) => console.log(rows.length))
 *   for await (const rows of t) { ... }          // AsyncIterable of change notifications
 *   t.close()
 *
 * See docs/python-client-design.md (ac-co.ai-4 repo) for the full design and rationale this
 * client shares with the Python one; see this package's README for what's TypeScript-specific
 * (gRPC is Node-only, the transport is chosen once and logged, `await using` teardown).
 */

import { resolveConfig } from "./config.js";
import { AuthError, StreamsForgeError } from "./errors.js";
import type { GrpcIngestCapable } from "./ingest.js";
import * as ingestModule from "./ingest.js";
import { RestClient } from "./http.js";
import { LiveTable } from "./live-table.js";
import * as sqlModule from "./sql.js";
import * as tablesModule from "./tables.js";
import { SignalRTransport, probeSignalRMode, type SignalRMode } from "./signalr-transport.js";
import { PlainTransport, type PlainMode } from "./plain-transport.js";
import { AwarenessSession, type AwarenessOptions } from "./awareness.js";
import type { Transport } from "./transport.js";
import type { Row } from "./zset.js";
import type { ConfigImportReport, TableDefinitionDto, TableValidateResponse } from "./types.js";

export { StreamsForgeError, AuthError, NotReady, SqlError, IngestRejected, type SqlDiagnostic } from "./errors.js";
export { LiveTable, type ChangeListener } from "./live-table.js";
export { ZSet, canonicalKey, groupKeyOf, type Row, type Delta, type Entry } from "./zset.js";
export type { Transport } from "./transport.js";
export { PlainTransport, type PlainMode } from "./plain-transport.js";
export { ADHOC_PREFIX, adhocTableName } from "./sql.js";
export type { ConfigImportReport, TableDefinitionDto, TableValidateResponse } from "./types.js";
export { AwarenessSession, type AwarenessPeer, type AwarenessOptions, type AwarenessListener } from "./awareness.js";

/** `ws` / `sse` are the plain (non-SignalR) transports -- for `@streamsforge/server` or any
 * server speaking the bare tableDelta contract (plain-transport.ts). Never chosen by "auto". */
export type TransportName = "grpc" | "signalr" | "signalr:ws" | "signalr:sse" | "signalr:lp" | "ws" | "sse" | "auto";

const SIGNALR_MODES: Partial<Record<TransportName, SignalRMode>> = {
  signalr: "ws",
  "signalr:ws": "ws",
  "signalr:sse": "sse",
  "signalr:lp": "lp",
};
const VALID_TRANSPORTS = new Set<TransportName>(["grpc", "auto", "signalr", "signalr:ws", "signalr:sse", "signalr:lp", "ws", "sse"]);

export interface ConnectOptions {
  url?: string;
  grpc?: string;
  user?: string;
  password?: string;
  token?: string;
  ingestKey?: string;
  /** "grpc" | "signalr" | "signalr:ws" | "signalr:sse" | "signalr:lp" | "auto" (default). "auto"
   * tries gRPC (Node only), then SignalR ws -> sse -> lp, and always logs which one it got -- a
   * client that silently degrades and lets a caller believe it's on the fast path is worse than
   * one that fails loudly. */
  transport?: TransportName;
  /** false = accept self-signed/invalid TLS certs (local dev with a portless cert). Spelled out
   * rather than defaulted -- see http.ts. */
  verify?: boolean;
}

export interface TableOptions {
  key?: string[];
  timeoutMs?: number;
  /** Coalescing window (ms) for `onChange`/AsyncIterable emissions -- leading edge + trailing
   * coalesce, see LiveTable's own doc comment. Default 16 (one frame at 60Hz); 0 disables
   * coalescing and emits synchronously per applied batch. */
  flushMs?: number;
}

export interface SqlOptions {
  name: string;
  key?: string[];
  timeoutMs?: number;
  /** See TableOptions.flushMs -- same default (16ms), same meaning. */
  flushMs?: number;
}

export interface PushOptions {
  idempotencyKey?: string;
  partial?: boolean;
}

function isNodeRuntime(): boolean {
  return typeof process !== "undefined" && Boolean(process.versions?.node);
}

function defaultGrpcTarget(baseUrl: string): string {
  // Guesses the gRPC port from the REST base_url following Program.cs's own PORT/PORT+100
  // convention. Only a fallback -- pass grpc= (or STREAMSFORGE_GRPC) whenever the two ports don't
  // follow that relationship, e.g. an explicit --Http:Port/--Grpc:Port pair that isn't +100 apart.
  const u = new URL(baseUrl);
  const httpPort = u.port ? Number(u.port) : u.protocol === "https:" ? 443 : 80;
  return `${u.hostname}:${httpPort + 100}`;
}

export class Client {
  constructor(
    private readonly http: RestClient,
    private readonly grpcTransport: (GrpcIngestCapable & Transport) | null,
    private readonly liveTransport: Transport,
    private readonly ingestKey: string | undefined,
    /** Which transport connect() actually chose -- "grpc" | "signalr:ws" | "signalr:sse" | "signalr:lp". */
    readonly transportName: string,
  ) {}

  // ---- tables / live ----

  async table(name: string, opts: TableOptions = {}): Promise<LiveTable> {
    const keyFields = opts.key ?? (await tablesModule.resolveKeyFields(this.http, name));
    return LiveTable.connect(this.liveTransport, name, keyFields, opts.timeoutMs ?? 30_000, opts.flushMs);
  }

  snapshot(name: string, limit = 500): Promise<Row[]> {
    return tablesModule.snapshotRows(this.http, name, limit);
  }

  tables(): Promise<TableDefinitionDto[]> {
    return tablesModule.listTables(this.http);
  }

  search(name: string, query: string, limit = 50): Promise<Row[]> {
    return tablesModule.search(this.http, name, query, limit);
  }

  history(name: string, row: Row, limit?: number): Promise<unknown[]> {
    return tablesModule.history(this.http, name, row, limit);
  }

  // ---- ad-hoc SQL ----

  sql(sqlText: string, opts: SqlOptions): Promise<LiveTable> {
    return sqlModule.run(
      { http: this.http, table: (name, o) => this.table(name, o) },
      opts.name,
      sqlText,
      opts.key,
      opts.timeoutMs ?? 30_000,
      opts.flushMs,
    ) as Promise<LiveTable>;
  }

  validate(sqlText: string): Promise<TableValidateResponse> {
    return sqlModule.validate(this.http, sqlText);
  }

  adhoc(): Promise<TableDefinitionDto[]> {
    return sqlModule.listAdhoc(this.http);
  }

  dropAdhoc(name: string): Promise<boolean> {
    return sqlModule.dropAdhoc(this.http, name);
  }

  // ---- ingest ----

  push(source: string, rows: Row[], opts: PushOptions = {}): Promise<unknown> {
    return ingestModule.push({ http: this.http, grpc: this.grpcTransport, ingestKey: this.ingestKey }, source, rows, opts);
  }

  // ---- CRDT awareness (plan 020 wave G) ----

  /** Joins presence on a `crdt`-kind source that has opted into `CrdtSourceConfig.Awareness` --
   * see `AwarenessSession.join`'s own doc comment for the refusal cases. Always over its own
   * SignalR connection, independent of whichever transport this `Client` chose for table deltas
   * (that class's own doc comment explains why). Caller owns the returned session's lifetime --
   * call `.close()` (or use `await using`) when done with it; this `Client` does not track or
   * close sessions it handed out. */
  awareness(sourceName: string, opts: AwarenessOptions = {}): Promise<AwarenessSession> {
    return AwarenessSession.join(this.http, sourceName, opts);
  }

  async close(): Promise<void> {
    // liveTransport IS grpcTransport when gRPC was chosen -- close it once, not twice.
    if (this.grpcTransport && this.grpcTransport !== this.liveTransport) {
      await this.grpcTransport.close();
    }
    await this.liveTransport.close();
    await this.http.close();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

export async function connect(opts: ConnectOptions = {}): Promise<Client> {
  const transportOpt = opts.transport ?? "auto";
  if (!VALID_TRANSPORTS.has(transportOpt)) {
    throw new StreamsForgeError(
      `unknown transport '${transportOpt}' -- expected grpc, signalr, signalr:ws, signalr:sse, signalr:lp, ws, sse or auto`,
    );
  }

  const cfg = resolveConfig(opts);
  if (!cfg.baseUrl) {
    throw new StreamsForgeError("no base URL: pass url=, or set STREAMSFORGE_BASE_URL");
  }

  const http = new RestClient({ baseUrl: cfg.baseUrl, user: cfg.user, password: cfg.password, token: opts.token, verify: opts.verify });

  let grpcTransport: (GrpcIngestCapable & Transport) | null = null;
  if (transportOpt === "grpc" || transportOpt === "auto") {
    if (!isNodeRuntime()) {
      if (transportOpt === "grpc") {
        throw new StreamsForgeError(
          "gRPC transport requires Node -- a browser cannot speak h2c gRPC. Use transport: 'signalr' (or omit " +
            "transport and let 'auto' fall back for you) instead.",
        );
      }
      // "auto" in a browser: skip the gRPC attempt entirely rather than trying and failing --
      // there is no scenario where it could ever succeed there.
    } else {
      const target = cfg.grpc ?? defaultGrpcTarget(cfg.baseUrl);
      try {
        const { GrpcTransport } = await import("./grpc-transport.js");
        const candidate = new GrpcTransport(target, () => http.token());
        await candidate.listTables(); // proves the channel AND the JWT actually work
        grpcTransport = candidate;
      } catch (err) {
        if (transportOpt === "grpc") {
          throw new StreamsForgeError(
            `gRPC channel to ${target} refused. If the host was started with --urls, Program.cs's guard binds ` +
              "no gRPC port at all -- start it with --Http:Port/--Grpc:Port instead (design doc §3.2). " +
              `Underlying error: ${String(err)}`,
          );
        }
        console.warn(`streamsforge: gRPC unavailable (${String(err)}), falling back to SignalR`);
      }
    }
  }

  let liveTransport: Transport;
  let chosen: string;
  if (grpcTransport) {
    liveTransport = grpcTransport;
    chosen = "grpc";
  } else if (transportOpt === "ws" || transportOpt === "sse") {
    liveTransport = new PlainTransport(http, transportOpt as PlainMode);
    chosen = transportOpt;
  } else {
    const mode = SIGNALR_MODES[transportOpt] ?? (await probeSignalRMode(http));
    const hub = new SignalRTransport(http, mode);
    liveTransport = hub;
    chosen = hub.name;
  }

  console.info(`streamsforge: connected via ${chosen} transport (${cfg.baseUrl})`);

  return new Client(http, grpcTransport, liveTransport, cfg.ingestKey, chosen);
}
