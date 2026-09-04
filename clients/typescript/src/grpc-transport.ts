/**
 * Tier 1 gRPC transport (design doc §3): StreamService.SubscribeTable for deltas,
 * TableService.Rows/Search/Validate/List for the catalog and snapshot, and the bidi
 * IngestService.Ingest for pushes. Plaintext h2c by default, prior knowledge -- no TLS
 * negotiation, matching how the engine is actually run from source (§3.2's --urls trap). A
 * `https://` target switches to ALPN-negotiated h2 over TLS (the server side of this is
 * `--Tls:Enabled true` on the host, see `tools/tls/dev-cert.sh`) -- see `parseGrpcTarget` below.
 *
 * Node-only, and deliberately never imported eagerly: index.ts reaches this module only via a
 * dynamic `import()` gated on a Node-runtime check, so a browser bundle that imports
 * `@streamsforge/client` never pulls in `@grpc/grpc-js` (a browser cannot speak h2c gRPC at all --
 * see the package README).
 *
 * Proto loaded dynamically via @grpc/proto-loader (no generated stubs to keep in sync, unlike the
 * Python client's checked-in `_pb/streamsforge_pb2*.py`) -- `keepCase: false` gives camelCase
 * field names matching the REST DTOs' own convention (types.ts), and `longs: 'String'` avoids
 * silently truncating an int64 seq/weight to a JS double.
 *
 * Row payloads travel as google.protobuf.Struct; proto-loader represents one as its own raw wire
 * shape (`{fields: {name: {kind: 'stringValue', stringValue: ...}}}`), not a plain object --
 * structToRow()/rowToStruct() below are the "typing" story (design doc §2: rows stay dicts, a
 * DataFrame/consumer re-types them anyway). A number that doesn't round-trip through Struct's
 * IEEE-754 double (an int64 beyond 2**53) is a documented, not fixed, edge -- nothing in the
 * reference demo crosses it.
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { StreamsForgeError } from "./errors.js";
import type { IngestAckDto } from "./ingest.js";
import type { Transport } from "./transport.js";
import type { Delta, Row } from "./zset.js";

const PROTO_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), "proto", "streamsforge.proto");

// ---- Struct <-> Row conversion -------------------------------------------------------------

interface ProtoValue {
  kind?: string;
  nullValue?: unknown;
  numberValue?: number;
  stringValue?: string;
  boolValue?: boolean;
  structValue?: ProtoStruct;
  listValue?: { values?: ProtoValue[] };
}
interface ProtoStruct {
  fields?: Record<string, ProtoValue>;
}

function valueToJs(v: ProtoValue): unknown {
  switch (v.kind) {
    case "nullValue":
      return null;
    case "numberValue":
      return v.numberValue;
    case "stringValue":
      return v.stringValue;
    case "boolValue":
      return v.boolValue;
    case "structValue":
      return structToRow(v.structValue);
    case "listValue":
      return (v.listValue?.values ?? []).map(valueToJs);
    default:
      return null;
  }
}

function structToRow(struct: ProtoStruct | undefined): Row {
  const row: Row = {};
  for (const [k, v] of Object.entries(struct?.fields ?? {})) row[k] = valueToJs(v);
  return row;
}

function jsToValue(x: unknown): ProtoValue {
  if (x === null || x === undefined) return { kind: "nullValue", nullValue: "NULL_VALUE" };
  if (typeof x === "number") return { kind: "numberValue", numberValue: x };
  if (typeof x === "string") return { kind: "stringValue", stringValue: x };
  if (typeof x === "boolean") return { kind: "boolValue", boolValue: x };
  if (Array.isArray(x)) return { kind: "listValue", listValue: { values: x.map(jsToValue) } };
  return { kind: "structValue", structValue: rowToStruct(x as Row) };
}

function rowToStruct(row: Row): ProtoStruct {
  const fields: Record<string, ProtoValue> = {};
  for (const [k, v] of Object.entries(row)) fields[k] = jsToValue(v);
  return { fields };
}

// ---- proto loading (cached: loadSync parses + compiles descriptors, not free) ---------------

interface StreamsForgeV1Package {
  TableService: grpc.ServiceClientConstructor;
  StreamService: grpc.ServiceClientConstructor;
  IngestService: grpc.ServiceClientConstructor;
}

let cachedPackage: StreamsForgeV1Package | null = null;

function loadV1(): StreamsForgeV1Package {
  if (cachedPackage) return cachedPackage;
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const pkg = grpc.loadPackageDefinition(packageDef) as unknown as {
    streamsforge: { v1: StreamsForgeV1Package };
  };
  cachedPackage = pkg.streamsforge.v1;
  return cachedPackage;
}

// ---- wire DTOs (post proto-loader, camelCase, longs-as-string) ------------------------------

interface TableRowWire {
  row: ProtoStruct;
  weight: string;
}
interface TableDeltaBatchWire {
  tableName: string;
  seq: string;
  deltas: TableRowWire[];
}
interface TableDefinitionWire {
  id: string;
  name: string;
  [key: string]: unknown;
}
interface ValidateTableResponseWire {
  ok: boolean;
  diagnostics: Array<{ message: string; line: number; column: number; severity: string }>;
  planSummary?: string;
  streamInputs: string[];
  tableInputs: string[];
  outputSchema: Array<{ name: string; type: string }>;
}

function promisify<Req, Res>(
  fn: (req: Req, md: grpc.Metadata, cb: (err: grpc.ServiceError | null, res: Res) => void) => void,
  req: Req,
  md: grpc.Metadata,
): Promise<Res> {
  return new Promise((resolve, reject) => {
    fn(req, md, (err, res) => (err ? reject(err) : resolve(res)));
  });
}

// ---- target parsing (scheme decides plaintext vs TLS) -----------------------------------

export interface ParsedGrpcTarget {
  /** Bare `host:port`, scheme stripped -- what `@grpc/grpc-js`'s client constructors expect. */
  target: string;
  /** Whether to dial with `createSsl` (true) or `createInsecure` (false). */
  tls: boolean;
}

/**
 * `grpc=` (ConnectOptions.grpc / STREAMSFORGE_GRPC) may be a bare `host:port` (plaintext,
 * unchanged since before TLS support), an explicit `http://host:port` (also plaintext), or
 * `https://host:port` (TLS). Anything else with no recognized scheme is treated as bare
 * `host:port` -- the historical default, so an existing caller's target string keeps working
 * unmodified.
 */
export function parseGrpcTarget(raw: string): ParsedGrpcTarget {
  if (raw.startsWith("https://")) return { target: raw.slice("https://".length), tls: true };
  if (raw.startsWith("http://")) return { target: raw.slice("http://".length), tls: false };
  return { target: raw, tls: false };
}

export interface GrpcTransportOptions {
  /** PEM text (not a path -- callers resolve a file path before reaching here, see index.ts's
   * `resolveCa`). Passed to `createSsl`'s `rootCerts`; omitted means "trust the system roots",
   * which is what a certificate from a real CA needs and a self-signed dev cert does not have. */
  ca?: string;
  /** false = accept any certificate (self-signed/invalid), the gRPC equivalent of http.ts's
   * `verify: false` -- dev-only, see this option's doc comment on ConnectOptions in index.ts. */
  verify?: boolean;
}

// ---- Transport ---------------------------------------------------------------------------

export class GrpcTransport implements Transport {
  readonly name = "grpc";
  private readonly tables: grpc.Client & Record<string, unknown>;
  private readonly stream: grpc.Client & Record<string, unknown>;
  private readonly ingestClient: grpc.Client & Record<string, unknown>;

  constructor(
    target: string,
    private readonly getToken: () => Promise<string>,
    opts: GrpcTransportOptions = {},
  ) {
    const v1 = loadV1();
    const { target: bareTarget, tls } = parseGrpcTarget(target);
    let creds: grpc.ChannelCredentials;
    if (tls) {
      const rootCerts = opts.ca ? Buffer.from(opts.ca, "utf-8") : null;
      // grpc-js's VerifyOptions (confirmed against the installed @grpc/grpc-js@1.14.4) supports
      // both rejectUnauthorized and checkServerIdentity -- setting both is belt-and-braces (some
      // older grpc-js releases only honored the latter for the "wrong hostname" half of
      // verification, not "untrusted issuer").
      const verifyOptions = opts.verify === false ? { rejectUnauthorized: false, checkServerIdentity: () => undefined } : undefined;
      creds = grpc.credentials.createSsl(rootCerts, null, null, verifyOptions);
    } else {
      creds = grpc.credentials.createInsecure();
    }
    this.tables = new v1.TableService(bareTarget, creds) as unknown as grpc.Client & Record<string, unknown>;
    this.stream = new v1.StreamService(bareTarget, creds) as unknown as grpc.Client & Record<string, unknown>;
    this.ingestClient = new v1.IngestService(bareTarget, creds) as unknown as grpc.Client & Record<string, unknown>;
  }

  private async metadata(): Promise<grpc.Metadata> {
    const md = new grpc.Metadata();
    md.add("authorization", `Bearer ${await this.getToken()}`);
    return md;
  }

  /** Proves the channel AND the JWT actually work -- called by connect()'s gRPC attempt and its
   * "auto" fallback path (index.ts), mirroring the Python client's own `list_tables(timeout=3.0)`
   * probe. */
  async listTables(): Promise<TableDefinitionWire[]> {
    const md = await this.metadata();
    const call = (this.tables as unknown as {
      List: (req: unknown, md: grpc.Metadata, cb: (err: grpc.ServiceError | null, res: { tables: TableDefinitionWire[] }) => void) => void;
    }).List;
    const resp = await promisify(call.bind(this.tables), {}, md);
    return resp.tables;
  }

  private async resolveTableId(name: string): Promise<string> {
    for (const t of await this.listTables()) {
      if (t.name === name) return t.id;
    }
    throw new StreamsForgeError(`no such table '${name}'`);
  }

  async validate(sql: string): Promise<ValidateTableResponseWire> {
    const md = await this.metadata();
    const call = (this.tables as unknown as {
      Validate: (req: unknown, md: grpc.Metadata, cb: (err: grpc.ServiceError | null, res: ValidateTableResponseWire) => void) => void;
    }).Validate;
    return promisify(call.bind(this.tables), { sql }, md);
  }

  async search(tableName: string, query: string, limit = 50): Promise<Row[]> {
    const id = await this.resolveTableId(tableName);
    const md = await this.metadata();
    const call = (this.tables as unknown as {
      Search: (req: unknown, md: grpc.Metadata, cb: (err: grpc.ServiceError | null, res: { rows: TableRowWire[] }) => void) => void;
    }).Search;
    const resp = await promisify(call.bind(this.tables), { id, query, limit }, md);
    return resp.rows.filter((r) => Number(r.weight) > 0).map((r) => structToRow(r.row));
  }

  // ---- Transport interface ----

  async snapshot(tableName: string, limit = 500): Promise<readonly [Delta[], number]> {
    const id = await this.resolveTableId(tableName);
    const md = await this.metadata();
    const call = (this.tables as unknown as {
      Rows: (req: unknown, md: grpc.Metadata, cb: (err: grpc.ServiceError | null, res: { rows: TableRowWire[]; seq: string }) => void) => void;
    }).Rows;
    const resp = await promisify(call.bind(this.tables), { id, limit, offset: 0 }, md);
    const deltas: Delta[] = resp.rows.map((r) => [structToRow(r.row), Number(r.weight)] as const);
    return [deltas, Number(resp.seq)] as const;
  }

  /**
   * Establishes the subscription -- awaits auth metadata, then CREATES the streaming call -- and
   * only THEN resolves, per Transport.subscribe()'s contract (see transport.ts's doc comment).
   * Creating the call is the real handshake here: @grpc/grpc-js puts the request on the wire as
   * soon as the method is invoked, independent of whether/when anything reads the response
   * stream (confirmed empirically against this engine -- a delta pushed after call-creation but
   * before the first read is still delivered once reading starts), so by the time this function
   * returns, the server has already registered the subscription. This function must NOT be an
   * `async function*` itself: a generator's body -- including this handshake -- would not run
   * until the caller's first `.next()`, silently deferring the handshake past whatever the
   * caller does next (in live-table.ts's case, the snapshot read) and reopening the exact race
   * this contract exists to close.
   */
  async subscribe(tableName: string, signal: AbortSignal): Promise<AsyncIterable<readonly [Delta[], number]>> {
    const md = await this.metadata();
    const subscribeFn = (this.stream as unknown as {
      SubscribeTable: (req: unknown, md: grpc.Metadata) => grpc.ClientReadableStream<TableDeltaBatchWire>;
    }).SubscribeTable;
    const call = subscribeFn.call(this.stream, { name: tableName }, md);

    // grpc-js's Call.cancel() is documented safe from any context (unlike a bare async
    // generator's own .close(), which is a footgun only in threaded runtimes -- moot in JS, but
    // AbortSignal is still the cleanest way to tie this stream's lifetime to live-table.ts's).
    const onAbort = () => call.cancel();
    if (signal.aborted) {
      // Establishment raced a close() that landed first -- cancel immediately rather than
      // leaking a half-open call nothing will ever read from.
      call.cancel();
    } else {
      signal.addEventListener("abort", onAbort);
    }

    const iterate = async function* (): AsyncGenerator<readonly [Delta[], number]> {
      try {
        for await (const batch of call as unknown as AsyncIterable<TableDeltaBatchWire>) {
          const deltas: Delta[] = batch.deltas.map((d) => [structToRow(d.row), Number(d.weight)] as const);
          yield [deltas, Number(batch.seq)] as const;
        }
      } catch (err) {
        if (signal.aborted) return; // an intentional cancel() surfaces as a CANCELLED error -- not a real failure
        throw new StreamsForgeError(`gRPC SubscribeTable('${tableName}') stream ended: ${String(err)}`);
      } finally {
        signal.removeEventListener("abort", onAbort);
      }
    };
    return { [Symbol.asyncIterator]: () => iterate() };
  }

  // ---- ingest ----

  /** One request, one ack, over a fresh bidi stream -- real backpressure semantics from the bidi
   * RPC (the server does not ack until PushAsync returns) without holding a stream open across
   * calls. A long-lived streaming session (sustained backpressure across many pushes) is future
   * work, not needed for this client's push() surface -- mirrors _grpc.py's own ingest(). */
  async ingest(sourceName: string, rows: Row[], idempotencyKey: string | undefined, partial: boolean): Promise<IngestAckDto> {
    const md = await this.metadata();
    const ingestFn = (this.ingestClient as unknown as {
      Ingest: (md: grpc.Metadata) => grpc.ClientDuplexStream<unknown, IngestAckDto>;
    }).Ingest;
    const call = ingestFn.call(this.ingestClient, md);
    return new Promise((resolve, reject) => {
      let settled = false;
      call.on("data", (ack: IngestAckDto) => {
        settled = true;
        resolve(ack);
        call.end();
      });
      call.on("error", (err: Error) => {
        if (!settled) reject(new StreamsForgeError(`gRPC Ingest('${sourceName}') failed: ${err.message}`));
      });
      call.on("end", () => {
        if (!settled) reject(new StreamsForgeError(`gRPC Ingest('${sourceName}') stream closed with no ack`));
      });
      call.write({
        sourceName,
        rows: rows.map(rowToStruct),
        partial,
        idempotencyKey: idempotencyKey ?? "",
      });
    });
  }

  close(): void {
    this.tables.close();
    this.stream.close();
    this.ingestClient.close();
  }
}
