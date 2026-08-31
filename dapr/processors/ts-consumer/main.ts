// StreamsForge — polyglot pub/sub reach (plan 005, wave W8-B).
//
// A standalone bun process — no Dapr SDK, no npm deps, just Bun.serve() — that subscribes to two of
// the platform's frozen pub/sub topics (dapr/POLYGLOT.md) via its own Dapr sidecar:
//
//   sf-table-delta   -> TableDeltaEnvelope   { table, seq, deltas: [{ row, weight }] }
//   sf-pipeline-out  -> PipelineResultsEnvelope { pipelineId, results: [{ pipelineId, seq, timestampMs, row }] }
//
// The sidecar discovers subscriptions via GET /dapr/subscribe (declarative HTTP subscription
// handshake) and then POSTs each message, CloudEvents-wrapped, to the declared route. We unwrap
// `.data` (the sidecar parses the JSON body into a real object when `datacontenttype` is
// `application/json`, which is Dapr's default for a JSON publish) and fall back to `.data_base64`
// for a raw/binary publish, matching the same tolerance the .NET host's endpoints have for
// malformed/unexpected payloads (dapr/POLYGLOT.md's "Malformed-payload handling" section) — we log
// and still return 200 rather than let the sidecar redeliver forever.

const APP_PORT = Number(process.env.APP_PORT ?? process.env.PORT ?? 8499);
const DAPR_HTTP_PORT = Number(process.env.DAPR_HTTP_PORT ?? 3999);
const DAPR_GRPC_PORT = Number(process.env.DAPR_GRPC_PORT ?? 4999);
const PUBSUB_NAME = process.env.PUBSUB_NAME ?? "pubsub";

// --- tiny ANSI color helpers (zero deps) ---------------------------------------------------------
const color = {
  reset: "\x1b[0m",
  dim: (s: string) => `\x1b[2m${s}\x1b[0m`,
  bold: (s: string) => `\x1b[1m${s}\x1b[0m`,
  cyan: (s: string) => `\x1b[36m${s}\x1b[0m`,
  magenta: (s: string) => `\x1b[35m${s}\x1b[0m`,
  green: (s: string) => `\x1b[32m${s}\x1b[0m`,
  red: (s: string) => `\x1b[31m${s}\x1b[0m`,
  yellow: (s: string) => `\x1b[33m${s}\x1b[0m`,
  gray: (s: string) => `\x1b[90m${s}\x1b[0m`,
};

// --- running counters, printed every 10s ---------------------------------------------------------
const counters = {
  tableDeltaMessages: 0,
  tableDeltaRows: 0,
  pipelineMessages: 0,
  pipelineResults: 0,
};

function printCounters() {
  const ts = new Date().toISOString();
  console.log(
    color.gray(
      `[${ts}] counters: table-delta msgs=${counters.tableDeltaMessages} rows=${counters.tableDeltaRows} | ` +
        `pipeline-out msgs=${counters.pipelineMessages} results=${counters.pipelineResults}`,
    ),
  );
}
setInterval(printCounters, 10_000);

// --- shape of the two envelopes we care about (mirrors shared/StreamsForge.Contracts/Streaming/Envelopes.cs) ---
interface TableDeltaDto {
  row: Record<string, unknown>;
  weight: number;
}
interface TableDeltaEnvelope {
  table: string;
  seq: number;
  deltas: TableDeltaDto[];
}
interface ResultEnvelope {
  pipelineId: string;
  seq: number;
  timestampMs: number;
  row: Record<string, unknown>;
}
interface PipelineResultsEnvelope {
  pipelineId: string;
  results: ResultEnvelope[];
}

/** Extracts the actual envelope payload from a Dapr CloudEvents-wrapped POST body. Falls back to
 * treating the whole body as the payload if it doesn't look CloudEvents-shaped, and to base64
 * decoding `data_base64` for a raw/binary publish. Returns null (never throws) on anything
 * unparseable — the caller logs + still 200s, matching the host's own poison-message handling. */
function extractData(body: unknown): unknown {
  if (body === null || typeof body !== "object") return body;
  const obj = body as Record<string, unknown>;
  if ("data" in obj) return obj.data;
  if (typeof obj.data_base64 === "string") {
    try {
      return JSON.parse(Buffer.from(obj.data_base64, "base64").toString("utf8"));
    } catch {
      return null;
    }
  }
  return obj;
}

function weightGlyph(weight: number): string {
  return weight >= 0 ? color.green(`+${weight}`) : color.red(`${weight}`);
}

function handleTableDelta(data: unknown): void {
  const env = data as Partial<TableDeltaEnvelope> | null;
  if (!env || typeof env.table !== "string" || !Array.isArray(env.deltas)) {
    console.warn(color.yellow(`[sf-table-delta] malformed payload, dropped: ${JSON.stringify(data)}`));
    return;
  }
  counters.tableDeltaMessages++;
  counters.tableDeltaRows += env.deltas.length;
  const rows = env.deltas
    .map((d) => `${weightGlyph(d?.weight ?? 0)} ${JSON.stringify(d?.row ?? {})}`)
    .join("  ");
  console.log(`${color.cyan("[sf-table-delta]")} ${env.table}#${env.seq ?? "?"}  ${rows}`);
}

function handlePipelineOut(data: unknown): void {
  const env = data as Partial<PipelineResultsEnvelope> | null;
  if (!env || typeof env.pipelineId !== "string" || !Array.isArray(env.results)) {
    console.warn(color.yellow(`[sf-pipeline-out] malformed payload, dropped: ${JSON.stringify(data)}`));
    return;
  }
  counters.pipelineMessages++;
  counters.pipelineResults += env.results.length;
  const summary =
    env.results.length === 0
      ? "(no results)"
      : env.results
          .map((r) => `seq=${r?.seq} ts=${r?.timestampMs} row=${JSON.stringify(r?.row ?? {})}`)
          .join("  ");
  console.log(`${color.magenta("[sf-pipeline-out]")} ${env.pipelineId}  ${env.results.length} result(s): ${summary}`);
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

const subscriptions = [
  { pubsubname: PUBSUB_NAME, topic: "sf-table-delta", route: "/sf-table-delta" },
  { pubsubname: PUBSUB_NAME, topic: "sf-pipeline-out", route: "/sf-pipeline-out" },
];

Bun.serve({
  port: APP_PORT,
  async fetch(req) {
    const url = new URL(req.url);

    if (req.method === "GET" && url.pathname === "/dapr/subscribe") {
      return jsonResponse(subscriptions);
    }

    if (req.method === "GET" && url.pathname === "/healthz") {
      return jsonResponse({ status: "ok" });
    }

    if (req.method === "POST" && (url.pathname === "/sf-table-delta" || url.pathname === "/sf-pipeline-out")) {
      let parsedBody: unknown = null;
      try {
        parsedBody = await req.json();
      } catch {
        console.warn(color.yellow(`${url.pathname}: request body was not valid JSON, dropped`));
        // Always 200 — a non-2xx here is exactly the signal that triggers Dapr's at-least-once
        // redelivery, and a permanently-malformed message would otherwise retry forever.
        return jsonResponse({ status: "SUCCESS" });
      }

      const data = extractData(parsedBody);
      if (url.pathname === "/sf-table-delta") {
        handleTableDelta(data);
      } else {
        handlePipelineOut(data);
      }
      return jsonResponse({ status: "SUCCESS" });
    }

    return new Response("not found", { status: 404 });
  },
});

console.log(
  color.bold(
    `sf-ts-consumer listening on :${APP_PORT} (sidecar http :${DAPR_HTTP_PORT}, grpc :${DAPR_GRPC_PORT}, pubsub "${PUBSUB_NAME}")`,
  ),
);
console.log(color.gray(`subscriptions: ${subscriptions.map((s) => s.topic).join(", ")}`));
