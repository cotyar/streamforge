#!/usr/bin/env bun
// Plan 005 (Dapr sibling runtime) W9: end-to-end latency benchmark, one runtime per invocation.
//
// PRIMARY signal (--signal tableDelta, the default): wall-clock latency from a source event's
// *business* generation timestamp to the moment its resulting row is observed as a SignalR
// `tableDelta` ASSERT (weight === 1) on the seeded `order_states` table — identical on both runtimes
// (same SeedCatalog, same shared endpoint/hub contract, decision D-B). `order_states` is
// `SELECT ... stage_ts, ... FROM order_events LATEST BY (order_id)`
// (shared/StreamsForge.AppCore/SeedCatalog.cs): `stage_ts` is copied verbatim from the generator's own
// `_ts` at event-creation time (shared/StreamsForge.AppCore/Generators/MarketDataProfiles.cs:
// `evt["stage_ts"] = evt[EventRecord.TimestampField]`), i.e. it is the exact instant the synthetic
// event was generated — NOT a receipt/processing timestamp — so
// `latency_ms = client wall clock at delta arrival - row.stage_ts` is a genuine end-to-end number:
// generation -> (generator timer/tick) -> ingress -> LATEST BY table op -> delta publish ->
// (SignalR relay) -> this script's `tableDelta` handler.
//
// KNOWN LIVE BUG (found running this benchmark, NOT fixed per this wave's scope — report only):
// on the Orleans flavor, `tableDelta` (and, observed the same session, `pipelineResult`) events never
// reach a SignalR subscriber at all — 0 events over repeated 15-90s windows across three different
// seeded tables, on a freshly booted instance, while the SAME table's REST `/metrics` endpoint shows
// `deltasIn`/`deltasOut` growing continuously the whole time, and a real browser against the same
// instance shows the same "rows never update live" symptom. `sourceEvent` and `pipelineMetrics` (both
// single-object SignalR args, vs. `tableDelta`/`pipelineResult`'s List<T>-of-DTO args) work fine in
// the same session — see `orleans/docs/comparison.html`'s latency section and this wave's report for
// the full repro. Net effect: `--signal tableDelta` against the Orleans flavor legitimately collects
// ZERO samples right now; this is the live system's actual behavior, not a bug in this script.
//
// FALLBACK signal (--signal sourceEvent): measures the generator-tick -> SignalR-relay hop only
// (latency_ms = client wall clock at sourceEvent arrival - evt._ts) on a raw source (default
// `order_events`) rather than the processed table row. Real and comparable on both runtimes (this
// path is NOT affected by the tableDelta bug above), used as a supplementary, honestly-labeled
// substitute metric when the primary path is unavailable.
//
// Only ASSERT rows (weight === 1) count as samples in tableDelta mode (RETRACT rows, weight === -1,
// are the "old value leaving" half of every LATEST BY update and would double every measurement).
// The benchmark discards: (a) the first `--warmup-ms` (default 10s) of wall-clock time, to skip
// TCP/SignalR handshake and any burst of backlog catch-up right after connecting, and (b) any row
// whose timestamp predates the benchmark's own start time (a rebuild/backlog artifact — e.g. a delta
// whose *processing* is only happening now for an event generated well before this script started,
// which would report a huge bogus "latency" that has nothing to do with steady-state pipeline
// speed).
//
// Runtime-agnostic: both flavors serve `/hubs/stream` with the byte-identical group/event/arg shape
// (dapr/POLYGLOT.md's SignalR relay table) — this script has zero Orleans/Dapr-specific branching.
//
// Usage:
//   bun tools/bench/latency.ts --url http://localhost:6199 --token <jwt> --out results.json \
//     [--runtime orleans] [--signal tableDelta|sourceEvent] [--table order_states] \
//     [--source order_events] [--field stage_ts] [--warmup-ms 10000] \
//     [--max-duration-ms 90000] [--min-samples 500]
//
// bun only (never npm) — imports @microsoft/signalr straight out of web/node_modules by relative
// path so no separate install step is needed for this tool.

import * as signalR from "../../web/node_modules/@microsoft/signalr/dist/esm/index.js";

interface Args {
  url: string;
  token: string;
  out: string;
  runtime: string;
  signal: "tableDelta" | "sourceEvent";
  table: string;
  source: string;
  field: string;
  warmupMs: number;
  maxDurationMs: number;
  minSamples: number;
}

function parseArgs(argv: string[]): Args {
  const get = (flag: string, def?: string): string | undefined => {
    const i = argv.indexOf(flag);
    return i >= 0 && i + 1 < argv.length ? argv[i + 1] : def;
  };
  const url = get("--url");
  const token = get("--token");
  const out = get("--out");
  if (!url || !token || !out) {
    console.error("usage: latency.ts --url <http://host:port> --token <jwt> --out <path.json> " +
      "[--runtime name] [--signal tableDelta|sourceEvent] [--table order_states] " +
      "[--source order_events] [--field stage_ts] [--warmup-ms 10000] " +
      "[--max-duration-ms 90000] [--min-samples 500]");
    process.exit(2);
  }
  const signal = get("--signal", "tableDelta")!;
  if (signal !== "tableDelta" && signal !== "sourceEvent") {
    console.error(`--signal must be tableDelta or sourceEvent, got: ${signal}`);
    process.exit(2);
  }
  return {
    url,
    token,
    out,
    runtime: get("--runtime", "unknown")!,
    signal: signal as "tableDelta" | "sourceEvent",
    table: get("--table", "order_states")!,
    source: get("--source", "order_events")!,
    field: get("--field", "stage_ts")!,
    warmupMs: Number(get("--warmup-ms", "10000")),
    maxDurationMs: Number(get("--max-duration-ms", "90000")),
    minSamples: Number(get("--min-samples", "500")),
  };
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return NaN;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, idx)];
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  const conn = new signalR.HubConnectionBuilder()
    .withUrl(`${args.url}/hubs/stream?access_token=${encodeURIComponent(args.token)}`)
    .withAutomaticReconnect()
    .build();

  const samples: number[] = [];
  let discardedWarmup = 0;
  let discardedBacklog = 0;
  let discardedNoField = 0;
  let totalAssertsSeen = 0;

  let benchStart = 0; // set once the connection is up and the subscription is issued

  function recordSample(now: number, ts: unknown) {
    totalAssertsSeen++;
    if (typeof ts !== "number") {
      discardedNoField++;
      return;
    }
    if (benchStart === 0) return; // shouldn't happen (we subscribe after setting benchStart)
    if (ts < benchStart) {
      discardedBacklog++; // backlog/rebuild artifact — generated before this run started
      return;
    }
    if (now - benchStart < args.warmupMs) {
      discardedWarmup++;
      return;
    }
    samples.push(now - ts);
  }

  if (args.signal === "tableDelta") {
    conn.on("tableDelta", (tableName: string, deltas: Array<{ row: Record<string, unknown>; weight: number }>, _seq: number) => {
      if (tableName !== args.table) return;
      const now = Date.now();
      for (const d of deltas) {
        if (d.weight !== 1) continue; // ASSERT only — skip RETRACT halves of LATEST BY updates
        recordSample(now, d.row?.[args.field]);
      }
    });
  } else {
    conn.on("sourceEvent", (name: string, evt: Record<string, unknown>) => {
      if (name !== args.source) return;
      recordSample(Date.now(), evt?.[args.field]);
    });
  }

  await conn.start();
  benchStart = Date.now();
  if (args.signal === "tableDelta") {
    await conn.invoke("SubscribeTable", args.table);
  } else {
    await conn.invoke("SubscribeSource", args.source);
  }

  const deadline = benchStart + args.maxDurationMs;
  while (Date.now() < deadline) {
    const elapsedSincePostWarmup = Date.now() - (benchStart + args.warmupMs);
    if (elapsedSincePostWarmup > 0 && samples.length >= args.minSamples) break;
    await new Promise((r) => setTimeout(r, 200));
  }

  const totalElapsedMs = Date.now() - benchStart;
  await conn.stop();

  const sorted = [...samples].sort((a, b) => a - b);
  const result = {
    runtime: args.runtime,
    signal: args.signal,
    entity: args.signal === "tableDelta" ? args.table : args.source,
    field: args.field,
    url: args.url,
    startedAtIso: new Date(benchStart).toISOString(),
    warmupMs: args.warmupMs,
    maxDurationMs: args.maxDurationMs,
    minSamples: args.minSamples,
    totalElapsedMs,
    sampleCount: sorted.length,
    totalAssertsSeen,
    discardedWarmup,
    discardedBacklog,
    discardedNoField,
    p50Ms: percentile(sorted, 50),
    p90Ms: percentile(sorted, 90),
    p99Ms: percentile(sorted, 99),
    maxMs: sorted.length ? sorted[sorted.length - 1] : NaN,
    minMs: sorted.length ? sorted[0] : NaN,
    meanMs: sorted.length ? sorted.reduce((a, b) => a + b, 0) / sorted.length : NaN,
  };

  await Bun.write(args.out, JSON.stringify(result, null, 2) + "\n");
  console.log(JSON.stringify(result, null, 2));

  if (sorted.length < args.minSamples) {
    console.error(`warning: only collected ${sorted.length} samples (wanted >= ${args.minSamples}) ` +
      `within ${args.maxDurationMs}ms budget`);
  }
}

main().catch((err) => {
  console.error("latency.ts failed:", err);
  process.exit(1);
});
