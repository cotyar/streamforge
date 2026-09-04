/**
 * Live TLS coverage: a real StreamsForge host started with `--Tls:Enabled true` and a fresh dev
 * certificate minted by `tools/tls/dev-cert.sh` (see engine-fixture.ts's `bootTlsEngine()`), on
 * the reserved 7199/7299 pair (silo 17199/gateway 37199). Proves the client-side half of TLS
 * support (this file's own repo, `ccc9a98` on the server side: `Tls:Enabled` on both listeners,
 * `OutboundTls`) against a REAL host and a REAL self-signed certificate, not just the unit-level
 * target/CA parsing tls.test.ts already covers.
 *
 * gRPC gets the full treatment (connect, list tables, a live row round-trip) since `ca=`/`verify=`
 * are fully wired for it (grpc-transport.ts's `createSsl`). SignalR is covered too, but through
 * `NODE_EXTRA_CA_CERTS` rather than this client's own `ca=` option: `@microsoft/signalr`'s Node
 * HTTP client (and the `ws`/`eventsource` packages it reaches for under the hood) build their own
 * request/socket options and never see this client's `RestClient`, so there is no clean hook to
 * thread a per-connection CA into them (see index.ts's ConnectOptions.ca doc comment and the
 * README's TLS section) -- `NODE_EXTRA_CA_CERTS` is the documented, process-wide fallback for
 * that one gap.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { connect } from "../src/index.js";
import { bootTlsEngine, tlsPreflightSkipReason, type TlsEngine } from "./engine-fixture.js";

const skipReason = tlsPreflightSkipReason();
const describeOrSkip = skipReason ? describe.skip : describe;
if (skipReason) console.warn(`streamsforge TLS live tests: SKIPPED -- ${skipReason}`);

describeOrSkip("TLS: isolated engine over https", () => {
  let engine: TlsEngine;

  beforeAll(async () => {
    engine = await bootTlsEngine();
  }, 180_000);

  afterAll(async () => {
    await engine?.stop();
  }, 20_000);

  test("gRPC over TLS: connects, trusts the dev cert via ca= (inline PEM), lists tables, and a live row round-trips", async () => {
    const client = await connect({
      url: engine.baseUrl,
      grpc: engine.grpcTarget,
      user: engine.user,
      password: engine.password,
      ca: engine.certPem,
      transport: "grpc",
    });
    try {
      expect(client.transportName).toBe("grpc");

      const tables = await client.tables();
      expect(tables.some((t) => t.name === engine.latestTable)).toBe(true);

      await using table = await client.table(engine.latestTable, { key: ["trade_id"], timeoutMs: 30_000 });
      const tag = "tls_grpc";
      await client.push(engine.source, [{ trade_id: `${tag}-T1`, desk: "Rates", notional: 111 }]);
      await table.waitFor((rows) => rows.some((r) => r.trade_id === `${tag}-T1`), 15_000);
      expect(table.value("notional", { trade_id: `${tag}-T1` })).toBe(111);
    } finally {
      await client.close();
    }
  }, 30_000);

  test("gRPC over TLS: ca= also accepts a file path, not just inline PEM text", async () => {
    const client = await connect({
      url: engine.baseUrl,
      grpc: engine.grpcTarget,
      user: engine.user,
      password: engine.password,
      ca: engine.certPath,
      transport: "grpc",
    });
    try {
      expect(client.transportName).toBe("grpc");
      const tables = await client.tables();
      expect(tables.some((t) => t.name === engine.latestTable)).toBe(true);
    } finally {
      await client.close();
    }
  }, 30_000);

  test("SignalR over TLS via NODE_EXTRA_CA_CERTS -- must be set BEFORE the process starts", async () => {
    // Confirmed empirically (not just asserted in a doc comment): setting
    // `process.env.NODE_EXTRA_CA_CERTS` mid-process, as a first attempt at this test did, has NO
    // effect -- Bun (like Node) reads that variable once, into its TLS trust store, at startup.
    // A `ws`/`eventsource` connection made later in the SAME process still sees "self signed
    // certificate", the exact failure this whole test exists to rule out. So this test spawns a
    // CHILD process with the env var set on its `env` before it ever starts -- the only way this
    // fallback actually works, and the only way to test it honestly. The child gets NO `ca=` at
    // all: this is specifically proving the env-var-only recipe the README documents, covering
    // both this client's own REST calls (login, push) and the SignalR hub connection itself.
    const dir = mkdtempSync(path.join(tmpdir(), "sf-ts-tls-signalr-child-"));
    const scriptPath = path.join(dir, "check.mjs");
    const here = path.dirname(new URL(import.meta.url).pathname);
    const clientEntry = path.join(here, "..", "src", "index.ts");
    const tag = "tls_signalr_child";
    writeFileSync(
      scriptPath,
      `
      import { connect } from ${JSON.stringify(clientEntry)};
      const client = await connect({
        url: ${JSON.stringify(engine.baseUrl)},
        user: ${JSON.stringify(engine.user)},
        password: ${JSON.stringify(engine.password)},
        transport: "signalr:ws",
      });
      try {
        if (client.transportName !== "signalr:ws") throw new Error("wrong transport: " + client.transportName);
        const table = await client.table(${JSON.stringify(engine.latestTable)}, { key: ["trade_id"], timeoutMs: 30000 });
        try {
          await client.push(${JSON.stringify(engine.source)}, [{ trade_id: "${tag}-T1", desk: "Rates", notional: 222 }]);
          await table.waitFor((rows) => rows.some((r) => r.trade_id === "${tag}-T1"), 15000);
          if (table.value("notional", { trade_id: "${tag}-T1" }) !== 222) throw new Error("wrong value");
        } finally {
          table.close();
        }
      } finally {
        await client.close();
      }
      `,
    );
    try {
      const result = spawnSync("bun", ["run", scriptPath], {
        encoding: "utf-8",
        timeout: 30_000,
        env: { ...process.env, NODE_EXTRA_CA_CERTS: engine.certPath },
      });
      expect(result.status, `child stdout/stderr:\n${result.stdout}\n${result.stderr}`).toBe(0);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 40_000);

  test("negative: connecting over https with no ca and the default verify is refused, not silently accepted", async () => {
    await expect(
      connect({
        url: engine.baseUrl,
        grpc: engine.grpcTarget,
        user: engine.user,
        password: engine.password,
        transport: "grpc",
      }),
    ).rejects.toThrow();
  }, 30_000);
});
