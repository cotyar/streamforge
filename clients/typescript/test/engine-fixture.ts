/**
 * Contract-test fixture: boots an ISOLATED StreamsForge instance on 8199/8299 (never 5199/5299 --
 * the live dev server -- and never 6199 -- the demo container), imports a tiny config (one
 * ingest source, one LATEST BY table, one aggregate over that derived LATEST BY), and tears it
 * down after the suite. Asserts the ports are free first and skips with a clear message rather
 * than colliding -- ported from clients/python/tests/conftest.py, which implements the identical
 * fixture for the Python client's own contract tests.
 *
 * `bootTlsEngine()` (below `bootEngine()`) is the TLS twin, on the reserved 7199/7299 pair with
 * its own silo ports (17199/37199 -- a second host in the same test run needs those, see
 * `--Silo:Port`/`--Silo:GatewayPort` in CLAUDE.md's Ports section) -- it mints a fresh dev
 * certificate via `tools/tls/dev-cert.sh` per boot and passes `--Tls:Enabled true` plus the two
 * certificate args. Both boot functions share one core (`bootEngineWith`) so the publish/spawn/
 * drain/health/import machinery is written and tested exactly once.
 */

import { execSync, spawn, spawnSync, type ChildProcessByStdio } from "node:child_process";
import type { Readable } from "node:stream";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

export const HTTP_PORT = 8199;
export const GRPC_PORT = 8299;
/** TLS twin of HTTP_PORT/GRPC_PORT -- reserved for this fixture, see CLAUDE.md's Ports section. */
export const TLS_HTTP_PORT = 7199;
export const TLS_GRPC_PORT = 7299;
export const TLS_SILO_PORT = 17199;
export const TLS_GATEWAY_PORT = 37199;
const FORBIDDEN_PORTS = new Set([5199, 5299, 6199]);

const DOTNET = path.join(process.env.HOME ?? "", ".dotnet", "dotnet");
const HERE = path.dirname(new URL(import.meta.url).pathname);
const PROJECT_DIR = path.normalize(path.join(HERE, "..", "..", "..", "orleans", "src", "StreamsForge.Host"));
const DEV_CERT_SCRIPT = path.normalize(path.join(HERE, "..", "..", "..", "tools", "tls", "dev-cert.sh"));

export const BASE_URL = `http://localhost:${HTTP_PORT}`;
export const GRPC_TARGET = `localhost:${GRPC_PORT}`;
export const TLS_BASE_URL = `https://localhost:${TLS_HTTP_PORT}`;
export const TLS_GRPC_TARGET = `https://localhost:${TLS_GRPC_PORT}`;
export const ADMIN_USER = "admin";
export const ADMIN_PASS = "admin123!";

export const SOURCE_NAME = "sf_ts_client_trades";
export const LATEST_TABLE = "sf_ts_client_latest_trade";
export const AGG_TABLE = "sf_ts_client_desk_totals";
export const GLOBAL_AGG_TABLE = "sf_ts_client_all_totals";

/** Plan 020 wave G fixture: a crdt-kind source with awareness turned ON (TtlSeconds/MaxEntries
 * both deliberately small -- see awareness.test.ts for why -- MaxEntries=2 in particular so the
 * cap refusal is reachable with two real connections rather than needing dozens). Reuses the SAME
 * engine boot as the transport contract tests (one more `config/import` entry, no extra publish or
 * process) -- awareness is orthogonal to which live-table transport is chosen. */
export const CRDT_AWARENESS_SOURCE = "sf_ts_client_crdt_doc";
export const CRDT_AWARENESS_TTL_SECONDS = 20;
export const CRDT_AWARENESS_MAX_ENTRIES = 2;

/** Last-N-lines buffer for a spawned process's combined stdout/stderr -- see bootEngine()'s
 * stream-draining comment for why lines must be read continuously regardless of whether anyone
 * ends up wanting them. */
class RingBuffer {
  private lines: string[] = [];
  private carry = "";
  constructor(private readonly maxLines: number) {}

  push(chunk: string): void {
    const combined = this.carry + chunk;
    const parts = combined.split("\n");
    this.carry = parts.pop() ?? "";
    this.lines.push(...parts);
    if (this.lines.length > this.maxLines) {
      this.lines.splice(0, this.lines.length - this.maxLines);
    }
  }

  text(): string {
    return [...this.lines, this.carry].filter(Boolean).join("\n");
  }
}

export function portFree(port: number): boolean {
  try {
    execSync(`nc -z -w1 127.0.0.1 ${port}`, { stdio: "ignore" });
    return false; // nc connected => something is listening => NOT free
  } catch {
    return true;
  }
}

function baseEnginePreflight(httpPort: number, grpcPort: number): string | null {
  if (FORBIDDEN_PORTS.has(httpPort) || FORBIDDEN_PORTS.has(grpcPort)) {
    return "refusing to configure the contract-test fixture onto a forbidden port";
  }
  if (!portFree(httpPort) || !portFree(grpcPort)) {
    return `port ${httpPort} or ${grpcPort} is already in use -- refusing to collide with a running instance`;
  }
  if (!existsSync(DOTNET)) {
    return `dotnet not found at ${DOTNET} -- cannot boot the contract-test engine`;
  }
  if (!existsSync(PROJECT_DIR)) {
    return `StreamsForge.Host project not found at ${PROJECT_DIR}`;
  }
  return null;
}

/** Synchronous, so callers can gate `test.skipIf` at module-load time (mirrors pytest.skip). */
export function preflightSkipReason(): string | null {
  return baseEnginePreflight(HTTP_PORT, GRPC_PORT);
}

/** TLS twin of preflightSkipReason(): the plain checks (ports, dotnet, project dir) plus openssl
 * on PATH and tools/tls/dev-cert.sh actually existing -- the same two things DevCert.Preflight()
 * checks on the C# side of this exact fixture (StreamsForge.Chain.Tests/DevCert.cs). */
export function tlsPreflightSkipReason(): string | null {
  const base = baseEnginePreflight(TLS_HTTP_PORT, TLS_GRPC_PORT);
  if (base) return base;
  if (!portFree(TLS_SILO_PORT) || !portFree(TLS_GATEWAY_PORT)) {
    return `port ${TLS_SILO_PORT} or ${TLS_GATEWAY_PORT} is already in use -- refusing to collide with a running instance`;
  }
  if (!existsSync(DEV_CERT_SCRIPT)) {
    return `${DEV_CERT_SCRIPT} not found -- cannot mint a development certificate`;
  }
  try {
    execSync("command -v openssl", { stdio: "ignore" });
  } catch {
    return "openssl not found on PATH -- cannot run tools/tls/dev-cert.sh";
  }
  return null;
}

type SpawnedEngine = ChildProcessByStdio<null, Readable, Readable>;

async function waitHealthy(proc: SpawnedEngine, baseUrl: string, fetchInit: RequestInit, timeoutMs = 90_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    if (proc.exitCode !== null) {
      throw new Error(`engine process exited early (code ${proc.exitCode})`);
    }
    try {
      const res = await fetch(`${baseUrl}/api/healthz`, fetchInit);
      if (res.ok) return;
    } catch (err) {
      lastErr = err;
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`engine did not become healthy within ${timeoutMs}ms (last error: ${String(lastErr)})`);
}

async function importFixtureConfig(baseUrl: string, fetchInit: RequestInit): Promise<void> {
  const loginRes = await fetch(`${baseUrl}/api/auth/login`, {
    ...fetchInit,
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ username: ADMIN_USER, password: ADMIN_PASS }),
  });
  if (!loginRes.ok) throw new Error(`fixture login failed: ${loginRes.status}`);
  const { token } = (await loginRes.json()) as { token: string };

  const doc = {
    version: 1,
    sources: [
      {
        name: SOURCE_NAME,
        description: "TypeScript client contract test fixture",
        kind: "ingest",
        fields: [
          { name: "trade_id", type: "String" },
          { name: "desk", type: "String" },
          { name: "notional", type: "Double" },
        ],
        ingest: {},
        enabled: true,
      },
      {
        name: CRDT_AWARENESS_SOURCE,
        description: "TypeScript client awareness fixture (plan 020 wave G)",
        kind: "crdt",
        fields: [
          { name: "id", type: "String" },
          { name: "value", type: "String" },
        ],
        connector: {
          crdt: {
            rootMap: "root",
            keyField: "id",
            awareness: { ttlSeconds: CRDT_AWARENESS_TTL_SECONDS, maxEntries: CRDT_AWARENESS_MAX_ENTRIES },
          },
        },
        enabled: true,
      },
    ],
    pipelines: [],
    tables: [
      {
        name: LATEST_TABLE,
        description: "latest row per trade_id",
        sql: `SELECT trade_id, desk, notional FROM ${SOURCE_NAME} LATEST BY (trade_id)`,
        running: true,
      },
      {
        name: AGG_TABLE,
        description: "aggregate over the derived LATEST BY",
        sql: `SELECT desk, SUM(notional) AS total FROM ${LATEST_TABLE} GROUP BY desk`,
        running: true,
      },
      {
        name: GLOBAL_AGG_TABLE,
        description: "unkeyed global aggregate (no GROUP BY) -- exercises keyFields=[] over the wire",
        sql: `SELECT COUNT(*) AS trade_count, SUM(notional) AS total_notional FROM ${LATEST_TABLE}`,
        running: true,
      },
    ],
  };

  const res = await fetch(`${baseUrl}/api/config/import?mode=merge`, {
    ...fetchInit,
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
    body: JSON.stringify(doc),
  });
  const report = (await res.json()) as { entries: Array<{ action: string }> };
  const errored = report.entries.filter((e) => e.action === "error");
  if (!res.ok || errored.length > 0) {
    throw new Error(`fixture config import failed: ${JSON.stringify(errored)}`);
  }
}

/**
 * This machine's own dotnet RID, e.g. `osx-arm64` -- REQUIRED on the `dotnet publish` command
 * line below. Plan 022's `Publish.props` (host-owned, not this file's to change) turns EVERY
 * `dotnet publish` of this project into a self-contained single-file build
 * (`PublishSingleFile`+`SelfContained`, gated on MSBuild's own `_IsPublishing`, unconditional on
 * Configuration) and, when no `-r` is given at all, defaults `RuntimeIdentifier` to `linux-x64` --
 * fine for `tools/publish.sh`'s container target, but on any other host that silently cross-
 * publishes a Linux ELF binary this fixture then cannot run. Passing the host's own RID here
 * avoids the fallback; see `hostExeName` below for what the resulting output looks like.
 */
function hostRid(): string {
  const os = process.platform === "darwin" ? "osx" : process.platform === "win32" ? "win" : "linux";
  const arch = process.arch === "arm64" ? "arm64" : "x64";
  return `${os}-${arch}`;
}

/** A self-contained single-file publish (see `hostRid`'s doc comment) names its executable after
 * the project's own assembly name, `.exe` on Windows only -- there is no `.dll` to `dotnet run`
 * anymore; this fixture spawns the executable directly instead (also true of `deploy/`'s
 * Dockerfiles per CLAUDE.md's "Plugins & single-file" paragraph, for the same reason). */
function hostExeName(): string {
  return process.platform === "win32" ? "StreamsForge.Host.exe" : "StreamsForge.Host";
}

function publish(publishDir: string): void {
  // `dotnet publish` into an isolated directory nothing else owns -- running the produced
  // executable directly (rather than `dotnet run`) sidesteps colliding with a live `dotnet run`
  // process's own bin/obj output (see conftest.py's identical comment for the failure mode this
  // avoids).
  const result = spawnSync(DOTNET, ["publish", PROJECT_DIR, "-c", "Debug", "-r", hostRid(), "-o", publishDir], {
    encoding: "utf-8",
    timeout: 300_000,
  });
  if (result.status !== 0) {
    throw new Error(`dotnet publish failed (code ${result.status}):\n${(result.stdout + result.stderr).slice(-6000)}`);
  }
}

export interface Engine {
  baseUrl: string;
  grpcTarget: string;
  user: string;
  password: string;
  source: string;
  latestTable: string;
  aggTable: string;
  globalAggTable: string;
  crdtAwarenessSource: string;
  /** Last ~200 lines of the engine's combined stdout/stderr, for a test to print on its own
   * failure (e.g. a hang/deadlock that never triggers the process-exit tail above). */
  tail: () => string;
  stop: () => Promise<void>;
}

export interface TlsEngine extends Engine {
  /** PEM text of the dev certificate this instance's TLS listeners present -- pass as `ca:` to
   * connect() (or write it out yourself; `certPath` below is the file dev-cert.sh already wrote). */
  certPem: string;
  /** Path to the same certificate on disk. */
  certPath: string;
}

interface BootConfig {
  httpPort: number;
  grpcPort: number;
  /** Extra CLI args appended after the always-present --Http:Port/--Grpc:Port/--DataDir triple. */
  extraArgs: string[];
  baseUrl: string;
  /** Merged into every fetch this fixture itself makes against the engine (health probe, fixture
   * config import) -- `{}` for plain http, `{ tls: { ca } }` for the TLS variant so the readiness
   * probe and the config import both go through the same trust path a real caller would use. */
  fetchInit: RequestInit;
}

async function bootEngineWith(cfg: BootConfig): Promise<Engine> {
  const prebuilt = process.env.SF_TEST_PUBLISH_DIR;
  const publishDir = prebuilt ?? mkdtempSync(path.join(tmpdir(), "sf-ts-client-publish-"));
  const dataDir = mkdtempSync(path.join(tmpdir(), "sf-ts-client-test-"));

  if (!prebuilt) {
    try {
      publish(publishDir);
    } catch (err) {
      rmSync(publishDir, { recursive: true, force: true });
      rmSync(dataDir, { recursive: true, force: true });
      throw err;
    }
  }

  const exe = path.join(publishDir, hostExeName());
  // WebApplication.CreateBuilder takes its content root from the CURRENT DIRECTORY -- run the
  // executable from anywhere else and appsettings.json is never found (Jwt:Key is null, every
  // request 500s including /api/healthz). See §"Two traps" in the task brief / conftest.py.
  const proc = spawn(
    exe,
    ["--Http:Port", String(cfg.httpPort), "--Grpc:Port", String(cfg.grpcPort), "--DataDir", dataDir, ...cfg.extraArgs],
    { cwd: publishDir, stdio: ["ignore", "pipe", "pipe"] },
  );
  // MUST drain stdout/stderr continuously: once the OS pipe buffer fills (64KB on macOS) with no
  // reader, the child's next write blocks forever -- an engine that "hangs" with no error is
  // almost always this, not an actual engine bug. Bounded to the last N lines (rather than an
  // unbounded string) so a long-running suite's own log volume can't grow this without limit --
  // same ring-buffer shape as clients/python/tests/conftest.py's `_Drain`.
  const tail = new RingBuffer(200);
  proc.stdout.on("data", (d: Buffer) => tail.push(d.toString()));
  proc.stderr.on("data", (d: Buffer) => tail.push(d.toString()));

  // The original version of this fixture only printed `tail` from the catch block around
  // waitHealthy()/importFixtureConfig() -- i.e. only a failure during BOOT. A crash mid-suite
  // (the engine dies between two tests, or between two requests inside one test) produced no
  // catch here at all: the test just sees `ECONNREFUSED`/"socket closed unexpectedly" with the
  // engine's own explanation of why it died silently discarded. Print the tail the moment the
  // process exits for any reason we didn't ourselves request via stop() -- this is the only place
  // that reliably fires exactly once, exactly when it's needed, regardless of which test (or
  // which transport's beforeAll) happens to be running at the time.
  let stopRequested = false;
  proc.on("exit", (code, signal) => {
    if (stopRequested) return;
    console.error(
      `engine process exited UNEXPECTEDLY (code ${code}, signal ${signal}) -- this almost always means a test is ` +
        `about to fail with ECONNREFUSED/"socket closed unexpectedly" right after this. Engine output (tail):\n` +
        tail.text(),
    );
  });

  const stop = async () => {
    stopRequested = true;
    proc.kill("SIGTERM");
    await new Promise<void>((resolve) => {
      const t = setTimeout(() => {
        proc.kill("SIGKILL");
        resolve();
      }, 15_000);
      proc.once("exit", () => {
        clearTimeout(t);
        resolve();
      });
    });
    rmSync(dataDir, { recursive: true, force: true });
    if (!prebuilt) rmSync(publishDir, { recursive: true, force: true });
  };

  try {
    await waitHealthy(proc, cfg.baseUrl, cfg.fetchInit);
    await importFixtureConfig(cfg.baseUrl, cfg.fetchInit);
  } catch (err) {
    console.error("engine boot output (tail):\n" + tail.text());
    await stop();
    throw err;
  }

  return {
    baseUrl: cfg.baseUrl,
    grpcTarget: cfg.baseUrl.startsWith("https://") ? `https://${new URL(cfg.baseUrl).hostname}:${cfg.grpcPort}` : `${new URL(cfg.baseUrl).hostname}:${cfg.grpcPort}`,
    user: ADMIN_USER,
    password: ADMIN_PASS,
    source: SOURCE_NAME,
    latestTable: LATEST_TABLE,
    aggTable: AGG_TABLE,
    globalAggTable: GLOBAL_AGG_TABLE,
    crdtAwarenessSource: CRDT_AWARENESS_SOURCE,
    tail: () => tail.text(),
    stop,
  };
}

export async function bootEngine(): Promise<Engine> {
  return bootEngineWith({
    httpPort: HTTP_PORT,
    grpcPort: GRPC_PORT,
    extraArgs: ["--Streams:Transport", "push"],
    baseUrl: BASE_URL,
    fetchInit: {},
  });
}

/**
 * TLS twin of bootEngine(): mints a fresh dev certificate via `tools/tls/dev-cert.sh` into its own
 * temp directory, boots the host with `--Tls:Enabled true` and that pair, on the reserved
 * 7199/7299 (silo 17199/gateway 37199 -- a second host in the same process needs its own, see
 * CLAUDE.md's Ports section). Call `tlsPreflightSkipReason()` first and skip when it returns
 * non-null, same convention as `bootEngine()`/`preflightSkipReason()`.
 */
export async function bootTlsEngine(): Promise<TlsEngine> {
  const certDir = mkdtempSync(path.join(tmpdir(), "sf-ts-client-tls-cert-"));
  let certPath: string;
  let keyPath: string;
  try {
    const result = spawnSync("/bin/bash", [DEV_CERT_SCRIPT, certDir, "127.0.0.1"], { encoding: "utf-8", timeout: 30_000 });
    if (result.status !== 0) {
      throw new Error(`tools/tls/dev-cert.sh exited ${result.status}:\n${result.stdout}${result.stderr}`);
    }
    certPath = path.join(certDir, "cert.pem");
    keyPath = path.join(certDir, "key.pem");
    if (!existsSync(certPath) || !existsSync(keyPath)) {
      throw new Error(`tools/tls/dev-cert.sh produced no cert.pem/key.pem in ${certDir}:\n${result.stdout}`);
    }
  } catch (err) {
    rmSync(certDir, { recursive: true, force: true });
    throw err;
  }
  const certPem = readFileSync(certPath, "utf-8");

  const engine = await bootEngineWith({
    httpPort: TLS_HTTP_PORT,
    grpcPort: TLS_GRPC_PORT,
    extraArgs: [
      "--Streams:Transport",
      "push",
      "--Tls:Enabled",
      "true",
      "--Kestrel:Certificates:Default:Path",
      certPath,
      "--Kestrel:Certificates:Default:KeyPath",
      keyPath,
      "--Silo:Port",
      String(TLS_SILO_PORT),
      "--Silo:GatewayPort",
      String(TLS_GATEWAY_PORT),
    ],
    baseUrl: TLS_BASE_URL,
    fetchInit: { tls: { ca: certPem } } as RequestInit,
  });

  const baseStop = engine.stop;
  return {
    ...engine,
    certPem,
    certPath,
    stop: async () => {
      await baseStop();
      rmSync(certDir, { recursive: true, force: true });
    },
  };
}
