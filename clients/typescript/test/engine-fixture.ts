/**
 * Contract-test fixture: boots an ISOLATED StreamForge instance on 8199/8299 (never 5199/5299 --
 * the live dev server -- and never 6199 -- the demo container), imports a tiny config (one
 * ingest source, one LATEST BY table, one aggregate over that derived LATEST BY), and tears it
 * down after the suite. Asserts the ports are free first and skips with a clear message rather
 * than colliding -- ported from clients/python/tests/conftest.py, which implements the identical
 * fixture for the Python client's own contract tests.
 */

import { execSync, spawn, spawnSync, type ChildProcessByStdio } from "node:child_process";
import type { Readable } from "node:stream";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

export const HTTP_PORT = 8199;
export const GRPC_PORT = 8299;
const FORBIDDEN_PORTS = new Set([5199, 5299, 6199]);

const DOTNET = path.join(process.env.HOME ?? "", ".dotnet", "dotnet");
const HERE = path.dirname(new URL(import.meta.url).pathname);
const PROJECT_DIR = path.normalize(path.join(HERE, "..", "..", "..", "orleans", "src", "StreamForge.Host"));

export const BASE_URL = `http://localhost:${HTTP_PORT}`;
export const GRPC_TARGET = `localhost:${GRPC_PORT}`;
export const ADMIN_USER = "admin";
export const ADMIN_PASS = "admin123!";

export const SOURCE_NAME = "sf_ts_client_trades";
export const LATEST_TABLE = "sf_ts_client_latest_trade";
export const AGG_TABLE = "sf_ts_client_desk_totals";

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

/** Synchronous, so callers can gate `test.skipIf` at module-load time (mirrors pytest.skip). */
export function preflightSkipReason(): string | null {
  if (FORBIDDEN_PORTS.has(HTTP_PORT) || FORBIDDEN_PORTS.has(GRPC_PORT)) {
    return "refusing to configure the contract-test fixture onto a forbidden port";
  }
  if (!portFree(HTTP_PORT) || !portFree(GRPC_PORT)) {
    return `port ${HTTP_PORT} or ${GRPC_PORT} is already in use -- refusing to collide with a running instance`;
  }
  if (!existsSync(DOTNET)) {
    return `dotnet not found at ${DOTNET} -- cannot boot the contract-test engine`;
  }
  if (!existsSync(PROJECT_DIR)) {
    return `StreamForge.Host project not found at ${PROJECT_DIR}`;
  }
  return null;
}

type SpawnedEngine = ChildProcessByStdio<null, Readable, Readable>;

async function waitHealthy(proc: SpawnedEngine, timeoutMs = 90_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    if (proc.exitCode !== null) {
      throw new Error(`engine process exited early (code ${proc.exitCode})`);
    }
    try {
      const res = await fetch(`${BASE_URL}/api/healthz`);
      if (res.ok) return;
    } catch (err) {
      lastErr = err;
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`engine did not become healthy within ${timeoutMs}ms (last error: ${String(lastErr)})`);
}

async function importFixtureConfig(): Promise<void> {
  const loginRes = await fetch(`${BASE_URL}/api/auth/login`, {
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
    ],
  };

  const res = await fetch(`${BASE_URL}/api/config/import?mode=merge`, {
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

function publish(publishDir: string): void {
  // `dotnet publish` into an isolated directory nothing else owns -- running the produced DLL
  // directly (rather than `dotnet run`) sidesteps colliding with a live `dotnet run` process's
  // own bin/obj output (see conftest.py's identical comment for the failure mode this avoids).
  const result = spawnSync(DOTNET, ["publish", PROJECT_DIR, "-c", "Debug", "-o", publishDir], {
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
  stop: () => Promise<void>;
}

export async function bootEngine(): Promise<Engine> {
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

  const dll = path.join(publishDir, "StreamForge.Host.dll");
  // WebApplication.CreateBuilder takes its content root from the CURRENT DIRECTORY -- run the
  // DLL from anywhere else and appsettings.json is never found (Jwt:Key is null, every request
  // 500s including /api/healthz). See §"Two traps" in the task brief / conftest.py.
  const proc = spawn(
    DOTNET,
    [dll, "--Http:Port", String(HTTP_PORT), "--Grpc:Port", String(GRPC_PORT), "--Streams:Transport", "push", "--DataDir", dataDir],
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

  const stop = async () => {
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
    await waitHealthy(proc);
    await importFixtureConfig();
  } catch (err) {
    console.error("engine boot output (tail):\n" + tail.text());
    await stop();
    throw err;
  }

  return {
    baseUrl: BASE_URL,
    grpcTarget: GRPC_TARGET,
    user: ADMIN_USER,
    password: ADMIN_PASS,
    source: SOURCE_NAME,
    latestTable: LATEST_TABLE,
    aggTable: AGG_TABLE,
    stop,
  };
}
