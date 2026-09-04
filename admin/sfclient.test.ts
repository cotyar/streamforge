// TLS support for the admin CLI/MCP client: SF_CA_FILE (a PEM file path) and SF_INSECURE=1, both
// read fresh on every request (tlsRequestInit()) so login() and request() -- the two `fetch(` call
// sites in sfclient.ts -- see the same trust config through the one `sfFetch` helper both route
// through. Two layers: tlsRequestInit() in isolation (no network), then a real Bun.serve HTTPS
// server (cert minted by tools/tls/dev-cert.sh, the same script the TypeScript client's own TLS
// tests use) proving SfClient actually connects with the right env var and is refused without it.

import { afterEach, describe, expect, test } from "bun:test";
import { execSync, spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { SfClient, tlsRequestInit } from "./sfclient.ts";

const ORIGINAL_ENV = { SF_CA_FILE: process.env.SF_CA_FILE, SF_INSECURE: process.env.SF_INSECURE };

afterEach(() => {
  for (const [k, v] of Object.entries(ORIGINAL_ENV)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
});

describe("tlsRequestInit", () => {
  test("neither env var set -> no tls option at all", () => {
    delete process.env.SF_CA_FILE;
    delete process.env.SF_INSECURE;
    expect(tlsRequestInit()).toEqual({});
  });

  test("SF_INSECURE=1 -> rejectUnauthorized: false", () => {
    delete process.env.SF_CA_FILE;
    process.env.SF_INSECURE = "1";
    expect(tlsRequestInit()).toEqual({ tls: { rejectUnauthorized: false } });
  });

  test("SF_INSECURE set to anything else is not truthy -- only the literal '1' counts", () => {
    delete process.env.SF_CA_FILE;
    process.env.SF_INSECURE = "true";
    expect(tlsRequestInit()).toEqual({});
  });

  test("SF_CA_FILE reads the PEM off disk and passes its content, not the path", () => {
    const dir = mkdtempSync(join(tmpdir(), "sf-admin-ca-"));
    const certPath = join(dir, "cert.pem");
    const pem = "-----BEGIN CERTIFICATE-----\nfake-for-this-unit-test\n-----END CERTIFICATE-----\n";
    try {
      writeFileSync(certPath, pem);
      delete process.env.SF_INSECURE;
      process.env.SF_CA_FILE = certPath;
      expect(tlsRequestInit()).toEqual({ tls: { ca: pem } });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("both set at once merge into one tls object", () => {
    const dir = mkdtempSync(join(tmpdir(), "sf-admin-ca-"));
    const certPath = join(dir, "cert.pem");
    const pem = "-----BEGIN CERTIFICATE-----\nfake-for-this-unit-test\n-----END CERTIFICATE-----\n";
    try {
      writeFileSync(certPath, pem);
      process.env.SF_CA_FILE = certPath;
      process.env.SF_INSECURE = "1";
      expect(tlsRequestInit()).toEqual({ tls: { ca: pem, rejectUnauthorized: false } });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---- live: a real Bun.serve HTTPS server, a real self-signed cert -----------------------------

const HERE = dirname(new URL(import.meta.url).pathname);
const DEV_CERT_SCRIPT = join(HERE, "..", "tools", "tls", "dev-cert.sh");

function tlsPreflight(): string | null {
  if (!existsSync(DEV_CERT_SCRIPT)) return `${DEV_CERT_SCRIPT} not found -- cannot mint a development certificate`;
  try {
    execSync("command -v openssl", { stdio: "ignore" });
  } catch {
    return "openssl not found on PATH -- cannot run tools/tls/dev-cert.sh";
  }
  return null;
}

const skipReason = tlsPreflight();
const describeOrSkip = skipReason ? describe.skip : describe;
if (skipReason) console.warn(`admin sfclient TLS live tests: SKIPPED -- ${skipReason}`);

describeOrSkip("SfClient over a real HTTPS server", () => {
  test("SF_CA_FILE trusts the dev cert; healthz succeeds", async () => {
    const dir = mkdtempSync(join(tmpdir(), "sf-admin-tls-"));
    const result = spawnSync("/bin/bash", [DEV_CERT_SCRIPT, dir, "127.0.0.1"], { encoding: "utf-8" });
    expect(result.status).toBe(0);
    const certPath = join(dir, "cert.pem");
    const keyPath = join(dir, "key.pem");

    const server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      tls: { cert: readFileSync(certPath), key: readFileSync(keyPath) },
      fetch(req) {
        const url = new URL(req.url);
        if (url.pathname === "/api/healthz") return Response.json({ status: "ok" });
        return new Response("not found", { status: 404 });
      },
    });
    try {
      const url = `https://127.0.0.1:${server.port}`;

      delete process.env.SF_INSECURE;
      process.env.SF_CA_FILE = certPath;
      const trusting = new SfClient({ url });
      await expect(trusting.health()).resolves.toEqual({ status: "ok" });

      delete process.env.SF_CA_FILE;
      const untrusting = new SfClient({ url });
      await expect(untrusting.health()).rejects.toThrow();

      process.env.SF_INSECURE = "1";
      const insecure = new SfClient({ url });
      await expect(insecure.health()).resolves.toEqual({ status: "ok" });
    } finally {
      server.stop(true);
      rmSync(dir, { recursive: true, force: true });
    }
  }, 20_000);
});
