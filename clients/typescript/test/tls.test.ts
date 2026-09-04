/**
 * Unit coverage for TLS support -- no engine, no network. Three pure-ish pieces:
 *
 *   - grpc-transport.ts's parseGrpcTarget: scheme -> {target, tls}.
 *   - index.ts's defaultGrpcTarget: the REST base URL's scheme carries over to the guessed gRPC
 *     target (a `https://` REST URL must not silently produce a plaintext gRPC guess against
 *     what is, once `--Tls:Enabled true` is on, a TLS-only port).
 *   - ca-resolve.ts's isPemText/resolveCaPem: inline PEM text vs. a file path.
 *
 * The live counterpart (an actual TLS host, real certs, gRPC + SignalR over https) is
 * tls-live.test.ts.
 */

import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseGrpcTarget } from "../src/grpc-transport.js";
import { defaultGrpcTarget } from "../src/index.js";
import { isPemText, resolveCaPem } from "../src/ca-resolve.js";

describe("parseGrpcTarget", () => {
  test("bare host:port is plaintext, unchanged from before TLS support", () => {
    expect(parseGrpcTarget("localhost:8299")).toEqual({ target: "localhost:8299", tls: false });
  });

  test("http:// is plaintext, scheme stripped", () => {
    expect(parseGrpcTarget("http://localhost:8299")).toEqual({ target: "localhost:8299", tls: false });
  });

  test("https:// is TLS, scheme stripped", () => {
    expect(parseGrpcTarget("https://localhost:7299")).toEqual({ target: "localhost:7299", tls: true });
  });

  test("an IPv4 host works the same as a hostname", () => {
    expect(parseGrpcTarget("https://127.0.0.1:7299")).toEqual({ target: "127.0.0.1:7299", tls: true });
  });
});

describe("defaultGrpcTarget", () => {
  test("http base URL guesses a plaintext target at port+100", () => {
    expect(defaultGrpcTarget("http://localhost:5199")).toBe("localhost:5299");
  });

  test("https base URL preserves the scheme in the guessed target", () => {
    expect(defaultGrpcTarget("https://localhost:7199")).toBe("https://localhost:7299");
  });

  test("https base URL with a non-default hostname", () => {
    expect(defaultGrpcTarget("https://example.internal:9199")).toBe("https://example.internal:9299");
  });
});

describe("ca-resolve", () => {
  test("isPemText recognizes inline PEM", () => {
    expect(isPemText("-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n")).toBe(true);
    expect(isPemText("/tmp/whatever/cert.pem")).toBe(false);
    expect(isPemText("relative/cert.pem")).toBe(false);
  });

  test("resolveCaPem returns inline PEM text verbatim", () => {
    const pem = "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n";
    expect(resolveCaPem(pem)).toBe(pem);
  });

  test("resolveCaPem reads a file path", () => {
    const dir = mkdtempSync(join(tmpdir(), "sf-ts-ca-resolve-"));
    const certPath = join(dir, "cert.pem");
    const pem = "-----BEGIN CERTIFICATE-----\nfake-for-this-unit-test\n-----END CERTIFICATE-----\n";
    writeFileSync(certPath, pem);
    try {
      expect(resolveCaPem(certPath)).toBe(pem);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
