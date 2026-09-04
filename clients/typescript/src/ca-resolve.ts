/**
 * Resolves `ConnectOptions.ca` / `STREAMSFORGE_CA` (PEM text OR a file path) into PEM text ready
 * to hand to `RestClient`'s `tls.ca` (Bun's fetch extension accepts a plain string) and
 * `GrpcTransport`'s `createSsl` (which wants a Buffer -- `Buffer.from(pem, "utf-8")` at the call
 * site, not here, so this module stays string-in/string-out).
 *
 * Node-only (`node:fs`) -- like grpc-transport.ts, this module is never imported eagerly.
 * index.ts's `resolveCa` only reaches it via a dynamic `import()`, guarded by the same
 * `isNodeRuntime()` check the gRPC transport uses, so a browser bundle of `@streamsforge/client`
 * never pulls in `node:fs` merely because TLS's `ca=` option exists. A browser caller can still
 * use `ca=` -- but only as inline PEM text (`isPemText` below is checked in index.ts BEFORE this
 * module is ever loaded), since a browser has no filesystem to resolve a path against.
 */

import { readFileSync } from "node:fs";

/** A `-----BEGIN` marker anywhere in the string is treated as "this is PEM text, not a path" --
 * mirrors how a filesystem path could never legally contain that literal sequence in practice. */
export function isPemText(ca: string): boolean {
  return ca.includes("-----BEGIN");
}

/** `ca` is PEM text (returned verbatim) or a file path (read as utf-8). */
export function resolveCaPem(ca: string): string {
  return isPemText(ca) ? ca : readFileSync(ca, "utf-8");
}
