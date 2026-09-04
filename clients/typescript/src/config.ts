/**
 * Config resolution: explicit kwargs -> env. Ported from clients/python/src/streamsforge/_config.py,
 * minus the `~/.config/streamsforge/config.toml` layer (a Node/browser-isomorphic package has no
 * uniform "home directory" story the way a Python CLI-adjacent tool does -- env and explicit
 * kwargs cover every deployment this client targets; add a file layer if a Node-only CLI wants one).
 */

export interface ResolvedConfig {
  baseUrl: string | undefined;
  grpc: string | undefined;
  user: string | undefined;
  password: string | undefined;
  ingestKey: string | undefined;
  /** PEM text or a file path -- see ConnectOptions.ca's doc comment in index.ts. Unresolved: the
   * caller (connect()) turns this into PEM text via resolveCa before using it. */
  ca: string | undefined;
}

function env(name: string): string | undefined {
  // `process` is undefined in a browser bundle; guarded so this module is safe to import there.
  if (typeof process === "undefined" || !process.env) return undefined;
  return process.env[name];
}

export function resolveConfig(opts: {
  url?: string;
  grpc?: string;
  user?: string;
  password?: string;
  ingestKey?: string;
  ca?: string;
}): ResolvedConfig {
  return {
    baseUrl: opts.url ?? env("STREAMSFORGE_BASE_URL"),
    grpc: opts.grpc ?? env("STREAMSFORGE_GRPC"),
    user: opts.user ?? env("STREAMSFORGE_ADMIN_USER"),
    password: opts.password ?? env("STREAMSFORGE_ADMIN_PASS"),
    ingestKey: opts.ingestKey ?? env("SF_INGEST_KEY"),
    ca: opts.ca ?? env("STREAMSFORGE_CA"),
  };
}
