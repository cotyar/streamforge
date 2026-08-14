// StreamForge REST client — the one file that knows the API's shape. Shared by the CLI (sf.ts) and
// the MCP server (mcp.ts) so the two can never drift about what "start a table" means.
//
// Zero npm dependencies, same house rule as main.ts. Both runtime flavors serve the identical REST
// surface (shared/StreamForge.Api), so a base URL is the whole difference between administering
// Orleans on :5199 and Dapr on :5399.

import { chmodSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

export const DEFAULT_URL = "http://localhost:5199";
export const TOKEN_FILE = join(homedir(), ".streamforge", "token.json");

/** The three catalog entity kinds, as they appear in REST paths. Sources are addressed by NAME,
 * pipelines and tables by ID — a REST-level asymmetry the callers would otherwise each rediscover. */
export const KINDS = ["sources", "pipelines", "tables"] as const;
export type Kind = (typeof KINDS)[number];

export function isKind(x: string): x is Kind {
  return (KINDS as readonly string[]).includes(x);
}

/** Kinds with a lifecycle. A source is enabled/disabled through its definition, not started. */
export const RUNNABLE: Kind[] = ["pipelines", "tables"];

export class SfError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = "SfError";
  }
}

export interface ClientOptions {
  url?: string;
  token?: string;
  /** Credentials for an on-the-spot login when no token is available. */
  user?: string;
  password?: string;
}

interface StoredToken {
  url: string;
  token: string;
  username: string;
  role: string;
}

/** Reads the token `sf login` stored, but only when it belongs to the instance being addressed —
 * a token minted by one host is meaningless to another, and silently sending it would produce a
 * confusing 401 instead of "you are not logged in to this one". */
export function readStoredToken(url: string): StoredToken | null {
  try {
    const stored = JSON.parse(readFileSync(TOKEN_FILE, "utf8")) as StoredToken;
    return stored.url === url ? stored : null;
  } catch {
    return null;
  }
}

export function writeStoredToken(stored: StoredToken): string {
  mkdirSync(dirname(TOKEN_FILE), { recursive: true });
  writeFileSync(TOKEN_FILE, JSON.stringify(stored, null, 2) + "\n");
  // The JWT is a bearer credential for its 12h lifetime — no wider than the owner.
  chmodSync(TOKEN_FILE, 0o600);
  return TOKEN_FILE;
}

export class SfClient {
  readonly url: string;
  private token: string | null;
  private readonly user?: string;
  private readonly password?: string;

  constructor(opts: ClientOptions = {}) {
    this.url = (opts.url ?? process.env.SF_URL ?? DEFAULT_URL).replace(/\/+$/, "");
    this.token = opts.token ?? process.env.SF_TOKEN ?? readStoredToken(this.url)?.token ?? null;
    this.user = opts.user ?? process.env.SF_USER;
    this.password = opts.password ?? process.env.SF_PASSWORD;
  }

  /** Exchanges credentials for a JWT. Called explicitly by `sf login`, and lazily by any request
   * when no token was supplied but SF_USER/SF_PASSWORD were — which is what makes the MCP server
   * configurable with nothing but env vars in the client's config file. */
  async login(user?: string, password?: string): Promise<StoredToken> {
    const username = user ?? this.user;
    const secret = password ?? this.password;
    if (!username || !secret) {
      throw new SfError("no credentials: set SF_TOKEN, or SF_USER + SF_PASSWORD, or run `sf login`");
    }

    const res = await fetch(`${this.url}/api/auth/login`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username, password: secret }),
    });
    if (!res.ok) {
      throw new SfError(`login failed as '${username}' (${res.status})`, res.status);
    }

    const body = (await res.json()) as { token: string; username: string; role: string };
    this.token = body.token;
    return { url: this.url, token: body.token, username: body.username, role: body.role };
  }

  private async authorize(): Promise<Record<string, string>> {
    if (!this.token && (this.user || this.password)) {
      await this.login();
    }
    return this.token ? { authorization: `Bearer ${this.token}` } : {};
  }

  /** One request. `raw` returns the body as text (CSV, proto, a config export) instead of parsing
   * JSON. A non-2xx becomes an SfError carrying the server's own message where it sent one — the
   * API answers with {"error": "..."} throughout, and repeating that verbatim beats inventing a
   * client-side vocabulary for the same failures. */
  async request<T = unknown>(
    method: string,
    path: string,
    opts: { body?: unknown; raw?: boolean; contentType?: string } = {},
  ): Promise<T> {
    const headers: Record<string, string> = { accept: "application/json", ...(await this.authorize()) };
    let body: string | undefined;
    if (opts.body !== undefined) {
      headers["content-type"] = opts.contentType ?? "application/json";
      body = typeof opts.body === "string" ? opts.body : JSON.stringify(opts.body);
    }

    let res: Response;
    try {
      res = await fetch(`${this.url}${path}`, { method, headers, body });
    } catch (err) {
      throw new SfError(`cannot reach ${this.url}: ${err instanceof Error ? err.message : String(err)}`);
    }

    if (!res.ok) {
      throw new SfError(`${method} ${path} → ${res.status}: ${await errorText(res)}`, res.status);
    }

    if (res.status === 204) return undefined as T;
    return (opts.raw ? await res.text() : await res.json()) as T;
  }

  // ---- the admin surface, named ----------------------------------------------------------------

  health(): Promise<unknown> {
    return this.request("GET", "/api/healthz");
  }

  me(): Promise<unknown> {
    return this.request("GET", "/api/auth/me");
  }

  list(kind: Kind): Promise<unknown[]> {
    return this.request<unknown[]>("GET", `/api/${kind}`);
  }

  get(kind: Kind, id: string): Promise<unknown> {
    return this.request("GET", `/api/${kind}/${encodeURIComponent(id)}`);
  }

  create(kind: Kind, definition: unknown): Promise<unknown> {
    return this.request("POST", `/api/${kind}`, { body: definition });
  }

  update(kind: Kind, id: string, definition: unknown): Promise<unknown> {
    return this.request("PUT", `/api/${kind}/${encodeURIComponent(id)}`, { body: definition });
  }

  remove(kind: Kind, id: string): Promise<unknown> {
    return this.request("DELETE", `/api/${kind}/${encodeURIComponent(id)}`);
  }

  /** Sources have no start/stop — the caller is told so rather than getting a 404 from a path that
   * was never going to exist. */
  // `async` so the guard below REJECTS rather than throwing synchronously out of a Promise-returning
  // method — a caller writing `client.lifecycle(...).catch(...)` would otherwise miss it entirely.
  async lifecycle(kind: Kind, id: string, action: "start" | "stop"): Promise<unknown> {
    if (!RUNNABLE.includes(kind)) {
      throw new SfError(`'${kind}' has no ${action} action (only ${RUNNABLE.join(", ")} do)`);
    }
    return this.request("POST", `/api/${kind}/${encodeURIComponent(id)}/${action}`);
  }

  metrics(kind: Kind, id: string): Promise<unknown> {
    if (kind === "sources") {
      return this.request("GET", `/api/sources/${encodeURIComponent(id)}/status`);
    }
    return this.request("GET", `/api/${kind}/${encodeURIComponent(id)}/metrics`);
  }

  /** Compiles SQL without creating anything — the check to run before committing to a definition. */
  validate(kind: "pipelines" | "tables", sql: string): Promise<unknown> {
    return this.request("POST", `/api/${kind}/validate`, { body: { sql } });
  }

  rows(id: string, limit = 100, csv = false): Promise<unknown> {
    const path = csv
      ? `/api/tables/${encodeURIComponent(id)}/rows.csv?limit=${limit}`
      : `/api/tables/${encodeURIComponent(id)}/rows?limit=${limit}`;
    return this.request("GET", path, { raw: csv });
  }

  results(id: string, limit = 100, csv = false): Promise<unknown> {
    const path = csv
      ? `/api/pipelines/${encodeURIComponent(id)}/results.csv?limit=${limit}`
      : `/api/pipelines/${encodeURIComponent(id)}/results?limit=${limit}`;
    return this.request("GET", path, { raw: csv });
  }

  exportConfig(format: "json" | "yaml" = "json", includeSecrets = false): Promise<string> {
    return this.request<string>(
      "GET",
      `/api/config/export?format=${format}&includeSecrets=${includeSecrets}`,
      { raw: true },
    );
  }

  importConfig(document: string, mode: "validate" | "merge" | "replace" = "validate"): Promise<unknown> {
    const contentType = document.trimStart().startsWith("{") || document.trimStart().startsWith("[")
      ? "application/json"
      : "application/yaml";
    return this.request("POST", `/api/config/import?mode=${mode}`, { body: document, contentType });
  }
}

async function errorText(res: Response): Promise<string> {
  const text = await res.text().catch(() => "");
  try {
    const parsed = JSON.parse(text) as Record<string, unknown>;
    for (const key of ["error", "message", "title"]) {
      if (typeof parsed[key] === "string") return parsed[key] as string;
    }
  } catch {
    // not JSON — the raw text is the best message available.
  }
  return text.slice(0, 500) || res.statusText;
}
