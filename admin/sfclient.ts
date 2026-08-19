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

/** Plan 015's `ApprovalState`, verbatim and in the server's own order.
 *
 * Duplicated here for ONE reason: `GET /api/approvals?state=Bogus` is answered with a 400 carrying an
 * EMPTY BODY — ASP.NET's enum binder refuses before any handler runs, so unlike every other refusal on
 * these routes there is no server sentence to relay. A client that just forwarded the string would
 * print nothing at all. Validating here turns that into "unknown state 'Bogus' (expected …)". */
export const APPROVAL_STATES = [
  "Pending",
  "Approved",
  "Rejected",
  "Expired",
  "Executed",
  "Failed",
  "Cancelled",
] as const;
export type ApprovalState = (typeof APPROVAL_STATES)[number];

/** Case-insensitive on the way in, exact on the way out: `pending` is what somebody types. */
export function toApprovalState(raw: string): ApprovalState {
  const match = APPROVAL_STATES.find((s) => s.toLowerCase() === raw.toLowerCase());
  if (!match) {
    throw new SfError(`unknown approval state '${raw}' (expected ${APPROVAL_STATES.join(", ")})`);
  }
  return match;
}

/** The audit day is a STORAGE KEY (a grain key on Orleans, an actor id on Dapr), not a filter — which
 * is why the server validates its shape rather than forwarding it, and why this does too. */
export function assertAuditDay(day: string): string {
  if (!/^\d{8}$/.test(day)) {
    throw new SfError(`'${day}' is not a day; expected yyyyMMdd (UTC), e.g. ${todayUtc()}`);
  }
  return day;
}

export function todayUtc(): string {
  return new Date().toISOString().slice(0, 10).replace(/-/g, "");
}

/** Filters `GET /api/audit/{day}` accepts, and deliberately no more — `actor` is EXACT and `action` is
 * a PREFIX. A day's rows are a stream somebody can point platform SQL at one layer up, which is where
 * a real query language belongs; do not grow a client-side one here. */
export interface AuditQuery {
  actor?: string;
  action?: string;
  limit?: number;
  offset?: number;
  includeChanges?: boolean;
}

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

    // Wrapped like request()'s fetch, and for the same reason: a host that is down here produced a raw
    // Bun stack trace ("ECONNRESET", ten lines) instead of one sentence, on the very first call every
    // env-configured command makes. Found while verifying the plan-015 commands against a wedged host.
    let res: Response;
    try {
      res = await fetch(`${this.url}/api/auth/login`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ username, password: secret }),
      });
    } catch (err) {
      throw new SfError(`cannot reach ${this.url}: ${err instanceof Error ? err.message : String(err)}`);
    }
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

  // ---- plan 015: access, approvals, audit -------------------------------------------------------
  //
  // One method per route, no cleverness: `web/src/api/{access,approvals,audit}.ts` is a working client
  // for the same surface and this is the same shape, so the two can be read against each other.

  /** The whole policy document — roles, groups, user entries, approval templates, version stamp. The
   * server deliberately answers all four lists at once; this is not four calls. */
  accessPolicy(): Promise<unknown> {
    return this.request("GET", "/api/access");
  }

  /** What one user can actually do, flattened the way an authorization decision flattens it.
   *
   * Short-circuits on a DISABLED user and answers everything-empty, which is indistinguishable from
   * "configured with nothing" except through the `disabled` flag — so anything rendering this must
   * print that flag. See sf.ts's `printEffective`. */
  effectivePermissions(username: string): Promise<unknown> {
    return this.request("GET", `/api/access/effective/${encodeURIComponent(username)}`);
  }

  upsertRole(name: string, body: unknown): Promise<unknown> {
    return this.request("PUT", `/api/access/roles/${encodeURIComponent(name)}`, { body });
  }

  deleteRole(name: string): Promise<unknown> {
    return this.request("DELETE", `/api/access/roles/${encodeURIComponent(name)}`);
  }

  upsertGroup(name: string, body: unknown): Promise<unknown> {
    return this.request("PUT", `/api/access/groups/${encodeURIComponent(name)}`, { body });
  }

  deleteGroup(name: string): Promise<unknown> {
    return this.request("DELETE", `/api/access/groups/${encodeURIComponent(name)}`);
  }

  upsertUserAccess(username: string, body: unknown): Promise<unknown> {
    return this.request("PUT", `/api/access/users/${encodeURIComponent(username)}`, { body });
  }

  deleteUserAccess(username: string): Promise<unknown> {
    return this.request("DELETE", `/api/access/users/${encodeURIComponent(username)}`);
  }

  /** Its own route on purpose, and its own method here for the same reason: disabling a login is done
   * under pressure, and a caller that had to send the whole entry to flip one boolean would sooner or
   * later send it without the grants it did not know about. Never fold this into upsertUserAccess. */
  setUserDisabled(username: string, disabled: boolean): Promise<unknown> {
    return this.request("PUT", `/api/access/users/${encodeURIComponent(username)}/disabled`, {
      body: { disabled },
    });
  }

  upsertApprovalTemplate(name: string, body: unknown): Promise<unknown> {
    return this.request("PUT", `/api/access/approval-templates/${encodeURIComponent(name)}`, { body });
  }

  deleteApprovalTemplate(name: string): Promise<unknown> {
    return this.request("DELETE", `/api/access/approval-templates/${encodeURIComponent(name)}`);
  }

  /** The inbox, server-filtered to the administrator, the requester and the entitled approver.
   *
   * ponytail: no paging. The server applies `limit` BEFORE that visibility filter, so a page is "your
   * requests among the most recent N", not "your N most recent" — an `--offset` here would be a lie
   * about what it skipped. Ceiling: raise `--limit` (server caps at 500). Upgrade path is the server's
   * (push the visibility predicate into the store), not this file's. */
  listApprovals(state?: ApprovalState, limit = 100): Promise<unknown[]> {
    const query = new URLSearchParams({ limit: String(limit) });
    if (state) query.set("state", state);
    return this.request<unknown[]>("GET", `/api/approvals?${query}`);
  }

  getApproval(id: string): Promise<unknown> {
    return this.request("GET", `/api/approvals/${encodeURIComponent(id)}`);
  }

  /** File a request for a privileged action. There is no `requestedBy`: the server stamps the
   * authenticated principal, and the "you cannot approve your own request" rule rests on that. */
  fileApproval(body: {
    action: string;
    scope?: string;
    reason?: string;
    payloadJson?: string;
  }): Promise<unknown> {
    return this.request("POST", "/api/approvals", { body: { scope: "*", ...body } });
  }

  /** approve/reject are the same transition with a different verb; cancel is the requester withdrawing.
   * The vote's username and timestamp are server-set — only the comment is the caller's. */
  decideApproval(
    id: string,
    decision: "approve" | "reject" | "cancel",
    comment?: string,
  ): Promise<unknown> {
    return this.request("POST", `/api/approvals/${encodeURIComponent(id)}/${decision}`, {
      body: { comment: comment ?? null },
    });
  }

  /** Which days hold entries. Reads an index and wakes no day shard, which is what makes it the cheap
   * first call before asking for a day. */
  auditDays(): Promise<string[]> {
    return this.request<string[]>("GET", "/api/audit/days");
  }

  // `async` for the same reason `lifecycle` is: assertAuditDay must REJECT rather than throw
  // synchronously out of a Promise-returning method, or a caller writing `.catch(...)` misses it.
  async auditDay(day: string, query: AuditQuery = {}): Promise<unknown> {
    const params = new URLSearchParams();
    if (query.actor) params.set("actor", query.actor);
    if (query.action) params.set("action", query.action);
    if (query.limit !== undefined) params.set("limit", String(query.limit));
    if (query.offset !== undefined) params.set("offset", String(query.offset));
    if (query.includeChanges) params.set("includeChanges", "true");
    const suffix = params.toString() ? `?${params}` : "";
    return this.request("GET", `/api/audit/${encodeURIComponent(assertAuditDay(day))}${suffix}`);
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
