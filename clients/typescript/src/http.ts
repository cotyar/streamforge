/**
 * fetch-based REST client with cached, self-refreshing StreamForge auth.
 *
 * Ported from clients/python/src/streamforge/_http.py (itself a port of otc-terms'
 * lib/streamforge/server.ts sfFetch): the JWT is cached in memory for ~11h (the server issues
 * 12h tokens) and re-minted exactly once on any 401, then the request is retried once with the
 * fresh token -- if THAT also 401s, we throw rather than looping forever (a StreamForge restart
 * invalidates every token minted before it, which is a normal event, but an auth system that is
 * actually broken should fail loudly, not spin).
 */

import { AuthError } from "./errors.js";

const TOKEN_LIFETIME_MS = 11 * 60 * 60 * 1000; // server mints 12h tokens; refresh a bit early

export interface RestClientOptions {
  baseUrl: string;
  user?: string;
  password?: string;
  token?: string;
  /** false = accept self-signed/invalid TLS certs (local dev with a portless cert). */
  verify?: boolean;
}

export class RestClient {
  readonly baseUrl: string;
  private readonly user: string | undefined;
  private readonly password: string | undefined;
  private readonly verify: boolean;
  private token_: string | null;
  private tokenMintedAtMs: number | null;
  private loginInFlight: Promise<void> | null = null;

  constructor(opts: RestClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
    this.user = opts.user;
    this.password = opts.password;
    this.verify = opts.verify ?? true;
    this.token_ = opts.token ?? null;
    this.tokenMintedAtMs = opts.token ? Date.now() : null;
  }

  private fetchInit(): RequestInit {
    // Bun's fetch accepts a `tls` extension for per-request cert verification; there is no
    // portable standard fetch option for this. verify=false is spelled out by the caller
    // (connect()'s own doc comment) rather than silently defaulted, matching the design doc's
    // "a client that quietly stops verifying TLS is not a habit worth teaching" (§4).
    return this.verify ? {} : ({ tls: { rejectUnauthorized: false } } as RequestInit);
  }

  async token(): Promise<string> {
    if (this.token_ === null || this.expired()) {
      await this.login();
    }
    if (this.token_ === null) throw new AuthError("StreamForge login did not return a token");
    return this.token_;
  }

  private expired(): boolean {
    return this.tokenMintedAtMs === null || Date.now() - this.tokenMintedAtMs > TOKEN_LIFETIME_MS;
  }

  private async login(): Promise<void> {
    // Coalesce concurrent callers into one login POST rather than one per stalled request.
    if (this.loginInFlight) return this.loginInFlight;
    this.loginInFlight = this.loginNow().finally(() => {
      this.loginInFlight = null;
    });
    return this.loginInFlight;
  }

  private async loginNow(): Promise<void> {
    if (!this.user || !this.password) {
      throw new AuthError(
        "no StreamForge credentials configured -- pass user=/password= to connect(), or set " +
          "STREAMFORGE_ADMIN_USER/STREAMFORGE_ADMIN_PASS",
      );
    }
    const res = await fetch(`${this.baseUrl}/api/auth/login`, {
      ...this.fetchInit(),
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username: this.user, password: this.password }),
    });
    if (!res.ok) {
      throw new AuthError(`StreamForge login failed: ${res.status} ${await res.text()}`);
    }
    const body = (await res.json()) as { token: string };
    this.token_ = body.token;
    this.tokenMintedAtMs = Date.now();
  }

  invalidateToken(): void {
    this.token_ = null;
  }

  /** `auth: false` skips minting/attaching a Bearer token entirely -- used for the ingest path
   * when only an ingest key is configured, so a caller that only feeds a source never forces an
   * admin login (design doc §4). */
  async request(
    method: string,
    path: string,
    init: { body?: unknown; headers?: Record<string, string>; auth?: boolean; params?: Record<string, string | number | boolean | undefined> } = {},
  ): Promise<Response> {
    const { body, headers: extraHeaders, auth = true, params } = init;
    const url = new URL(`${this.baseUrl}${path}`);
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        if (v !== undefined) url.searchParams.set(k, String(v));
      }
    }
    const headers: Record<string, string> = { accept: "application/json", ...extraHeaders };
    const requestInit: RequestInit = {
      ...this.fetchInit(),
      method,
      headers,
    };
    if (body !== undefined) {
      headers["content-type"] = "application/json";
      requestInit.body = JSON.stringify(body);
    }

    if (!auth) {
      return fetch(url, requestInit);
    }

    headers.authorization = `Bearer ${await this.token()}`;
    let res = await fetch(url, requestInit);
    if (res.status === 401) {
      this.invalidateToken();
      headers.authorization = `Bearer ${await this.token()}`;
      res = await fetch(url, requestInit);
      if (res.status === 401) {
        throw new AuthError(`StreamForge rejected the re-minted token for ${method} ${path}`);
      }
    }
    return res;
  }

  get(path: string, init?: Parameters<RestClient["request"]>[2]): Promise<Response> {
    return this.request("GET", path, init);
  }

  post(path: string, init?: Parameters<RestClient["request"]>[2]): Promise<Response> {
    return this.request("POST", path, init);
  }

  delete(path: string, init?: Parameters<RestClient["request"]>[2]): Promise<Response> {
    return this.request("DELETE", path, init);
  }

  async close(): Promise<void> {
    // Nothing to release: fetch has no persistent connection object to close.
  }
}
