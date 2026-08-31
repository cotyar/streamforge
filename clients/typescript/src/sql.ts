/**
 * Ad-hoc SQL: validate (creates nothing) -> POST /api/config/import?mode=merge (create-or-update)
 * -> LiveTable. Two-stage flow, the `adhoc_` name slug and the drop-outside-the-namespace refusal
 * are ported from otc-terms' lib/streamsforge/adhoc.ts (runAdhocQuery/adhocTableName/
 * dropAdhocTable) via clients/python/src/streamsforge/sql.py. Always REST: there is no gRPC RPC
 * for config import.
 */

import { StreamsForgeError, SqlError, type SqlDiagnostic } from "./errors.js";
import type { RestClient } from "./http.js";
import * as tablesModule from "./tables.js";
import type { ConfigImportReport, TableDefinitionDto, TableValidateResponse } from "./types.js";

export const ADHOC_PREFIX = "adhoc_";

/** `Exposure vs Ostrava!` -> `adhoc_exposure_vs_ostrava`. Already-prefixed names pass through, so
 * re-running an edited query updates the same table. */
export function adhocTableName(raw: string): string {
  let slug = raw.trim().toLowerCase();
  slug = slug.replace(/^adhoc_/, "");
  slug = slug.replace(/[^a-z0-9]+/g, "_");
  slug = slug.replace(/^_+|_+$/g, "").slice(0, 48);
  return `${ADHOC_PREFIX}${slug || "scratch_1"}`;
}

export async function validate(http: RestClient, sqlText: string): Promise<TableValidateResponse> {
  const res = await http.post("/api/tables/validate", { body: { sql: sqlText } });
  const body = res.headers.get("content-length") === "0" ? {} : ((await res.json().catch(() => ({}))) as Partial<TableValidateResponse>);
  if (!res.ok || typeof body !== "object" || body === null) {
    throw new SqlError(`validate failed: ${res.status}`, [], sqlText);
  }
  return body as TableValidateResponse;
}

function diagnosticsError(sqlText: string, diagnostics: SqlDiagnostic[]): SqlError {
  const message = diagnostics[0]?.message ?? "SQL rejected";
  return new SqlError(message, diagnostics, sqlText);
}

export interface RunSqlDeps {
  http: RestClient;
  table: (name: string, opts?: { key?: string[]; timeoutMs?: number; flushMs?: number }) => Promise<unknown>;
}

export async function run(
  deps: RunSqlDeps,
  name: string,
  sqlText: string,
  key: string[] | undefined,
  timeoutMs: number,
  flushMs?: number,
): Promise<unknown> {
  const tableName = adhocTableName(name);
  const validated = await validate(deps.http, sqlText);
  if (!validated.ok) {
    throw diagnosticsError(
      sqlText,
      validated.diagnostics.map((d) => ({ message: d.message, line: d.line, column: d.column, severity: d.severity })),
    );
  }

  const res = await deps.http.post("/api/config/import", {
    params: { mode: "merge" },
    body: {
      version: 1,
      sources: [],
      pipelines: [],
      tables: [{ name: tableName, description: "Ad-hoc query from the TypeScript client", sql: sqlText, running: true }],
    },
  });
  const body = (res.headers.get("content-length") === "0" ? {} : await res.json().catch(() => ({}))) as Partial<ConfigImportReport>;
  const entries = body.entries ?? [];
  const errored = entries.filter((e) => e.action === "error");
  if (!res.ok || errored.length > 0) {
    const diagnostics: SqlDiagnostic[] = [];
    for (const e of errored) {
      const messages = e.diagnostics.length > 0 ? e.diagnostics : [`import rejected '${e.name}'`];
      for (const m of messages) diagnostics.push({ message: m, line: 0, column: 0, severity: "Error" });
    }
    throw diagnosticsError(sqlText, diagnostics);
  }

  return deps.table(tableName, { key, timeoutMs, flushMs });
}

export async function listAdhoc(http: RestClient): Promise<TableDefinitionDto[]> {
  const all = await tablesModule.listTables(http);
  return all
    .filter((t) => t.name.startsWith(ADHOC_PREFIX))
    .sort((a, b) => (Number(b.updatedAtMs ?? 0) as number) - (Number(a.updatedAtMs ?? 0) as number));
}

export async function dropAdhoc(http: RestClient, name: string): Promise<boolean> {
  if (!name.startsWith(ADHOC_PREFIX)) {
    throw new StreamsForgeError(`refusing to drop non-ad-hoc table '${name}'`);
  }
  const match = await tablesModule.getTable(http, name);
  if (match === null) return false;
  const res = await http.delete(`/api/tables/${encodeURIComponent(match.id)}`);
  if (res.status === 404) return false;
  if (res.status !== 200 && res.status !== 204) {
    throw new StreamsForgeError(`drop '${name}' failed: ${res.status} ${await res.text()}`);
  }
  return true;
}
