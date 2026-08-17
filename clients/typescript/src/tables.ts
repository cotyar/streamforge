/**
 * Table catalog reads: list/get/search/history, and snapshotRows() -- a one-shot REST read with
 * no subscription, dropping weight<=0 rows exactly like readTableRows in otc-terms'
 * lib/streamforge/server.ts. Always REST regardless of which live transport a Client picked --
 * these are cheap, infrequent calls (ported from clients/python/src/streamforge/tables.py).
 */

import { StreamForgeError } from "./errors.js";
import type { RestClient } from "./http.js";
import type { Delta, Row } from "./zset.js";
import type { TableDefinitionDto, TableRowDto, TableRowsResponse, TableSearchResponse } from "./types.js";

export async function listTables(http: RestClient): Promise<TableDefinitionDto[]> {
  const res = await http.get("/api/tables");
  if (!res.ok) throw new StreamForgeError(`GET /api/tables failed: ${res.status} ${await res.text()}`);
  return (await res.json()) as TableDefinitionDto[];
}

export async function getTable(http: RestClient, nameOrId: string): Promise<TableDefinitionDto | null> {
  for (const t of await listTables(http)) {
    if (t.name === nameOrId || t.id === nameOrId) return t;
  }
  return null;
}

export async function resolveTableId(http: RestClient, name: string): Promise<string> {
  const t = await getTable(http, name);
  if (t === null) throw new StreamForgeError(`no such table '${name}'`);
  return t.id;
}

/** Wishlist #18: this table's row-identity key, read from its own definition (`GET
 * /api/tables`'s `keyFields`) instead of a hand-maintained map. TableDefinitionDto.keyFields is a
 * three-way answer -- a non-empty array is the resolved GROUP BY/LATEST BY key, `[]` is an
 * unkeyed global aggregate (one row, one group), and `null` is whole-row identity -- see
 * Models.cs's `TableDefinition.KeyFields` doc comment for the full contract.
 *
 * An unknown table and an engine build that doesn't report the field at all (predates wishlist
 * #18) both come back here as `null`, which zset.ts's `groupKeyOf`/`ZSet` already treats as
 * whole-row identity -- the exact fallback an unmapped name got from the old, now-deleted
 * `KEY_FIELDS` map, so an old engine keeps working exactly as it did before this change.
 * `Client.table()`'s `key` option bypasses this entirely and always wins. */
export async function resolveKeyFields(http: RestClient, name: string): Promise<string[] | null> {
  const t = await getTable(http, name);
  if (t === null) return null;
  return t.keyFields ?? null;
}

/** Public search: only positively-weighted rows, matching table.search.rows()'s filter elsewhere. */
export async function search(http: RestClient, name: string, query: string, limit = 50): Promise<Row[]> {
  const id = await resolveTableId(http, name);
  const res = await http.get(`/api/tables/${encodeURIComponent(id)}/search`, { params: { q: query, limit } });
  if (!res.ok) throw new StreamForgeError(`search '${name}' failed: ${res.status} ${await res.text()}`);
  const body = (await res.json()) as TableSearchResponse;
  return body.rows.filter((r) => (r.weight ?? 1) > 0).map((r) => r.row);
}

export async function history(http: RestClient, name: string, row: Row, limit?: number): Promise<unknown[]> {
  const id = await resolveTableId(http, name);
  const res = await http.post(`/api/tables/${encodeURIComponent(id)}/history/lookup`, {
    body: { row },
    params: limit !== undefined ? { limit } : undefined,
  });
  if (!res.ok) throw new StreamForgeError(`history '${name}' failed: ${res.status} ${await res.text()}`);
  return (await res.json()) as unknown[];
}

/** Public snapshot: consolidated, positively-weighted rows -- what `client.snapshot(name)` returns. */
export async function snapshotRows(http: RestClient, name: string, limit = 500): Promise<Row[]> {
  const [deltas] = await snapshotDeltas(http, name, limit);
  return deltas.filter(([, weight]) => weight > 0).map(([row]) => row);
}

/**
 * Transport-level snapshot: raw (row, weight) pairs, unfiltered, plus the read's own `seq` --
 * what live-table.ts seeds a ZSet from. ZSet.seed() does the weight<=0 filtering itself, exactly
 * like _grpc.py/_hub.py's snapshot() on the Python side.
 */
export async function snapshotDeltas(http: RestClient, name: string, limit = 500): Promise<readonly [Delta[], number]> {
  const id = await resolveTableId(http, name);
  const res = await http.get(`/api/tables/${encodeURIComponent(id)}/rows`, { params: { limit } });
  if (!res.ok) throw new StreamForgeError(`rows '${name}' failed: ${res.status} ${await res.text()}`);
  const body = (await res.json()) as TableRowsResponse;
  const deltas: Delta[] = body.rows.map((r: TableRowDto) => [r.row, r.weight] as const);
  return [deltas, body.seq] as const;
}
