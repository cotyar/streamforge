/**
 * Minimal REST DTOs this client actually uses -- a deliberate subset of web/src/api/types.ts (917
 * lines covering the whole console), not a re-export of it: this package must stand alone as a
 * publishable artifact, and the console's admin/config-editor surface (sinks, sharding, transport
 * probes, etc.) is out of scope for a live-table + ingest + ad-hoc-SQL client.
 */

import type { Row } from "./zset.js";

export type Role = "Admin" | "Editor" | "Viewer";

export interface LoginResponse {
  token: string;
  username: string;
  displayName: string;
  role: Role;
}

export interface TableRowDto {
  row: Row;
  weight: number;
}

export interface TableRowsResponse {
  rows: TableRowDto[];
  totalRows: number;
  seq: number;
}

export interface TableSearchResponse {
  rows: TableRowDto[];
  mode: string;
  enabled: boolean;
  total: number;
}

export interface TableDefinitionDto {
  id: string;
  name: string;
  /** Wishlist #18: this table's row-identity key, recomputed on every successful compile --
   * server-owned, never client-writable. A non-empty array is the resolved GROUP BY/LATEST BY key
   * columns; `[]` is an unkeyed GLOBAL AGGREGATE (exactly one row, one group); `null` is WHOLE-ROW
   * identity (no supersession key applies). Absent entirely on an engine build older than
   * wishlist #18 -- `[key: string]: unknown` below means a missing property and an explicit
   * `null` both type-check as `undefined` through plain property access, so code that must tell
   * them apart (this package's own `tables.ts#resolveKeyFields`) uses `"keyFields" in def`
   * instead of `def.keyFields === undefined`. See Models.cs's doc comment on `KeyFields` for the
   * full three-state contract. */
  keyFields?: string[] | null;
  [key: string]: unknown;
}

export interface SqlDiagnosticDto {
  message: string;
  line: number;
  column: number;
  severity?: string;
}

export interface TableValidateResponse {
  ok: boolean;
  diagnostics: SqlDiagnosticDto[];
  planSummary: string | null;
  streamInputs: string[];
  tableInputs: string[];
  outputSchema: { name: string; kind: string }[];
}

export interface ConfigImportReportEntry {
  kind: "source" | "pipeline" | "table";
  name: string;
  action: "created" | "updated" | "deleted" | "skipped" | "error";
  diagnostics: string[];
}

export interface ConfigImportReport {
  mode: "validate" | "merge" | "replace";
  entries: ConfigImportReportEntry[];
  ok: boolean;
}

export interface IngestAcceptedResponse {
  accepted: number;
  dropped: number;
  invalid: number;
  depthRows: number;
  capacityRows: number;
  duplicate?: number;
  replayed?: boolean;
}

export interface IngestErrorResponse {
  error: string;
  retryAfterMs: number;
  rowErrors: string[];
}
