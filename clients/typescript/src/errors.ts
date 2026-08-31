/**
 * Error types the client raises on purpose. Everything below extends StreamsForgeError, so
 * `err instanceof StreamsForgeError` catches the whole family. Transport/network errors that are
 * NOT one of these (a raw grpc error, a fetch failure) are allowed to propagate as-is rather than
 * being wrapped and losing information -- ported 1:1 from clients/python/src/streamsforge/errors.py.
 */

export class StreamsForgeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StreamsForgeError";
  }
}

/** Login failed, or a 401 survived the one-shot re-mint (see http.ts's RestClient.request). */
export class AuthError extends StreamsForgeError {
  constructor(message: string) {
    super(message);
    this.name = "AuthError";
  }
}

/**
 * A LiveTable did not fill, or a waitFor() predicate never matched, within its timeout. The
 * common cause is named explicitly because it's easy to misdiagnose as a bug: a brand-new table
 * gets no backfill, so subscribing to one nobody has pushed to yet blocks until data arrives or
 * this fires.
 */
export class NotReady extends StreamsForgeError {
  constructor(message: string) {
    super(message);
    this.name = "NotReady";
  }
}

/** An ingest push was not accepted (non-202 REST, or a non-ACCEPTED gRPC outcome). */
export class IngestRejected extends StreamsForgeError {
  readonly rowErrors: string[];
  constructor(message: string, rowErrors: string[] = []) {
    super(message);
    this.name = "IngestRejected";
    this.rowErrors = rowErrors;
  }
}

export interface SqlDiagnostic {
  message: string;
  line: number;
  column: number;
  severity?: string;
}

/**
 * A SQL statement failed `validate` or the `config/import` create step. `.diagnostics` is the
 * engine's own `{message, line, column, severity}` list, verbatim. `.message` renders the first
 * diagnostic against `.sql` with a caret under the offending column -- the same "engine
 * explaining itself" the /sql page's editor shows, ported rather than flattened (design doc §2).
 */
export class SqlError extends StreamsForgeError {
  readonly diagnostics: SqlDiagnostic[];
  readonly sql: string | undefined;

  constructor(message: string, diagnostics: SqlDiagnostic[], sql?: string) {
    super(SqlError._render(message, diagnostics, sql));
    this.name = "SqlError";
    this.diagnostics = diagnostics;
    this.sql = sql;
  }

  private static _render(base: string, diagnostics: SqlDiagnostic[], sql?: string): string {
    const d = diagnostics[0];
    if (!d) return base;
    const message = d.message || base;
    const lineNo = d.line || 0;
    const column = d.column || 0;
    if (!sql || !lineNo) return `${message} (line ${lineNo}, column ${column})`;
    const lines = sql.split("\n");
    const sourceLine = lines[lineNo - 1];
    if (lineNo < 1 || lineNo > lines.length || sourceLine === undefined) {
      return `${message} (line ${lineNo}, column ${column})`;
    }
    const caret = " ".repeat(Math.max(column - 1, 0)) + "^";
    return `${message}\n${sourceLine}\n${caret}`;
  }
}
