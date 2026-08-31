"""Error types the client raises on purpose.

Everything below subclasses StreamsForgeError, so `except StreamsForgeError` catches the whole
family. Transport/network errors that are NOT one of these (a raw grpc.RpcError, an httpx
exception) are allowed to propagate as-is rather than being wrapped and losing information.
"""

from __future__ import annotations


class StreamsForgeError(Exception):
    """Base class for every error this client raises intentionally."""


class AuthError(StreamsForgeError):
    """Login failed, or a 401 survived the one-shot re-mint.

    Mirrors sfFetch in lib/streamsforge/server.ts: the cached token is discarded and re-minted
    exactly once on a 401; if the retry also 401s, this is raised rather than looping.
    """


class NotReady(StreamsForgeError):
    """A LiveTable did not fill, or a wait_for() predicate never matched, within its timeout.

    The common cause named explicitly because it is easy to misdiagnose as a bug: a brand-new
    table gets no backfill, so subscribing to one nobody has pushed to yet blocks until data
    arrives or this fires.
    """


class IngestRejected(StreamsForgeError):
    """An ingest push was not accepted (non-202 REST, or a non-ACCEPTED gRPC outcome).

    `.row_errors` carries the per-row reasons the server gave, when it gave any.
    """

    def __init__(self, message: str, row_errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.row_errors: list[str] = row_errors or []


class SqlError(StreamsForgeError):
    """A SQL statement failed `validate` or the `config/import` create step.

    `.diagnostics` is the engine's own `{message, line, column, severity}` list, verbatim.
    `str(err)` renders the first diagnostic against `.sql` with a caret under the offending
    column — the same "engine explaining itself" the /sql page's editor shows, ported rather
    than flattened into a plain message (design doc §2).
    """

    def __init__(self, message: str, diagnostics: list[dict], sql: str | None = None) -> None:
        super().__init__(message)
        self.diagnostics: list[dict] = diagnostics
        self.sql = sql

    def __str__(self) -> str:
        base = super().__str__()
        if not self.diagnostics:
            return base
        d = self.diagnostics[0]
        message = d.get("message", base)
        line_no = d.get("line") or 0
        column = d.get("column") or 0
        if not self.sql or not line_no:
            return f"{message} (line {line_no}, column {column})"
        lines = self.sql.splitlines()
        if line_no < 1 or line_no > len(lines):
            return f"{message} (line {line_no}, column {column})"
        source_line = lines[line_no - 1]
        caret = " " * max(column - 1, 0) + "^"
        return f"{message}\n{source_line}\n{caret}"
