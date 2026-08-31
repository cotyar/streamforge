package streamsforge

/** Base of every error this client raises on purpose -- transport/network errors that are NOT
 * one of these (a raw io.grpc.StatusException, an IOException) propagate as-is instead of being
 * wrapped and losing information, same rule as the Python client's errors.py. */
sealed class StreamsForgeException(message: String, cause: Throwable? = null) : Exception(message, cause)

/** Login failed, or a 401 survived the one-shot re-mint (see [AuthClient]: the cached token is
 * discarded and re-minted exactly once on a 401; if the retry also 401s, this is raised rather
 * than looping). */
class AuthException(message: String, cause: Throwable? = null) : StreamsForgeException(message, cause)

/** A [LiveTable] did not fill, or `waitFor` never matched its predicate, within its timeout. The
 * common cause: a brand-new table gets no backfill, so subscribing to one nobody has pushed to
 * yet blocks until data arrives or this fires. */
class NotReadyException(message: String) : StreamsForgeException(message)

/** An ingest push was not accepted (non-202 REST, or a non-ACCEPTED gRPC outcome). [rowErrors]
 * carries the per-row reasons the server gave, when it gave any. */
class IngestRejectedException(message: String, val rowErrors: List<String> = emptyList()) :
    StreamsForgeException(message)

/** Catch-all for everything that doesn't warrant its own subtype (no such table, refused
 * `dropAdhoc` outside the `adhoc_` namespace, an unreachable transport). */
class StreamsForgeError(message: String, cause: Throwable? = null) : StreamsForgeException(message, cause)

/** One entry of a `/api/tables/validate` (or gRPC `TableService.Validate`) response. */
data class Diagnostic(val message: String, val line: Int, val column: Int, val severity: String)

/**
 * A SQL statement failed `validate` or the `config/import` create step. [diagnostics] is the
 * engine's own `{message, line, column, severity}` list, verbatim. The rendered message is the
 * first diagnostic against [sqlText] with a caret under the offending column -- the engine
 * explaining itself is the good part of `/sql` and must survive the port (design doc §2).
 */
class SqlException(rawMessage: String, val diagnostics: List<Diagnostic>, val sqlText: String? = null) :
    StreamsForgeException(renderSqlMessage(rawMessage, diagnostics, sqlText))

private fun renderSqlMessage(rawMessage: String, diagnostics: List<Diagnostic>, sqlText: String?): String {
    val d = diagnostics.firstOrNull() ?: return rawMessage
    val message = d.message.ifBlank { rawMessage }
    if (sqlText == null || d.line < 1) return "$message (line ${d.line}, column ${d.column})"
    val lines = sqlText.lines()
    if (d.line > lines.size) return "$message (line ${d.line}, column ${d.column})"
    val sourceLine = lines[d.line - 1]
    val caret = " ".repeat(maxOf(d.column - 1, 0)) + "^"
    return "$message\n$sourceLine\n$caret"
}
