package streamforge

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.reflect.TypeToken

/**
 * Table catalog reads (list/search/validate/config-import/adhoc) and REST ingest -- always REST
 * regardless of which live [TableTransport] a client picked. Ported from the Python client's
 * `tables.py`/`sql.py`/`ingest.py`: these are cheap, infrequent calls, and one code path here is
 * simpler than duplicating them for gRPC (design tradeoff, not a limitation -- `TableService.
 * List/Rows/Search/Validate` exist on the gRPC side too, for a client that wants everything on
 * one channel; [GrpcTransport] uses them for the LIVE snapshot/subscribe path specifically).
 */
internal object RestCatalog {
    const val ADHOC_PREFIX = "adhoc_"

    private val mapType = object : TypeToken<Map<String, Any?>>() {}.type
    private val listOfMapType = object : TypeToken<List<Map<String, Any?>>>() {}.type

    private fun checkOk(response: java.net.http.HttpResponse<String>, what: String) {
        if (response.statusCode() >= 400) {
            throw StreamForgeError("$what failed: ${response.statusCode()} ${response.body()}")
        }
    }

    private fun AuthClient.parseListOfMaps(json: String): List<Map<String, Any?>> =
        if (json.isBlank()) emptyList() else gson.fromJson(json, listOfMapType) ?: emptyList()

    private fun AuthClient.parseRow(el: JsonElement): Row = gson.fromJson(el, mapType)

    private fun JsonElement.toDiagnostic(): Diagnostic {
        val o = asJsonObject
        return Diagnostic(
            message = o.get("message")?.takeIf { !it.isJsonNull }?.asString ?: "",
            line = o.get("line")?.takeIf { !it.isJsonNull }?.asInt ?: 0,
            column = o.get("column")?.takeIf { !it.isJsonNull }?.asInt ?: 0,
            severity = o.get("severity")?.takeIf { !it.isJsonNull }?.asString ?: "Error",
        )
    }

    suspend fun listTables(http: AuthClient): List<Map<String, Any?>> {
        val resp = http.get("/api/tables")
        checkOk(resp, "list tables")
        return http.parseListOfMaps(resp.body())
    }

    suspend fun resolveTableId(http: AuthClient, name: String): String {
        val table = listTables(http).firstOrNull { it["name"] == name }
            ?: throw StreamForgeError("no such table '$name'")
        return table["id"] as? String ?: throw StreamForgeError("table '$name' has no id")
    }

    /** One-shot snapshot read, dropping weight<=0 rows -- same rule `readTableRows` in
     * `lib/streamforge/server.ts` applies. This backs [SignalRTransport]'s [TableTransport.
     * snapshot] AND the public `StreamForgeClient.snapshot()` one-shot read. */
    suspend fun snapshotRows(http: AuthClient, id: String, limit: Int): Pair<List<RowDelta>, Long> {
        val resp = http.get("/api/tables/$id/rows", mapOf("limit" to limit.toString()))
        checkOk(resp, "snapshot")
        val body = JsonParser.parseString(resp.body()).asJsonObject
        val rows = (body.getAsJsonArray("rows") ?: JsonArray()).map { el ->
            val o = el.asJsonObject
            RowDelta(http.parseRow(o.get("row")), o.get("weight")?.asLong ?: 1L)
        }
        return rows to (body.get("seq")?.asLong ?: 0L)
    }

    suspend fun search(http: AuthClient, name: String, query: String, limit: Int): List<Row> {
        val id = resolveTableId(http, name)
        val resp = http.get("/api/tables/$id/search", mapOf("q" to query, "limit" to limit.toString()))
        checkOk(resp, "search '$name'")
        val body = JsonParser.parseString(resp.body()).asJsonObject
        return (body.getAsJsonArray("rows") ?: JsonArray()).mapNotNull { el ->
            val o = el.asJsonObject
            val weight = o.get("weight")?.asLong ?: 1L
            if (weight > 0) http.parseRow(o.get("row")) else null
        }
    }

    suspend fun validate(http: AuthClient, sqlText: String): ValidateResult {
        val resp = http.post("/api/tables/validate", mapOf("sql" to sqlText))
        val body = if (resp.body().isNotBlank()) JsonParser.parseString(resp.body()).asJsonObject else JsonObject()
        if (resp.statusCode() >= 400 || !body.has("ok")) {
            throw SqlException("validate failed: ${resp.statusCode()}", emptyList(), sqlText)
        }
        return ValidateResult(
            ok = body.get("ok")?.takeIf { !it.isJsonNull }?.asBoolean ?: false,
            diagnostics = (body.getAsJsonArray("diagnostics") ?: JsonArray()).map { it.toDiagnostic() },
            planSummary = body.get("planSummary")?.takeIf { !it.isJsonNull }?.asString,
        )
    }

    /** `Exposure vs Ostrava!` -> `adhoc_exposure_vs_ostrava`. Already-prefixed names pass
     * through, so re-running an edited query updates the same table (ported from `lib/
     * streamforge/adhoc.ts`'s `adhocTableName`). */
    fun adhocTableName(raw: String): String {
        var slug = raw.trim().lowercase().removePrefix(ADHOC_PREFIX)
        slug = slug.replace(Regex("[^a-z0-9]+"), "_").trim('_').take(48)
        return "$ADHOC_PREFIX${slug.ifEmpty { "scratch_1" }}"
    }

    /** validate (creates nothing) -> `POST /api/config/import?mode=merge` (create-or-update).
     * Always REST: there is no gRPC RPC for config import. Returns the resolved adhoc table name
     * for the caller to subscribe to. */
    suspend fun runAdhocSql(http: AuthClient, name: String, sqlText: String): String {
        val tableName = adhocTableName(name)
        val validated = validate(http, sqlText)
        if (!validated.ok) {
            throw SqlException(validated.diagnostics.firstOrNull()?.message ?: "SQL rejected", validated.diagnostics, sqlText)
        }

        val doc = mapOf(
            "version" to 1,
            "sources" to emptyList<Any>(),
            "pipelines" to emptyList<Any>(),
            "tables" to listOf(
                mapOf(
                    "name" to tableName,
                    "description" to "Ad-hoc query from the Kotlin client",
                    "sql" to sqlText,
                    "running" to true,
                )
            ),
        )
        val resp = http.post("/api/config/import", doc, query = mapOf("mode" to "merge"))
        val body = if (resp.body().isNotBlank()) JsonParser.parseString(resp.body()).asJsonObject else JsonObject()
        val entries = body.getAsJsonArray("entries") ?: JsonArray()
        val errored = entries.filter { it.asJsonObject.get("action")?.asString == "error" }
        if (resp.statusCode() >= 400 || errored.isNotEmpty()) {
            val diagnostics = errored.flatMap { e ->
                val obj = e.asJsonObject
                val messages = obj.getAsJsonArray("diagnostics")?.map { it.asString }
                    ?: listOf("import rejected '${obj.get("name")?.asString}'")
                messages.map { Diagnostic(it, 0, 0, "Error") }
            }
            throw SqlException(diagnostics.firstOrNull()?.message ?: "SQL rejected", diagnostics, sqlText)
        }
        return tableName
    }

    suspend fun listAdhoc(http: AuthClient): List<Map<String, Any?>> =
        listTables(http)
            .filter { (it["name"] as? String)?.startsWith(ADHOC_PREFIX) == true }
            .sortedByDescending { (it["updatedAtMs"] as? Double) ?: 0.0 }

    /** Refuses anything outside the `adhoc_` namespace, so a caller cannot drop `fund_exposure`. */
    suspend fun dropAdhoc(http: AuthClient, name: String): Boolean {
        if (!name.startsWith(ADHOC_PREFIX)) throw StreamForgeError("refusing to drop non-ad-hoc table '$name'")
        val match = listTables(http).firstOrNull { it["name"] == name } ?: return false
        val id = match["id"] as? String ?: return false
        val resp = http.delete("/api/tables/$id")
        if (resp.statusCode() == 404) return false
        if (resp.statusCode() != 200 && resp.statusCode() != 204) {
            throw StreamForgeError("drop '$name' failed: ${resp.statusCode()} ${resp.body()}")
        }
        return true
    }

    /** Ported from `ingest.py`'s `_push_rest`: prefers `X-SF-Ingest-Key` over the admin JWT
     * whenever one is configured, so a caller that only feeds a source never needs to hold one
     * (design doc §4) -- the route is AllowAnonymous with its own header check. */
    suspend fun push(
        http: AuthClient,
        source: String,
        rows: List<Row>,
        idempotencyKey: String?,
        partial: Boolean,
        ingestKey: String?,
    ): IngestAckResult {
        val useIngestKey = !ingestKey.isNullOrBlank()
        val headers = if (useIngestKey) mapOf("X-SF-Ingest-Key" to ingestKey!!) else emptyMap()
        val body = buildMap<String, Any?> {
            put("events", rows)
            put("partial", partial)
            if (!idempotencyKey.isNullOrBlank()) put("idempotencyKey", idempotencyKey)
        }
        val resp = http.post("/api/sources/$source/events", body, headers = headers, auth = !useIngestKey)
        if (resp.statusCode() != 202) {
            val obj = if (resp.body().isNotBlank()) JsonParser.parseString(resp.body()).asJsonObject else JsonObject()
            val error = obj.get("error")?.takeIf { !it.isJsonNull }?.asString
                ?: "$source ingest push failed: ${resp.statusCode()}"
            val rowErrors = (obj.getAsJsonArray("rowErrors") ?: JsonArray()).map { it.asString }
            throw IngestRejectedException(error, rowErrors)
        }
        val obj = if (resp.body().isNotBlank()) JsonParser.parseString(resp.body()).asJsonObject else JsonObject()
        return IngestAckResult(
            outcome = "INGEST_OUTCOME_ACCEPTED",
            accepted = obj.get("accepted")?.takeIf { !it.isJsonNull }?.asInt ?: 0,
            dropped = obj.get("dropped")?.takeIf { !it.isJsonNull }?.asInt ?: 0,
            invalid = obj.get("invalid")?.takeIf { !it.isJsonNull }?.asInt ?: 0,
            retryAfterMs = obj.get("retryAfterMs")?.takeIf { !it.isJsonNull }?.asInt ?: 0,
            error = "",
            rowErrors = emptyList(),
            duplicate = obj.get("duplicate")?.takeIf { !it.isJsonNull }?.asInt ?: 0,
            replayed = obj.get("replayed")?.takeIf { !it.isJsonNull }?.asBoolean ?: false,
        )
    }
}

data class ValidateResult(val ok: Boolean, val diagnostics: List<Diagnostic>, val planSummary: String?)
