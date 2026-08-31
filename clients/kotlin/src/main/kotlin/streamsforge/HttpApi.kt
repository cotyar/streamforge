package streamsforge
import java.io.Closeable

import com.google.gson.Gson
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private val TOKEN_LIFETIME: Duration = Duration.ofHours(11) // server mints 12h tokens; refresh a bit early

/**
 * REST client with cached, self-refreshing StreamsForge auth. Ported from `lib/streamsforge/
 * server.ts`'s `sfFetch` (via the Python client's `_http.py`): the JWT is cached in memory for
 * ~11h and re-minted exactly once on any 401, then the request is retried once with the fresh
 * token -- if THAT also 401s, this raises rather than looping forever (a StreamsForge restart
 * invalidates every token minted before it, a normal event, but an auth system that's actually
 * broken should fail loudly, not spin).
 *
 * Shared between REST and gRPC ([GrpcTransport] takes `AuthClient::token` as its token provider)
 * so both transports mint/refresh the exact same JWT.
 */
class AuthClient(
    baseUrl: String,
    private val user: String?,
    private val password: String?,
    token: String? = null,
) : Closeable {
    val baseUrl: String = baseUrl.trimEnd('/')
    internal val gson = Gson()

    private val client: HttpClient = HttpClient.newHttpClient()
    private val lock = ReentrantLock()
    private var cachedToken: String? = token
    private var tokenMintedAt: Instant? = if (token != null) Instant.now() else null

    override fun close() = Unit

    suspend fun token(): String = withContext(Dispatchers.IO) {
        lock.withLock {
            if (cachedToken == null || expired()) loginLocked()
            cachedToken!!
        }
    }

    fun invalidateToken() = lock.withLock { cachedToken = null }

    private fun expired(): Boolean {
        val mintedAt = tokenMintedAt ?: return true
        return Duration.between(mintedAt, Instant.now()) > TOKEN_LIFETIME
    }

    private fun loginLocked() {
        if (user.isNullOrBlank() || password.isNullOrBlank()) {
            throw AuthException(
                "no StreamsForge credentials configured -- pass user=/password= to StreamsForge.connect()"
            )
        }
        val body = gson.toJson(mapOf("username" to user, "password" to password))
        val request = HttpRequest.newBuilder(URI.create("$baseUrl/api/auth/login"))
            .header("content-type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 200) {
            throw AuthException("StreamsForge login failed: ${response.statusCode()} ${response.body()}")
        }
        val json = gson.fromJson(response.body(), com.google.gson.JsonObject::class.java)
        cachedToken = json.get("token")?.asString
            ?: throw AuthException("StreamsForge login response had no 'token' field")
        tokenMintedAt = Instant.now()
    }

    /** `auth=false` skips minting/attaching a Bearer token entirely -- used for the ingest path
     * when only an ingest key is configured, so a caller that only feeds a source never forces an
     * admin login (design doc §4). */
    suspend fun request(
        method: String,
        path: String,
        query: Map<String, String?> = emptyMap(),
        body: String? = null,
        headers: Map<String, String> = emptyMap(),
        auth: Boolean = true,
    ): HttpResponse<String> = withContext(Dispatchers.IO) {
        val uri = buildUri(path, query)
        fun build(bearer: String?): HttpRequest {
            val builder = HttpRequest.newBuilder(uri)
            headers.forEach { (k, v) -> builder.header(k, v) }
            if (bearer != null) builder.header("authorization", "Bearer $bearer")
            if (body != null) builder.header("content-type", "application/json")
            val publisher = if (body != null) HttpRequest.BodyPublishers.ofString(body) else HttpRequest.BodyPublishers.noBody()
            builder.method(method, publisher)
            return builder.build()
        }
        if (!auth) {
            return@withContext client.send(build(null), HttpResponse.BodyHandlers.ofString())
        }
        var response = client.send(build(token()), HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() == 401) {
            invalidateToken()
            response = client.send(build(token()), HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 401) {
                throw AuthException("StreamsForge rejected the re-minted token for $method $path")
            }
        }
        response
    }

    suspend fun get(path: String, query: Map<String, String?> = emptyMap()): HttpResponse<String> =
        request("GET", path, query = query)

    suspend fun post(
        path: String,
        body: Any? = null,
        query: Map<String, String?> = emptyMap(),
        auth: Boolean = true,
        headers: Map<String, String> = emptyMap(),
    ): HttpResponse<String> = request("POST", path, query = query, body = body?.let { gson.toJson(it) }, headers = headers, auth = auth)

    suspend fun delete(path: String): HttpResponse<String> = request("DELETE", path)

    private fun buildUri(path: String, query: Map<String, String?>): URI {
        val qs = query.entries.filter { it.value != null }
            .joinToString("&") { (k, v) -> "${URLEncoder.encode(k, Charsets.UTF_8)}=${URLEncoder.encode(v!!, Charsets.UTF_8)}" }
        return URI.create(if (qs.isEmpty()) "$baseUrl$path" else "$baseUrl$path?$qs")
    }
}
