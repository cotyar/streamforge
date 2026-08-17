package streamforge

import com.google.gson.Gson
import com.google.gson.JsonParser
import java.io.File
import java.net.InetSocketAddress
import java.net.Socket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit

/**
 * Boots an ISOLATED StreamForge instance on 9199/9299 (overridable via `SF_TEST_HTTP_PORT`/
 * `SF_TEST_GRPC_PORT` -- three client tasks run concurrently against this same repo and were
 * briefed onto the same default ports before being split up; 9199/9299 are Kotlin's) -- never
 * 5199/5299 (the live dev server) and never 6199 (the shared demo) -- imports a tiny fixture
 * config (one ingest source, one LATEST BY table, one aggregate over that derived LATEST BY), and
 * tears it down after the session. Ported from the Python client's `tests/conftest.py`, including
 * all three of its documented traps:
 *
 * 1. No `--urls` (or the gRPC port is silently never bound -- design doc §3.2).
 * 2. The process must run with its cwd set to the publish directory (`WebApplication.
 *    CreateBuilder` resolves the content root from the CURRENT DIRECTORY, not the assembly's --
 *    run the DLL from anywhere else and `Jwt:Key` is null, so every request 500s including
 *    `/api/healthz`).
 * 3. Its stdout MUST be drained continuously, not just read-on-failure. `redirectErrorStream(true)`
 *    merges stderr into the same pipe, and the OS pipe buffer is finite (64KB on macOS) -- once a
 *    long-running test class has produced that much log output, the engine's next write to it
 *    BLOCKS FOREVER, hanging with no error anywhere. [_Drain] below is a daemon thread doing
 *    nothing but reading that pipe into a bounded ring buffer, so the tail is available for a
 *    failure report without ever letting the pipe fill.
 */
object EngineFixture {
    val HTTP_PORT = System.getenv("SF_TEST_HTTP_PORT")?.toIntOrNull() ?: 9199
    val GRPC_PORT = System.getenv("SF_TEST_GRPC_PORT")?.toIntOrNull() ?: 9299
    private val FORBIDDEN_PORTS = setOf(5199, 5299, 6199)

    const val ADMIN_USER = "admin"
    const val ADMIN_PASS = "admin123!"
    const val SOURCE_NAME = "sf_kotlin_client_trades"
    const val LATEST_TABLE = "sf_kotlin_client_latest_trade"
    const val AGG_TABLE = "sf_kotlin_client_desk_totals"

    val baseUrl = "http://localhost:$HTTP_PORT"
    val grpcTarget = "localhost:$GRPC_PORT"

    private val dotnet = File(System.getProperty("user.home"), ".dotnet/dotnet")
    private val projectDir = File(System.getProperty("user.dir"), "../../orleans/src/StreamForge.Host")

    // A publish takes ~2 minutes; reuse one if it's sitting around (this session's own scratchpad
    // build, or SF_TEST_PUBLISH_DIR) rather than re-publishing every run.
    private val knownPrebuiltDir = File(
        "/private/tmp/claude-501/-Users-yuriyhabarov-work-ac-co-ai-4/c0016c8c-b917-406e-bb3c-19da6fa7173a/scratchpad/sfhost"
    )

    /** Reads a process's merged stdout on a daemon thread, keeping only the tail -- see the
     * class-level doc's point 3. Draining continuously means the tail is available at any moment,
     * not only after the process has already exited. */
    class Drain(process: Process, keepLines: Int = 400) {
        private val lines = ArrayDeque<String>(keepLines)
        private val cap = keepLines

        init {
            val stream = process.inputStream
            Thread({
                stream.bufferedReader().forEachLine { line ->
                    synchronized(lines) {
                        if (lines.size >= cap) lines.removeFirst()
                        lines.addLast(line)
                    }
                }
            }, "sf-engine-log-drain").apply { isDaemon = true }.start()
        }

        fun tail(chars: Int = 6000): String = synchronized(lines) { lines.joinToString("\n") }.takeLast(chars)
    }

    data class Handle(val process: Process, val dataDir: File, val drain: Drain)

    private fun portFree(port: Int): Boolean = try {
        Socket().use { it.connect(InetSocketAddress("127.0.0.1", port), 200) }
        false
    } catch (e: Exception) {
        true
    }

    /** Non-null means "skip, for this reason" -- mirrors conftest.py's `pytest.skip(...)` calls:
     * asserts the ports are free first and refuses to collide with a running instance. */
    fun preconditionsOrSkipReason(): String? {
        if (HTTP_PORT in FORBIDDEN_PORTS || GRPC_PORT in FORBIDDEN_PORTS) {
            return "refusing to configure the contract-test fixture onto a forbidden port"
        }
        if (!portFree(HTTP_PORT) || !portFree(GRPC_PORT)) {
            return "port $HTTP_PORT or $GRPC_PORT is already in use -- refusing to collide with a running instance"
        }
        if (!dotnet.exists()) return "dotnet not found at $dotnet -- cannot boot the contract-test engine"
        if (!knownPrebuiltDir.exists() && !projectDir.isDirectory) {
            return "StreamForge.Host project not found at ${projectDir.absolutePath}"
        }
        return null
    }

    fun start(): Handle {
        val publishDirEnv = System.getenv("SF_TEST_PUBLISH_DIR")
        val publishDir = when {
            publishDirEnv != null -> File(publishDirEnv)
            knownPrebuiltDir.exists() -> knownPrebuiltDir
            else -> publish()
        }
        val dll = File(publishDir, "StreamForge.Host.dll")
        check(dll.exists()) { "StreamForge.Host.dll not found under ${publishDir.absolutePath}" }
        val dataDir = Files.createTempDirectory("sf-kotlin-client-test-").toFile()

        val process = ProcessBuilder(
            dotnet.absolutePath, dll.absolutePath,
            "--Http:Port", HTTP_PORT.toString(),
            "--Grpc:Port", GRPC_PORT.toString(),
            "--Streams:Transport", "push",
            "--DataDir", dataDir.absolutePath,
        )
            .directory(publishDir)
            .redirectErrorStream(true)
            .start()
        val drain = Drain(process)

        try {
            waitHealthy(process, drain)
            importFixtureConfig()
        } catch (e: Exception) {
            process.destroyForcibly()
            dataDir.deleteRecursively()
            throw e
        }
        return Handle(process, dataDir, drain)
    }

    fun stop(handle: Handle) {
        handle.process.destroy()
        if (!handle.process.waitFor(15, TimeUnit.SECONDS)) handle.process.destroyForcibly()
        handle.dataDir.deleteRecursively()
    }

    private fun publish(): File {
        val publishDir = Files.createTempDirectory("sf-kotlin-client-publish-").toFile()
        val process = ProcessBuilder(
            dotnet.absolutePath, "publish", projectDir.absolutePath, "-c", "Debug", "-o", publishDir.absolutePath,
        ).redirectErrorStream(true).start()
        // A single blocking publish with nothing else happening concurrently -- reading to EOF
        // here doubles as continuous draining, no separate Drain needed.
        val output = process.inputStream.bufferedReader().readText()
        val exit = process.waitFor()
        check(exit == 0) { "dotnet publish failed (code $exit):\n${output.takeLast(6000)}" }
        return publishDir
    }

    private fun waitHealthy(process: Process, drain: Drain, timeoutSeconds: Long = 90) {
        val client = HttpClient.newHttpClient()
        val deadline = System.nanoTime() + timeoutSeconds * 1_000_000_000L
        var lastError: Exception? = null
        while (System.nanoTime() < deadline) {
            if (!process.isAlive) {
                throw RuntimeException("engine process exited early (code ${process.exitValue()}):\n${drain.tail()}")
            }
            try {
                val resp = client.send(
                    HttpRequest.newBuilder(URI.create("$baseUrl/api/healthz")).timeout(Duration.ofSeconds(2)).GET().build(),
                    HttpResponse.BodyHandlers.ofString(),
                )
                if (resp.statusCode() == 200) return
            } catch (e: Exception) {
                lastError = e
            }
            Thread.sleep(500)
        }
        throw RuntimeException("engine did not become healthy within ${timeoutSeconds}s (last error: $lastError)\n${drain.tail()}")
    }

    private fun importFixtureConfig() {
        val client = HttpClient.newHttpClient()
        val gson = Gson()

        val loginResp = client.send(
            HttpRequest.newBuilder(URI.create("$baseUrl/api/auth/login"))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(mapOf("username" to ADMIN_USER, "password" to ADMIN_PASS))))
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        check(loginResp.statusCode() == 200) { "fixture login failed: ${loginResp.statusCode()} ${loginResp.body()}" }
        val token = JsonParser.parseString(loginResp.body()).asJsonObject.get("token").asString

        val doc = mapOf(
            "version" to 1,
            "sources" to listOf(
                mapOf(
                    "name" to SOURCE_NAME,
                    "description" to "kotlin client contract test fixture",
                    "kind" to "ingest",
                    "fields" to listOf(
                        mapOf("name" to "trade_id", "type" to "String"),
                        mapOf("name" to "desk", "type" to "String"),
                        mapOf("name" to "notional", "type" to "Double"),
                    ),
                    "ingest" to emptyMap<String, Any>(),
                    "enabled" to true,
                )
            ),
            "pipelines" to emptyList<Any>(),
            "tables" to listOf(
                mapOf(
                    "name" to LATEST_TABLE,
                    "description" to "latest row per trade_id",
                    "sql" to "SELECT trade_id, desk, notional FROM $SOURCE_NAME LATEST BY (trade_id)",
                    "running" to true,
                ),
                mapOf(
                    "name" to AGG_TABLE,
                    "description" to "aggregate over the derived LATEST BY (per design doc §8's fixture spec)",
                    "sql" to "SELECT desk, SUM(notional) AS total FROM $LATEST_TABLE GROUP BY desk",
                    "running" to true,
                ),
            ),
        )
        val importResp = client.send(
            HttpRequest.newBuilder(URI.create("$baseUrl/api/config/import?mode=merge"))
                .header("content-type", "application/json")
                .header("authorization", "Bearer $token")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(doc)))
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        check(importResp.statusCode() < 400) { "fixture config import failed: ${importResp.statusCode()} ${importResp.body()}" }
        val report = JsonParser.parseString(importResp.body()).asJsonObject
        val errored = report.getAsJsonArray("entries")?.filter { it.asJsonObject.get("action")?.asString == "error" } ?: emptyList()
        check(errored.isEmpty()) { "fixture config import had errors: $errored" }
    }
}
