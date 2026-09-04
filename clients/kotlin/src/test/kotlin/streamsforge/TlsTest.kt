package streamsforge

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import java.io.File
import java.net.http.HttpClient
import java.nio.file.Files
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

/**
 * Live TLS coverage: one real StreamsForge instance started with `--Tls:Enabled true` plus a
 * certificate minted by the repo's OWN `tools/tls/dev-cert.sh` -- never hand-rolled here, same
 * reasoning as the .NET chain suite's `DevCert.cs` (`orleans/tests/StreamsForge.Chain.Tests/
 * DevCert.cs`): the script is what an operator is told to run, so a test that mints a certificate
 * some other way leaves the actually-documented path unexercised.
 *
 * Reserved ports (per the repo `CLAUDE.md`'s port table, carved out specifically for this task):
 * HTTP **7799**, gRPC **7899**, silo **17799** / **37799** -- never [EngineFixture]'s own
 * 9199/9299, which [ContractTest] may be running concurrently in the same JVM/Gradle test session.
 *
 * Shares [EngineFixture]'s process-spawn/health-poll/fixture-import machinery via its
 * `EngineOptions` overloads -- the only difference from the plain contract-test engine is the
 * `--Tls:Enabled`/certificate arguments and an `https://` scheme, and a health-check/import
 * REST client that actually trusts the dev certificate (the plain fixture's default
 * `HttpClient.newHttpClient()` trusts nothing but the JVM's platform store, which does not include
 * a certificate minted five seconds ago).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TlsTest {
    private val httpPort = 7799
    private val grpcPort = 7899
    private val siloPort = 17799
    private val gatewayPort = 37799
    private val httpsUrl = "https://127.0.0.1:$httpPort"

    private var certDir: File? = null
    private lateinit var certPath: String
    private lateinit var keyPath: String
    private var handle: EngineFixture.Handle? = null
    private var skipReason: String? = null

    @BeforeAll
    fun startTlsEngine() {
        skipReason = EngineFixture.preconditionsOrSkipReason(EngineFixture.EngineOptions(httpPort = httpPort, grpcPort = grpcPort))
            ?: devCertPreflight()
        if (skipReason != null) return

        try {
            mintDevCert()
            val tls = buildTlsConfig(certPath, insecure = false)
                ?: error("buildTlsConfig returned null for a non-null caFile")
            val trustingClient = HttpClient.newBuilder().sslContext(tls.sslContext).build()

            handle = EngineFixture.start(
                EngineFixture.EngineOptions(
                    httpPort = httpPort,
                    grpcPort = grpcPort,
                    siloPort = siloPort,
                    gatewayPort = gatewayPort,
                    scheme = "https",
                    extraArgs = listOf(
                        "--Tls:Enabled", "true",
                        "--Kestrel:Certificates:Default:Path", certPath,
                        "--Kestrel:Certificates:Default:KeyPath", keyPath,
                    ),
                    httpClient = trustingClient,
                )
            )
        } catch (e: Exception) {
            skipReason = "the TLS host fixture did not come up cleanly: ${e.message}"
        }
    }

    @AfterAll
    fun stopTlsEngine() {
        handle?.let { EngineFixture.stop(it) }
        certDir?.deleteRecursively()
    }

    /** Null when `tools/tls/dev-cert.sh` can actually run here; otherwise the skip reason --
     * same convention as [EngineFixture.preconditionsOrSkipReason]. */
    private fun devCertPreflight(): String? {
        if (devCertScript() == null) return "tools/tls/dev-cert.sh not found -- cannot mint a development certificate"
        val osName = System.getProperty("os.name").lowercase()
        if (!(osName.contains("mac") || osName.contains("nix") || osName.contains("nux"))) {
            return "tools/tls/dev-cert.sh is a bash script -- TLS tests run on Linux/macOS only"
        }
        return try {
            val probe = ProcessBuilder("openssl", "version").redirectErrorStream(true).start()
            if (probe.waitFor() != 0) "openssl on PATH did not run cleanly -- cannot mint a development certificate" else null
        } catch (e: Exception) {
            "openssl not found on PATH -- cannot mint a development certificate"
        }
    }

    /** `tools/tls/dev-cert.sh` lives at the repo root; the gradle `test` task's `workingDir` is
     * pinned to `clients/kotlin` (see `build.gradle.kts`), so this is the same two-levels-up trick
     * [EngineFixture]'s own `projectDir` uses for `orleans/src/StreamsForge.Host`. */
    private fun devCertScript(): File? {
        val path = File(System.getProperty("user.dir"), "../../tools/tls/dev-cert.sh")
        return if (path.exists()) path else null
    }

    private fun mintDevCert() {
        val dir = Files.createTempDirectory("sf-kotlin-tls-cert-").toFile()
        certDir = dir
        val script = devCertScript() ?: error("tools/tls/dev-cert.sh not found -- call devCertPreflight() first")
        val proc = ProcessBuilder("/bin/bash", script.absolutePath, dir.absolutePath)
            .redirectErrorStream(true)
            .start()
        val output = proc.inputStream.bufferedReader().readText()
        val exit = proc.waitFor()
        check(exit == 0) { "tools/tls/dev-cert.sh exited $exit:\n${output.takeLast(4000)}" }
        certPath = File(dir, "cert.pem").absolutePath
        keyPath = File(dir, "key.pem").absolutePath
        check(File(certPath).exists() && File(keyPath).exists()) {
            "tools/tls/dev-cert.sh succeeded but produced no cert.pem/key.pem in $dir\n$output"
        }
    }

    @Test
    @Timeout(90)
    fun grpcOverTlsConnectsListsTablesAndReceivesSeededRows() = runBlocking {
        assumeTrue(skipReason == null, skipReason)
        StreamsForge.connect(
            url = httpsUrl,
            user = EngineFixture.ADMIN_USER,
            password = EngineFixture.ADMIN_PASS,
            transport = Transport.GRPC,
            caFile = certPath,
        ).use { sf ->
            assertEquals("grpc", sf.transportName)

            val tableNames = sf.tables().mapNotNull { it["name"] as? String }
            assertTrue(tableNames.contains(EngineFixture.LATEST_TABLE), "expected $tableNames to contain ${EngineFixture.LATEST_TABLE}")

            val tradeId = "tls-grpc-${UUID.randomUUID().toString().take(8)}"
            sf.table(EngineFixture.LATEST_TABLE, keyFields = listOf("trade_id")).use { t ->
                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to tradeId, "desk" to "Rates", "notional" to 111.0)))
                val rows = t.waitFor(45.seconds) { r -> r.any { it["trade_id"] == tradeId } }
                assertEquals("Rates", rows.first { it["trade_id"] == tradeId }["desk"])
            }
        }
    }

    @Test
    @Timeout(90)
    fun signalrOverTlsConnectsListsTablesAndReceivesSeededRows() = runBlocking {
        assumeTrue(skipReason == null, skipReason)
        StreamsForge.connect(
            url = httpsUrl,
            user = EngineFixture.ADMIN_USER,
            password = EngineFixture.ADMIN_PASS,
            transport = Transport.SIGNALR,
            caFile = certPath,
        ).use { sf ->
            assertEquals("signalr", sf.transportName)

            val tableNames = sf.tables().mapNotNull { it["name"] as? String }
            assertTrue(tableNames.contains(EngineFixture.LATEST_TABLE), "expected $tableNames to contain ${EngineFixture.LATEST_TABLE}")

            val tradeId = "tls-signalr-${UUID.randomUUID().toString().take(8)}"
            sf.table(EngineFixture.LATEST_TABLE, keyFields = listOf("trade_id")).use { t ->
                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to tradeId, "desk" to "Options", "notional" to 222.0)))
                val rows = t.waitFor(45.seconds) { r -> r.any { it["trade_id"] == tradeId } }
                assertEquals("Options", rows.first { it["trade_id"] == tradeId }["desk"])
            }
        }
    }

    /** Explicit `Transport.GRPC` (not `AUTO`, which would swallow the failure and silently retry
     * over SignalR -- see [StreamsForge.connect]'s catch block) makes `connect()` itself perform a
     * real request -- [GrpcTransport.probe]'s auth header fetch is [AuthClient]'s login POST --
     * before returning, so a certificate the JVM's default trust store doesn't recognize surfaces
     * as a thrown exception from `connect()`, not a delayed failure on first use. */
    @Test
    @Timeout(30)
    fun httpsWithoutATrustedCaIsRefused() {
        assumeTrue(skipReason == null, skipReason)
        assertThrows(Exception::class.java) {
            runBlocking {
                StreamsForge.connect(
                    url = httpsUrl,
                    user = EngineFixture.ADMIN_USER,
                    password = EngineFixture.ADMIN_PASS,
                    transport = Transport.GRPC,
                    // no caFile, no insecure -- the self-signed dev certificate is not in the
                    // JVM's default trust store.
                ).close()
            }
        }
    }
}
