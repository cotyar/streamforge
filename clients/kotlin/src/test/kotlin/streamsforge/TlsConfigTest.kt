package streamsforge

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files

/**
 * Offline unit coverage for [buildTlsConfig] (no engine, no `tools/tls/dev-cert.sh` dependency --
 * [TlsTest] is what exercises the real script and a real TLS handshake end to end). Mints its own
 * throwaway self-signed cert directly via `openssl req` for the CA case, rather than reusing
 * `dev-cert.sh`, so this stays a pure unit test of the parsing/trust-store logic and not a second
 * copy of the live test.
 */
class TlsConfigTest {
    @Test
    fun `neither caFile nor insecure means use the platform default trust store`() {
        assertNull(buildTlsConfig(caFile = null, insecure = false))
    }

    @Test
    fun `insecure builds a trust-all context even with no caFile`() {
        val tls = buildTlsConfig(caFile = null, insecure = true)
        assertNotNull(tls)
        assertTrue(tls!!.insecure)
        // Trust-all: an empty chain (nothing real to check against) must not throw.
        tls.trustManager.checkServerTrusted(emptyArray(), "RSA")
    }

    @Test
    fun `a caFile PEM builds a non-insecure context backed by that certificate`() {
        val opensslAvailable = try {
            ProcessBuilder("openssl", "version").redirectErrorStream(true).start().waitFor() == 0
        } catch (e: Exception) {
            false
        }
        assumeTrue(opensslAvailable, "openssl not found on PATH -- cannot mint a throwaway cert for this test")

        val dir = Files.createTempDirectory("sf-kotlin-tlsconfig-test-").toFile()
        try {
            val cert = File(dir, "cert.pem")
            val key = File(dir, "key.pem")
            val proc = ProcessBuilder(
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1", "-nodes",
                "-keyout", key.absolutePath, "-out", cert.absolutePath, "-subj", "/CN=localhost",
            ).redirectErrorStream(true).start()
            val output = proc.inputStream.bufferedReader().readText()
            check(proc.waitFor() == 0) { "openssl req failed:\n$output" }

            val tls = buildTlsConfig(caFile = cert.absolutePath, insecure = false)
            assertNotNull(tls)
            assertFalse(tls!!.insecure)
            assertNotNull(tls.sslContext)
            assertNotNull(tls.trustManager)
            // The trust manager built from THIS certificate must accept a chain consisting of
            // exactly that certificate (self-signed, so it is its own issuer).
            val certificateFactory = java.security.cert.CertificateFactory.getInstance("X.509")
            val parsed = cert.inputStream().use { certificateFactory.generateCertificate(it) } as java.security.cert.X509Certificate
            tls.trustManager.checkServerTrusted(arrayOf(parsed), "RSA")
        } finally {
            dir.deleteRecursively()
        }
    }
}
