package streamsforge

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

/**
 * Offline, no engine needed: [parseGrpcTarget] (the three `grpcTarget` shapes
 * [StreamsForge.connect] accepts) and [StreamsForge.defaultGrpcTarget] (the `PORT+100` guess,
 * which must preserve an `https://` scheme -- see its own doc).
 */
class GrpcTargetTest {
    @Test
    fun `bare host colon port is plaintext, unchanged from before TLS support`() {
        assertEquals(GrpcAddress("localhost", 9299, tls = false), parseGrpcTarget("localhost:9299"))
    }

    @Test
    fun `http scheme is plaintext`() {
        assertEquals(GrpcAddress("localhost", 9299, tls = false), parseGrpcTarget("http://localhost:9299"))
    }

    @Test
    fun `https scheme requests TLS`() {
        assertEquals(GrpcAddress("127.0.0.1", 7899, tls = true), parseGrpcTarget("https://127.0.0.1:7899"))
    }

    @Test
    fun `https without an explicit port defaults to 443`() {
        assertEquals(GrpcAddress("example.com", 443, tls = true), parseGrpcTarget("https://example.com"))
    }

    @Test
    fun `http without an explicit port defaults to 80`() {
        assertEquals(GrpcAddress("example.com", 80, tls = false), parseGrpcTarget("http://example.com"))
    }

    @Test
    fun `a bare target with no port is rejected -- gRPC's own convention requires one`() {
        assertThrows(IllegalArgumentException::class.java) { parseGrpcTarget("localhost") }
    }

    @Test
    fun `an http url guesses a scheme-less plaintext target`() {
        assertEquals("localhost:9299", StreamsForge.defaultGrpcTarget("http://localhost:9199"))
    }

    @Test
    fun `an https url preserves the scheme in its PORT plus 100 guess`() {
        assertEquals("https://127.0.0.1:7899", StreamsForge.defaultGrpcTarget("https://127.0.0.1:7799"))
    }
}
