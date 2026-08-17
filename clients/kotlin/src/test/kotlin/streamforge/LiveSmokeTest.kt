package streamforge

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration as JDuration
import kotlin.time.Duration.Companion.seconds

/**
 * Read-only smoke test against the demo already running at http://localhost:6199 (admin/
 * admin123!) -- per the task brief, REST + SignalR only. That instance was started with `--urls`
 * (design doc §3.2's trap), so no gRPC port is bound; `Transport.GRPC`/`AUTO` are deliberately
 * NOT exercised here. Never mutates, restarts, or kills it -- reads a snapshot and watches a live
 * subscription's `seq` the whole test does is observe.
 */
class LiveSmokeTest {
    companion object {
        private const val DEMO_URL = "http://localhost:6199"

        private fun demoReachable(): Boolean = runCatching {
            HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("$DEMO_URL/api/healthz")).timeout(JDuration.ofSeconds(3)).GET().build(),
                HttpResponse.BodyHandlers.ofString(),
            ).statusCode() == 200
        }.getOrDefault(false)
    }

    @Test
    @Timeout(30)
    fun snapshotTriggerMonitor() = runBlocking {
        assumeTrue(demoReachable(), "demo at $DEMO_URL is not reachable -- skipping live smoke test")
        val sf = StreamForge.connect(url = DEMO_URL, user = "admin", password = "admin123!", transport = Transport.SIGNALR)
        try {
            assertEquals("signalr", sf.transportName)
            val rows = sf.snapshot("trigger_monitor")
            println("live smoke: trigger_monitor snapshot rows=${rows.size}")
        } finally {
            sf.close()
        }
    }

    @Test
    @Timeout(45)
    fun signalRSubscriptionTicks() = runBlocking {
        assumeTrue(demoReachable(), "demo at $DEMO_URL is not reachable -- skipping live smoke test")
        val sf = StreamForge.connect(url = DEMO_URL, user = "admin", password = "admin123!", transport = Transport.SIGNALR)
        try {
            val t = sf.table("trigger_monitor", timeout = 20.seconds)
            try {
                assertTrue(t.ready)
                val seq0 = t.seq
                delay(6000)
                // Logged rather than hard-asserted: whether trigger_monitor ticks in any given
                // 6s window depends on the demo generator's current activity, not on this client.
                println("live smoke: trigger_monitor seq $seq0 -> ${t.seq} (ticking=${t.seq != seq0}, reconnects=${t.reconnects})")
            } finally {
                t.close()
            }
        } finally {
            sf.close()
        }
    }
}
