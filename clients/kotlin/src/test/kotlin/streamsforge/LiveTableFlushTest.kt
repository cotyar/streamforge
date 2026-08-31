package streamsforge

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * No engine needed -- exercises [LiveTable]'s leading-edge/trailing-coalesce publish logic
 * against [FakeTransport], the way [ZSetConformanceTest] exercises the reducer against handcrafted
 * fixtures. `kotlinx-coroutines-test` (virtual time / `TestScope`/`runTest`) is NOT a dependency of
 * this module (see build.gradle.kts) and this test does not add it -- timing assertions below are
 * deliberately generous wall-clock bounds, not exact virtual-time expectations, to stay reliable
 * without it.
 *
 * All three tests wait past [LiveTable.DEFAULT_FLUSH_WINDOW] once, right after `awaitReady`, so
 * the readiness publish itself (fired by `doSnapshotAndReplay`) is never inside the window a test
 * is trying to measure.
 */
class LiveTableFlushTest {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @AfterEach
    fun tearDown() {
        scope.cancel()
    }

    /** A push transport backed by an unlimited [Channel] a test can feed directly -- no network,
     * no engine. `snapshot()` always returns empty so every table starts ready-but-empty. */
    private class FakeTransport : TableTransport {
        override val name = "fake"
        private val channel = Channel<DeltaBatch>(Channel.UNLIMITED)
        private var seqCounter = 0L

        override suspend fun snapshot(tableName: String, limit: Int): Pair<List<RowDelta>, Long> =
            emptyList<RowDelta>() to 0L

        override suspend fun subscribeTable(tableName: String): Flow<DeltaBatch> = channel.receiveAsFlow()

        suspend fun push(deltas: List<RowDelta>) {
            seqCounter++
            channel.send(DeltaBatch(deltas, seqCounter))
        }

        override fun close() {
            channel.close()
        }
    }

    private fun row(id: Int) = RowDelta(mapOf("id" to id), 1L)

    @Test
    @Timeout(10)
    fun `lone batch on a quiet table publishes with no artificial delay`() = runBlocking {
        val transport = FakeTransport()
        val table = LiveTable(transport, "t", listOf("id"), scope, flushWindow = 16.milliseconds)
        table.awaitReady(5.seconds)

        // Past the window since the readiness publish, so the next batch takes the leading-edge
        // path -- this is the whole point of the change: no unconditional wait.
        delay(50)

        val start = TimeSource.Monotonic.markNow()
        transport.push(listOf(row(1)))

        val deadline = TimeSource.Monotonic.markNow() + 2.seconds
        while (table.rows.isEmpty() && !deadline.hasPassedNow()) delay(1)

        val elapsed = start.elapsedNow()
        assertEquals(1, table.rows.size)
        // The OLD trailing-only window was 120ms; this must land far under that -- generous
        // wall-clock bound (no virtual time available), not a tight one.
        assertTrue(elapsed < 100.milliseconds, "lone update took $elapsed, expected well under the old 120ms window")

        table.close()
    }

    @Test
    @Timeout(10)
    fun `a burst inside one window produces exactly one publish`() = runBlocking {
        val transport = FakeTransport()
        val table = LiveTable(transport, "t", listOf("id"), scope, flushWindow = 200.milliseconds)
        table.awaitReady(5.seconds)

        val publishCount = java.util.concurrent.atomic.AtomicInteger(0)
        val collectorJob = scope.launch {
            table.rowsFlow.collect { publishCount.incrementAndGet() }
        }
        delay(20) // let the collector attach and record its initial (readiness) value

        // Pushed back-to-back, all well inside the 200ms window opened by the readiness publish
        // -- every one of these must merge into a SINGLE trailing publish, not five.
        transport.push(listOf(row(1)))
        transport.push(listOf(row(2)))
        transport.push(listOf(row(3)))
        transport.push(listOf(row(4)))
        transport.push(listOf(row(5)))

        val deadline = TimeSource.Monotonic.markNow() + 2.seconds
        while (table.rows.size < 5 && !deadline.hasPassedNow()) delay(5)
        delay(300) // past the window with margin -- no further publish should ever land

        collectorJob.cancel()
        assertEquals(5, table.rows.size)
        // 1 initial value at collector-attach time (the readiness snapshot) + exactly 1 coalesced
        // publish for the whole burst.
        assertEquals(2, publishCount.get(), "expected exactly one coalesced publish for the burst")

        table.close()
    }

    @Test
    @Timeout(10)
    fun `a zero window publishes per batch`() = runBlocking {
        val transport = FakeTransport()
        val table = LiveTable(transport, "t", listOf("id"), scope, flushWindow = Duration.ZERO)
        table.awaitReady(5.seconds)

        // Pushed and awaited ONE AT A TIME rather than as a burst -- proving "publishes per
        // batch" via StateFlow-collector emission counts would be unreliable here: StateFlow only
        // guarantees delivering the LATEST value to a collector, so back-to-back `.value =`
        // writes issued faster than the collector's own dispatch can race away an intermediate
        // one even though the reducer really did publish it (see the README's backpressure
        // section -- that conflation is a feature, not something this test should fight). Waiting
        // for each row to actually land in `table.rows` before sending the next one instead proves
        // the real claim directly: with coalescing off, a batch is visible almost immediately, not
        // held back to merge with anything else.
        suspend fun pushAndAwait(id: Int, expectedSize: Int): Duration {
            val start = TimeSource.Monotonic.markNow()
            transport.push(listOf(row(id)))
            val deadline = TimeSource.Monotonic.markNow() + 2.seconds
            while (table.rows.size < expectedSize && !deadline.hasPassedNow()) delay(1)
            return start.elapsedNow()
        }

        val t1 = pushAndAwait(1, 1)
        val t2 = pushAndAwait(2, 2)
        val t3 = pushAndAwait(3, 3)

        assertEquals(3, table.rows.size)
        // Same "well under the old 120ms window" bound as the lone-update test -- each of these
        // three arrived on its own, with no coalescing wait.
        for ((i, elapsed) in listOf(t1, t2, t3).withIndex()) {
            assertTrue(elapsed < 100.milliseconds, "batch #${i + 1} took $elapsed, expected no coalescing wait")
        }

        table.close()
    }
}
