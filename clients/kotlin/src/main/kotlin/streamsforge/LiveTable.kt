package streamsforge
import java.io.Closeable

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * One table's Z-set state, kept current by a coroutine (a child of the owning
 * [StreamsForgeClient]'s scope) that runs subscribe -> buffer -> snapshot -> replay -- see
 * [ZSet]'s docstring for why buffering is necessary -- and then keeps applying live deltas,
 * publishing through a LEADING edge + TRAILING coalesce window ([flushWindow], default 16ms --
 * one frame at 60Hz, the natural ceiling for a UI consumer that cannot display more than one
 * frame per 16ms anyway). If at least [flushWindow] has elapsed since the last publish, a batch
 * is published immediately after it's applied -- no delay, no wait -- so a lone update on an
 * otherwise-quiet table is never held back. Only a batch that lands INSIDE the window opened by
 * the previous publish gets merged into a single pending publish, fired at `lastPublish +
 * flushWindow`; further batches inside that same window merge into the same pending publish, so
 * at most one publish is ever pending. `flushWindow = ZERO` disables coalescing entirely and
 * publishes synchronously per applied batch. The window exists at all because a firehose of tens
 * of thousands of deltas/sec would otherwise fire one [StateFlow] emission per delta and melt a
 * collector, not because this repo's own hub-driven UI does anything similar --
 * `web/src/hooks/useTableRows.ts` publishes on every batch with no coalescing window of its own
 * (its 900ms timer is an unrelated flash-highlight effect).
 *
 * [rows] is an immutable snapshot, not a live view: built fresh from the reducer on every
 * publish, same reasoning as the Python client's `.df` -- a row list that mutated in place would
 * race the reducer with no change notification a consumer could rely on. [rowsFlow] is the same
 * thing as a [StateFlow] for consumers that want to react rather than poll -- a [StateFlow]
 * inherently conflates (only the latest value is ever delivered, assigning `.value` never
 * suspends), which is exactly the "latest-wins, not a queue" semantics a state-snapshot stream
 * needs: a slow collector never blocks the reader coroutine, and there is no backlog to grow.
 *
 * Closing a `LiveTable` cancels its own subscription only; closing the owning client cancels
 * every `LiveTable` it created (structured concurrency: this table's job is a child of the
 * client's).
 */
class LiveTable internal constructor(
    private val transport: TableTransport,
    private val tableName: String,
    private val keyFields: List<String>?,
    parentScope: CoroutineScope,
    private val flushWindow: Duration = DEFAULT_FLUSH_WINDOW,
) : Closeable {
    private val job = SupervisorJob(parentScope.coroutineContext[Job])
    private val scope = CoroutineScope(parentScope.coroutineContext + job)

    // Mutated only by the single reader coroutine below; external reads go through `_rows`
    // (StateFlow, safe to read cross-thread) rather than touching the reducer directly.
    private var zset = ZSet(keyFields)

    // Also touched only by the reader coroutine -- when the most recent publish happened, so the
    // leading-edge check in liveLoop() can tell whether the window has already elapsed. Null
    // means "no publish yet this session", which the check below treats as elapsed (the snapshot
    // fill in doSnapshotAndReplay sets this before liveLoop ever runs, so in practice this is only
    // ever null for the instant between construction and the first snapshot).
    private var lastPublish: TimeSource.Monotonic.ValueTimeMark? = null

    private val _rows = MutableStateFlow<List<Row>>(emptyList())
    val rowsFlow: StateFlow<List<Row>> = _rows.asStateFlow()
    val rows: List<Row> get() = _rows.value

    @Volatile var ready: Boolean = false
        private set

    @Volatile var reconnects: Int = 0
        private set

    @Volatile var seq: Long = 0
        private set

    private val readyGate = CompletableDeferred<Unit>()

    init {
        scope.launch { runLoop() }
    }

    /** Suspends until the first snapshot+replay lands, or [timeout] elapses -- called right
     * after construction by [StreamsForgeClient.table] (a coroutine can't block inside `init`,
     * unlike the Python client's constructor-time thread join). */
    internal suspend fun awaitReady(timeout: Duration) {
        val completed = withTimeoutOrNull(timeout) { readyGate.await() }
        if (completed == null) {
            close()
            throw NotReadyException(
                "table '$tableName' did not fill within $timeout -- a brand-new table gets no " +
                    "backfill, so this is expected until something pushes to it"
            )
        }
    }

    fun value(col: String, keys: Map<String, Any?> = emptyMap()): Any? =
        rows.firstOrNull { row -> keys.all { (k, v) -> row[k] == v } }?.get(col)

    /** Polls [pred] against [rows] until it returns true, or raises [NotReadyException] after
     * [timeout]. A predicate that throws (e.g. indexing a column that doesn't exist yet on an
     * empty table) is treated as "not yet", same as the Python client's `wait_for`. */
    suspend fun waitFor(timeout: Duration = 30.seconds, pred: (List<Row>) -> Boolean): List<Row> {
        val deadline = TimeSource.Monotonic.markNow() + timeout
        while (true) {
            val snapshot = rows
            if (runCatching { pred(snapshot) }.getOrDefault(false)) return snapshot
            if (deadline.hasPassedNow()) throw NotReadyException("waitFor on '$tableName' timed out after $timeout")
            delay(50)
        }
    }

    override fun close() {
        job.cancel()
    }

    // ---- reader coroutine ----

    private suspend fun runLoop() {
        var backoff = 1.seconds
        while (true) {
            try {
                subscribeSnapshotReplay()
                backoff = 1.seconds
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                ready = false
                reconnects++
                delay(backoff)
                backoff = minOf(backoff * 2, 15.seconds)
            }
        }
    }

    private suspend fun subscribeSnapshotReplay(): Unit = coroutineScope {
        // A resumed connection without a fresh snapshot silently corrupts the Z-set (deltas
        // emitted while it was down are gone) -- every (re)connect starts from a clean reducer.
        zset = ZSet(keyFields)
        // `subscribeTable` is itself suspend -- it returns only once the subscription is
        // registered (or as near to it as the transport can guarantee), which is what makes it
        // safe to call `snapshot()` right after without racing the live channel's own setup (see
        // [TableTransport.subscribeTable]'s doc).
        val channel = transport.subscribeTable(tableName).produceIn(this)
        try {
            doSnapshotAndReplay(channel)
            liveLoop(channel)
        } finally {
            channel.cancel()
        }
    }

    private suspend fun doSnapshotAndReplay(channel: ReceiveChannel<DeltaBatch>) {
        val (snapRows, snapSeq) = transport.snapshot(tableName)
        val buffered = drainNowait(channel) // arrived while the snapshot read was in flight

        zset.seed(snapRows)
        seq = snapSeq
        for (batch in buffered) {
            if (!zset.alreadyReflected(batch.deltas)) {
                zset.apply(batch.deltas)
                seq = batch.seq
            }
        }
        publishNow()
        ready = true
        readyGate.complete(Unit) // no-op on reconnect; only the first fill unblocks awaitReady()
    }

    private suspend fun liveLoop(channel: ReceiveChannel<DeltaBatch>) {
        while (true) {
            val first = channel.receive() // throws ClosedReceiveChannelException when the stream ends
            zset.apply(first.deltas)
            seq = first.seq

            if (flushWindow == Duration.ZERO) {
                publishNow() // coalescing disabled -- publish this batch on its own
                continue
            }

            val last = lastPublish
            if (last == null || last.elapsedNow() >= flushWindow) {
                // LEADING EDGE: the window has already elapsed since the last publish (or there
                // has never been one), so this batch -- lone or not -- goes out with no delay.
                publishNow()
                continue
            }

            // TRAILING COALESCE: still inside the window opened by the last publish. Keep
            // draining the channel (never stalling it) until that window closes, folding
            // whatever else arrives into the same reducer state, then publish exactly once.
            val deadline = last + flushWindow
            while (true) {
                val remaining = deadline - TimeSource.Monotonic.markNow()
                if (remaining <= Duration.ZERO) break
                val next = withTimeoutOrNull(remaining) { channel.receive() } ?: break
                zset.apply(next.deltas)
                seq = next.seq
            }
            publishNow()
        }
    }

    /** The only place `_rows` is assigned -- also stamps [lastPublish] so the leading-edge check
     * above measures from the true last publish, not from whenever a batch happened to arrive. */
    private fun publishNow() {
        _rows.value = zset.rows()
        lastPublish = TimeSource.Monotonic.markNow()
    }

    private fun drainNowait(channel: ReceiveChannel<DeltaBatch>): List<DeltaBatch> {
        val items = mutableListOf<DeltaBatch>()
        while (true) {
            val result = channel.tryReceive()
            if (result.isSuccess) items.add(result.getOrThrow()) else break
        }
        return items
    }

    companion object {
        /** One frame at 60Hz -- a UI cannot display more than one frame per 16ms, so it's the
         * natural ceiling for a change-notification window rather than a compromise. */
        val DEFAULT_FLUSH_WINDOW = 16.milliseconds
    }
}
