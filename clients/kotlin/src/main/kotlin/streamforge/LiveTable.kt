package streamforge
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
 * [StreamForgeClient]'s scope) that runs subscribe -> buffer -> snapshot -> replay -- see
 * [ZSet]'s docstring for why buffering is necessary -- and then keeps applying live deltas,
 * coalescing publishes to roughly one per [FLUSH_WINDOW] regardless of how fast deltas arrive
 * (handing a consumer a fresh row list per delta melts a collector under a Monte-Carlo firehose).
 *
 * [rows] is an immutable snapshot, not a live view: built fresh from the reducer on every
 * publish, same reasoning as the Python client's `.df` -- a row list that mutated in place would
 * race the reducer with no change notification a consumer could rely on. [rowsFlow] is the same
 * thing as a [StateFlow] for consumers that want to react rather than poll.
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
) : Closeable {
    private val job = SupervisorJob(parentScope.coroutineContext[Job])
    private val scope = CoroutineScope(parentScope.coroutineContext + job)

    // Mutated only by the single reader coroutine below; external reads go through `_rows`
    // (StateFlow, safe to read cross-thread) rather than touching the reducer directly.
    private var zset = ZSet(keyFields)

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
     * after construction by [StreamForgeClient.table] (a coroutine can't block inside `init`,
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
        _rows.value = zset.rows()
        ready = true
        readyGate.complete(Unit) // no-op on reconnect; only the first fill unblocks awaitReady()
    }

    private suspend fun liveLoop(channel: ReceiveChannel<DeltaBatch>) {
        while (true) {
            val first = channel.receive() // throws ClosedReceiveChannelException when the stream ends
            zset.apply(first.deltas)
            seq = first.seq

            // Coalesce whatever else is already queued (up to FLUSH_WINDOW) before publishing,
            // so a burst of batches costs one StateFlow emission, not one per batch.
            val deadline = TimeSource.Monotonic.markNow() + FLUSH_WINDOW
            while (true) {
                val remaining = deadline - TimeSource.Monotonic.markNow()
                if (remaining <= Duration.ZERO) break
                val next = withTimeoutOrNull(remaining) { channel.receive() } ?: break
                zset.apply(next.deltas)
                seq = next.seq
            }
            _rows.value = zset.rows()
        }
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
        private val FLUSH_WINDOW = 120.milliseconds
    }
}
