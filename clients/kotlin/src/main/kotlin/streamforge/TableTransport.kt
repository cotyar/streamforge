package streamforge
import java.io.Closeable

import kotlinx.coroutines.flow.Flow

/**
 * The one interface every live transport implements: gRPC ([GrpcTransport]) and SignalR
 * ([SignalRTransport]). [LiveTable], the reducer and the contract-test suite are all written
 * against THIS and never know which concrete transport is underneath -- that is the whole point
 * of the contract suite running once per transport (design doc §3.6/§8): two implementations that
 * agree on every assertion are interchangeable, and one that drifts fails on the same line the
 * other passes.
 *
 * (Named `TableTransport` rather than `Transport` to leave that name for the public
 * `Transport` enum `StreamForge.connect(transport = ...)` takes -- same "one interface" idiom the
 * design doc describes, different Kotlin type so the two don't collide.)
 */
interface TableTransport : Closeable {
    val name: String

    /** One-shot read of a table's current consolidated rows (weight already summed
     * server-side) plus the read's own sequence number. Not comparable to [subscribeTable]'s
     * `seq` -- see [ZSet]'s docstring. */
    suspend fun snapshot(tableName: String, limit: Int = 500): Pair<List<RowDelta>, Long>

    /** Delta batches for `tableName`, from whenever the subscription goes live -- no backfill.
     * `suspend` rather than a bare `Flow`-returning function DELIBERATELY: it means the
     * connection is established and the subscribe invocation sent (or is far enough along that
     * the remaining work is negligible) before this call returns, rather than deferred to
     * whenever the returned [Flow] happens to get collected. A cold Flow here raced [snapshot]
     * for real -- SignalR's negotiate+upgrade+hub-handshake is slow enough, and Kotlin's
     * `produceIn`/dispatcher scheduling loose enough, that the REST snapshot routinely finished
     * and reported the table "ready" for pushes before the live subscription was actually
     * registered server-side, silently dropping the first delta forever (no backfill, and
     * nothing else re-asserts a row nobody pushed to twice). Cancelling collection of the
     * returned Flow still tears the connection down for free (structured concurrency); callers
     * pair this with [snapshot] and buffer/replay ([LiveTable]), never rely on it alone. */
    suspend fun subscribeTable(tableName: String): Flow<DeltaBatch>
}
