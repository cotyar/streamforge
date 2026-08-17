package streamforge

import com.google.protobuf.Empty
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import streamforge.v1.GetTableRowsRequest
import streamforge.v1.IngestRequest
import streamforge.v1.IngestServiceGrpcKt
import streamforge.v1.StreamServiceGrpcKt
import streamforge.v1.SubscribeTableRequest
import streamforge.v1.TableServiceGrpcKt

private val AUTH_KEY: Metadata.Key<String> = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

/** Result of one `IngestService.Ingest` bidi push (design doc §3.1: real HTTP/2 backpressure --
 * the server does not ack until the push is admitted -- instead of REST's `retry_after_ms`
 * guess). Mirrors `StreamForge.Abstractions.IngestOutcome` (`IngestAck` in streamforge.proto). */
data class IngestAckResult(
    val outcome: String,
    val accepted: Int,
    val dropped: Int,
    val invalid: Int,
    val retryAfterMs: Int,
    val error: String,
    val rowErrors: List<String>,
    val duplicate: Int,
    val replayed: Boolean,
)

/**
 * Tier 1 gRPC transport (design doc §3): `StreamService.SubscribeTable` for deltas,
 * `TableService.Rows/List` for the catalog and snapshot, bidi `IngestService.Ingest` for pushes.
 * One insecure h2c channel, prior knowledge -- matching how the engine actually runs from source
 * (§3.2's `--urls` trap: it must be started with `--Http:Port`/`--Grpc:Port`, never `--urls`, or
 * no gRPC port is bound at all).
 *
 * Every gRPC service carries `[Authorize(Policy = "Viewer")]`, so an `authorization: Bearer <jwt>`
 * metadata entry is all that's needed -- [tokenProvider] is `AuthClient::token`, shared with the
 * REST side so both transports mint/refresh the same JWT (design doc §3.1).
 */
class GrpcTransport(target: String, private val tokenProvider: suspend () -> String) : TableTransport {
    override val name = "grpc"

    private val channel: ManagedChannel = ManagedChannelBuilder.forTarget(target).usePlaintext().build()
    private val tableStub = TableServiceGrpcKt.TableServiceCoroutineStub(channel)
    private val streamStub = StreamServiceGrpcKt.StreamServiceCoroutineStub(channel)
    private val ingestStub = IngestServiceGrpcKt.IngestServiceCoroutineStub(channel)

    private suspend fun authHeaders(): Metadata = Metadata().apply { put(AUTH_KEY, "Bearer ${tokenProvider()}") }

    override fun close() {
        channel.shutdownNow()
    }

    /** Proves the channel AND the JWT actually work -- used by `StreamForge.connect(transport =
     * AUTO)` to decide whether gRPC is reachable before committing to it (design doc: `AUTO`
     * "tries gRPC, falls back to SignalR, and logs which one it got"). */
    suspend fun probe() {
        tableStub.list(Empty.getDefaultInstance(), authHeaders())
    }

    private suspend fun resolveTableId(tableName: String): String {
        val resp = tableStub.list(Empty.getDefaultInstance(), authHeaders())
        return resp.tablesList.firstOrNull { it.name == tableName }?.id
            ?: throw StreamForgeError("no such table '$tableName'")
    }

    override suspend fun snapshot(tableName: String, limit: Int): Pair<List<RowDelta>, Long> {
        val id = resolveTableId(tableName)
        val request = GetTableRowsRequest.newBuilder().setId(id).setLimit(limit).build()
        val response = tableStub.rows(request, authHeaders())
        return response.rowsList.map { RowDelta(it.row.toRow(), it.weight) } to response.seq
    }

    override suspend fun subscribeTable(tableName: String): Flow<DeltaBatch> {
        // Metadata (the bearer token) is fetched HERE, as part of this suspend call, not lazily
        // inside a `flow {}` builder -- see [TableTransport.subscribeTable]'s doc: a cold Flow
        // that defers even cheap prep work to collection time can let the caller's REST snapshot
        // race ahead of the subscription actually being registered. gRPC's own stream setup
        // (`streamStub.subscribeTable`) stays a plain non-suspend call returning a Flow, same as
        // grpc-kotlin generates it -- fetching the token first is what matters here, since the
        // token itself refreshes on every (re)subscribe, mirroring live.py's "every reconnect
        // starts fresh".
        val headers = authHeaders()
        val request = SubscribeTableRequest.newBuilder().setName(tableName).build()
        return streamStub.subscribeTable(request, headers).map { batch ->
            DeltaBatch(batch.deltasList.map { RowDelta(it.row.toRow(), it.weight) }, batch.seq)
        }
    }

    /** One request, one ack, over a fresh bidi stream -- gets real backpressure semantics from
     * the bidi RPC without holding a stream open across calls (same simplification the Python
     * client makes in `_grpc.py`'s `ingest()`: a long-lived streaming session across many pushes
     * is future work, not needed for `push()`'s surface). */
    suspend fun ingest(sourceName: String, rows: List<Row>, idempotencyKey: String?, partial: Boolean): IngestAckResult {
        val request = IngestRequest.newBuilder()
            .setSourceName(sourceName)
            .addAllRows(rows.map { it.toStruct() })
            .setPartial(partial)
            .setIdempotencyKey(idempotencyKey ?: "")
            .build()
        val ack = ingestStub.ingest(flowOf(request), authHeaders()).first()
        return IngestAckResult(
            outcome = ack.outcome.name,
            accepted = ack.accepted,
            dropped = ack.dropped,
            invalid = ack.invalid,
            retryAfterMs = ack.retryAfterMs,
            error = ack.error,
            rowErrors = ack.rowErrorsList,
            duplicate = ack.duplicate,
            replayed = ack.replayed,
        )
    }
}
