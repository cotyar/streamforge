package streamsforge

import com.google.protobuf.Empty
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import java.io.File
import streamsforge.v1.GetTableRowsRequest
import streamsforge.v1.IngestRequest
import streamsforge.v1.IngestServiceGrpcKt
import streamsforge.v1.StreamServiceGrpcKt
import streamsforge.v1.SubscribeTableRequest
import streamsforge.v1.TableServiceGrpcKt

private val AUTH_KEY: Metadata.Key<String> = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

/** One parsed `grpcTarget`: [host]/[port] plus whether it asked for TLS. See [parseGrpcTarget]. */
internal data class GrpcAddress(val host: String, val port: Int, val tls: Boolean)

/**
 * Parses the three shapes [StreamsForge.connect]'s `grpcTarget` accepts:
 * - `host:port` -- gRPC's own scheme-less target syntax, plaintext, unchanged from before TLS
 *   support existed.
 * - `http://host:port` -- plaintext, spelled out explicitly (e.g. by `StreamsForge`'s own
 *   `defaultGrpcTarget` guess when the REST `url` itself was `http://`).
 * - `https://host:port` -- TLS, ALPN h2 (matches a host started with `--Tls:Enabled true`).
 *
 * A bare target's port is required (gRPC's own convention); a schemed target without an explicit
 * port falls back to the scheme's standard port (80/443) -- matching how a browser would resolve
 * the same URL, even though StreamsForge hosts always bind an explicit port in practice.
 */
internal fun parseGrpcTarget(target: String): GrpcAddress = when {
    target.startsWith("https://") -> splitHostPort(target.removePrefix("https://"), 443).let { (h, p) -> GrpcAddress(h, p, tls = true) }
    target.startsWith("http://") -> splitHostPort(target.removePrefix("http://"), 80).let { (h, p) -> GrpcAddress(h, p, tls = false) }
    else -> splitHostPort(target, -1).let { (h, p) ->
        require(p != -1) { "grpcTarget '$target' has no port -- expected host:port, http://host:port or https://host:port" }
        GrpcAddress(h, p, tls = false)
    }
}

private fun splitHostPort(hostPort: String, defaultPort: Int): Pair<String, Int> {
    val idx = hostPort.lastIndexOf(':')
    return if (idx < 0) hostPort to defaultPort else hostPort.substring(0, idx) to hostPort.substring(idx + 1).toInt()
}

/**
 * Plaintext (`host:port` / `http://host:port`) goes through the SAME `ManagedChannelBuilder.
 * forTarget(...).usePlaintext()` call as before TLS support existed -- an h2c channel, prior
 * knowledge, matching how the engine runs from source. `https://host:port` switches to
 * `NettyChannelBuilder.forAddress` with ALPN h2 (`useTransportSecurity()`), trusting [caFile] (the
 * dev cert IS its own CA -- see `tools/tls/dev-cert.sh`) or, for [insecure], every certificate via
 * grpc-netty-shaded's own `InsecureTrustManagerFactory` -- development only, never the default.
 *
 * Note this builds a grpc-netty `SslContext`, NOT the `javax.net.ssl.SSLContext` [buildTlsConfig]
 * produces for REST/SignalR -- the two SSL stacks don't share a type, so gRPC gets its own
 * construction from the same raw `caFile`/`insecure` inputs (see [TlsConfig]'s doc for why).
 */
private fun buildChannel(address: GrpcAddress, caFile: String?, insecure: Boolean): ManagedChannel {
    if (!address.tls) {
        return ManagedChannelBuilder.forTarget("${address.host}:${address.port}").usePlaintext().build()
    }
    val sslContextBuilder = GrpcSslContexts.forClient()
    when {
        insecure -> sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE)
        caFile != null -> sslContextBuilder.trustManager(File(caFile))
        // Neither insecure nor a CA given for an https:// target: fall through to the JVM's
        // default trust store, same as any other TLS client -- correct for a host with a
        // CA-issued certificate, and the caller's job to have supplied caFile/insecure otherwise.
    }
    return NettyChannelBuilder.forAddress(address.host, address.port)
        .useTransportSecurity()
        .sslContext(sslContextBuilder.build())
        .build()
}

/** Result of one `IngestService.Ingest` bidi push (design doc §3.1: real HTTP/2 backpressure --
 * the server does not ack until the push is admitted -- instead of REST's `retry_after_ms`
 * guess). Mirrors `StreamsForge.Abstractions.IngestOutcome` (`IngestAck` in streamsforge.proto). */
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
class GrpcTransport(
    target: String,
    caFile: String? = null,
    insecure: Boolean = false,
    private val tokenProvider: suspend () -> String,
) : TableTransport {
    override val name = "grpc"

    private val channel: ManagedChannel = buildChannel(parseGrpcTarget(target), caFile, insecure)
    private val tableStub = TableServiceGrpcKt.TableServiceCoroutineStub(channel)
    private val streamStub = StreamServiceGrpcKt.StreamServiceCoroutineStub(channel)
    private val ingestStub = IngestServiceGrpcKt.IngestServiceCoroutineStub(channel)

    private suspend fun authHeaders(): Metadata = Metadata().apply { put(AUTH_KEY, "Bearer ${tokenProvider()}") }

    override fun close() {
        channel.shutdownNow()
    }

    /** Proves the channel AND the JWT actually work -- used by `StreamsForge.connect(transport =
     * AUTO)` to decide whether gRPC is reachable before committing to it (design doc: `AUTO`
     * "tries gRPC, falls back to SignalR, and logs which one it got"). */
    suspend fun probe() {
        tableStub.list(Empty.getDefaultInstance(), authHeaders())
    }

    private suspend fun resolveTableId(tableName: String): String {
        val resp = tableStub.list(Empty.getDefaultInstance(), authHeaders())
        return resp.tablesList.firstOrNull { it.name == tableName }?.id
            ?: throw StreamsForgeError("no such table '$tableName'")
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
