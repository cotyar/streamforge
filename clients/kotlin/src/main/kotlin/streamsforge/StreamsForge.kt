package streamsforge
import java.io.Closeable

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import java.net.URI
import java.util.logging.Logger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private val logger = Logger.getLogger("streamsforge")

/** Which live transport [StreamsForge.connect] uses. `AUTO` tries gRPC, falls back to SignalR,
 * and always logs which one it got -- a client that silently degrades and lets a caller believe
 * they're on the fast path is worse than one that fails loudly (design doc §3.5).
 *
 * (This is the PUBLIC selector enum; the internal SPI both concrete transports implement is
 * [TableTransport] -- kept as a separate type specifically so this name stays free for the enum
 * `StreamsForge.connect(transport = Transport.GRPC)` is meant to read as.) */
enum class Transport { GRPC, SIGNALR, AUTO }

object StreamsForge {
    /**
     * One-line connect, mirroring the Python client's `connect()`. `AUTO` (the default) tries
     * gRPC first -- proving the channel AND the JWT actually work via a cheap `TableService.List`
     * call -- and falls back to SignalR on any failure, logging which one it got either way. When
     * gRPC is refused, the likely cause is the host having been started with `--urls`, which trips
     * `Program.cs`'s guard so no gRPC port is bound at all (design doc §3.2).
     *
     * @param url REST base URL, e.g. `http://localhost:9199`.
     * @param grpcTarget gRPC `host:port`, e.g. `localhost:9299`. Defaults to guessing `PORT+100`
     *   off [url] (`Program.cs`'s own convention) -- pass this explicitly whenever the two ports
     *   don't follow that relationship.
     * @param ingestKey `X-SF-Ingest-Key`, preferred over the admin JWT for [StreamsForgeClient.push]
     *   whenever set, so a caller that only feeds a source never needs to hold an admin token.
     * @param caFile Path to a PEM certificate to trust for TLS (REST, SignalR AND gRPC), used as
     *   its own trust anchor -- matches `tools/tls/dev-cert.sh`'s self-signed dev certificate.
     *   Only meaningful when [url] and/or the resolved [grpcTarget] are `https://`; ignored for
     *   a plaintext connection. `null` (the default) trusts the JVM's platform trust store.
     * @param insecure Development-only escape hatch: trusts every certificate and skips hostname
     *   verification on all three transports, no [caFile] required. Never turn this on outside a
     *   throwaway/dev environment -- it accepts literally any TLS server.
     */
    suspend fun connect(
        url: String,
        grpcTarget: String? = null,
        user: String? = null,
        password: String? = null,
        token: String? = null,
        ingestKey: String? = null,
        transport: Transport = Transport.AUTO,
        caFile: String? = null,
        insecure: Boolean = false,
    ): StreamsForgeClient {
        val tls = buildTlsConfig(caFile, insecure)
        val http = AuthClient(url, user, password, token, tls)

        var grpc: GrpcTransport? = null
        if (transport == Transport.GRPC || transport == Transport.AUTO) {
            val target = grpcTarget ?: defaultGrpcTarget(url)
            try {
                val candidate = GrpcTransport(target, caFile, insecure) { http.token() }
                candidate.probe()
                grpc = candidate
            } catch (e: Exception) {
                if (transport == Transport.GRPC) {
                    throw StreamsForgeError(
                        "gRPC channel to $target refused. If the host was started with --urls, " +
                            "Program.cs's guard binds no gRPC port at all -- start it with " +
                            "--Http:Port/--Grpc:Port instead (design doc §3.2). Over https, check " +
                            "caFile/insecure too.",
                        e,
                    )
                }
                logger.warning(
                    "streamsforge: gRPC unavailable (${e.javaClass.simpleName}: ${e.message}), falling back to SignalR"
                )
            }
        }

        val liveTransport: TableTransport
        val chosen: String
        if (grpc != null) {
            liveTransport = grpc
            chosen = "grpc"
        } else {
            liveTransport = SignalRTransport(url, http, tls)
            chosen = "signalr"
        }
        logger.info("streamsforge: connected via $chosen transport ($url)")

        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        return StreamsForgeClient(http, grpc, liveTransport, ingestKey, chosen, scope)
    }

    /** Guesses the gRPC port as `PORT+100` off [baseUrl] (`Program.cs`'s own convention),
     * preserving an `https://` scheme so a caller who only passed `url = "https://host:port"`
     * still gets a TLS gRPC target rather than a silently-plaintext one. */
    internal fun defaultGrpcTarget(baseUrl: String): String {
        val uri = URI.create(baseUrl)
        val host = uri.host ?: "localhost"
        val httpPort = if (uri.port != -1) uri.port else if (uri.scheme == "https") 443 else 80
        val prefix = if (uri.scheme == "https") "https://" else ""
        return "$prefix$host:${httpPort + 100}"
    }
}

/**
 * Returned by [StreamsForge.connect]. Owns the REST auth client, the chosen live [TableTransport]
 * (gRPC or SignalR), and one [CoroutineScope] every [LiveTable] it creates is a structured-
 * concurrency child of -- [close] cancels that scope, which tears down every live subscription
 * this client ever handed out, in addition to the transport's own connection(s).
 */
class StreamsForgeClient internal constructor(
    private val http: AuthClient,
    private val grpc: GrpcTransport?,
    private val liveTransport: TableTransport,
    private val ingestKey: String?,
    val transportName: String,
    private val scope: CoroutineScope,
) : Closeable {

    /** Subscribes, snapshots, and replays -- suspends until ready or [timeout] elapses.
     * [keyFields] omitted (null, the default) resolves the table's row-identity key from its own
     * definition instead (wishlist #18 -- see [RestCatalog.resolveKeyFields]); pass it explicitly
     * to bypass resolution and always win. [flushWindow] governs how the returned [LiveTable]
     * coalesces change notifications -- see its class doc -- and defaults to
     * [LiveTable.DEFAULT_FLUSH_WINDOW] (16ms, one frame at 60Hz); pass [Duration.ZERO] to publish
     * every applied batch with no coalescing. */
    suspend fun table(
        name: String,
        keyFields: List<String>? = null,
        timeout: Duration = 30.seconds,
        flushWindow: Duration = LiveTable.DEFAULT_FLUSH_WINDOW,
    ): LiveTable {
        val resolvedKeyFields = keyFields ?: RestCatalog.resolveKeyFields(http, name)
        val liveTable = LiveTable(liveTransport, name, resolvedKeyFields, scope, flushWindow)
        liveTable.awaitReady(timeout)
        return liveTable
    }

    /** One-shot REST read, no subscription, no coroutine -- always REST regardless of the live
     * transport this client picked (design doc §2/§8: cheap, infrequent, one code path). */
    suspend fun snapshot(name: String, limit: Int = 500): List<Row> {
        val id = RestCatalog.resolveTableId(http, name)
        return RestCatalog.snapshotRows(http, id, limit).first.map { it.row }
    }

    suspend fun tables(): List<Map<String, Any?>> = RestCatalog.listTables(http)

    suspend fun search(name: String, query: String, limit: Int = 50): List<Row> =
        RestCatalog.search(http, name, query, limit)

    /** `POST /api/tables/validate` -- creates nothing, costs nothing, returns the engine's own
     * diagnostics. A rejected query from [sql] raises [SqlException] built from this same call. */
    suspend fun validate(sqlText: String): ValidateResult = RestCatalog.validate(http, sqlText)

    /** validate -> `POST /api/config/import?mode=merge` -> subscribe. Namespace stays `adhoc_`
     * (see [dropAdhoc]); a re-run of an edited query updates the same table in place. [flushWindow]
     * is forwarded to [table] -- see its doc. */
    suspend fun sql(
        sqlText: String,
        name: String,
        keyFields: List<String>? = null,
        timeout: Duration = 30.seconds,
        flushWindow: Duration = LiveTable.DEFAULT_FLUSH_WINDOW,
    ): LiveTable {
        val tableName = RestCatalog.runAdhocSql(http, name, sqlText)
        return table(tableName, keyFields, timeout, flushWindow)
    }

    suspend fun adhocTables(): List<Map<String, Any?>> = RestCatalog.listAdhoc(http)

    /** Refuses any name outside the `adhoc_` prefix -- a caller cannot drop `fund_exposure`. */
    suspend fun dropAdhoc(name: String): Boolean = RestCatalog.dropAdhoc(http, name)

    /** gRPC bidi (`IngestService.Ingest`, real HTTP/2 backpressure) when this client's live
     * transport is gRPC; REST `POST /api/sources/{name}/events` otherwise. */
    suspend fun push(
        source: String,
        rows: List<Row>,
        idempotencyKey: String? = null,
        partial: Boolean = false,
    ): IngestAckResult {
        val currentGrpc = grpc
        if (currentGrpc != null) {
            val ack = currentGrpc.ingest(source, rows, idempotencyKey, partial)
            if (ack.outcome != "INGEST_OUTCOME_ACCEPTED") {
                throw IngestRejectedException(
                    ack.error.ifBlank { "$source ingest push rejected: ${ack.outcome}" },
                    ack.rowErrors,
                )
            }
            return ack
        }
        return RestCatalog.push(http, source, rows, idempotencyKey, partial, ingestKey)
    }

    override fun close() {
        scope.cancel()
        liveTransport.close()
        http.close()
    }
}
