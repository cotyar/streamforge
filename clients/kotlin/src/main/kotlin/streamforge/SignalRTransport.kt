package streamforge

import com.google.gson.reflect.TypeToken
import com.microsoft.signalr.HubConnection
import com.microsoft.signalr.HubConnectionBuilder
import io.reactivex.rxjava3.core.Single
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.withContext
import java.lang.reflect.Type

private data class WireDelta(val row: Row, val weight: Long)

/**
 * SignalR transport using Microsoft's own Java client (`com.microsoft.signalr`) against
 * `/hubs/stream` -- not hand-rolled, per the task brief: don't reimplement a protocol a library
 * already speaks correctly. `app.MapHub<StreamHub>("/hubs/stream")` restricts no transports on
 * the server side (design doc §3.4), but **the Java client itself only supports WebSockets and
 * Long Polling** -- it has no SSE transport (unlike the browser/Python clients' three-way split).
 * That's a limitation of the library to document, not to hand-roll around (task brief). The
 * client negotiates and picks WebSockets first, falling back to Long Polling, on its own.
 *
 * `SubscribeTable` is sent via `invoke` (not `send`): an "Invocation" WITH an `invocationId`,
 * whose returned `Completable` only resolves once the server's completion message arrives. That
 * completion is a hard guarantee, not a heuristic -- `StreamHub.SubscribeTable` on the server
 * (`shared/StreamForge.Api/Hubs/StreamHub.cs`) is
 * `public Task SubscribeTable(string name) => Groups.AddToGroupAsync(Context.ConnectionId,
 * $"table:{name}")`, i.e. the hub method's OWN return value is the group-add task, so SignalR
 * sends the completion frame only once this connection is actually in the table's broadcast
 * group. Awaiting it before returning means a delta genuinely cannot be missed afterwards.
 * `tableDelta(name, deltas, seq)` arrives as a plain hub callback, registered before the invoke
 * so that a delta racing the completion (a real possibility -- both travel over the same
 * connection but are independent frames, and nothing orders one before the other) is buffered
 * rather than dropped.
 */
class SignalRTransport(baseUrl: String, private val http: AuthClient) : TableTransport {
    override val name = "signalr"

    private val hubUrl = "${baseUrl.trimEnd('/')}/hubs/stream"

    override fun close() = Unit

    override suspend fun snapshot(tableName: String, limit: Int): Pair<List<RowDelta>, Long> {
        // Snapshot is REST for every SignalR wire mode -- there is no "SignalR version" of
        // GET /rows (design doc §3.6).
        val id = RestCatalog.resolveTableId(http, tableName)
        return RestCatalog.snapshotRows(http, id, limit)
    }

    override suspend fun subscribeTable(tableName: String): Flow<DeltaBatch> {
        val deltaListType: Type = TypeToken.getParameterized(List::class.java, WireDelta::class.java).type
        val token = http.token()
        val connection: HubConnection = HubConnectionBuilder.create(hubUrl)
            .withAccessTokenProvider(Single.just(token))
            .build()

        // The slow part (negotiate -> websocket upgrade -> hub protocol handshake) happens HERE,
        // as part of this suspend call, NOT lazily inside a Flow -- see
        // [TableTransport.subscribeTable]'s doc for why that distinction is load-bearing.
        withContext(Dispatchers.IO) { connection.start().blockingAwait() }

        // A plain Channel, not callbackFlow -- `on(...)` must be registered, and therefore able
        // to push somewhere, BEFORE `invoke(...)` is even called (see class doc: a delta can
        // arrive while the invoke's completion is still in flight), and callbackFlow's builder
        // block would defer that registration to collection time, reopening the exact race this
        // whole method exists to close.
        val channel = Channel<DeltaBatch>(Channel.BUFFERED)
        val subscription = connection.on(
            "tableDelta",
            { name: String, deltas: List<WireDelta>, seq: Long ->
                if (name == tableName) {
                    channel.trySend(DeltaBatch(deltas.map { RowDelta(it.row, it.weight) }, seq))
                }
            },
            String::class.java,
            deltaListType,
            Long::class.javaObjectType,
        )

        // `invoke`, not `send`: awaits the server's completion of Groups.AddToGroupAsync (see
        // class doc). By the time this returns, the subscription is a fact on the server, not a
        // hope on the wire.
        withContext(Dispatchers.IO) { connection.invoke("SubscribeTable", tableName).blockingAwait() }

        return channel.consumeAsFlow().onCompletion {
            subscription.unsubscribe()
            runCatching { connection.stop().blockingAwait() }
            connection.close()
        }
    }
}
