package streamsforge

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

/**
 * Contract tests against a REAL, isolated StreamsForge instance ([EngineFixture]), parametrized
 * over every live transport (gRPC + SignalR) -- one set of assertions proving they're actually
 * interchangeable, per [TableTransport]'s whole reason for existing (design doc §3.6/§8). Ported
 * from the Python client's `tests/test_contract.py`.
 *
 * All tests share one engine instance ([EngineFixture] boots once per class) and its two fixture
 * tables -- [Order] pins `handshakeAndSnapshot` first, mirroring the Python suite's file-order
 * reliance on running before anything else has pushed a row into the shared `latest_table`
 * (JUnit5 does not otherwise guarantee method execution order).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class ContractTest {
    private lateinit var handle: EngineFixture.Handle

    @BeforeAll
    fun startEngine() {
        val skipReason = EngineFixture.preconditionsOrSkipReason()
        assumeTrue(skipReason == null, skipReason)
        handle = EngineFixture.start()
    }

    @AfterAll
    fun stopEngine() {
        if (::handle.isInitialized) EngineFixture.stop(handle)
    }

    private suspend fun connectVia(transport: Transport): StreamsForgeClient {
        val client = StreamsForge.connect(
            url = EngineFixture.baseUrl,
            grpcTarget = EngineFixture.grpcTarget,
            user = EngineFixture.ADMIN_USER,
            password = EngineFixture.ADMIN_PASS,
            transport = transport,
        )
        assertEquals(transport.name.lowercase(), client.transportName)
        return client
    }

    @Order(1)
    @ParameterizedTest
    @EnumSource(Transport::class, names = ["GRPC", "SIGNALR"])
    @Timeout(90)
    fun handshakeAndSnapshot(transport: Transport) = runBlocking {
        connectVia(transport).use { sf ->
            // A freshly-imported table has no rows yet -- snapshot must succeed and be empty,
            // not error.
            val rows = sf.snapshot(EngineFixture.LATEST_TABLE)
            assertTrue(rows.isEmpty())
        }
    }

    @Order(2)
    @ParameterizedTest
    @EnumSource(Transport::class, names = ["GRPC", "SIGNALR"])
    @Timeout(90)
    fun pushThenLiveTableSeesIt(transport: Transport) = runBlocking {
        connectVia(transport).use { sf ->
            val tradeId = "t-${UUID.randomUUID().toString().take(8)}"
            val ack = sf.push(
                EngineFixture.SOURCE_NAME,
                listOf(mapOf("trade_id" to tradeId, "desk" to "Rates", "notional" to 100.0)),
            )
            assertEquals(1, ack.accepted)

            // REST ingest (used here under SIGNALR -- gRPC's bidi Ingest is synchronous
            // backpressure, so a GRPC-transport push is already committed by the time `ack`
            // returns) acks 202 once admitted, not once the row store is updated. Subscribing to
            // a FRESH table gets no backfill (design doc), so if this single push's own commit
            // hasn't landed yet, nothing will ever re-assert it and waitFor below would hang
            // until timeout. Wait for the row store itself to catch up first -- same technique
            // `tests/conftest.py`'s `test_reader_thread_reconnects_after_close` uses for the
            // identical race.
            waitForSnapshotToContain(sf, EngineFixture.LATEST_TABLE, tradeId)

            sf.table(EngineFixture.LATEST_TABLE, keyFields = listOf("trade_id")).use { t ->
                val rows = t.waitFor(45.seconds) { r -> r.any { it["trade_id"] == tradeId } }
                val row = rows.first { it["trade_id"] == tradeId }
                assertEquals("Rates", row["desk"])
                assertEquals(100.0, row["notional"])
            }
        }
    }

    private suspend fun waitForSnapshotToContain(sf: StreamsForgeClient, table: String, tradeId: String) {
        val deadline = kotlin.time.TimeSource.Monotonic.markNow() + 15.seconds
        while (sf.snapshot(table).none { it["trade_id"] == tradeId }) {
            if (deadline.hasPassedNow()) org.junit.jupiter.api.Assertions.fail<Unit>("engine snapshot never caught up with the earlier push")
            kotlinx.coroutines.delay(250)
        }
    }

    @Order(3)
    @ParameterizedTest
    @EnumSource(Transport::class, names = ["GRPC", "SIGNALR"])
    @Timeout(90)
    fun supersessionLatestBy(transport: Transport) = runBlocking {
        connectVia(transport).use { sf ->
            val tradeId = "t-${UUID.randomUUID().toString().take(8)}"
            sf.table(EngineFixture.LATEST_TABLE, keyFields = listOf("trade_id")).use { t ->
                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to tradeId, "desk" to "Rates", "notional" to 100.0)))
                t.waitFor(45.seconds) { r -> r.any { it["trade_id"] == tradeId } }

                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to tradeId, "desk" to "Rates", "notional" to 250.0)))
                val rows = t.waitFor(45.seconds) { r ->
                    val match = r.filter { it["trade_id"] == tradeId }
                    match.size == 1 && match.first()["notional"] == 250.0
                }
                // LATEST BY: exactly one row for this trade_id, never two.
                assertEquals(1, rows.count { it["trade_id"] == tradeId })
            }
        }
    }

    @Order(4)
    @ParameterizedTest
    @EnumSource(Transport::class, names = ["GRPC", "SIGNALR"])
    @Timeout(90)
    fun globalAggregateReflectsPushes(transport: Transport) = runBlocking {
        connectVia(transport).use { sf ->
            val desk = "Desk-${UUID.randomUUID().toString().take(6)}"
            sf.table(EngineFixture.AGG_TABLE, keyFields = listOf("desk")).use { agg ->
                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to "a-${UUID.randomUUID()}", "desk" to desk, "notional" to 40.0)))
                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to "b-${UUID.randomUUID()}", "desk" to desk, "notional" to 60.0)))
                agg.waitFor(45.seconds) { r ->
                    val match = r.filter { it["desk"] == desk }
                    match.size == 1 && match.first()["total"] == 100.0
                }
            }
        }
    }

    @Order(5)
    @ParameterizedTest
    @EnumSource(Transport::class, names = ["GRPC", "SIGNALR"])
    @Timeout(30)
    fun ingestRowErrorsOnBadRow(transport: Transport) = runBlocking {
        connectVia(transport).use { sf ->
            // A string where a Double is declared fails coercion under this source's default
            // OnCoercionFailure -- a real rejection, not a lenient null-fill.
            val ex = runCatching {
                sf.push(
                    EngineFixture.SOURCE_NAME,
                    listOf(mapOf("trade_id" to "t-${UUID.randomUUID()}", "desk" to "Ops", "notional" to "not-a-number")),
                )
            }.exceptionOrNull()
            assertTrue(ex is IngestRejectedException, "expected IngestRejectedException, got $ex")
        }
    }

    @Order(6)
    @ParameterizedTest
    @EnumSource(Transport::class, names = ["GRPC", "SIGNALR"])
    @Timeout(30)
    fun validateRejectsBadSql(transport: Transport) = runBlocking {
        connectVia(transport).use { sf ->
            val ex = runCatching {
                sf.sql("SELECT nonexistent_column FROM nowhere_table", name = "bad_${UUID.randomUUID().toString().take(6)}")
            }.exceptionOrNull()
            assertTrue(ex is SqlException, "expected SqlException, got $ex")
            assertTrue((ex as SqlException).diagnostics.isNotEmpty())
        }
    }

    @Order(7)
    @ParameterizedTest
    @EnumSource(Transport::class, names = ["GRPC", "SIGNALR"])
    @Timeout(90)
    fun keyFieldsResolvedFromEngineForLatestBy(transport: Transport) = runBlocking {
        // Wishlist #18: sf.table() with keyFields omitted must read the table's own keyFields
        // (GET /api/tables) instead of a hand-maintained map -- this client never had one, so the
        // proof is that the SAME supersession behavior as supersessionLatestBy above still holds
        // with no keyFields argument at all.
        connectVia(transport).use { sf ->
            val tradeId = "t-${UUID.randomUUID().toString().take(8)}"
            sf.table(EngineFixture.LATEST_TABLE).use { t -> // no keyFields=
                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to tradeId, "desk" to "Rates", "notional" to 100.0)))
                t.waitFor(45.seconds) { r -> r.any { it["trade_id"] == tradeId } }

                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to tradeId, "desk" to "Rates", "notional" to 250.0)))
                val rows = t.waitFor(45.seconds) { r ->
                    val match = r.filter { it["trade_id"] == tradeId }
                    match.size == 1 && match.first()["notional"] == 250.0
                }
                assertEquals(1, rows.count { it["trade_id"] == tradeId })
            }
        }
    }

    @Order(8)
    @ParameterizedTest
    @EnumSource(Transport::class, names = ["GRPC", "SIGNALR"])
    @Timeout(90)
    fun keyFieldsResolvedFromEngineForGroupBy(transport: Transport) = runBlocking {
        connectVia(transport).use { sf ->
            val desk = "Desk-${UUID.randomUUID().toString().take(6)}"
            sf.table(EngineFixture.AGG_TABLE).use { agg -> // no keyFields=
                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to "a-${UUID.randomUUID()}", "desk" to desk, "notional" to 40.0)))
                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to "b-${UUID.randomUUID()}", "desk" to desk, "notional" to 60.0)))
                val rows = agg.waitFor(45.seconds) { r ->
                    val match = r.filter { it["desk"] == desk }
                    match.size == 1 && match.first()["total"] == 100.0
                }
                assertEquals(1, rows.count { it["desk"] == desk })
            }
        }
    }

    @Order(9)
    @ParameterizedTest
    @EnumSource(Transport::class, names = ["GRPC", "SIGNALR"])
    @Timeout(90)
    fun keyFieldsResolvedFromEngineForGlobalAggregateStaysOneRow(transport: Transport) = runBlocking {
        // No GROUP BY at all -- engine-resolved keyFields is [] (TableDefinition.KeyFields's "one
        // global group" state, not "no identity"). If the resolver ever collapsed [] to null
        // (whole-row identity) this table would grow a duplicate row on the second push instead
        // of superseding down to exactly one.
        connectVia(transport).use { sf ->
            sf.table(EngineFixture.GLOBAL_AGG_TABLE).use { t -> // no keyFields=
                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to "g-${UUID.randomUUID()}", "desk" to "Global", "notional" to 10.0)))
                t.waitFor(45.seconds) { r -> r.isNotEmpty() }

                sf.push(EngineFixture.SOURCE_NAME, listOf(mapOf("trade_id" to "g-${UUID.randomUUID()}", "desk" to "Global", "notional" to 20.0)))
                val rows = t.waitFor(45.seconds) { r ->
                    r.size == 1 && (((r.first()["trade_count"] as? Number)?.toLong() ?: 0L) >= 2L)
                }
                assertEquals(1, rows.size)
            }
        }
    }

    @Order(10)
    @ParameterizedTest
    @EnumSource(Transport::class, names = ["GRPC", "SIGNALR"])
    @Timeout(90)
    fun adhocSqlRoundtrip(transport: Transport) = runBlocking {
        connectVia(transport).use { sf ->
            val name = "adhoc_roundtrip_${UUID.randomUUID().toString().take(6)}"
            val q = sf.sql(
                "SELECT desk, SUM(notional) AS total FROM ${EngineFixture.LATEST_TABLE} GROUP BY desk",
                name = name,
                keyFields = listOf("desk"),
            )
            try {
                assertTrue(q.ready)
                val listing = sf.adhocTables()
                assertTrue(listing.any { it["name"] == name })
            } finally {
                q.close()
                assertTrue(sf.dropAdhoc(name))
                assertTrue(!sf.dropAdhoc(name)) // already gone
            }
        }
    }
}
