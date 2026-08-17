package streamforge

import com.google.gson.Gson
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import java.io.File

private data class WireRowDelta(val row: Map<String, Any?>, val weight: Long)
private data class WireBatch(val deltas: List<WireRowDelta>, val seq: Long)
private data class ConformanceCase(
    val name: String,
    val description: String,
    val keyFields: List<String>?,
    val bufferedBatches: List<WireBatch>,
    val snapshot: List<WireRowDelta>,
    val liveBatches: List<WireBatch>,
    val expectedRows: List<Map<String, Any?>>,
)
private data class ConformanceFile(val version: Int, val cases: List<ConformanceCase>)

/**
 * Reads `clients/conformance/zset-cases.json` (a sibling of `clients/kotlin`) and runs the runner
 * contract documented in that suite's README VERBATIM:
 *
 * ```
 * z = ZSet(case.keyFields)
 * z.seed(case.snapshot)
 * for b in case.bufferedBatches:
 *     if not z.alreadyReflected(b.deltas): z.apply(b.deltas)
 * for b in case.liveBatches:
 *     z.apply(b.deltas)
 * assert rows(z) == case.expectedRows  (order-insensitive)
 * ```
 *
 * This is THE cross-language conformance suite -- every StreamForge client is required to run it,
 * not just Kotlin's own hand-written tests. All 14 cases must pass.
 */
class ZSetConformanceTest {
    @TestFactory
    fun conformanceCases(): List<DynamicTest> {
        val file = File(System.getProperty("user.dir"), "../conformance/zset-cases.json")
        check(file.exists()) { "conformance fixture not found at ${file.absolutePath}" }
        val fixture = Gson().fromJson(file.readText(), ConformanceFile::class.java)
        check(fixture.cases.isNotEmpty()) { "conformance fixture had no cases" }

        return fixture.cases.map { case ->
            DynamicTest.dynamicTest(case.name) {
                val zset = ZSet(case.keyFields)
                zset.seed(case.snapshot.map { RowDelta(it.row, it.weight) })
                for (batch in case.bufferedBatches) {
                    val deltas = batch.deltas.map { RowDelta(it.row, it.weight) }
                    if (!zset.alreadyReflected(deltas)) zset.apply(deltas)
                }
                for (batch in case.liveBatches) {
                    zset.apply(batch.deltas.map { RowDelta(it.row, it.weight) })
                }

                val actualCounts = zset.rows().groupingBy { it }.eachCount()
                val expectedCounts = case.expectedRows.groupingBy { it }.eachCount()
                assertEquals(expectedCounts, actualCounts, "case '${case.name}': ${case.description}")
            }
        }
    }
}
