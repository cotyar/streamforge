package streamforge

import com.google.protobuf.ListValue
import com.google.protobuf.NullValue
import com.google.protobuf.Struct
import com.google.protobuf.Value

/**
 * Tier 1 row payloads travel as `google.protobuf.Struct` (design doc §3.0/§3.1) -- this is the
 * whole "typing" story for gRPC, same as the Python client's `MessageToDict`. Written by hand
 * rather than pulling in protobuf-java-util for one direction of one conversion.
 *
 * `Struct`'s NUMBER_VALUE is an IEEE-754 double, so a `Long` beyond 2^53 loses precision going
 * this way -- a documented edge (design doc §3.0), not something worth fixing here; nothing in
 * the reference demo crosses it.
 */
fun Struct.toRow(): Row = fieldsMap.mapValues { (_, v) -> v.toKotlin() }

private fun Value.toKotlin(): Any? = when (kindCase) {
    Value.KindCase.NULL_VALUE -> null
    Value.KindCase.NUMBER_VALUE -> numberValue
    Value.KindCase.STRING_VALUE -> stringValue
    Value.KindCase.BOOL_VALUE -> boolValue
    Value.KindCase.STRUCT_VALUE -> structValue.toRow()
    Value.KindCase.LIST_VALUE -> listValue.valuesList.map { it.toKotlin() }
    Value.KindCase.KIND_NOT_SET, null -> null
}

fun Row.toStruct(): Struct {
    val builder = Struct.newBuilder()
    for ((k, v) in this) builder.putFields(k, v.toProtoValue())
    return builder.build()
}

private fun Any?.toProtoValue(): Value = when (this) {
    null -> Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()
    is String -> Value.newBuilder().setStringValue(this).build()
    is Boolean -> Value.newBuilder().setBoolValue(this).build()
    is Number -> Value.newBuilder().setNumberValue(this.toDouble()).build()
    is Map<*, *> -> Value.newBuilder()
        .setStructValue(Struct.newBuilder().apply {
            @Suppress("UNCHECKED_CAST")
            for ((k, v) in this@toProtoValue as Map<String, Any?>) putFields(k, v.toProtoValue())
        }.build())
        .build()
    is List<*> -> Value.newBuilder()
        .setListValue(ListValue.newBuilder().addAllValues(this.map { it.toProtoValue() }).build())
        .build()
    else -> Value.newBuilder().setStringValue(this.toString()).build()
}
