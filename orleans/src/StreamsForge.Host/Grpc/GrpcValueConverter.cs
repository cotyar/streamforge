using System.Linq;
using Google.Protobuf.WellKnownTypes;

namespace StreamsForge.Host.Grpc;

/// <summary>
/// Converts dynamic row data (EventRecord / ResultEnvelope.Row / TableDeltaDto.Row — all
/// Dictionary&lt;string, object?&gt; with primitive or nested Dictionary/List leaves) to
/// google.protobuf.Struct/Value for gRPC transport. Tier 1 carries rows dynamically; a future
/// tier may add typed row messages per-source/-pipeline/-table.
/// </summary>
public static class GrpcValueConverter
{
    public static Struct ToStruct(IReadOnlyDictionary<string, object?> row)
    {
        var result = new Struct();
        foreach (var (key, value) in row)
        {
            result.Fields[key] = ToValue(value);
        }

        return result;
    }

    public static Value ToValue(object? value)
    {
        switch (value)
        {
            case null:
                return Value.ForNull();
            case bool b:
                return Value.ForBool(b);
            case string s:
                return Value.ForString(s);
            case double d:
                return Value.ForNumber(d);
            case float f:
                return Value.ForNumber(f);
            case long l:
                // Protobuf JSON/Value numbers are IEEE-754 doubles; a long beyond 2^53 would lose
                // precision going through Value.ForNumber, so carry it as a string instead (the same
                // trade-off protobuf-json makes for int64 fields).
                return IsSafeInteger(l) ? Value.ForNumber(l) : Value.ForString(l.ToString());
            case int i:
                return Value.ForNumber(i);
            case IReadOnlyDictionary<string, object?> nestedDict:
                return Value.ForStruct(ToStruct(nestedDict));
            case IEnumerable<object?> list:
                var listValue = new ListValue();
                foreach (var item in list)
                {
                    listValue.Values.Add(ToValue(item));
                }

                return Value.ForList([.. listValue.Values]);
            default:
                // Fallback for any other primitive (e.g. DateTime, enum) — string representation.
                return Value.ForString(value.ToString() ?? "");
        }
    }

    private const long MaxSafeInteger = 9_007_199_254_740_992; // 2^53

    private static bool IsSafeInteger(long l) => l is > -MaxSafeInteger and < MaxSafeInteger;

    // ------------------------------------------------------------------
    // Struct/Value -> Dictionary/object? (plan 008 W4: the reverse direction, needed by
    // IngestGrpcService — every prior gRPC surface only ever WROTE Struct/Value (server-streaming
    // reads); ingest is the first RPC that RECEIVES row data). Produces the same plain-CLR-leaf shape
    // (string/double/bool/Dictionary/List, no protobuf types) that IngressRowAcceptance.Accept expects
    // — equivalent to what JsonValueNormalizer does for the REST path's System.Text.Json JsonElement
    // leaves, just against protobuf's own typed Value oneof instead.
    // ------------------------------------------------------------------

    public static Dictionary<string, object?> FromStruct(Struct s)
    {
        var result = new Dictionary<string, object?>();
        foreach (var (key, value) in s.Fields)
        {
            result[key] = FromValue(value);
        }

        return result;
    }

    public static object? FromValue(Value v) => v.KindCase switch
    {
        Value.KindOneofCase.NullValue => null,
        Value.KindOneofCase.BoolValue => v.BoolValue,
        Value.KindOneofCase.NumberValue => v.NumberValue,
        Value.KindOneofCase.StringValue => v.StringValue,
        Value.KindOneofCase.StructValue => (object)FromStruct(v.StructValue),
        Value.KindOneofCase.ListValue => v.ListValue.Values.Select(FromValue).ToList(),
        _ => null, // KindOneofCase.None — an unset Value has no meaningful leaf.
    };
}
