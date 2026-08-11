using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;

namespace StreamForge.Host.Grpc.Dynamic;

/// <summary>
/// Encodes a dynamic row (an EventRecord / Dictionary&lt;string, object?&gt;) straight to protobuf
/// wire format against a StreamForge <see cref="FieldDef"/> schema — no codegen, no DynamicMessage
/// (C# has none): field tags/wire-types are derived directly from the schema + a
/// <see cref="FieldNumberMap"/>, matching exactly what <see cref="DescriptorFactory"/> would produce
/// for the same inputs.
///
/// <para>Value handling: string/double/long/int/bool/Dictionary&lt;string,object?&gt;/List&lt;object?&gt;/null
/// are all accepted for a row's values. A missing key or a null value omits the field entirely
/// (valid proto3 "absent = default" semantics). A <i>present</i> value — including 0, "", or false —
/// is always written; this library does not additionally suppress explicit zero/empty/false values
/// the way idiomatic protoc-generated code does, which keeps "what's in the row is what's on the
/// wire" simple to reason about and to hand-verify in tests. Both behaviors are valid, parseable
/// proto3 wire data.</para>
///
/// <para>Wire types: string/nested-message/Struct → length-delimited; double → fixed64; long/bool/
/// Timestamp(millis) → varint; the envelope's <c>weight</c> → sint64 (zigzag varint) via
/// <see cref="CodedOutputStream.WriteSInt64"/>, which zigzag-encodes internally.</para>
///
/// <para><see cref="FieldDef.IsArray"/> fields expect a <c>List&lt;object?&gt;</c> (or other
/// <c>IEnumerable&lt;object?&gt;</c>) value; each non-null element is written as its own tag+value entry
/// at the field's number (plain/unpacked repeated — see <see cref="WriteRepeatedField"/>). An empty or
/// all-null list omits the field entirely, same as a missing/null scalar.</para>
/// </summary>
public static class ProtoWireEncoder
{
    /// <summary>Encodes just the entity row (no envelope) against the schema root scope.</summary>
    public static byte[] EncodeRow(IReadOnlyList<FieldDef> fields, FieldNumberMap numbers, IReadOnlyDictionary<string, object?> row)
    {
        using var ms = new MemoryStream();
        using (var output = new CodedOutputStream(ms, leaveOpen: true))
        {
            WriteMessageFields(output, "", fields, numbers, row);
            output.Flush();
        }
        return ms.ToArray();
    }

    /// <summary>Encodes a <c>{Entity}Event</c> envelope: row (field 1) + seq (field 2, int64) +
    /// ts_ms (field 3, int64) — matching the layout <see cref="DescriptorFactory"/> generates.</summary>
    public static byte[] EncodeEvent(IReadOnlyList<FieldDef> fields, FieldNumberMap numbers, IReadOnlyDictionary<string, object?> row, long seq, long tsMs)
    {
        var rowBytes = EncodeRow(fields, numbers, row);
        using var ms = new MemoryStream();
        using (var output = new CodedOutputStream(ms, leaveOpen: true))
        {
            output.WriteTag(1, WireFormat.WireType.LengthDelimited);
            output.WriteBytes(ByteString.CopyFrom(rowBytes));
            output.WriteTag(2, WireFormat.WireType.Varint);
            output.WriteInt64(seq);
            output.WriteTag(3, WireFormat.WireType.Varint);
            output.WriteInt64(tsMs);
            output.Flush();
        }
        return ms.ToArray();
    }

    /// <summary>Encodes a <c>{Entity}Delta</c> envelope: row (field 1) + weight (field 2, sint64
    /// zigzag) + seq (field 3, int64) — matching the layout <see cref="DescriptorFactory"/> generates.</summary>
    public static byte[] EncodeDelta(IReadOnlyList<FieldDef> fields, FieldNumberMap numbers, IReadOnlyDictionary<string, object?> row, long weight, long seq)
    {
        var rowBytes = EncodeRow(fields, numbers, row);
        using var ms = new MemoryStream();
        using (var output = new CodedOutputStream(ms, leaveOpen: true))
        {
            output.WriteTag(1, WireFormat.WireType.LengthDelimited);
            output.WriteBytes(ByteString.CopyFrom(rowBytes));
            output.WriteTag(2, WireFormat.WireType.Varint);
            output.WriteSInt64(weight);
            output.WriteTag(3, WireFormat.WireType.Varint);
            output.WriteInt64(seq);
            output.Flush();
        }
        return ms.ToArray();
    }

    private static void WriteMessageFields(
        CodedOutputStream output, string scope, IReadOnlyList<FieldDef> defs, FieldNumberMap numbers, IReadOnlyDictionary<string, object?> row)
    {
        foreach (var f in defs)
        {
            if (!row.TryGetValue(f.Name, out var value) || value is null)
            {
                continue; // proto3: missing key or null value -> omit field
            }

            var path = FieldNumberMap.ChildPath(scope, f.Name);
            var number = numbers.Active[path];

            if (f.IsArray)
            {
                WriteRepeatedField(output, number, path, f, numbers, value);
            }
            else
            {
                WriteFieldValue(output, number, path, f, numbers, value);
            }
        }
    }

    /// <summary>Writes an IsArray field as plain (non-packed) repeated entries: one tag+value per
    /// element, all at the same field number — valid proto3 wire data for every field kind this library
    /// emits (length-delimited kinds have no packed form at all; for varint/fixed64 scalar kinds, a
    /// conformant reader accepts unpacked entries interchangeably with the packed form real protoc
    /// output would use, per the protobuf encoding spec). An empty (or entirely-null) list writes
    /// nothing, so the field is omitted exactly like a missing/null scalar.</summary>
    private static void WriteRepeatedField(
        CodedOutputStream output, int number, string path, FieldDef f, FieldNumberMap numbers, object value)
    {
        if (value is not IEnumerable<object?> list)
        {
            throw new InvalidOperationException(
                $"Field \"{path}\" is declared IsArray and requires a List<object?> (or other IEnumerable<object?>) value; got {value.GetType()}.");
        }

        foreach (var element in list)
        {
            if (element is null)
            {
                continue; // proto3: a null list element carries no value to write - skip it, matching the missing/null-scalar convention above
            }
            WriteFieldValue(output, number, path, f, numbers, element);
        }
    }

    /// <summary>Writes one value (a scalar leaf, a nested-message element, or a schemaless Json/Struct
    /// element) at <paramref name="number"/>, dispatching on <paramref name="f"/>.Type exactly like the
    /// non-array case — shared by both the singular field path and each element of a repeated field, so
    /// element shape is guaranteed identical to the non-array shape for the same FieldDef.Type/Children.</summary>
    private static void WriteFieldValue(
        CodedOutputStream output, int number, string path, FieldDef f, FieldNumberMap numbers, object value)
    {
        switch (f.Type)
        {
            case FieldType.String:
                output.WriteTag(number, WireFormat.WireType.LengthDelimited);
                output.WriteString((string)CoerceOrThrow(f, path, value));
                break;
            case FieldType.Double:
                output.WriteTag(number, WireFormat.WireType.Fixed64);
                output.WriteDouble((double)CoerceOrThrow(f, path, value));
                break;
            case FieldType.Long:
                output.WriteTag(number, WireFormat.WireType.Varint);
                output.WriteInt64((long)CoerceOrThrow(f, path, value));
                break;
            case FieldType.Bool:
                output.WriteTag(number, WireFormat.WireType.Varint);
                output.WriteBool((bool)CoerceOrThrow(f, path, value));
                break;
            case FieldType.Timestamp:
                output.WriteTag(number, WireFormat.WireType.Varint);
                output.WriteInt64((long)CoerceOrThrow(f, path, value)); // already epoch millis
                break;
            case FieldType.Json when f.Children is { Count: > 0 }:
                WriteNestedMessage(output, number, path, f.Children, numbers, value);
                break;
            case FieldType.Json:
                WriteStructField(output, number, value);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(f), f.Type, "Unknown field type");
        }
    }

    private static void WriteNestedMessage(
        CodedOutputStream output, int number, string path, List<FieldDef> children, FieldNumberMap numbers, object value)
    {
        if (value is not IReadOnlyDictionary<string, object?> nestedRow)
        {
            throw new InvalidOperationException(
                $"Field \"{path}\" is a nested Json schema and requires a Dictionary<string, object?> value; got {value.GetType()}.");
        }

        using var nestedMs = new MemoryStream();
        using (var nestedOutput = new CodedOutputStream(nestedMs, leaveOpen: true))
        {
            WriteMessageFields(nestedOutput, path, children, numbers, nestedRow);
            nestedOutput.Flush();
        }

        output.WriteTag(number, WireFormat.WireType.LengthDelimited);
        output.WriteBytes(ByteString.CopyFrom(nestedMs.ToArray()));
    }

    private static void WriteStructField(CodedOutputStream output, int number, object value)
    {
        if (value is not IReadOnlyDictionary<string, object?> dict)
        {
            throw new InvalidOperationException(
                $"Schemaless Json field expects a Dictionary<string, object?> value at the top level (encodes as google.protobuf.Struct); got {value.GetType()}.");
        }

        var structValue = ToStruct(dict);
        output.WriteTag(number, WireFormat.WireType.LengthDelimited);
        output.WriteMessage(structValue);
    }

    private static Struct ToStruct(IReadOnlyDictionary<string, object?> dict)
    {
        var s = new Struct();
        foreach (var (key, value) in dict)
        {
            s.Fields[key] = ToProtoValue(value);
        }
        return s;
    }

    private static Value ToProtoValue(object? value) => value switch
    {
        null => Value.ForNull(),
        string s => Value.ForString(s),
        bool b => Value.ForBool(b),
        double d => Value.ForNumber(d),
        float f => Value.ForNumber(f),
        long l => Value.ForNumber(l),
        int i => Value.ForNumber(i),
        IReadOnlyDictionary<string, object?> nested => Value.ForStruct(ToStruct(nested)),
        IEnumerable<object?> list => Value.ForList([.. list.Select(ToProtoValue)]),
        _ => throw new InvalidOperationException($"Unsupported value type in schemaless Json subtree: {value.GetType()}"),
    };

    /// <summary>Scalar coercion is shared with the connector-mapping and client-push ingest paths
    /// via <see cref="FieldValueCoercion.TryCoerce"/> (extracted plan 008 W4) — this just restores
    /// the throwing contract this encoder always had: it encodes already-accepted rows, so a value
    /// that cannot be coerced here is a bug upstream, not something to report per-row.</summary>
    private static object CoerceOrThrow(FieldDef f, string path, object value)
    {
        if (!FieldValueCoercion.TryCoerce(f.Type, value, out var coerced))
        {
            throw new InvalidOperationException($"Field \"{path}\" (type {f.Type}) cannot convert {value.GetType()} value");
        }
        return coerced!;
    }
}
