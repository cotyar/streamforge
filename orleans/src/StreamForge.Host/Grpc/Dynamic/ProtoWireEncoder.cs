using System.Globalization;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using StreamForge.Abstractions;

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

            switch (f.Type)
            {
                case FieldType.String:
                    output.WriteTag(number, WireFormat.WireType.LengthDelimited);
                    output.WriteString(ToStringValue(value));
                    break;
                case FieldType.Double:
                    output.WriteTag(number, WireFormat.WireType.Fixed64);
                    output.WriteDouble(ToDouble(value));
                    break;
                case FieldType.Long:
                    output.WriteTag(number, WireFormat.WireType.Varint);
                    output.WriteInt64(ToInt64(value));
                    break;
                case FieldType.Bool:
                    output.WriteTag(number, WireFormat.WireType.Varint);
                    output.WriteBool(ToBool(value));
                    break;
                case FieldType.Timestamp:
                    output.WriteTag(number, WireFormat.WireType.Varint);
                    output.WriteInt64(ToInt64(value)); // already epoch millis
                    break;
                case FieldType.Json when f.Children is { Count: > 0 }:
                    WriteNestedMessage(output, number, path, f.Children, numbers, value);
                    break;
                case FieldType.Json:
                    WriteStructField(output, number, value);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(defs), f.Type, "Unknown field type");
            }
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

    private static string ToStringValue(object value) => value switch
    {
        string s => s,
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    private static double ToDouble(object value) => value switch
    {
        double d => d,
        float f => f,
        long l => l,
        int i => i,
        bool b => b ? 1 : 0,
        string s => double.Parse(s, CultureInfo.InvariantCulture),
        _ => throw new InvalidOperationException($"Cannot convert {value.GetType()} to double"),
    };

    private static long ToInt64(object value) => value switch
    {
        long l => l,
        int i => i,
        double d => (long)d,
        float f => (long)f,
        bool b => b ? 1L : 0L,
        string s => long.Parse(s, CultureInfo.InvariantCulture),
        _ => throw new InvalidOperationException($"Cannot convert {value.GetType()} to long"),
    };

    private static bool ToBool(object value) => value switch
    {
        bool b => b,
        long l => l != 0,
        int i => i != 0,
        double d => d != 0,
        string s => bool.TryParse(s, out var parsed) ? parsed : s is not ("" or "0"),
        _ => throw new InvalidOperationException($"Cannot convert {value.GetType()} to bool"),
    };
}
