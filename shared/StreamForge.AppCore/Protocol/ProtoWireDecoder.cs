using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using StreamForge.Abstractions;

namespace StreamForge.Host.Grpc.Dynamic;

/// <summary>
/// Decodes protobuf wire bytes produced by <see cref="ProtoWireEncoder"/> back into a dynamic row
/// (a <c>Dictionary&lt;string, object?&gt;</c>) against the SAME <see cref="FieldDef"/> schema +
/// <see cref="FieldNumberMap"/> the encoder used — the exact counterpart, field kind for field kind,
/// envelope for envelope. Plan 006 D-G: this is what lets <c>GrpcSubscriberCore</c> turn a remote
/// StreamForge instance's <c>DynamicFrame.payload</c> bytes back into rows without any codegen
/// (C# has no <c>DynamicMessage</c>, so hand-decoding against the schema is the only option, mirroring
/// <c>ProtoWireEncoder</c>'s own hand-encoding).
///
/// <para><b>Unknown fields</b>: any tag whose field number isn't in the schema's
/// <see cref="FieldNumberMap.Active"/> for the current message scope is skipped whole via
/// <see cref="CodedInputStream.SkipLastField"/> (wire-type-aware skip — correct for varint, fixed64,
/// and length-delimited alike). This makes decoding forward-compatible with a remote schema that has
/// gained fields the local (possibly stale, snapshot-at-subscribe) schema doesn't know about yet.</para>
///
/// <para><b>Repeated fields</b>: <see cref="FieldDef.IsArray"/> fields are unpacked on the wire (see
/// <see cref="ProtoWireEncoder.WriteRepeatedField"/>) — every tag at that field's number is a separate
/// element. The first occurrence creates a <c>List&lt;object?&gt;</c> and stores it under the field's
/// key; subsequent occurrences append to that SAME list instance.</para>
///
/// <para><b>Absent fields</b>: proto3 "missing = default" means a field that was never written (or
/// whose only value was null at encode time) simply never appears on the wire — so it's simply never
/// added to the decoded dictionary, matching <see cref="ProtoWireEncoder"/>'s omission convention
/// exactly (no explicit null entries are ever produced by this decoder).</para>
///
/// <para><b>Value shapes</b>: string/long/double/bool decode to their obvious CLR types; Timestamp
/// decodes to a <see cref="long"/> (epoch milliseconds, matching what was encoded — the decoder has no
/// way to render it as a <see cref="DateTime"/> without losing the "already epoch millis" contract the
/// rest of the codebase relies on); a nested Json field (declared <see cref="FieldDef.Children"/>)
/// decodes to a <c>Dictionary&lt;string, object?&gt;</c> (recursing into the same message-decode logic
/// at the field's scope); a schemaless Json field decodes its <see cref="Struct"/> submessage into the
/// same plain-CLR value model <see cref="StreamForge.AppCore.Json.JsonValueNormalizer"/> produces
/// elsewhere in this codebase (string/bool/double/null/Dictionary&lt;string,object?&gt;/List&lt;object?&gt;) —
/// note <see cref="Value"/>'s <c>NumberValue</c> is always a C# <c>double</c> (google.protobuf.Struct has
/// no integer number kind), so a schemaless numeric field that started life as a CLR <c>long</c> comes
/// back as a <c>double</c> after an encode→decode round trip through Struct; this is an inherent
/// limitation of the Struct wire representation, not a decoder bug — <see cref="ProtoWireEncoder"/>
/// itself has no way to preserve the distinction on the wire either.</para>
/// </summary>
public static class ProtoWireDecoder
{
    /// <summary>Decodes just the entity row (no envelope) against the schema root scope — the
    /// counterpart of <see cref="ProtoWireEncoder.EncodeRow"/>.</summary>
    public static Dictionary<string, object?> DecodeRow(IReadOnlyList<FieldDef> fields, FieldNumberMap numbers, byte[] payload)
    {
        var row = new Dictionary<string, object?>();
        var input = new CodedInputStream(payload);
        DecodeMessageFields(input, "", fields, numbers, row);
        return row;
    }

    /// <summary>Decodes a <c>{Entity}Event</c> envelope: row (field 1) + seq (field 2, int64) +
    /// ts_ms (field 3, int64) — the counterpart of <see cref="ProtoWireEncoder.EncodeEvent"/>. Any
    /// envelope field not present on the wire (should not happen for well-formed frames) decodes as 0.</summary>
    public static (Dictionary<string, object?> Row, long Seq, long TsMs) DecodeEvent(IReadOnlyList<FieldDef> fields, FieldNumberMap numbers, byte[] payload)
    {
        var input = new CodedInputStream(payload);
        byte[]? rowBytes = null;
        long seq = 0;
        long tsMs = 0;

        uint tag;
        while ((tag = input.ReadTag()) != 0)
        {
            switch (WireFormat.GetTagFieldNumber(tag))
            {
                case 1:
                    rowBytes = input.ReadBytes().ToByteArray();
                    break;
                case 2:
                    seq = input.ReadInt64();
                    break;
                case 3:
                    tsMs = input.ReadInt64();
                    break;
                default:
                    input.SkipLastField();
                    break;
            }
        }

        var row = rowBytes is null ? new Dictionary<string, object?>() : DecodeRow(fields, numbers, rowBytes);
        return (row, seq, tsMs);
    }

    /// <summary>Decodes a <c>{Entity}Delta</c> envelope: row (field 1) + weight (field 2, sint64
    /// zigzag) + seq (field 3, int64) — the counterpart of <see cref="ProtoWireEncoder.EncodeDelta"/>.
    /// Weight is read with <see cref="CodedInputStream.ReadSInt64"/> (zigzag), matching
    /// <see cref="CodedOutputStream.WriteSInt64"/> on the encode side — a raw varint read would decode
    /// negative weights to huge positive values.</summary>
    public static (Dictionary<string, object?> Row, long Weight, long Seq) DecodeDelta(IReadOnlyList<FieldDef> fields, FieldNumberMap numbers, byte[] payload)
    {
        var input = new CodedInputStream(payload);
        byte[]? rowBytes = null;
        long weight = 0;
        long seq = 0;

        uint tag;
        while ((tag = input.ReadTag()) != 0)
        {
            switch (WireFormat.GetTagFieldNumber(tag))
            {
                case 1:
                    rowBytes = input.ReadBytes().ToByteArray();
                    break;
                case 2:
                    weight = input.ReadSInt64();
                    break;
                case 3:
                    seq = input.ReadInt64();
                    break;
                default:
                    input.SkipLastField();
                    break;
            }
        }

        var row = rowBytes is null ? new Dictionary<string, object?>() : DecodeRow(fields, numbers, rowBytes);
        return (row, weight, seq);
    }

    /// <summary>Reads every tag in <paramref name="input"/> until it's exhausted (works uniformly for
    /// the top-level row and for a nested submessage's own byte range, since both are simply "read
    /// tags until there are none left"). Builds a number→FieldDef lookup for <paramref name="scope"/>
    /// from <paramref name="numbers"/>.Active (mirrors <see cref="ProtoWireEncoder.WriteMessageFields"/>'s
    /// number lookup, inverted) so an unrecognized tag can be told apart from a recognized one and
    /// skipped by wire type.</summary>
    private static void DecodeMessageFields(
        CodedInputStream input, string scope, IReadOnlyList<FieldDef> defs, FieldNumberMap numbers, Dictionary<string, object?> row)
    {
        var lookup = BuildNumberLookup(scope, defs, numbers);

        uint tag;
        while ((tag = input.ReadTag()) != 0)
        {
            var number = WireFormat.GetTagFieldNumber(tag);
            if (!lookup.TryGetValue(number, out var f))
            {
                input.SkipLastField(); // unknown field (not in this scope's schema) - skip whole, by wire type
                continue;
            }

            var path = FieldNumberMap.ChildPath(scope, f.Name);
            var value = ReadFieldValue(input, f, path, numbers);

            if (f.IsArray)
            {
                if (row.TryGetValue(f.Name, out var existing) && existing is List<object?> list)
                {
                    list.Add(value);
                }
                else
                {
                    row[f.Name] = new List<object?> { value };
                }
            }
            else
            {
                row[f.Name] = value;
            }
        }
    }

    private static Dictionary<int, FieldDef> BuildNumberLookup(string scope, IReadOnlyList<FieldDef> defs, FieldNumberMap numbers)
    {
        var map = new Dictionary<int, FieldDef>();
        foreach (var f in defs)
        {
            var path = FieldNumberMap.ChildPath(scope, f.Name);
            if (numbers.Active.TryGetValue(path, out var number))
            {
                map[number] = f;
            }
            // A field declared in the schema but with no assigned number (shouldn't happen for a
            // FieldNumberMap produced by FieldNumberMap.Assign/DescriptorFactory.Generate for these
            // exact `defs`) simply can't be matched on the wire - nothing to add.
        }
        return map;
    }

    /// <summary>Reads one value (a scalar leaf, a nested-message element, or a schemaless Json/Struct
    /// element) — the read-side counterpart of <see cref="ProtoWireEncoder.WriteFieldValue"/>, dispatched
    /// on the same <see cref="FieldDef.Type"/>/<see cref="FieldDef.Children"/> combination so a repeated
    /// field's element shape always matches its singular-field counterpart, same as the encoder.</summary>
    private static object? ReadFieldValue(CodedInputStream input, FieldDef f, string path, FieldNumberMap numbers)
    {
        switch (f.Type)
        {
            case FieldType.String:
                return input.ReadString();
            case FieldType.Double:
                return input.ReadDouble();
            case FieldType.Long:
                return input.ReadInt64();
            case FieldType.Bool:
                return input.ReadBool();
            case FieldType.Timestamp:
                return input.ReadInt64(); // already epoch millis, per ProtoWireEncoder
            case FieldType.Json when f.Children is { Count: > 0 }:
            {
                var nestedBytes = input.ReadBytes().ToByteArray();
                var nestedRow = new Dictionary<string, object?>();
                var nestedInput = new CodedInputStream(nestedBytes);
                DecodeMessageFields(nestedInput, path, f.Children, numbers, nestedRow);
                return nestedRow;
            }
            case FieldType.Json:
            {
                var structBytes = input.ReadBytes().ToByteArray();
                var structValue = Struct.Parser.ParseFrom(structBytes);
                return FromStruct(structValue);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(f), f.Type, "Unknown field type");
        }
    }

    private static Dictionary<string, object?> FromStruct(Struct s)
    {
        var result = new Dictionary<string, object?>();
        foreach (var (key, value) in s.Fields)
        {
            result[key] = FromProtoValue(value);
        }
        return result;
    }

    private static object? FromProtoValue(Value value) => value.KindCase switch
    {
        Value.KindOneofCase.NullValue => null,
        Value.KindOneofCase.StringValue => value.StringValue,
        Value.KindOneofCase.BoolValue => value.BoolValue,
        Value.KindOneofCase.NumberValue => value.NumberValue, // Struct has no integer kind - always double
        Value.KindOneofCase.StructValue => FromStruct(value.StructValue),
        Value.KindOneofCase.ListValue => value.ListValue.Values.Select(FromProtoValue).ToList(),
        _ => null, // KindOneofCase.None - an unset Value, treat like null
    };
}
