using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// The key test suite: encode rows with <see cref="ProtoWireEncoder"/>, then parse the resulting
/// bytes back BY HAND with <see cref="CodedInputStream"/> — asserting the exact tag (field number +
/// wire type) and value StreamForge's own encoder is expected to produce. C# has no DynamicMessage,
/// so this hand-decoding is the only way to verify wire-format correctness without generating and
/// compiling protoc output as part of the test run.
/// </summary>
public class WireRoundTripTests
{
    private static void AssertTag(CodedInputStream input, int expectedField, WireFormat.WireType expectedWireType)
    {
        var tag = input.ReadTag();
        Assert.Equal(expectedField, WireFormat.GetTagFieldNumber(tag));
        Assert.Equal(expectedWireType, WireFormat.GetTagWireType(tag));
    }

    [Fact]
    public void All_scalar_types_encode_with_correct_tags_wire_types_and_values()
    {
        var fields = TestHelpers.FlatFields; // symbol:String, price:Double, qty:Long, active:Bool, traded_at:Timestamp
        var numbers = FieldNumberMap.Assign(fields); // sequential 1..5 in declaration order (documented default)

        var row = new Dictionary<string, object?>
        {
            ["symbol"] = "AAPL",
            ["price"] = 189.5,
            ["qty"] = 42L,
            ["active"] = true,
            ["traded_at"] = 1_700_000_000_000L,
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, 1, WireFormat.WireType.LengthDelimited);
        Assert.Equal("AAPL", input.ReadString());

        AssertTag(input, 2, WireFormat.WireType.Fixed64); // double is wire-type fixed64
        Assert.Equal(189.5, input.ReadDouble());

        AssertTag(input, 3, WireFormat.WireType.Varint);
        Assert.Equal(42L, input.ReadInt64());

        AssertTag(input, 4, WireFormat.WireType.Varint);
        Assert.True(input.ReadBool());

        AssertTag(input, 5, WireFormat.WireType.Varint); // Timestamp -> int64 millis, varint
        Assert.Equal(1_700_000_000_000L, input.ReadInt64());

        Assert.True(input.IsAtEnd);
    }

    [Fact]
    public void Missing_keys_omit_the_field_entirely()
    {
        var fields = TestHelpers.FlatFields;
        var numbers = FieldNumberMap.Assign(fields);

        // Only symbol and qty present; price/active/traded_at keys are absent from the row.
        var row = new Dictionary<string, object?> { ["symbol"] = "MSFT", ["qty"] = 7L };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, 1, WireFormat.WireType.LengthDelimited);
        Assert.Equal("MSFT", input.ReadString());

        AssertTag(input, 3, WireFormat.WireType.Varint); // qty, not 2 (price) - price was skipped entirely
        Assert.Equal(7L, input.ReadInt64());

        Assert.True(input.IsAtEnd);
    }

    [Fact]
    public void Null_values_are_treated_like_missing_keys()
    {
        var fields = TestHelpers.FlatFields;
        var numbers = FieldNumberMap.Assign(fields);

        var row = new Dictionary<string, object?>
        {
            ["symbol"] = "GOOG",
            ["price"] = null,
            ["qty"] = 5L,
            ["active"] = null,
            ["traded_at"] = 100L,
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, 1, WireFormat.WireType.LengthDelimited);
        Assert.Equal("GOOG", input.ReadString());
        AssertTag(input, 3, WireFormat.WireType.Varint);
        Assert.Equal(5L, input.ReadInt64());
        AssertTag(input, 5, WireFormat.WireType.Varint);
        Assert.Equal(100L, input.ReadInt64());
        Assert.True(input.IsAtEnd);
    }

    [Fact]
    public void Unicode_strings_round_trip_exactly()
    {
        var fields = new List<FieldDef> { new("name", FieldType.String) };
        var numbers = FieldNumberMap.Assign(fields);
        const string text = "héllo wörld 你好 🎉";

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, new Dictionary<string, object?> { ["name"] = text });
        var input = new CodedInputStream(bytes);

        AssertTag(input, 1, WireFormat.WireType.LengthDelimited);
        Assert.Equal(text, input.ReadString());
        Assert.True(input.IsAtEnd);
    }

    [Fact]
    public void Scalar_values_arriving_as_int_or_float_still_encode_correctly()
    {
        var fields = new List<FieldDef> { new("qty", FieldType.Long), new("price", FieldType.Double) };
        var numbers = FieldNumberMap.Assign(fields);

        var row = new Dictionary<string, object?> { ["qty"] = 9, ["price"] = 4.5f }; // int, float (not long, double)

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, 1, WireFormat.WireType.Varint);
        Assert.Equal(9L, input.ReadInt64());
        AssertTag(input, 2, WireFormat.WireType.Fixed64);
        Assert.Equal(4.5, input.ReadDouble());
        Assert.True(input.IsAtEnd);
    }

    [Fact]
    public void Empty_row_encodes_to_zero_bytes()
    {
        var fields = TestHelpers.FlatFields;
        var numbers = FieldNumberMap.Assign(fields);

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, new Dictionary<string, object?>());

        Assert.Empty(bytes);
    }

    [Fact]
    public void Nested_message_encodes_as_a_length_delimited_embedded_message()
    {
        var fields = TestHelpers.NestedJsonFields; // symbol:String, payload:Json{ user:Json{id,tier}, amount:Double }
        var numbers = FieldNumberMap.Assign(fields);

        var row = new Dictionary<string, object?>
        {
            ["symbol"] = "AAPL",
            ["payload"] = new Dictionary<string, object?>
            {
                ["user"] = new Dictionary<string, object?> { ["id"] = "u1", ["tier"] = "gold" },
                ["amount"] = 99.5,
            },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, numbers.Active["symbol"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("AAPL", input.ReadString());

        AssertTag(input, numbers.Active["payload"], WireFormat.WireType.LengthDelimited);
        var payloadBytes = input.ReadBytes().ToByteArray();
        Assert.True(input.IsAtEnd);

        var payloadInput = new CodedInputStream(payloadBytes);
        AssertTag(payloadInput, numbers.Active["payload.user"], WireFormat.WireType.LengthDelimited);
        var userBytes = payloadInput.ReadBytes().ToByteArray();

        AssertTag(payloadInput, numbers.Active["payload.amount"], WireFormat.WireType.Fixed64);
        Assert.Equal(99.5, payloadInput.ReadDouble());
        Assert.True(payloadInput.IsAtEnd);

        var userInput = new CodedInputStream(userBytes);
        AssertTag(userInput, numbers.Active["payload.user.id"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("u1", userInput.ReadString());
        AssertTag(userInput, numbers.Active["payload.user.tier"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("gold", userInput.ReadString());
        Assert.True(userInput.IsAtEnd);
    }

    [Fact]
    public void Schemaless_json_field_encodes_as_a_google_protobuf_struct_parseable_by_Struct_Parser()
    {
        var fields = TestHelpers.SchemalessJsonFields; // symbol:String, meta:Json (no children -> Struct)
        var numbers = FieldNumberMap.Assign(fields);

        var row = new Dictionary<string, object?>
        {
            ["symbol"] = "AAPL",
            ["meta"] = new Dictionary<string, object?>
            {
                ["active"] = true,
                ["score"] = 12.5,
                ["note"] = "hi",
                ["nested"] = new Dictionary<string, object?> { ["x"] = 1.0 },
                ["tags"] = new List<object?> { "a", "b" },
                ["nothing"] = null,
            },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, numbers.Active["symbol"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("AAPL", input.ReadString());

        AssertTag(input, numbers.Active["meta"], WireFormat.WireType.LengthDelimited);
        var metaBytes = input.ReadBytes().ToByteArray();
        Assert.True(input.IsAtEnd);

        var parsed = Struct.Parser.ParseFrom(metaBytes);
        Assert.True(parsed.Fields["active"].BoolValue);
        Assert.Equal(12.5, parsed.Fields["score"].NumberValue);
        Assert.Equal("hi", parsed.Fields["note"].StringValue);
        Assert.Equal(1.0, parsed.Fields["nested"].StructValue.Fields["x"].NumberValue);
        Assert.Equal(["a", "b"], parsed.Fields["tags"].ListValue.Values.Select(v => v.StringValue));
        Assert.Equal(Value.KindOneofCase.NullValue, parsed.Fields["nothing"].KindCase);
    }

    [Fact]
    public void EncodeEvent_writes_row_seq_and_ts_ms_envelope_fields()
    {
        var fields = new List<FieldDef> { new("symbol", FieldType.String) };
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?> { ["symbol"] = "AAPL" };

        var bytes = ProtoWireEncoder.EncodeEvent(fields, numbers, row, seq: 123, tsMs: 999);
        var input = new CodedInputStream(bytes);

        AssertTag(input, 1, WireFormat.WireType.LengthDelimited); // row
        var rowBytes = input.ReadBytes().ToByteArray();
        var rowInput = new CodedInputStream(rowBytes);
        AssertTag(rowInput, 1, WireFormat.WireType.LengthDelimited);
        Assert.Equal("AAPL", rowInput.ReadString());

        AssertTag(input, 2, WireFormat.WireType.Varint); // seq
        Assert.Equal(123L, input.ReadInt64());
        AssertTag(input, 3, WireFormat.WireType.Varint); // ts_ms
        Assert.Equal(999L, input.ReadInt64());
        Assert.True(input.IsAtEnd);
    }

    [Fact]
    public void EncodeDelta_writes_weight_as_zigzag_sint64_including_negative_values()
    {
        var fields = new List<FieldDef>(); // empty schema is fine - only exercising the envelope here
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>();

        var bytes = ProtoWireEncoder.EncodeDelta(fields, numbers, row, weight: -42, seq: 7);
        var input = new CodedInputStream(bytes);

        AssertTag(input, 1, WireFormat.WireType.LengthDelimited); // row (empty message, still present)
        Assert.Empty(input.ReadBytes().ToByteArray());

        AssertTag(input, 2, WireFormat.WireType.Varint); // weight
        Assert.Equal(-42L, input.ReadSInt64());
        AssertTag(input, 3, WireFormat.WireType.Varint); // seq
        Assert.Equal(7L, input.ReadInt64());
        Assert.True(input.IsAtEnd);

        // A raw (non-zigzag) varint read of a negative sint64-encoded value would NOT equal -42 -
        // this pins down that WriteSInt64/ReadSInt64 zigzag encoding is actually in effect.
        var rawBytes = ProtoWireEncoder.EncodeDelta(fields, numbers, row, weight: -1, seq: 0);
        var rawInput = new CodedInputStream(rawBytes);
        rawInput.ReadTag();
        rawInput.ReadBytes();
        rawInput.ReadTag();
        Assert.Equal(1UL, rawInput.ReadUInt64()); // zigzag(-1) == 1, not a giant two's-complement value
    }
}
