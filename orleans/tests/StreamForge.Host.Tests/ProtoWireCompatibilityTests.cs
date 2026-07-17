using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// THE key test for Tier-3 typed-client codegen: proves that <see cref="ProtoWireEncoder"/> — what the
/// server actually puts on the wire in a <c>DynamicFrame.payload</c> — produces bytes whose field tags
/// match, field-for-field, the numbers <see cref="DescriptorFactory.Generate"/> assigned into the SAME
/// entity's downloadable .proto (<see cref="ProtoFileBuilder.Build"/>'s output). A client compiled from
/// that downloaded .proto parses those bytes correctly ONLY if this holds; both the REST download
/// endpoints and <c>DynamicStreamService</c> (sibling work) obtain their numbers from
/// <c>IRegistryGrain.EnsureFieldNumbersAsync</c>, so <see cref="GeneratedSchema.FieldNumbers"/> here
/// stands in for exactly what that call would return for a fields list it has never seen before
/// (empty existing map → same fresh sequential assignment).
///
/// <para>Verified by hand-decoding with <see cref="CodedInputStream"/> rather than by generating and
/// compiling protoc output as part of THIS test (C# has no DynamicMessage — see
/// <c>WireRoundTripTests</c>' doc comment) — <see cref="ProtoFileBuilderCompileTests"/> already proves,
/// separately, that the exact same generated .proto text is protoc-buildable for this fattest schema
/// shape, so together the two suites cover both halves of "a real client built from the download works
/// against real server bytes": the .proto is valid, AND its declared numbers match the wire.</para>
///
/// <para>Covers every <see cref="FieldType"/> scalar (String/Double/Long/Bool/Timestamp), a two-level
/// nested Json message, a schemaless Json/Struct field, and a negative envelope weight (zigzag sint64)
/// — see <see cref="TestHelpers.KitchenSinkFields"/>.</para>
/// </summary>
public class ProtoWireCompatibilityTests
{
    private static void AssertTag(CodedInputStream input, int expectedField, WireFormat.WireType expectedWireType)
    {
        var tag = input.ReadTag();
        Assert.Equal(expectedField, WireFormat.GetTagFieldNumber(tag));
        Assert.Equal(expectedWireType, WireFormat.GetTagWireType(tag));
    }

    /// <summary>Cross-checks that the field number CodedInputStream decodes at each tag equals the
    /// number literally printed next to that field name in the downloadable .proto text — i.e. what a
    /// human (or protoc) reading the download would assign is exactly what's on the wire.</summary>
    private static void AssertNumberMatchesProtoText(string protoText, string fieldName, int number) =>
        Assert.Contains($" {fieldName} = {number};", protoText);

    [Fact]
    public void All_scalar_kinds_nested_json_and_struct_decode_at_the_field_numbers_the_proto_declares()
    {
        var fields = TestHelpers.KitchenSinkFields;
        var schema = DescriptorFactory.Generate("kitchen_sink", fields); // fresh map: same as EnsureFieldNumbersAsync would return for a never-seen entity
        var protoText = ProtoFileBuilder.Build("source", "kitchen_sink", schema);
        var numbers = schema.FieldNumbers;

        var row = new Dictionary<string, object?>
        {
            ["event_type"] = "order_placed",
            ["active"] = true,
            ["occurred_at"] = 1_700_000_000_000L,
            ["payload"] = new Dictionary<string, object?>
            {
                ["user"] = new Dictionary<string, object?> { ["id"] = "u42", ["tier"] = "gold" },
                ["order"] = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = 100L, ["price"] = 189.5 },
            },
            ["meta"] = new Dictionary<string, object?>
            {
                ["source"] = "mobile",
                ["retries"] = 2.0,
                ["flagged"] = false,
            },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        // Root scope fields, in declaration order — each tag's field number must equal the number
        // DescriptorFactory printed for that exact field name in the .proto text.
        AssertNumberMatchesProtoText(protoText, "event_type", numbers.Active["event_type"]);
        AssertTag(input, numbers.Active["event_type"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("order_placed", input.ReadString());

        AssertNumberMatchesProtoText(protoText, "active", numbers.Active["active"]);
        AssertTag(input, numbers.Active["active"], WireFormat.WireType.Varint);
        Assert.True(input.ReadBool());

        AssertNumberMatchesProtoText(protoText, "occurred_at", numbers.Active["occurred_at"]);
        AssertTag(input, numbers.Active["occurred_at"], WireFormat.WireType.Varint);
        Assert.Equal(1_700_000_000_000L, input.ReadInt64());

        AssertNumberMatchesProtoText(protoText, "payload", numbers.Active["payload"]);
        AssertTag(input, numbers.Active["payload"], WireFormat.WireType.LengthDelimited);
        var payloadBytes = input.ReadBytes().ToByteArray();

        AssertNumberMatchesProtoText(protoText, "meta", numbers.Active["meta"]);
        AssertTag(input, numbers.Active["meta"], WireFormat.WireType.LengthDelimited);
        var metaBytes = input.ReadBytes().ToByteArray();
        Assert.True(input.IsAtEnd);

        // Nested message (two levels): "payload.user" and "payload.order" scopes.
        var payloadInput = new CodedInputStream(payloadBytes);

        AssertNumberMatchesProtoText(protoText, "user", numbers.Active["payload.user"]);
        AssertTag(payloadInput, numbers.Active["payload.user"], WireFormat.WireType.LengthDelimited);
        var userBytes = payloadInput.ReadBytes().ToByteArray();

        AssertNumberMatchesProtoText(protoText, "order", numbers.Active["payload.order"]);
        AssertTag(payloadInput, numbers.Active["payload.order"], WireFormat.WireType.LengthDelimited);
        var orderBytes = payloadInput.ReadBytes().ToByteArray();
        Assert.True(payloadInput.IsAtEnd);

        var userInput = new CodedInputStream(userBytes);
        AssertTag(userInput, numbers.Active["payload.user.id"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("u42", userInput.ReadString());
        AssertTag(userInput, numbers.Active["payload.user.tier"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("gold", userInput.ReadString());
        Assert.True(userInput.IsAtEnd);

        var orderInput = new CodedInputStream(orderBytes);
        AssertTag(orderInput, numbers.Active["payload.order.symbol"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("AAPL", orderInput.ReadString());
        AssertTag(orderInput, numbers.Active["payload.order.qty"], WireFormat.WireType.Varint);
        Assert.Equal(100L, orderInput.ReadInt64());
        AssertTag(orderInput, numbers.Active["payload.order.price"], WireFormat.WireType.Fixed64);
        Assert.Equal(189.5, orderInput.ReadDouble());
        Assert.True(orderInput.IsAtEnd);

        // Schemaless Json -> google.protobuf.Struct, parsed with the real well-known-type Parser.
        var metaStruct = Struct.Parser.ParseFrom(metaBytes);
        Assert.Equal("mobile", metaStruct.Fields["source"].StringValue);
        Assert.Equal(2.0, metaStruct.Fields["retries"].NumberValue);
        Assert.False(metaStruct.Fields["flagged"].BoolValue);
    }

    [Fact]
    public void EncodeEvent_envelope_tags_match_the_proto_declared_numbers_for_row_seq_ts_ms()
    {
        var fields = TestHelpers.FlatFields;
        var schema = DescriptorFactory.Generate("trades", fields);
        var protoText = ProtoFileBuilder.Build("source", "trades", schema);

        // Envelope layout (row=1, seq=2, ts_ms=3) is fixed by DescriptorFactory, independent of the
        // entity's own field numbers — confirm the .proto text agrees before trusting the wire bytes.
        Assert.Contains("Trades row = 1;", protoText);
        Assert.Contains("int64 seq = 2;", protoText);
        Assert.Contains("int64 ts_ms = 3;", protoText);

        var row = new Dictionary<string, object?> { ["symbol"] = "MSFT", ["qty"] = 7L };
        var bytes = ProtoWireEncoder.EncodeEvent(fields, schema.FieldNumbers, row, seq: 555, tsMs: 1_234_567);
        var input = new CodedInputStream(bytes);

        AssertTag(input, 1, WireFormat.WireType.LengthDelimited); // row
        var rowBytes = input.ReadBytes().ToByteArray();
        AssertTag(input, 2, WireFormat.WireType.Varint); // seq
        Assert.Equal(555L, input.ReadInt64());
        AssertTag(input, 3, WireFormat.WireType.Varint); // ts_ms
        Assert.Equal(1_234_567L, input.ReadInt64());
        Assert.True(input.IsAtEnd);

        var rowInput = new CodedInputStream(rowBytes);
        AssertTag(rowInput, schema.FieldNumbers.Active["symbol"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("MSFT", rowInput.ReadString());
    }

    [Fact]
    public void EncodeDelta_negative_weight_matches_the_proto_declared_sint64_field_and_zigzags_correctly()
    {
        var fields = TestHelpers.FlatFields;
        var schema = DescriptorFactory.Generate("trades", fields);
        var protoText = ProtoFileBuilder.Build("source", "trades", schema);

        Assert.Contains("sint64 weight = 2;", protoText);

        var row = new Dictionary<string, object?> { ["symbol"] = "GOOG" };
        var bytes = ProtoWireEncoder.EncodeDelta(fields, schema.FieldNumbers, row, weight: -777, seq: 42);
        var input = new CodedInputStream(bytes);

        AssertTag(input, 1, WireFormat.WireType.LengthDelimited); // row
        input.ReadBytes();
        AssertTag(input, 2, WireFormat.WireType.Varint); // weight, sint64/zigzag per the .proto
        Assert.Equal(-777L, input.ReadSInt64());
        AssertTag(input, 3, WireFormat.WireType.Varint); // seq
        Assert.Equal(42L, input.ReadInt64());
        Assert.True(input.IsAtEnd);
    }
}
