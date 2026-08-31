using Google.Protobuf;
using StreamsForge.Abstractions;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Hand-decode wire round-trip coverage for FieldDef.IsArray (Phase L1), mirroring
/// WireRoundTripTests' methodology: encode with ProtoWireEncoder, decode by hand with
/// CodedInputStream, and assert the exact tag/wire-type/value sequence a real repeated field parses
/// as. Covers: empty list (field omitted), one element, three elements (same field number reused per
/// element - the "plain/unpacked repeated" encoding ProtoWireEncoder uses), nested leg messages with
/// every scalar kind, unicode inside a repeated element, and null elements being skipped.</summary>
public class RepeatedFieldWireRoundTripTests
{
    private static void AssertTag(CodedInputStream input, int expectedField, WireFormat.WireType expectedWireType)
    {
        var tag = input.ReadTag();
        Assert.Equal(expectedField, WireFormat.GetTagFieldNumber(tag));
        Assert.Equal(expectedWireType, WireFormat.GetTagWireType(tag));
    }

    [Fact]
    public void Empty_list_omits_the_repeated_field_entirely()
    {
        var fields = TestHelpers.RepeatedNestedFields; // trade_id:String, legs:Json[]{leg_no,ccy,notional,active,as_of}
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?> { ["trade_id"] = "T1", ["legs"] = new List<object?>() };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, numbers.Active["trade_id"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("T1", input.ReadString());
        Assert.True(input.IsAtEnd); // legs: [] writes nothing at all, same as a missing/null scalar
    }

    [Fact]
    public void One_element_nested_message_encodes_a_single_length_delimited_entry_with_every_scalar_kind()
    {
        var fields = TestHelpers.RepeatedNestedFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>
        {
            ["legs"] = new List<object?>
            {
                new Dictionary<string, object?>
                {
                    ["leg_no"] = 1L,
                    ["ccy"] = "USD",
                    ["notional"] = 1_000_000.0,
                    ["active"] = true,
                    ["as_of"] = 1_700_000_000_000L,
                },
            },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, numbers.Active["legs"], WireFormat.WireType.LengthDelimited);
        var legBytes = input.ReadBytes().ToByteArray();
        Assert.True(input.IsAtEnd); // only one entry for the one element

        var legInput = new CodedInputStream(legBytes);
        AssertTag(legInput, numbers.Active["legs.leg_no"], WireFormat.WireType.Varint);
        Assert.Equal(1L, legInput.ReadInt64());
        AssertTag(legInput, numbers.Active["legs.ccy"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("USD", legInput.ReadString());
        AssertTag(legInput, numbers.Active["legs.notional"], WireFormat.WireType.Fixed64);
        Assert.Equal(1_000_000.0, legInput.ReadDouble());
        AssertTag(legInput, numbers.Active["legs.active"], WireFormat.WireType.Varint);
        Assert.True(legInput.ReadBool());
        AssertTag(legInput, numbers.Active["legs.as_of"], WireFormat.WireType.Varint);
        Assert.Equal(1_700_000_000_000L, legInput.ReadInt64());
        Assert.True(legInput.IsAtEnd);
    }

    [Fact]
    public void Three_elements_encode_as_three_separate_length_delimited_entries_at_the_same_field_number()
    {
        var fields = TestHelpers.RepeatedNestedFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>
        {
            ["legs"] = new List<object?>
            {
                new Dictionary<string, object?> { ["leg_no"] = 1L, ["ccy"] = "USD" },
                new Dictionary<string, object?> { ["leg_no"] = 2L, ["ccy"] = "EUR" },
                new Dictionary<string, object?> { ["leg_no"] = 3L, ["ccy"] = "GBP" },
            },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        foreach (var (legNo, ccy) in new (long, string)[] { (1L, "USD"), (2L, "EUR"), (3L, "GBP") })
        {
            AssertTag(input, numbers.Active["legs"], WireFormat.WireType.LengthDelimited); // same field number, one entry per element
            var legInput = new CodedInputStream(input.ReadBytes().ToByteArray());
            AssertTag(legInput, numbers.Active["legs.leg_no"], WireFormat.WireType.Varint);
            Assert.Equal(legNo, legInput.ReadInt64());
            AssertTag(legInput, numbers.Active["legs.ccy"], WireFormat.WireType.LengthDelimited);
            Assert.Equal(ccy, legInput.ReadString());
            Assert.True(legInput.IsAtEnd);
        }
        Assert.True(input.IsAtEnd);
    }

    [Fact]
    public void Repeated_scalar_field_encodes_one_length_delimited_string_entry_per_element()
    {
        var fields = TestHelpers.RepeatedScalarFields; // id:String, tags:String[]
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>
        {
            ["id"] = "src-1",
            ["tags"] = new List<object?> { "web", "priority", "retry" },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, numbers.Active["id"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("src-1", input.ReadString());

        foreach (var tag in new[] { "web", "priority", "retry" })
        {
            AssertTag(input, numbers.Active["tags"], WireFormat.WireType.LengthDelimited);
            Assert.Equal(tag, input.ReadString());
        }
        Assert.True(input.IsAtEnd);
    }

    [Fact]
    public void Repeated_schemaless_json_field_encodes_one_struct_entry_per_element()
    {
        var fields = TestHelpers.RepeatedStructFields; // id:String, blobs:Json[] (no Children -> Struct)
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>
        {
            ["blobs"] = new List<object?>
            {
                new Dictionary<string, object?> { ["x"] = 1.0 },
                new Dictionary<string, object?> { ["y"] = "hi" },
            },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, numbers.Active["blobs"], WireFormat.WireType.LengthDelimited);
        var first = Google.Protobuf.WellKnownTypes.Struct.Parser.ParseFrom(input.ReadBytes().ToByteArray());
        Assert.Equal(1.0, first.Fields["x"].NumberValue);

        AssertTag(input, numbers.Active["blobs"], WireFormat.WireType.LengthDelimited);
        var second = Google.Protobuf.WellKnownTypes.Struct.Parser.ParseFrom(input.ReadBytes().ToByteArray());
        Assert.Equal("hi", second.Fields["y"].StringValue);

        Assert.True(input.IsAtEnd);
    }

    [Fact]
    public void Unicode_values_inside_a_repeated_nested_element_round_trip_exactly()
    {
        var fields = TestHelpers.RepeatedNestedFields;
        var numbers = FieldNumberMap.Assign(fields);
        const string text = "héllo wörld 你好 🎉";
        var row = new Dictionary<string, object?>
        {
            ["legs"] = new List<object?> { new Dictionary<string, object?> { ["ccy"] = text } },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);
        AssertTag(input, numbers.Active["legs"], WireFormat.WireType.LengthDelimited);
        var legInput = new CodedInputStream(input.ReadBytes().ToByteArray());
        AssertTag(legInput, numbers.Active["legs.ccy"], WireFormat.WireType.LengthDelimited);
        Assert.Equal(text, legInput.ReadString());
        Assert.True(legInput.IsAtEnd);
    }

    [Fact]
    public void Null_elements_inside_a_list_are_skipped_like_a_missing_or_null_scalar()
    {
        var fields = TestHelpers.RepeatedScalarFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?> { ["tags"] = new List<object?> { "web", null, "retry" } };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var input = new CodedInputStream(bytes);

        AssertTag(input, numbers.Active["tags"], WireFormat.WireType.LengthDelimited);
        Assert.Equal("web", input.ReadString());
        AssertTag(input, numbers.Active["tags"], WireFormat.WireType.LengthDelimited); // the null element in between wrote nothing
        Assert.Equal("retry", input.ReadString());
        Assert.True(input.IsAtEnd);
    }
}
