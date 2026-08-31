using StreamsForge.Abstractions;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;
using PbFieldType = Google.Protobuf.Reflection.FieldType;
using PbLabel = Google.Protobuf.Reflection.FieldDescriptorProto.Types.Label;

namespace StreamsForge.Host.Tests;

/// <summary>Descriptor-level coverage for FieldDef.IsArray (Phase L1 typed leg arrays): a repeated
/// nested message (Children declared), a repeated scalar, and a repeated schemaless Json/Struct field
/// all round-trip through the same BuildFromByteStrings validity check WireRoundTripTests' non-array
/// siblings use, and render "repeated" in the .proto text.</summary>
public class RepeatedFieldDescriptorTests
{
    [Fact]
    public void Repeated_nested_message_field_round_trips_as_a_repeated_message_field()
    {
        var schema = DescriptorFactory.Generate("structures", TestHelpers.RepeatedNestedFields);
        var fd = DescriptorVerifier.Verify(schema.FileProto);

        var root = fd.MessageTypes.Single(m => m.Name == "Structures");
        var legsField = root.Fields.InFieldNumberOrder().Single(f => f.Name == "legs");

        Assert.Equal(PbFieldType.Message, legsField.FieldType);
        Assert.True(legsField.IsRepeated);
        // Element message naming matches the non-array convention (PascalCase of the field name) -
        // see DescriptorFactory's doc comment: no singularization heuristic is applied.
        Assert.Equal("streamsforge.dynamic.v1.Structures.Legs", legsField.MessageType.FullName);
        Assert.Equal(5, legsField.MessageType.Fields.InFieldNumberOrder().Count);
    }

    [Fact]
    public void Repeated_scalar_field_round_trips_as_a_repeated_scalar_field()
    {
        var schema = DescriptorFactory.Generate("tagged", TestHelpers.RepeatedScalarFields);
        var fd = DescriptorVerifier.Verify(schema.FileProto);

        var root = fd.MessageTypes.Single(m => m.Name == "Tagged");
        var tagsField = root.Fields.InFieldNumberOrder().Single(f => f.Name == "tags");

        Assert.Equal(PbFieldType.String, tagsField.FieldType);
        Assert.True(tagsField.IsRepeated);

        var idField = root.Fields.InFieldNumberOrder().Single(f => f.Name == "id");
        Assert.False(idField.IsRepeated); // non-array sibling unaffected
    }

    [Fact]
    public void Repeated_schemaless_json_field_round_trips_as_repeated_struct()
    {
        var schema = DescriptorFactory.Generate("blobby", TestHelpers.RepeatedStructFields);
        Assert.Contains("google/protobuf/struct.proto", schema.FileProto.Dependency);

        var fd = DescriptorVerifier.Verify(schema.FileProto);
        var root = fd.MessageTypes.Single(m => m.Name == "Blobby");
        var blobsField = root.Fields.InFieldNumberOrder().Single(f => f.Name == "blobs");

        Assert.Equal(PbFieldType.Message, blobsField.FieldType);
        Assert.True(blobsField.IsRepeated);
        Assert.Equal("google.protobuf.Struct", blobsField.MessageType.FullName);
    }

    [Fact]
    public void Proto_text_renders_the_repeated_keyword_for_nested_and_scalar_array_fields()
    {
        var nested = DescriptorFactory.Generate("structures", TestHelpers.RepeatedNestedFields);
        Assert.Contains("repeated Legs legs = ", nested.ProtoText);
        Assert.DoesNotContain("repeated string trade_id", nested.ProtoText); // non-array sibling stays bare

        var scalar = DescriptorFactory.Generate("tagged", TestHelpers.RepeatedScalarFields);
        Assert.Contains("repeated string tags = ", scalar.ProtoText);

        var structy = DescriptorFactory.Generate("blobby", TestHelpers.RepeatedStructFields);
        Assert.Contains("repeated google.protobuf.Struct blobs = ", structy.ProtoText);
    }

    [Fact]
    public void Non_array_fields_stay_optional_singular_alongside_array_fields_in_the_same_message()
    {
        var schema = DescriptorFactory.Generate("structures", TestHelpers.RepeatedNestedFields);
        var root = schema.FileProto.MessageType.Single(m => m.Name == "Structures");

        var tradeId = root.Field.Single(f => f.Name == "trade_id");
        Assert.Equal(PbLabel.Optional, tradeId.Label);

        var legs = root.Field.Single(f => f.Name == "legs");
        Assert.Equal(PbLabel.Repeated, legs.Label);
    }

    [Fact]
    public void FieldNumberMap_numbers_an_arrays_children_by_the_arrays_own_path_same_as_non_array_json()
    {
        // IsArray doesn't change FieldNumberMap's scoping rules: "legs.leg_no" etc. are still keyed by
        // the array field's own path, exactly like a non-array Json field's Children.
        var numbers = FieldNumberMap.Assign(TestHelpers.RepeatedNestedFields);

        Assert.True(numbers.Active.ContainsKey("legs"));
        Assert.True(numbers.Active.ContainsKey("legs.leg_no"));
        Assert.True(numbers.Active.ContainsKey("legs.ccy"));
        Assert.True(numbers.Active.ContainsKey("legs.notional"));
        Assert.True(numbers.Active.ContainsKey("legs.active"));
        Assert.True(numbers.Active.ContainsKey("legs.as_of"));
    }
}
