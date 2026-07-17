using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;
using Xunit;
using PbDescriptorProto = Google.Protobuf.Reflection.DescriptorProto;

namespace StreamForge.Host.Tests;

public class ProtoTextTests
{
    [Fact]
    public void Flat_schema_renders_exact_golden_proto_text()
    {
        var fields = new List<FieldDef> { new("id", FieldType.String), new("qty", FieldType.Long) };
        var schema = DescriptorFactory.Generate("orders", fields);

        const string expected =
            "syntax = \"proto3\";\n\n" +
            "package streamforge.dynamic.v1;\n\n" +
            "message Orders {\n" +
            "  string id = 1; // StreamForge: String\n" +
            "  int64 qty = 2; // StreamForge: Long\n" +
            "}\n\n" +
            "message OrdersEvent {\n" +
            "  Orders row = 1;\n" +
            "  int64 seq = 2;\n" +
            "  int64 ts_ms = 3;\n" +
            "}\n\n" +
            "message OrdersDelta {\n" +
            "  Orders row = 1;\n" +
            "  sint64 weight = 2;\n" +
            "  int64 seq = 3;\n" +
            "}\n";

        Assert.Equal(expected, schema.ProtoText);
    }

    [Fact]
    public void Struct_field_renders_google_protobuf_struct_type_and_import()
    {
        var schema = DescriptorFactory.Generate("audit_log", TestHelpers.SchemalessJsonFields);

        Assert.Contains("import \"google/protobuf/struct.proto\";", schema.ProtoText);
        Assert.Contains("google.protobuf.Struct meta = 2; // StreamForge: Json (schemaless)", schema.ProtoText);
    }

    [Fact]
    public void Reserved_single_number_renders_as_a_single_value()
    {
        var v1 = new List<FieldDef> { new("a", FieldType.String), new("b", FieldType.String), new("c", FieldType.String) };
        var gen1 = DescriptorFactory.Generate("widgets", v1);

        var v2 = new List<FieldDef> { new("a", FieldType.String), new("c", FieldType.String) }; // "b" (#2) removed
        var gen2 = DescriptorFactory.Generate("widgets", v2, gen1.FieldNumbers);

        Assert.Contains("reserved 2;\n", gen2.ProtoText);
    }

    [Fact]
    public void Reserved_contiguous_numbers_render_as_a_range()
    {
        var v1 = new List<FieldDef>
        {
            new("a", FieldType.String), new("b", FieldType.String), new("c", FieldType.String),
            new("d", FieldType.String), new("e", FieldType.String),
        };
        var gen1 = DescriptorFactory.Generate("widgets", v1);

        var v2 = new List<FieldDef> { new("a", FieldType.String), new("e", FieldType.String) }; // b,c,d (#2-4) removed
        var gen2 = DescriptorFactory.Generate("widgets", v2, gen1.FieldNumbers);

        Assert.Contains("reserved 2-4;\n", gen2.ProtoText);

        var reservedRanges = gen2.FileProto.MessageType.Single(m => m.Name == "Widgets").ReservedRange;
        var range = Assert.Single(reservedRanges);
        Assert.Equal(2, range.Start);
        Assert.Equal(5, range.End); // exclusive
    }

    [Fact]
    public void Text_and_descriptor_are_structurally_consistent_same_fields_and_numbers()
    {
        var combined = new List<FieldDef>(TestHelpers.NestedJsonFields);
        combined.AddRange(TestHelpers.SchemalessJsonFields);
        var schema = DescriptorFactory.Generate("kitchen_sink", combined);

        AssertMessageAndFieldsAppearInText(schema.FileProto.MessageType.ToList(), schema.ProtoText);
    }

    private static void AssertMessageAndFieldsAppearInText(IEnumerable<PbDescriptorProto> messages, string text)
    {
        foreach (var msg in messages)
        {
            Assert.Contains($"message {msg.Name} {{", text);
            foreach (var field in msg.Field)
            {
                Assert.Contains($" {field.Name} = {field.Number};", text);
            }
            AssertMessageAndFieldsAppearInText(msg.NestedType, text);
        }
    }
}
