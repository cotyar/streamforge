using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Structural/text-level checks on <see cref="ProtoFileBuilder.Build"/>'s output: header banner
/// content, exactly one syntax/package declaration, the appended DynamicStreamService streaming
/// contract present verbatim and exactly once, and no name collisions between the entity's own
/// generated messages and the fixed appended ones. Whether the COMBINED file is actually valid,
/// protoc-compilable proto3 is asserted separately in <see cref="ProtoFileBuilderCompileTests"/> (by
/// really compiling it), not here.
/// </summary>
public class ProtoFileBuilderTests
{
    [Fact]
    public void Header_banner_identifies_entity_kind_and_name()
    {
        var schema = DescriptorFactory.Generate("trades", TestHelpers.FlatFields);
        var proto = ProtoFileBuilder.Build("source", "trades", schema);

        Assert.Contains("// Entity: source \"trades\"", proto);
        Assert.Contains("StreamForge generated .proto", proto);
        Assert.Contains("do not edit by hand", proto);
    }

    [Fact]
    public void Combined_file_has_exactly_one_syntax_and_one_package_declaration()
    {
        var schema = DescriptorFactory.Generate("trades", TestHelpers.FlatFields);
        var proto = ProtoFileBuilder.Build("source", "trades", schema);

        Assert.Equal(1, CountOccurrences(proto, "syntax = \"proto3\";"));
        Assert.Equal(1, CountOccurrences(proto, $"package {DescriptorFactory.PackageName};"));
    }

    [Fact]
    public void Streaming_contract_is_appended_verbatim_and_exactly_once()
    {
        var schema = DescriptorFactory.Generate("trades", TestHelpers.FlatFields);
        var proto = ProtoFileBuilder.Build("source", "trades", schema);

        Assert.Equal(1, CountOccurrences(proto, "message EntitySubscribeRequest {"));
        Assert.Contains("string entity_key = 1;", proto);

        Assert.Equal(1, CountOccurrences(proto, "message DynamicFrame {"));
        Assert.Contains("bytes payload = 2;", proto);
        Assert.Contains("int64 seq = 3;", proto);

        Assert.Equal(1, CountOccurrences(proto, "service DynamicStreamService {"));
        Assert.Contains("rpc SubscribeEntity(EntitySubscribeRequest) returns (stream DynamicFrame);", proto);
    }

    [Fact]
    public void Entity_schema_text_is_embedded_unmodified()
    {
        var schema = DescriptorFactory.Generate("gold_tier_orders", TestHelpers.NestedJsonFields);
        var proto = ProtoFileBuilder.Build("table", "gold_tier_orders", schema);

        Assert.Contains(schema.ProtoText, proto);
    }

    [Theory]
    [InlineData("trades")]
    [InlineData("gold_tier_orders")]
    [InlineData("app_events")]
    public void Entitys_own_message_names_never_collide_with_the_appended_fixed_messages(string entityName)
    {
        var schema = DescriptorFactory.Generate(entityName, TestHelpers.NestedJsonFields);

        Assert.NotEqual("EntitySubscribeRequest", schema.MessageName);
        Assert.NotEqual("DynamicFrame", schema.MessageName);
        Assert.NotEqual("EntitySubscribeRequest", schema.EventMessageName);
        Assert.NotEqual("DynamicFrame", schema.EventMessageName);
        Assert.NotEqual("EntitySubscribeRequest", schema.DeltaMessageName);
        Assert.NotEqual("DynamicFrame", schema.DeltaMessageName);
    }

    [Fact]
    public void Download_filename_matches_FileProto_Name_and_ends_with_dot_proto()
    {
        var schema = DescriptorFactory.Generate("gold_tier_orders", TestHelpers.FlatFields);

        Assert.Equal("gold_tier_orders.proto", schema.FileProto.Name);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }
        return count;
    }
}
