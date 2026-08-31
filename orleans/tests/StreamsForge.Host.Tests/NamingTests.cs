using StreamsForge.Abstractions;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamsForge.Host.Tests;

public class NamingTests
{
    [Theory]
    [InlineData("gold_tier_orders", "GoldTierOrders")]
    [InlineData("trades", "Trades")]
    [InlineData("audit-log", "AuditLog")]
    [InlineData("Already Pascal", "AlreadyPascal")]
    [InlineData("mixedCase_name", "MixedCaseName")]
    [InlineData("VWAP by symbol (5s)", "VWAPBySymbol5s")]
    [InlineData("foo(bar)!baz", "FooBarBaz")]
    public void Entity_name_becomes_PascalCase_message_name(string entityName, string expectedMessageName)
    {
        var schema = DescriptorFactory.Generate(entityName, TestHelpers.FlatFields);
        Assert.Equal(expectedMessageName, schema.MessageName);
        Assert.Equal(expectedMessageName + "Event", schema.EventMessageName);
        Assert.Equal(expectedMessageName + "Delta", schema.DeltaMessageName);

        var root = schema.FileProto.MessageType.Single(m => m.Name == expectedMessageName);
        Assert.NotNull(root);
    }

    [Fact]
    public void Entity_name_with_arbitrary_punctuation_yields_a_valid_proto_identifier()
    {
        // Regression test: pipeline names like "VWAP by symbol (5s)" (see SeedPipelines in
        // RegistryGrain.cs) used to leave '(' and ')' embedded in the message name, producing
        // an invalid proto3 identifier that failed protoc. ToPascalCase must now treat every
        // non-alphanumeric character as a word separator, not just '_', '-', ' ', and '.'.
        var schema = DescriptorFactory.Generate("VWAP by symbol (5s)", TestHelpers.FlatFields);
        Assert.Equal("VWAPBySymbol5s", schema.MessageName);

        var fd = DescriptorVerifier.Verify(schema.FileProto);
        Assert.Contains(fd.MessageTypes, m => m.Name == "VWAPBySymbol5s");
    }

    [Fact]
    public void Field_names_are_preserved_as_is_snake_case_and_all()
    {
        // Proto allows lowercase/underscore field names; we must NOT PascalCase or camelCase them,
        // so generated typed clients line up with the original SQL column names.
        var schema = DescriptorFactory.Generate("trades", TestHelpers.FlatFields);
        var root = schema.FileProto.MessageType.Single(m => m.Name == "Trades");

        var names = root.Field.Select(f => f.Name).ToList();
        Assert.Equal(["symbol", "price", "qty", "active", "traded_at"], names);
    }

    [Fact]
    public void Nested_message_type_names_are_PascalCase_but_field_names_stay_as_is()
    {
        var schema = DescriptorFactory.Generate("gold_tier_orders", TestHelpers.NestedJsonFields);
        var root = schema.FileProto.MessageType.Single(m => m.Name == "GoldTierOrders");

        var payloadNested = root.NestedType.Single(t => t.Name == "Payload");
        var payloadField = root.Field.Single(f => f.Name == "payload");
        Assert.Equal(".streamsforge.dynamic.v1.GoldTierOrders.Payload", payloadField.TypeName);

        var userNested = payloadNested.NestedType.Single(t => t.Name == "User");
        Assert.Equal(["id", "tier"], userNested.Field.Select(f => f.Name).ToList());
    }
}
