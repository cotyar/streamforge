using StreamsForge.Abstractions;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;
using PbFieldType = Google.Protobuf.Reflection.FieldType;

namespace StreamsForge.Host.Tests;

public class DescriptorValidityTests
{
    [Fact]
    public void Flat_schema_round_trips_through_BuildFromByteStrings()
    {
        var schema = DescriptorFactory.Generate("trades", TestHelpers.FlatFields);
        var fd = DescriptorVerifier.Verify(schema.FileProto);

        var root = fd.MessageTypes.Single(m => m.Name == "Trades");
        Assert.Equal(5, root.Fields.InFieldNumberOrder().Count);
        Assert.Empty(root.NestedTypes);

        // Envelope messages are present too.
        Assert.Contains(fd.MessageTypes, m => m.Name == "TradesEvent");
        Assert.Contains(fd.MessageTypes, m => m.Name == "TradesDelta");
    }

    [Fact]
    public void Nested_json_schema_round_trips_with_a_real_nested_message_type()
    {
        var schema = DescriptorFactory.Generate("gold_tier_orders", TestHelpers.NestedJsonFields);
        var fd = DescriptorVerifier.Verify(schema.FileProto);

        var root = fd.MessageTypes.Single(m => m.Name == "GoldTierOrders");
        var payloadField = root.Fields.InFieldNumberOrder().Single(f => f.Name == "payload");
        Assert.Equal(PbFieldType.Message, payloadField.FieldType);
        Assert.Equal("streamsforge.dynamic.v1.GoldTierOrders.Payload", payloadField.MessageType.FullName);

        var payloadMsg = payloadField.MessageType;
        var userField = payloadMsg.Fields.InFieldNumberOrder().Single(f => f.Name == "user");
        Assert.Equal("streamsforge.dynamic.v1.GoldTierOrders.Payload.User", userField.MessageType.FullName);

        var userMsg = userField.MessageType;
        Assert.Equal(2, userMsg.Fields.InFieldNumberOrder().Count); // id, tier
    }

    [Fact]
    public void Schemaless_json_schema_round_trips_with_google_protobuf_struct()
    {
        var schema = DescriptorFactory.Generate("audit_log", TestHelpers.SchemalessJsonFields);
        Assert.Contains("google/protobuf/struct.proto", schema.FileProto.Dependency);

        var fd = DescriptorVerifier.Verify(schema.FileProto);
        var root = fd.MessageTypes.Single(m => m.Name == "AuditLog");
        var metaField = root.Fields.InFieldNumberOrder().Single(f => f.Name == "meta");

        Assert.Equal(PbFieldType.Message, metaField.FieldType);
        Assert.Equal("google.protobuf.Struct", metaField.MessageType.FullName);
    }

    [Fact]
    public void Reserved_numbers_schema_still_round_trips_after_a_field_is_removed()
    {
        // v1: symbol, price, qty
        var v1 = new List<FieldDef>
        {
            new("symbol", FieldType.String),
            new("price", FieldType.Double),
            new("qty", FieldType.Long),
        };
        var gen1 = DescriptorFactory.Generate("quotes", v1);

        // v2: qty removed, notional added -> qty's number (3) must be reserved, not reused.
        var v2 = new List<FieldDef>
        {
            new("symbol", FieldType.String),
            new("price", FieldType.Double),
            new("notional", FieldType.Double),
        };
        var gen2 = DescriptorFactory.Generate("quotes", v2, gen1.FieldNumbers);

        var fd = DescriptorVerifier.Verify(gen2.FileProto);
        var root = fd.MessageTypes.Single(m => m.Name == "Quotes");

        var reservedRanges = root.ToProto().ReservedRange;
        var range = Assert.Single(reservedRanges);
        Assert.Equal(3, range.Start);
        Assert.Equal(4, range.End); // exclusive

        var notional = root.Fields.InFieldNumberOrder().Single(f => f.Name == "notional");
        Assert.Equal(4, notional.FieldNumber); // max(1,2,3)+1, not 3
    }
}
