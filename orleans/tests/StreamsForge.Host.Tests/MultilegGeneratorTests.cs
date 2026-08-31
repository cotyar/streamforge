using StreamsForge.Abstractions;
using StreamsForge.Host.Generators;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Generator-shape coverage for the "multileg" profile / "structures" seed source (Phase L1):
/// asserts the declared schema is actually array-typed, and that generated events match the plan's
/// documented shapes (IR swaps: 2 legs; option strategies: 2-4 legs) closely enough to encode cleanly
/// through the full DescriptorFactory/ProtoWireEncoder pipeline.</summary>
public class MultilegGeneratorTests
{
    private static SourceDefinition StructuresSource() =>
        MarketDataProfiles.SeedSources().Single(s => s.Name == "structures");

    [Fact]
    public void Seed_sources_include_a_multileg_structures_source_with_a_typed_array_legs_field()
    {
        var src = StructuresSource();
        Assert.Equal("multileg", src.GeneratorProfile);

        var legs = src.Fields.Single(f => f.Name == "legs");
        Assert.True(legs.IsArray);
        Assert.Equal(FieldType.Json, legs.Type);
        Assert.NotEmpty(legs.Children!);

        // Sibling scalar fields are unaffected by adding an array field.
        Assert.False(src.Fields.Single(f => f.Name == "trade_id").IsArray);
        Assert.False(src.Fields.Single(f => f.Name == "product").IsArray);
    }

    [Fact]
    public void Generated_events_alternate_between_swaps_and_option_strategies_with_the_documented_leg_counts()
    {
        var src = StructuresSource();
        var sawSwap = false;
        var sawOption = false;

        for (var i = 0; i < 200; i++)
        {
            var evt = MarketDataProfiles.GenerateEvent(src);

            var legsValue = Assert.IsType<List<object?>>(evt["legs"]);
            var product = Assert.IsType<string>(evt["product"]);
            Assert.IsType<string>(evt["trade_id"]);

            if (product == "IRS")
            {
                sawSwap = true;
                Assert.Equal(2, legsValue.Count); // documented shape: IR swaps have exactly 2 legs
                Assert.IsType<string>(evt["notional_ccy"]);

                foreach (var legObj in legsValue)
                {
                    var leg = Assert.IsAssignableFrom<IDictionary<string, object?>>(legObj);
                    Assert.Contains("leg_no", leg.Keys);
                    Assert.Contains("pay_rcv", leg.Keys);
                    Assert.Contains("notional", leg.Keys);
                    Assert.Contains("ccy", leg.Keys);
                    Assert.Contains("rate_type", leg.Keys);
                    Assert.Contains("rate", leg.Keys);
                    // Option-strategy-only fields stay absent on a swap leg.
                    Assert.DoesNotContain("cp", leg.Keys);
                    Assert.DoesNotContain("strike", leg.Keys);
                }
            }
            else
            {
                sawOption = true;
                Assert.Contains(product, new[] { "STRADDLE", "STRANGLE", "FLY" });
                Assert.InRange(legsValue.Count, 2, 4); // documented shape: option strategies have 2-4 legs
                Assert.False(evt.ContainsKey("notional_ccy")); // swap-only field stays absent

                foreach (var legObj in legsValue)
                {
                    var leg = Assert.IsAssignableFrom<IDictionary<string, object?>>(legObj);
                    Assert.Contains("leg_no", leg.Keys);
                    Assert.Contains("cp", leg.Keys);
                    Assert.Contains("strike", leg.Keys);
                    Assert.Contains("expiry_ts", leg.Keys);
                    Assert.Contains("ratio", leg.Keys);
                    // Swap-only fields stay absent on an option leg.
                    Assert.DoesNotContain("pay_rcv", leg.Keys);
                    Assert.DoesNotContain("notional", leg.Keys);
                }
            }
        }

        Assert.True(sawSwap, "expected at least one IRS swap event across 200 draws");
        Assert.True(sawOption, "expected at least one option-strategy event across 200 draws");
    }

    [Fact]
    public void Structures_events_encode_cleanly_through_the_full_descriptor_and_wire_pipeline()
    {
        var src = StructuresSource();
        var numbers = FieldNumberMap.Assign(src.Fields);
        var schema = DescriptorFactory.Generate(src.Name, src.Fields, numbers);
        DescriptorVerifier.Verify(schema.FileProto); // structurally valid descriptor
        Assert.Contains("repeated Legs legs = ", schema.ProtoText);

        for (var i = 0; i < 20; i++)
        {
            var evt = MarketDataProfiles.GenerateEvent(src);
            var bytes = ProtoWireEncoder.EncodeRow(src.Fields, numbers, evt);
            Assert.NotEmpty(bytes); // must not throw, and must actually write something
        }
    }
}
