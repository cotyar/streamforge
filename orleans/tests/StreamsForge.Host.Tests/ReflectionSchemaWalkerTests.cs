using Google.Protobuf.Reflection;
using StreamsForge.AppCore.Connectors.Grpc;
using StreamsForge.Abstractions;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;
using FieldType = StreamsForge.Abstractions.FieldType;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 006 (2D): <see cref="ReflectionSchemaWalker"/> walks the <see cref="FileDescriptorProto"/>
/// <see cref="DescriptorFactory.Generate"/> itself produces (exactly what a real gRPC reflection
/// <c>FileContainingSymbol</c> response would carry, per <see cref="DescriptorVerifier"/>'s round trip)
/// and recovers the SAME fields/numbers that generated it - for every shape in
/// <see cref="TestHelpers"/> - with ONE documented exception: a binary descriptor cannot distinguish
/// <see cref="FieldType.Long"/> from <see cref="FieldType.Timestamp"/> (both compile to a bare proto3
/// <c>int64</c> with no comment carried in <see cref="FieldDescriptorProto"/>), so every Timestamp field
/// is expected back as Long here (see <see cref="ReflectionSchemaWalker"/>'s type doc; wire-harmless -
/// <see cref="ProtoWireDecoder"/> reads both identically).
/// </summary>
public class ReflectionSchemaWalkerTests
{
    /// <summary>Structural field comparison allowing exactly one substitution: an expected
    /// <see cref="FieldType.Timestamp"/> may come back as <see cref="FieldType.Long"/> (the documented
    /// binary-descriptor ambiguity) - every other property must match exactly.</summary>
    private static void AssertFieldsEqual(IReadOnlyList<FieldDef> expected, IReadOnlyList<FieldDef>? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.Count, actual!.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var a = actual[i];
            Assert.Equal(e.Name, a.Name);
            Assert.Equal(e.IsArray, a.IsArray);
            if (e.Type == FieldType.Timestamp)
            {
                Assert.Equal(FieldType.Long, a.Type); // documented reflection-path ambiguity
            }
            else
            {
                Assert.Equal(e.Type, a.Type);
            }

            if (e.Children is null or { Count: 0 })
            {
                Assert.True(a.Children is null or { Count: 0 });
            }
            else
            {
                AssertFieldsEqual(e.Children, a.Children);
            }
        }
    }

    private static void AssertNumbersEqual(FieldNumberMap expected, FieldNumberMap? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.Active, actual!.Active);

        var expectedReservedKeys = expected.Reserved.Where(kv => kv.Value.Count > 0).Select(kv => kv.Key).OrderBy(k => k, StringComparer.Ordinal);
        var actualReservedKeys = actual.Reserved.Where(kv => kv.Value.Count > 0).Select(kv => kv.Key).OrderBy(k => k, StringComparer.Ordinal);
        Assert.Equal(expectedReservedKeys, actualReservedKeys);
        foreach (var scope in expectedReservedKeys)
        {
            Assert.Equal(expected.Reserved[scope], actual.Reserved[scope]);
        }
    }

    private static void AssertRoundTrips(string entityName, List<FieldDef> fields)
    {
        var schema = DescriptorFactory.Generate(entityName, fields);

        var (walkedFields, walkedNumbers, diagnostics) =
            ReflectionSchemaWalker.FromDescriptors([schema.FileProto], $"source:{entityName}");

        Assert.Empty(diagnostics);
        AssertFieldsEqual(fields, walkedFields);
        AssertNumbersEqual(schema.FieldNumbers, walkedNumbers);
    }

    [Fact]
    public void Flat_fields_round_trip_with_timestamp_degrading_to_long() => AssertRoundTrips("trades", TestHelpers.FlatFields);

    [Fact]
    public void Nested_json_fields_round_trip() => AssertRoundTrips("nested_events", TestHelpers.NestedJsonFields);

    [Fact]
    public void Schemaless_json_fields_round_trip() => AssertRoundTrips("meta_events", TestHelpers.SchemalessJsonFields);

    [Fact]
    public void Repeated_nested_fields_round_trip() => AssertRoundTrips("multileg_trades", TestHelpers.RepeatedNestedFields);

    [Fact]
    public void Repeated_scalar_fields_round_trip() => AssertRoundTrips("tagged_sources", TestHelpers.RepeatedScalarFields);

    [Fact]
    public void Repeated_struct_fields_round_trip() => AssertRoundTrips("blob_sources", TestHelpers.RepeatedStructFields);

    [Fact]
    public void Kitchen_sink_fields_round_trip() => AssertRoundTrips("kitchen_sink", TestHelpers.KitchenSinkFields);

    [Fact]
    public void Timestamp_field_explicitly_comes_back_as_Long_not_an_error()
    {
        var fields = new List<FieldDef> { new("as_of", FieldType.Timestamp) };
        var schema = DescriptorFactory.Generate("ticks", fields);

        var (walkedFields, _, diagnostics) = ReflectionSchemaWalker.FromDescriptors([schema.FileProto], "source:ticks");

        Assert.Empty(diagnostics);
        Assert.Equal(FieldType.Long, walkedFields![0].Type);
    }

    [Fact]
    public void Reserved_numbers_are_recovered_from_reserved_ranges()
    {
        var full = DescriptorFactory.Generate("trades", TestHelpers.FlatFields);
        var reduced = TestHelpers.FlatFields.Where(f => f.Name != "price").ToList();
        var schema2 = DescriptorFactory.Generate("trades", reduced, full.FieldNumbers);

        var (walkedFields, walkedNumbers, diagnostics) =
            ReflectionSchemaWalker.FromDescriptors([schema2.FileProto], "source:trades");

        Assert.Empty(diagnostics);
        AssertFieldsEqual(reduced, walkedFields);
        AssertNumbersEqual(schema2.FieldNumbers, walkedNumbers);
    }

    [Fact]
    public void An_entity_key_whose_ident_does_not_match_the_generated_name_still_resolves_via_the_single_candidate_fallback()
    {
        // "table:{id}"/"pipeline:{id}" entity keys carry an ID, not the entity's display name - the
        // walker still recovers the schema when exactly one row-message triplet is present in the
        // supplied descriptor set, per its documented fallback.
        var schema = DescriptorFactory.Generate("positions", TestHelpers.FlatFields);

        var (walkedFields, walkedNumbers, diagnostics) =
            ReflectionSchemaWalker.FromDescriptors([schema.FileProto], "table:8f3e9a21-opaque-id");

        Assert.Empty(diagnostics);
        AssertFieldsEqual(TestHelpers.FlatFields, walkedFields);
        AssertNumbersEqual(schema.FieldNumbers, walkedNumbers);
    }

    [Fact]
    public void Ambiguous_multi_candidate_descriptor_set_with_no_matching_ident_fails_with_a_diagnostic()
    {
        var tradesSchema = DescriptorFactory.Generate("trades", TestHelpers.FlatFields);
        var quotesSchema = DescriptorFactory.Generate("quotes", TestHelpers.FlatFields);

        var (fields, numbers, diagnostics) = ReflectionSchemaWalker.FromDescriptors(
            [tradesSchema.FileProto, quotesSchema.FileProto], "table:some-opaque-id");

        Assert.Null(fields);
        Assert.Null(numbers);
        Assert.NotEmpty(diagnostics);
    }

    [Fact]
    public void Empty_descriptor_set_fails_with_a_diagnostic()
    {
        var (fields, numbers, diagnostics) = ReflectionSchemaWalker.FromDescriptors([], "source:trades");

        Assert.Null(fields);
        Assert.Null(numbers);
        Assert.NotEmpty(diagnostics);
    }

    [Fact]
    public void Descriptor_set_with_no_row_message_triplet_fails_with_a_diagnostic()
    {
        var file = new FileDescriptorProto { Name = "unrelated.proto", Package = "not.streamsforge" };
        file.MessageType.Add(new DescriptorProto { Name = "SomeUnrelatedMessage" });

        var (fields, numbers, diagnostics) = ReflectionSchemaWalker.FromDescriptors([file], "source:trades");

        Assert.Null(fields);
        Assert.Null(numbers);
        Assert.NotEmpty(diagnostics);
    }
}
