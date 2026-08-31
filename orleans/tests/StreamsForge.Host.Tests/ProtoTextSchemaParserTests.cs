using StreamsForge.AppCore.Connectors.Grpc;
using StreamsForge.Abstractions;
using StreamsForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>
/// Plan 006 (2D): <see cref="ProtoTextSchemaParser"/> parses exactly what
/// <see cref="ProtoFileBuilder.Build"/> produces (the <c>/api/{kind}/{key}/proto</c> download shape) and
/// recovers the SAME (Fields, Numbers) that generated it — for every shape in <see cref="TestHelpers"/> —
/// then separately proves a non-StreamsForge (or stripped-banner) proto is rejected with a diagnostic
/// naming the limitation, never silently guessed at.
/// </summary>
public class ProtoTextSchemaParserTests
{
    private static void AssertFieldsEqual(IReadOnlyList<FieldDef> expected, IReadOnlyList<FieldDef>? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.Count, actual!.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var a = actual[i];
            Assert.Equal(e.Name, a.Name);
            Assert.Equal(e.Type, a.Type);
            Assert.Equal(e.IsArray, a.IsArray);
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
        var protoText = ProtoFileBuilder.Build("source", entityName, schema);

        var (parsedFields, parsedNumbers, diagnostics) = ProtoTextSchemaParser.Parse(protoText);

        Assert.Empty(diagnostics);
        AssertFieldsEqual(fields, parsedFields);
        AssertNumbersEqual(schema.FieldNumbers, parsedNumbers);
    }

    [Fact]
    public void Flat_fields_round_trip() => AssertRoundTrips("trades", TestHelpers.FlatFields);

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
    public void Reserved_numbers_from_a_prior_generation_are_recovered_from_the_reserved_statement()
    {
        // Generate once with the full flat schema, then again with "price" removed (existing numbers
        // fed in) so DescriptorFactory emits a `reserved` statement for price's retired number - proves
        // the parser recovers Reserved, not just Active.
        var full = DescriptorFactory.Generate("trades", TestHelpers.FlatFields);
        var reduced = TestHelpers.FlatFields.Where(f => f.Name != "price").ToList();
        var schema2 = DescriptorFactory.Generate("trades", reduced, full.FieldNumbers);
        var protoText = ProtoFileBuilder.Build("source", "trades", schema2);

        Assert.Contains("reserved 2;", protoText); // "price" was field #2 in FlatFields' declaration order

        var (parsedFields, parsedNumbers, diagnostics) = ProtoTextSchemaParser.Parse(protoText);

        Assert.Empty(diagnostics);
        AssertFieldsEqual(reduced, parsedFields);
        AssertNumbersEqual(schema2.FieldNumbers, parsedNumbers);
    }

    [Fact]
    public void Long_and_timestamp_fields_are_disambiguated_via_the_generator_comment_not_the_bare_int64_keyword()
    {
        // Both compile to a bare `int64` - this is the one case where proto TEXT carries strictly more
        // information than a binary descriptor (see ReflectionSchemaWalker's doc comment on the same
        // ambiguity). Confirms the parser actually reads the trailing "// StreamsForge: ..." comment.
        var fields = new List<FieldDef> { new("qty", FieldType.Long), new("as_of", FieldType.Timestamp) };
        var schema = DescriptorFactory.Generate("ticks", fields);
        var protoText = ProtoFileBuilder.Build("source", "ticks", schema);

        Assert.Contains("int64 qty = 1; // StreamsForge: Long", protoText);
        Assert.Contains("int64 as_of = 2; // StreamsForge: Timestamp (epoch milliseconds)", protoText);

        var (parsedFields, _, diagnostics) = ProtoTextSchemaParser.Parse(protoText);

        Assert.Empty(diagnostics);
        Assert.Equal(FieldType.Long, parsedFields![0].Type);
        Assert.Equal(FieldType.Timestamp, parsedFields[1].Type);
    }

    [Fact]
    public void Empty_text_is_rejected_with_a_diagnostic()
    {
        var (fields, numbers, diagnostics) = ProtoTextSchemaParser.Parse("");

        Assert.Null(fields);
        Assert.Null(numbers);
        Assert.NotEmpty(diagnostics);
    }

    [Fact]
    public void A_hand_written_proto_without_the_StreamsForge_banner_is_rejected_with_a_diagnostic()
    {
        const string foreignProto = """
            syntax = "proto3";

            package some.other.package;

            message Trade {
              string symbol = 1;
              double price = 2;
            }
            """;

        var (fields, numbers, diagnostics) = ProtoTextSchemaParser.Parse(foreignProto);

        Assert.Null(fields);
        Assert.Null(numbers);
        Assert.Contains(diagnostics, d => d.Contains("StreamsForge-generated", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void The_bare_DescriptorFactory_schema_text_without_ProtoFileBuilders_banner_is_also_rejected()
    {
        // DescriptorFactory.Generate(...).ProtoText alone (never wrapped by ProtoFileBuilder.Build) has
        // no generator banner - this parser only accepts the full downloadable file, not the bare
        // schema fragment, so this must be rejected the same as any other foreign text (documented
        // limitation, not a gap: the banner is the only reliable signal this parser can check).
        var schema = DescriptorFactory.Generate("trades", TestHelpers.FlatFields);

        var (fields, numbers, diagnostics) = ProtoTextSchemaParser.Parse(schema.ProtoText);

        Assert.Null(fields);
        Assert.Null(numbers);
        Assert.NotEmpty(diagnostics);
    }

    [Fact]
    public void A_StreamsForge_banner_with_a_foreign_package_is_rejected_with_a_diagnostic()
    {
        const string tampered = """
            // StreamsForge generated .proto — do not edit by hand.
            // Entity: source "trades"

            syntax = "proto3";

            package not.the.right.package;

            message Trades {
              string symbol = 1;
            }

            message TradesEvent {
              Trades row = 1;
              int64 seq = 2;
              int64 ts_ms = 3;
            }

            message TradesDelta {
              Trades row = 1;
              sint64 weight = 2;
              int64 seq = 3;
            }
            """;

        var (fields, numbers, diagnostics) = ProtoTextSchemaParser.Parse(tampered);

        Assert.Null(fields);
        Assert.Null(numbers);
        Assert.Contains(diagnostics, d => d.Contains("package", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Missing_Event_Delta_envelope_messages_are_rejected_with_a_diagnostic()
    {
        var tampered = """
            // StreamsForge generated .proto — do not edit by hand.

            syntax = "proto3";

            package PACKAGE_PLACEHOLDER;

            message Trades {
              string symbol = 1;
            }
            """.Replace("PACKAGE_PLACEHOLDER", DescriptorFactory.PackageName);

        var (fields, numbers, diagnostics) = ProtoTextSchemaParser.Parse(tampered);

        Assert.Null(fields);
        Assert.Null(numbers);
        Assert.Contains(diagnostics, d => d.Contains("Event", StringComparison.Ordinal));
    }
}
