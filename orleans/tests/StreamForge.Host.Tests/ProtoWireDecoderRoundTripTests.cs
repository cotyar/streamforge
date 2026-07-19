using Google.Protobuf;
using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>
/// Plan 006 (2D): encode with <see cref="ProtoWireEncoder"/>, decode with <see cref="ProtoWireDecoder"/>,
/// assert IDENTITY against the original row for every shape in <see cref="TestHelpers"/> — the encoder's
/// counterpart of <see cref="WireRoundTripTests"/>/<see cref="RepeatedFieldWireRoundTripTests"/>, proving
/// the decoder is an exact inverse rather than just independently self-consistent.
/// </summary>
public class ProtoWireDecoderRoundTripTests
{
    /// <summary>Deep structural equality for decoded row values (Dictionary/List/scalar), with clear
    /// per-key/per-index failures instead of relying on xunit's generic collection comparer.</summary>
    private static void AssertRowEqual(object? expected, object? actual)
    {
        switch (expected)
        {
            case null:
                Assert.Null(actual);
                break;
            case Dictionary<string, object?> ed:
                var ad = Assert.IsType<Dictionary<string, object?>>(actual);
                Assert.Equal(ed.Keys.OrderBy(k => k, StringComparer.Ordinal), ad.Keys.OrderBy(k => k, StringComparer.Ordinal));
                foreach (var key in ed.Keys)
                {
                    AssertRowEqual(ed[key], ad[key]);
                }
                break;
            case List<object?> el:
                var al = Assert.IsType<List<object?>>(actual);
                Assert.Equal(el.Count, al.Count);
                for (var i = 0; i < el.Count; i++)
                {
                    AssertRowEqual(el[i], al[i]);
                }
                break;
            default:
                Assert.Equal(expected, actual);
                break;
        }
    }

    [Fact]
    public void Flat_scalar_fields_round_trip_exactly()
    {
        var fields = TestHelpers.FlatFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>
        {
            ["symbol"] = "AAPL",
            ["price"] = 189.5,
            ["qty"] = 42L,
            ["active"] = true,
            ["traded_at"] = 1_700_000_000_000L,
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        AssertRowEqual(row, decoded);
    }

    [Fact]
    public void Nested_json_fields_round_trip_exactly()
    {
        var fields = TestHelpers.NestedJsonFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>
        {
            ["symbol"] = "AAPL",
            ["payload"] = new Dictionary<string, object?>
            {
                ["user"] = new Dictionary<string, object?> { ["id"] = "u1", ["tier"] = "gold" },
                ["amount"] = 99.5,
            },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        AssertRowEqual(row, decoded);
    }

    [Fact]
    public void Schemaless_json_struct_field_round_trips_exactly_including_an_explicit_null_entry()
    {
        var fields = TestHelpers.SchemalessJsonFields;
        var numbers = FieldNumberMap.Assign(fields);

        // Numeric values are doubles throughout (not integers): google.protobuf.Struct has no integer
        // number kind, so a schemaless field's numbers are inherently double after an encode/decode
        // round trip through Struct - not a decoder bug, see ProtoWireDecoder's type doc.
        var row = new Dictionary<string, object?>
        {
            ["symbol"] = "AAPL",
            ["meta"] = new Dictionary<string, object?>
            {
                ["active"] = true,
                ["score"] = 12.5,
                ["note"] = "hi",
                ["nested"] = new Dictionary<string, object?> { ["x"] = 1.0 },
                ["tags"] = new List<object?> { "a", "b" },
                ["nothing"] = null, // explicit null WITHIN a Struct is preserved (unlike a schema-declared field)
            },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        AssertRowEqual(row, decoded);
    }

    [Fact]
    public void Repeated_nested_message_field_round_trips_exactly_for_three_elements()
    {
        var fields = TestHelpers.RepeatedNestedFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>
        {
            ["trade_id"] = "T1",
            ["legs"] = new List<object?>
            {
                new Dictionary<string, object?> { ["leg_no"] = 1L, ["ccy"] = "USD", ["notional"] = 1_000_000.0, ["active"] = true, ["as_of"] = 1_700_000_000_000L },
                new Dictionary<string, object?> { ["leg_no"] = 2L, ["ccy"] = "EUR", ["notional"] = 2_000_000.0, ["active"] = false, ["as_of"] = 1_700_000_001_000L },
                new Dictionary<string, object?> { ["leg_no"] = 3L, ["ccy"] = "GBP", ["notional"] = 3_000_000.0, ["active"] = true, ["as_of"] = 1_700_000_002_000L },
            },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        AssertRowEqual(row, decoded);
    }

    [Fact]
    public void Empty_repeated_list_decodes_back_to_a_missing_key_not_an_empty_list()
    {
        // The encoder omits an empty (or all-null) list entirely (same as a missing/null scalar) - so
        // there is nothing on the wire to reconstitute an empty List<object?> from; this is the honest,
        // documented shape of the round trip, not a gap.
        var fields = TestHelpers.RepeatedNestedFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?> { ["trade_id"] = "T1", ["legs"] = new List<object?>() };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        Assert.Equal("T1", decoded["trade_id"]);
        Assert.False(decoded.ContainsKey("legs"));
    }

    [Fact]
    public void Repeated_scalar_field_round_trips_exactly()
    {
        var fields = TestHelpers.RepeatedScalarFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>
        {
            ["id"] = "src-1",
            ["tags"] = new List<object?> { "web", "priority", "retry" },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        AssertRowEqual(row, decoded);
    }

    [Fact]
    public void Null_elements_inside_a_repeated_list_are_dropped_by_encode_so_decode_reflects_only_survivors()
    {
        var fields = TestHelpers.RepeatedScalarFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?> { ["tags"] = new List<object?> { "web", null, "retry" } };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        AssertRowEqual(new Dictionary<string, object?> { ["tags"] = new List<object?> { "web", "retry" } }, decoded);
    }

    [Fact]
    public void Repeated_schemaless_json_field_round_trips_exactly()
    {
        var fields = TestHelpers.RepeatedStructFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>
        {
            ["id"] = "b1",
            ["blobs"] = new List<object?>
            {
                new Dictionary<string, object?> { ["x"] = 1.0 },
                new Dictionary<string, object?> { ["y"] = "hi" },
            },
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        AssertRowEqual(row, decoded);
    }

    [Fact]
    public void Kitchen_sink_schema_round_trips_exactly_across_every_field_kind()
    {
        var fields = TestHelpers.KitchenSinkFields;
        var numbers = FieldNumberMap.Assign(fields);
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
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        AssertRowEqual(row, decoded);
    }

    [Fact]
    public void Missing_and_null_values_stay_absent_from_the_decoded_row()
    {
        var fields = TestHelpers.FlatFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?>
        {
            ["symbol"] = "GOOG",
            ["price"] = null,
            ["qty"] = 5L,
            ["active"] = null,
            // traded_at key entirely absent
        };

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        Assert.Equal(2, decoded.Count);
        Assert.Equal("GOOG", decoded["symbol"]);
        Assert.Equal(5L, decoded["qty"]);
        Assert.False(decoded.ContainsKey("price"));
        Assert.False(decoded.ContainsKey("active"));
        Assert.False(decoded.ContainsKey("traded_at"));
    }

    [Fact]
    public void Unicode_strings_round_trip_exactly()
    {
        var fields = new List<FieldDef> { new("name", FieldType.String) };
        var numbers = FieldNumberMap.Assign(fields);
        const string text = "héllo wörld 你好 🎉";

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, new Dictionary<string, object?> { ["name"] = text });
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        Assert.Equal(text, decoded["name"]);
    }

    [Fact]
    public void Empty_row_decodes_to_an_empty_dictionary()
    {
        var fields = TestHelpers.FlatFields;
        var numbers = FieldNumberMap.Assign(fields);

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, new Dictionary<string, object?>());
        Assert.Empty(bytes); // pinned by WireRoundTripTests too

        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);
        Assert.Empty(decoded);
    }

    [Fact]
    public void EncodeEvent_then_DecodeEvent_round_trips_row_seq_and_ts_ms()
    {
        var fields = TestHelpers.FlatFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?> { ["symbol"] = "MSFT", ["qty"] = 7L };

        var bytes = ProtoWireEncoder.EncodeEvent(fields, numbers, row, seq: 555, tsMs: 1_234_567);
        var (decodedRow, seq, tsMs) = ProtoWireDecoder.DecodeEvent(fields, numbers, bytes);

        AssertRowEqual(row, decodedRow);
        Assert.Equal(555L, seq);
        Assert.Equal(1_234_567L, tsMs);
    }

    [Fact]
    public void EncodeDelta_then_DecodeDelta_round_trips_including_a_negative_zigzag_weight()
    {
        var fields = TestHelpers.FlatFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?> { ["symbol"] = "GOOG" };

        var bytes = ProtoWireEncoder.EncodeDelta(fields, numbers, row, weight: -777, seq: 42);
        var (decodedRow, weight, seq) = ProtoWireDecoder.DecodeDelta(fields, numbers, bytes);

        AssertRowEqual(row, decodedRow);
        Assert.Equal(-777L, weight);
        Assert.Equal(42L, seq);
    }

    [Fact]
    public void EncodeDelta_then_DecodeDelta_round_trips_a_positive_weight_and_an_empty_row()
    {
        var fields = TestHelpers.FlatFields;
        var numbers = FieldNumberMap.Assign(fields);

        var bytes = ProtoWireEncoder.EncodeDelta(fields, numbers, new Dictionary<string, object?>(), weight: 3, seq: 0);
        var (decodedRow, weight, seq) = ProtoWireDecoder.DecodeDelta(fields, numbers, bytes);

        Assert.Empty(decodedRow);
        Assert.Equal(3L, weight);
        Assert.Equal(0L, seq);
    }

    [Fact]
    public void Unknown_field_numbers_at_the_root_are_skipped_by_wire_type_varint_fixed64_and_length_delimited()
    {
        var fields = TestHelpers.FlatFields;
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = 42L };
        var normalBytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);

        using var ms = new MemoryStream();
        ms.Write(normalBytes, 0, normalBytes.Length);
        using (var output = new CodedOutputStream(ms, leaveOpen: true))
        {
            output.WriteTag(999, WireFormat.WireType.Varint);
            output.WriteInt64(123_456_789L);
            output.WriteTag(998, WireFormat.WireType.Fixed64);
            output.WriteDouble(3.14);
            output.WriteTag(997, WireFormat.WireType.LengthDelimited);
            output.WriteString("unexpected-extra-field");
            output.Flush();
        }
        var withExtras = ms.ToArray();

        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, withExtras);

        Assert.Equal(2, decoded.Count);
        Assert.Equal("AAPL", decoded["symbol"]);
        Assert.Equal(42L, decoded["qty"]);
    }

    [Fact]
    public void Unknown_field_inside_a_nested_message_is_skipped_without_disturbing_sibling_fields()
    {
        var fields = TestHelpers.NestedJsonFields; // symbol:String, payload:Json{user:Json{id,tier}, amount:Double}
        var numbers = FieldNumberMap.Assign(fields);

        byte[] payloadBytes;
        using (var payloadMs = new MemoryStream())
        {
            using (var output = new CodedOutputStream(payloadMs, leaveOpen: true))
            {
                output.WriteTag(numbers.Active["payload.amount"], WireFormat.WireType.Fixed64);
                output.WriteDouble(42.0);
                output.WriteTag(50, WireFormat.WireType.Varint); // unknown field inside the nested scope
                output.WriteInt64(999L);
                output.Flush();
            }
            payloadBytes = payloadMs.ToArray();
        }

        byte[] rowBytes;
        using (var rowMs = new MemoryStream())
        {
            using (var output = new CodedOutputStream(rowMs, leaveOpen: true))
            {
                output.WriteTag(numbers.Active["symbol"], WireFormat.WireType.LengthDelimited);
                output.WriteString("AAPL");
                output.WriteTag(numbers.Active["payload"], WireFormat.WireType.LengthDelimited);
                output.WriteBytes(ByteString.CopyFrom(payloadBytes));
                output.Flush();
            }
            rowBytes = rowMs.ToArray();
        }

        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, rowBytes);

        Assert.Equal("AAPL", decoded["symbol"]);
        var payload = Assert.IsType<Dictionary<string, object?>>(decoded["payload"]);
        Assert.Single(payload);
        Assert.Equal(42.0, payload["amount"]);
        Assert.False(payload.ContainsKey("user"));
    }

    [Fact]
    public void Scalar_values_arriving_as_int_or_float_still_round_trip_via_their_declared_field_type()
    {
        var fields = new List<FieldDef> { new("qty", FieldType.Long), new("price", FieldType.Double) };
        var numbers = FieldNumberMap.Assign(fields);
        var row = new Dictionary<string, object?> { ["qty"] = 9, ["price"] = 4.5f }; // int, float on the way in

        var bytes = ProtoWireEncoder.EncodeRow(fields, numbers, row);
        var decoded = ProtoWireDecoder.DecodeRow(fields, numbers, bytes);

        // Decoded values come back typed exactly per FieldDef.Type (long/double), regardless of what
        // CLR numeric type the original row happened to use.
        Assert.Equal(9L, decoded["qty"]);
        Assert.Equal(4.5, decoded["price"]);
    }
}
