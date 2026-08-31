using System.Text.Json;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors;
using StreamsForge.AppCore.Connectors.Formats;
using StreamsForge.AppCore.Connectors.Polling;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 018 wave A: <see cref="FixParser"/> — the <see cref="FileFormats.Fix"/> tag=value
/// parser. Mirrors <see cref="FormatParsersTests"/>'s style: raw FIX text in, assertions on the shape and
/// typing of the resulting <see cref="JsonElement"/>s out, plus the malformed-input
/// <see cref="FormatException"/> cases.</summary>
public class FixParserTests
{
    // ---- 35=W MarketDataSnapshotFullRefresh: a flat repeating group ----

    private const string SnapshotPipeDelimited =
        "8=FIX.4.4|9=200|35=W|49=SENDER|56=TARGET|34=1|52=20260816-12:00:00|55=EURUSD|" +
        "268=2|269=0|270=1.2345|271=1000000|269=1|270=1.2350|271=500000|10=128|";

    [Fact]
    public void Snapshot_NoMDEntries_becomes_one_item_with_a_two_element_MDEntries_array()
    {
        var items = FixParser.Parse(SnapshotPipeDelimited);

        Assert.Single(items);
        var msg = items[0];
        Assert.Equal("EURUSD", msg.GetProperty("Symbol").GetString());

        var entries = msg.GetProperty("MDEntries");
        Assert.Equal(JsonValueKind.Array, entries.ValueKind);
        Assert.Equal(2, entries.GetArrayLength());

        var e0 = entries[0];
        Assert.Equal(JsonValueKind.Number, e0.GetProperty("MDEntryPx").ValueKind);
        Assert.Equal(1.2345, e0.GetProperty("MDEntryPx").GetDouble());
        Assert.Equal(1000000L, e0.GetProperty("MDEntrySize").GetInt64());

        var e1 = entries[1];
        Assert.Equal(1.2350, e1.GetProperty("MDEntryPx").GetDouble());
        Assert.Equal(500000L, e1.GetProperty("MDEntrySize").GetInt64());

        // The counter's own value is never also emitted — the array length IS the count.
        Assert.False(msg.TryGetProperty("NoMDEntries", out _));
    }

    [Fact]
    public void Snapshot_SOH_and_pipe_delimited_produce_byte_identical_output_because_the_delimiter_is_sniffed()
    {
        var sohDelimited = SnapshotPipeDelimited.Replace('|', '\u0001');

        var pipeItems = FixParser.Parse(SnapshotPipeDelimited);
        var sohItems = FixParser.Parse(sohDelimited);

        Assert.Single(pipeItems);
        Assert.Single(sohItems);
        Assert.Equal(pipeItems[0].GetRawText(), sohItems[0].GetRawText());
    }

    // ---- 35=8 ExecutionReport: char-typed fields as strings, numeric fields as numbers ----

    [Fact]
    public void ExecutionReport_types_char_fields_as_strings_and_price_qty_fields_as_numbers()
    {
        const string text =
            "8=FIX.4.2|9=150|35=8|49=SENDER|56=TARGET|34=2|52=20260816-12:00:01|" +
            "37=ORDER1|11=CLORD1|17=EXEC1|150=0|39=0|55=123|54=1|38=100|44=50.25|" +
            "32=100|31=50.25|14=100|151=0|6=50.25|10=045|";

        var items = FixParser.Parse(text);
        Assert.Single(items);
        var msg = items[0];

        Assert.Equal(JsonValueKind.String, msg.GetProperty("OrdStatus").ValueKind);
        Assert.Equal("0", msg.GetProperty("OrdStatus").GetString());
        Assert.Equal(JsonValueKind.String, msg.GetProperty("ExecType").ValueKind);
        Assert.Equal("0", msg.GetProperty("ExecType").GetString());

        Assert.Equal(JsonValueKind.Number, msg.GetProperty("LastQty").ValueKind);
        Assert.Equal(100L, msg.GetProperty("LastQty").GetInt64());
        Assert.Equal(JsonValueKind.Number, msg.GetProperty("LastPx").ValueKind);
        Assert.Equal(50.25, msg.GetProperty("LastPx").GetDouble());

        // 55=123: Symbol is typed from the tag table, never sniffed — stays a string even though it's all digits.
        Assert.Equal(JsonValueKind.String, msg.GetProperty("Symbol").ValueKind);
        Assert.Equal("123", msg.GetProperty("Symbol").GetString());
    }

    // ---- 35=X MarketDataIncrementalRefresh: a nested group (NoMDEntries containing NoPartyIDs) ----

    [Fact]
    public void Incremental_refresh_nests_NoPartyIDs_inside_each_NoMDEntries_entry()
    {
        const string text =
            "8=FIX.4.4|9=100|35=X|49=SENDER|56=TARGET|34=3|52=20260816-12:00:02|" +
            "268=1|279=0|269=0|270=1.2345|453=2|448=PARTY1|447=D|452=1|448=PARTY2|447=D|452=2|271=1000000|";

        var items = FixParser.Parse(text);
        Assert.Single(items);
        var entries = items[0].GetProperty("MDEntries");
        Assert.Equal(1, entries.GetArrayLength());

        var entry = entries[0];
        Assert.Equal(1.2345, entry.GetProperty("MDEntryPx").GetDouble());
        Assert.Equal(1000000L, entry.GetProperty("MDEntrySize").GetInt64());

        var parties = entry.GetProperty("PartyIDs");
        Assert.Equal(JsonValueKind.Array, parties.ValueKind);
        Assert.Equal(2, parties.GetArrayLength());
        Assert.Equal("PARTY1", parties[0].GetProperty("PartyID").GetString());
        Assert.Equal(1L, parties[0].GetProperty("PartyRole").GetInt64());
        Assert.Equal("PARTY2", parties[1].GetProperty("PartyID").GetString());
        Assert.Equal(2L, parties[1].GetProperty("PartyRole").GetInt64());
    }

    /// <summary>The standard trailer (SignatureLength 93, Signature 89, CheckSum 10) is the one part of
    /// every FIX message that can NEVER be inside a repeating group, in any version — so it bounds a group
    /// without needing a dictionary. Without that rule a SINGLE-entry group (which has no second entry to
    /// reveal where it ends) swallows the trailer into the entry, and CheckSum goes missing from the
    /// message it checksums. The most common message shape there is — one group entry, then the trailer —
    /// would have been the broken one.</summary>
    [Fact]
    public void The_standard_trailer_bounds_a_single_entry_group_instead_of_being_swallowed_by_it()
    {
        const string text =
            "8=FIX.4.4|9=90|35=W|49=SENDER|56=TARGET|34=4|52=20260816-12:00:03|55=EUR/USD|" +
            "268=1|269=0|270=1.2345|271=1000000|10=123|";

        var items = FixParser.Parse(text);
        var msg = items[0];

        var entries = msg.GetProperty("MDEntries");
        Assert.Equal(1, entries.GetArrayLength());
        Assert.Equal(1.2345, entries[0].GetProperty("MDEntryPx").GetDouble());

        // The trailer belongs to the MESSAGE...
        Assert.Equal("123", msg.GetProperty("CheckSum").GetString());
        // ...and emphatically not to the group entry that happened to precede it.
        Assert.False(entries[0].TryGetProperty("CheckSum", out _));
    }

    // ---- Length-prefixed fields: the raw value contains both the delimiter and '=' ----

    [Fact]
    public void RawData_is_read_verbatim_even_when_it_contains_the_delimiter_and_an_equals_sign()
    {
        // Raw payload "A\u0001B=C\u0001De" (8 chars) deliberately contains the wire's own SOH delimiter
        // and an '=' sign — RawDataLength(95)/RawData(96) must survive both untouched.
        var raw = "A\u0001B=C\u0001De";
        Assert.Equal(8, raw.Length);
        var text =
            "8=FIX.4.4\u00019=1\u000135=8\u000149=S\u000156=T\u000134=1\u000152=X\u0001" +
            $"95={raw.Length}\u000196={raw}\u000110=1\u0001";

        var items = FixParser.Parse(text);
        Assert.Single(items);
        var msg = items[0];

        Assert.Equal(raw, msg.GetProperty("RawData").GetString());
        Assert.Equal(8L, msg.GetProperty("RawDataLength").GetInt64());
        Assert.Equal("1", msg.GetProperty("CheckSum").GetString());
    }

    // ---- Unknown tag, duplicate tag ----

    [Fact]
    public void Unknown_tag_becomes_tagN_key_with_a_string_value()
    {
        const string text = "8=FIX.4.4|9=1|35=0|49=S|56=T|34=1|52=X|9999=hello|10=1|";

        var items = FixParser.Parse(text);
        var msg = items[0];

        Assert.Equal(JsonValueKind.String, msg.GetProperty("tag9999").ValueKind);
        Assert.Equal("hello", msg.GetProperty("tag9999").GetString());
    }

    [Fact]
    public void Duplicate_non_group_tag_keeps_the_last_occurrence()
    {
        const string text = "8=FIX.4.4|9=1|35=0|49=S|56=T|34=1|52=X|58=first|58=second|10=1|";

        var items = FixParser.Parse(text);
        var msg = items[0];

        Assert.Equal("second", msg.GetProperty("Text").GetString());
    }

    // ---- Multiple messages; a log-file prefix ahead of the first 8= is ignored ----

    [Fact]
    public void Multiple_messages_concatenated_yield_one_item_each_and_a_log_prefix_before_the_first_8_is_ignored()
    {
        const string text =
            "2026-08-16 12:00:00 IN\n" +
            "8=FIX.4.4|9=1|35=0|49=S|56=T|34=1|52=X|10=1|" +
            "8=FIX.4.4|9=1|35=0|49=S|56=T|34=2|52=X|10=2|";

        var items = FixParser.Parse(text);

        Assert.Equal(2, items.Count);
        Assert.Equal(1L, items[0].GetProperty("MsgSeqNum").GetInt64());
        Assert.Equal(2L, items[1].GetProperty("MsgSeqNum").GetInt64());
    }

    // ---- Malformed input ----

    [Fact]
    public void Frame_with_no_35_throws_naming_the_message_index()
    {
        const string text = "8=FIX.4.4|9=1|49=S|56=T|34=1|52=X|10=1|";

        var ex = Assert.Throws<FormatException>(() => FixParser.Parse(text));
        Assert.Contains("message 1", ex.Message);
        Assert.Contains("35", ex.Message);
    }

    [Fact]
    public void A_token_with_no_equals_sign_throws()
    {
        const string text = "8=FIX.4.4|9=1|35=8|FOO|10=1|";

        var ex = Assert.Throws<FormatException>(() => FixParser.Parse(text));
        Assert.Contains("no '='", ex.Message);
    }

    // ---- N = 0 ----

    [Fact]
    public void NoMDEntries_zero_emits_an_empty_array()
    {
        const string text = "8=FIX.4.4|9=1|35=W|49=S|56=T|34=1|52=X|55=EURUSD|268=0|10=1|";

        var items = FixParser.Parse(text);
        var entries = items[0].GetProperty("MDEntries");

        Assert.Equal(JsonValueKind.Array, entries.ValueKind);
        Assert.Equal(0, entries.GetArrayLength());
    }

    // ---- The documented ceiling: a group whose first entry omits a field a later entry carries terminates early ----

    [Fact]
    public void Ceiling_a_later_entrys_extra_field_is_NOT_captured_and_lands_on_the_parent_instead()
    {
        // Entry 1 has only MDEntryType+MDEntryPx; entry 2 additionally carries MDEntrySize. Without a
        // dictionary, entry 1's tag set {MDEntryType, MDEntryPx} is all this parser knows to bound entry
        // 2 with — so MDEntrySize (271), being outside that set, ends entry 2 right there and is parsed
        // onto the PARENT object instead of living inside MDEntries[1]. See FixParser.ParseGroup's
        // "ponytail:" comment for the full rule and its upgrade path (a real FIX dictionary).
        const string text =
            "8=FIX.4.4|9=1|35=W|49=S|56=T|34=1|52=X|" +
            "268=2|269=0|270=1.1|269=1|270=1.2|271=999999|10=1|";

        var items = FixParser.Parse(text);
        var msg = items[0];
        var entries = msg.GetProperty("MDEntries");

        Assert.Equal(2, entries.GetArrayLength());
        Assert.False(entries[1].TryGetProperty("MDEntrySize", out _)); // NOT on the second entry...
        Assert.Equal(999999L, msg.GetProperty("MDEntrySize").GetInt64()); // ...it's on the parent instead.
        Assert.Equal("1", msg.GetProperty("CheckSum").GetString()); // parsing resumed normally afterward.
    }

    // ---- Wired into ConnectorPollCycle: ItemsPath fans a snapshot's MDEntries into one row per entry ----

    [Fact]
    public void ConnectorPollCycle_ExecuteMessage_fans_MDEntries_into_one_row_per_entry_via_ItemsPath()
    {
        var def = new SourceDefinition
        {
            Name = "fix-md",
            Kind = SourceKinds.Nats,
            Fields = [new FieldDef("MDEntryPx", FieldType.Double), new FieldDef("MDEntrySize", FieldType.Long)],
            Connector = new ConnectorConfig
            {
                Mapping = new MappingSpec
                {
                    ItemsPath = "$.MDEntries[*]",
                    Fields =
                    [
                        new FieldMapEntry { Field = new FieldDef("MDEntryPx", FieldType.Double) },
                        new FieldMapEntry { Field = new FieldDef("MDEntrySize", FieldType.Long) },
                    ],
                },
            },
        };

        var result = ConnectorPollCycle.ExecuteMessage(def, FileFormats.Fix, SnapshotPipeDelimited, new DedupTracker(), 1000);

        Assert.Null(result.Error);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal(1.2345, result.Rows[0]["MDEntryPx"]);
        Assert.Equal(1000000L, result.Rows[0]["MDEntrySize"]);
        Assert.Equal(1.2350, result.Rows[1]["MDEntryPx"]);
        Assert.Equal(500000L, result.Rows[1]["MDEntrySize"]);
    }
}
