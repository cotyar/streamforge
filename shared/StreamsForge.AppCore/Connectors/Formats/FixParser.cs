using System.Buffers;
using System.Globalization;
using System.Text.Json;

namespace StreamsForge.AppCore.Connectors.Formats;

/// <summary>
/// Plan 018: <see cref="StreamsForge.Abstractions.FileFormats.Fix"/> — tag=value FIX protocol text (a
/// session's own wire framing, or a FIX log/ticket file: the same grammar either way) turned into one
/// flat <see cref="JsonElement"/> per FIX message, the same contract <see cref="FormatParsers.ParseNdjson"/>
/// and <see cref="FormatParsers.ParseCsv(string)"/> already keep: a list of items ready for
/// <see cref="StreamsForge.AppCore.Connectors.Mapping.RecordExtractor"/>, and a
/// <see cref="FormatException"/> naming the 1-based MESSAGE index (FIX's natural unit, the way NDJSON's
/// natural unit is the line) for anything malformed.
///
/// <para><b>No FIX dictionary, on purpose</b> (plan 018's "Decisions" section states the why): tag
/// numbers are globally unique across FIX versions by design — tag 35 is <c>MsgType</c> in every version
/// from 4.0 through 5.0 — so ONE static table of names/types, covering the common 4.2/4.4/5.0 field set,
/// is correct rather than a version-specific compromise. Every tag this table doesn't know becomes
/// <c>"tag&lt;N&gt;"</c> keyed to a JSON string, which is the designed fallback, not a gap: an operator
/// who wants more has <c>ConnectorRowCoercion</c> (declared field types) and 009-C1's SQL conversions
/// downstream, exactly as CSV's untyped columns do.</para>
///
/// <para><b>Repeating groups become nested JSON arrays</b>, dictionary-free — see <see cref="BuildRow"/>
/// for the framing rule and its ceiling (also marked with a <c>// ponytail:</c> comment at the code that
/// hits it).</para>
/// </summary>
public static class FixParser
{
    /// <summary>Delimiter candidates in precedence order — mirrors <see cref="FormatParsers"/>'s private
    /// <c>SniffDelimiter</c> doctrine exactly: count candidates over a representative sample, highest
    /// count wins, a tie goes to the earlier candidate, and "nothing counted" falls back to the FIRST
    /// candidate rather than erroring. SOH (<c>\x01</c>) is what a real session speaks; <c>|</c> and
    /// <c>^</c> are what logs, tickets and test fixtures use, because SOH doesn't paste into a text
    /// editor.</summary>
    private static readonly char[] DelimiterCandidates = ['\x01', '|', '^'];

    /// <summary>The standard trailer. These three are the one part of a FIX message that can never appear
    /// inside a repeating group, in any version — the spec puts them last, after every body field — so
    /// they bound a group without a dictionary knowing anything about that group, exactly as
    /// <see cref="CounterTags"/> opens one. Without this a SINGLE-entry group has nothing to end it (it
    /// has no second entry to reveal its own tag set, and the trailer repeats none of its tags) and
    /// swallows the trailer into the entry — which is the most ordinary message shape there is: one group
    /// entry, then the checksum.</summary>
    private static readonly HashSet<int> TrailerTags = [93, 89, 10];

    /// <summary>Parses <paramref name="text"/> into one <see cref="JsonElement"/> per FIX message.
    /// The delimiter is sniffed ONCE for the whole text (see <see cref="SniffDelimiter"/>) — one input is
    /// one session's log or one HTTP/NATS payload, never a mix of delimiters within itself.</summary>
    public static List<JsonElement> Parse(string text)
    {
        var delimiter = SniffDelimiter(text);
        var frames = SplitFrames(text, delimiter);

        var items = new List<JsonElement>(frames.Count);
        for (var f = 0; f < frames.Count; f++)
        {
            var messageIndex = f + 1;
            var fields = Tokenize(frames[f], delimiter, messageIndex);

            var hasMsgType = false;
            foreach (var field in fields)
            {
                if (field.Tag == 35) { hasMsgType = true; break; }
            }
            if (!hasMsgType)
            {
                throw new FormatException($"FIX message {messageIndex}: no MsgType (tag 35) field — not a FIX message frame");
            }

            items.Add(BuildJsonElement(BuildRow(fields)));
        }

        return items;
    }

    /// <summary>Counts each candidate over the FIRST FRAME ONLY — the text up to (not including) the
    /// first <c>"10="</c> checksum terminator, or the whole text when no terminator is found (a
    /// truncated fixture, or one message with no trailing checksum). Same honest failure mode as CSV's:
    /// guess wrong, and the message becomes one unparseable field with no recognizable <c>35=</c>, which
    /// throws on the very first frame rather than silently mis-splitting every value downstream.</summary>
    private static char SniffDelimiter(string text)
    {
        var terminator = text.IndexOf("10=", StringComparison.Ordinal);
        var sample = terminator >= 0 ? text.AsSpan(0, terminator) : text.AsSpan();

        var counts = new int[DelimiterCandidates.Length];
        foreach (var c in sample)
        {
            var idx = Array.IndexOf(DelimiterCandidates, c);
            if (idx >= 0) counts[idx]++;
        }

        var best = 0;
        for (var i = 1; i < counts.Length; i++)
        {
            if (counts[i] > counts[best]) best = i;
        }

        return counts[best] == 0 ? '\x01' : DelimiterCandidates[best];
    }

    /// <summary>A message begins at <c>8=</c> occurring at the start of the text, immediately after the
    /// sniffed delimiter, or immediately after a newline (<c>\r</c>/<c>\n</c>) — a log file routinely
    /// prefixes each line with a timestamp/direction marker ahead of the actual <c>8=</c>, and that
    /// prefix is silently ignored rather than rejected. Each frame runs to the start of the next
    /// <c>8=</c> or the end of the text; trailing whitespace/newlines are trimmed (a trailing DELIMITER
    /// is deliberately NOT trimmed here — <see cref="Tokenize"/> already skips the empty token it would
    /// produce, same as a doubled delimiter anywhere else in the frame).</summary>
    private static List<string> SplitFrames(string text, char delimiter)
    {
        var starts = new List<int>();
        for (var i = 0; i + 1 < text.Length; i++)
        {
            if (text[i] == '8' && text[i + 1] == '='
                && (i == 0 || text[i - 1] == delimiter || text[i - 1] == '\r' || text[i - 1] == '\n'))
            {
                starts.Add(i);
            }
        }

        var frames = new List<string>(starts.Count);
        for (var s = 0; s < starts.Count; s++)
        {
            var end = s + 1 < starts.Count ? starts[s + 1] : text.Length;
            var frame = text[starts[s]..end].TrimEnd();
            if (frame.Length > 0)
            {
                frames.Add(frame);
            }
        }

        return frames;
    }

    // ---- Tokenizing one frame into an ordered (tag, rawValue) list ----

    /// <summary>Length tag → its paired data tag (plan 018's list). The value carried under the length
    /// tag is a character COUNT; the data tag's value is then taken verbatim for exactly that many
    /// characters, delimiters and <c>=</c> signs included. This is FIX's answer to CSV's quoted field,
    /// and skipping it corrupts exactly the messages (raw/encoded/XML payloads) that are hardest to
    /// debug after the fact.</summary>
    private static readonly Dictionary<int, int> LengthToDataTag = new()
    {
        [90] = 91, [95] = 96, [212] = 213,
        [350] = 351, [354] = 355, [358] = 359, [360] = 361, [362] = 363, [364] = 365,
    };

    /// <summary>Splits one frame into an ordered list of (tag, raw value) pairs. <c>tag=value</c> is
    /// split at the FIRST <c>=</c> only — values legitimately contain <c>=</c> (an XML blob, a raw
    /// secure-data payload). An empty token (a trailing or doubled delimiter) is skipped, not an error.
    /// A token with no <c>=</c>, or whose tag isn't a positive integer, throws — see the two
    /// <see cref="FormatException"/> sites below.</summary>
    private static List<(int Tag, string Value)> Tokenize(string frame, char delimiter, int messageIndex)
    {
        var fields = new List<(int Tag, string Value)>();
        var n = frame.Length;
        var i = 0;

        while (i < n)
        {
            var delimiterIdx = frame.IndexOf(delimiter, i);
            var tokenEnd = delimiterIdx < 0 ? n : delimiterIdx;
            var afterToken = delimiterIdx < 0 ? n : delimiterIdx + 1;

            if (tokenEnd == i)
            {
                i = afterToken; // empty token: a trailing or doubled delimiter, not an error.
                continue;
            }

            var eq = frame.IndexOf('=', i, tokenEnd - i);
            if (eq < 0)
            {
                throw new FormatException(
                    $"FIX message {messageIndex}: field has no '=': \"{Truncate(frame[i..tokenEnd])}\"");
            }

            if (!TryParseTag(frame, i, eq - i, out var tag))
            {
                throw new FormatException(
                    $"FIX message {messageIndex}: field has a non-numeric or non-positive tag: \"{Truncate(frame[i..tokenEnd])}\"");
            }

            var value = frame[(eq + 1)..tokenEnd];

            if (LengthToDataTag.TryGetValue(tag, out var expectedDataTag)
                && int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var declaredLength)
                && TryReadLengthPrefixed(frame, afterToken, delimiter, expectedDataTag, declaredLength, out var dataValue, out var resumeAt))
            {
                fields.Add((tag, value));
                fields.Add((expectedDataTag, dataValue));
                i = resumeAt;
                continue;
            }

            fields.Add((tag, value));
            i = afterToken;
        }

        return fields;
    }

    /// <summary>Reads the field immediately after a length tag's own token, verbatim, for exactly
    /// <paramref name="length"/> characters. When that next field is NOT the expected paired data tag,
    /// returns false so <see cref="Tokenize"/> falls back to parsing the length tag as an ordinary field
    /// and re-examining what follows normally — some producers omit an optional encoded field entirely,
    /// and a length tag with nothing to pair with is not malformed, just unpaired.</summary>
    private static bool TryReadLengthPrefixed(
        string frame, int start, char delimiter, int expectedDataTag, int length,
        out string dataValue, out int resumeAt)
    {
        dataValue = "";
        resumeAt = start;

        if (start >= frame.Length)
        {
            return false;
        }

        var peekEnd = frame.IndexOf(delimiter, start);
        if (peekEnd < 0) peekEnd = frame.Length;
        var peek = frame[start..peekEnd];

        var eq = peek.IndexOf('=');
        if (eq <= 0 || !TryParseTag(peek, 0, eq, out var peekTag) || peekTag != expectedDataTag)
        {
            return false; // not the expected pair — fall back to normal parsing.
        }

        var dataStart = start + eq + 1;
        var take = Math.Min(length, frame.Length - dataStart); // a producer whose declared length overruns what's left is not this parser's call to make fatal.
        dataValue = frame.Substring(dataStart, take);

        var after = dataStart + take;
        if (after < frame.Length && frame[after] == delimiter)
        {
            after++; // one delimiter, if present, is the raw field's own terminator — skip it once.
        }
        resumeAt = after;
        return true;
    }

    private static bool TryParseTag(string s, int start, int length, out int tag)
        => int.TryParse(s.AsSpan(start, length), NumberStyles.None, CultureInfo.InvariantCulture, out tag) && tag > 0;

    private static string Truncate(string s) => s.Length > 40 ? s[..40] + "…" : s;

    // ---- Repeating groups: counter tag → group field name (leading "No" stripped) ----

    /// <summary>Group-COUNTER tags this parser recognizes (plan 018's list), mapped to the JSON array
    /// property name emitted for the group — the counter's OWN value is never also emitted alongside it:
    /// the array's <c>Length</c> already says how many entries there are, and a sibling
    /// <c>"NoXxx": N</c> property would just be a second number that could disagree with the first after
    /// any downstream edit. A tag in this table whose value does NOT parse as a usable non-negative
    /// count is NOT treated as a group opener at all — <see cref="Consume"/> falls back to typing it as
    /// an ordinary field via <see cref="TagTypes"/>/<see cref="TagNames"/> instead of throwing, matching
    /// this parser's general "fall back rather than fail the batch" doctrine.</summary>
    private static readonly Dictionary<int, string> CounterTags = new()
    {
        [73] = "Orders", [78] = "Allocs", [136] = "MiscFees", [146] = "RelatedSym",
        [215] = "RoutingIDs", [232] = "Stipulations", [267] = "MDEntryTypes", [268] = "MDEntries",
        [295] = "QuoteEntries", [296] = "QuoteSets", [382] = "ContraBrokers", [384] = "MsgTypes",
        [386] = "TradingSessions", [428] = "Strikes", [453] = "PartyIDs", [454] = "SecurityAltID",
        [518] = "ContAmts", [534] = "AffectedOrders", [539] = "NestedPartyIDs", [552] = "Sides",
        [555] = "Legs", [683] = "LegStipulations", [702] = "Positions", [711] = "Underlyings",
        [768] = "TrdRegTimestamps", [862] = "Capacities", [864] = "Events", [870] = "InstrAttrib",
        [957] = "StrategyParameters",
    };

    /// <summary>Builds one FIX message's flat field list into the nested row <see cref="BuildJsonElement"/>
    /// serializes, per plan 018's dictionary-free group-framing rule:
    ///
    /// <list type="bullet">
    /// <item>a tag in <see cref="CounterTags"/> whose value is a usable non-negative integer N opens a
    /// group; the tag immediately after it is the group's delimiter tag D;</item>
    /// <item>a new entry begins at each occurrence of D; the FIRST entry ends at the next D, at the
    /// first tag that repeats one already seen in that entry, or at end of message — this is the only
    /// boundary rule available without a dictionary, and it is also the parser's ceiling, below;</item>
    /// <item>the first entry's tag set then bounds every LATER entry: from entry 2 on, a tag OUTSIDE
    /// that set ends the group right there and is handed back to whatever level opened it;</item>
    /// <item>parsing stops after N entries; a D found beyond that is handed back the same way (a
    /// counter that disagrees with reality is the producer's bug, not this parser's problem to fail
    /// over);</item>
    /// <item>a counter tag found INSIDE an entry opens a nested group — recursion, not a second code
    /// path (<see cref="ParseGroup"/> calls <see cref="Consume"/> calls <see cref="ParseGroup"/>).</item>
    /// </list>
    /// </summary>
    private static Dictionary<string, object?> BuildRow(List<(int Tag, string Value)> fields)
    {
        var row = new Dictionary<string, object?>();
        var j = 0;
        while (j < fields.Count)
        {
            j = Consume(fields, j, row);
        }
        return row;
    }

    /// <summary>Consumes exactly one field at <paramref name="fields"/>[<paramref name="j"/>] into
    /// <paramref name="target"/> — a plain typed value, or (when the tag is a group counter with a
    /// usable count) a whole nested group parsed by <see cref="ParseGroup"/>. Returns the index of the
    /// next unconsumed field. <b>Duplicate non-group tag: last occurrence wins</b>, matching
    /// <see cref="FormatParsers.ParseCsv(string)"/>'s duplicate-header rule — a
    /// <see cref="Dictionary{TKey,TValue}"/> already overwrites on re-assignment, so this needs no
    /// special case at all.</summary>
    private static int Consume(List<(int Tag, string Value)> fields, int j, Dictionary<string, object?> target)
    {
        var (tag, value) = fields[j];

        if (CounterTags.TryGetValue(tag, out var groupName)
            && int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var count))
        {
            if (count == 0 || j + 1 >= fields.Count)
            {
                target[groupName] = new List<Dictionary<string, object?>>();
                return j + 1;
            }

            var delimiterTag = fields[j + 1].Tag;
            var (entries, next) = ParseGroup(fields, j + 1, delimiterTag, count);
            target[groupName] = entries;
            return next;
        }

        target[FieldName(tag)] = TypedValue(tag, value);
        return j + 1;
    }

    /// <summary>Parses up to <paramref name="count"/> entries of a group whose delimiter tag is
    /// <paramref name="delimiterTag"/>, starting at <paramref name="start"/> (which the caller has
    /// already positioned AT an occurrence of that tag). See <see cref="BuildRow"/> for the framing rule
    /// this implements.</summary>
    private static (List<Dictionary<string, object?>> Entries, int NextIndex) ParseGroup(
        List<(int Tag, string Value)> fields, int start, int delimiterTag, int count)
    {
        var entries = new List<Dictionary<string, object?>>();
        HashSet<int>? firstEntryTags = null;
        var idx = start;

        for (var entryNum = 1; entryNum <= count; entryNum++)
        {
            if (idx >= fields.Count || fields[idx].Tag != delimiterTag)
            {
                break; // the counter disagreed with reality — stop; whatever remains belongs to the caller.
            }

            var entry = new Dictionary<string, object?>();
            var entryTags = new HashSet<int>();
            var j = idx;
            while (j < fields.Count)
            {
                var tag = fields[j].Tag;
                if (TrailerTags.Contains(tag))
                {
                    break; // the standard trailer ends every group at every nesting level — see TrailerTags.
                }

                if (j > idx)
                {
                    if (entryNum == 1)
                    {
                        if (tag == delimiterTag || entryTags.Contains(tag))
                        {
                            break; // first entry's own boundary: next D, or a repeated tag.
                        }
                    }
                    else if (tag == delimiterTag || !firstEntryTags!.Contains(tag))
                    {
                        // ponytail: no FIX dictionary means a group's membership is inferred from the
                        // first entry's tag set, never declared anywhere — so a group whose FIRST entry
                        // omits an optional field that a LATER entry carries terminates that later entry
                        // early, right at the tag the first entry never had, and everything from there to
                        // the end of the enclosing level lands on the PARENT object instead of the entry
                        // it visually belongs to. (A single-entry group has no "later entry" to expose
                        // the gap at all, so it is bounded only by "next D, a repeated tag, or the standard
                        // trailer" — see TrailerTags, which is what keeps the ordinary one-entry-then-
                        // checksum message correct. A single-entry group that is NOT the last thing at its
                        // level still absorbs the fields that follow it.) Upgrade path: a
                        // real FIX dictionary, which knows a group's field set without needing a second
                        // entry to reveal it. FixParserTests names this ceiling explicitly.
                        break;
                    }
                }

                entryTags.Add(tag);
                j = Consume(fields, j, entry);
            }

            entries.Add(entry);
            firstEntryTags ??= entryTags;
            idx = j;
        }

        return (entries, idx);
    }

    // ---- Tag → name, tag → JSON type ----

    private enum Kind { Number, Boolean }

    /// <summary>FIX tags whose type this table maps to a JSON number (FIX type int/Length/SeqNum/
    /// NumInGroup/Qty/Price/PriceOffset/Amt/Percentage/float) or a JSON boolean (FIX's own
    /// <c>Boolean</c> type — exactly "Y"/"N"). Every tag NOT in this table — known or unknown — is a
    /// JSON string; see the class doc for why that is a deliberate default and not a gap.</summary>
    private static readonly Dictionary<int, Kind> TagTypes = new()
    {
        // Session/trailer.
        [9] = Kind.Number, [34] = Kind.Number, [43] = Kind.Boolean, [93] = Kind.Number,

        // Length-prefixed pairs (see LengthToDataTag): the LENGTH half is Kind.Number (FIX type
        // Length); the paired DATA half is deliberately absent from this table — RawData/SecureData/
        // XmlData/Encoded* are FIX type "data", always a string, verbatim.
        [90] = Kind.Number, [95] = Kind.Number, [212] = Kind.Number,
        [350] = Kind.Number, [354] = Kind.Number, [358] = Kind.Number,
        [360] = Kind.Number, [362] = Kind.Number, [364] = Kind.Number,

        // Order/execution.
        [6] = Kind.Number, [14] = Kind.Number, [31] = Kind.Number, [32] = Kind.Number,
        [38] = Kind.Number, [44] = Kind.Number, [99] = Kind.Number, [103] = Kind.Number,
        [110] = Kind.Number, [111] = Kind.Number, [151] = Kind.Number, [201] = Kind.Number,
        [202] = Kind.Number, [223] = Kind.Number, [231] = Kind.Number,

        // Market data.
        [264] = Kind.Number, [265] = Kind.Number, [266] = Kind.Boolean, [270] = Kind.Number,
        [271] = Kind.Number, [287] = Kind.Number,

        // Session status / parties / misc.
        [340] = Kind.Number, [377] = Kind.Boolean, [378] = Kind.Number, [381] = Kind.Number,
        [423] = Kind.Number, [452] = Kind.Number,

        // Repeating-group counters (FIX type NumInGroup — int) — used only when Consume falls back to
        // typing one as an ordinary field because its value wasn't a usable group count.
        [73] = Kind.Number, [78] = Kind.Number, [136] = Kind.Number, [146] = Kind.Number,
        [215] = Kind.Number, [232] = Kind.Number, [267] = Kind.Number, [268] = Kind.Number,
        [295] = Kind.Number, [296] = Kind.Number, [382] = Kind.Number, [384] = Kind.Number,
        [386] = Kind.Number, [428] = Kind.Number, [453] = Kind.Number, [454] = Kind.Number,
        [518] = Kind.Number, [534] = Kind.Number, [539] = Kind.Number, [552] = Kind.Number,
        [555] = Kind.Number, [683] = Kind.Number, [702] = Kind.Number, [711] = Kind.Number,
        [768] = Kind.Number, [862] = Kind.Number, [864] = Kind.Number, [870] = Kind.Number,
        [957] = Kind.Number,
    };

    /// <summary>Tag → field name, for every tag this parser names. A tag not present here becomes
    /// <c>"tag&lt;N&gt;"</c> (see <see cref="FieldName"/>) — deliberately incomplete rather than guessed:
    /// plan 018 is explicit that a wrong name is worse than the fallback, so every mapping below was
    /// checked against the FIX 4.2/4.4/5.0 spec before being added.</summary>
    private static readonly Dictionary<int, string> TagNames = new()
    {
        // Session header/trailer.
        [8] = "BeginString", [9] = "BodyLength", [10] = "CheckSum", [34] = "MsgSeqNum",
        [35] = "MsgType", [43] = "PossDupFlag", [49] = "SenderCompID", [50] = "SenderSubID",
        [52] = "SendingTime", [56] = "TargetCompID", [57] = "TargetSubID", [89] = "Signature",
        [93] = "SignatureLength", [115] = "OnBehalfOfCompID", [122] = "OrigSendingTime",
        [128] = "DeliverToCompID",

        // Length-prefixed pairs (see LengthToDataTag's own doc comment for why these travel together).
        [90] = "SecureDataLen", [91] = "SecureData", [95] = "RawDataLength", [96] = "RawData",
        [212] = "XmlDataLen", [213] = "XmlData",
        [350] = "EncodedIssuerLen", [351] = "EncodedIssuer",
        [354] = "EncodedSecurityDescLen", [355] = "EncodedSecurityDesc",
        [358] = "EncodedTextLen", [359] = "EncodedText",
        [360] = "EncodedHeadlineLen", [361] = "EncodedHeadline",
        [362] = "EncodedAllocTextLen", [363] = "EncodedAllocText",
        [364] = "EncodedUnderlyingIssuerLen", [365] = "EncodedUnderlyingIssuer",

        // Order / execution.
        [1] = "Account", [6] = "AvgPx", [11] = "ClOrdID", [14] = "CumQty", [17] = "ExecID",
        [18] = "ExecInst", [20] = "ExecTransType", [21] = "HandlInst", [22] = "SecurityIDSource",
        [31] = "LastPx", [32] = "LastQty", [37] = "OrderID", [38] = "OrderQty", [39] = "OrdStatus",
        [40] = "OrdType", [41] = "OrigClOrdID", [44] = "Price", [48] = "SecurityID", [54] = "Side",
        [55] = "Symbol", [58] = "Text", [59] = "TimeInForce", [60] = "TransactTime",
        [63] = "SettlType", [64] = "SettlDate", [65] = "SymbolSfx", [75] = "TradeDate",
        [99] = "StopPx", [100] = "ExDestination", [103] = "OrdRejReason", [107] = "SecurityDesc",
        [109] = "ClientID", [110] = "MinQty", [111] = "MaxFloor", [126] = "ExpireTime",
        [150] = "ExecType", [151] = "LeavesQty", [167] = "SecurityType", [200] = "MaturityMonthYear",
        [201] = "PutOrCall", [202] = "StrikePrice", [207] = "SecurityExchange", [223] = "CouponRate",
        [231] = "ContractMultiplier",

        // Market data.
        [262] = "MDReqID", [263] = "SubscriptionRequestType", [264] = "MarketDepth",
        [265] = "MDUpdateType", [266] = "AggregatedBook", [269] = "MDEntryType",
        [270] = "MDEntryPx", [271] = "MDEntrySize", [272] = "MDEntryDate", [273] = "MDEntryTime",
        [274] = "TickDirection", [278] = "MDEntryID", [279] = "MDUpdateAction",
        [280] = "MDEntryRefID", [285] = "DeleteReason", [286] = "OpenCloseSettlFlag",
        [287] = "SellerDays",

        // Trading session / parties / instrument.
        [336] = "TradingSessionID", [340] = "TradSesStatus", [375] = "ContraBroker",
        [376] = "ComplianceID", [377] = "SolicitedFlag", [378] = "ExecRestatementReason",
        [379] = "BusinessRejectRefID", [381] = "GrossTradeAmt", [423] = "PriceType",
        [432] = "ExpireDate", [442] = "MultiLegReportingType", [447] = "PartyIDSource",
        [448] = "PartyID", [452] = "PartyRole", [455] = "SecurityAltID", [456] = "SecurityAltIDSource",
        [527] = "SecondaryExecID", [541] = "MaturityDate",

        // Repeating-group counters — the FULL "No…" name, used only in the fallback where Consume types
        // one as an ordinary field; CounterTags holds the STRIPPED name used when the counter
        // successfully opens a group (see that table's own doc comment).
        [73] = "NoOrders", [78] = "NoAllocs", [136] = "NoMiscFees", [146] = "NoRelatedSym",
        [215] = "NoRoutingIDs", [232] = "NoStipulations", [267] = "NoMDEntryTypes",
        [268] = "NoMDEntries", [295] = "NoQuoteEntries", [296] = "NoQuoteSets",
        [382] = "NoContraBrokers", [384] = "NoMsgTypes", [386] = "NoTradingSessions",
        [428] = "NoStrikes", [453] = "NoPartyIDs", [454] = "NoSecurityAltID", [518] = "NoContAmts",
        [534] = "NoAffectedOrders", [539] = "NoNestedPartyIDs", [552] = "NoSides", [555] = "NoLegs",
        [683] = "NoLegStipulations", [702] = "NoPositions", [711] = "NoUnderlyings",
        [768] = "NoTrdRegTimestamps", [862] = "NoCapacities", [864] = "NoEvents",
        [870] = "NoInstrAttrib", [957] = "NoStrategyParameters",
    };

    private static string FieldName(int tag) => TagNames.TryGetValue(tag, out var name) ? name : $"tag{tag}";

    /// <summary>Types <paramref name="raw"/> from <see cref="TagTypes"/>, never by sniffing the text —
    /// plan 018's "Decisions" section states why: CSV sniffs because it has nothing better, but FIX has
    /// a spec that says tag 44 is a price and tag 55 is a symbol, so sniffing would silently turn
    /// <c>55=123</c> into the NUMBER 123 instead of the symbol "123". A numeric-typed tag whose value
    /// doesn't parse as either <see cref="long"/> or <see cref="double"/> — a venue sending garbage in a
    /// price field — falls back to a JSON string rather than throwing: one bad field must not kill the
    /// whole batch. A boolean-typed tag is JSON <c>true</c>/<c>false</c> for EXACTLY "Y"/"N"
    /// (case-sensitive, per the FIX spec's own Boolean type) and a string for anything else.</summary>
    private static object TypedValue(int tag, string raw)
    {
        if (!TagTypes.TryGetValue(tag, out var kind))
        {
            return raw;
        }

        if (kind == Kind.Boolean)
        {
            return raw switch { "Y" => true, "N" => false, _ => raw };
        }

        if (long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l)) return l;
        if (double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var d)) return d;
        return raw;
    }

    // ---- Serializing the nested Dictionary/List/scalar tree — same shape BuildCsvRowElement uses ----

    private static JsonElement BuildJsonElement(Dictionary<string, object?> row)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            WriteObject(writer, row);
        }

        using var doc = JsonDocument.Parse(buffer.WrittenMemory);
        return doc.RootElement.Clone();
    }

    private static void WriteObject(Utf8JsonWriter writer, Dictionary<string, object?> row)
    {
        writer.WriteStartObject();
        foreach (var (key, value) in row)
        {
            writer.WritePropertyName(key);
            WriteValue(writer, value);
        }
        writer.WriteEndObject();
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case long l: writer.WriteNumberValue(l); break;
            case double d: writer.WriteNumberValue(d); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case string s: writer.WriteStringValue(s); break;
            case List<Dictionary<string, object?>> entries:
                writer.WriteStartArray();
                foreach (var entry in entries)
                {
                    WriteObject(writer, entry);
                }
                writer.WriteEndArray();
                break;
            default: writer.WriteNullValue(); break; // unreachable: Consume/TypedValue only ever produce the above.
        }
    }
}
