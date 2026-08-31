using System.Globalization;
using System.Text;
using QuickFix;

namespace StreamsForge.Connectors.Fix;

/// <summary>
/// Plan 019 wave E: row → FIX message, the minimum that works (D6/D7 — a real dictionary, required-field
/// validation, <c>ClOrdID</c> generation and the <c>OrigClOrdID</c> chain — are explicitly wave 019-F's
/// job, not this one's).
///
/// <para><b>The name → tag table is the REVERSE of <c>FixParser.TagNames</c></b> (`shared/StreamsForge.
/// AppCore/Connectors/Formats/FixParser.cs`), built independently rather than by reaching into it: that
/// table is private to a class in a DIFFERENT project (<c>StreamsForge.AppCore</c>), and this is the exact
/// same call <see cref="FixBridgeApplication"/>'s own <c>ToWireText</c> already made for the delimiter
/// sniffer it duplicates from <c>FixParser</c> — see that method's doc comment: "duplicated rather than
/// shared … the two are one line of logic each and diverging would be a worse cost than the duplication."
/// This table is bigger than one line, but the same trade applies: exposing a private table across an
/// assembly boundary for one reader is a worse cost than a second copy that a code reviewer can diff
/// against the original by eye. Covers the order/execution and market-data/party fields — the ones an
/// outbound message plausibly carries — and deliberately EXCLUDES <c>FixParser.TagNames</c>' session
/// header/trailer entries (BeginString, SenderCompID, TargetCompID, MsgSeqNum, SendingTime, …): those are
/// stamped by QuickFIX/n's own <c>Session</c> layer from the session's config, and a row column that tried
/// to set them would be silently overridden anyway, or worse, accepted and misleading. Also excludes the
/// length-prefixed pairs (<c>RawDataLength</c>/<c>RawData</c> and siblings — inbound-only, no outbound
/// encoder for them this wave) and the repeating-group counters (outbound groups are not this wave's
/// "minimum that works").</para>
/// </summary>
/// <remarks>Public, not internal — house style (see <see cref="FixBridgeApplication"/>'s own doc comment):
/// specifically so a test project can drive the mapping directly with hand-built rows, the same "fake seam
/// (no socket)" plan 018-C established for <see cref="FixBridgeApplication"/> itself.</remarks>
public static class FixRowMapper
{
    /// <summary>The row column FIX's MsgType (tag 35) is read from. <b>The message type comes from the
    /// ROW, not from <see cref="FixSourceConfig"/></b> — a single duplex source's outbound half plausibly
    /// carries more than one message type in one pipeline (a <c>NewOrderSingle</c> here, an
    /// <c>OrderCancelRequest</c> there), and a per-source config default cannot express that; requiring the
    /// row to carry it costs nothing since the SELECT that produced the row already knows what kind of
    /// message it is describing. Named "MsgType" to match <c>FixParser.TagNames[35]</c> exactly, so a table
    /// mapped ONE way round on the inbound side and read the OTHER way round here still agree on the
    /// column name — an operator who has looked at an inbound FIX row already knows this column.</summary>
    private const string MsgTypeColumn = "MsgType";

    /// <summary>The row column a failed send's <see cref="DuplexSendFailure.CorrelationId"/> is read from
    /// — tag 11, <c>ClOrdID</c>, the field <see cref="StreamsForge.AppCore.Transports.IDuplexSession.LastFailure"/>'s
    /// own doc comment names as "the part an operator can actually act on".</summary>
    private const string ClOrdIdColumn = "ClOrdID";

    /// <summary>Plan 019 wave 019-I2 (found live, not by a unit test): the row keys the PLATFORM stamps
    /// onto a row on its way to a sink, in addition to whatever columns the row's own SQL selected —
    /// <c>StreamsForge.Engine.PublicApi.EventRecord.TimestampField</c> ("_ts") and <c>.SourceField</c>
    /// ("_source"), present on every row the engine produces, plus <c>SinkStepGuard.RowOf</c>'s own
    /// "_weight" (`shared/StreamsForge.AppCore/Sinks/SinkStepGuard.cs`), stamped ONLY for a TABLE delta —
    /// "so a retraction is not indistinguishable from an insert downstream", that class's own doc comment
    /// says. This project cannot reference <c>StreamsForge.Engine</c> OR <c>StreamsForge.AppCore.Sinks</c>
    /// (see this file's own doc comment on why the tag table is typed out rather than shared across a
    /// project boundary — the same reasoning applies here), so the three literals are duplicated rather
    /// than imported.
    ///
    /// <para><b>Before this set existed, EVERY row reaching <see cref="TryBuildMessage"/> from a real
    /// duplex sink send — as opposed to a hand-built <c>Dictionary</c> in a unit test, which never carries
    /// these keys — failed, 100% of the time.</b> A PIPELINE-sourced duplex sink failed on "_ts" (the
    /// first key <see cref="Dictionary{TKey,TValue}"/> happened to enumerate); a TABLE-sourced one (the
    /// only shape wave 019-I2's drop-copy check exercises — a duplex sink is declared on a
    /// <c>TableDefinition</c>, not a <c>PipelineDefinition</c>, because D7's correlation table needs a
    /// materialized "orders sent" table to join against) additionally carries "_weight" and failed on
    /// THAT instead, one send at a time, only after the first two were fixed — this class's own doc
    /// comment's "minimum that works" undersold how far short of a real row shape wave E/F's own unit
    /// tests stayed. Wave E/F's acceptance tests never caught any of this because they call
    /// <c>IDuplexSession.SendAsync</c> (via <c>FixDuplexSession</c>) directly with rows they constructed
    /// by hand; only a check driven through a real <c>TableDefinition</c>'s SQL output — a duplex sink's
    /// actual, only calling convention in production — carries the platform's own stamped keys and
    /// exposed the gap.</para>
    ///
    /// <para>Skipped here exactly like <see cref="MsgTypeColumn"/> already is: a reserved platform column
    /// is not a business field with an opinion about FIX tags, and silently dropping it (rather than
    /// refusing the whole message) is the same judgment call <see cref="MsgTypeColumn"/>'s existing skip
    /// already makes. A row's own genuine business column that happens to collide with one of these three
    /// names cannot occur — all three are names the SQL layer itself reserves (see
    /// <c>StreamsForge.Engine.PublicApi</c>'s own doc comment: "Reserved keys: '_ts' … '_source'"), so no
    /// column with these names ever reaches a row for a reason this class would need to second-guess.</para></summary>
    private static readonly HashSet<string> ReservedRowColumns = new(StringComparer.Ordinal) { "_ts", "_source", "_weight" };

    /// <summary>Tag → name is <c>FixParser.TagNames</c>; this is that table's mirror image, name → tag,
    /// for the fields an outbound row plausibly carries. See this class's doc comment for why it is typed
    /// out again rather than shared.</summary>
    private static readonly Dictionary<string, int> TagByName = new(StringComparer.Ordinal)
    {
        // Order / execution.
        ["Account"] = 1, ["AvgPx"] = 6, ["ClOrdID"] = 11, ["CumQty"] = 14, ["ExecID"] = 17,
        ["ExecInst"] = 18, ["ExecTransType"] = 20, ["HandlInst"] = 21, ["SecurityIDSource"] = 22,
        ["LastPx"] = 31, ["LastQty"] = 32, ["OrderID"] = 37, ["OrderQty"] = 38, ["OrdStatus"] = 39,
        ["OrdType"] = 40, ["OrigClOrdID"] = 41, ["Price"] = 44, ["SecurityID"] = 48, ["Side"] = 54,
        ["Symbol"] = 55, ["Text"] = 58, ["TimeInForce"] = 59, ["TransactTime"] = 60,
        ["SettlType"] = 63, ["SettlDate"] = 64, ["SymbolSfx"] = 65, ["TradeDate"] = 75,
        ["StopPx"] = 99, ["ExDestination"] = 100, ["OrdRejReason"] = 103, ["SecurityDesc"] = 107,
        ["ClientID"] = 109, ["MinQty"] = 110, ["MaxFloor"] = 111, ["ExpireTime"] = 126,
        ["ExecType"] = 150, ["LeavesQty"] = 151, ["SecurityType"] = 167, ["MaturityMonthYear"] = 200,
        ["PutOrCall"] = 201, ["StrikePrice"] = 202, ["SecurityExchange"] = 207, ["CouponRate"] = 223,
        ["ContractMultiplier"] = 231,

        // Market data.
        ["MDReqID"] = 262, ["SubscriptionRequestType"] = 263, ["MarketDepth"] = 264,
        ["MDUpdateType"] = 265, ["AggregatedBook"] = 266, ["MDEntryType"] = 269,
        ["MDEntryPx"] = 270, ["MDEntrySize"] = 271, ["MDEntryDate"] = 272, ["MDEntryTime"] = 273,
        ["TickDirection"] = 274, ["MDEntryID"] = 278, ["MDUpdateAction"] = 279,
        ["MDEntryRefID"] = 280, ["DeleteReason"] = 285, ["OpenCloseSettlFlag"] = 286,
        ["SellerDays"] = 287,

        // Trading session / parties / instrument.
        ["TradingSessionID"] = 336, ["TradSesStatus"] = 340, ["ContraBroker"] = 375,
        ["ComplianceID"] = 376, ["SolicitedFlag"] = 377, ["ExecRestatementReason"] = 378,
        ["BusinessRejectRefID"] = 379, ["GrossTradeAmt"] = 381, ["PriceType"] = 423,
        ["ExpireDate"] = 432, ["MultiLegReportingType"] = 442, ["PartyIDSource"] = 447,
        ["PartyID"] = 448, ["PartyRole"] = 452, ["SecurityAltID"] = 455, ["SecurityAltIDSource"] = 456,
        ["SecondaryExecID"] = 527, ["MaturityDate"] = 541,
    };

    /// <summary>The message types whose FIX definition REQUIRES <c>TransactTime</c> (tag 60) — the order
    /// entry types wave F curated. Kept here beside the tag table rather than in
    /// <see cref="FixRequiredFields"/> because this is a send-time stamping concern, not a validation
    /// one.</summary>
    private static readonly HashSet<string> StampTransactTimeFor = new(StringComparer.Ordinal) { "D", "F", "G" };

    /// <summary>Stamps <c>TransactTime</c> (tag 60) when an order-entry row does not carry one, returning
    /// the row unchanged in every other case. The caller's dictionary is never mutated.
    ///
    /// <para><b>Why this exists.</b> Wave F left <c>TransactTime</c> out of
    /// <see cref="FixRequiredFields"/>'s table on the reasoning that it is "stamped at send time, like
    /// <c>SendingTime</c>". Half of that is right and the half that is wrong is the dangerous half:
    /// QuickFIX/n's <c>Session</c> layer does stamp <c>SendingTime</c> (tag 52) — it does NOT stamp tag 60.
    /// FIX 4.4 requires 60 on <c>D</c>/<c>F</c>/<c>G</c>, so leaving it neither required of the row nor
    /// stamped here sent an order the venue rejects — precisely the outcome plan 019 D6 calls worse than
    /// refusing to send. Stamping is the friendlier of the two available fixes: a row carrying its own
    /// value keeps it (the platform never overwrites a caller's timestamp), and a row without one still
    /// produces a message a venue will accept.</para>
    ///
    /// <para>ponytail: UTC now, formatted as FIX's UTCTimestamp with milliseconds. The value is the moment
    /// this platform handed the row to the session, which is the best answer available here — a row that
    /// wants the time its ORIGINATING system considered the transaction to have occurred must carry that
    /// itself, and can.</para></summary>
    public static Dictionary<string, object?> WithTransactTimeIfNeeded(Dictionary<string, object?> row)
    {
        if (MsgTypeOf(row) is not { } msgType
            || !StampTransactTimeFor.Contains(msgType)
            || (row.TryGetValue("TransactTime", out var existing) && existing is not null && existing.ToString()?.Length > 0))
        {
            return row;
        }

        return new Dictionary<string, object?>(row, StringComparer.Ordinal)
        {
            ["TransactTime"] = DateTime.UtcNow.ToString("yyyyMMdd-HH:mm:ss.fff", CultureInfo.InvariantCulture),
        };
    }

    /// <summary>Builds a <see cref="Message"/> from one row, or reports why it could not. NEVER throws for
    /// an ordinary mapping problem — see <see cref="StreamsForge.AppCore.Transports.IDuplexSession.SendAsync"/>'s
    /// own doc comment for why that contract starts here, one layer below it.
    ///
    /// <para>Builds raw SOH-delimited wire text and parses it with <c>new Message(text, false)</c> —
    /// EXACTLY <see cref="FixBridgeApplication.OnLogon"/>'s own technique for its raw request lines (and
    /// <c>FixTestSupport.BuildMessage</c>'s, for tests), rather than constructing typed <c>Field</c>
    /// objects one by one. <c>validate: false</c> is correct here for the same reason it is correct there:
    /// <c>UseDataDictionary=N</c> is this platform's unconditional choice (plan 018), so there is no
    /// dictionary to validate against even if this call asked for it. Only tag 35 (MsgType) is written
    /// explicitly first; every other header field (BeginString, SenderCompID, TargetCompID, MsgSeqNum,
    /// SendingTime, the trailer) is stamped by QuickFIX/n's <c>Session</c> when it sends this message, not
    /// by this method.</para></summary>
    public static bool TryBuildMessage(Dictionary<string, object?> row, out Message message, out string? failureReason)
    {
        message = null!;

        if (!row.TryGetValue(MsgTypeColumn, out var msgTypeValue) || msgTypeValue is not string { Length: > 0 } msgType)
        {
            failureReason = $"row has no '{MsgTypeColumn}' column (tag 35, e.g. \"D\" for NewOrderSingle) — "
                + "cannot construct a FIX message without one";
            return false;
        }

        var sb = new StringBuilder();
        sb.Append("35=").Append(msgType).Append('\x01');

        foreach (var (key, value) in row)
        {
            if (string.Equals(key, MsgTypeColumn, StringComparison.Ordinal) || value is null
                || ReservedRowColumns.Contains(key))
            {
                continue; // MsgType already written above; a null value is "no value provided", not a
                          // failure; a reserved platform column (see ReservedRowColumns) is not a business
                          // field with an opinion about FIX tags.
            }

            if (!TagByName.TryGetValue(key, out var tag))
            {
                failureReason = $"row column '{key}' has no known outbound FIX tag mapping "
                    + "(see TRANSPORTS.md's FIX section, or add it once wave 019-F's dictionary lands)";
                return false;
            }

            if (!TryFormatValue(value, out var wireValue))
            {
                failureReason = $"row column '{key}' has a value of type {value.GetType().Name} this wave "
                    + "does not know how to put on the wire";
                return false;
            }

            sb.Append(tag).Append('=').Append(wireValue).Append('\x01');
        }

        message = new Message(sb.ToString(), false);
        failureReason = null;
        return true;
    }

    /// <summary>The row's <see cref="ClOrdIdColumn"/>, or null — see this class's own doc comment on that
    /// constant for why <see cref="ClOrdIdColumn"/> specifically.</summary>
    public static string? CorrelationIdOf(Dictionary<string, object?> row) =>
        row.TryGetValue(ClOrdIdColumn, out var v) && v is string { Length: > 0 } s ? s : null;

    /// <summary>The row's <see cref="MsgTypeColumn"/> (tag 35), or null — the exact same rule
    /// <see cref="TryBuildMessage"/> itself enforces before ever returning true. Plan 019 wave F: exists for
    /// a caller (<see cref="FixDuplexSession.SendAsync"/>) that needs the value AFTER
    /// <see cref="TryBuildMessage"/> has already succeeded (so MsgType is known present) and would
    /// otherwise have to re-implement this two-line extraction itself.</summary>
    public static string? MsgTypeOf(Dictionary<string, object?> row) =>
        row.TryGetValue(MsgTypeColumn, out var v) && v is string { Length: > 0 } s ? s : null;

    /// <summary>Tag number for a known outbound column NAME, or false when this table does not carry it.
    /// Plan 019 wave F: lets <see cref="FixRequiredFields"/> name a missing field's TAG NUMBER, not just
    /// its name, in a refusal message — by reading THIS table rather than typing the numbers out a third
    /// time. That is not the cross-assembly duplication this class's own doc comment argues for (this
    /// reader and <see cref="TagByName"/> are two classes in the SAME project); it is the ordinary
    /// single-source-of-truth choice within one project boundary.</summary>
    public static bool TryTagOf(string name, out int tag) => TagByName.TryGetValue(name, out tag);

    /// <summary>CLR value → FIX wire text. No FIX-type-aware formatting (that needs the dictionary wave
    /// 019-F adds) — just the handful of shapes a row realistically carries, matching
    /// <c>ConnectorRowCoercion</c>'s own output types. <c>bool</c> → "Y"/"N" mirrors <c>FixParser.TypedValue</c>'s
    /// reverse (tag 43/266/377 etc. are FIX's own Boolean type, exactly "Y"/"N").</summary>
    private static bool TryFormatValue(object value, out string wire)
    {
        switch (value)
        {
            case string s: wire = s; return true;
            case bool b: wire = b ? "Y" : "N"; return true;
            case long l: wire = l.ToString(CultureInfo.InvariantCulture); return true;
            case int i: wire = i.ToString(CultureInfo.InvariantCulture); return true;
            case double d: wire = d.ToString(CultureInfo.InvariantCulture); return true;
            case decimal m: wire = m.ToString(CultureInfo.InvariantCulture); return true;
            case DateTime dt: wire = dt.ToString("yyyyMMdd-HH:mm:ss.fff", CultureInfo.InvariantCulture); return true;
            case DateTimeOffset dto: wire = dto.UtcDateTime.ToString("yyyyMMdd-HH:mm:ss.fff", CultureInfo.InvariantCulture); return true;
            default: wire = ""; return false;
        }
    }
}
