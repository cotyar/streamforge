namespace StreamsForge.Connectors.Fix;

/// <summary>
/// Plan 019 wave F (D6): outbound required-field validation without a real FIX dictionary.
///
/// <para><b>Investigated first, as the brief asked, and came back empty-handed.</b> <c>QuickFIXn.Core</c>
/// 1.14.1 (the only FIX package this repository carries — see this project's own <c>.csproj</c> comment)
/// ships exactly two files per target framework: <c>QuickFix.dll</c> and its <c>.xml</c> API-doc-comment
/// sidecar — checked directly against the NuGet cache
/// (<c>~/.nuget/packages/quickfixn.core/1.14.1/lib/net10.0/</c>), not assumed. <c>QuickFix.dll</c> DOES
/// contain a <c>QuickFix.DataDictionary</c> class capable of PARSING a FIX spec file (<c>FIX44.xml</c> and
/// siblings) — but no such file ships IN the package: <c>Assembly.GetManifestResourceNames()</c> against
/// the loaded assembly returns an empty array, confirming the dictionary XML is not an embedded resource
/// either. The spec files live in the separate quickfixengine.org / QuickFIXn source tree's own
/// <c>spec/</c> folder, one XML per FIX version, each hundreds of KB — exactly the kind of large data
/// file plan 018 already refused to vendor for the inbound side, and this wave's brief refuses again for
/// the outbound side. <b>So: no dictionary, by the same honest ceiling.</b> This class is the curated
/// substitute — the inbound-side equivalent is <c>FixParser.TagNames</c>' <c>tag&lt;N&gt;</c> fallback,
/// same idea, same size class.</para>
///
/// <para>Covers only the three MsgTypes plan 019 actually deals in — <see cref="NewOrderSingle"/> (D),
/// <see cref="OrderCancelRequest"/> (F), <see cref="OrderCancelReplaceRequest"/> (G) — not a
/// general-purpose dictionary. A MsgType with no entry here is not gated at all (see
/// <see cref="TryValidate"/>): this table only ever ADDS a refusal for the three types it knows, never a
/// new way for something else to fail.</para>
///
/// <para><b>The required set per message is deliberately the intersection that holds across every
/// <c>BeginString</c> this platform recognizes</b> (FIX.4.0 through FIX.4.4/FIXT.1.1 —
/// <see cref="FixDuplexTransport"/>'s single <c>KnownBeginStrings</c> list, not a per-version table), and
/// deliberately conservative: a field required in SOME versions but not others (<c>HandlInst</c>,
/// mandatory in FIX.4.2, optional from FIX.4.4 on) is left OUT rather than incorrectly rejected on a
/// version where the venue does not require it. <c>TransactTime</c> (tag 60) is ALSO left out, on
/// different grounds: in most real gateways it is a value the SENDER stamps at the moment of transmission
/// — the same role <c>SendingTime</c> already plays in the session header, which QuickFIX/n's own
/// <c>Session</c> layer stamps unconditionally rather than requiring of the row (see
/// <see cref="FixRowMapper"/>'s own class doc on why session fields are excluded from its table
/// entirely). Gating a send on the ROW supplying a timestamp the platform could just as easily stamp
/// itself is the wrong ceiling to enforce before that auto-stamp exists; it is a natural candidate for a
/// later wave, not a reason to refuse a send today.</para>
///
/// <para><b>Upgrade path</b>: a real per-<c>BeginString</c> dictionary — the same upgrade path
/// <c>FixParser</c>'s own doc comment names for the inbound ceilings. ponytail: extend this table only
/// when a real venue integration needs a field it does not carry, rather than guessing ahead of a need
/// this wave has no evidence for.</para>
/// </summary>
public static class FixRequiredFields
{
    public const string NewOrderSingle = "D";
    public const string OrderCancelRequest = "F";
    public const string OrderCancelReplaceRequest = "G";

    /// <summary>Field NAMES, matching <see cref="FixRowMapper"/>'s row-column vocabulary exactly (the same
    /// table <see cref="FixRowMapper.TryTagOf"/> reads to attach a tag number to a refusal message).</summary>
    private static readonly Dictionary<string, string[]> RequiredByMsgType = new(StringComparer.Ordinal)
    {
        // Identity (ClOrdID), what to trade (Symbol/Side), and how (OrdType/OrderQty) — the fields a
        // NewOrderSingle cannot be reconstructed without. See this class's doc comment for why
        // TransactTime is deliberately not here.
        [NewOrderSingle] = ["ClOrdID", "Symbol", "Side", "OrdType", "OrderQty"],

        // A cancel identifies itself (ClOrdID) AND the order it cancels (OrigClOrdID) — see plan 019 D7:
        // "the minimum honest thing is to carry and validate the chain, not to model it."
        [OrderCancelRequest] = ["OrigClOrdID", "ClOrdID", "Symbol", "Side"],

        // A replace carries everything a cancel does PLUS the new order terms it is replacing the old
        // ones with.
        [OrderCancelReplaceRequest] = ["OrigClOrdID", "ClOrdID", "Symbol", "Side", "OrdType", "OrderQty"],
    };

    /// <summary>True, with <paramref name="failureReason"/> null, when every required field for
    /// <paramref name="msgType"/> is present and non-empty on <paramref name="row"/> — or when
    /// <paramref name="msgType"/> has no curated entry at all (this table does not gate a MsgType it does
    /// not know about; see this class's own doc comment). False with a reason naming BOTH the missing
    /// field and the MsgType otherwise — see <see cref="FixDuplexSession.SendAsync"/>'s own doc comment
    /// for why a reason an operator cannot act on is barely better than the venue's own reject.</summary>
    public static bool TryValidate(string msgType, Dictionary<string, object?> row, out string? failureReason)
    {
        if (!RequiredByMsgType.TryGetValue(msgType, out var required))
        {
            failureReason = null;
            return true;
        }

        foreach (var name in required)
        {
            // Present means non-null and, if it happens to be a string, non-empty — matching
            // FixRowMapper.TryBuildMessage's own "a null value is no value provided" rule. Any other CLR
            // shape (long/int/double/decimal/bool/DateTime, see FixRowMapper.TryFormatValue) counts as
            // present outright: OrderQty=1000000L or Price=1.2345 are ordinary, valid values, not absent
            // ones, and this check must not require a string where the row legitimately carries a number.
            if (row.TryGetValue(name, out var value) && value is not null && value is not string { Length: 0 })
            {
                continue;
            }

            var tagSuffix = FixRowMapper.TryTagOf(name, out var tag) ? $" (tag {tag})" : "";
            failureReason = $"MsgType '{msgType}' is missing required field '{name}'{tagSuffix}";
            return false;
        }

        failureReason = null;
        return true;
    }
}
