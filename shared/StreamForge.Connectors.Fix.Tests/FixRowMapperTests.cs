using StreamForge.AppCore.Connectors.Formats;
using Xunit;

namespace StreamForge.Connectors.Fix.Tests;

/// <summary>Plan 019 wave E: row → FIX message. No socket, no session — <see cref="FixRowMapper"/> is a
/// pure function over a hand-built row, the same "fake seam (no socket)" plan 018-C used for
/// <see cref="FixBridgeApplication"/>.</summary>
public class FixRowMapperTests
{
    [Fact]
    public void MapsAKnownColumnNameToItsTag()
    {
        var row = new Dictionary<string, object?> { ["MsgType"] = "D", ["ClOrdID"] = "ORD1" };

        Assert.True(FixRowMapper.TryBuildMessage(row, out var message, out var failure));
        Assert.Null(failure);
        Assert.Equal("D", message.Header.GetString(35));
        Assert.Equal("ORD1", message.GetString(11));
    }

    [Fact]
    public void MapsSeveralKnownColumnsOfDifferentClrTypes()
    {
        var row = new Dictionary<string, object?>
        {
            ["MsgType"] = "D",
            ["ClOrdID"] = "ORD1",
            ["Symbol"] = "EUR/USD",
            ["Side"] = "1",
            ["OrderQty"] = 1000000L,
            ["Price"] = 1.2345,
            ["OrdType"] = "2",
        };

        Assert.True(FixRowMapper.TryBuildMessage(row, out var message, out var failure));
        Assert.Null(failure);
        Assert.Equal("EUR/USD", message.GetString(55));
        Assert.Equal("1", message.GetString(54));
        Assert.Equal("1000000", message.GetString(38));
        Assert.Equal("1.2345", message.GetString(44));
        Assert.Equal("2", message.GetString(40));
    }

    [Fact]
    public void FailsWhenTheMsgTypeColumnIsMissing()
    {
        var row = new Dictionary<string, object?> { ["ClOrdID"] = "ORD1" };

        Assert.False(FixRowMapper.TryBuildMessage(row, out _, out var failure));
        Assert.Contains("MsgType", failure);
    }

    [Fact]
    public void FailsWhenTheMsgTypeColumnIsBlank()
    {
        var row = new Dictionary<string, object?> { ["MsgType"] = "" };

        Assert.False(FixRowMapper.TryBuildMessage(row, out _, out var failure));
        Assert.Contains("MsgType", failure);
    }

    [Fact]
    public void FailsForARowColumnWithNoKnownOutboundTag()
    {
        var row = new Dictionary<string, object?> { ["MsgType"] = "D", ["NotARealFixField"] = "x" };

        Assert.False(FixRowMapper.TryBuildMessage(row, out _, out var failure));
        Assert.Contains("NotARealFixField", failure);
    }

    [Fact]
    public void ExcludesSessionHeaderFieldsFromTheOutboundTable()
    {
        // SenderCompID/TargetCompID/BeginString etc. are stamped by QuickFIX/n's own Session layer, never
        // by a row -- see FixRowMapper's own class doc comment for why they are deliberately absent.
        var row = new Dictionary<string, object?> { ["MsgType"] = "D", ["SenderCompID"] = "SNEAKY" };

        Assert.False(FixRowMapper.TryBuildMessage(row, out _, out var failure));
        Assert.Contains("SenderCompID", failure);
    }

    [Fact]
    public void SkipsNullValuedColumnsRatherThanFailing()
    {
        var row = new Dictionary<string, object?> { ["MsgType"] = "D", ["ClOrdID"] = "ORD1", ["Account"] = null };

        Assert.True(FixRowMapper.TryBuildMessage(row, out var message, out var failure));
        Assert.Null(failure);
        Assert.False(message.IsSetField(1));
    }

    [Fact]
    public void FormatsABooleanColumnAsFixYN()
    {
        var trueRow = new Dictionary<string, object?> { ["MsgType"] = "8", ["SolicitedFlag"] = true };
        var falseRow = new Dictionary<string, object?> { ["MsgType"] = "8", ["SolicitedFlag"] = false };

        Assert.True(FixRowMapper.TryBuildMessage(trueRow, out var trueMessage, out _));
        Assert.True(FixRowMapper.TryBuildMessage(falseRow, out var falseMessage, out _));

        Assert.Equal("Y", trueMessage.GetString(377));
        Assert.Equal("N", falseMessage.GetString(377));
    }

    // ------------------------------------------------------------------
    // CorrelationIdOf
    // ------------------------------------------------------------------

    [Fact]
    public void CorrelationIdOfReadsClOrdId()
    {
        var row = new Dictionary<string, object?> { ["ClOrdID"] = "ORD42" };
        Assert.Equal("ORD42", FixRowMapper.CorrelationIdOf(row));
    }

    [Fact]
    public void CorrelationIdOfIsNullWhenAbsent()
    {
        Assert.Null(FixRowMapper.CorrelationIdOf(new Dictionary<string, object?>()));
    }

    /// <summary>The one hazard this wave's design deliberately accepts, pinned so it cannot happen quietly.
    ///
    /// <para><see cref="FixRowMapper"/>'s name-to-tag table is typed out again rather than shared with
    /// <c>FixParser</c>'s tag-to-name table — for good reasons stated in that class's doc comment (it is a
    /// curated SUBSET, excluding the session header and trailer that QuickFIX/n stamps itself, so a
    /// mechanical reversal of the full table would be wrong). But two independently maintained tables can
    /// drift on a NAME, and the consequence is silent and asymmetric: a column an inbound FIX row was
    /// parsed INTO would stop being recognised on the way back OUT, and the row would fail to map with a
    /// reason that says nothing about why.</para>
    ///
    /// <para>Both tables are private, so they cannot be compared directly. They can be compared through
    /// their public behaviour, which is what actually matters anyway: map a row out, then parse the wire
    /// text back in with the very parser the inbound side uses, and require the column names to survive
    /// the round trip unchanged.</para></summary>
    [Fact]
    public void ColumnNamesSurviveARoundTripThroughTheInboundParser()
    {
        // A spread across every section of the mapper's table — order/execution, market data, and
        // parties/instrument — so a drift in any one of them fails here rather than in production.
        var row = new Dictionary<string, object?>
        {
            ["MsgType"] = "D",
            ["ClOrdID"] = "ORD-1",
            ["OrigClOrdID"] = "ORD-0",
            ["Symbol"] = "EUR/USD",
            ["Side"] = "1",
            ["OrderQty"] = "1000000",
            ["Price"] = "1.2345",
            ["OrdType"] = "2",
            ["TimeInForce"] = "0",
            ["Account"] = "ACC1",
            ["ExDestination"] = "XNAS",
            ["MDEntryType"] = "0",
            ["PartyID"] = "PARTY1",
            ["TradingSessionID"] = "SESS1",
        };

        Assert.True(FixRowMapper.TryBuildMessage(row, out var message, out var failure));
        Assert.Null(failure);

        // ConstructString() renders the message as it stands, WITHOUT the session header and trailer —
        // QuickFIX/n's Session stamps BeginString/BodyLength/SenderCompID/MsgSeqNum/SendingTime and the
        // checksum only when it actually sends. FixParser frames on 8=, so wrap the body in a minimal
        // synthetic envelope; the values are irrelevant to a dictionary-free structural parse, only the
        // framing is.
        var body = message.ConstructString();
        var wire = body.StartsWith("8=", StringComparison.Ordinal)
            ? body
            : "8=FIX.4.4\u00019=0\u0001" + body + (body.Contains("\u000110=", StringComparison.Ordinal) ? "" : "10=000\u0001");

        var parsed = FixParser.Parse(wire);
        Assert.True(parsed.Count == 1, $"expected one parsed message, got {parsed.Count}; wire was: {wire.Replace('\u0001', '|')}");
        var back = parsed[0];

        foreach (var column in row.Keys)
        {
            Assert.True(
                back.TryGetProperty(column, out _),
                $"column '{column}' maps OUT to a tag that the inbound parser names something else — the two "
                + "tag tables have drifted, and a round trip through this platform would silently lose it");
        }
    }
}
