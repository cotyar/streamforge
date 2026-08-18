using System.Globalization;
using StreamForge.AppCore.Connectors.Formats;
using Xunit;

namespace StreamForge.Connectors.Fix.Tests;

/// <summary>Plan 019, orchestrator follow-up to wave F: tag 60 must be on the wire for an order.
///
/// <para>Wave F excluded <c>TransactTime</c> from <see cref="FixRequiredFields"/>'s table, reasoning that
/// it is stamped at send time "like <c>SendingTime</c>". QuickFIX/n's <c>Session</c> stamps tag 52; it does
/// NOT stamp tag 60, and FIX 4.4 requires 60 on D/F/G — so the row was neither required to carry it nor
/// given one, and the message a venue received was invalid. These tests pin the fix, and pin the part that
/// matters more: the platform never overwrites a timestamp the caller supplied.</para></summary>
public class FixTransactTimeStampingTests
{
    private static Dictionary<string, object?> Order() => new(StringComparer.Ordinal)
    {
        ["MsgType"] = "D",
        ["ClOrdID"] = "ORD-1",
        ["Symbol"] = "EUR/USD",
        ["Side"] = "1",
        ["OrdType"] = "2",
        ["OrderQty"] = "1000000",
    };

    [Fact]
    public void AnOrderWithoutATransactTimeGetsOneOnTheWire()
    {
        var stamped = FixRowMapper.WithTransactTimeIfNeeded(Order());

        Assert.True(FixRowMapper.TryBuildMessage(stamped, out var message, out var failure));
        Assert.Null(failure);

        var value = message.GetString(60);
        Assert.True(
            DateTime.TryParseExact(value, "yyyyMMdd-HH:mm:ss.fff", CultureInfo.InvariantCulture, DateTimeStyles.None, out _),
            $"tag 60 was '{value}', which is not a FIX UTCTimestamp");
    }

    [Fact]
    public void ACallerSuppliedTransactTimeIsNeverOverwritten()
    {
        var row = Order();
        row["TransactTime"] = "20260101-09:30:00.000";

        var stamped = FixRowMapper.WithTransactTimeIfNeeded(row);

        Assert.Equal("20260101-09:30:00.000", stamped["TransactTime"]);
    }

    [Fact]
    public void TheCallersRowIsNotMutated()
    {
        var row = Order();

        FixRowMapper.WithTransactTimeIfNeeded(row);

        Assert.False(row.ContainsKey("TransactTime"));
    }

    [Theory]
    [InlineData("F")]
    [InlineData("G")]
    public void CancelAndReplaceAreStampedToo(string msgType)
    {
        var row = Order();
        row["MsgType"] = msgType;
        row["OrigClOrdID"] = "ORD-0";

        Assert.True(FixRowMapper.WithTransactTimeIfNeeded(row).ContainsKey("TransactTime"));
    }

    [Fact]
    public void AMessageTypeThatDoesNotRequireItIsLeftAlone()
    {
        var row = new Dictionary<string, object?>(StringComparer.Ordinal) { ["MsgType"] = "W", ["Symbol"] = "EUR/USD" };

        Assert.Same(row, FixRowMapper.WithTransactTimeIfNeeded(row));
    }

    [Fact]
    public void TheStampedColumnNameSurvivesTheInboundParser()
    {
        // The same hazard FixRowMapperTests' round-trip test guards: a stamped column whose name the
        // inbound parser does not agree with would vanish on the way back through this platform.
        var stamped = FixRowMapper.WithTransactTimeIfNeeded(Order());
        Assert.True(FixRowMapper.TryBuildMessage(stamped, out var message, out _));

        var wire = "8=FIX.4.4\u00019=0\u0001" + message.ConstructString() + "10=000\u0001";
        var parsed = FixParser.Parse(wire);

        Assert.True(parsed[0].TryGetProperty("TransactTime", out _));
    }
}
