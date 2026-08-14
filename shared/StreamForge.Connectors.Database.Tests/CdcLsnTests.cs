using NpgsqlTypes;
using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

/// <summary>
/// LSN round-trips for both dialects. The interesting half is MSSQL: a <c>binary(10)</c> LSN is wider
/// than a <see cref="ulong"/>, so any comparison that parses it as a number before comparing silently
/// truncates the high bytes and gets the order wrong exactly at a byte-boundary carry — the case these
/// tests exist to pin down.
/// </summary>
public class CdcLsnTests
{
    [Fact]
    public void PgLsnRoundTripsThroughItsCanonicalTextForm()
    {
        var lsn = NpgsqlLogSequenceNumber.Parse("0/16B3748");
        var encoded = CdcLsn.EncodePg(lsn);

        Assert.Equal("0/16B3748", encoded);
        Assert.Equal(lsn, CdcLsn.DecodePg(encoded));
    }

    [Fact]
    public void PgLsnZeroRoundTrips()
    {
        var encoded = CdcLsn.EncodePg(NpgsqlLogSequenceNumber.Parse("0/0"));
        Assert.Equal("0/0", encoded);
        Assert.Equal(NpgsqlLogSequenceNumber.Parse("0/0"), CdcLsn.DecodePg(encoded));
    }

    [Fact]
    public void PgLsnHighValueRoundTrips()
    {
        var high = (NpgsqlLogSequenceNumber)ulong.MaxValue;
        var encoded = CdcLsn.EncodePg(high);

        Assert.Equal(high, CdcLsn.DecodePg(encoded));
    }

    [Fact]
    public void PgLsnMalformedTextThrowsWithTheOffendingTextInTheMessage()
    {
        var ex = Assert.Throws<FormatException>(() => CdcLsn.DecodePg("not-an-lsn"));
        Assert.Contains("not-an-lsn", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void PgLsnMissingSlashThrows()
    {
        var ex = Assert.Throws<FormatException>(() => CdcLsn.DecodePg("16B3748"));
        Assert.Contains("16B3748", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void PgLsnEmptyStringThrows()
    {
        Assert.Throws<FormatException>(() => CdcLsn.DecodePg(""));
    }

    [Fact]
    public void MsSqlLsnAllZeroRoundTrips()
    {
        var zero = new byte[10];
        var encoded = CdcLsn.EncodeMsSql(zero);

        Assert.Equal("00000000000000000000", encoded);
        Assert.Equal(20, encoded.Length);
        Assert.Equal(zero, CdcLsn.DecodeMsSql(encoded));
    }

    [Fact]
    public void MsSqlLsnAllFfRoundTrips()
    {
        var allFf = Enumerable.Repeat((byte)0xff, 10).ToArray();
        var encoded = CdcLsn.EncodeMsSql(allFf);

        Assert.Equal("ffffffffffffffffffff", encoded);
        Assert.Equal(allFf, CdcLsn.DecodeMsSql(encoded));
    }

    [Fact]
    public void MsSqlLsnEncodeIsLowercaseNoPrefix()
    {
        byte[] lsn = [0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x03, 0x90, 0x00, 0x01];
        var encoded = CdcLsn.EncodeMsSql(lsn);

        Assert.Equal("00000025000003900001", encoded);
        Assert.DoesNotContain("0x", encoded, StringComparison.Ordinal);
        Assert.Equal(encoded, encoded.ToLowerInvariant(), StringComparer.Ordinal);
    }

    [Fact]
    public void MsSqlLsnWrongLengthByteArrayThrowsOnEncode()
    {
        var ex = Assert.Throws<FormatException>(() => CdcLsn.EncodeMsSql(new byte[9]));
        Assert.Contains("9", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MsSqlLsnWrongLengthTextThrowsOnDecode()
    {
        var tooShort = new string('a', 19);
        var ex = Assert.Throws<FormatException>(() => CdcLsn.DecodeMsSql(tooShort));
        Assert.Contains(tooShort, ex.Message, StringComparison.Ordinal);

        var tooLong = new string('a', 21);
        Assert.Throws<FormatException>(() => CdcLsn.DecodeMsSql(tooLong));
    }

    [Fact]
    public void MsSqlLsnNonHexCharactersThrowOnDecode()
    {
        var badText = "0000000000000000000g";
        var notHex = badText[..20];
        var ex = Assert.Throws<FormatException>(() => CdcLsn.DecodeMsSql(notHex));
        Assert.Contains(notHex, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MsSqlLsnUppercaseHexIsRejectedOnDecode()
    {
        // Encode always produces lowercase; decode is strict about it rather than silently accepting
        // a form this codec never itself writes. Needs an actual hex LETTER to exercise the case
        // check — an all-digit LSN is unchanged by ToUpperInvariant and would pass either way.
        var lower = CdcLsn.EncodeMsSql([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff]);
        Assert.Equal("000000000000000000ff", lower);

        var upper = lower.ToUpperInvariant();
        Assert.Throws<FormatException>(() => CdcLsn.DecodeMsSql(upper));
    }

    [Fact]
    public void MsSqlLsnEmptyStringThrowsOnDecode()
    {
        var ex = Assert.Throws<FormatException>(() => CdcLsn.DecodeMsSql(""));
        Assert.NotNull(ex.Message);
    }

    [Fact]
    public void MsSqlCompareOrdersEqualLsnsAsEqual()
    {
        var a = CdcLsn.EncodeMsSql(new byte[10]);
        var b = CdcLsn.EncodeMsSql(new byte[10]);

        Assert.Equal(0, CdcLsn.CompareMsSql(a, b));
    }

    [Fact]
    public void MsSqlCompareOrdersCorrectlyAcrossAByteBoundaryCarry()
    {
        // …00 ff… vs …01 00… — numerically 0x00ff (255) < 0x0100 (256) at that byte pair, but a
        // comparison that parsed the LSN as a 64-bit integer would already have lost the two
        // highest-order bytes (10 bytes is 80 bits) and could get this backwards.
        var lower = CdcLsn.EncodeMsSql([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff]);
        var higher = CdcLsn.EncodeMsSql([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00]);

        Assert.True(CdcLsn.CompareMsSql(lower, higher) < 0);
        Assert.True(CdcLsn.CompareMsSql(higher, lower) > 0);
    }

    [Fact]
    public void MsSqlCompareOrdersCorrectlyAcrossTheHighestByteBoundary()
    {
        // The carry that a naive ulong-based comparison would drop entirely: byte 0 (the
        // most-significant byte of a 10-byte value) differs, everything else is identical.
        var lower = CdcLsn.EncodeMsSql([0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        var higher = CdcLsn.EncodeMsSql([0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

        Assert.True(CdcLsn.CompareMsSql(lower, higher) < 0);
        Assert.True(CdcLsn.CompareMsSql(higher, lower) > 0);
    }

    [Fact]
    public void MsSqlCompareThrowsOnMalformedInputRatherThanGuessingAnOrder()
    {
        var valid = CdcLsn.EncodeMsSql(new byte[10]);
        Assert.Throws<FormatException>(() => CdcLsn.CompareMsSql("not-an-lsn", valid));
        Assert.Throws<FormatException>(() => CdcLsn.CompareMsSql(valid, "not-an-lsn"));
    }
}
