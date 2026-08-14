using StreamForge.Abstractions;
using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

/// <summary>
/// Cursor round-trips. The interesting half is the timestamp kind, where the ONE bit that has to survive
/// is whether the value carried a zone: get it wrong and the bound parameter binds to the wrong column
/// type, the comparison shifts by the server's offset, and the source silently re-reads or skips hours of
/// rows depending on the sign.
/// </summary>
public class DbCursorTests
{
    [Fact]
    public void LongCursorsRoundTrip()
    {
        Assert.Equal("42", DbCursor.Encode(42L, CursorKinds.Long));
        Assert.Equal("42", DbCursor.Encode(42, CursorKinds.Long));
        Assert.Equal(42L, DbCursor.Decode("42", CursorKinds.Long));
        Assert.Equal(-1L, DbCursor.Decode(" -1 ", CursorKinds.Long));
    }

    [Fact]
    public void StringCursorsRoundTripUntouched()
    {
        Assert.Equal("0/1A2B3C", DbCursor.Encode("0/1A2B3C", CursorKinds.String));
        Assert.Equal("0/1A2B3C", DbCursor.Decode("0/1A2B3C", CursorKinds.String));
    }

    [Fact]
    public void AZonelessTimestampComesBackUnspecifiedBecauseThatIsTheOnlyThingAZonelessColumnAccepts()
    {
        var value = new DateTime(2026, 8, 14, 10, 30, 0, DateTimeKind.Unspecified);
        var encoded = DbCursor.Encode(value, CursorKinds.Timestamp);

        Assert.DoesNotContain("Z", encoded!, StringComparison.Ordinal);
        var decoded = Assert.IsType<DateTime>(DbCursor.Decode(encoded!, CursorKinds.Timestamp));
        Assert.Equal(DateTimeKind.Unspecified, decoded.Kind);
        Assert.Equal(value, decoded);
    }

    [Fact]
    public void AUtcTimestampComesBackUtc()
    {
        // What Npgsql hands back for a timestamptz column.
        var value = new DateTime(2026, 8, 14, 10, 30, 0, DateTimeKind.Utc);
        var encoded = DbCursor.Encode(value, CursorKinds.Timestamp);

        Assert.EndsWith("Z", encoded!, StringComparison.Ordinal);
        var decoded = Assert.IsType<DateTime>(DbCursor.Decode(encoded!, CursorKinds.Timestamp));
        Assert.Equal(DateTimeKind.Utc, decoded.Kind);
        Assert.Equal(value, decoded);
    }

    [Fact]
    public void AnOffsetTimestampComesBackAsADateTimeOffset()
    {
        // What SQL Server hands back for a datetimeoffset column.
        var value = new DateTimeOffset(2026, 8, 14, 10, 30, 0, TimeSpan.FromHours(2));
        var encoded = DbCursor.Encode(value, CursorKinds.Timestamp);

        var decoded = Assert.IsType<DateTimeOffset>(DbCursor.Decode(encoded!, CursorKinds.Timestamp));
        Assert.Equal(value, decoded);
        Assert.Equal(TimeSpan.FromHours(2), decoded.Offset);
    }

    [Fact]
    public void ADateOnlyValueDoesNotAcquireAZoneOnTheWayThrough()
    {
        var encoded = DbCursor.Encode(new DateOnly(2026, 8, 14), CursorKinds.Timestamp);

        Assert.Equal("2026-08-14", encoded);
        // The two hyphens in the date must not be mistaken for a negative UTC offset.
        var decoded = Assert.IsType<DateTime>(DbCursor.Decode(encoded!, CursorKinds.Timestamp));
        Assert.Equal(DateTimeKind.Unspecified, decoded.Kind);
    }

    [Fact]
    public void NullEncodesToNullWhichMeansLeaveThePersistedCursorAlone()
    {
        Assert.Null(DbCursor.Encode(null, CursorKinds.Long));
        Assert.Null(DbCursor.Encode(DBNull.Value, CursorKinds.Timestamp));
    }

    [Fact]
    public void AnUnparseableCursorThrowsRatherThanDefaulting()
    {
        // Defaulting to zero re-reads the whole table; defaulting to "now" skips it. Failing the cycle
        // leaves the persisted cursor untouched and puts the reason on the source's status.
        var ex = Assert.Throws<FormatException>(() => DbCursor.Decode("not-a-number", CursorKinds.Long));
        Assert.Contains("not-a-number", ex.Message, StringComparison.Ordinal);

        Assert.Throws<FormatException>(() => DbCursor.Decode("yesterday", CursorKinds.Timestamp));
    }

    [Fact]
    public void ProblemIsTheValidationTimeFormOfTheSameCheck()
    {
        Assert.Null(DbCursor.Problem("", CursorKinds.Long));
        Assert.Null(DbCursor.Problem("17", CursorKinds.Long));
        Assert.NotNull(DbCursor.Problem("seventeen", CursorKinds.Long));
        Assert.NotNull(DbCursor.Problem("nope", CursorKinds.Timestamp));
    }

    [Fact]
    public void TheQueryPlaceholderIsTheTokenAnOperatorsSqlMustContain()
    {
        Assert.Equal("@cursor", DbCursor.Placeholder);
        Assert.Equal("cursor", DbCursor.ParameterName);
    }
}
