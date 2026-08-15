using StreamForge.Engine.Runtime;
using Xunit;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 009 Round C wave C1 — direct unit coverage of <see cref="FieldValueConversion"/>, the public,
/// <see cref="FieldKind"/>-keyed canonical conversion behind the SQL dialect's TO_LONG/TO_DOUBLE/
/// TO_BOOL/TO_TIMESTAMP/TO_STRING functions (see <see cref="TypeConversionFunctionsTests"/> for the
/// SQL-level behavior) and the intended future delegation target for
/// StreamForge.AppCore.Ingest.FieldValueCoercion.TryCoerce (FieldType-keyed) and
/// StreamForge.AppCore.Ingest.RowTimestamp.Resolve — see this type's own doc comment for exactly which
/// existing behaviors are pinned on purpose (including two documented "Findings": the permissive
/// bool-from-string rule, and unchecked double-to-long overflow).
/// </summary>
public class FieldValueConversionTests
{
    // ------------------------------------------------------------------
    // TryCoerce(FieldKind.String, …) — always succeeds
    // ------------------------------------------------------------------

    [Fact]
    public void String_passes_a_string_through_unchanged()
    {
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.String, "hello", out var coerced));
        Assert.Equal("hello", coerced);
    }

    [Theory]
    [InlineData(42L, "42")]
    [InlineData(3.5, "3.5")]
    public void String_coerces_a_number_via_invariant_ToString(object input, string expected)
    {
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.String, input, out var coerced));
        Assert.Equal(expected, coerced);
    }

    [Theory]
    [InlineData(true, "true")]
    [InlineData(false, "false")]
    public void String_coerces_a_bool_as_lowercase_literal(bool input, string expected)
    {
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.String, input, out var coerced));
        Assert.Equal(expected, coerced);
    }

    // ------------------------------------------------------------------
    // TryCoerce(FieldKind.Double, …)
    // ------------------------------------------------------------------

    /// <summary>The other half of that finding: a date/time column declared as a Timestamp field NULLed
    /// out the same way, because Timestamp shared Long's purely-numeric rule. Utc and Local carry their
    /// own offset and convert exactly; Unspecified is read as UTC, the same rule the string paths below
    /// already apply via DateTimeStyles.AssumeUniversal.</summary>
    [Fact]
    public void Timestamp_accepts_clr_date_times_on_every_DateTimeKind()
    {
        var utc = new DateTime(2026, 8, 15, 12, 0, 0, DateTimeKind.Utc);
        long expected = 1_786_795_200_000; // 2026-08-15T12:00:00Z
        Assert.Equal(expected, new DateTimeOffset(utc).ToUnixTimeMilliseconds()); // pins the constant itself

        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Timestamp, utc, out var fromUtc));
        Assert.Equal(expected, fromUtc);

        // Unspecified — a Postgres `timestamp without time zone` — reads as the same instant, NOT as
        // whatever the host process's timezone would make of it.
        var unspecified = DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Timestamp, unspecified, out var fromUnspecified));
        Assert.Equal(expected, fromUnspecified);

        // Local carries a known offset, so it converts to the instant it actually denotes.
        var local = utc.ToLocalTime();
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Timestamp, local, out var fromLocal));
        Assert.Equal(expected, fromLocal);

        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Timestamp, new DateTimeOffset(utc), out var fromOffset));
        Assert.Equal(expected, fromOffset);

        // An offset that is NOT zero must be honoured, not dropped.
        var plusTwo = new DateTimeOffset(2026, 8, 15, 14, 0, 0, TimeSpan.FromHours(2));
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Timestamp, plusTwo, out var fromPlusTwo));
        Assert.Equal(expected, fromPlusTwo);
    }

    /// <summary>The same three rules, reached through the two other timestamp entry points, so they
    /// cannot drift apart from TryCoerce's.</summary>
    [Fact]
    public void The_other_two_timestamp_entry_points_agree_with_TryCoerce()
    {
        var utc = new DateTime(2026, 8, 15, 12, 0, 0, DateTimeKind.Utc);
        long expected = 1_786_795_200_000;

        Assert.True(FieldValueConversion.TryToTimestamp(utc, out var fnUtc));
        Assert.Equal(expected, fnUtc);
        Assert.True(FieldValueConversion.TryToTimestamp(new DateTimeOffset(utc), out var fnOffset));
        Assert.Equal(expected, fnOffset);
        Assert.True(FieldValueConversion.TryToTimestamp(DateTime.SpecifyKind(utc, DateTimeKind.Unspecified), out var fnUnspec));
        Assert.Equal(expected, fnUnspec);

        Assert.Equal(expected, FieldValueConversion.ResolveTimestamp(utc, fallbackMs: 1));
        Assert.Equal(expected, FieldValueConversion.ResolveTimestamp(new DateTimeOffset(utc), fallbackMs: 1));
        Assert.Equal(expected, FieldValueConversion.ResolveTimestamp(DateTime.SpecifyKind(utc, DateTimeKind.Unspecified), fallbackMs: 1));
    }

    /// <summary>A date/time is meaningful for Timestamp only. Long stays "a number" — widening it would
    /// silently turn a mis-declared column into an epoch integer instead of the visible NULL that tells
    /// the operator the declaration is wrong.</summary>
    [Fact]
    public void Long_does_not_silently_accept_a_date_time()
    {
        var utc = new DateTime(2026, 8, 15, 12, 0, 0, DateTimeKind.Utc);
        Assert.False(FieldValueConversion.TryCoerce(FieldKind.Long, utc, out var coerced));
        Assert.Null(coerced);
    }

    /// <summary>The OTC-demo finding: a Postgres `numeric` column arrives from the CDC path as CLR
    /// `decimal` (PgCdcSource.Cell passes it through), and before this arm every money column declared
    /// Double coerced to NULL — silently, since coercion failure is not an error. `short`/`byte`/
    /// unsigned reach the same place from smallint/tinyint. Purely additive: each of these was a
    /// coercion FAILURE before, so nothing that used to produce a value produces a different one.</summary>
    [Theory]
    [InlineData(FieldKind.Double)]
    [InlineData(FieldKind.Long)]
    public void Driver_numerics_beyond_long_and_double_coerce_rather_than_null(FieldKind kind)
    {
        object[] inputs = [12.34m, (short)7, (ushort)7, (byte)7, (sbyte)7, 7u, 7ul];
        foreach (var input in inputs)
        {
            Assert.True(FieldValueConversion.TryCoerce(kind, input, out var coerced), $"{input.GetType().Name} failed to coerce to {kind}");
            Assert.NotNull(coerced);
        }
    }

    [Fact]
    public void Decimal_coerces_to_double_by_value_and_to_long_by_truncation()
    {
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Double, 12.34m, out var asDouble));
        Assert.Equal(12.34d, Assert.IsType<double>(asDouble), 10);

        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Long, 12.9m, out var asLong));
        Assert.Equal(12L, asLong); // toward zero, matching the existing double-to-long arm

        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Bool, 0m, out var zero));
        Assert.Equal(false, zero);
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Bool, 0.5m, out var nonZero));
        Assert.Equal(true, nonZero);
    }

    [Theory]
    [InlineData(3.5)]
    [InlineData(3L)]
    [InlineData(true)]
    public void Double_coercion_accepts_numeric_and_bool_types(object input)
    {
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Double, input, out var coerced));
        Assert.IsType<double>(coerced);
    }

    [Fact]
    public void Double_coercion_accepts_a_numeric_string()
    {
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Double, "3.5", out var coerced));
        Assert.Equal(3.5, coerced);
    }

    [Fact]
    public void Double_coercion_rejects_a_non_numeric_string()
    {
        Assert.False(FieldValueConversion.TryCoerce(FieldKind.Double, "not-a-number", out var coerced));
        Assert.Null(coerced);
    }

    [Fact]
    public void Double_coercion_rejects_an_empty_string()
    {
        Assert.False(FieldValueConversion.TryCoerce(FieldKind.Double, "", out var coerced));
        Assert.Null(coerced);
    }

    [Fact]
    public void Double_coercion_rejects_an_unsupported_type()
    {
        Assert.False(FieldValueConversion.TryCoerce(FieldKind.Double, new object(), out var coerced));
        Assert.Null(coerced);
    }

    // ------------------------------------------------------------------
    // TryCoerce(FieldKind.Long / .Timestamp, …) — deliberately identical rule (see class doc)
    // ------------------------------------------------------------------

    [Theory]
    [InlineData(FieldKind.Long)]
    [InlineData(FieldKind.Timestamp)]
    public void Long_and_Timestamp_share_identical_coercion(FieldKind kind)
    {
        Assert.True(FieldValueConversion.TryCoerce(kind, "42", out var fromString));
        Assert.Equal(42L, fromString);

        Assert.True(FieldValueConversion.TryCoerce(kind, 3.9, out var fromDouble));
        Assert.Equal(3L, fromDouble); // truncates, does not round

        Assert.True(FieldValueConversion.TryCoerce(kind, true, out var fromBool));
        Assert.Equal(1L, fromBool);

        Assert.False(FieldValueConversion.TryCoerce(kind, "not-a-number", out _));
        Assert.False(FieldValueConversion.TryCoerce(kind, new object(), out _));
    }

    [Fact]
    public void Long_coercion_rejects_an_overflowing_numeric_string()
    {
        // long.TryParse fails outright for a string outside the long range — the one overflow path
        // that DOES come back NULL (see the class doc's "Finding" about the double-direct path, which
        // does not, on purpose, to match FieldValueCoercion.TryToInt64's existing unchecked cast).
        Assert.False(FieldValueConversion.TryCoerce(FieldKind.Long, "99999999999999999999", out var coerced));
        Assert.Null(coerced);
    }

    // ------------------------------------------------------------------
    // TryCoerce(FieldKind.Bool, …) — pinned to FieldValueCoercion.TryToBool's existing, permissive rule
    // ------------------------------------------------------------------

    [Theory]
    [InlineData("true", true)]
    [InlineData("TRUE", true)]
    [InlineData("false", false)]
    [InlineData("FALSE", false)]
    [InlineData("", false)]
    [InlineData("0", false)]
    [InlineData("anything-else", true)] // documented Finding: not a fixed spelling list, see class doc
    public void Bool_coercion_from_string(string input, bool expected)
    {
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Bool, input, out var coerced));
        Assert.Equal(expected, coerced);
    }

    [Theory]
    [InlineData(1L, true)]
    [InlineData(0L, false)]
    [InlineData(2.5, true)] // nonzero-is-true, not "must equal 1" — matches FieldValueCoercion
    [InlineData(0.0, false)]
    public void Bool_coercion_from_number(object input, bool expected)
    {
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Bool, input, out var coerced));
        Assert.Equal(expected, coerced);
    }

    [Fact]
    public void Bool_coercion_rejects_an_unsupported_type()
    {
        Assert.False(FieldValueConversion.TryCoerce(FieldKind.Bool, new object(), out _));
    }

    // ------------------------------------------------------------------
    // TryCoerce(FieldKind.Json, …) — structural passthrough, never fails
    // ------------------------------------------------------------------

    [Fact]
    public void Json_coercion_always_passes_the_value_through_unchanged()
    {
        var dict = new Dictionary<string, object?> { ["a"] = 1L };
        Assert.True(FieldValueConversion.TryCoerce(FieldKind.Json, dict, out var coerced));
        Assert.Same(dict, coerced);
    }

    // ------------------------------------------------------------------
    // TryToTimestamp — the TO_TIMESTAMP SQL function's own, wider rule
    // ------------------------------------------------------------------

    [Fact]
    public void TryToTimestamp_accepts_epoch_ms_as_a_number()
    {
        Assert.True(FieldValueConversion.TryToTimestamp(1_700_000_000_000L, out var coerced));
        Assert.Equal(1_700_000_000_000L, coerced);
    }

    [Fact]
    public void TryToTimestamp_accepts_epoch_ms_as_a_numeric_string()
    {
        // The genuine superset over both TryCoerce(Timestamp, …) (no ISO text) and ResolveTimestamp
        // (no numeric-string) — see class doc.
        Assert.True(FieldValueConversion.TryToTimestamp("1700000000000", out var coerced));
        Assert.Equal(1_700_000_000_000L, coerced);
    }

    [Fact]
    public void TryToTimestamp_accepts_iso8601_text()
    {
        Assert.True(FieldValueConversion.TryToTimestamp("2023-11-14T22:13:20Z", out var coerced));
        Assert.Equal(1_700_000_000_000L, coerced);
    }

    [Theory]
    [InlineData("not-a-timestamp")]
    [InlineData("")]
    public void TryToTimestamp_rejects_unparseable_text(string input)
    {
        Assert.False(FieldValueConversion.TryToTimestamp(input, out var coerced));
        Assert.Null(coerced);
    }

    [Fact]
    public void TryToTimestamp_rejects_bool_and_unsupported_types()
    {
        Assert.False(FieldValueConversion.TryToTimestamp(true, out _));
        Assert.False(FieldValueConversion.TryToTimestamp(new object(), out _));
    }

    // ------------------------------------------------------------------
    // ResolveTimestamp — exact port of AppCore's RowTimestamp.Resolve (fallback-shaped, not NULL-shaped)
    // ------------------------------------------------------------------

    [Fact]
    public void ResolveTimestamp_returns_a_long_value_as_is()
    {
        Assert.Equal(1234L, FieldValueConversion.ResolveTimestamp(1234L, fallbackMs: 999));
    }

    [Fact]
    public void ResolveTimestamp_truncates_a_double()
    {
        Assert.Equal(3L, FieldValueConversion.ResolveTimestamp(3.9, fallbackMs: 999));
    }

    [Fact]
    public void ResolveTimestamp_parses_iso8601_text()
    {
        Assert.Equal(1_700_000_000_000L, FieldValueConversion.ResolveTimestamp("2023-11-14T22:13:20Z", fallbackMs: 999));
    }

    [Fact]
    public void ResolveTimestamp_falls_back_on_a_numeric_string_unlike_TryToTimestamp()
    {
        // The one deliberate behavioral difference from TryToTimestamp: ResolveTimestamp has no
        // numeric-string support (matching RowTimestamp.Resolve exactly), so a numeric string falls
        // back rather than parsing as epoch-ms.
        Assert.Equal(999L, FieldValueConversion.ResolveTimestamp("1700000000000", fallbackMs: 999));
    }

    [Fact]
    public void ResolveTimestamp_falls_back_on_unparseable_or_missing_input()
    {
        Assert.Equal(999L, FieldValueConversion.ResolveTimestamp("garbage", fallbackMs: 999));
        Assert.Equal(999L, FieldValueConversion.ResolveTimestamp(null, fallbackMs: 999));
    }
}
