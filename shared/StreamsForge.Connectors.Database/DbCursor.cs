using System.Globalization;
using StreamsForge.Abstractions;

namespace StreamsForge.Connectors.Database;

/// <summary>
/// The codec between the driver's opaque persisted <c>string?</c> cursor and the CLR value bound as
/// <c>@cursor</c>. Three kinds, because those are the three things a monotonic column is in practice
/// (see <see cref="CursorKinds"/>), and because the platform cannot infer which one a persisted
/// <c>"1700000000"</c> was — an epoch second and a surrogate key are the same eleven characters.
///
/// <para><b>Why the round-trip has to be exact.</b> <c>@cursor</c> is a bound parameter (never
/// interpolated), so the driver types it from the CLR value handed over. Decode a
/// <c>timestamp without time zone</c> watermark into a UTC <see cref="DateTime"/> and Npgsql will bind it
/// as <c>timestamptz</c>; the comparison then silently shifts by the server's zone offset and the source
/// either re-reads or skips hours of rows depending on the sign. So the timestamp encoding preserves
/// exactly one bit — whether the value carried a zone — and the decode reads it back:</para>
/// <list type="bullet">
/// <item>trailing <c>Z</c> → a UTC <see cref="DateTime"/> (what Npgsql hands back for <c>timestamptz</c>).</item>
/// <item>an explicit <c>±hh:mm</c> → a <see cref="DateTimeOffset"/> (what SQL Server's
/// <c>datetimeoffset</c> hands back).</item>
/// <item>neither → an <b>Unspecified</b> <see cref="DateTime"/>, the only thing Npgsql will bind to a
/// zoneless <c>timestamp</c> column.</item>
/// </list>
///
/// <para>A cursor that will not parse for its declared kind throws rather than being coerced to a
/// default. Silently starting from zero would re-read an entire table; silently starting from "now"
/// would skip one. Failing the cycle leaves the persisted cursor untouched
/// (<c>PolledSourceCore</c>'s rule) and puts the reason on the source's status, which is the only one of
/// the three an operator can act on.</para>
/// </summary>
public static class DbCursor
{
    /// <summary>The bound parameter's name. Fixed, and part of the contract with a custom
    /// <see cref="DbSourceConfig.Query"/> — which must contain this token literally.</summary>
    public const string ParameterName = "cursor";

    /// <summary>The token an operator's <see cref="DbSourceConfig.Query"/> must contain.</summary>
    public const string Placeholder = "@" + ParameterName;

    /// <summary>Turns a value read out of the cursor column into the opaque string the driver persists.
    /// Null in, null out — which <c>PolledBatch.Cursor</c> reads as "leave the persisted cursor exactly as
    /// it was", never as "reset".</summary>
    public static string? Encode(object? value, string cursorKind)
    {
        if (value is null or DBNull)
        {
            return null;
        }

        return Kind(cursorKind) switch
        {
            CursorKinds.Long => Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
            CursorKinds.Timestamp => EncodeTimestamp(value),
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "",
        };
    }

    /// <summary>Turns the persisted opaque cursor back into the CLR value bound as <c>@cursor</c>.
    /// Throws <see cref="FormatException"/> with the kind and the offending text when they disagree —
    /// see this class's doc for why that is preferable to a default.</summary>
    public static object Decode(string cursor, string cursorKind)
    {
        var kind = Kind(cursorKind);
        var text = cursor.Trim();
        try
        {
            return kind switch
            {
                CursorKinds.Long => long.Parse(text, CultureInfo.InvariantCulture),
                CursorKinds.Timestamp => DecodeTimestamp(text),
                _ => text,
            };
        }
        catch (Exception ex) when (ex is FormatException or OverflowException or ArgumentException)
        {
            throw new FormatException($"cursor '{cursor}' is not a valid '{kind}' cursor: {ex.Message}", ex);
        }
    }

    /// <summary>Validation-time check for <see cref="DbSourceConfig.InitialCursor"/>: does this text parse
    /// as this kind? Returns the message to show, or null when it is fine.</summary>
    public static string? Problem(string text, string cursorKind)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        try
        {
            Decode(text, cursorKind);
            return null;
        }
        catch (FormatException ex)
        {
            return ex.Message;
        }
    }

    /// <summary>Unknown kinds fall back to <see cref="CursorKinds.String"/> rather than throwing: a
    /// mistyped kind on an imported config should still poll (ordinally, which is at least monotonic for
    /// zero-padded ids) while validation says so, rather than taking the source down.</summary>
    private static string Kind(string cursorKind) => cursorKind switch
    {
        CursorKinds.Long => CursorKinds.Long,
        CursorKinds.Timestamp => CursorKinds.Timestamp,
        _ => CursorKinds.String,
    };

    private static string EncodeTimestamp(object value) => value switch
    {
        DateTimeOffset dto => dto.ToString("O", CultureInfo.InvariantCulture),
        DateTime dt => dt.ToString("O", CultureInfo.InvariantCulture),
        DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        // A string already in the column (or an InitialCursor the operator typed) is passed through
        // unchanged rather than re-formatted — re-formatting is where a zone gets invented.
        string s => s,
        _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "",
    };

    private static object DecodeTimestamp(string text)
    {
        if (text.EndsWith('Z'))
        {
            return DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        }

        // NOT a conditional expression: DateTime converts IMPLICITLY to DateTimeOffset, so a `? :` over
        // these two branches types as DateTimeOffset and quietly converts the zoneless case using the
        // HOST's local zone — which is the precise bug this whole codec exists to prevent, arriving
        // through the type system rather than through the parsing.
        if (HasExplicitOffset(text))
        {
            return DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        }

        return DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
    }

    /// <summary>Looks for a <c>±hh:mm</c> AFTER the date part, so the two hyphens in <c>2026-08-14</c>
    /// are not mistaken for a negative offset.</summary>
    private static bool HasExplicitOffset(string text)
    {
        var timePart = text.IndexOf('T', StringComparison.Ordinal) is var t and >= 0 ? text[(t + 1)..] : "";
        return timePart.Contains('+', StringComparison.Ordinal) || timePart.Contains('-', StringComparison.Ordinal);
    }
}
