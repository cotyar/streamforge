using NpgsqlTypes;

namespace StreamForge.Connectors.Database;

/// <summary>
/// The codec between the driver's opaque persisted <c>string?</c> cursor (see <see cref="DbCursor"/>,
/// whose CDC sibling this is) and each dialect's native log position: Postgres's
/// <see cref="NpgsqlLogSequenceNumber"/> and SQL Server's <c>binary(10)</c> LSN. Pure functions, no I/O,
/// same split as <see cref="DbCursor"/> and for the same reason.
///
/// <para><b>Why the round-trip has to be exact.</b> The cursor is the only thing standing between a
/// restart and either re-reading everything already consumed or silently skipping data — and neither
/// failure announces itself. A poll resumes from whatever this codec decodes; get the decode wrong by
/// one byte and the reader either replays a chunk of the log it already applied, or starts past a
/// change it never sent. There is no downstream signal that catches either mistake; the log position is
/// the only thing that would have.</para>
///
/// <para><b>Postgres</b> round-trips through <see cref="NpgsqlLogSequenceNumber.Parse(string)"/> and
/// <see cref="object.ToString"/> — its own canonical <c>"0/16B3748"</c> text form. We do not hand-roll
/// that format; Npgsql already owns it and already gets it right.</para>
///
/// <para><b>SQL Server's</b> <c>binary(10)</c> LSN has no built-in text form, so this codec defines one:
/// lowercase hex, no <c>0x</c> prefix, exactly 20 characters. A naive "compare as a number" over an LSN
/// this wide is exactly the bug a fixed byte-wise comparison exists to avoid — 10 bytes is 80 bits, wider
/// than <see cref="ulong"/>, so parsing it into any built-in integer type before comparing silently
/// truncates the high bytes and gets the order wrong across exactly the byte-boundary carries
/// (<c>…00ff…</c> vs <c>…0100…</c>) that matter for retention-breach detection (<c>from &lt; min_lsn</c>).
/// <see cref="CompareMsSql"/> decodes both sides to <see cref="byte"/>[] and compares byte by byte instead.</para>
/// </summary>
public static class CdcLsn
{
    /// <summary>MSSQL LSNs are always this many bytes.</summary>
    private const int MsSqlLsnLength = 10;

    /// <summary>...and always this many hex characters once encoded.</summary>
    private const int MsSqlLsnHexLength = MsSqlLsnLength * 2;

    /// <summary>Postgres LSN → its canonical <c>"0/16B3748"</c> text form.</summary>
    public static string EncodePg(NpgsqlLogSequenceNumber lsn) => lsn.ToString();

    /// <summary>Text → Postgres LSN. Throws <see cref="FormatException"/> naming the offending text
    /// when it will not parse — never coerced to <see cref="NpgsqlLogSequenceNumber.Invalid"/> or to
    /// zero, for the same reason <see cref="DbCursor.Decode"/> never defaults: either wrong guess is a
    /// silent re-read or a silent skip, and both look identical to "everything is fine" until an
    /// operator notices missing or duplicated rows much later.</summary>
    public static NpgsqlLogSequenceNumber DecodePg(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        try
        {
            return NpgsqlLogSequenceNumber.Parse(text);
        }
        catch (FormatException ex)
        {
            throw new FormatException($"'{text}' is not a valid Postgres LSN: {ex.Message}", ex);
        }
    }

    /// <summary>MSSQL <c>binary(10)</c> LSN → lowercase hex, no <c>0x</c> prefix, exactly
    /// <see cref="MsSqlLsnHexLength"/> characters.</summary>
    public static string EncodeMsSql(byte[] lsn)
    {
        ArgumentNullException.ThrowIfNull(lsn);
        if (lsn.Length != MsSqlLsnLength)
        {
            throw new FormatException(
                $"MSSQL LSN must be {MsSqlLsnLength} bytes, got {lsn.Length} (0x{Convert.ToHexStringLower(lsn)})");
        }

        return Convert.ToHexStringLower(lsn);
    }

    /// <summary>Lowercase hex text → MSSQL <c>binary(10)</c> LSN. Rejects anything that is not exactly
    /// <see cref="MsSqlLsnHexLength"/> lowercase hex characters, or that (defensively) decodes to a
    /// length other than <see cref="MsSqlLsnLength"/> bytes — throwing with the offending text in the
    /// message rather than truncating or zero-padding it into shape.</summary>
    public static byte[] DecodeMsSql(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        if (text.Length != MsSqlLsnHexLength || !IsLowerHex(text))
        {
            throw new FormatException(
                $"'{text}' is not a valid MSSQL LSN: expected exactly {MsSqlLsnHexLength} lowercase hex characters, no '0x' prefix");
        }

        var bytes = Convert.FromHexString(text);
        if (bytes.Length != MsSqlLsnLength)
        {
            throw new FormatException($"'{text}' decodes to {bytes.Length} bytes, expected {MsSqlLsnLength}");
        }

        return bytes;
    }

    /// <summary>Ordinal comparison of two encoded MSSQL LSNs, decoded and compared byte by byte —
    /// unsigned, most-significant byte first — rather than as text or as a parsed number. Wave C needs
    /// this exact comparison to detect a retention breach (<c>from &lt; min_lsn</c>) without
    /// reintroducing the 64-bit-truncation bug this class's own doc comment describes.</summary>
    public static int CompareMsSql(string a, string b)
    {
        var bytesA = DecodeMsSql(a);
        var bytesB = DecodeMsSql(b);

        for (var i = 0; i < MsSqlLsnLength; i++)
        {
            var c = bytesA[i].CompareTo(bytesB[i]);
            if (c != 0)
            {
                return c;
            }
        }

        return 0;
    }

    private static bool IsLowerHex(string text)
    {
        foreach (var ch in text)
        {
            var isLowerHexDigit = (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
            if (!isLowerHexDigit)
            {
                return false;
            }
        }

        return true;
    }
}
