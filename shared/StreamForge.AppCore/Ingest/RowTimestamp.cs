using System.Globalization;

namespace StreamForge.AppCore.Ingest;

/// <summary>
/// Shared "_ts" resolution, extracted from <c>RecordExtractor.ResolveTimestamp</c> (plan 008 W4) so
/// the connector mapping path (<c>MappingSpec.TimestampField</c>) and the client-push ingest path
/// (the raw "_ts" key a client may send) agree on what a timestamp value means.
/// </summary>
public static class RowTimestamp
{
    /// <summary>A number is epoch-ms, a string is parsed as ISO-8601 (UTC); anything else (missing,
    /// unparseable, wrong type) falls back to <paramref name="fallbackMs"/>.</summary>
    public static long Resolve(object? value, long fallbackMs)
    {
        switch (value)
        {
            case long l:
                return l;
            case double d:
                return (long)d;
            case string s when DateTimeOffset.TryParse(
                s, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dto):
                return dto.ToUnixTimeMilliseconds();
        }

        return fallbackMs;
    }
}
