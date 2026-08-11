using StreamForge.Engine.Runtime;

namespace StreamForge.AppCore.Ingest;

/// <summary>
/// Shared "_ts" resolution, extracted from <c>RecordExtractor.ResolveTimestamp</c> (plan 008 W4) so
/// the connector mapping path (<c>MappingSpec.TimestampField</c>) and the client-push ingest path
/// (the raw "_ts" key a client may send) agree on what a timestamp value means.
///
/// <para>Plan 009 C1: the rule itself now lives in the Engine
/// (<see cref="FieldValueConversion.ResolveTimestamp"/>), because the SQL dialect's
/// <c>TO_TIMESTAMP</c> has to apply exactly the same one and the Engine is the semantic core both
/// sides depend on (AppCore references Engine, not the other way round). This type stays as the
/// inbound path's name for it — one implementation, two call sites, nothing to drift.</para>
/// </summary>
public static class RowTimestamp
{
    /// <summary>A number is epoch-ms, a string is parsed as ISO-8601 (UTC); anything else (missing,
    /// unparseable, wrong type) falls back to <paramref name="fallbackMs"/>.</summary>
    public static long Resolve(object? value, long fallbackMs) =>
        FieldValueConversion.ResolveTimestamp(value, fallbackMs);
}
