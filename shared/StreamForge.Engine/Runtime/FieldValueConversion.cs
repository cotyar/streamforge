using System.Globalization;

namespace StreamForge.Engine.Runtime;

/// <summary>
/// Canonical, non-throwing scalar coercion by <see cref="FieldKind"/> — plan 009 Round C wave C1. This
/// is the semantic core behind the dialect's <c>TO_LONG</c>/<c>TO_DOUBLE</c>/<c>TO_BOOL</c>/
/// <c>TO_TIMESTAMP</c>/<c>TO_STRING</c> SQL functions (see <c>Sql/Validator.cs</c>'s
/// <c>KnownFunctions</c> + arity/result-kind switches, and <c>ExpressionEvaluator</c>'s function
/// dispatch in this same folder) and the <c>CAST(expr AS type)</c> sugar that desugars to them
/// (<c>Sql/Parser.cs</c>).
///
/// Public and keyed on the Engine's own <see cref="FieldKind"/> — not <c>StreamForge.Abstractions</c>'
/// <c>FieldType</c> — precisely so it can ALSO be the single implementation behind
/// <c>StreamForge.AppCore.Ingest.FieldValueCoercion.TryCoerce</c>, which already implements the
/// identical rules keyed on <c>FieldType</c> for every inbound row-shaping path (proto wire encoding,
/// connector mapping, client-push ingest). AppCore already references this Engine project (not the
/// other way around — the Engine may not depend on AppCore or on
/// <c>StreamForge.Abstractions</c>/Contracts), so once that call site maps <c>FieldType</c> →
/// <see cref="FieldKind"/> and delegates to <see cref="TryCoerce"/> below, the two conversions become
/// literally the same executable code instead of a hand-kept-in-sync duplicate.
///
/// <see cref="TryCoerce"/>'s rules are pinned to match <c>FieldValueCoercion.TryCoerce</c>'s CURRENT
/// behavior exactly, including two spots that look surprising but are deliberately left as-is —
/// changing either one changes the inbound ingest path's real behavior once the delegation above
/// lands, not just a SQL function's:
///   - Bool-from-string is permissive, not a fixed spelling list: "true"/"false" parse case-
///     insensitively, and every OTHER non-empty, non-"0" string (garbage included, e.g. "abc") coerces
///     to <c>true</c>. <c>TO_BOOL('abc')</c> is therefore <c>true</c>, not NULL — see
///     <c>FieldValueCoercionTests.Bool_coercion_from_string("anything-else", true)</c>, an existing
///     pinned AppCore test this wave leaves untouched. <b>Finding</b>: if a stricter "true/false/0/1
///     else NULL" rule is ever wanted for the SQL function, it must NOT be made here without also
///     re-checking every inbound path that already relies on the permissive rule.
///   - Bool-from-number is nonzero-is-true (any nonzero long/double, not just 1), matching
///     <c>FieldValueCoercion.TryToBool</c>'s <c>l != 0</c> / <c>d != 0</c>.
///   - Double-to-long (both the direct <see cref="FieldKind.Long"/>/<see cref="FieldKind.Timestamp"/>
///     coercion here and <c>TO_LONG</c>'s runtime behavior) uses an <i>unchecked</i> narrowing cast,
///     matching <c>FieldValueCoercion.TryToInt64</c>'s existing `(long)d`: a double outside the `long`
///     range does NOT come back NULL, it comes back as whatever the CLR's unchecked conversion
///     produces. <b>Finding</b>: this reads as a real gap against wave C1's own "overflow ⇒ NULL"
///     goal, but the fix belongs on the inbound path too if it ever happens, not silently in only one
///     of the two callers — reported rather than quietly special-cased here. A numeric-STRING overflow
///     (e.g. `TO_LONG('99999999999999999999')`) DOES already come back NULL, via `long.TryParse`
///     returning false — that path has no such gap.
/// <see cref="FieldKind.Timestamp"/> shares <see cref="FieldKind.Long"/>'s exact coercion rule here
/// (epoch millis, numeric or numeric-string, no ISO-8601 text) — see <see cref="TryToTimestamp"/>
/// below for the SQL <c>TO_TIMESTAMP</c> function's own, wider rule, which is intentionally NOT what
/// this method uses for <see cref="FieldKind.Timestamp"/>, so this method stays a byte-for-byte match
/// of the existing inbound path.
/// </summary>
public static class FieldValueConversion
{
    /// <summary>Coerces one already-normalized scalar leaf value to the CLR shape
    /// <paramref name="kind"/> expects. <see cref="FieldKind.Json"/> is structural, not a value
    /// conversion, so it always succeeds by passing the value through unchanged.</summary>
    public static bool TryCoerce(FieldKind kind, object value, out object? coerced)
    {
        switch (kind)
        {
            case FieldKind.String:
                coerced = ToDisplayString(value);
                return true;
            case FieldKind.Double:
                return TryToDouble(value, out coerced);
            case FieldKind.Long:
                return TryToLong(value, out coerced);
            case FieldKind.Timestamp:
                // Still epoch millis, with the identical wire/CLR representation Long has — the only
                // difference is that a CLR date/time VALUE means something here, so it is accepted
                // before falling through to Long's numeric rule.
                return TryToTimestampMillis(value, out coerced);
            case FieldKind.Bool:
                return TryToBool(value, out coerced);
            case FieldKind.Json:
                coerced = value;
                return true;
            default:
                coerced = null;
                return false;
        }
    }

    /// <summary>The <c>TO_TIMESTAMP</c> SQL function's own, wider rule: epoch-ms as a number OR a
    /// numeric string, or ISO-8601 text. A genuine superset of both <c>TryCoerce(Timestamp, …)</c>
    /// above (which has no ISO-8601 text support) and <see cref="ResolveTimestamp"/> below (which has
    /// no numeric-string support) — combining the two is the plan's own explicit direction ("epoch-ms
    /// (numeric or numeric string) and ISO-8601 … reusing the rules RowTimestamp.Resolve already
    /// applies"). Returns <c>false</c> (NULL) for anything else — never a fallback value; this is the
    /// "total, never throwing" SQL function, not the "_ts" resolution helper below.</summary>
    public static bool TryToTimestamp(object? value, out object? coerced)
    {
        switch (value)
        {
            case long l:
                coerced = l;
                return true;
            case double d:
                coerced = (long)d;
                return true;
            case DateTimeOffset off:
                coerced = off.ToUnixTimeMilliseconds();
                return true;
            case DateTime dt:
                coerced = ToEpochMs(dt);
                return true;
            case string s:
                if (long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var epoch))
                {
                    coerced = epoch;
                    return true;
                }
                if (DateTimeOffset.TryParse(
                        s, CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dto))
                {
                    coerced = dto.ToUnixTimeMilliseconds();
                    return true;
                }
                coerced = null;
                return false;
            default:
                coerced = null;
                return false;
        }
    }

    /// <summary>Exact behavioral port of <c>StreamForge.AppCore.Ingest.RowTimestamp.Resolve</c> — kept
    /// here as that file's intended delegation target (plan 009 Round C wave C1 correction) once its
    /// call site is updated to call this instead of keeping its own copy. Deliberately narrower than
    /// <see cref="TryToTimestamp"/> above (no numeric-string epoch support) and fallback- rather than
    /// NULL-shaped: a number is epoch-ms, a string is parsed as ISO-8601 (UTC); anything else (missing,
    /// unparseable, wrong type) falls back to <paramref name="fallbackMs"/>. This is the "_ts"
    /// resolution rule (a value always comes out), not the SQL function's total-with-NULL rule.</summary>
    public static long ResolveTimestamp(object? value, long fallbackMs)
    {
        switch (value)
        {
            case long l:
                return l;
            case double d:
                return (long)d;
            case DateTimeOffset dto:
                return dto.ToUnixTimeMilliseconds();
            case DateTime dt:
                return ToEpochMs(dt);
            case string s when DateTimeOffset.TryParse(
                s, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dto):
                return dto.ToUnixTimeMilliseconds();
        }

        return fallbackMs;
    }

    /// <summary>Culture-invariant rendering. A CLR date/time is written as ISO-8601 UTC rather than
    /// through <see cref="IFormattable"/>, which produced the invariant culture's US-style
    /// <c>08/15/2026 12:00:00</c> — not sortable as text, not round-trippable by
    /// <see cref="TryToTimestamp"/>'s own ISO reader, and differently shaped for
    /// <see cref="DateTime"/> vs <see cref="DateTimeOffset"/>. The format matches the one
    /// <c>ExpressionEvaluator.EvalToString</c> already emits for <c>TO_STRING(TO_TIMESTAMP(x))</c>, so
    /// a timestamp renders identically however it reached a String field, and the zone rule is
    /// <see cref="ToEpochMs"/>'s — one decision, not a second one made here.</summary>
    private static string ToDisplayString(object value) => value switch
    {
        string s => s,
        bool b => b ? "true" : "false",
        DateTime or DateTimeOffset => FormatIso8601(value),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    /// <summary>ISO-8601 UTC, milliseconds, always <c>Z</c> — the single textual shape for an instant in
    /// this dialect. An offset-bearing value is normalised to UTC rather than printed with its offset,
    /// so two rows describing the same instant compare and sort equal as text.</summary>
    public static string FormatIso8601(object value)
    {
        long epochMs = value switch
        {
            DateTimeOffset off => off.ToUnixTimeMilliseconds(),
            DateTime dt => ToEpochMs(dt),
            long l => l,
            _ => 0,
        };
        return FormatEpochMsIso8601(epochMs);
    }

    /// <summary>The one formatter for epoch millis → ISO-8601 text, shared with
    /// <c>ExpressionEvaluator.EvalToString</c> so <c>TO_STRING(TO_TIMESTAMP(x))</c> and a Timestamp
    /// value landing in a String field cannot print differently.</summary>
    public static string FormatEpochMsIso8601(long epochMs) =>
        DateTimeOffset.FromUnixTimeMilliseconds(epochMs)
            .ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);

    /// <summary>The Timestamp-only half of the coercion rule: a CLR date/time value is epoch millis
    /// directly, and anything else falls through to <see cref="TryToLong"/>'s numeric rule (a bare number
    /// or numeric string), which is what this kind accepted before. Additive — a DateTime used to be a
    /// coercion failure, i.e. a silent NULL, which is how a Postgres `timestamptz` column declared as a
    /// Timestamp field emptied itself out.</summary>
    private static bool TryToTimestampMillis(object value, out object? coerced)
    {
        switch (value)
        {
            case DateTimeOffset dto:
                coerced = dto.ToUnixTimeMilliseconds();
                return true;
            case DateTime dt:
                coerced = ToEpochMs(dt);
                return true;
            default:
                return TryToLong(value, out coerced);
        }
    }

    /// <summary>The one place this file decides what a <see cref="DateTime"/> means, so the three
    /// timestamp rules above (<see cref="TryCoerce"/>'s Timestamp kind, <c>TO_TIMESTAMP</c>, and
    /// <see cref="ResolveTimestamp"/>) cannot drift apart.
    ///
    /// <para>Two of the three <see cref="DateTimeKind"/>s carry their own answer: <c>Utc</c> (what a
    /// driver hands back for a zone-aware column) and <c>Local</c> both convert exactly, because the CLR
    /// knows the offset. <c>Unspecified</c> — a Postgres `timestamp without time zone`, a SQL Server
    /// `datetime2` — genuinely does not, and is read as UTC. That is not a fresh guess: it is the rule
    /// this file already applies to timestamp TEXT, where both parsers use
    /// <c>DateTimeStyles.AssumeUniversal</c>, and the same one <c>PgCdcSource.ToUnixMs</c> applies to a
    /// pgoutput commit timestamp. Reading it as Local instead would make the value depend on the host
    /// process's timezone — a deploy detail, not a property of the data.</para></summary>
    private static long ToEpochMs(DateTime value) => new DateTimeOffset(
        value.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(value, DateTimeKind.Utc) : value)
        .ToUnixTimeMilliseconds();

    private static bool TryToDouble(object value, out object? coerced)
    {
        switch (value)
        {
            case double d: coerced = d; return true;
            case float f: coerced = (double)f; return true;
            case long l: coerced = (double)l; return true;
            case int i: coerced = (double)i; return true;
            // Every remaining CLR numeric a database driver actually hands back. `decimal` is the one
            // that bites: Postgres `numeric`/`money` and SQL Server `decimal`/`money` arrive as decimal
            // through the CDC and polled-source Cell mappings (both pass it through untouched), so
            // without this arm a declared Double field silently NULLed every money column. `short`/
            // `byte`/unsigned come from smallint/tinyint the same way. Additive only — these were
            // coercion failures (NULL) before, never a different value.
            case decimal m: coerced = (double)m; return true;
            case short sh: coerced = (double)sh; return true;
            case ushort us: coerced = (double)us; return true;
            case byte by: coerced = (double)by; return true;
            case sbyte sb: coerced = (double)sb; return true;
            case uint ui: coerced = (double)ui; return true;
            case ulong ul: coerced = (double)ul; return true;
            case bool b: coerced = b ? 1d : 0d; return true;
            case string s when double.TryParse(
                s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed):
                coerced = parsed;
                return true;
            default:
                coerced = null;
                return false;
        }
    }

    private static bool TryToLong(object value, out object? coerced)
    {
        switch (value)
        {
            case long l: coerced = l; return true;
            case int i: coerced = (long)i; return true;
            case double d: coerced = (long)d; return true; // unchecked, see class doc's "Finding"
            case float f: coerced = (long)f; return true;
            // See TryToDouble's note: the driver-produced numerics that had no arm at all until now.
            // decimal narrows unchecked for the same reason double does, and truncates toward zero.
            case decimal m: coerced = (long)m; return true;
            case short sh: coerced = (long)sh; return true;
            case ushort us: coerced = (long)us; return true;
            case byte by: coerced = (long)by; return true;
            case sbyte sb: coerced = (long)sb; return true;
            case uint ui: coerced = (long)ui; return true;
            case ulong ul: coerced = (long)ul; return true;
            case bool b: coerced = b ? 1L : 0L; return true;
            case string s when long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed):
                coerced = parsed;
                return true;
            default:
                coerced = null;
                return false;
        }
    }

    private static bool TryToBool(object value, out object? coerced)
    {
        switch (value)
        {
            case bool b: coerced = b; return true;
            case long l: coerced = l != 0; return true;
            case int i: coerced = i != 0; return true;
            case double d: coerced = d != 0; return true;
            // Same additive widening as TryToDouble/TryToLong, on the same nonzero-is-true rule.
            case decimal m: coerced = m != 0; return true;
            case short sh: coerced = sh != 0; return true;
            case ushort us: coerced = us != 0; return true;
            case byte by: coerced = by != 0; return true;
            case sbyte sb: coerced = sb != 0; return true;
            case uint ui: coerced = ui != 0; return true;
            case ulong ul: coerced = ul != 0; return true;
            case string s: coerced = bool.TryParse(s, out var parsed) ? parsed : s is not ("" or "0"); return true;
            default:
                coerced = null;
                return false;
        }
    }
}
