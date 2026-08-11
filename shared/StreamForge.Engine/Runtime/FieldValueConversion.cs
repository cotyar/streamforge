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
            case FieldKind.Timestamp: // epoch millis - identical wire/CLR representation to Long
                return TryToLong(value, out coerced);
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
            case string s when DateTimeOffset.TryParse(
                s, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dto):
                return dto.ToUnixTimeMilliseconds();
        }

        return fallbackMs;
    }

    private static string ToDisplayString(object value) => value switch
    {
        string s => s,
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    private static bool TryToDouble(object value, out object? coerced)
    {
        switch (value)
        {
            case double d: coerced = d; return true;
            case float f: coerced = (double)f; return true;
            case long l: coerced = (double)l; return true;
            case int i: coerced = (double)i; return true;
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
            case string s: coerced = bool.TryParse(s, out var parsed) ? parsed : s is not ("" or "0"); return true;
            default:
                coerced = null;
                return false;
        }
    }
}
