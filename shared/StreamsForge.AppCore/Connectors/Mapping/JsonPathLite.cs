using System.Globalization;
using System.Text.Json;

namespace StreamsForge.AppCore.Connectors.Mapping;

/// <summary>
/// A deliberately tiny JSONPath subset used to locate values inside connector response payloads
/// (plan 006, D-A). Supported grammar, and nothing else:
/// <list type="bullet">
/// <item><c>$</c> — the root itself.</item>
/// <item><c>.name</c> — a property access.</item>
/// <item><c>['name']</c> / <c>["name"]</c> — a quoted property access (for names containing
/// characters that don't fit a bare identifier, e.g. spaces or dots).</item>
/// <item><c>[n]</c> — a zero-based array index (non-negative integer literal only).</item>
/// <item><c>[*]</c> — every element of an array.</item>
/// </list>
/// A leading <c>$</c> is optional for a relative path (e.g. both <c>"$.data.trades[*]"</c> and
/// <c>"data.trades[*]"</c> parse identically); a path may also start with a bare identifier with no
/// leading <c>.</c> or <c>$</c> (e.g. <c>"user.tier"</c>, <c>"price"</c>).
///
/// <para><b>Not supported</b> (rejected with a <see cref="FormatException"/> naming the offending
/// token, closed subset — no silent partial support): recursive descent (<c>..</c>), wildcard
/// property keys (<c>.*</c>, only <c>[*]</c> array-wildcard exists), filter expressions
/// (<c>[?(...)]</c>), slices (<c>[1:3]</c>), and negative indices.</para>
///
/// <para><b>Matching semantics</b>: a missing segment (property not present on the current object,
/// index out of range, wildcard applied to a non-array) simply yields no match for that branch — it
/// is NOT an error. <see cref="Select"/> therefore only ever throws for a syntactically invalid path,
/// never because the data didn't happen to have the shape the path expects.</para>
/// </summary>
public static class JsonPathLite
{
    /// <summary>Evaluates <paramref name="path"/> against <paramref name="root"/> and returns every
    /// matching element (possibly empty). Throws <see cref="FormatException"/> if
    /// <paramref name="path"/> uses syntax outside the supported subset.</summary>
    public static IReadOnlyList<JsonElement> Select(JsonElement root, string path)
    {
        var segments = Parse(path);

        var current = new List<JsonElement> { root };
        foreach (var segment in segments)
        {
            var next = new List<JsonElement>();
            foreach (var element in current)
            {
                switch (segment.Kind)
                {
                    case SegmentKind.Property:
                        if (element.ValueKind == JsonValueKind.Object && element.TryGetProperty(segment.Name!, out var value))
                        {
                            next.Add(value);
                        }
                        break;

                    case SegmentKind.Index:
                        if (element.ValueKind == JsonValueKind.Array)
                        {
                            var i = 0;
                            foreach (var item in element.EnumerateArray())
                            {
                                if (i == segment.Index)
                                {
                                    next.Add(item);
                                    break;
                                }
                                i++;
                            }
                        }
                        break;

                    case SegmentKind.Wildcard:
                        if (element.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var item in element.EnumerateArray())
                            {
                                next.Add(item);
                            }
                        }
                        break;
                }
            }

            current = next;
        }

        return current;
    }

    private enum SegmentKind { Property, Index, Wildcard }

    private readonly record struct Segment(SegmentKind Kind, string? Name, int Index);

    private static List<Segment> Parse(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        var segments = new List<Segment>();
        var i = 0;
        var n = path.Length;

        if (n > 0 && path[0] == '$')
        {
            i = 1;
        }

        var first = true;
        while (i < n)
        {
            var c = path[i];
            if (c == '.')
            {
                i++;
                if (i < n && path[i] == '.')
                {
                    throw new FormatException(
                        $"JSONPath-lite: recursive descent '..' is not supported (position {i - 1} in \"{path}\").");
                }
                segments.Add(ParseIdentifier(path, ref i));
            }
            else if (c == '[')
            {
                segments.Add(ParseBracket(path, ref i));
            }
            else if (first)
            {
                // Bare leading identifier with no '$' and no leading '.', e.g. "user.tier" or "price".
                segments.Add(ParseIdentifier(path, ref i));
            }
            else
            {
                throw new FormatException($"JSONPath-lite: unexpected token '{c}' at position {i} in \"{path}\".");
            }

            first = false;
        }

        return segments;
    }

    private static Segment ParseIdentifier(string path, ref int i)
    {
        var start = i;
        while (i < path.Length && path[i] != '.' && path[i] != '[')
        {
            i++;
        }

        var name = path[start..i];
        if (name.Length == 0)
        {
            throw new FormatException($"JSONPath-lite: empty property name at position {start} in \"{path}\".");
        }

        if (name == "*")
        {
            throw new FormatException(
                $"JSONPath-lite: wildcard key '.*' is not supported at position {start} in \"{path}\" (only '[*]' is).");
        }

        return new Segment(SegmentKind.Property, name, 0);
    }

    private static Segment ParseBracket(string path, ref int i)
    {
        var openPos = i;
        i++; // consume '['

        var start = i;
        while (i < path.Length && path[i] != ']')
        {
            i++;
        }

        if (i >= path.Length)
        {
            throw new FormatException($"JSONPath-lite: unterminated '[' at position {openPos} in \"{path}\".");
        }

        var inner = path[start..i];
        i++; // consume ']'

        if (inner == "*")
        {
            return new Segment(SegmentKind.Wildcard, null, 0);
        }

        if (inner.Length >= 2 && ((inner[0] == '\'' && inner[^1] == '\'') || (inner[0] == '"' && inner[^1] == '"')))
        {
            return new Segment(SegmentKind.Property, inner[1..^1], 0);
        }

        if (inner.Length > 0 && IsAllAsciiDigits(inner))
        {
            return new Segment(SegmentKind.Index, null, int.Parse(inner, NumberStyles.None, CultureInfo.InvariantCulture));
        }

        throw new FormatException(
            $"JSONPath-lite: unsupported bracket expression '[{inner}]' at position {openPos} in \"{path}\" " +
            "(filters, slices, and negative indices are not supported).");
    }

    private static bool IsAllAsciiDigits(string s)
    {
        foreach (var c in s)
        {
            if (c is < '0' or > '9')
            {
                return false;
            }
        }
        return true;
    }
}
