using System.Globalization;
using System.Text.Json;
using StreamsForge.Abstractions;

namespace StreamsForge.AppCore.Connectors.Formats;

/// <summary>
/// Infers a <see cref="FieldDef"/> list from a sample of parsed items (plan 006 — schema derivation
/// for the "start from real data" onboarding flow, as opposed to the OpenAPI-derivation path).
///
/// <para><b>Sampling</b>: only the first 100 items are examined (a fixed ceiling — large feeds are
/// not fully scanned). Only object items contribute fields; non-object items in the sample are
/// ignored.</para>
///
/// <para><b>Per-key typing rule</b> (applied across every value found under that key among the sample
/// items that actually have it — <b>a key missing from some items is typed purely from the items that
/// do have it</b>, a missing key never counts as "mixed"; an explicit JSON <c>null</c> is likewise
/// ignored when determining type):</para>
/// <list type="bullet">
/// <item>every value numeric and integral (no <c>.</c>, no exponent in the literal) →
/// <see cref="FieldType.Long"/>; if any value is fractional or written in scientific notation →
/// <see cref="FieldType.Double"/> (a rule based on the literal's notation, not its mathematical
/// value — <c>5.0</c> is Double even though it happens to be a whole number).</item>
/// <item>every value a JSON boolean → <see cref="FieldType.Bool"/>.</item>
/// <item>every value a JSON string, and EVERY one of them parses as ISO-8601 →
/// <see cref="FieldType.Timestamp"/>; otherwise <see cref="FieldType.String"/>.</item>
/// <item>every value a JSON object → <see cref="FieldType.Json"/> with <see cref="FieldDef.Children"/>
/// recursively inferred from those objects' own properties.</item>
/// <item>every value a JSON array → <see cref="FieldDef.IsArray"/> = true; the element TYPE (and
/// Children, if the elements are objects) is inferred the same way, over every element flattened
/// across every array value found for that key. Nested arrays-of-arrays are not deeply inferred and
/// fall into the "mixed/other" bucket below (documented ceiling).</item>
/// <item>anything else — no values at all (all-null), or a genuine mix of kinds (e.g. sometimes a
/// string, sometimes a number) — → <see cref="FieldType.String"/>.</item>
/// </list>
///
/// <para>Field order in the result follows first-seen order across the sample.</para>
/// </summary>
public static class SchemaInference
{
    private const int SampleSize = 100;

    /// <summary>Infers a field list from up to the first 100 of <paramref name="items"/>.</summary>
    public static List<FieldDef> Infer(IReadOnlyList<JsonElement> items)
    {
        var sample = items.Count > SampleSize ? items.Take(SampleSize).ToList() : items.ToList();
        return InferFields(sample);
    }

    private static List<FieldDef> InferFields(IReadOnlyList<JsonElement> items)
    {
        var keyOrder = new List<string>();
        var seenKeys = new HashSet<string>(StringComparer.Ordinal);
        var valuesByKey = new Dictionary<string, List<JsonElement>>(StringComparer.Ordinal);

        foreach (var item in items)
        {
            if (item.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            foreach (var property in item.EnumerateObject())
            {
                if (seenKeys.Add(property.Name))
                {
                    keyOrder.Add(property.Name);
                    valuesByKey[property.Name] = [];
                }

                valuesByKey[property.Name].Add(property.Value);
            }
        }

        var fields = new List<FieldDef>(keyOrder.Count);
        foreach (var key in keyOrder)
        {
            fields.Add(InferField(key, valuesByKey[key]));
        }

        return fields;
    }

    private static FieldDef InferField(string name, List<JsonElement> values)
    {
        var nonNull = FilterNonNull(values);
        if (nonNull.Count == 0)
        {
            return new FieldDef(name, FieldType.String);
        }

        if (nonNull.All(v => v.ValueKind == JsonValueKind.Array))
        {
            var elements = new List<JsonElement>();
            foreach (var array in nonNull)
            {
                elements.AddRange(array.EnumerateArray());
            }

            var (elementType, elementChildren) = InferScalarType(elements);
            return new FieldDef(name, elementType, elementChildren, IsArray: true);
        }

        var (type, children) = InferScalarType(nonNull);
        return new FieldDef(name, type, children);
    }

    /// <summary>Infers a (Type, Children) pair for a flat set of values — used both for a plain
    /// field's own values and for an array field's flattened element values.</summary>
    private static (FieldType Type, List<FieldDef>? Children) InferScalarType(List<JsonElement> values)
    {
        var nonNull = FilterNonNull(values);
        if (nonNull.Count == 0)
        {
            return (FieldType.String, null);
        }

        if (nonNull.All(v => v.ValueKind == JsonValueKind.Object))
        {
            return (FieldType.Json, InferFields(nonNull));
        }

        if (nonNull.All(v => v.ValueKind is JsonValueKind.True or JsonValueKind.False))
        {
            return (FieldType.Bool, null);
        }

        if (nonNull.All(v => v.ValueKind == JsonValueKind.Number))
        {
            return (InferNumberType(nonNull), null);
        }

        if (nonNull.All(v => v.ValueKind == JsonValueKind.String))
        {
            return AllStringsAreIso8601(nonNull) ? (FieldType.Timestamp, null) : (FieldType.String, null);
        }

        // Mixed kinds (including nested arrays-of-arrays, which never match any branch above) ->
        // schemaless String.
        return (FieldType.String, null);
    }

    private static List<JsonElement> FilterNonNull(List<JsonElement> values) =>
        values.Where(v => v.ValueKind is not (JsonValueKind.Null or JsonValueKind.Undefined)).ToList();

    private static FieldType InferNumberType(List<JsonElement> values)
    {
        foreach (var v in values)
        {
            var raw = v.GetRawText();
            if (raw.Contains('.') || raw.Contains('e') || raw.Contains('E'))
            {
                return FieldType.Double;
            }
        }

        return FieldType.Long;
    }

    // A representative, documented (not exhaustive) subset of ISO-8601: calendar dates and
    // date-times with 0-7 fractional-second digits, offset expressed as 'Z' or +hh:mm/-hh:mm, or
    // omitted (unspecified/local). Omitted-seconds forms ("yyyy-MM-ddTHH:mm") are not recognized.
    private static readonly string[] Iso8601Formats =
    [
        "yyyy-MM-dd",
        "yyyy-MM-ddTHH:mm:ssK",
        "yyyy-MM-ddTHH:mm:ss.fK",
        "yyyy-MM-ddTHH:mm:ss.ffK",
        "yyyy-MM-ddTHH:mm:ss.fffK",
        "yyyy-MM-ddTHH:mm:ss.ffffK",
        "yyyy-MM-ddTHH:mm:ss.fffffK",
        "yyyy-MM-ddTHH:mm:ss.ffffffK",
        "yyyy-MM-ddTHH:mm:ss.fffffffK",
    ];

    private static bool AllStringsAreIso8601(List<JsonElement> values)
    {
        foreach (var v in values)
        {
            var s = v.GetString();
            if (s is null || !DateTimeOffset.TryParseExact(
                    s, Iso8601Formats, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out _))
            {
                return false;
            }
        }

        return true;
    }
}
