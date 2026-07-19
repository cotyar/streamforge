using System.Text.Json;

namespace StreamForge.AppCore.Json;

/// <summary>
/// Recursively normalizes <see cref="JsonElement"/> values into plain CLR values so they behave like
/// any other in-memory row value (Engine JSON-path expressions, dictionary equality, serialization)
/// instead of leaking System.Text.Json's document-backed representation. Plan 005 (Dapr sibling
/// runtime) D-D: used at every Dapr pub/sub ingress — sources, generator batches, external
/// publisher/processor payloads — before rows reach the Engine, mirroring how Orleans stream payloads
/// already arrive as plain CLR values.
/// </summary>
public static class JsonValueNormalizer
{
    /// <summary>Normalizes a single value. Non-<see cref="JsonElement"/> inputs pass through unchanged
    /// (already-normalized values, e.g. from a prior call, are idempotent).</summary>
    public static object? Normalize(object? value) => value is JsonElement element ? Normalize(element) : value;

    /// <summary>Normalizes a <see cref="JsonElement"/> into a plain CLR value:
    /// <list type="bullet">
    /// <item>string → <see cref="string"/></item>
    /// <item>true/false → <see cref="bool"/></item>
    /// <item>null → <c>null</c></item>
    /// <item>number → <see cref="long"/> when it round-trips exactly as an integer, otherwise
    /// <see cref="double"/></item>
    /// <item>object → <see cref="Dictionary{TKey,TValue}"/> of <see cref="string"/> to
    /// <c>object?</c>, recursively normalized</item>
    /// <item>array → <see cref="List{T}"/> of <c>object?</c>, recursively normalized</item>
    /// </list>
    /// </summary>
    public static object? Normalize(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => element.GetString(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        JsonValueKind.Number => NormalizeNumber(element),
        JsonValueKind.Object => NormalizeObject(element),
        JsonValueKind.Array => NormalizeArray(element),
        _ => throw new ArgumentOutOfRangeException(nameof(element), element.ValueKind, "Unknown JSON value kind"),
    };

    private static object NormalizeNumber(JsonElement element) =>
        element.TryGetInt64(out var l) ? l : element.GetDouble();

    private static Dictionary<string, object?> NormalizeObject(JsonElement element)
    {
        var result = new Dictionary<string, object?>();
        foreach (var property in element.EnumerateObject())
        {
            result[property.Name] = Normalize(property.Value);
        }

        return result;
    }

    private static List<object?> NormalizeArray(JsonElement element)
    {
        var result = new List<object?>();
        foreach (var item in element.EnumerateArray())
        {
            result.Add(Normalize(item));
        }

        return result;
    }

    /// <summary>Normalizes every value in <paramref name="row"/> in place, replacing any
    /// <see cref="JsonElement"/> value (including ones nested inside already-normalized
    /// dictionaries/lists from a prior partial normalization) with its plain-CLR equivalent. No-op if
    /// no value is a <see cref="JsonElement"/>. Intended for a freshly-deserialized envelope row
    /// dictionary at a pub/sub ingress boundary.</summary>
    public static void NormalizeInPlace(Dictionary<string, object?> row)
    {
        List<string>? keys = null;
        foreach (var kvp in row)
        {
            if (kvp.Value is JsonElement)
            {
                (keys ??= []).Add(kvp.Key);
            }
        }

        if (keys is null)
        {
            return;
        }

        foreach (var key in keys)
        {
            row[key] = Normalize((JsonElement)row[key]!);
        }
    }
}
