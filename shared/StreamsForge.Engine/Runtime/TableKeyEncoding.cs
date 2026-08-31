namespace StreamsForge.Engine.Runtime;

/// <summary>Encodes a scalar value (join key, GROUP BY key component) into a comparable string —
/// mirrors WindowOperator's group-key encoding so equal SQL values always collide to the same bucket.</summary>
internal static class TableKeyEncoding
{
    public static string EncodeScalar(object? value) => value switch
    {
        null => "N",
        long l => $"L:{l}",
        double d => $"D:{d}",
        string s => $"S:{s}",
        bool b => $"B:{b}",
        Dictionary<string, object?> or List<object?> => $"J:{JsonText.Serialize(value)}",
        _ => "?",
    };

    public static string EncodeGroupKey(object?[] values)
    {
        if (values.Length == 0) return "∅";
        return string.Join("", values.Select(EncodeScalar));
    }
}
