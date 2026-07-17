namespace StreamForge.Engine.Runtime;

/// <summary>Shared numeric/ordering helpers implementing the dialect's value semantics:
/// long/double promotion on mixed arithmetic, ordinal string comparison.</summary>
internal static class SqlValues
{
    public static bool IsNumber(object? v) => v is long or double;

    public static double ToDouble(object v) => v switch
    {
        double d => d,
        long l => l,
        _ => 0d,
    };

    /// <summary>Compares two same-domain non-null values (numbers promoted, strings ordinal, bools by value).</summary>
    public static int Compare(object a, object b)
    {
        if (a is string sa && b is string sb) return string.CompareOrdinal(sa, sb);
        if (a is bool ba && b is bool bb) return ba.CompareTo(bb);
        if (IsNumber(a) && IsNumber(b)) return ToDouble(a).CompareTo(ToDouble(b));
        return 0;
    }
}
