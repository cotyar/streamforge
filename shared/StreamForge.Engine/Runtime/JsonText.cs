using System.Globalization;
using System.Text;

namespace StreamForge.Engine.Runtime;

/// <summary>Compact JSON text rendering for the dialect's JSON value domain (Dictionary&lt;string,object?&gt;,
/// List&lt;object?&gt;, and primitive leaves). Used by `->>` when the accessed node is itself a dict/list —
/// Postgres's `->>` returns the JSON text of composite values rather than erroring.</summary>
internal static class JsonText
{
    public static string Serialize(object? value)
    {
        var sb = new StringBuilder();
        Write(sb, value);
        return sb.ToString();
    }

    /// <summary>Canonical (key-sorted) JSON object text for a row's fields — used by the table (Z-set)
    /// runtime as the multiset key: two rows with identical field values must serialize identically
    /// regardless of dictionary insertion order.</summary>
    internal static string SerializeCanonicalRow(IReadOnlyDictionary<string, object?> fields)
    {
        var sb = new StringBuilder();
        sb.Append('{');
        bool first = true;
        foreach (var kv in fields.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            if (!first) sb.Append(',');
            first = false;
            WriteString(sb, kv.Key);
            sb.Append(':');
            Write(sb, kv.Value);
        }
        sb.Append('}');
        return sb.ToString();
    }

    private static void Write(StringBuilder sb, object? value)
    {
        switch (value)
        {
            case null:
                sb.Append("null");
                break;
            case string s:
                WriteString(sb, s);
                break;
            case bool b:
                sb.Append(b ? "true" : "false");
                break;
            case long l:
                sb.Append(l.ToString(CultureInfo.InvariantCulture));
                break;
            case double d:
                sb.Append(d.ToString(CultureInfo.InvariantCulture));
                break;
            case Dictionary<string, object?> dict:
                sb.Append('{');
                bool firstEntry = true;
                foreach (var kv in dict)
                {
                    if (!firstEntry) sb.Append(',');
                    firstEntry = false;
                    WriteString(sb, kv.Key);
                    sb.Append(':');
                    Write(sb, kv.Value);
                }
                sb.Append('}');
                break;
            case List<object?> list:
                sb.Append('[');
                for (int i = 0; i < list.Count; i++)
                {
                    if (i > 0) sb.Append(',');
                    Write(sb, list[i]);
                }
                sb.Append(']');
                break;
            default:
                // Unreachable for well-formed JSON values (see PublicApi.cs's value-domain note); fail soft.
                sb.Append("null");
                break;
        }
    }

    private static void WriteString(StringBuilder sb, string s)
    {
        sb.Append('"');
        foreach (var c in s)
        {
            switch (c)
            {
                case '"': sb.Append("\\\""); break;
                case '\\': sb.Append("\\\\"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (c < 0x20) sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    else sb.Append(c);
                    break;
            }
        }
        sb.Append('"');
    }
}
