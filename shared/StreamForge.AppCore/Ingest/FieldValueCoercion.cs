using System.Globalization;
using StreamForge.Abstractions;

namespace StreamForge.AppCore.Ingest;

/// <summary>
/// Non-throwing scalar coercion, extracted from <c>ProtoWireEncoder</c> (plan 008 W4) so every row-
/// shaping path — proto wire encoding, connector mapping extraction, and client-push ingest row
/// acceptance — agrees on what each <see cref="FieldType"/> accepts. <c>ProtoWireEncoder</c> still
/// throws on a bad value (it encodes already-accepted rows, so a bad value there is a bug, not a
/// client error); ingest row acceptance turns a <c>false</c> return into a per-row 400 instead.
/// </summary>
public static class FieldValueCoercion
{
    /// <summary>Coerces one already-JSON-normalized leaf value (no <c>JsonElement</c> — run it
    /// through <c>JsonValueNormalizer</c> first) to the CLR shape <paramref name="type"/> expects on
    /// the wire. <see cref="FieldType.Json"/> is structural (nested message / <c>Struct</c>), not a
    /// value conversion, so it always succeeds by passing the value through unchanged — validating
    /// its shape is the caller's job.</summary>
    public static bool TryCoerce(FieldType type, object value, out object? coerced)
    {
        switch (type)
        {
            case FieldType.String:
                coerced = ToStringValue(value);
                return true;
            case FieldType.Double:
                return TryToDouble(value, out coerced);
            case FieldType.Long:
            case FieldType.Timestamp: // epoch millis - identical wire/CLR representation to Long
                return TryToInt64(value, out coerced);
            case FieldType.Bool:
                return TryToBool(value, out coerced);
            case FieldType.Json:
                coerced = value;
                return true;
            default:
                coerced = null;
                return false;
        }
    }

    private static string ToStringValue(object value) => value switch
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

    private static bool TryToInt64(object value, out object? coerced)
    {
        switch (value)
        {
            case long l: coerced = l; return true;
            case int i: coerced = (long)i; return true;
            case double d: coerced = (long)d; return true;
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
