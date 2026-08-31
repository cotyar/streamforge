using StreamsForge.Abstractions;
using StreamsForge.Engine;
using StreamsForge.Engine.Runtime;

namespace StreamsForge.AppCore.Ingest;

/// <summary>
/// Non-throwing scalar coercion, extracted from <c>ProtoWireEncoder</c> (plan 008 W4) so every row-
/// shaping path — proto wire encoding, connector mapping extraction, and client-push ingest row
/// acceptance — agrees on what each <see cref="FieldType"/> accepts. <c>ProtoWireEncoder</c> still
/// throws on a bad value (it encodes already-accepted rows, so a bad value there is a bug, not a
/// client error); ingest row acceptance turns a <c>false</c> return into a per-row 400 instead.
///
/// <para>Plan 009 C1: the conversions themselves moved DOWN into the Engine
/// (<see cref="FieldValueConversion"/>) once the SQL dialect gained <c>TO_LONG</c>/<c>CAST</c> and
/// needed exactly the same rules. AppCore already references the Engine, so there is one
/// implementation rather than two that agree by convention — what remains here is the
/// <see cref="FieldType"/> → <see cref="FieldKind"/> adapter, because the Engine deliberately knows
/// nothing about the Contracts assembly. If these rules ever need to change, change them there:
/// a change is visible in SQL and on every inbound path at once, which is the point.</para>
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
        if (ToFieldKind(type) is not { } kind)
        {
            // Preserves the pre-009 default arm exactly: a value outside the enum is "no conversion
            // applies", not "stringify it". Only reachable via a cast of an out-of-range int.
            coerced = null;
            return false;
        }

        return FieldValueConversion.TryCoerce(kind, value, out coerced);
    }

    /// <summary>The two enums are parallel by construction (same six members, same meanings); they are
    /// separate types only because the Engine deliberately does not depend on Contracts. Null for a
    /// value that is not a declared member — see the caller.</summary>
    private static FieldKind? ToFieldKind(FieldType type) => type switch
    {
        FieldType.String => FieldKind.String,
        FieldType.Double => FieldKind.Double,
        FieldType.Long => FieldKind.Long,
        FieldType.Bool => FieldKind.Bool,
        FieldType.Timestamp => FieldKind.Timestamp,
        FieldType.Json => FieldKind.Json,
        _ => null,
    };
}
