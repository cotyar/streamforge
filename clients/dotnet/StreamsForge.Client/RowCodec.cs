using System.Text.Json;
using Google.Protobuf.WellKnownTypes;

namespace StreamsForge.Client;

/// <summary>
/// The single place a wire row becomes the <c>IReadOnlyDictionary&lt;string, object?&gt;</c> that
/// the rest of this client (the Z-set reducer, <see cref="LiveTable"/>, user code) works with.
/// Kept as one function per wire shape rather than scattered across the transports, specifically
/// so a future typed-row path (Tier 3 codegen off a per-table <c>GET /api/tables/{id}/proto</c>)
/// only has to replace what happens HERE -- the reducer and everything above it are written
/// against the row shape, not against Struct or JsonElement.
///
/// HAZARD -- read this before "fixing" a column's type. <see cref="Struct"/>'s <c>NumberValue</c>
/// is always an IEEE-754 double. The engine's own GrpcValueConverter deliberately serializes a
/// <c>long</c> beyond 2^53 as a Struct STRING instead of losing precision, so the identical
/// logical column can arrive as a <c>double</c> on most rows and a <c>string</c> on the rare row
/// whose value is large (nothing in the reference demo crosses this today -- epoch-ms tops out
/// around 1.7e12 -- but a raw counter or a hashed id could). Any code downstream that assumes one
/// CLR type per column for a Struct-sourced row is wrong on real data; this codec does not paper
/// over it by guessing -- it passes the value through exactly as the wire sent it.
/// </summary>
internal static class RowCodec
{
    // ---- gRPC (Tier 1): google.protobuf.Struct -> row ----

    public static IReadOnlyDictionary<string, object?> FromStruct(Struct s)
    {
        var result = new Dictionary<string, object?>(s.Fields.Count);
        foreach (var (key, value) in s.Fields) result[key] = FromValue(value);
        return result;
    }

    private static object? FromValue(Value v) => v.KindCase switch
    {
        Value.KindOneofCase.NullValue => null,
        Value.KindOneofCase.NumberValue => v.NumberValue, // see the class hazard note
        Value.KindOneofCase.StringValue => v.StringValue,
        Value.KindOneofCase.BoolValue => v.BoolValue,
        Value.KindOneofCase.StructValue => FromStruct(v.StructValue),
        Value.KindOneofCase.ListValue => v.ListValue.Values.Select(FromValue).ToList(),
        _ => null,
    };

    public static Struct ToStruct(IReadOnlyDictionary<string, object?> row)
    {
        var s = new Struct();
        foreach (var (k, v) in row) s.Fields[k] = ToValue(v);
        return s;
    }

    private static Value ToValue(object? v) => v switch
    {
        null => Value.ForNull(),
        bool b => Value.ForBool(b),
        string s => Value.ForString(s),
        double d => Value.ForNumber(d),
        long l => Value.ForNumber(l),
        int i => Value.ForNumber(i),
        IReadOnlyDictionary<string, object?> dict => Value.ForStruct(ToStruct(dict)),
        System.Collections.IEnumerable list => Value.ForList(list.Cast<object?>().Select(ToValue).ToArray()),
        _ => throw new StreamsForgeException($"unsupported ingest row value type '{v.GetType()}'"),
    };

    // ---- SignalR / REST: System.Text.Json JsonElement -> row ----
    //
    // A structurally different wire shape from Struct (plain JSON, not protobuf), but the same
    // conceptual seam: one function, called from exactly one place per transport, so a future
    // typed-row path can replace this half too without touching the reducer.

    public static IReadOnlyDictionary<string, object?> FromJson(JsonElement e)
    {
        var result = new Dictionary<string, object?>();
        foreach (var prop in e.EnumerateObject()) result[prop.Name] = FromJsonValue(prop.Value);
        return result;
    }

    private static object? FromJsonValue(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.String => e.GetString(),
        JsonValueKind.Number => e.TryGetInt64(out var l) ? l : e.GetDouble(),
        JsonValueKind.Object => FromJson(e),
        JsonValueKind.Array => e.EnumerateArray().Select(FromJsonValue).ToList(),
        _ => null,
    };
}
