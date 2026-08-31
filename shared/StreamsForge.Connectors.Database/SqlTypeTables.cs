using StreamsForge.Abstractions;

namespace StreamsForge.Connectors.Database;

/// <summary>
/// The two type tables and the one fallback both dialects share, kept out of the dialect classes so the
/// tables read as tables — the thing a reader actually wants to check against their own schema.
///
/// <para><b>Everything unmapped becomes <see cref="FieldType.String"/> deliberately.</b> A <c>uuid</c>, an
/// enum, a <c>bytea</c> or a domain type the platform has never seen arrives as text and stays readable;
/// the alternative, refusing to probe a table because one column is exotic, would make discovery useless
/// on exactly the tables that need it most.</para>
///
/// <para><b>The one lossy mapping is named rather than hidden.</b> This platform has six field types and
/// none of them is exact decimal, so <c>numeric</c>/<c>decimal</c>/<c>money</c> map to
/// <see cref="FieldType.Double"/> and lose precision beyond ~15 significant digits. The probe reports that
/// per column; the workaround (<c>CAST(x AS text)</c> in a <c>Query</c>) is in the note itself, because a
/// diagnostic an operator cannot act on is just noise.</para>
/// </summary>
internal static class SqlTypeTables
{
    /// <summary>Appended to every lossy mapping's note. Written once so both dialects say the same
    /// sentence — an operator reading two probes should not have to decide whether two wordings mean two
    /// different things.</summary>
    private const string PrecisionAdvice =
        "maps to Double and loses precision beyond ~15 significant digits; select it as CAST(x AS text) in a Query to keep it exact";

    private static readonly Dictionary<string, FieldType> Postgres = new(StringComparer.Ordinal)
    {
        ["smallint"] = FieldType.Long, ["int2"] = FieldType.Long, ["smallserial"] = FieldType.Long, ["serial2"] = FieldType.Long,
        ["integer"] = FieldType.Long, ["int"] = FieldType.Long, ["int4"] = FieldType.Long, ["serial"] = FieldType.Long, ["serial4"] = FieldType.Long,
        ["bigint"] = FieldType.Long, ["int8"] = FieldType.Long, ["bigserial"] = FieldType.Long, ["serial8"] = FieldType.Long,

        ["real"] = FieldType.Double, ["float4"] = FieldType.Double,
        ["double precision"] = FieldType.Double, ["float8"] = FieldType.Double, ["float"] = FieldType.Double,
        ["numeric"] = FieldType.Double, ["decimal"] = FieldType.Double, ["money"] = FieldType.Double,

        ["boolean"] = FieldType.Bool, ["bool"] = FieldType.Bool,

        ["timestamp"] = FieldType.Timestamp,
        ["timestamp without time zone"] = FieldType.Timestamp,
        ["timestamp with time zone"] = FieldType.Timestamp,
        ["timestamptz"] = FieldType.Timestamp,
        ["date"] = FieldType.Timestamp,
        ["time"] = FieldType.Timestamp,
        ["time without time zone"] = FieldType.Timestamp,
        ["time with time zone"] = FieldType.Timestamp,
        ["timetz"] = FieldType.Timestamp,

        ["json"] = FieldType.Json, ["jsonb"] = FieldType.Json, ["hstore"] = FieldType.Json, ["record"] = FieldType.Json,
    };

    private static readonly Dictionary<string, FieldType> SqlServer = new(StringComparer.Ordinal)
    {
        ["tinyint"] = FieldType.Long, ["smallint"] = FieldType.Long, ["int"] = FieldType.Long, ["bigint"] = FieldType.Long,

        ["real"] = FieldType.Double, ["float"] = FieldType.Double,
        ["decimal"] = FieldType.Double, ["numeric"] = FieldType.Double,
        ["money"] = FieldType.Double, ["smallmoney"] = FieldType.Double,

        ["bit"] = FieldType.Bool,

        ["date"] = FieldType.Timestamp, ["datetime"] = FieldType.Timestamp, ["datetime2"] = FieldType.Timestamp,
        ["smalldatetime"] = FieldType.Timestamp, ["datetimeoffset"] = FieldType.Timestamp, ["time"] = FieldType.Timestamp,

        // nvarchar/varchar/char/nchar/text/uniqueidentifier/varbinary/binary/xml/sql_variant all fall
        // through to String below — listing them would only be a second place to keep in sync.
    };

    /// <summary>Type names whose mapping to <see cref="FieldType.Double"/> is lossy. Same set in both
    /// dialects, which is why it is one set.</summary>
    private static readonly HashSet<string> Lossy = new(StringComparer.Ordinal)
    {
        "numeric", "decimal", "money", "smallmoney",
    };

    public static TypeMapping MapPostgres(string? dataTypeName, Type? clrType)
    {
        var name = Normalize(dataTypeName);

        // A PostgreSQL array of ANY element type is JSON here — the platform has no array-of-scalar
        // column concept at probe time (FieldDef.IsArray exists but nothing consumes it from a probe),
        // and a rendered JSON array is the shape the -> operators in the SQL dialect can actually reach.
        if (name.EndsWith("[]", StringComparison.Ordinal))
        {
            return new TypeMapping(FieldType.Json);
        }

        return Lookup(Postgres, name, clrType);
    }

    public static TypeMapping MapSqlServer(string? dataTypeName, Type? clrType)
        => Lookup(SqlServer, Normalize(dataTypeName), clrType);

    private static TypeMapping Lookup(Dictionary<string, FieldType> table, string name, Type? clrType)
    {
        if (table.TryGetValue(name, out var mapped))
        {
            return new TypeMapping(mapped, Lossy.Contains(name) ? $"{name} {PrecisionAdvice}" : null);
        }

        // Unknown NAME but a CLR type the driver already resolved — the case the plan's own machine
        // check leaned on (bigint→Int64, decimal→Decimal, datetimeoffset→DateTimeOffset come off the
        // reader directly). A composite/domain/enum type lands here and, correctly, stays String.
        return new TypeMapping(FromClr(clrType), clrType == typeof(decimal) ? $"decimal {PrecisionAdvice}" : null);
    }

    /// <summary>The driver-agnostic fallback: what the CLR type alone implies.</summary>
    public static FieldType FromClr(Type? clrType) => clrType switch
    {
        null => FieldType.String,
        _ when clrType == typeof(bool) => FieldType.Bool,
        _ when clrType == typeof(byte) || clrType == typeof(sbyte) => FieldType.Long,
        _ when clrType == typeof(short) || clrType == typeof(ushort) => FieldType.Long,
        _ when clrType == typeof(int) || clrType == typeof(uint) => FieldType.Long,
        _ when clrType == typeof(long) || clrType == typeof(ulong) => FieldType.Long,
        _ when clrType == typeof(float) || clrType == typeof(double) || clrType == typeof(decimal) => FieldType.Double,
        _ when clrType == typeof(DateTime) || clrType == typeof(DateTimeOffset) => FieldType.Timestamp,
        _ when clrType == typeof(DateOnly) || clrType == typeof(TimeOnly) || clrType == typeof(TimeSpan) => FieldType.Timestamp,
        _ => FieldType.String,
    };

    /// <summary>Lowercases, drops a length/precision suffix (<c>numeric(19,4)</c> → <c>numeric</c>) and
    /// collapses inner whitespace, so <c>TIMESTAMP  WITH TIME ZONE</c> and <c>timestamp with time
    /// zone</c> are one key. Preserves a trailing <c>[]</c>, which is load-bearing above.</summary>
    private static string Normalize(string? dataTypeName)
    {
        if (string.IsNullOrWhiteSpace(dataTypeName))
        {
            return "";
        }

        var name = dataTypeName.Trim().ToLowerInvariant();
        var array = name.EndsWith("[]", StringComparison.Ordinal);
        if (array)
        {
            name = name[..^2];
        }

        var paren = name.IndexOf('(', StringComparison.Ordinal);
        if (paren >= 0)
        {
            name = name[..paren];
        }

        name = string.Join(' ', name.Split(' ', StringSplitOptions.RemoveEmptyEntries)).Trim();
        return array ? name + "[]" : name;
    }
}
