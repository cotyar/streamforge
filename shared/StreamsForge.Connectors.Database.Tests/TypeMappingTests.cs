using StreamsForge.Abstractions;
using StreamsForge.Connectors.Database;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests;

/// <summary>
/// The two type tables, as tables. Written out case by case rather than reflected over the dictionaries
/// they come from — a test that derives its expectations from the implementation asserts only that the
/// implementation equals itself.
/// </summary>
public class TypeMappingTests
{
    private static readonly PostgresDialect Pg = new();
    private static readonly SqlServerDialect Ms = new();

    [Theory]
    [InlineData("smallint", FieldType.Long)]
    [InlineData("int2", FieldType.Long)]
    [InlineData("integer", FieldType.Long)]
    [InlineData("int4", FieldType.Long)]
    [InlineData("bigint", FieldType.Long)]
    [InlineData("int8", FieldType.Long)]
    [InlineData("serial", FieldType.Long)]
    [InlineData("bigserial", FieldType.Long)]
    [InlineData("real", FieldType.Double)]
    [InlineData("float4", FieldType.Double)]
    [InlineData("double precision", FieldType.Double)]
    [InlineData("float8", FieldType.Double)]
    [InlineData("numeric", FieldType.Double)]
    [InlineData("money", FieldType.Double)]
    [InlineData("boolean", FieldType.Bool)]
    [InlineData("bool", FieldType.Bool)]
    [InlineData("timestamp without time zone", FieldType.Timestamp)]
    [InlineData("timestamp with time zone", FieldType.Timestamp)]
    [InlineData("timestamptz", FieldType.Timestamp)]
    [InlineData("date", FieldType.Timestamp)]
    [InlineData("time without time zone", FieldType.Timestamp)]
    [InlineData("json", FieldType.Json)]
    [InlineData("jsonb", FieldType.Json)]
    [InlineData("hstore", FieldType.Json)]
    [InlineData("integer[]", FieldType.Json)]
    [InlineData("text[]", FieldType.Json)]
    [InlineData("uuid", FieldType.String)]
    [InlineData("text", FieldType.String)]
    [InlineData("character varying", FieldType.String)]
    [InlineData("bytea", FieldType.String)]
    [InlineData("order_status", FieldType.String)]
    public void PostgresTypeTable(string dataTypeName, FieldType expected)
        => Assert.Equal(expected, Pg.MapType(dataTypeName, clrType: null).Type);

    [Theory]
    [InlineData("tinyint", FieldType.Long)]
    [InlineData("smallint", FieldType.Long)]
    [InlineData("int", FieldType.Long)]
    [InlineData("bigint", FieldType.Long)]
    [InlineData("real", FieldType.Double)]
    [InlineData("float", FieldType.Double)]
    [InlineData("decimal", FieldType.Double)]
    [InlineData("numeric", FieldType.Double)]
    [InlineData("money", FieldType.Double)]
    [InlineData("smallmoney", FieldType.Double)]
    [InlineData("bit", FieldType.Bool)]
    [InlineData("date", FieldType.Timestamp)]
    [InlineData("datetime", FieldType.Timestamp)]
    [InlineData("datetime2", FieldType.Timestamp)]
    [InlineData("smalldatetime", FieldType.Timestamp)]
    [InlineData("datetimeoffset", FieldType.Timestamp)]
    [InlineData("time", FieldType.Timestamp)]
    [InlineData("nvarchar", FieldType.String)]
    [InlineData("varchar", FieldType.String)]
    [InlineData("char", FieldType.String)]
    [InlineData("uniqueidentifier", FieldType.String)]
    [InlineData("varbinary", FieldType.String)]
    [InlineData("xml", FieldType.String)]
    [InlineData("sql_variant", FieldType.String)]
    public void SqlServerTypeTable(string dataTypeName, FieldType expected)
        => Assert.Equal(expected, Ms.MapType(dataTypeName, clrType: null).Type);

    [Theory]
    [InlineData("numeric")]
    [InlineData("decimal")]
    [InlineData("money")]
    public void ExactDecimalTypesReportTheirPrecisionLossInsteadOfRoundingSilently(string dataTypeName)
    {
        // The platform has no exact decimal FieldType. Reporting it at discovery time is the whole
        // difference between an operator who knows and one who finds out from a reconciliation break.
        foreach (var mapped in new[] { Pg.MapType(dataTypeName, null), Ms.MapType(dataTypeName, null) })
        {
            Assert.Equal(FieldType.Double, mapped.Type);
            Assert.NotNull(mapped.Note);
            Assert.Contains("loses precision", mapped.Note, StringComparison.Ordinal);
            // The note has to carry the workaround, or it is just noise.
            Assert.Contains("CAST(x AS text)", mapped.Note, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void NonLossyMappingsCarryNoNote()
    {
        Assert.Null(Pg.MapType("bigint", null).Note);
        Assert.Null(Pg.MapType("double precision", null).Note);
        Assert.Null(Ms.MapType("float", null).Note);
    }

    [Theory]
    [InlineData("numeric(19,4)", FieldType.Double)]
    [InlineData("NUMERIC(19, 4)", FieldType.Double)]
    [InlineData("TIMESTAMP  WITH TIME ZONE", FieldType.Timestamp)]
    [InlineData("nvarchar(200)", FieldType.String)]
    public void TypeNamesAreNormalizedBeforeLookup(string dataTypeName, FieldType expected)
    {
        Assert.Equal(expected, Pg.MapType(dataTypeName, null).Type);
    }

    [Fact]
    public void AnUnknownTypeNameFallsBackToTheClrTypeTheDriverAlreadyResolved()
    {
        // GetSchemaTableAsync hands back CLR types directly (bigint→Int64, decimal→Decimal,
        // datetimeoffset→DateTimeOffset), which is what makes a domain or composite type still probe.
        Assert.Equal(FieldType.Long, Pg.MapType("some_domain", typeof(long)).Type);
        Assert.Equal(FieldType.Timestamp, Pg.MapType("some_domain", typeof(DateTimeOffset)).Type);
        Assert.Equal(FieldType.Bool, Ms.MapType("unheard_of", typeof(bool)).Type);
        Assert.Equal(FieldType.String, Ms.MapType("unheard_of", typeof(Guid)).Type);

        // Even through the fallback the precision loss is still reported.
        var mapped = Ms.MapType("unheard_of", typeof(decimal));
        Assert.Equal(FieldType.Double, mapped.Type);
        Assert.NotNull(mapped.Note);
    }

    [Fact]
    public void NothingKnownAtAllIsStringRatherThanARefusalToProbe()
    {
        Assert.Equal(FieldType.String, Pg.MapType(null, null).Type);
        Assert.Equal(FieldType.String, Ms.MapType("", null).Type);
    }
}
