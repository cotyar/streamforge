using StreamsForge.Abstractions;
using StreamsForge.AppCore.Transports;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests.Integration;

/// <summary>
/// <see cref="PostgresTypeProbeTests"/>' opposite number: one column of every type
/// <c>SqlTypeTables.MapSqlServer</c> claims, probed and then read off a real SQL Server.
///
/// <para><b>Why this cannot be the same test as PostgreSQL's.</b> The type LISTS genuinely differ — there
/// is no <c>jsonb</c> and no array type here, and there are four exact-decimal types (<c>decimal</c>,
/// <c>numeric</c>, <c>money</c>, <c>smallmoney</c>) where PostgreSQL has three — so a shared "every type"
/// test would either test the intersection, which is the uninteresting part, or grow a per-dialect table
/// that is this file with extra indirection.</para>
///
/// <para><b>Four precision diagnostics, not one.</b> Every exact-decimal type maps to Double and says so;
/// an operator storing money in <c>money</c> deserves the same warning as one storing it in
/// <c>decimal</c>.</para>
/// </summary>
[Collection(DbServers.CollectionName)]
public sealed class MsSqlTypeProbeTests(DbServers servers)
{
    private static readonly DbBackend Backend = DbBackends.SqlServer;

    private readonly DbServers _servers = servers;

    [MsSqlFact]
    public async Task ProbeMapsEveryDocumentedTypeAndNamesEveryLossyOne()
    {
        var table = await SeedAsync().ConfigureAwait(false);
        var probe = (ISchemaProbe)new DbSource(Backend.Dialect);

        var result = await probe.ProbeAsync(Definition(table), CancellationToken.None).ConfigureAwait(false);
        var fields = result.Fields.ToDictionary(f => f.Name, f => f.Type, StringComparer.Ordinal);

        Assert.Equal(FieldType.Long, fields["id"]);
        Assert.Equal(FieldType.Long, fields["c_tinyint"]);
        Assert.Equal(FieldType.Long, fields["c_smallint"]);
        Assert.Equal(FieldType.Long, fields["c_int"]);
        Assert.Equal(FieldType.Long, fields["c_bigint"]);
        Assert.Equal(FieldType.Double, fields["c_real"]);
        Assert.Equal(FieldType.Double, fields["c_float"]);
        Assert.Equal(FieldType.Double, fields["c_decimal"]);
        Assert.Equal(FieldType.Double, fields["c_numeric"]);
        Assert.Equal(FieldType.Double, fields["c_money"]);
        Assert.Equal(FieldType.Double, fields["c_smallmoney"]);
        Assert.Equal(FieldType.Bool, fields["c_bit"]);
        Assert.Equal(FieldType.Timestamp, fields["c_date"]);
        Assert.Equal(FieldType.Timestamp, fields["c_datetime"]);
        Assert.Equal(FieldType.Timestamp, fields["c_datetime2"]);
        Assert.Equal(FieldType.Timestamp, fields["c_smalldatetime"]);
        Assert.Equal(FieldType.Timestamp, fields["c_datetimeoffset"]);
        Assert.Equal(FieldType.Timestamp, fields["c_time"]);
        Assert.Equal(FieldType.String, fields["c_nvarchar"]);
        Assert.Equal(FieldType.String, fields["c_varchar"]);
        Assert.Equal(FieldType.String, fields["c_char"]);
        Assert.Equal(FieldType.String, fields["c_uid"]);
        Assert.Equal(FieldType.String, fields["c_varbinary"]);
        Assert.Equal(FieldType.String, fields["c_xml"]);

        foreach (var column in new[] { "c_decimal", "c_numeric", "c_money", "c_smallmoney" })
        {
            var note = Assert.Single(result.Diagnostics, d => d.StartsWith(column + ":", StringComparison.Ordinal));
            Assert.Contains("loses precision", note, StringComparison.Ordinal);
        }
    }

    [MsSqlFact]
    public async Task ReadingThoseColumnsProducesTheDocumentedValues()
    {
        var table = await SeedAsync().ConfigureAwait(false);
        var batch = await new DbSource(Backend.Dialect)
            .PollAsync(Definition(table), null, CancellationToken.None).ConfigureAwait(false);

        var row = Assert.Single(batch.Rows);

        Assert.Equal((byte)200, row["c_tinyint"]);
        Assert.Equal((short)32000, row["c_smallint"]);
        Assert.Equal(2000000000, row["c_int"]);
        Assert.Equal(9000000000000000000L, row["c_bigint"]);
        Assert.Equal(1.5f, row["c_real"]);
        Assert.Equal(2.25d, row["c_float"]);
        Assert.Equal(1234.5678m, row["c_decimal"]);
        Assert.Equal(12.34m, row["c_numeric"]);
        Assert.Equal(19.99m, row["c_money"]);
        Assert.Equal(1.99m, row["c_smallmoney"]);
        Assert.Equal(true, row["c_bit"]);

        // datetime2 has no zone; datetimeoffset carries one, and keeps it.
        var plain = Assert.IsType<DateTime>(row["c_datetime2"]);
        Assert.Equal(DateTimeKind.Unspecified, plain.Kind);
        var offset = Assert.IsType<DateTimeOffset>(row["c_datetimeoffset"]);
        Assert.Equal(TimeSpan.FromHours(2), offset.Offset);

        // No CLR representation among the six field types, so these are converted.
        Assert.Equal("09:30:00", row["c_time"]);
        Assert.Equal("00000000-0000-0000-0000-000000000001", row["c_uid"]);
        Assert.Equal("AQID", row["c_varbinary"]);

        Assert.Equal("nvarchar", row["c_nvarchar"]);
        Assert.Equal("varchar", row["c_varchar"]);
        Assert.Equal("abc", row["c_char"]);
        Assert.Equal("<r><a>1</a></r>", row["c_xml"]);
    }

    private static SourceDefinition Definition(string table)
        => Backend.Definition(Backend.SourceConfig(table, c => { c.Snapshot = true; c.BatchSize = 10; }));

    private static async Task<string> SeedAsync()
    {
        var table = Backend.NewTable("mstypes");
        var quoted = $"{Backend.Dialect.QuoteIdent(Backend.Dialect.DefaultSchema)}.{Backend.Dialect.QuoteIdent(table)}";
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);

        await Sql.ExecAsync(connection, $"""
            CREATE TABLE {quoted} (
              id bigint NOT NULL PRIMARY KEY,
              c_tinyint tinyint, c_smallint smallint, c_int int, c_bigint bigint,
              c_real real, c_float float, c_decimal decimal(19,4), c_numeric numeric(10,2),
              c_money money, c_smallmoney smallmoney,
              c_bit bit,
              c_date date, c_datetime datetime, c_datetime2 datetime2,
              c_smalldatetime smalldatetime, c_datetimeoffset datetimeoffset, c_time time,
              c_nvarchar nvarchar(50), c_varchar varchar(50), c_char char(3),
              c_uid uniqueidentifier, c_varbinary varbinary(16), c_xml xml)
            """).ConfigureAwait(false);

        await Sql.ExecAsync(connection, $"""
            INSERT INTO {quoted} VALUES (
              1, 200, 32000, 2000000000, 9000000000000000000,
              1.5, 2.25, 1234.5678, 12.34,
              19.99, 1.99,
              1,
              '2026-08-14', '2026-08-14T09:30:00', '2026-08-14T09:30:00',
              '2026-08-14T09:30:00', '2026-08-14T09:30:00+02:00', '09:30:00',
              N'nvarchar', 'varchar', 'abc',
              '00000000-0000-0000-0000-000000000001', 0x010203,
              '<r><a>1</a></r>')
            """).ConfigureAwait(false);

        return table;
    }
}
