using StreamsForge.Abstractions;
using StreamsForge.AppCore.Transports;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests.Integration;

/// <summary>
/// <c>SqlTypeTables</c> is a paper table until a server produces the type names it keys off. This is where
/// the two meet: a PostgreSQL table carrying one column of every type the connector claims to map, probed
/// through <c>ISchemaProbe</c> and then READ through <c>PollAsync</c>, so both halves of the claim — "this
/// is the field type" and "this is the value you will get" — are checked against the same real column.
///
/// <para><b>The value half matters as much as the type half.</b> A <c>bytea</c> that arrives as a
/// <c>byte[]</c>, a <c>uuid</c> as a <see cref="Guid"/> and an <c>integer[]</c> as an <c>int[]</c> have no
/// representation in the platform's six field types, so <c>DbSource.Cell</c> converts them — and a
/// conversion nobody has ever watched happen against a driver is a conversion that is probably wrong about
/// which CLR type the driver actually hands over.</para>
/// </summary>
[Collection(DbServers.CollectionName)]
public sealed class PostgresTypeProbeTests(DbServers servers)
{
    private static readonly DbBackend Backend = DbBackends.Postgres;

    private readonly DbServers _servers = servers;

    /// <summary>Every mapped PostgreSQL type, in one table, with the field type the connector documents
    /// for it. The unmapped-but-common ones (<c>varchar</c>, <c>char</c>, <c>uuid</c>, <c>bytea</c>) are
    /// here too: they are SUPPOSED to fall through to String rather than make the probe fail, and that is
    /// a claim worth pinning.</summary>
    [PostgresFact]
    public async Task ProbeMapsEveryDocumentedTypeAndNamesTheOneLossyOne()
    {
        var table = await SeedAsync().ConfigureAwait(false);
        var probe = (ISchemaProbe)new DbSource(Backend.Dialect);

        var result = await probe.ProbeAsync(Definition(table), CancellationToken.None).ConfigureAwait(false);
        var fields = result.Fields.ToDictionary(f => f.Name, f => f.Type, StringComparer.Ordinal);

        Assert.Equal(FieldType.Long, fields["id"]);
        Assert.Equal(FieldType.Long, fields["c_smallint"]);
        Assert.Equal(FieldType.Long, fields["c_integer"]);
        Assert.Equal(FieldType.Long, fields["c_bigint"]);
        Assert.Equal(FieldType.Double, fields["c_real"]);
        Assert.Equal(FieldType.Double, fields["c_double"]);
        Assert.Equal(FieldType.Double, fields["c_numeric"]);
        Assert.Equal(FieldType.Bool, fields["c_bool"]);
        Assert.Equal(FieldType.Timestamp, fields["c_timestamp"]);
        Assert.Equal(FieldType.Timestamp, fields["c_timestamptz"]);
        Assert.Equal(FieldType.Timestamp, fields["c_date"]);
        Assert.Equal(FieldType.Timestamp, fields["c_time"]);
        Assert.Equal(FieldType.Json, fields["c_json"]);
        Assert.Equal(FieldType.Json, fields["c_jsonb"]);
        Assert.Equal(FieldType.String, fields["c_text"]);
        Assert.Equal(FieldType.String, fields["c_varchar"]);
        Assert.Equal(FieldType.String, fields["c_char"]);
        Assert.Equal(FieldType.String, fields["c_uuid"]);
        Assert.Equal(FieldType.String, fields["c_bytea"]);
        Assert.Equal(FieldType.Json, fields["c_intarray"]);

        // The one mapping that loses something an operator can act on, reported rather than rounded away.
        var precision = Assert.Single(result.Diagnostics, d => d.StartsWith("c_numeric:", StringComparison.Ordinal));
        Assert.Contains("loses precision", precision, StringComparison.Ordinal);
        Assert.Contains("CAST(x AS text)", precision, StringComparison.Ordinal);
    }

    /// <summary>...and what the same columns are worth once <c>PollAsync</c> has read them. Everything
    /// with a CLR representation the six field types can hold stays as it is; everything else — bytes,
    /// GUIDs, dates, arrays — is converted, and this is the record of what those conversions produce.</summary>
    [PostgresFact]
    public async Task ReadingThoseColumnsProducesTheDocumentedValues()
    {
        var table = await SeedAsync().ConfigureAwait(false);
        var batch = await new DbSource(Backend.Dialect)
            .PollAsync(Definition(table), null, CancellationToken.None).ConfigureAwait(false);

        var row = Assert.Single(batch.Rows);

        Assert.Equal((short)32000, row["c_smallint"]);
        Assert.Equal(2000000000, row["c_integer"]);
        Assert.Equal(9000000000000000000L, row["c_bigint"]);
        Assert.Equal(1.5f, row["c_real"]);
        Assert.Equal(2.25d, row["c_double"]);
        Assert.Equal(1234.5678m, row["c_numeric"]);
        Assert.Equal(true, row["c_bool"]);

        // timestamp is zoneless and timestamptz is not — the distinction DbCursor preserves one bit for.
        var local = Assert.IsType<DateTime>(row["c_timestamp"]);
        Assert.Equal(DateTimeKind.Unspecified, local.Kind);
        var utc = Assert.IsType<DateTime>(row["c_timestamptz"]);
        Assert.Equal(DateTimeKind.Utc, utc.Kind);

        // No CLR representation among the six field types, so these are converted.
        Assert.Equal("2026-08-14", row["c_date"]);
        Assert.Equal("09:30:00.0000000", row["c_time"]);
        Assert.Equal("00000000-0000-0000-0000-000000000001", row["c_uuid"]);
        Assert.Equal("AQID", row["c_bytea"]);
        Assert.Equal("[1,2,3]", row["c_intarray"]);

        // json/jsonb come off the driver as text already — passed through untouched, not re-serialized.
        Assert.Equal("{\"a\": 1}", row["c_json"]);
        Assert.Equal("{\"b\": 2}", row["c_jsonb"]);
        Assert.Equal("text", row["c_text"]);
        Assert.Equal("varchar", row["c_varchar"]);
        Assert.Equal("abc", row["c_char"]);
    }

    private static SourceDefinition Definition(string table)
        => Backend.Definition(Backend.SourceConfig(table, c => { c.Snapshot = true; c.BatchSize = 10; }));

    private static async Task<string> SeedAsync()
    {
        var table = Backend.NewTable("pgtypes");
        var quoted = $"{Backend.Dialect.QuoteIdent(Backend.Dialect.DefaultSchema)}.{Backend.Dialect.QuoteIdent(table)}";
        await using var connection = await Backend.OpenAsync().ConfigureAwait(false);

        await Sql.ExecAsync(connection, $"""
            CREATE TABLE {quoted} (
              id bigint NOT NULL PRIMARY KEY,
              c_smallint smallint, c_integer integer, c_bigint bigint,
              c_real real, c_double double precision, c_numeric numeric(19,4),
              c_bool boolean,
              c_timestamp timestamp, c_timestamptz timestamptz, c_date date, c_time time,
              c_json json, c_jsonb jsonb,
              c_text text, c_varchar varchar(20), c_char char(3),
              c_uuid uuid, c_bytea bytea, c_intarray integer[])
            """).ConfigureAwait(false);

        // $$ raw interpolation: {{quoted}} is the hole, and the single braces below stay literal so the
        // JSON and array literals read exactly as they would in psql.
        await Sql.ExecAsync(connection, $$"""
            INSERT INTO {{quoted}} VALUES (
              1, 32000, 2000000000, 9000000000000000000,
              1.5, 2.25, 1234.5678,
              true,
              TIMESTAMP '2026-08-14 09:30:00', TIMESTAMPTZ '2026-08-14 09:30:00+00',
              DATE '2026-08-14', TIME '09:30:00',
              '{"a": 1}'::json, '{"b": 2}'::jsonb,
              'text', 'varchar', 'abc',
              '00000000-0000-0000-0000-000000000001'::uuid,
              '\x010203'::bytea,
              '{1,2,3}'::integer[])
            """).ConfigureAwait(false);

        return table;
    }
}
