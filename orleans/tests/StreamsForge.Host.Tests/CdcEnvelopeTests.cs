using System.Linq;
using System.Text.Json;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors;
using StreamsForge.AppCore.Connectors.Mapping;
using StreamsForge.AppCore.Connectors.Polling;
using Xunit;

namespace StreamsForge.Host.Tests;

/// <summary>Plan 014: <see cref="CdcEnvelope"/> — the Debezium unwrapper — plus its wiring into
/// <see cref="ConnectorPollCycle"/> (via <see cref="ConnectorPollCycle.ExecuteMessage"/>, which is the
/// transport-neutral name <see cref="ConnectorPollCycle.ExecuteNatsMessage"/> now shares — see that
/// method's own doc comment). Fixtures below are realistic recorded Debezium output for a Postgres and
/// a Microsoft SQL Server connector: they differ in <c>source</c>'s fields (Postgres: <c>lsn</c>/<c>txId</c>;
/// SQL Server: <c>change_lsn</c>/<c>commit_lsn</c>) and in column-naming convention (Postgres:
/// lower_snake; SQL Server: PascalCase) — deliberately, so a test asserting on the wrong connector's
/// shape would fail loudly rather than pass by accident.</summary>
public class CdcEnvelopeTests
{
    private const long TsMs = 1_700_000_000_123;

    // ---- Fixture builders -------------------------------------------------------------------

    /// <summary>A Postgres connector's <c>source</c> block (Debezium 2.5, decoderbufs plugin).</summary>
    private static string PostgresSource(string op) => $$"""
        {"version":"2.5.0.Final","connector":"postgresql","name":"pgserver1","ts_ms":{{TsMs}},
        "snapshot":"{{(op == "r" ? "true" : "false")}}","db":"inventory","sequence":"[\"24023128\",\"24023130\"]",
        "schema":"public","table":"customers","txId":563,"lsn":24023130,"xmin":null}
        """;

    /// <summary>A SQL Server connector's <c>source</c> block — an entirely different capture model
    /// (LSNs from a change table, not WAL) surfaced as different field names.</summary>
    private static string SqlServerSource(string op) => $$"""
        {"version":"2.5.0.Final","connector":"sqlserver","name":"mssqlserver1","ts_ms":{{TsMs}},
        "snapshot":"{{(op == "r" ? "true" : "false")}}","db":"testDB","schema":"dbo","table":"Customers",
        "change_lsn":"00000027:00000ac0:0007","commit_lsn":"00000027:00000ac0:0009","event_serial_no":1}
        """;

    private static string Payload(string op, string? before, string? after, string source) => $$"""
        {"before":{{before ?? "null"}},"after":{{after ?? "null"}},"source":{{source}},"op":"{{op}}","ts_ms":{{TsMs}},"transaction":null}
        """;

    /// <summary>Shape 1: the raw <c>{schema,payload}</c> form Debezium Server emits by default.</summary>
    private static string Wrapped(string payload) => $$"""
        {"schema":{"type":"struct","optional":false,"name":"pgserver1.inventory.customers.Envelope","fields":[]},"payload":{{payload}}}
        """;

    private static string PgRow(long id, string name, double price) => $$"""{"id":{{id}},"name":"{{name}}","price":{{price}}}""";
    private static string MsRow(long id, string name, double price) => $$"""{"Id":{{id}},"Name":"{{name}}","Price":{{price}}}""";

    /// <summary>Builds one connector's envelope message for one op, wrapped or bare (shapes 1 and 2).</summary>
    private static string Envelope(bool postgres, string op, bool wrapped)
    {
        var source = postgres ? PostgresSource(op) : SqlServerSource(op);
        var (before, after) = op switch
        {
            "c" or "r" => ((string?)null, postgres ? PgRow(1001, "Sally Thomas", 9.99) : MsRow(2001, "John Doe", 9.99)),
            "u" => (postgres ? PgRow(1001, "Sally Thomas", 9.99) : MsRow(2001, "John Doe", 9.99),
                    postgres ? PgRow(1001, "Sally Thomas", 19.99) : MsRow(2001, "John Doe", 19.99)),
            "d" => (postgres ? PgRow(1001, "Sally Thomas", 19.99) : MsRow(2001, "John Doe", 19.99), (string?)null),
            _ => throw new ArgumentException(op),
        };
        var payload = Payload(op, before, after, source);
        return wrapped ? Wrapped(payload) : payload;
    }

    private static SourceDefinition Source(bool postgres, string? envelope = null) => new()
    {
        Name = postgres ? "pg-customers" : "mssql-customers",
        Kind = SourceKinds.Nats, // realistic carrier for a Debezium Server envelope (plan 014's own example)
        Fields = postgres
            ? [new FieldDef("id", FieldType.Long), new FieldDef("name", FieldType.String), new FieldDef("price", FieldType.Double)]
            : [new FieldDef("Id", FieldType.Long), new FieldDef("Name", FieldType.String), new FieldDef("Price", FieldType.Double)],
        Connector = new ConnectorConfig
        {
            Mapping = new MappingSpec
            {
                ItemsPath = "$",
                Envelope = envelope ?? CdcEnvelopes.Debezium,
                Fields = postgres
                    ? [
                        new FieldMapEntry { Field = new FieldDef("id", FieldType.Long) },
                        new FieldMapEntry { Field = new FieldDef("name", FieldType.String) },
                        new FieldMapEntry { Field = new FieldDef("price", FieldType.Double) },
                      ]
                    : [
                        new FieldMapEntry { Field = new FieldDef("Id", FieldType.Long) },
                        new FieldMapEntry { Field = new FieldDef("Name", FieldType.String) },
                        new FieldMapEntry { Field = new FieldDef("Price", FieldType.Double) },
                      ],
            },
        },
    };

    // ---- c / u / d / r, both connectors, both wrapped and SMT-unwrapped (shapes 1 and 2) ------

    [Theory]
    [InlineData(true, "c", true)]
    [InlineData(true, "c", false)]
    [InlineData(true, "r", true)]
    [InlineData(true, "r", false)]
    [InlineData(true, "u", true)]
    [InlineData(true, "u", false)]
    [InlineData(true, "d", true)]
    [InlineData(true, "d", false)]
    [InlineData(false, "c", true)]
    [InlineData(false, "c", false)]
    [InlineData(false, "r", true)]
    [InlineData(false, "r", false)]
    [InlineData(false, "u", true)]
    [InlineData(false, "u", false)]
    [InlineData(false, "d", true)]
    [InlineData(false, "d", false)]
    public void Op_takes_the_right_row_and_stamps__op___weight_and__ts(bool postgres, string op, bool wrapped)
    {
        var def = Source(postgres);
        var message = Envelope(postgres, op, wrapped);

        var result = ConnectorPollCycle.ExecuteMessage(def, FileFormats.JsonArray, message, new DedupTracker(), nowMs: 999);

        Assert.Null(result.Error);
        Assert.Equal(0, result.EnvelopeSkipped);
        Assert.Single(result.Rows);
        var row = result.Rows[0];

        Assert.Equal(op, row["_op"]);
        Assert.Equal(op == "d" ? -1 : 1, row["_weight"]);
        Assert.Equal(TsMs, row["_ts"]); // payload.ts_ms wins over arrival time (999)

        var priceField = postgres ? "price" : "Price";
        // c/r/u take "after" (price 9.99 for c/r, 19.99 for the post-update value); d takes "before" (19.99).
        var expectedPrice = op switch { "c" or "r" => 9.99, "u" => 19.99, "d" => 19.99, _ => throw new Exception() };
        Assert.Equal(expectedPrice, row[priceField]);
    }

    // ---- Shape 3: no "op" key at all — ExtractNewRecordState already flattened it. Pass through. --

    [Fact]
    public void No_op_key_is_the_SMT_flattened_shape_and_passes_through_untouched()
    {
        var def = Source(postgres: true);
        // What ExtractNewRecordState leaves behind: a plain row, no envelope wrapper, no "op".
        var message = PgRow(1001, "Sally Thomas", 9.99);

        var result = ConnectorPollCycle.ExecuteMessage(def, FileFormats.JsonArray, message, new DedupTracker(), nowMs: 999);

        Assert.Null(result.Error);
        Assert.Equal(0, result.EnvelopeSkipped);
        Assert.Single(result.Rows);
        var row = result.Rows[0];
        Assert.Equal(1001L, row["id"]);
        Assert.Equal("Sally Thomas", row["name"]);
        Assert.Equal(9.99, row["price"]);
        // Nothing to stamp — this was never a change event.
        Assert.False(row.ContainsKey("_op"));
        Assert.False(row.ContainsKey("_weight"));
        Assert.Equal(999L, row["_ts"]); // falls back to arrival time exactly like any non-CDC message.
    }

    // ---- A delete with no "before" cannot produce a row. Visible, not silent. -------------------

    [Fact]
    public void Delete_with_no_before_cannot_produce_a_row_and_is_counted_not_silently_dropped()
    {
        var def = Source(postgres: true);
        var source = PostgresSource("d");
        // REPLICA IDENTITY not FULL: Debezium sends "before": null on a delete.
        var message = Wrapped(Payload("d", before: null, after: null, source));

        var result = ConnectorPollCycle.ExecuteMessage(def, FileFormats.JsonArray, message, new DedupTracker(), nowMs: 999);

        Assert.Null(result.Error); // one unrepresentable event must not fail the whole cycle
        Assert.Empty(result.Rows);
        Assert.Equal(1, result.EnvelopeSkipped); // ...but it IS counted, not silently swallowed
    }

    // ---- A tombstone (null/empty payload) likewise cannot produce a row. -------------------------

    [Fact]
    public void Tombstone_as_a_literal_JSON_null_message_is_skipped_and_counted()
    {
        var def = Source(postgres: true);

        var result = ConnectorPollCycle.ExecuteMessage(def, FileFormats.JsonArray, "null", new DedupTracker(), nowMs: 999);

        Assert.Null(result.Error);
        Assert.Empty(result.Rows);
        Assert.Equal(1, result.EnvelopeSkipped);
    }

    [Fact]
    public void Tombstone_as_schema_null_payload_null_is_skipped_and_counted()
    {
        var def = Source(postgres: true);
        var message = """{"schema":null,"payload":null}""";

        var result = ConnectorPollCycle.ExecuteMessage(def, FileFormats.JsonArray, message, new DedupTracker(), nowMs: 999);

        Assert.Null(result.Error);
        Assert.Empty(result.Rows);
        Assert.Equal(1, result.EnvelopeSkipped);
    }

    // ---- Envelope = "none" is byte-identical to the pre-014 path. Proved, not asserted-in-comment. -

    [Fact]
    public void Envelope_none_leaves_a_Debezium_shaped_payload_completely_alone()
    {
        // A mapping that has never heard of CDC: it reads "payload.op" and "payload.after.id" as
        // perfectly ordinary JSON fields, the way any pre-014 mapping would.
        var spec = new MappingSpec
        {
            ItemsPath = "$",
            Envelope = CdcEnvelopes.None,
            Fields =
            [
                new FieldMapEntry { SourcePath = "payload.op", Field = new FieldDef("op", FieldType.String) },
                new FieldMapEntry { SourcePath = "payload.after.id", Field = new FieldDef("id", FieldType.Long) },
            ],
        };
        var def = new SourceDefinition
        {
            Name = "raw-json",
            Kind = SourceKinds.Nats,
            Fields = [new FieldDef("op", FieldType.String), new FieldDef("id", FieldType.Long)],
            Connector = new ConnectorConfig { Mapping = spec },
        };
        var message = Wrapped(Payload("c", before: null, after: PgRow(1001, "Sally Thomas", 9.99), PostgresSource("c")));

        var result = ConnectorPollCycle.ExecuteMessage(def, FileFormats.JsonArray, message, new DedupTracker(), nowMs: 999);

        // Proof, not a comment: this is exactly what RecordExtractor.Extract (the pre-014 code path,
        // called with the untouched message) produces, plus only the "_source"/"_ts" stamp EmitCore
        // has always added — nothing CdcEnvelope-shaped leaked in.
        using var doc = JsonDocument.Parse(message);
        var expected = RecordExtractor.Extract(doc.RootElement, spec, arrivalMs: 999);
        Assert.Single(expected);
        expected[0]["_source"] = "raw-json";

        Assert.Null(result.Error);
        Assert.Equal(0, result.EnvelopeSkipped);
        Assert.Single(result.Rows);
        // Same keys, same values, as the pre-014 extraction — nothing CdcEnvelope-shaped leaked in.
        Assert.Equal(expected[0].Keys.OrderBy(k => k, StringComparer.Ordinal), result.Rows[0].Keys.OrderBy(k => k, StringComparer.Ordinal));
        foreach (var key in expected[0].Keys)
        {
            Assert.Equal(expected[0][key], result.Rows[0][key]);
        }
        Assert.Equal("c", result.Rows[0]["op"]);
        Assert.Equal(1001L, result.Rows[0]["id"]);
        Assert.False(result.Rows[0].ContainsKey("_op")); // "op" is a mapped FIELD here, "_op" is a CDC stamp — not the same key
    }

    // ---- An array of envelopes: the format layer splits it, CdcEnvelope unwraps each element. ----

    [Fact]
    public void An_array_of_envelopes_unwraps_each_element_independently()
    {
        var def = Source(postgres: true);
        var insert = Envelope(postgres: true, op: "c", wrapped: true);
        var delete = Envelope(postgres: true, op: "d", wrapped: false);
        var batch = $"[{insert},{delete}]";

        var result = ConnectorPollCycle.ExecuteMessage(def, FileFormats.JsonArray, batch, new DedupTracker(), nowMs: 999);

        Assert.Null(result.Error);
        Assert.Equal(0, result.EnvelopeSkipped);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("c", result.Rows[0]["_op"]);
        Assert.Equal(1, result.Rows[0]["_weight"]);
        Assert.Equal("d", result.Rows[1]["_op"]);
        Assert.Equal(-1, result.Rows[1]["_weight"]);
    }

    // ---- CdcEnvelope.Unwrap directly, for the None short-circuit and skip-reason text. ------------

    [Fact]
    public void Unwrap_with_envelope_None_never_looks_at_the_message()
    {
        using var doc = JsonDocument.Parse("""{"op":"d"}"""); // would Skip under Debezium (no before) — must NOT under None
        var result = CdcEnvelope.Unwrap(doc.RootElement, CdcEnvelopes.None);

        Assert.False(result.Skip);
        Assert.Null(result.Op);
        Assert.Null(result.Weight);
        Assert.Null(result.TsMs);
        Assert.Equal(doc.RootElement.ToString(), result.Row.ToString());
    }

    [Fact]
    public void Unwrap_skip_reason_names_the_case()
    {
        using var doc = JsonDocument.Parse("""{"payload":{"op":"d","before":null,"after":null,"ts_ms":1}}""");
        var result = CdcEnvelope.Unwrap(doc.RootElement, CdcEnvelopes.Debezium);

        Assert.True(result.Skip);
        Assert.Contains("before", result.SkipReason, StringComparison.OrdinalIgnoreCase);
    }
}
