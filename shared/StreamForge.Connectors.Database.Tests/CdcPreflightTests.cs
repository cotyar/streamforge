using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;
using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

/// <summary>
/// Both probes are I/O-bound against a real catalog, so no live server means no exercising the query
/// logic honestly — that would need Docker (out of scope here, see <c>Tests/Integration</c>, owned by
/// waves C/D). What CAN be tested honestly without one: both probes throw when the endpoint is
/// unreachable, and — the actual point of this file — every pure helper <see cref="CdcPreflight"/>
/// extracts its diagnostics-composition logic into, tested directly rather than through a probe that
/// never gets far enough to reach them.
/// </summary>
public class CdcPreflightTests
{
    private static SourceDefinition PgSource(Action<DbSourceConfig>? tweak = null)
    {
        DbSourceConfig config = new()
        {
            // Port 1 on loopback: connection refused immediately, exactly like DbSinkClientTests.
            Host = "127.0.0.1",
            Port = 1,
            Database = "market",
            Username = "sf",
            Password = "pw",
            CommandTimeoutSeconds = 5,
        };
        tweak?.Invoke(config);
        return new SourceDefinition { Name = "trades-cdc", Kind = SourceKinds.PostgresCdc, Connector = new ConnectorConfig { Db = config } };
    }

    private static SourceDefinition MsSource(Action<DbSourceConfig>? tweak = null)
    {
        DbSourceConfig config = new()
        {
            Host = "127.0.0.1",
            Port = 1,
            Database = "market",
            Username = "sf",
            Password = "pw",
            CommandTimeoutSeconds = 5,
        };
        tweak?.Invoke(config);
        return new SourceDefinition { Name = "orders-cdc", Kind = SourceKinds.MsSqlCdc, Connector = new ConnectorConfig { Db = config } };
    }

    // ---- Both probes throw against an unreachable endpoint, and the message names the host. ----

    [Fact]
    public async Task ProbePostgresAsyncThrowsWithTheHostNamedWhenTheConnectionCannotBeOpened()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => CdcPreflight.ProbePostgresAsync(PgSource(), CancellationToken.None));
        Assert.Contains("127.0.0.1", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ProbeMsSqlAsyncThrowsWithTheHostNamedWhenTheConnectionCannotBeOpened()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => CdcPreflight.ProbeMsSqlAsync(MsSource(), CancellationToken.None));
        Assert.Contains("127.0.0.1", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ProbePostgresAsyncThrowsBeforeConnectingWhenConnectorDbIsMissing()
    {
        var def = new SourceDefinition { Name = "no-config", Kind = SourceKinds.PostgresCdc };
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => CdcPreflight.ProbePostgresAsync(def, CancellationToken.None));
        Assert.Contains("no-config", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ProbeMsSqlAsyncThrowsBeforeConnectingWhenConnectorDbIsMissing()
    {
        var def = new SourceDefinition { Name = "no-config", Kind = SourceKinds.MsSqlCdc };
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => CdcPreflight.ProbeMsSqlAsync(def, CancellationToken.None));
        Assert.Contains("no-config", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ProbeMsSqlAsyncRefusesAMalformedCaptureInstanceBeforeEverConnecting()
    {
        // A regex reject is structural, like DbSinkClientTests' pipeline-upsert refusal: it must not touch
        // the network at all, so it has to be fast regardless of whether the endpoint is reachable.
        var started = DateTimeOffset.UtcNow;
        var ex = await Assert.ThrowsAsync<FormatException>(() => CdcPreflight.ProbeMsSqlAsync(MsSource(c => c.CaptureInstance = "dbo.orders; DROP TABLE x"), CancellationToken.None));
        Assert.Contains("dbo.orders; DROP TABLE x", ex.Message, StringComparison.Ordinal);
        Assert.True(DateTimeOffset.UtcNow - started < TimeSpan.FromSeconds(1));
    }

    // ---- SlotAndWalLagSql: regression guard for the Npgsql "no MARS" bug wave G caught live (plan 017
    // wave E follow-up). max_slot_wal_keep_size used to be fetched with a SEPARATE `SHOW` command issued
    // while the slot/lag reader was still open, which throws NpgsqlOperationInProgressException on
    // Npgsql — and it fired on almost every real slot, since almost every real slot has non-zero lag. The
    // fix folds it into the same query via current_setting(...). The actual thrown-exception behavior can
    // only be observed against a live open connection (verified manually against a Docker Postgres — see
    // the task report), which belongs to the Docker-backed Integration suite this file does not own; this
    // is the honest substitute available without mocking a database: pin that both facts live in ONE query
    // string, because splitting them back into two is exactly the regression this guards against.

    [Fact]
    public void TheSlotAndWalLagQueryFoldsMaxSlotWalKeepSizeIntoOneRoundTrip()
    {
        Assert.Contains("FROM pg_replication_slots", CdcPreflight.SlotAndWalLagSql, StringComparison.Ordinal);
        Assert.Contains("current_setting('max_slot_wal_keep_size')", CdcPreflight.SlotAndWalLagSql, StringComparison.Ordinal);
    }

    // ---- CdcMetadataFields: exactly _op/_weight/_ts/_table with the right FieldTypes. ----

    [Fact]
    public void CdcMetadataFieldsAreExactlyTheFourStampedColumns()
    {
        var fields = CdcPreflight.CdcMetadataFields();

        Assert.Equal(
            [("_op", FieldType.String), ("_weight", FieldType.Long), ("_ts", FieldType.Timestamp), ("_table", FieldType.String)],
            fields.Select(f => (f.Name, f.Type)));
    }

    [Fact]
    public void CdcMetadataFieldsReturnsFreshInstancesEveryCall()
    {
        // Returned FieldDefs are mutable records the caller may own from here on; two probes sharing one
        // instance would let one caller's edit bleed into the other's result.
        var first = CdcPreflight.CdcMetadataFields();
        var second = CdcPreflight.CdcMetadataFields();

        Assert.NotSame(first[0], second[0]);
    }

    // ---- FormatBytes: WAL-lag byte formatting. ----

    [Theory]
    [InlineData(0, "0 B")]
    [InlineData(500, "500 B")]
    [InlineData(1536, "1.5 KB")]
    [InlineData(1024 * 1024 * 3, "3.0 MB")]
    [InlineData(1024L * 1024 * 1024 * 2, "2.0 GB")]
    public void FormatBytesRendersHumanReadableUnits(long bytes, string expected)
        => Assert.Equal(expected, CdcPreflight.FormatBytes(bytes));

    // ---- WalLagDiagnostic: the single most valuable line the Postgres probe emits. ----

    [Fact]
    public void WalLagDiagnosticNamesTheSlotAndTheMeasuredLag()
    {
        var message = CdcPreflight.WalLagDiagnostic("sf_slot", 1536, "0/16B3748", "0/16B4000", "512MB");

        Assert.Contains("sf_slot", message, StringComparison.Ordinal);
        Assert.Contains("1.5 KB", message, StringComparison.Ordinal);
        Assert.Contains("0/16B3748", message, StringComparison.Ordinal);
        Assert.Contains("0/16B4000", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("0")]
    [InlineData("")]
    public void WalLagDiagnosticCallsOutAnUnboundedSafetyValve(string keepSize)
    {
        var message = CdcPreflight.WalLagDiagnostic("sf_slot", 100, null, null, keepSize);
        Assert.Contains("unbounded", message, StringComparison.Ordinal);
        Assert.Contains("DISK", message, StringComparison.Ordinal);
    }

    [Fact]
    public void WalLagDiagnosticNamesInvalidationWhenASafetyValveIsConfigured()
    {
        var message = CdcPreflight.WalLagDiagnostic("sf_slot", 100, null, null, "512MB");
        Assert.Contains("512MB", message, StringComparison.Ordinal);
        Assert.Contains("INVALIDATE", message, StringComparison.Ordinal);
    }

    // ---- ReplicaIdentityDiagnostic: the relreplident letter → sentence mapping. ----

    [Fact]
    public void ReplicaIdentityFullCarriesNoDiagnostic()
        => Assert.Null(CdcPreflight.ReplicaIdentityDiagnostic("public.orders", 'f'));

    [Fact]
    public void ReplicaIdentityNothingIsCalledOutAsUnableToReplicateAtAll()
    {
        var message = CdcPreflight.ReplicaIdentityDiagnostic("public.orders", 'n');
        Assert.Contains("public.orders", message, StringComparison.Ordinal);
        Assert.Contains("NOTHING", message, StringComparison.Ordinal);
        Assert.Contains("REPLICA IDENTITY FULL", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData('d')]
    [InlineData('i')]
    public void ReplicaIdentityDefaultOrIndexWarnsDeleteCarriesOnlyKeyColumns(char code)
    {
        var message = CdcPreflight.ReplicaIdentityDiagnostic("public.orders", code);
        Assert.Contains("public.orders", message!, StringComparison.Ordinal);
        Assert.Contains("REPLICA IDENTITY FULL", message!, StringComparison.Ordinal);
    }

    [Fact]
    public void ReplicaIdentityAnUnrecognizedCodeIsNamedRatherThanSwallowed()
        => Assert.Contains("unrecognized", CdcPreflight.ReplicaIdentityDiagnostic("public.orders", 'z')!, StringComparison.Ordinal);

    // ---- FormatElapsed: retention window, in the units an operator reasons in. ----

    [Theory]
    [InlineData(3, 4, 0, "3d 4h")]
    [InlineData(0, 2, 30, "2h 30m")]
    [InlineData(0, 0, 45, "45m")]
    public void FormatElapsedUsesTheCoarsestTwoUnits(int days, int hours, int minutes, string expected)
        => Assert.Equal(expected, CdcPreflight.FormatElapsed(new TimeSpan(days, hours, minutes, 0)));

    [Fact]
    public void FormatElapsedClampsANegativeSpanToZeroRatherThanPrintingMinusSigns()
        => Assert.Equal("0m", CdcPreflight.FormatElapsed(TimeSpan.FromMinutes(-5)));

    // ---- IsToastable: which Postgres types can arrive as the TOAST sentinel. ----

    [Theory]
    [InlineData("text", true)]
    [InlineData("numeric", true)]
    [InlineData("jsonb", true)]
    [InlineData("int4[]", true)]
    [InlineData("int4", false)]
    [InlineData("bool", false)]
    [InlineData("timestamptz", false)]
    public void IsToastableClassifiesByType(string typeName, bool expected)
        => Assert.Equal(expected, CdcPreflight.IsToastable(typeName));

    // ---- PgArrayAwareTypeName: information_schema's ARRAY/udt_name pair → the type table's own key shape. ----

    [Theory]
    [InlineData("ARRAY", "_int4", "int4[]")]
    [InlineData("array", "_text", "text[]")]
    [InlineData("integer", "int4", "int4")]
    public void PgArrayAwareTypeNameRebuildsTheBracketedForm(string dataType, string udtName, string expected)
        => Assert.Equal(expected, CdcPreflight.PgArrayAwareTypeName(dataType, udtName));

    // ---- ParseTables: the Tables CSV, and its fallback to Schema/Table. ----

    [Fact]
    public void ParseTablesSplitsAndDefaultsSchema()
    {
        var tables = CdcPreflight.ParseTables("public.orders, sales.invoices, accounts", "app", "");
        Assert.Equal([("public", "orders"), ("sales", "invoices"), ("app", "accounts")], tables);
    }

    [Fact]
    public void ParseTablesFallsBackToSchemaAndTableWhenTablesIsEmpty()
    {
        var tables = CdcPreflight.ParseTables("", "", "trades");
        Assert.Equal([("public", "trades")], tables);
    }

    [Fact]
    public void ParseTablesIsEmptyWhenNothingIsConfigured()
        => Assert.Empty(CdcPreflight.ParseTables("", "", ""));

    // ---- ValidateCaptureInstance: the identifier guard. ----

    [Theory]
    [InlineData("dbo_Orders")]
    [InlineData("cdc_instance1")]
    [InlineData("_leading_underscore")]
    public void ValidateCaptureInstanceAcceptsPlainIdentifiers(string instance)
        => Assert.Equal(instance, CdcPreflight.ValidateCaptureInstance(instance));

    [Theory]
    [InlineData("dbo'orders")]
    [InlineData("dbo;DROP TABLE x")]
    [InlineData("dbo orders")]
    [InlineData("1dbo_orders")]
    [InlineData("")]
    public void ValidateCaptureInstanceRejectsAnythingThatIsNotAPlainIdentifier(string instance)
        => Assert.Throws<FormatException>(() => CdcPreflight.ValidateCaptureInstance(instance));
}
