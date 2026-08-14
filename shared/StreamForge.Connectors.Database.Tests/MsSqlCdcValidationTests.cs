using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;
using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

/// <summary>The per-kind half of <see cref="MsSqlCdcSource"/> validation. Every rejected field here is
/// one that belongs to a DIFFERENT kind (the polled 'mssql' source or the Postgres CDC source) sharing the
/// same <see cref="DbSourceConfig"/> — catching it in Validate turns "silently ignored" into "loud and
/// named" before the source ever runs a cycle.</summary>
public class MsSqlCdcValidationTests
{
    private static readonly MsSqlCdcSource Source = new(new SqlServerDialect());

    private static SourceDefinition Definition(Action<DbSourceConfig>? tweak = null)
    {
        DbSourceConfig config = new()
        {
            Host = "db",
            Database = "market",
            CaptureInstance = "dbo_Orders",
        };
        tweak?.Invoke(config);
        return new SourceDefinition { Name = "orders-cdc", Kind = SourceKinds.MsSqlCdc, Connector = new ConnectorConfig { Db = config } };
    }

    private static List<string> Errors(SourceDefinition def)
    {
        List<string> errors = [];
        Source.Validate(def, errors);
        return errors;
    }

    [Fact]
    public void AWellFormedConfigIsAccepted()
        => Assert.Empty(Errors(Definition()));

    [Fact]
    public void AMissingConfigObjectIsTheFirstAndOnlyComplaint()
    {
        var errors = Errors(new SourceDefinition { Name = "x", Kind = SourceKinds.MsSqlCdc });

        Assert.Equal("kind 'mssql-cdc' requires connector.db", Assert.Single(errors));
    }

    [Fact]
    public void HostAndDatabaseAreRequiredUnlessAConnectionStringSuppliesThem()
    {
        Assert.Contains(Errors(Definition(c => c.Host = "")), e => e.Contains("host + database", StringComparison.Ordinal));
        Assert.Contains(Errors(Definition(c => c.Database = "")), e => e.Contains("host + database", StringComparison.Ordinal));

        Assert.DoesNotContain(
            Errors(Definition(c => { c.Host = ""; c.Database = ""; c.ConnectionString = "Server=elsewhere;Database=other"; })),
            e => e.Contains("host + database", StringComparison.Ordinal));
    }

    [Fact]
    public void ACaptureInstanceIsRequired()
        => Assert.Contains(Errors(Definition(c => c.CaptureInstance = "")), e => e.Contains("captureInstance", StringComparison.Ordinal));

    [Fact]
    public void ACaptureInstanceThatIsOnlyWhitespaceIsRejectedAsMissing()
        => Assert.Contains(Errors(Definition(c => c.CaptureInstance = "   ")), e => e.Contains("needs a captureInstance", StringComparison.Ordinal));

    [Theory]
    [InlineData("dbo'; DROP TABLE x; --")]
    [InlineData("dbo.Orders")]
    [InlineData("dbo Orders")]
    [InlineData("dbo;Orders")]
    [InlineData("1dbo")]
    public void ACaptureInstanceWithAQuoteASemicolonASpaceOrABadLeadingCharacterIsRejectedAsTheInjectionGuard(string capture)
    {
        var errors = Errors(Definition(c => c.CaptureInstance = capture));

        Assert.Contains(errors, e => e.Contains("captureInstance", StringComparison.Ordinal) && e.Contains("^[A-Za-z_]", StringComparison.Ordinal));
    }

    [Fact]
    public void ACursorColumnIsRejectedBecauseItBelongsToThePolledMssqlKind()
    {
        var errors = Errors(Definition(c => c.CursorColumn = "id"));

        Assert.Contains(errors, e => e.Contains("cursorColumn", StringComparison.Ordinal) && e.Contains("not to CDC", StringComparison.Ordinal));
    }

    [Fact]
    public void TheDefaultCursorKindIsNotFlaggedSinceEveryDbSourceConfigCarriesItRegardlessOfKind()
        => Assert.DoesNotContain(Errors(Definition()), e => e.Contains("cursorKind", StringComparison.Ordinal));

    [Fact]
    public void ACursorKindExplicitlyChangedAwayFromTheDefaultIsRejected()
    {
        var errors = Errors(Definition(c => c.CursorKind = CursorKinds.Timestamp));

        Assert.Contains(errors, e => e.Contains("cursorKind", StringComparison.Ordinal) && e.Contains("not to CDC", StringComparison.Ordinal));
    }

    [Fact]
    public void AQueryIsRejectedBecauseItBelongsToThePolledMssqlKind()
    {
        var errors = Errors(Definition(c => c.Query = "SELECT * FROM t WHERE id > @cursor"));

        Assert.Contains(errors, e => e.Contains("query", StringComparison.Ordinal) && e.Contains("not to CDC", StringComparison.Ordinal));
    }

    [Fact]
    public void AWhereClauseIsRejectedBecauseItBelongsToThePolledMssqlKind()
    {
        var errors = Errors(Definition(c => c.Where = "status = 'settled'"));

        Assert.Contains(errors, e => e.Contains("where", StringComparison.Ordinal) && e.Contains("not to CDC", StringComparison.Ordinal));
    }

    [Fact]
    public void ASlotNameIsRejectedBecauseItBelongsToPostgresCdc()
    {
        var errors = Errors(Definition(c => c.SlotName = "my_slot"));

        Assert.Contains(errors, e => e.Contains("slotName", StringComparison.Ordinal) && e.Contains("postgres-cdc", StringComparison.Ordinal));
    }

    [Fact]
    public void APublicationNameIsRejectedBecauseItBelongsToPostgresCdc()
    {
        var errors = Errors(Definition(c => c.PublicationName = "my_pub"));

        Assert.Contains(errors, e => e.Contains("publicationName", StringComparison.Ordinal) && e.Contains("postgres-cdc", StringComparison.Ordinal));
    }

    [Fact]
    public void SnapshotAndTablesAndBatchSizeAreNotRejectedTheyAreActivelyUsedByThisKind()
    {
        var errors = Errors(Definition(c =>
        {
            c.Snapshot = true;
            c.Tables = "dbo.Orders,dbo.OrderLines";
            c.BatchSize = 250;
        }));

        Assert.Empty(errors);
    }

    [Fact]
    public void ValidationNeverThrowsOnAnEmptyConfig()
    {
        List<string> errors = [];
        Source.Validate(new SourceDefinition { Kind = SourceKinds.MsSqlCdc, Connector = new ConnectorConfig { Db = new DbSourceConfig() } }, errors);

        Assert.NotEmpty(errors);
    }

    [Fact]
    public async Task PollingASourceWithNoConfigFailsWithANamedMessageRatherThanANullReference()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Source.PollAsync(new SourceDefinition { Name = "orders-cdc", Kind = SourceKinds.MsSqlCdc }, null, CancellationToken.None));

        Assert.Contains("connector.db", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void KindIsFixedAtMsSqlCdcNotTheDialectsOwnKind()
        => Assert.Equal(SourceKinds.MsSqlCdc, Source.Kind);

    [Fact]
    public void DescribeDeclaresTheThreeFlagsThatDriveTheConsole()
    {
        var descriptor = Source.Describe();

        Assert.True(descriptor.Polled);
        Assert.False(descriptor.Mapping);
        Assert.True(descriptor.CanProbe);
        Assert.Equal("db", descriptor.ConfigProperty);
    }

    [Fact]
    public void DescribeHelpStatesTheAtLeastOnceCeilingCdcPrerequisitesRetentionAndWhatSnapshotMeansHere()
    {
        var help = Source.Describe().Help!;

        Assert.Contains("AT-LEAST-ONCE", help, StringComparison.Ordinal);
        Assert.Contains("sp_cdc_enable_db", help, StringComparison.Ordinal);
        Assert.Contains("sp_cdc_enable_table", help, StringComparison.Ordinal);
        Assert.Contains("SQL Server Agent", help, StringComparison.Ordinal);
        Assert.Contains("3 DAYS", help, StringComparison.Ordinal);
        Assert.Contains("NOT a full-table snapshot", help, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryDescribedSecretFieldMatchesAnActualSecretProperty()
    {
        // Same agreement DatabaseConnectorsTests pins for the postgres/mssql descriptors — this kind is
        // not registered through DatabaseConnectors yet (wave F does that), so nothing else checks it.
        var declared = typeof(DbSourceConfig).GetProperties()
            .Where(p => p.IsDefined(typeof(SecretAttribute), inherit: true))
            .Select(p => char.ToLowerInvariant(p.Name[0]) + p.Name[1..])
            .ToHashSet(StringComparer.Ordinal);

        var described = Source.Describe().Fields
            .Where(f => f.Type == TransportFieldTypes.Secret)
            .Select(f => f.Key)
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(declared.SetEquals(described), $"declared [{string.Join(",", declared)}] vs described [{string.Join(",", described)}]");
    }

    [Fact]
    public void EveryDescribedFieldNamesARealPropertyOfDbSourceConfig()
    {
        var properties = typeof(DbSourceConfig).GetProperties()
            .Select(p => char.ToLowerInvariant(p.Name[0]) + p.Name[1..])
            .ToHashSet(StringComparer.Ordinal);

        foreach (var field in Source.Describe().Fields)
        {
            Assert.Contains(field.Key, properties);
        }
    }
}
