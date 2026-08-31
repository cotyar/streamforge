using StreamsForge.Abstractions;
using StreamsForge.AppCore.Transports;
using StreamsForge.Connectors.Database;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests;

/// <summary>The per-kind half of <see cref="PgCdcSource"/>'s validation, plus its console descriptor —
/// mirrors <c>DbSourceValidationTests</c>'s style and, for the descriptor half,
/// <c>DatabaseConnectorsTests.EverySecretFieldMatchesAnActualSecretProperty</c>'s (this kind is not
/// registered through <c>DatabaseConnectors</c> yet — that is wave F's job — so the equivalent assertion
/// has to be self-contained here, against a directly-constructed <see cref="PgCdcSource"/>).</summary>
public class PgCdcValidationTests
{
    private static readonly PgCdcSource Source = new(new PostgresDialect());

    private static SourceDefinition Definition(Action<DbSourceConfig>? tweak = null)
    {
        DbSourceConfig config = new()
        {
            Host = "db",
            Database = "market",
            SlotName = "sf_slot",
            PublicationName = "sf_pub",
        };
        tweak?.Invoke(config);
        return new SourceDefinition { Name = "trades", Kind = SourceKinds.PostgresCdc, Connector = new ConnectorConfig { Db = config } };
    }

    private static List<string> Errors(SourceDefinition def)
    {
        List<string> errors = [];
        Source.Validate(def, errors);
        return errors;
    }

    [Fact]
    public void AWellFormedCdcSourceIsAccepted()
        => Assert.Empty(Errors(Definition()));

    [Fact]
    public void AMissingConfigObjectIsTheFirstAndOnlyComplaint()
    {
        var errors = Errors(new SourceDefinition { Name = "x", Kind = SourceKinds.PostgresCdc });

        Assert.Equal($"kind '{SourceKinds.PostgresCdc}' requires connector.db", Assert.Single(errors));
    }

    [Fact]
    public void HostAndDatabaseAreRequiredUnlessAConnectionStringSuppliesThem()
    {
        Assert.Contains(Errors(Definition(c => c.Host = "")), e => e.Contains("host + database", StringComparison.Ordinal));
        Assert.Contains(Errors(Definition(c => c.Database = "")), e => e.Contains("host + database", StringComparison.Ordinal));

        Assert.DoesNotContain(
            Errors(Definition(c => { c.Host = ""; c.Database = ""; c.ConnectionString = "Host=elsewhere;Database=other"; })),
            e => e.Contains("host + database", StringComparison.Ordinal));
    }

    [Fact]
    public void ASlotNameIsRequired()
        => Assert.Contains(Errors(Definition(c => c.SlotName = "")), e => e.Contains("slotName", StringComparison.Ordinal));

    [Fact]
    public void APublicationNameIsRequired()
        => Assert.Contains(Errors(Definition(c => c.PublicationName = "")), e => e.Contains("publicationName", StringComparison.Ordinal));

    [Fact]
    public void ACursorColumnIsRejectedAsBelongingToThePolledKind()
    {
        var errors = Errors(Definition(c => c.CursorColumn = "id"));

        Assert.Contains(errors, e => e.Contains("cursorColumn", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains($"'{SourceKinds.Postgres}'", StringComparison.Ordinal));
    }

    [Fact]
    public void TheDefaultCursorKindIsNotFlaggedButAnExplicitNonDefaultOneIs()
    {
        // CursorKind defaults to "long" on every DbSourceConfig, CDC or not — the default itself must not
        // read as evidence the operator configured the polled kind's field.
        Assert.Empty(Errors(Definition()));

        var errors = Errors(Definition(c => c.CursorKind = CursorKinds.Timestamp));
        Assert.Contains(errors, e => e.Contains("cursorKind", StringComparison.Ordinal));
    }

    [Fact]
    public void AQueryIsRejectedAsBelongingToThePolledKind()
    {
        var errors = Errors(Definition(c => c.Query = "SELECT * FROM trades WHERE id > @cursor"));

        Assert.Contains(errors, e => e.Contains("query", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains($"'{SourceKinds.Postgres}'", StringComparison.Ordinal));
    }

    [Fact]
    public void AWhereClauseIsRejectedAsBelongingToThePolledKind()
        => Assert.Contains(Errors(Definition(c => c.Where = "status = 'settled'")), e => e.Contains("where", StringComparison.Ordinal));

    [Fact]
    public void ACaptureInstanceIsRejectedAsBelongingToMsSqlCdc()
    {
        var errors = Errors(Definition(c => c.CaptureInstance = "dbo_orders"));

        Assert.Contains(errors, e => e.Contains("captureInstance", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains($"'{SourceKinds.MsSqlCdc}'", StringComparison.Ordinal));
    }

    [Fact]
    public void SnapshotIsRejectedAndTheMessageNamesThePolledKindAsTheFix()
    {
        var errors = Errors(Definition(c => c.Snapshot = true));

        var message = Assert.Single(errors, e => e.Contains("snapshot", StringComparison.OrdinalIgnoreCase));
        Assert.Contains($"'{SourceKinds.Postgres}'", message, StringComparison.Ordinal);
        Assert.Contains("connection-per-cycle", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidationNeverThrowsOnAnEmptyConfig()
    {
        List<string> errors = [];
        Source.Validate(new SourceDefinition { Kind = SourceKinds.PostgresCdc, Connector = new ConnectorConfig { Db = new DbSourceConfig() } }, errors);

        Assert.NotEmpty(errors);
    }

    [Fact]
    public async Task PollingASourceWithNoConfigFailsWithANamedMessageRatherThanANullReference()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Source.PollAsync(new SourceDefinition { Name = "trades", Kind = SourceKinds.PostgresCdc }, null, CancellationToken.None));

        Assert.Contains("connector.db", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void KindIsPostgresCdc()
        => Assert.Equal(SourceKinds.PostgresCdc, Source.Kind);

    [Fact]
    public async Task ProbeAsyncDelegatesToCdcPreflight()
    {
        // No live server here — CdcPreflight.ProbePostgresAsync attempts a real connection to "db" and
        // wraps the failure so the message names the dialect, host and database it tried. Reaching THAT
        // exact message (rather than, say, a NullReferenceException out of PgCdcSource itself) is what
        // proves ProbeAsync is a straight, unwrapped delegation to CdcPreflight — nothing here re-catches
        // or re-messages what the preflight already says.
        var def = Definition();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Source.ProbeAsync(def, CancellationToken.None));
        Assert.Contains("PostgreSQL", ex.Message, StringComparison.Ordinal);
        Assert.Contains("market", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDescriptorDeclaresTheThreeFlagsThatDriveTheConsole()
    {
        var descriptor = Source.Describe();

        Assert.True(descriptor.Polled);
        Assert.False(descriptor.Mapping);
        Assert.True(descriptor.CanProbe);
        Assert.Equal("db", descriptor.ConfigProperty);
        Assert.Equal(SourceKinds.PostgresCdc, descriptor.Kind);
    }

    [Fact]
    public void TheDescriptorHelpStatesTheOperationalHazardsPlainly()
    {
        var help = Source.Describe().Help!;

        Assert.Contains("wal_level", help, StringComparison.Ordinal);
        Assert.Contains("REPLICATION", help, StringComparison.Ordinal);
        Assert.Contains("PINS WAL", help, StringComparison.Ordinal);
        Assert.Contains("max_slot_wal_keep_size", help, StringComparison.Ordinal);
        Assert.Contains("REPLICA IDENTITY", help, StringComparison.Ordinal);
        Assert.Contains("__debezium_unavailable_value", help, StringComparison.Ordinal);
        Assert.Contains("AT-LEAST-ONCE", help, StringComparison.Ordinal);
        Assert.Contains("Snapshot", help, StringComparison.Ordinal);
        Assert.Contains($"'{SourceKinds.Postgres}'", help, StringComparison.Ordinal);
    }

    [Fact]
    public void OnlyCdcRelevantFieldsAreDeclared()
    {
        var keys = Source.Describe().Fields.Select(f => f.Key).ToHashSet(StringComparer.Ordinal);

        // Belongs to this kind.
        Assert.Contains("slotName", keys);
        Assert.Contains("publicationName", keys);
        Assert.Contains("tables", keys);
        Assert.Contains("initialCursor", keys);
        Assert.Contains("createSlotIfMissing", keys);
        Assert.Contains("maxPollMs", keys);

        // Belongs to the polled kind, not this one.
        Assert.DoesNotContain("cursorColumn", keys);
        Assert.DoesNotContain("cursorKind", keys);
        Assert.DoesNotContain("query", keys);
        Assert.DoesNotContain("where", keys);
        Assert.DoesNotContain("batchSize", keys);
        Assert.DoesNotContain("snapshot", keys);
        Assert.DoesNotContain("dedupKeyColumn", keys);
        Assert.DoesNotContain("table", keys);
        Assert.DoesNotContain("schema", keys);
        // Belongs to the OTHER CDC kind.
        Assert.DoesNotContain("captureInstance", keys);
    }

    [Fact]
    public void EveryDescriptorFieldNamesARealPropertyOfDbSourceConfig()
    {
        var properties = typeof(DbSourceConfig).GetProperties()
            .Select(p => char.ToLowerInvariant(p.Name[0]) + p.Name[1..])
            .ToHashSet(StringComparer.Ordinal);

        foreach (var field in Source.Describe().Fields)
        {
            Assert.Contains(field.Key, properties);
        }
    }

    [Fact]
    public void EverySecretFieldMatchesAnActualSecretPropertyOnDbSourceConfig()
    {
        // A field typed "secret" that is NOT a [Secret] property would render masked in the console while
        // being exported in plaintext — see DatabaseConnectorsTests' identical assertion for the polled kinds.
        var declaredSecrets = typeof(DbSourceConfig).GetProperties()
            .Where(p => p.IsDefined(typeof(SecretAttribute), inherit: true))
            .Select(p => char.ToLowerInvariant(p.Name[0]) + p.Name[1..])
            .ToHashSet(StringComparer.Ordinal);

        var describedSecretKeys = Source.Describe().Fields
            .Where(f => f.Type == TransportFieldTypes.Secret)
            .Select(f => f.Key)
            .ToHashSet(StringComparer.Ordinal);

        // Every field this descriptor marks "secret" really is one.
        Assert.True(describedSecretKeys.IsSubsetOf(declaredSecrets));
        // And every secret this kind actually uses (password, connectionString) is declared as such — this
        // descriptor just doesn't necessarily surface every field DbSourceConfig has, unlike the polled kinds'.
        Assert.Contains("password", describedSecretKeys);
        Assert.Contains("connectionString", describedSecretKeys);
    }
}
