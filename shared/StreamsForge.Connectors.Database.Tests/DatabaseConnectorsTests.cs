using StreamsForge.Abstractions;
using StreamsForge.AppCore.Sinks;
using StreamsForge.AppCore.Transports;
using StreamsForge.Connectors.Database;
using Xunit;

namespace StreamsForge.Connectors.Database.Tests;

/// <summary>
/// Registration and the console descriptors. The descriptor assertions mirror
/// <c>TransportRegistryTests</c>'s — including the descriptor↔<c>[Secret]</c> agreement, which is pinned
/// there for the pre-014 transports and has to be pinned HERE for these, since this assembly is not in
/// either host's test project and nothing else would ever look.
///
/// <para>Registration is process-global and permanent, so <see cref="DatabaseConnectors.RegisterAll"/> is
/// called once from the static constructor and its idempotence is tested rather than worked around.</para>
/// </summary>
public class DatabaseConnectorsTests
{
    static DatabaseConnectorsTests() => DatabaseConnectors.RegisterAll();

    /// <summary>The four kinds this assembly registers as polled sources — the two cursor-polled kinds
    /// from plan 014 and the two CDC-polled kinds from plan 017. Shared across the tests below so
    /// extending coverage to the new kinds is a matter of using this list, not writing a parallel set of
    /// CDC-specific assertions.</summary>
    private static readonly string[] AllPolledKinds =
        [SourceKinds.Postgres, SourceKinds.MsSql, SourceKinds.PostgresCdc, SourceKinds.MsSqlCdc];

    [Fact]
    public void RegisterAllPutsBothKindsInBothRegistries()
    {
        Assert.NotNull(PolledTransports.Find(SourceKinds.Postgres));
        Assert.NotNull(PolledTransports.Find(SourceKinds.MsSql));
        Assert.NotNull(SinkTransports.Find(SinkKinds.Postgres));
        Assert.NotNull(SinkTransports.Find(SinkKinds.MsSql));
    }

    [Fact]
    public void TheCdcKindsAreRegisteredAsPolledSourcesAndNothingElse()
    {
        // CDC is ingress only — there is no CDC sink kind at all, so the assertion is that these two
        // kinds simply do not resolve through SinkTransports, not that they resolve to something null-ish.
        Assert.NotNull(PolledTransports.Find(SourceKinds.PostgresCdc));
        Assert.NotNull(PolledTransports.Find(SourceKinds.MsSqlCdc));
        Assert.Null(SinkTransports.Find(SourceKinds.PostgresCdc));
        Assert.Null(SinkTransports.Find(SourceKinds.MsSqlCdc));
    }

    [Fact]
    public void PolledTransportsKindsContainsExactlyTheFourExpectedKindsAfterRegisterAll()
    {
        Assert.Equal(AllPolledKinds.ToHashSet(StringComparer.Ordinal), PolledTransports.Kinds.ToHashSet(StringComparer.Ordinal));
    }

    [Fact]
    public void CallingItTwiceIsANoOpRatherThanTheDuplicateKindException()
    {
        // Two hosts in one test process, or a re-entrant startup, must not take the host down for a reason
        // that has nothing to do with the operator.
        DatabaseConnectors.RegisterAll();
        DatabaseConnectors.RegisterAll();

        Assert.Equal(2, DatabaseConnectors.Dialects.Count);
    }

    [Fact]
    public void TheDatabaseKindsAreNotMistakenForMessageTransports()
    {
        // The message registry drives a subscription; routing a polled kind through it would silence its
        // timer, which is exactly the confusion two registries exist to prevent.
        Assert.All(AllPolledKinds, kind => Assert.Null(InboundTransports.Find(kind)));
    }

    [Fact]
    public void TheSourceDescriptorsDeclareTheThreeFlagsThatDriveTheConsole()
    {
        foreach (var kind in AllPolledKinds)
        {
            var descriptor = PolledTransports.Find(kind)!.Describe();

            Assert.True(descriptor.Polled, "a polled kind runs on the source's Schedule");
            // For a row source the SELECT list IS the mapping; a second way to say it can only disagree.
            Assert.False(descriptor.Mapping);
            Assert.True(descriptor.CanProbe);
            Assert.IsAssignableFrom<ISchemaProbe>(PolledTransports.Find(kind)!);
            Assert.Equal("db", descriptor.ConfigProperty);
        }
    }

    [Fact]
    public void TheCursorColumnsHelpCarriesTheTimestampHazardWhereAnOperatorWillReadIt()
    {
        var field = Assert.Single(
            PolledTransports.Find(SourceKinds.Postgres)!.Describe().Fields, f => f.Key == "cursorColumn");

        Assert.NotNull(field.Help);
        Assert.Contains("updated_at", field.Help, StringComparison.Ordinal);
        Assert.Contains("same millisecond", field.Help, StringComparison.Ordinal);
        Assert.Contains("CDC", field.Help, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSinkHelpStatesTheAtMostOnceCeilingInPlainWords()
    {
        foreach (var kind in new[] { SinkKinds.Postgres, SinkKinds.MsSql })
        {
            var help = SinkTransports.Find(kind)!.Describe().Help!;

            Assert.Contains("AT MOST ONCE", help, StringComparison.Ordinal);
            Assert.Contains("DROPPED", help, StringComparison.Ordinal);
            Assert.Contains("no DDL", help, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SqlHeavyFieldsAreDeclaredAsTextSoTheConsoleGivesThemATextarea()
    {
        var source = PolledTransports.Find(SourceKinds.Postgres)!.Describe();

        Assert.Equal(TransportFieldTypes.Text, source.Fields.Single(f => f.Key == "query").Type);
        Assert.Equal(TransportFieldTypes.Text, source.Fields.Single(f => f.Key == "where").Type);
        Assert.Equal(TransportFieldTypes.Text, SinkTransports.Find(SinkKinds.Postgres)!.Describe().Fields.Single(f => f.Key == "columns").Type);
    }

    [Fact]
    public void EverySecretFieldMatchesAnActualSecretProperty()
    {
        // A field typed "secret" that is NOT a [Secret] property would render masked in the console while
        // being EXPORTED IN PLAINTEXT. TransportRegistryTests pins this for the NATS descriptors; these
        // six (four polled + two sink) live out of tree, so the equivalent assertion has to live here.
        // All four polled kinds share DbSourceConfig (plan 017 kept the CDC fields on the same class — see
        // its doc comment), so the same helper runs over all four rather than a separate CDC variant.
        Assert.All(AllPolledKinds, kind => AssertSecretsAgree(PolledTransports.Find(kind)!.Describe(), typeof(DbSourceConfig)));
        AssertSecretsAgree(SinkTransports.Find(SinkKinds.Postgres)!.Describe(), typeof(DbSinkConfig));
        AssertSecretsAgree(SinkTransports.Find(SinkKinds.MsSql)!.Describe(), typeof(DbSinkConfig));

        static void AssertSecretsAgree(TransportDescriptor descriptor, Type configType)
        {
            var declared = configType.GetProperties()
                .Where(p => p.IsDefined(typeof(SecretAttribute), inherit: true))
                .Select(p => char.ToLowerInvariant(p.Name[0]) + p.Name[1..])
                .ToHashSet(StringComparer.Ordinal);

            var described = descriptor.Fields
                .Where(f => f.Type == TransportFieldTypes.Secret)
                .Select(f => f.Key)
                .ToHashSet(StringComparer.Ordinal);

            Assert.Equal(declared, described);
        }
    }

    [Fact]
    public void EveryDescriptorFieldNamesARealPropertyOfItsConfigObject()
    {
        // The console reads and writes connector.db[key] generically, so a key with no property behind it
        // is an input that silently writes nowhere. All four polled kinds share DbSourceConfig, so the
        // same helper runs over all four.
        Assert.All(AllPolledKinds, kind => AssertKeysExist(PolledTransports.Find(kind)!.Describe(), typeof(DbSourceConfig)));
        AssertKeysExist(SinkTransports.Find(SinkKinds.Postgres)!.Describe(), typeof(DbSinkConfig));

        static void AssertKeysExist(TransportDescriptor descriptor, Type configType)
        {
            var properties = configType.GetProperties()
                .Select(p => char.ToLowerInvariant(p.Name[0]) + p.Name[1..])
                .ToHashSet(StringComparer.Ordinal);

            Assert.All(descriptor.Fields, f => Assert.Contains(f.Key, properties));
        }
    }

    [Fact]
    public void TheDescriptorsMeetTheSameShapeRulesTheCatalogPinsForEveryOtherTransport()
    {
        List<TransportDescriptor> descriptors =
        [
            .. AllPolledKinds.Select(k => PolledTransports.Find(k)!.Describe()),
            .. new[] { SinkKinds.Postgres, SinkKinds.MsSql }.Select(k => SinkTransports.Find(k)!.Describe()),
        ];

        Assert.All(descriptors, d =>
        {
            Assert.False(string.IsNullOrWhiteSpace(d.Label));
            Assert.False(string.IsNullOrWhiteSpace(d.ConfigProperty));
            Assert.NotEmpty(d.Fields);

            var groups = d.Groups.Select(g => g.Key).ToHashSet(StringComparer.Ordinal);
            Assert.All(d.Fields, f => Assert.True(f.Group is null || groups.Contains(f.Group), $"{d.Kind}.{f.Key} names an undeclared group '{f.Group}'"));

            Assert.All(d.Fields, f => Assert.Equal(f.Type == TransportFieldTypes.Select, f.Options is { Count: > 0 }));
        });
    }

    [Fact]
    public void TheFourKindsAreTheContractsOwnConstants()
    {
        Assert.Equal("postgres", SourceKinds.Postgres);
        Assert.Equal("mssql", SourceKinds.MsSql);
        Assert.Equal("postgres-cdc", SourceKinds.PostgresCdc);
        Assert.Equal("mssql-cdc", SourceKinds.MsSqlCdc);
        Assert.Equal(SourceKinds.Postgres, SinkKinds.Postgres);
        Assert.Equal(SourceKinds.MsSql, SinkKinds.MsSql);
    }
}
