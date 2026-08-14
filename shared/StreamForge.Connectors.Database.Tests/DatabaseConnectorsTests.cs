using StreamForge.Abstractions;
using StreamForge.AppCore.Sinks;
using StreamForge.AppCore.Transports;
using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

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

    [Fact]
    public void RegisterAllPutsBothKindsInBothRegistries()
    {
        Assert.NotNull(PolledTransports.Find(SourceKinds.Postgres));
        Assert.NotNull(PolledTransports.Find(SourceKinds.MsSql));
        Assert.NotNull(SinkTransports.Find(SinkKinds.Postgres));
        Assert.NotNull(SinkTransports.Find(SinkKinds.MsSql));
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
        Assert.Null(InboundTransports.Find(SourceKinds.Postgres));
        Assert.Null(InboundTransports.Find(SourceKinds.MsSql));
    }

    [Fact]
    public void TheSourceDescriptorsDeclareTheThreeFlagsThatDriveTheConsole()
    {
        foreach (var kind in new[] { SourceKinds.Postgres, SourceKinds.MsSql })
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
        // four live out of tree, so the equivalent assertion has to live here.
        AssertSecretsAgree(PolledTransports.Find(SourceKinds.Postgres)!.Describe(), typeof(DbSourceConfig));
        AssertSecretsAgree(PolledTransports.Find(SourceKinds.MsSql)!.Describe(), typeof(DbSourceConfig));
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
        // is an input that silently writes nowhere.
        AssertKeysExist(PolledTransports.Find(SourceKinds.Postgres)!.Describe(), typeof(DbSourceConfig));
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
            .. new[] { SourceKinds.Postgres, SourceKinds.MsSql }.Select(k => PolledTransports.Find(k)!.Describe()),
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
    public void TheTwoKindsAreTheContractsOwnConstants()
    {
        Assert.Equal("postgres", SourceKinds.Postgres);
        Assert.Equal("mssql", SourceKinds.MsSql);
        Assert.Equal(SourceKinds.Postgres, SinkKinds.Postgres);
        Assert.Equal(SourceKinds.MsSql, SinkKinds.MsSql);
    }
}
