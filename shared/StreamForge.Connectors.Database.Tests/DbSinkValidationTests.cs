using StreamForge.Abstractions;
using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

/// <summary>
/// The first real implementation of <c>ISinkTransport.Validate</c> in this repo — the seam plan 014 added
/// precisely because a sink with a broken config was previously just <c>IsConfigured == false</c>, i.e.
/// silently inert with nothing an operator could act on.
/// </summary>
public class DbSinkValidationTests
{
    private static readonly DbSink Sink = new(new PostgresDialect());

    private static SinkSpec Spec(Action<DbSinkConfig>? tweak = null)
    {
        DbSinkConfig config = new() { Host = "db", Database = "market", Table = "trades", Mode = DbSinkModes.Append };
        tweak?.Invoke(config);
        return new SinkSpec { Kind = SinkKinds.Postgres, Enabled = true, Db = config };
    }

    private static List<string> Errors(SinkSpec spec)
    {
        List<string> errors = [];
        Sink.Validate(spec, errors);
        return errors;
    }

    [Fact]
    public void AWellFormedAppendSinkIsAccepted() => Assert.Empty(Errors(Spec()));

    [Fact]
    public void AWellFormedUpsertSinkIsAccepted()
        => Assert.Empty(Errors(Spec(c => { c.Mode = DbSinkModes.Upsert; c.KeyColumns = "symbol, venue"; })));

    [Fact]
    public void AMissingConfigObjectIsRejected()
        => Assert.Single(Errors(new SinkSpec { Kind = SinkKinds.Postgres, Enabled = true }));

    [Fact]
    public void HostAndDatabaseAreRequiredUnlessAConnectionStringSuppliesThem()
    {
        Assert.Contains(Errors(Spec(c => c.Host = "")), e => e.Contains("host + database", StringComparison.Ordinal));
        Assert.DoesNotContain(
            Errors(Spec(c => { c.Host = ""; c.Database = ""; c.ConnectionString = "Host=elsewhere;Database=other"; })),
            e => e.Contains("host + database", StringComparison.Ordinal));
    }

    [Fact]
    public void AMissingTableIsRejectedAndTheMessageSaysThisSinkIssuesNoDdl()
    {
        var error = Assert.Single(Errors(Spec(c => c.Table = "")));

        Assert.Contains("no DDL", error, StringComparison.Ordinal);
    }

    [Fact]
    public void UpsertWithoutKeyColumnsIsRejected()
    {
        var errors = Errors(Spec(c => c.Mode = DbSinkModes.Upsert));

        Assert.Contains(errors, e => e.Contains("keyColumns", StringComparison.Ordinal));
    }

    [Fact]
    public void UpsertWithIncludeWeightIsRejectedBecauseTheWeightIsTheOperation()
    {
        var errors = Errors(Spec(c => { c.Mode = DbSinkModes.Upsert; c.KeyColumns = "symbol"; c.IncludeWeight = true; }));

        Assert.Contains(errors, e => e.Contains("includeWeight", StringComparison.Ordinal));
    }

    [Fact]
    public void AppendWithIncludeWeightIsFine()
        => Assert.Empty(Errors(Spec(c => c.IncludeWeight = true)));

    [Fact]
    public void AnUnknownModeIsRejected()
        => Assert.Contains(Errors(Spec(c => c.Mode = "merge")), e => e.Contains("mode must be", StringComparison.Ordinal));

    [Fact]
    public void IsConfiguredNeedsBothAnAddressAndATable()
    {
        Assert.True(Sink.IsConfigured(Spec()));
        Assert.False(Sink.IsConfigured(Spec(c => c.Table = "")));
        Assert.False(Sink.IsConfigured(Spec(c => { c.Host = ""; c.Database = ""; })));
        Assert.False(Sink.IsConfigured(new SinkSpec { Kind = SinkKinds.Postgres }));
    }

    [Fact]
    public void CreateReturnsAClientBoundToTheEntityItWasAskedFor()
    {
        var client = Sink.Create(Spec(), "table", "trades", null);

        Assert.Equal("trades", client.EntityName);
        Assert.IsType<DbSinkClient>(client);
    }
}
