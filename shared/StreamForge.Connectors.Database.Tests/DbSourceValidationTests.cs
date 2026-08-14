using StreamForge.Abstractions;
using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

/// <summary>The per-kind half of source validation. Every rule here exists because the alternative is a
/// source that runs and quietly does nothing useful — the failure mode this repo's <c>ISinkTransport.Validate</c>
/// doc comment calls out by name.</summary>
public class DbSourceValidationTests
{
    private static readonly DbSource Source = new(new PostgresDialect());

    private static SourceDefinition Definition(Action<DbSourceConfig>? tweak = null)
    {
        DbSourceConfig config = new()
        {
            Host = "db",
            Database = "market",
            Table = "trades",
            CursorColumn = "id",
            CursorKind = CursorKinds.Long,
            BatchSize = 1000,
        };
        tweak?.Invoke(config);
        return new SourceDefinition { Name = "trades", Kind = SourceKinds.Postgres, Connector = new ConnectorConfig { Db = config } };
    }

    private static List<string> Errors(SourceDefinition def)
    {
        List<string> errors = [];
        Source.Validate(def, errors);
        return errors;
    }

    [Fact]
    public void AWellFormedTableSourceIsAccepted()
        => Assert.Empty(Errors(Definition()));

    [Fact]
    public void AMissingConfigObjectIsTheFirstAndOnlyComplaint()
    {
        var errors = Errors(new SourceDefinition { Name = "x", Kind = SourceKinds.Postgres });

        Assert.Equal("kind 'postgres' requires connector.db", Assert.Single(errors));
    }

    [Fact]
    public void HostAndDatabaseAreRequiredUnlessAConnectionStringSuppliesThem()
    {
        Assert.Contains(Errors(Definition(c => c.Host = "")), e => e.Contains("host + database", StringComparison.Ordinal));
        Assert.Contains(Errors(Definition(c => c.Database = "")), e => e.Contains("host + database", StringComparison.Ordinal));

        // The escape hatch satisfies it wholesale — that is what it is for.
        Assert.DoesNotContain(
            Errors(Definition(c => { c.Host = ""; c.Database = ""; c.ConnectionString = "Host=elsewhere;Database=other"; })),
            e => e.Contains("host + database", StringComparison.Ordinal));
    }

    [Fact]
    public void ATableOrAQueryIsRequired()
        => Assert.Contains(Errors(Definition(c => c.Table = "")), e => e.Contains("needs a table", StringComparison.Ordinal));

    [Fact]
    public void ACursorColumnIsRequiredBecauseWithoutItThereIsNoOrderingNoWatermarkAndNoPaging()
        => Assert.Contains(Errors(Definition(c => c.CursorColumn = "")), e => e.Contains("cursorColumn", StringComparison.Ordinal));

    [Fact]
    public void ACustomQueryMustContainTheCursorPlaceholder()
    {
        var errors = Errors(Definition(c => { c.Query = "SELECT * FROM trades"; c.InitialCursor = "0"; }));

        Assert.Contains(errors, e => e.Contains("@cursor", StringComparison.Ordinal));
        // The reason matters as much as the rule: bound, never interpolated.
        Assert.Contains(errors, e => e.Contains("never interpolated", StringComparison.Ordinal));
    }

    [Fact]
    public void ACustomQueryNeedsAnInitialCursorBecauseThereIsNoMaxToSeedFrom()
    {
        var errors = Errors(Definition(c => c.Query = "SELECT * FROM trades WHERE id > @cursor"));

        Assert.Contains(errors, e => e.Contains("initialCursor", StringComparison.Ordinal));
    }

    [Fact]
    public void AWellFormedQuerySourceIsAccepted()
        => Assert.Empty(Errors(Definition(c =>
        {
            c.Table = "";
            c.Query = "SELECT id, symbol FROM trades WHERE id > @cursor ORDER BY id LIMIT 100";
            c.InitialCursor = "0";
        })));

    [Fact]
    public void AnUnknownCursorKindIsRejected()
        => Assert.Contains(Errors(Definition(c => c.CursorKind = "lsn")), e => e.Contains("cursorKind", StringComparison.Ordinal));

    [Fact]
    public void AnInitialCursorThatDoesNotParseForItsKindIsRejectedBeforeItCanFailEveryCycle()
    {
        Assert.Contains(Errors(Definition(c => c.InitialCursor = "yesterday")), e => e.Contains("initialCursor", StringComparison.Ordinal));

        Assert.Contains(
            Errors(Definition(c => { c.CursorKind = CursorKinds.Timestamp; c.InitialCursor = "17"; })),
            e => e.Contains("initialCursor", StringComparison.Ordinal));
    }

    [Fact]
    public void AZeroBatchSizeIsRejected()
        => Assert.Contains(Errors(Definition(c => c.BatchSize = 0)), e => e.Contains("batchSize", StringComparison.Ordinal));

    [Fact]
    public void ValidationNeverThrowsOnAnEmptyConfig()
    {
        // Never throws is part of the SPI contract; an empty object is what a half-created source looks like.
        List<string> errors = [];
        Source.Validate(new SourceDefinition { Kind = SourceKinds.Postgres, Connector = new ConnectorConfig { Db = new DbSourceConfig() } }, errors);

        Assert.NotEmpty(errors);
    }

    [Fact]
    public async Task PollingASourceWithNoConfigFailsWithANamedMessageRatherThanANullReference()
    {
        // PolledSourceCore turns a throw into an error status with the cursor untouched, so the message is
        // the whole of what an operator gets to see.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => Source.PollAsync(new SourceDefinition { Name = "trades", Kind = SourceKinds.Postgres }, null, CancellationToken.None));

        Assert.Contains("connector.db", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AQueryWithNoStartingPointFailsBeforeAnyConnectionIsAttempted()
    {
        // The host here does not exist. Reaching the planner's refusal proves the failure is structural
        // rather than a connection timeout wearing a confusing message.
        var def = Definition(c =>
        {
            c.Host = "no.such.host.invalid";
            c.Query = "SELECT * FROM trades WHERE id > @cursor";
        });

        var started = DateTimeOffset.UtcNow;
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Source.PollAsync(def, null, CancellationToken.None));

        Assert.Contains("initialCursor", ex.Message, StringComparison.Ordinal);
        Assert.True(DateTimeOffset.UtcNow - started < TimeSpan.FromSeconds(1));
    }
}
