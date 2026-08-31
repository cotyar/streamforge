using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Actors;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W6: unit tests for <see cref="PipelineCompilation.TryCompile"/> — the
/// pure SQL-compile-to-executor logic extracted from <see cref="PipelineActor"/> specifically so it can
/// be tested without any actor/timer/Dapr-sidecar machinery (mirrors how
/// <see cref="GeneratorBatchingTests"/> exercises <see cref="GeneratorBatching"/> rather than
/// <see cref="GeneratorActor"/> directly).
/// </summary>
public class PipelineCompilationTests
{
    private static SourceDefinition Trades() => new()
    {
        Name = "trades",
        Fields =
        [
            new FieldDef("symbol", FieldType.String),
            new FieldDef("price", FieldType.Double),
            new FieldDef("qty", FieldType.Long),
        ],
    };

    private static SourceDefinition Quotes() => new()
    {
        Name = "quotes",
        Fields =
        [
            new FieldDef("symbol", FieldType.String),
            new FieldDef("bid", FieldType.Double),
        ],
    };

    [Fact]
    public void TryCompile_ValidSql_ReturnsExecutorAndSourceNames()
    {
        var def = new PipelineDefinition
        {
            Id = "p1",
            Sql = "SELECT symbol, SUM(price * qty) AS notional FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)",
        };

        var (executor, sourceNames, error) = PipelineCompilation.TryCompile(def, [Trades()]);

        Assert.NotNull(executor);
        Assert.Null(error);
        Assert.Equal(["trades"], sourceNames);
    }

    [Fact]
    public void TryCompile_MultiSourceJoin_ReturnsBothDistinctSourceNames()
    {
        var def = new PipelineDefinition
        {
            Id = "p1",
            Sql = "SELECT t.symbol, t.price, q.bid FROM trades t JOIN quotes q WITHIN 5 SECONDS ON t.symbol = q.symbol",
        };

        var (executor, sourceNames, error) = PipelineCompilation.TryCompile(def, [Trades(), Quotes()]);

        Assert.NotNull(executor);
        Assert.Null(error);
        Assert.Equal(2, sourceNames.Count);
        Assert.Contains("trades", sourceNames);
        Assert.Contains("quotes", sourceNames);
    }

    [Fact]
    public void TryCompile_UnknownSource_ReturnsNullExecutorAndDiagnosticMessage()
    {
        var def = new PipelineDefinition { Id = "p1", Sql = "SELECT * FROM nonexistent" };

        var (executor, sourceNames, error) = PipelineCompilation.TryCompile(def, [Trades()]);

        Assert.Null(executor);
        Assert.Empty(sourceNames);
        Assert.NotNull(error);
        Assert.NotEmpty(error!);
    }

    [Fact]
    public void TryCompile_SyntacticallyInvalidSql_ReturnsNullExecutorAndDiagnosticMessage()
    {
        var def = new PipelineDefinition { Id = "p1", Sql = "SELEKT * FROM trades" };

        var (executor, _, error) = PipelineCompilation.TryCompile(def, [Trades()]);

        Assert.Null(executor);
        Assert.NotNull(error);
    }

    [Fact]
    public void TryCompile_EmptySourceList_FailsWithDiagnostic()
    {
        // This dialect requires a FROM target that resolves against a known schema (like ksqlDB) — an
        // empty Sources list can never compile anything beyond a syntax check, mirroring
        // PipelineGrain.StartAsync throwing when GetSourcesAsync() comes back empty for a SQL that
        // references a stream.
        var def = new PipelineDefinition { Id = "p1", Sql = "SELECT symbol FROM trades" };

        var (executor, sourceNames, error) = PipelineCompilation.TryCompile(def, []);

        Assert.Null(executor);
        Assert.Empty(sourceNames);
        Assert.NotNull(error);
    }
}
