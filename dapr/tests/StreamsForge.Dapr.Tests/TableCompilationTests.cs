using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Actors;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-A: unit tests for <see cref="TableCompilation.TryCompile"/> — the
/// pure SQL-compile-to-executor logic <see cref="TableActor"/> extracts specifically so it's testable
/// without any actor/timer/Dapr-sidecar machinery (mirrors <c>PipelineCompilationTests</c>'s own
/// rationale). Covers the CLASSIC (Parallelism==1) path only — see <see cref="ITableActor"/>'s class doc
/// for the partitioned-execution descope (decision D-F).
/// </summary>
public class TableCompilationTests
{
    private static SourceDefinition Trades() => new()
    {
        Name = "trades",
        Fields =
        [
            new FieldDef("symbol", FieldType.String),
            new FieldDef("qty", FieldType.Long),
            new FieldDef("price", FieldType.Double),
        ],
        Enabled = true,
    };

    private static TableDefinition Positions() => new()
    {
        Id = "positions-id",
        Name = "positions",
        Sql = "SELECT symbol, COUNT(*) AS trades, SUM(qty) AS total_qty FROM trades GROUP BY symbol",
        Status = PipelineStatus.Stopped,
    };

    [Fact]
    public void TryCompile_ValidStreamOnlySql_ReturnsExecutorAndStreamInputs()
    {
        var def = Positions();

        var (executor, streamInputs, tableInputs, pipelineInputs, error) = TableCompilation.TryCompile(def, [Trades()], []);

        Assert.NotNull(executor);
        Assert.Null(error);
        Assert.Contains("trades", streamInputs);
        Assert.Empty(tableInputs);
        Assert.Empty(pipelineInputs);
    }

    [Fact]
    public void TryCompile_InvalidSql_ReturnsNullExecutorAndErrorMessage()
    {
        var def = Positions();
        def.Sql = "SELECT * FROM nonexistent_stream";

        var (executor, streamInputs, tableInputs, pipelineInputs, error) = TableCompilation.TryCompile(def, [Trades()], []);

        Assert.Null(executor);
        Assert.False(string.IsNullOrWhiteSpace(error));
        Assert.Empty(streamInputs);
        Assert.Empty(tableInputs);
        Assert.Empty(pipelineInputs);
    }

    [Fact]
    public void TryCompile_TableOverTableSql_ReturnsTableInputs()
    {
        var positions = Positions();
        // Give "positions" an OutputFields shape as if it had already compiled once (mirrors what
        // Catalog.CatalogStore persists after a successful compile) — TableCompilation.TryCompile only
        // considers upstream tables with a non-empty OutputFields as valid FROM/JOIN targets, exactly like
        // TableGrain.StartClassicAsync's own `tables.Where(t => t.OutputFields.Count > 0)` filter.
        positions.OutputFields =
        [
            new FieldDef("symbol", FieldType.String),
            new FieldDef("trades", FieldType.Long),
            new FieldDef("total_qty", FieldType.Long),
        ];

        var hotSymbols = new TableDefinition
        {
            Id = "hot-id",
            Name = "hot_symbols",
            Sql = "SELECT p.symbol, p.trades FROM positions p WHERE p.trades > 50",
            Status = PipelineStatus.Stopped,
        };

        var (executor, streamInputs, tableInputs, pipelineInputs, error) = TableCompilation.TryCompile(hotSymbols, [Trades()], [positions]);

        Assert.NotNull(executor);
        Assert.Null(error);
        Assert.Empty(streamInputs);
        Assert.Contains("positions", tableInputs);
        Assert.Empty(pipelineInputs);
    }

    [Fact]
    public void TryCompile_DistinctsRepeatedInputNames()
    {
        // A SQL query that references "trades" more than once (e.g. via a subquery) must still report it
        // exactly once in StreamInputs — mirrors PipelineCompilation.TryCompile's own .Distinct() call.
        var def = Positions();
        def.Sql = "SELECT symbol, COUNT(*) AS trades FROM trades WHERE qty > 0 GROUP BY symbol";

        var (executor, streamInputs, _, _, _) = TableCompilation.TryCompile(def, [Trades()], []);

        Assert.NotNull(executor);
        Assert.Single(streamInputs);
        Assert.Equal("trades", streamInputs[0]);
    }

    // ------------------------------------------------------------------
    // Table-over-pipeline (plan 025) — a table may name a PIPELINE (one with a compiled OutputFields) as a
    // relation, exactly like a source or another table. See Catalog.CatalogStore's identical
    // BuildTableStreamSchemas/ApplyCompileResult split for the shared server-side rule this mirrors.
    // ------------------------------------------------------------------

    private static PipelineDefinition VwapPipeline() => new()
    {
        Id = "vwap-id",
        Name = "vwap",
        Sql = "SELECT symbol, AVG(price) AS avg_price FROM trades GROUP BY symbol",
        OutputFields = [new FieldDef("symbol", FieldType.String), new FieldDef("avg_price", FieldType.Double)],
    };

    [Fact]
    public void TryCompile_TableOverPipelineSql_ReturnsPipelineInputs_NotStreamInputs()
    {
        var def = new TableDefinition
        {
            Id = "over-pipeline-id",
            Name = "vwap_table",
            Sql = "SELECT symbol, avg_price FROM vwap",
        };

        var (executor, streamInputs, tableInputs, pipelineInputs, error) =
            TableCompilation.TryCompile(def, [Trades()], [], [VwapPipeline()]);

        Assert.NotNull(executor);
        Assert.Null(error);
        Assert.Empty(streamInputs);
        Assert.Empty(tableInputs);
        Assert.Contains("vwap", pipelineInputs);
    }

    [Fact]
    public void TryCompile_PipelineWithEmptyOutputFields_OffersNoRelation()
    {
        // A pipeline that has never compiled (or was written before OutputFields existed) contributes NO
        // relation — naming it is then an ordinary "unknown relation" diagnostic, mirroring
        // Catalog.CatalogStore.BuildTableStreamSchemas' own "continue" for an empty OutputFields.
        var draftPipeline = VwapPipeline();
        draftPipeline.OutputFields = [];

        var def = new TableDefinition { Id = "x", Name = "x", Sql = "SELECT symbol FROM vwap" };

        var (executor, _, _, _, error) = TableCompilation.TryCompile(def, [Trades()], [], [draftPipeline]);

        Assert.Null(executor);
        Assert.False(string.IsNullOrWhiteSpace(error));
    }

    [Fact]
    public void TryCompile_SourceAndPipelineSameName_SchemaComesFromSource()
    {
        // Defensive tiebreak mirroring Catalog.CatalogStore.BuildTableStreamSchemas' own doc comment — the
        // write paths (ValidateUniquePipelineName/UpsertSourceAsync) refuse this collision at CREATE time,
        // so this state should never be reachable in practice; this only proves the SCHEMA fallback
        // ordering (sources win the DICTIONARY entry). The pipeline's schema differs from the source's so
        // the two are distinguishable: if the pipeline's schema had won, "qty" (source-only) would not
        // compile. Note what this does NOT prove: the relation is still CLASSIFIED as a pipeline input
        // (not a stream input) below, because that split is purely name-membership-based — mirrors
        // RegistryGrain.ApplyCompileResult's identical behavior for the identical unreachable state.
        var collidingPipeline = VwapPipeline();
        collidingPipeline.Name = "trades";
        var def = new TableDefinition { Id = "x", Name = "x", Sql = "SELECT symbol, qty FROM trades" };

        var (executor, streamInputs, _, pipelineInputs, error) =
            TableCompilation.TryCompile(def, [Trades()], [], [collidingPipeline]);

        Assert.NotNull(executor);
        Assert.Null(error);
        Assert.Empty(streamInputs);
        Assert.Contains("trades", pipelineInputs);
    }
}
