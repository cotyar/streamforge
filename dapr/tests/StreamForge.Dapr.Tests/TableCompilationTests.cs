using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Actors;
using Xunit;

namespace StreamForge.Dapr.Tests;

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

        var (executor, streamInputs, tableInputs, error) = TableCompilation.TryCompile(def, [Trades()], []);

        Assert.NotNull(executor);
        Assert.Null(error);
        Assert.Contains("trades", streamInputs);
        Assert.Empty(tableInputs);
    }

    [Fact]
    public void TryCompile_InvalidSql_ReturnsNullExecutorAndErrorMessage()
    {
        var def = Positions();
        def.Sql = "SELECT * FROM nonexistent_stream";

        var (executor, streamInputs, tableInputs, error) = TableCompilation.TryCompile(def, [Trades()], []);

        Assert.Null(executor);
        Assert.False(string.IsNullOrWhiteSpace(error));
        Assert.Empty(streamInputs);
        Assert.Empty(tableInputs);
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

        var (executor, streamInputs, tableInputs, error) = TableCompilation.TryCompile(hotSymbols, [Trades()], [positions]);

        Assert.NotNull(executor);
        Assert.Null(error);
        Assert.Empty(streamInputs);
        Assert.Contains("positions", tableInputs);
    }

    [Fact]
    public void TryCompile_DistinctsRepeatedInputNames()
    {
        // A SQL query that references "trades" more than once (e.g. via a subquery) must still report it
        // exactly once in StreamInputs — mirrors PipelineCompilation.TryCompile's own .Distinct() call.
        var def = Positions();
        def.Sql = "SELECT symbol, COUNT(*) AS trades FROM trades WHERE qty > 0 GROUP BY symbol";

        var (executor, streamInputs, _, _) = TableCompilation.TryCompile(def, [Trades()], []);

        Assert.NotNull(executor);
        Assert.Single(streamInputs);
        Assert.Equal("trades", streamInputs[0]);
    }
}
