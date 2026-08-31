using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 008 W3 — `UNION ALL` end to end in PIPELINE mode. A union-root PipelineExecutor is structurally "N
/// nested executors, no join/filter/project/window stage of its own, concatenate emissions" — each branch
/// is a complete, independently-compiled CompiledPlan whose own WHERE/projection already ran before its
/// emissions reach the union root (see ExecutorImpl.EnsureInitUnion). These tests exercise that fan-out/
/// concatenation shape live: three branches, the same source feeding two of them, and no dedup (pipeline
/// mode has no Z-set weights — see SetOperationValidatorTests for the UNION-distinct rejection).
/// </summary>
public class UnionAllPipelineTests
{
    // Branch 0: trades.price AS amt. Branch 1: quotes.bid AS amt. Branch 2: quotes.ask AS amt — the SAME
    // source ("quotes") feeds two different branches, proving fan-out; three branches overall proves the
    // chain isn't hardcoded to two.
    private const string ThreeBranchSql =
        "SELECT symbol, price AS amt FROM trades " +
        "UNION ALL SELECT symbol, bid AS amt FROM quotes " +
        "UNION ALL SELECT symbol, ask AS amt FROM quotes";

    [Fact]
    public void CompilesAndReportsAUnifiedTwoColumnOutputSchema()
    {
        var r = Compile(ThreeBranchSql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(2, r.OutputSchema!.Fields.Count);
        Assert.Equal(FieldKind.String, r.OutputSchema.Fields["symbol"]);
        Assert.Equal(FieldKind.Double, r.OutputSchema.Fields["amt"]);
    }

    [Fact]
    public void EventOnASourceUsedByOnlyOneBranchProducesExactlyOneRow()
    {
        var exec = CompileAndCreate(ThreeBranchSql, Trades, Quotes);

        var results = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 101.5), ("qty", 1L), ("active", true)));

        var row = Assert.Single(results);
        Assert.Equal("AAPL", row["symbol"]);
        Assert.Equal(101.5, row["amt"]);
    }

    [Fact]
    public void EventOnASourceSharedByTwoBranchesFansOutToBoth()
    {
        // "quotes" feeds branch 1 (bid AS amt) AND branch 2 (ask AS amt) — a SINGLE incoming quotes event
        // must reach both nested branch executors and produce one row from each (plan 008's own "one
        // event/delta must be fanned out to every branch that subscribes to that input" requirement).
        var exec = CompileAndCreate(ThreeBranchSql, Trades, Quotes);

        var results = exec.OnEvent("quotes", Evt(1000, "quotes", ("symbol", "AAPL"), ("bid", 99.0), ("ask", 101.0)));

        Assert.Equal(2, results.Count);
        var amounts = results.Select(r => (double)r["amt"]!).OrderBy(x => x).ToList();
        Assert.Equal([99.0, 101.0], amounts);
        Assert.All(results, r => Assert.Equal("AAPL", r["symbol"]));
    }

    [Fact]
    public void NoDedupIdenticalRowsFromDifferentBranchesBothAppear()
    {
        // Two branches over the SAME underlying source, with WHERE clauses that both admit the same row —
        // UNION ALL in pipeline mode never dedups (there is no Z-set weight to dedup with): the identical
        // row appears twice in the output.
        var sql = "SELECT symbol FROM trades WHERE price > 0 UNION ALL SELECT symbol FROM trades WHERE qty > 0";
        var exec = CompileAndCreate(sql, Trades);

        var results = exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 10.0), ("qty", 5L), ("active", true)));

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("AAPL", r["symbol"]));
    }

    [Fact]
    public void AdvanceWatermarkDrivesEveryBranchAndClosesWindowedBranches()
    {
        // Branch 0 is windowed (needs AdvanceWatermark to close and emit); branch 1 is a plain unwindowed
        // passthrough (emits immediately on OnEvent). The union root must drive BOTH branches' own
        // AdvanceWatermark and concatenate whatever each one emits.
        var sql =
            "SELECT symbol, AVG(price) AS amt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL " +
            "UNION ALL SELECT symbol, bid AS amt FROM quotes";
        var exec = CompileAndCreate(sql, Trades, Quotes);

        Assert.Empty(exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true))));
        var quoteResult = exec.OnEvent("quotes", Evt(1500, "quotes", ("symbol", "AAPL"), ("bid", 50.0), ("ask", 51.0)));
        var quoteRow = Assert.Single(quoteResult);
        Assert.Equal(50.0, quoteRow["amt"]);

        var closed = exec.AdvanceWatermark(12000);
        var windowRow = Assert.Single(closed);
        Assert.Equal("AAPL", windowRow["symbol"]);
        Assert.Equal(100.0, windowRow["amt"]);
    }

    [Fact]
    public void PlanSummaryNamesUnionAllAndTheOutputColumnCount()
    {
        var r = Compile(ThreeBranchSql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("UNION ALL", r.PlanSummary);
        Assert.Contains("SELECT 2", r.PlanSummary);
    }

    [Fact]
    public void SourceNamesUnionAcrossBranchesWithoutDuplicates()
    {
        // "quotes" is a source in TWO branches — CompileResult.SourceNames must still list it once.
        var r = Compile(ThreeBranchSql, Trades, Quotes);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(new[] { "trades", "quotes" }.OrderBy(x => x), r.SourceNames.OrderBy(x => x));
    }
}
