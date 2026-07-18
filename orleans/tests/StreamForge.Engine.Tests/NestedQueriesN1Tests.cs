using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 004 N1 — derived tables (`FROM ( SELECT ... ) alias`) and WITH (CTEs, desugared to derived tables
/// at parse time — see Parser.SubstituteCtes). Covers grammar (parser), scope-stack validation, and
/// end-to-end executor semantics for both pipeline (windowed) and table (retraction-propagating) modes,
/// riding plan 003 M1's IPipelineOpChain composability seam (ExecutorImpl's derived-node wiring) and table
/// mode's existing table-over-table chaining machinery (TableExecutorImpl's derived-node wiring).
/// </summary>
public class NestedQueriesN1Tests
{
    // ------------------------------------------------------------------
    // Parser
    // ------------------------------------------------------------------

    [Fact]
    public void DerivedTableWithoutAliasIsRejected()
    {
        var r = Compile("SELECT symbol FROM (SELECT symbol FROM trades)", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("requires an alias"));
    }

    [Fact]
    public void DerivedTableInFromWithAliasCompiles()
    {
        var r = Compile("SELECT d.symbol FROM (SELECT symbol FROM trades) d", Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void DerivedTableCanAppearInJoinPosition()
    {
        var sql = "SELECT t.symbol, d.tag FROM trades t JOIN (SELECT symbol, tag FROM ref) d WITHIN 1 SECONDS ON t.symbol = d.symbol";
        var r = Compile(sql, Trades, Ref);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void WithSingleCteDesugarsToDerivedTable()
    {
        var sql = "WITH hot AS (SELECT symbol FROM trades) SELECT hot.symbol FROM hot";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void WithMultipleCtesCanReferenceEarlierOnes()
    {
        var sql = "WITH a AS (SELECT symbol FROM trades), b AS (SELECT a.symbol FROM a) SELECT b.symbol FROM b";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void WithSelfReferenceIsRejectedAsRecursion()
    {
        var sql = "WITH x AS (SELECT symbol FROM x) SELECT symbol FROM x";
        var r = Compile(sql, Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Recursive or forward CTE reference"));
    }

    [Fact]
    public void WithForwardReferenceIsRejectedAsRecursion()
    {
        // 'a' references 'b', but 'b' is declared AFTER 'a' in the WITH list — forward reference.
        var sql = "WITH a AS (SELECT symbol FROM b), b AS (SELECT symbol FROM trades) SELECT symbol FROM a";
        var r = Compile(sql, Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Recursive or forward CTE reference 'b'"));
    }

    [Fact]
    public void DuplicateCteNameInWithListIsRejected()
    {
        var sql = "WITH a AS (SELECT symbol FROM trades), a AS (SELECT symbol FROM trades) SELECT symbol FROM a";
        var r = Compile(sql, Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Duplicate CTE name"));
    }

    // ------------------------------------------------------------------
    // Validator: scope stack
    // ------------------------------------------------------------------

    [Fact]
    public void DerivedAliasDuplicatingAnotherAliasIsError()
    {
        var sql = "SELECT * FROM trades t JOIN (SELECT symbol FROM ref) t ON true";
        var r = Compile(sql, Trades, Ref);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Duplicate alias 't'"));
    }

    [Fact]
    public void UnknownColumnInsideDerivedQueryKeepsInnerPosition()
    {
        // 'bogus' is on line 1; its column offset is inside the parenthesized inner SELECT — inner
        // diagnostics must keep their own (already-absolute, single-token-stream) position, not get
        // rewritten to point at the outer FROM.
        var sql = "SELECT d.symbol FROM (SELECT bogus FROM trades) d";
        var r = Compile(sql, Trades);
        Assert.False(r.Ok);
        var diag = Assert.Single(r.Diagnostics, d => d.Message.Contains("Unknown column 'bogus'"));
        Assert.Equal(1, diag.Line);
        Assert.Equal(30, diag.Column); // position of 'bogus' itself, not the derived table's '('
    }

    [Fact]
    public void OuterCannotSeeInnerDerivedTablesOwnAliases()
    {
        // 't' is an alias local to the derived query's own FROM — invisible at the outer level.
        var sql = "SELECT t.symbol FROM (SELECT symbol FROM trades t) d";
        var r = Compile(sql, Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unknown source 't'"));
    }

    [Fact]
    public void DerivedSubqueryIsUncorrelatedAndCannotSeeOuterAliases()
    {
        // N1 derived tables are uncorrelated: the inner query cannot reference the outer FROM's alias.
        var sql = "SELECT t.symbol FROM trades t JOIN (SELECT symbol FROM ref WHERE ref.symbol = t.symbol) d ON true";
        var r = Compile(sql, Trades, Ref);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("Unknown source 't'") || d.Message.Contains("Unknown column"));
    }

    [Fact]
    public void QualifiedStarOverDerivedAliasExpandsInnerProjectionColumns()
    {
        var sql = "SELECT d.* FROM (SELECT symbol, price FROM trades) d";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.String, r.OutputSchema!.Fields["symbol"]);
        Assert.Equal(FieldKind.Double, r.OutputSchema.Fields["price"]);
    }

    [Fact]
    public void DerivedTableOutputSchemaFeedsOuterColumnResolutionWithRenamedColumn()
    {
        var sql = "SELECT d.avg_px FROM (SELECT AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)) d";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.Double, r.OutputSchema!.Fields["avg_px"]);
    }

    [Fact]
    public void TableModeDerivedTableForbidsWindow()
    {
        var sql = "SELECT d.symbol FROM (SELECT symbol FROM trades WINDOW TUMBLING(SIZE 5 SECONDS)) d";
        var r = CompileTable(sql, Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("WINDOW clause not allowed in table mode"));
    }

    // ------------------------------------------------------------------
    // Executor — pipeline mode
    // ------------------------------------------------------------------

    [Fact]
    public void TwoLevelWindowedPipelineThroughTheParser()
    {
        // Upgrade of plan 003 M1's hand-built PipelineComposabilityTests smoke test: same scenario, same
        // expected result, but now compiled from ONE piece of SQL through WITH — no manual row-forwarding.
        var sql = "WITH hot AS (SELECT symbol, AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL) " +
                  "SELECT symbol, avg_px FROM hot WHERE avg_px > 50";
        var exec = CompileAndCreate(sql, Trades);

        var results = new List<EventRecord>();
        results.AddRange(exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true))));
        results.AddRange(exec.OnEvent("trades", Evt(1500, "trades", ("symbol", "AAPL"), ("price", 200.0), ("qty", 1L), ("active", true))));
        results.AddRange(exec.OnEvent("trades", Evt(2000, "trades", ("symbol", "MSFT"), ("price", 10.0), ("qty", 1L), ("active", true))));
        results.AddRange(exec.OnEvent("trades", Evt(2500, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 1L), ("active", true))));
        results.AddRange(exec.AdvanceWatermark(12000));

        var aapl = Assert.Single(results);
        Assert.Equal("AAPL", aapl["symbol"]);
        Assert.Equal(150.0, aapl["avg_px"]);
    }

    [Fact]
    public void DerivedCteFeedsAJoinInTheOuterChain()
    {
        var sql = "WITH hot AS (SELECT symbol, AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL) " +
                  "SELECT h.symbol, h.avg_px, q.bid FROM hot h JOIN quotes q WITHIN 1 SECONDS ON h.symbol = q.symbol";
        var exec = CompileAndCreate(sql, Trades, Quotes);

        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        // Advance by the MINIMUM nowMs that closes hot's inner window (end=10000, 1s allowed lateness ->
        // nowMs=11000): this single AdvanceWatermark call drives both the inner executor's watermark
        // (closing its window) AND the outer's own (shared nowMs) — picking the minimum keeps the outer's
        // watermark from outrunning the still-to-arrive quotes event below, same trap any interval join
        // has always had (advancing too far makes a legitimately-on-time event look late).
        // No quotes have arrived yet, so the closed-window emission finds no match: same "no matching
        // quote yet" shape as plan 003's hand-built PipelineComposabilityTests's analogous join test.
        var closed = exec.AdvanceWatermark(11000);
        Assert.Empty(closed);

        var matched = exec.OnEvent("quotes", Evt(10200, "quotes", ("symbol", "AAPL"), ("bid", 99.0), ("ask", 101.0)));
        var row = Assert.Single(matched);
        Assert.Equal("AAPL", row["symbol"]);
        Assert.Equal(100.0, row["avg_px"]);
        Assert.Equal(99.0, row["bid"]);
    }

    [Fact]
    public void WindowsInWindowsBucketsInnerEmissionsByWindowEndTimestamp()
    {
        // Inner: 5s tumbling windows close at t=5000 and t=10000. Outer: a wider 20s tumbling window over
        // the inner's emissions — both inner closes (ts 5000 and 10000) fall inside the SAME outer window
        // [0, 20000), so the outer COUNT(*) over hot's emissions should be 2 once both are closed and the
        // outer window itself closes.
        var sql = "WITH hot AS (SELECT symbol, AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS) EMIT FINAL) " +
                  "SELECT symbol, COUNT(*) AS cnt FROM hot GROUP BY symbol WINDOW TUMBLING(SIZE 20 SECONDS) EMIT FINAL";
        var exec = CompileAndCreate(sql, Trades);

        exec.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        exec.OnEvent("trades", Evt(6000, "trades", ("symbol", "AAPL"), ("price", 200.0), ("qty", 1L), ("active", true)));

        var results = exec.AdvanceWatermark(30000); // closes both inner windows AND the outer window

        var row = Assert.Single(results);
        Assert.Equal("AAPL", row["symbol"]);
        Assert.Equal(2L, row["cnt"]);
    }

    [Fact]
    public void PipelineCompileResultSourceNamesReportsRealLeafStreamsNotSyntheticDerivedMarker()
    {
        var sql = "WITH hot AS (SELECT symbol FROM trades) SELECT symbol FROM hot";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(["trades"], r.SourceNames);
    }

    // ------------------------------------------------------------------
    // Executor — table mode (retraction-correct)
    // ------------------------------------------------------------------

    [Fact]
    public void TableModeDerivedJoinTargetPropagatesRetraction()
    {
        var sql = "SELECT t.symbol, d.tag FROM trades t JOIN (SELECT symbol, tag FROM ref) d ON t.symbol = d.symbol";
        var exec = CompileTableAndCreate(sql, [Trades], [Ref]);

        exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));

        var asserted = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 1));
        var assertedDelta = Assert.Single(asserted);
        Assert.Equal(1, assertedDelta.Weight);
        Assert.Equal("watchlist", assertedDelta.Row["tag"]);
        Assert.Single(exec.Snapshot());

        var retracted = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), -1));
        var retractedDelta = Assert.Single(retracted);
        Assert.Equal(-1, retractedDelta.Weight);
        Assert.Equal("watchlist", retractedDelta.Row["tag"]);
        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void TableModeDerivedTablesMayNestTwoLevelsDeep()
    {
        var sql = "SELECT x.symbol FROM (SELECT y.symbol FROM (SELECT symbol FROM trades) y) x";
        var exec = CompileTableAndCreate(sql, [Trades]);

        var results = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true)));
        var row = Assert.Single(results);
        Assert.Equal("AAPL", row.Row["symbol"]);
        Assert.Equal(1, row.Weight);
    }

    [Fact]
    public void TableModeDerivedGroupByRetractsOldGroupRowOnChange()
    {
        // The derived subquery itself runs a running GROUP BY — its own retraction/assertion pairs must
        // propagate through the outer (here: a trivial passthrough) unchanged.
        var sql = "SELECT d.symbol, d.total FROM (SELECT symbol, SUM(qty) AS total FROM trades GROUP BY symbol) d";
        var exec = CompileTableAndCreate(sql, [Trades]);

        var first = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true)));
        var firstDelta = Assert.Single(first);
        Assert.Equal(1, firstDelta.Weight);
        Assert.Equal(10L, firstDelta.Row["total"]);

        var second = exec.OnStreamEvent("trades", Evt(2000, "trades", ("symbol", "AAPL"), ("price", 101.0), ("qty", 5L), ("active", true)));
        Assert.Equal(2, second.Count);
        var retraction = Assert.Single(second, d => d.Weight == -1);
        Assert.Equal(10L, retraction.Row["total"]);
        var assertion = Assert.Single(second, d => d.Weight == 1);
        Assert.Equal(15L, assertion.Row["total"]);

        var snapshot = Assert.Single(exec.Snapshot().Values);
        Assert.Equal(15L, snapshot.Row["total"]);
    }

    // ------------------------------------------------------------------
    // Acceptance (plan 004's "WITH hot ... " example, N1 slice — the IN(...) rewrite is N2, not yet
    // implemented; this exercises the WITH/derived-table half of that same acceptance query using an
    // equivalent JOIN instead of the N2 semi-join rewrite).
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // Determinism replay on a nested table plan (plan 003 M1's replay guarantee, re-proven for N1's
    // nested-TableExecutor derived-node wiring — a fresh outer TableExecutor also means a fresh nested
    // derived TableExecutor each run; nothing shared/static should leak between runs).
    // ------------------------------------------------------------------

    private const string NestedTableSql = "SELECT t.symbol, d.tag, t.price FROM trades t JOIN (SELECT symbol, tag FROM ref) d ON t.symbol = d.symbol WHERE t.price > 10";

    private static List<IReadOnlyList<TableDelta>> RunNestedTableBatchSequence(TableExecutor exec)
    {
        var perCallOutputs = new List<IReadOnlyList<TableDelta>>
        {
            exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1)),
            exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 10L), ("active", true))),
            exec.OnStreamEvent("trades", Evt(1500, "trades", ("symbol", "AAPL"), ("price", 5.0), ("qty", 1L), ("active", true))), // fails WHERE
            exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "MSFT"), ("tag", "watch")), 2)),
            exec.OnStreamEvent("trades", Evt(2000, "trades", ("symbol", "MSFT"), ("price", 50.0), ("qty", 3L), ("active", true))),
            exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1)), // retraction cascades through the derived source's join
        };
        return perCallOutputs;
    }

    private static string CanonDelta(IReadOnlyList<TableDelta> deltas) =>
        string.Join("|", deltas.Select(d => $"{d.Weight}:{Runtime.JsonText.SerializeCanonicalRow(d.Row)}"));

    [Fact]
    public void NestedTablePlanReplaysDeterministicallyAcrossFreshExecutors()
    {
        var compileResult = CompileTable(NestedTableSql, [Trades], [Ref]);
        Assert.True(compileResult.Ok, string.Join(";", compileResult.Diagnostics));
        var plan = compileResult.Plan!;

        var runs = Enumerable.Range(0, 3).Select(_ => RunNestedTableBatchSequence(plan.CreateExecutor())).ToList();

        var canonical = runs.Select(r => string.Join(";", r.Select(CanonDelta))).Distinct().ToList();
        Assert.Single(canonical); // all three independent runs collapse to the same canonical sequence
    }

    [Fact]
    public void AcceptanceQueryN1Slice_WithHotJoinedAgainstOuterWindow()
    {
        var sql = "WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS)) " +
                  "SELECT t.symbol, AVG(t.price) AS px FROM trades t JOIN hot h WITHIN 5 SECONDS ON t.symbol = h.symbol " +
                  "GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.String, r.OutputSchema!.Fields["symbol"]);
        Assert.Equal(FieldKind.Double, r.OutputSchema.Fields["px"]);
    }
}
