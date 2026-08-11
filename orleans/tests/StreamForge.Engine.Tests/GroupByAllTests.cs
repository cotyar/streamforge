using StreamForge.Engine.Sql;
using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 008 W3 — `GROUP BY ALL` (DuckDB/Snowflake sugar): expands to every non-aggregate select-list
/// expression, in select-list order. The expansion lives entirely in the parser (Parser.ParseSelectQuery's
/// GROUP BY branch) — everything downstream (Validator, Planner, TablePlanner, the runtime ops) consumes
/// CompiledPlan.GroupBy / CompiledTablePlan.GroupBy as an opaque List&lt;Expr&gt; and never needs to know
/// whether it was written explicitly or expanded from ALL. The first block of tests exercises the parser
/// directly (via Tokenizer+Parser, same as TokenizerAndParserTests) to pin down exactly which Expr nodes
/// end up in SelectQuery.GroupBy and that they are the SAME instances as the matching select items (the
/// sharing StructurallyEqual/AssignGroupByIndexes rely on) — this is more precise than asserting on a full
/// compile, and lets the scalar-subquery-exclusion case be checked without also tripping the unrelated
/// "scalar subquery in a grouped SELECT list" rule (Validator.cs's FindScalarSubqueryOutsideAggregate) that
/// would otherwise obscure whether exclusion actually happened. The second block exercises full
/// compile/execute behavior (table mode and pipeline mode), including the two decided edge cases
/// (`SELECT *` rejection, empty-expansion global group) and CTE/derived-table survival.
/// </summary>
public class GroupByAllTests
{
    // ------------------------------------------------------------------
    // Parser-level: exactly which Expr nodes GROUP BY ALL expands to.
    // ------------------------------------------------------------------

    private static SelectQuery ParseOk(string sql)
    {
        var (tokens, tokDiags) = new Tokenizer(sql).Tokenize();
        Assert.Empty(tokDiags);
        var (query, diags) = Parser.Parse(tokens);
        Assert.True(query is not null, string.Join(";", diags));
        Assert.Empty(diags);
        return query!;
    }

    [Fact]
    public void ExpansionPicksNonAggregateItemsInSelectListOrder()
    {
        // Select-list order is side, symbol — deliberately NOT the schema's declared field order — to
        // prove expansion follows the SELECT list, not the source schema.
        var q = ParseOk("SELECT side, symbol, COUNT(*) AS cnt FROM trades GROUP BY ALL");

        Assert.NotNull(q.GroupBy);
        Assert.Equal(2, q.GroupBy!.Count);
        var side = Assert.IsType<Identifier>(q.GroupBy[0]);
        var symbol = Assert.IsType<Identifier>(q.GroupBy[1]);
        Assert.Equal("side", side.Name);
        Assert.Equal("symbol", symbol.Name);

        // The sharing is deliberate (see class doc comment) — assert it directly, not just structurally.
        Assert.Same(q.Select.Items[0].Expression, q.GroupBy[0]);
        Assert.Same(q.Select.Items[1].Expression, q.GroupBy[1]);
    }

    [Fact]
    public void AggregateSelectItemIsExcludedFromExpansion()
    {
        var q = ParseOk("SELECT symbol, COUNT(*) AS cnt, SUM(price) AS total FROM trades GROUP BY ALL");

        var only = Assert.Single(q.GroupBy!);
        Assert.Same(q.Select.Items[0].Expression, only);
    }

    [Fact]
    public void ScalarSubqueryItemIsExcludedFromExpansion()
    {
        // Validator.ContainsAggregate deliberately treats a ScalarSubqueryExpr as aggregate-like (plan 004
        // N3/N4) — GROUP BY ALL reuses that exact predicate, so a bare scalar subquery in the select list
        // must NOT show up in the expanded GROUP BY, same as a real aggregate wouldn't.
        var q = ParseOk("SELECT symbol, (SELECT COUNT(*) FROM trades) AS total FROM trades GROUP BY ALL");

        var only = Assert.Single(q.GroupBy!);
        Assert.IsType<Identifier>(only);
        Assert.Equal("symbol", ((Identifier)only).Name);
    }

    [Fact]
    public void EmptyExpansionYieldsNullGroupByNotAnEmptyList()
    {
        // Every select item is an aggregate -> nothing to group by -> null (the implicit global group),
        // never an empty non-null list (that would trip the `GroupBy is not null` gates downstream).
        var q = ParseOk("SELECT COUNT(*) AS cnt FROM trades GROUP BY ALL");
        Assert.Null(q.GroupBy);
    }

    [Fact]
    public void StarWithAllIsRejectedAtParseTimeWithAPosition()
    {
        var sql = "SELECT * FROM trades GROUP BY ALL";
        var (tokens, _) = new Tokenizer(sql).Tokenize();
        var (query, diags) = Parser.Parse(tokens);

        Assert.Null(query);
        var d = Assert.Single(diags);
        Assert.Contains("GROUP BY ALL", d.Message);
        Assert.Equal(1, d.Line);
        Assert.Equal(sql.IndexOf("ALL", StringComparison.Ordinal) + 1, d.Column);
    }

    [Fact]
    public void QualifiedStarMixedWithOtherItemsStillCompilesButIsRejectedByTheExistingStarRule()
    {
        // `alias.*` isn't a bare `SELECT *`, so the parser happily includes it in the expansion (it's not
        // an aggregate) — but Validator's pre-existing "star is not allowed with GROUP BY/aggregates" rule
        // (Validator.cs ~:390) still fires on it exactly as it would for an explicit GROUP BY, since that
        // check is unconditional on every select item once GroupBy is non-null.
        var sql = "SELECT t.*, COUNT(*) AS cnt FROM trades t GROUP BY ALL WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = Compile(sql, Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("star is not allowed with GROUP BY/aggregates"));
    }

    [Fact]
    public void AllIsNotAReservedWordSelectAllStillParsesAsAColumnReference()
    {
        // ALL is only special-cased immediately after the GROUP BY keyword (Parser.ParseSelectQuery) — it
        // must not become globally reserved, so a column literally named "all" still works everywhere else,
        // including as a bare (AS-less) select item.
        var withAllColumn = Schema("t", ("all", FieldKind.String));
        var r = Compile("SELECT all FROM t", withAllColumn);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    // ------------------------------------------------------------------
    // Validator idempotence: GROUP BY ALL resolves the shared nodes twice (once walking GROUP BY, once
    // walking SELECT — Validator.Run always does both, GROUP BY first) but must not double-report.
    // ------------------------------------------------------------------

    [Fact]
    public void UnknownColumnInAnExpandedGroupByItemIsReportedOnlyOnce()
    {
        var sql = "SELECT bogus, COUNT(*) AS cnt FROM trades GROUP BY ALL WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = Compile(sql, Trades);
        Assert.False(r.Ok);
        var matches = r.Diagnostics.Where(d => d.Message.Contains("Unknown column 'bogus'")).ToList();
        Assert.Single(matches);
    }

    [Fact]
    public void AmbiguousColumnInAnExpandedGroupByItemIsReportedOnlyOnce()
    {
        var sql = "SELECT symbol, COUNT(*) AS cnt FROM trades t JOIN ref r WITHIN 1 SECONDS ON t.symbol = r.symbol " +
                  "GROUP BY ALL WINDOW TUMBLING(SIZE 5 SECONDS)";
        var r = Compile(sql, Trades, Ref);
        Assert.False(r.Ok);
        var matches = r.Diagnostics.Where(d => d.Message.Contains("Ambiguous column 'symbol'")).ToList();
        Assert.Single(matches);
    }

    // ------------------------------------------------------------------
    // Table mode: end-to-end compile + execute, ALL vs. explicit twin.
    // ------------------------------------------------------------------

    private static readonly SourceSchema TradesWithSide =
        Schema("trades", ("symbol", FieldKind.String), ("side", FieldKind.String), ("price", FieldKind.Double));

    [Fact]
    public void TableModeAllExpansionMatchesExplicitGroupByPlanSummaryAndResults()
    {
        var sqlAll = "SELECT symbol, side, COUNT(*) AS cnt FROM trades GROUP BY ALL";
        var sqlExplicit = "SELECT symbol, side, COUNT(*) AS cnt FROM trades GROUP BY symbol, side";

        var rAll = CompileTable(sqlAll, [TradesWithSide]);
        var rExplicit = CompileTable(sqlExplicit, [TradesWithSide]);
        Assert.True(rAll.Ok, string.Join(";", rAll.Diagnostics));
        Assert.True(rExplicit.Ok, string.Join(";", rExplicit.Diagnostics));
        Assert.Equal(rExplicit.PlanSummary, rAll.PlanSummary);

        var execAll = rAll.Plan!.CreateExecutor();
        var execExplicit = rExplicit.Plan!.CreateExecutor();

        EventRecord Row(string symbol, string side) =>
            Evt(1000, "trades", ("symbol", symbol), ("side", side), ("price", 1.0));

        var deltasAll = execAll.OnStreamEvent("trades", Row("AAPL", "BUY"));
        var deltasExplicit = execExplicit.OnStreamEvent("trades", Row("AAPL", "BUY"));
        AssertSameDeltas(deltasExplicit, deltasAll);

        // A second contribution to the same group (retract-then-assert) must line up too.
        deltasAll = execAll.OnStreamEvent("trades", Row("AAPL", "BUY"));
        deltasExplicit = execExplicit.OnStreamEvent("trades", Row("AAPL", "BUY"));
        AssertSameDeltas(deltasExplicit, deltasAll);

        // A distinct group (different side) must produce its own, separately-tracked row.
        deltasAll = execAll.OnStreamEvent("trades", Row("AAPL", "SELL"));
        deltasExplicit = execExplicit.OnStreamEvent("trades", Row("AAPL", "SELL"));
        AssertSameDeltas(deltasExplicit, deltasAll);
    }

    private static void AssertSameDeltas(IReadOnlyList<TableDelta> expected, IReadOnlyList<TableDelta> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Weight, actual[i].Weight);
            Assert.Equal(expected[i].Row["symbol"], actual[i].Row["symbol"]);
            Assert.Equal(expected[i].Row["side"], actual[i].Row["side"]);
            Assert.Equal(expected[i].Row["cnt"], actual[i].Row["cnt"]);
        }
    }

    [Fact]
    public void EmptyExpansionProducesSameResultsAsWritingNoGroupByAtAllInTableMode()
    {
        var sqlAll = "SELECT COUNT(*) AS cnt FROM trades GROUP BY ALL";
        var sqlNone = "SELECT COUNT(*) AS cnt FROM trades";

        var rAll = CompileTable(sqlAll, Trades);
        var rNone = CompileTable(sqlNone, Trades);
        Assert.True(rAll.Ok, string.Join(";", rAll.Diagnostics));
        Assert.True(rNone.Ok, string.Join(";", rNone.Diagnostics));
        Assert.Equal(rNone.PlanSummary, rAll.PlanSummary);

        var execAll = rAll.Plan!.CreateExecutor();
        var execNone = rNone.Plan!.CreateExecutor();

        var evt1 = Evt(1000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 1L), ("active", true));
        var evt2 = Evt(1100, "trades", ("symbol", "MSFT"), ("price", 2.0), ("qty", 1L), ("active", true));

        var d1All = Assert.Single(execAll.OnStreamEvent("trades", evt1));
        var d1None = Assert.Single(execNone.OnStreamEvent("trades", evt1));
        Assert.Equal(d1None.Weight, d1All.Weight);
        Assert.Equal(d1None.Row["cnt"], d1All.Row["cnt"]);

        // Both rows land in the SAME single global group (no GROUP BY key at all) — a second, differently-
        // symbol'd row still retracts and re-asserts the one row rather than starting a second group.
        var d2All = execAll.OnStreamEvent("trades", evt2);
        var d2None = execNone.OnStreamEvent("trades", evt2);
        Assert.Equal(d2None.Count, d2All.Count);
        for (int i = 0; i < d2None.Count; i++)
        {
            Assert.Equal(d2None[i].Weight, d2All[i].Weight);
            Assert.Equal(d2None[i].Row["cnt"], d2All[i].Row["cnt"]);
        }
    }

    [Fact]
    public void GroupByAllInsideACteCompilesAndAggregatesCorrectly()
    {
        var sql = "WITH agg AS (SELECT symbol, side, COUNT(*) AS cnt FROM trades GROUP BY ALL) " +
                  "SELECT agg.symbol, agg.side, agg.cnt FROM agg";
        var r = CompileTable(sql, [TradesWithSide]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));

        var exec = r.Plan!.CreateExecutor();
        var deltas = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("side", "BUY"), ("price", 1.0)));
        var d = Assert.Single(deltas);
        Assert.Equal(1, d.Weight);
        Assert.Equal("AAPL", d.Row["symbol"]);
        Assert.Equal("BUY", d.Row["side"]);
        Assert.Equal(1L, d.Row["cnt"]);
    }

    [Fact]
    public void GroupByAllInsideADerivedTableCompilesAndAggregatesCorrectly()
    {
        var sql = "SELECT d.symbol, d.side, d.cnt FROM (SELECT symbol, side, COUNT(*) AS cnt FROM trades GROUP BY ALL) d";
        var r = CompileTable(sql, [TradesWithSide]);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));

        var exec = r.Plan!.CreateExecutor();
        var deltas = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("side", "BUY"), ("price", 1.0)));
        var d = Assert.Single(deltas);
        Assert.Equal(1, d.Weight);
        Assert.Equal("AAPL", d.Row["symbol"]);
        Assert.Equal(1L, d.Row["cnt"]);
    }

    // ------------------------------------------------------------------
    // Pipeline mode: same expansion, plus the WINDOW requirement the existing rule enforces.
    // ------------------------------------------------------------------

    [Fact]
    public void PipelineModeGroupByAllWithoutWindowIsStillAnError()
    {
        // GroupByWithoutWindowIsError (ValidatorTests.cs) covers the explicit form — this proves the same
        // rule fires for the expanded form (q.GroupBy is non-null either way by the time the check runs).
        var r = Compile("SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY ALL", Trades);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("GROUP BY requires a WINDOW clause"));
    }

    [Fact]
    public void PipelineModeAllExpansionMatchesExplicitGroupByPlanSummaryAndResults()
    {
        var sqlAll = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY ALL WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";
        var sqlExplicit = "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL";

        var rAll = Compile(sqlAll, Trades);
        var rExplicit = Compile(sqlExplicit, Trades);
        Assert.True(rAll.Ok, string.Join(";", rAll.Diagnostics));
        Assert.True(rExplicit.Ok, string.Join(";", rExplicit.Diagnostics));
        Assert.Equal(rExplicit.PlanSummary, rAll.PlanSummary);

        var execAll = rAll.Plan!.CreateExecutor();
        var execExplicit = rExplicit.Plan!.CreateExecutor();

        execAll.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 10L), ("active", true)));
        execAll.OnEvent("trades", Evt(2000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 20L), ("active", true)));
        execExplicit.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 10L), ("active", true)));
        execExplicit.OnEvent("trades", Evt(2000, "trades", ("symbol", "AAPL"), ("price", 1.0), ("qty", 20L), ("active", true)));

        var closedAll = Assert.Single(execAll.AdvanceWatermark(12000));
        var closedExplicit = Assert.Single(execExplicit.AdvanceWatermark(12000));
        Assert.Equal(closedExplicit["symbol"], closedAll["symbol"]);
        Assert.Equal(closedExplicit["cnt"], closedAll["cnt"]);
        Assert.Equal(closedExplicit["window_start"], closedAll["window_start"]);
        Assert.Equal(closedExplicit["window_end"], closedAll["window_end"]);
    }

    [Fact]
    public void GroupByAllInsideAPipelineDerivedTableCompiles()
    {
        var sql = "SELECT d.avg_px FROM (SELECT AVG(price) AS avg_px FROM trades GROUP BY ALL WINDOW TUMBLING(SIZE 5 SECONDS)) d";
        var r = Compile(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.Double, r.OutputSchema!.Fields["avg_px"]);
    }
}
