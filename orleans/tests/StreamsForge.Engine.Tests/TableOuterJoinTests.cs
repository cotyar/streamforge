using StreamsForge.Engine.Dataflow;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 008 wave 2b: table-mode LEFT/RIGHT/FULL JOIN, end to end through the wired <see cref="TableExecutor"/>
/// (compiled real SQL, public OnStreamEvent/OnTableDelta/Snapshot surface) — TableJoinTests/TableCrossJoinTests
/// style, unlike TableOuterJoinOpUnitTests' direct op-level construction. This is the failure mode the whole
/// wave exists to prevent: a table-mode LEFT/RIGHT/FULL JOIN that compiles and runs but silently behaves like
/// INNER (no null-padding) — every trace below asserts the actual pad/product/retraction deltas, not just a
/// final row count, and the last test proves it holds at Parallelism 1 AND 4.
/// </summary>
public class TableOuterJoinTests
{
    private const string LeftSql = "SELECT t.symbol, r.tag FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol";

    private static string Canon(TableExecutor exec) =>
        string.Join("\n", exec.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    private static string Canon(PartitionedTableHarness harness) =>
        string.Join("\n", harness.Snapshot().OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value.Weight}"));

    // ------------------------------------------------------------------------------------------------
    // LEFT — full lifecycle: pad on first left row; product + pad retraction on first match; a second
    // match producing products with no pad traffic; retracting the last match re-asserting the pad.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void LeftJoin_FullLifecycleTrace_PadThenProductsThenRepad()
    {
        var exec = CompileTableAndCreate(LeftSql, [Trades], [Ref]);

        // 1. Left arrives alone: right bucket is empty -> pads, and the pad is what's in the snapshot.
        var step1 = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL")));
        var pad1 = Assert.Single(step1);
        Assert.Equal(1, pad1.Weight);
        Assert.Equal("AAPL", pad1.Row["symbol"]);
        Assert.Null(pad1.Row["tag"]);
        Assert.Single(exec.Snapshot());

        // 2. First matching right row: the product asserts AND the earlier pad retracts (nets to zero,
        // pruned from the snapshot — only the product row survives).
        var step2 = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 1));
        Assert.Equal(2, step2.Count);
        var product1 = Assert.Single(step2, d => d.Row["tag"] is not null);
        Assert.Equal(1, product1.Weight);
        Assert.Equal("watchlist", product1.Row["tag"]);
        var padRetract1 = Assert.Single(step2, d => d.Row["tag"] is null);
        Assert.Equal(-1, padRetract1.Weight);
        var snap2 = exec.Snapshot();
        var only2 = Assert.Single(snap2);
        Assert.Equal("watchlist", only2.Value.Row["tag"]);

        // 3. A second right row under the SAME key: products only — the flip is per-key presence, not
        // per-delta, so a key that's already present doesn't re-trigger any pad traffic.
        var step3 = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1));
        var product2 = Assert.Single(step3);
        Assert.Equal(1, product2.Weight);
        Assert.Equal("core", product2.Row["tag"]);
        Assert.Equal(2, exec.Snapshot().Count);

        // 4. Retract one right row (NOT the last survivor): product retraction only, no pad — one right
        // row (core) still keeps the key present.
        var step4 = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), -1));
        var retract1 = Assert.Single(step4);
        Assert.Equal(-1, retract1.Weight);
        Assert.Equal("watchlist", retract1.Row["tag"]);
        Assert.Single(exec.Snapshot());

        // 5. Retract the LAST right row: the product retracts AND the pad re-asserts — presence flips
        // back to absent, so every currently-indexed left row under this key is padded again.
        var step5 = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), -1));
        Assert.Equal(2, step5.Count);
        var retract2 = Assert.Single(step5, d => d.Row["tag"] is not null);
        Assert.Equal(-1, retract2.Weight);
        var repad = Assert.Single(step5, d => d.Row["tag"] is null);
        Assert.Equal(1, repad.Weight);
        var final = Assert.Single(exec.Snapshot());
        Assert.Equal("AAPL", final.Value.Row["symbol"]);
        Assert.Null(final.Value.Row["tag"]);
    }

    // ------------------------------------------------------------------------------------------------
    // RIGHT mirrors LEFT: the lone (unmatched) RIGHT row pads itself; a matching LEFT arrival retracts it.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void RightJoin_LoneRightRowPads_ThenMatchingLeftRetractsThePad()
    {
        var exec = CompileTableAndCreate(
            "SELECT t.symbol, r.tag FROM trades t RIGHT JOIN ref r ON t.symbol = r.symbol", [Trades], [Ref]);

        var step1 = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 1));
        var pad = Assert.Single(step1);
        Assert.Equal(1, pad.Weight);
        Assert.Null(pad.Row["symbol"]);
        Assert.Equal("watchlist", pad.Row["tag"]);
        Assert.Single(exec.Snapshot());

        var step2 = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL")));
        Assert.Equal(2, step2.Count);
        var product = Assert.Single(step2, d => d.Row["symbol"] is not null);
        Assert.Equal(1, product.Weight);
        var padRetract = Assert.Single(step2, d => d.Row["symbol"] is null);
        Assert.Equal(-1, padRetract.Weight);

        var final = Assert.Single(exec.Snapshot());
        Assert.Equal("AAPL", final.Value.Row["symbol"]);
        Assert.Equal("watchlist", final.Value.Row["tag"]);
    }

    // ------------------------------------------------------------------------------------------------
    // FULL: both sides pad independently for unmatched keys; never emits an all-NULL (null, null) row.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void FullJoin_PadsBothSides_NeverEmitsNullNullRow()
    {
        var exec = CompileTableAndCreate(
            "SELECT t.symbol, r.tag FROM trades t FULL JOIN ref r ON t.symbol = r.symbol", [Trades], [Ref]);

        exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"))); // unmatched left -> pads itself
        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "MSFT"), ("tag", "other")), 1)); // unmatched right, different key -> pads itself

        var snapBefore = exec.Snapshot();
        Assert.Equal(2, snapBefore.Count);
        Assert.All(snapBefore.Values, v => Assert.False(v.Row["symbol"] is null && v.Row["tag"] is null));

        var step3 = exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 1)); // matches the AAPL left row
        Assert.Equal(2, step3.Count); // product + the AAPL left-pad's retraction

        var snapAfter = exec.Snapshot();
        Assert.Equal(2, snapAfter.Count); // AAPL product (was left-only-pad) + MSFT pad (untouched)
        Assert.Contains(snapAfter.Values, v => (string?)v.Row["symbol"] == "AAPL" && (string?)v.Row["tag"] == "watchlist");
        Assert.Contains(snapAfter.Values, v => v.Row["symbol"] is null && (string?)v.Row["tag"] == "other");
    }

    // ------------------------------------------------------------------------------------------------
    // Chained joins: LEFT then INNER eliminates the null-keyed pad (INNER's ordinary NULL-never-matches
    // rule applies to the pad's own NULL column); LEFT then LEFT propagates it instead, padding again.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void ChainedLeftThenInner_NullKeyedPadIsEliminated()
    {
        var exec = CompileTableAndCreate(
            "SELECT t.symbol, r.tag, q.bid FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol JOIN quotes q ON r.tag = q.symbol",
            [Trades, Quotes], [Ref]);

        // trades AAPL has no matching ref row -> the LEFT join pads (r.tag = NULL). That pad flows into
        // the SECOND join's Left input keyed on r.tag — an ordinary INNER equi-join, and r.tag is NULL,
        // so it is silently dropped (never indexed, never emitted) by TableJoinOp's usual NULL rule —
        // exactly like any other NULL-keyed row hitting an INNER join, pad or not.
        var step = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL")));
        Assert.Empty(step);
        Assert.Empty(exec.Snapshot());

        // ref now matches AAPL -> the pad retracts (itself dropped, so nothing to retract) and the
        // product (r.tag = "TAG1") flows into the second join, indexed but still unmatched (no quotes yet).
        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "TAG1")), 1));
        Assert.Empty(exec.Snapshot());

        // quotes(TAG1) arrives -> the second join's INNER match finally fires.
        exec.OnStreamEvent("quotes", Evt(2000, "quotes", ("symbol", "TAG1"), ("bid", 99.5), ("ask", 100.5)));
        var final = Assert.Single(exec.Snapshot());
        Assert.Equal("AAPL", final.Value.Row["symbol"]);
        Assert.Equal("TAG1", final.Value.Row["tag"]);
        Assert.Equal(99.5, final.Value.Row["bid"]);
    }

    [Fact]
    public void ChainedLeftThenLeft_NullKeyedPadIsPaddedAgain_NotEliminated()
    {
        var exec = CompileTableAndCreate(
            "SELECT t.symbol, r.tag, q.bid FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol LEFT JOIN quotes q ON r.tag = q.symbol",
            [Trades, Quotes], [Ref]);

        // Same unmatched trades row as above, but now the SECOND join is LEFT too: r.tag is NULL, so its
        // own key evaluates to NULL — T3's rule (a LEFT join pads its own NULL-keyed rows immediately,
        // never dropping them) fires again, producing a fully-padded (symbol, NULL, NULL) row instead of
        // silently vanishing.
        var step = exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL")));
        var row = Assert.Single(step);
        Assert.Equal(1, row.Weight);
        Assert.Equal("AAPL", row.Row["symbol"]);
        Assert.Null(row.Row["tag"]);
        Assert.Null(row.Row["bid"]);

        var only = Assert.Single(exec.Snapshot());
        Assert.Equal("AAPL", only.Value.Row["symbol"]);
        Assert.Null(only.Value.Row["tag"]);
        Assert.Null(only.Value.Row["bid"]);
    }

    // ------------------------------------------------------------------------------------------------
    // Pads flowing into GROUP BY: unmatched rows (NULL on the padded column) group together under a
    // NULL key exactly like any other NULL GROUP BY value — no special-casing needed downstream.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void PadsFlowIntoGroupBy_UnmatchedRowsGroupUnderNullTag()
    {
        var exec = CompileTableAndCreate(
            "SELECT r.tag, COUNT(*) AS cnt FROM trades t LEFT JOIN ref r ON t.symbol = r.symbol GROUP BY r.tag",
            [Trades], [Ref]);

        exec.OnStreamEvent("trades", Evt(1000, "trades", ("symbol", "AAPL")));
        exec.OnStreamEvent("trades", Evt(2000, "trades", ("symbol", "MSFT")));

        var snap1 = exec.Snapshot();
        var nullGroup1 = Assert.Single(snap1);
        Assert.Null(nullGroup1.Value.Row["tag"]);
        Assert.Equal(2L, nullGroup1.Value.Row["cnt"]);

        // AAPL now matches ref -> its pad leaves the NULL group (cnt 2 -> 1) and a new "core" group appears.
        exec.OnTableDelta("ref", new TableDelta(Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1));

        var snap2 = exec.Snapshot();
        Assert.Equal(2, snap2.Count);
        var nullGroup2 = Assert.Single(snap2.Values, v => v.Row["tag"] is null);
        Assert.Equal(1L, nullGroup2.Row["cnt"]);
        var coreGroup = Assert.Single(snap2.Values, v => (string?)v.Row["tag"] == "core");
        Assert.Equal(1L, coreGroup.Row["cnt"]);
    }

    // ------------------------------------------------------------------------------------------------
    // Parallelism 1 vs 4 equivalence — the silent-INNER check: a never-matched row must still appear
    // NULL-padded (not silently dropped, which is what a mistakenly-INNER-routed LEFT JOIN would do) in
    // BOTH the single-partition TableExecutor AND the partitioned dataflow, at both partition counts.
    // ------------------------------------------------------------------------------------------------

    [Fact]
    public void LeftJoin_Parallelism1And4_Equivalent_AndActuallyNullPads()
    {
        var result = CompileTable(LeftSql, [Trades], [Ref]);
        Assert.True(result.Ok, string.Join(";", result.Diagnostics));
        var plan = result.Plan!;

        var events = new (string Origin, EventRecord Row, long Weight, bool IsTable)[]
        {
            ("trades", Evt(1, "trades", ("symbol", "AAPL")), 1, false),
            ("trades", Evt(2, "trades", ("symbol", "MSFT")), 1, false), // never matches — must stay padded, not vanish
            ("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), 1, true),
            ("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "core")), 1, true),
            ("ref", Evt(0, "ref", ("symbol", "AAPL"), ("tag", "watchlist")), -1, true),
        };

        var classic = plan.CreateExecutor();
        foreach (var e in events)
        {
            if (e.IsTable) classic.OnTableDelta(e.Origin, new TableDelta(e.Row, e.Weight));
            else classic.OnStreamEvent(e.Origin, e.Row);
        }
        var classicCanon = Canon(classic);
        Assert.NotEmpty(classic.Snapshot()); // sanity: the comparison below isn't vacuously true

        // The silent-INNER check itself: MSFT never matched anything, so an INNER join (or a LEFT join
        // wired to the wrong op) would drop it entirely. A genuine LEFT join keeps it, NULL-padded.
        Assert.Contains(classic.Snapshot().Values, v => (string?)v.Row["symbol"] == "MSFT" && v.Row["tag"] is null);

        foreach (var p in new[] { 1, 4 })
        {
            var dataflow = plan.CreateDataflow(p);
            var harness = new PartitionedTableHarness(dataflow);
            foreach (var e in events) harness.Admit(e.Origin, e.Row, e.Weight);

            Assert.Equal(classicCanon, Canon(harness));
            Assert.Contains(harness.Snapshot().Values, v => (string?)v.Row["symbol"] == "MSFT" && v.Row["tag"] is null);
        }
    }
}
