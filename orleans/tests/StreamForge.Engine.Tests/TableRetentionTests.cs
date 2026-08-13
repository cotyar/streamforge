using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 011 wave C2 — the opt-in per-table ROW RETENTION policy (see PublicApi.cs's
/// <see cref="TableRetentionPolicy"/> and Runtime/TableRetention.cs).
///
/// The load-bearing assertion in this file is NOT "the row count plateaus" — a policy that only trimmed
/// the consolidated output would pass that while every structure holding memory kept growing. It is
/// <see cref="TableExecutor.RetainedStateCount"/>: the size of the state that actually OWNS the rows
/// (TableLatestByOp.Current for a LATEST BY plan, the ledger for a plain projection). Snapshot() is
/// checked too, but as a consequence, not as the proof.
/// </summary>
public class TableRetentionTests
{
    private static EventRecord OrderEvt(long ts, string orderId, string stage) =>
        Evt(ts, "order_events", ("order_id", orderId), ("stage", stage), ("filled_qty", 0L));

    private const string LatestBySql = "SELECT order_id, stage FROM order_events LATEST BY (order_id)";

    // ------------------------------------------------------------------
    // Default off — the property that lets this ship without touching a single existing test.
    // ------------------------------------------------------------------

    [Fact]
    public void RetentionIsOffByDefault_UnboundedTableKeepsEveryKey()
    {
        var exec = CompileTableAndCreate(LatestBySql, OrderEvents);

        for (int i = 0; i < 50; i++) exec.OnStreamEvent("order_events", OrderEvt(1000 + i, $"o{i}", "NEW"));

        Assert.Equal(50, exec.Snapshot().Count);
        Assert.Equal(-1, exec.RetainedStateCount); // no scope installed at all
    }

    [Fact]
    public void ConfiguringDisabledPolicyIsANoOp()
    {
        var exec = CompileTableAndCreate(LatestBySql, OrderEvents);
        exec.ConfigureRetention(TableRetentionPolicy.None);

        for (int i = 0; i < 20; i++) exec.OnStreamEvent("order_events", OrderEvt(1000 + i, $"o{i}", "NEW"));

        Assert.Equal(20, exec.Snapshot().Count);
    }

    // ------------------------------------------------------------------
    // MaxRows on a LATEST BY plan — the motivating case (an unbounded key space).
    // ------------------------------------------------------------------

    [Fact]
    public void MaxRows_EvictsOldestByEventTime_AndReclaimsTheOperatorsOwnPerKeyState()
    {
        var exec = CompileTableAndCreate(LatestBySql, OrderEvents);
        exec.ConfigureRetention(new TableRetentionPolicy(MaxRows: 3, TtlMs: 0));

        for (int i = 0; i < 10; i++) exec.OnStreamEvent("order_events", OrderEvt(1000 + i, $"o{i}", "NEW"));

        // The operator's per-key map — the structure that would otherwise grow forever — is bounded.
        Assert.Equal(3, exec.RetainedStateCount);
        // ...and so is the consolidated output, which follows from it rather than being trimmed separately.
        Assert.Equal(3, exec.Snapshot().Count);

        var surviving = exec.Snapshot().Values.Select(v => (string)v.Row["order_id"]!).OrderBy(x => x).ToList();
        Assert.Equal(new[] { "o7", "o8", "o9" }, surviving); // oldest _ts evicted first
    }

    [Fact]
    public void Eviction_EmitsARealRetraction_MarkedAsRetention()
    {
        var exec = CompileTableAndCreate(LatestBySql, OrderEvents);
        exec.ConfigureRetention(new TableRetentionPolicy(MaxRows: 2, TtlMs: 0));

        exec.OnStreamEvent("order_events", OrderEvt(1000, "a", "NEW"));
        exec.OnStreamEvent("order_events", OrderEvt(1001, "b", "NEW"));
        var deltas = exec.OnStreamEvent("order_events", OrderEvt(1002, "c", "NEW"));

        // The admission's own assertion, THEN the eviction retraction — one list, one path, so every
        // consumer of this return value stays consistent without knowing retention exists.
        Assert.Equal(2, deltas.Count);
        Assert.Equal(1, deltas[0].Weight);
        Assert.False(deltas[0].Retention);
        Assert.Equal(-1, deltas[1].Weight);
        Assert.True(deltas[1].Retention);
        Assert.Equal("a", deltas[1].Row["order_id"]); // the oldest key, by event time
    }

    [Fact]
    public void Eviction_LeavesNoResidueInTheConsolidationLedger()
    {
        var exec = CompileTableAndCreate(LatestBySql, OrderEvents);
        exec.ConfigureRetention(new TableRetentionPolicy(MaxRows: 5, TtlMs: 0));

        for (int i = 0; i < 200; i++) exec.OnStreamEvent("order_events", OrderEvt(1000 + i, $"o{i}", "NEW"));

        Assert.Equal(5, exec.Snapshot().Count);
        // An eviction that netted a key to a NEGATIVE weight would park it in the ledger's debt side-table
        // forever — a leak wearing a bound's clothes. Retracting the row's full running weight is what
        // makes this zero.
        Assert.Equal(0, exec.DebtCount);
    }

    [Fact]
    public void LateUpdateToARetainedKeyRefreshesItsPositionInTheEvictionOrder()
    {
        var exec = CompileTableAndCreate(LatestBySql, OrderEvents);
        exec.ConfigureRetention(new TableRetentionPolicy(MaxRows: 2, TtlMs: 0));

        exec.OnStreamEvent("order_events", OrderEvt(1000, "a", "NEW"));
        exec.OnStreamEvent("order_events", OrderEvt(1001, "b", "NEW"));
        // "a" progresses — it is now the NEWEST key, so the next arrival must evict "b", not "a".
        exec.OnStreamEvent("order_events", OrderEvt(1002, "a", "FILLED"));
        exec.OnStreamEvent("order_events", OrderEvt(1003, "c", "NEW"));

        var surviving = exec.Snapshot().Values.Select(v => (string)v.Row["order_id"]!).OrderBy(x => x).ToList();
        Assert.Equal(new[] { "a", "c" }, surviving);
        Assert.Equal("FILLED", exec.Snapshot().Values.Single(v => (string)v.Row["order_id"]! == "a").Row["stage"]);
    }

    // ------------------------------------------------------------------
    // TTL — event time, not wall clock.
    // ------------------------------------------------------------------

    [Fact]
    public void Ttl_EvictsByEventTimeMeasuredFromTheNewestAdmittedRow()
    {
        var exec = CompileTableAndCreate(LatestBySql, OrderEvents);
        exec.ConfigureRetention(new TableRetentionPolicy(MaxRows: 0, TtlMs: 100));

        // These timestamps are far in the PAST in wall-clock terms; a wall-clock TTL would evict all of
        // them immediately. An event-time TTL keeps everything within 100ms of the newest row.
        exec.OnStreamEvent("order_events", OrderEvt(1_000, "old", "NEW"));
        exec.OnStreamEvent("order_events", OrderEvt(1_050, "mid", "NEW"));
        Assert.Equal(2, exec.Snapshot().Count);

        exec.OnStreamEvent("order_events", OrderEvt(1_200, "new", "NEW")); // cutoff becomes 1100
        Assert.Equal(1, exec.RetainedStateCount);
        Assert.Equal("new", exec.Snapshot().Values.Single().Row["order_id"]);
    }

    [Fact]
    public void Ttl_DoesNotAgeAnythingOutWhileTheInputIsStalled()
    {
        var exec = CompileTableAndCreate(LatestBySql, OrderEvents);
        exec.ConfigureRetention(new TableRetentionPolicy(MaxRows: 0, TtlMs: 10));

        exec.OnStreamEvent("order_events", OrderEvt(1_000, "a", "NEW"));
        Thread.Sleep(50); // real time passes; event time does not

        // The documented consequence of an event-time TTL, asserted rather than left as prose: with no
        // further input there is no new high-water mark, so nothing expires.
        Assert.Equal(1, exec.Snapshot().Count);
    }

    [Fact]
    public void TtlAndMaxRowsCompose_AgeFirstThenCount()
    {
        var exec = CompileTableAndCreate(LatestBySql, OrderEvents);
        exec.ConfigureRetention(new TableRetentionPolicy(MaxRows: 2, TtlMs: 50));

        exec.OnStreamEvent("order_events", OrderEvt(1_000, "a", "NEW"));
        exec.OnStreamEvent("order_events", OrderEvt(1_010, "b", "NEW"));
        exec.OnStreamEvent("order_events", OrderEvt(1_020, "c", "NEW")); // count bound trims "a"
        Assert.Equal(2, exec.Snapshot().Count);

        exec.OnStreamEvent("order_events", OrderEvt(1_100, "d", "NEW")); // age bound trims b and c
        Assert.Equal(1, exec.RetainedStateCount);
        Assert.Equal("d", exec.Snapshot().Values.Single().Row["order_id"]);
    }

    // ------------------------------------------------------------------
    // Determinism — the invariant this codebase treats as testable, not aspirational.
    // ------------------------------------------------------------------

    [Fact]
    public void ReplayingTheSameInputProducesTheSameBoundedTable()
    {
        static List<string> Run()
        {
            var exec = CompileTableAndCreate(LatestBySql, OrderEvents);
            exec.ConfigureRetention(new TableRetentionPolicy(MaxRows: 7, TtlMs: 0));
            // Deliberately includes repeated timestamps, so the tie-break (and not arrival luck) is what
            // decides who goes.
            for (int i = 0; i < 60; i++) exec.OnStreamEvent("order_events", OrderEvt(1000 + (i / 3), $"o{i}", "NEW"));
            return exec.Snapshot().Values.Select(v => (string)v.Row["order_id"]!).OrderBy(x => x, StringComparer.Ordinal).ToList();
        }

        Assert.Equal(Run(), Run());
        Assert.Equal(7, Run().Count);
    }

    // ------------------------------------------------------------------
    // The plain-projection scope (no LATEST BY): the ledger IS the state.
    // ------------------------------------------------------------------

    [Fact]
    public void ProjectionTable_BoundsTheLedgerItself()
    {
        var exec = CompileTableAndCreate("SELECT symbol, price FROM trades", Trades);
        exec.ConfigureRetention(new TableRetentionPolicy(MaxRows: 4, TtlMs: 0));

        for (int i = 0; i < 25; i++)
        {
            exec.OnStreamEvent("trades", Evt(2000 + i, "trades", ("symbol", $"S{i}"), ("price", 1.0 + i), ("qty", 1L), ("active", true)));
        }

        Assert.Equal(4, exec.RetainedStateCount);
        Assert.Equal(4, exec.Snapshot().Count);
        Assert.Equal(0, exec.DebtCount);
        var survivors = exec.Snapshot().Values.Select(v => (string)v.Row["symbol"]!).OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "S21", "S22", "S23", "S24" }, survivors);
    }

    // ------------------------------------------------------------------
    // Refusal, not silent under-delivery, for shapes whose state retention cannot reclaim.
    // ------------------------------------------------------------------

    [Theory]
    [InlineData("SELECT symbol, COUNT(*) AS n FROM trades GROUP BY symbol")]
    [InlineData("SELECT COUNT(*) AS n FROM trades")]
    public void AggregatePlansRefuseRetention(string sql)
    {
        var result = CompileTable(sql, Trades);
        Assert.True(result.Ok, string.Join("; ", result.Diagnostics));
        Assert.False(result.Plan!.SupportsRetention);
        Assert.Throws<InvalidOperationException>(() => result.Plan.CreateExecutor().ConfigureRetention(new TableRetentionPolicy(10, 0)));
    }

    [Fact]
    public void JoinPlansRefuseRetention()
    {
        var result = CompileTable("SELECT t.symbol, q.bid FROM trades t JOIN quotes q ON t.symbol = q.symbol", Trades, Quotes);
        Assert.True(result.Ok, string.Join("; ", result.Diagnostics));
        Assert.False(result.Plan!.SupportsRetention);
    }

    [Fact]
    public void SetOperationPlansRefuseRetention()
    {
        var result = CompileTable("SELECT symbol FROM trades UNION ALL SELECT symbol FROM quotes", Trades, Quotes);
        Assert.True(result.Ok, string.Join("; ", result.Diagnostics));
        Assert.False(result.Plan!.SupportsRetention);
    }

    [Fact]
    public void DerivedSourcePlansRefuseRetention()
    {
        var result = CompileTable("SELECT symbol FROM (SELECT symbol FROM trades) d", Trades);
        Assert.True(result.Ok, string.Join("; ", result.Diagnostics));
        Assert.False(result.Plan!.SupportsRetention);
    }

    [Fact]
    public void SupportedShapesReportThemselvesAsSupported()
    {
        Assert.True(CompileTable(LatestBySql, OrderEvents).Plan!.SupportsRetention);
        Assert.True(CompileTable("SELECT symbol, price FROM trades WHERE price > 1", Trades).Plan!.SupportsRetention);
    }
}
