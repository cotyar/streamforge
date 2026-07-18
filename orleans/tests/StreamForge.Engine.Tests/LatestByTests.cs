using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 002 L3 (deferred sugar, landed alongside L2) — table-mode `LATEST BY (col[, col...])`: keeps the
/// most recent row per key, ordered by `_ts`, emitting retract(old)/assert(new) pairs on replacement. See
/// Runtime/Ops/TableLatestByOp.cs for the pinned semantics this file's executor tests prove.
/// </summary>
public class LatestByTests
{
    private static EventRecord OrderEvt(long ts, string orderId, string stage, long filledQty = 0L) =>
        Evt(ts, "order_events", ("order_id", orderId), ("stage", stage), ("filled_qty", filledQty));

    // ------------------------------------------------------------------
    // Parser / grammar + mutual-exclusion diagnostics
    // ------------------------------------------------------------------

    [Fact]
    public void LatestByParsesAndCompilesInTableMode()
    {
        var r = CompileTable("SELECT order_id, stage FROM order_events LATEST BY (order_id)", OrderEvents);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void LatestByWithMultipleKeysParses()
    {
        var r = CompileTable("SELECT order_id, stage FROM order_events LATEST BY (order_id, stage)", OrderEvents);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
    }

    [Fact]
    public void LatestByInPipelineModeIsRejected()
    {
        var r = Compile("SELECT order_id FROM order_events LATEST BY (order_id)", OrderEvents);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("LATEST BY is table-mode only"));
    }

    [Fact]
    public void LatestByCombinedWithGroupByIsRejected()
    {
        var r = CompileTable("SELECT order_id, COUNT(*) AS cnt FROM order_events LATEST BY (order_id) GROUP BY order_id", OrderEvents);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("LATEST BY may not be combined with GROUP BY"));
    }

    [Fact]
    public void LatestByCombinedWithAggregateIsRejected()
    {
        var r = CompileTable("SELECT order_id, COUNT(*) AS cnt FROM order_events LATEST BY (order_id)", OrderEvents);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("LATEST BY may not be combined with aggregate functions"));
    }

    [Fact]
    public void LatestByCombinedWithWindowIsRejected()
    {
        var r = CompileTable("SELECT order_id FROM order_events LATEST BY (order_id) WINDOW TUMBLING(SIZE 5 SECONDS)", OrderEvents);
        Assert.False(r.Ok);
        Assert.Contains(r.Diagnostics, d => d.Message.Contains("WINDOW clause not allowed in table mode"));
    }

    // ------------------------------------------------------------------
    // Executor
    // ------------------------------------------------------------------

    private static TableExecutor CreateLatestByOrderId() =>
        CompileTableAndCreate("SELECT order_id, stage FROM order_events LATEST BY (order_id)", OrderEvents);

    [Fact]
    public void FirstAssertionForAKeyEmitsOnlyAnAssertion()
    {
        var exec = CreateLatestByOrderId();
        var deltas = exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW"));

        var assertion = Assert.Single(deltas);
        Assert.Equal(1, assertion.Weight);
        Assert.Equal("NEW", assertion.Row["stage"]);
    }

    [Fact]
    public void NewerArrivalReplacesEmittingRetractThenAssert()
    {
        var exec = CreateLatestByOrderId();
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW"));

        var deltas = exec.OnStreamEvent("order_events", OrderEvt(2000, "O1", "ACK"));

        Assert.Equal(2, deltas.Count);
        Assert.Equal(-1, deltas[0].Weight);
        Assert.Equal("NEW", deltas[0].Row["stage"]);
        Assert.Equal(1, deltas[1].Weight);
        Assert.Equal("ACK", deltas[1].Row["stage"]);
    }

    [Fact]
    public void StrictlyOlderArrivalIsIgnored()
    {
        var exec = CreateLatestByOrderId();
        exec.OnStreamEvent("order_events", OrderEvt(2000, "O1", "ACK"));

        var deltas = exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW")); // older _ts

        Assert.Empty(deltas);
        var snapshot = exec.Snapshot();
        var current = Assert.Single(snapshot);
        Assert.Equal("ACK", current.Value.Row["stage"]);
    }

    [Fact]
    public void TiedTimestampArrivalReplaces()
    {
        var exec = CreateLatestByOrderId();
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW"));

        // Same _ts as the currently-retained row (">=" replaces on ties — see class doc).
        var deltas = exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "ACK"));

        Assert.Equal(2, deltas.Count);
        Assert.Equal(-1, deltas[0].Weight);
        Assert.Equal(1, deltas[1].Weight);
        Assert.Equal("ACK", deltas[1].Row["stage"]);
    }

    [Fact]
    public void UpstreamRetractionOfTheCurrentlyRetainedRowDropsTheKey()
    {
        var exec = CreateLatestByOrderId();
        var row = OrderEvt(1000, "O1", "NEW");
        exec.OnStreamEvent("order_events", row);

        // Simulates an upstream retraction (e.g. a WHERE flip further up a chain) of the SAME row this op
        // currently holds — see TableZSetTests' class doc on driving retractions via OnTableDelta.
        var deltas = exec.OnTableDelta("order_events", new TableDelta(row, -1));

        var retraction = Assert.Single(deltas);
        Assert.Equal(-1, retraction.Weight);
        Assert.Equal("NEW", retraction.Row["stage"]);
        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void RetractionOfANonCurrentRowIsANoOp()
    {
        var exec = CreateLatestByOrderId();
        var rowA = OrderEvt(1000, "O1", "NEW");
        var rowB = OrderEvt(2000, "O1", "ACK");
        exec.OnStreamEvent("order_events", rowA);
        exec.OnStreamEvent("order_events", rowB); // ACK is now current; NEW already retracted

        var deltas = exec.OnTableDelta("order_events", new TableDelta(rowA, -1)); // retract the STALE row

        Assert.Empty(deltas);
        var current = Assert.Single(exec.Snapshot());
        Assert.Equal("ACK", current.Value.Row["stage"]);
    }

    [Fact]
    public void DistinctKeysAreTrackedIndependently()
    {
        var exec = CreateLatestByOrderId();
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW"));
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O2", "NEW"));
        exec.OnStreamEvent("order_events", OrderEvt(2000, "O1", "ACK"));

        var snapshot = exec.Snapshot();
        Assert.Equal(2, snapshot.Count);
        Assert.Contains(snapshot.Values, v => (string)v.Row["order_id"]! == "O1" && (string)v.Row["stage"]! == "ACK");
        Assert.Contains(snapshot.Values, v => (string)v.Row["order_id"]! == "O2" && (string)v.Row["stage"]! == "NEW");
    }

    [Fact]
    public void ChainedLatestByTableFeedsAnotherTable()
    {
        var t1 = CompileTable("SELECT order_id, stage FROM order_events LATEST BY (order_id)", [OrderEvents]);
        Assert.True(t1.Ok, string.Join(";", t1.Diagnostics));
        var exec1 = t1.Plan!.CreateExecutor();

        var orderStatesSchema = new SourceSchema("order_states", t1.OutputSchema!.Fields);
        var t2 = CompileTable("SELECT order_id, stage FROM order_states WHERE stage = 'ACK'", [], [orderStatesSchema]);
        Assert.True(t2.Ok, string.Join(";", t2.Diagnostics));
        var exec2 = t2.Plan!.CreateExecutor();

        foreach (var d in exec1.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW")))
        {
            exec2.OnTableDelta("order_states", d);
        }
        Assert.Empty(exec2.Snapshot()); // NEW doesn't pass downstream WHERE stage = 'ACK'

        foreach (var d in exec1.OnStreamEvent("order_events", OrderEvt(2000, "O1", "ACK")))
        {
            exec2.OnTableDelta("order_states", d);
        }
        var downstream = Assert.Single(exec2.Snapshot());
        Assert.Equal("ACK", downstream.Value.Row["stage"]);
    }
}
