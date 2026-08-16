using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Wishlist "explicit key retraction through ingest": a client-pushed row carrying
/// <c>"_retract": true</c> emits weight -1 for the LAST ASSERTED ROW of that key in a LATEST BY table
/// — freeing the key from <see cref="TableExecutor.Snapshot"/> entirely, unlike the pre-existing
/// <c>WHERE _op &lt;&gt; 'd'</c> tombstone pattern (docs/cdc.md), which only hides it. Exercised through
/// <see cref="TableExecutor.OnStreamEvent"/> — the same public entry point LatestByTests.cs uses — so
/// these tests prove the real path: TableIngestOp reads "_retract" off the raw event and flips the
/// weight (TableExecutorImpl's OnStreamEventCore hardcodes every stream event to weight=1 and is
/// frozen, so TableIngestOp is the only place that override can happen); TableLatestByOp then retracts
/// by KEY rather than by matching row content, because a retraction event only ever carries the key
/// columns, never the row it means to remove.
/// </summary>
public class RetractKeyTests
{
    private static EventRecord OrderEvt(long ts, string orderId, string stage) =>
        Evt(ts, "order_events", ("order_id", orderId), ("stage", stage));

    /// <summary>A retract-flagged event still needs SOME value for the LATEST BY key column(s) — that is
    /// how TableLatestByOp.EncodeKey resolves which key to drop — but nothing else; "stage" here is
    /// intentionally absent/default, mirroring a real client that sends only the key plus "_retract".</summary>
    private static EventRecord RetractEvt(string orderId) =>
        Evt(0, "order_events", ("order_id", orderId), ("_retract", true));

    private static TableExecutor CreateLatestByOrderId() =>
        CompileTableAndCreate("SELECT order_id, stage FROM order_events LATEST BY (order_id)", OrderEvents);

    [Fact]
    public void RetractFreesTheKeyFromTheSnapshot_notJustHidesIt()
    {
        var exec = CreateLatestByOrderId();
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW"));
        Assert.Single(exec.Snapshot());

        var deltas = exec.OnStreamEvent("order_events", RetractEvt("O1"));

        var retraction = Assert.Single(deltas);
        Assert.Equal(-1, retraction.Weight);
        Assert.Equal("NEW", retraction.Row["stage"]); // the row it actually held, not the sparse retract row
        Assert.Empty(exec.Snapshot()); // freed, not merely filtered out of query results
    }

    [Fact]
    public void RetractOfAKeyThatWasNeverAssertedIsANoOp()
    {
        var exec = CreateLatestByOrderId();

        var deltas = exec.OnStreamEvent("order_events", RetractEvt("never-seen"));

        Assert.Empty(deltas);
        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void DoubleRetractDoesNotDoubleRetract()
    {
        var exec = CreateLatestByOrderId();
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW"));

        var first = exec.OnStreamEvent("order_events", RetractEvt("O1"));
        var second = exec.OnStreamEvent("order_events", RetractEvt("O1"));

        Assert.Single(first);
        Assert.Equal(-1, first[0].Weight);
        Assert.Empty(second); // the key is already gone — nothing left to retract, no phantom second -1
        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void RetractOnlyAffectsItsOwnKey()
    {
        var exec = CreateLatestByOrderId();
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW"));
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O2", "NEW"));

        exec.OnStreamEvent("order_events", RetractEvt("O1"));

        var remaining = Assert.Single(exec.Snapshot());
        Assert.Equal("O2", remaining.Value.Row["order_id"]);
    }

    [Fact]
    public void AssertAfterRetractStartsCleanNotAsAReplace()
    {
        var exec = CreateLatestByOrderId();
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW"));
        exec.OnStreamEvent("order_events", RetractEvt("O1"));

        // A fresh assertion for the freed key behaves exactly like the FIRST assertion for a key that
        // was never seen (LatestByTests.FirstAssertionForAKeyEmitsOnlyAnAssertion) — one assert, no
        // paired retraction of stale state, because the key genuinely has nothing retained anymore.
        var deltas = exec.OnStreamEvent("order_events", OrderEvt(2000, "O1", "REOPENED"));

        var assertion = Assert.Single(deltas);
        Assert.Equal(1, assertion.Weight);
        Assert.Equal("REOPENED", assertion.Row["stage"]);
    }

    /// <summary>The spec's fourth required case: a GROUP BY/aggregate table reading FROM a LATEST BY
    /// table (table-over-table — the same wiring ChainedLatestByTableFeedsAnotherTable in
    /// LatestByTests.cs already proves for ordinary replacement) must reflect a key retraction
    /// correctly — the count must go DOWN, never corrupt into a negative or a phantom re-add. This is
    /// exactly the "unmatched retraction" hazard TableReduceOp's own doc warns about, except this test
    /// proves the case where it does NOT apply: the aggregate was attached BEFORE any rows existed, so
    /// every assert it ever saw was replayed to it in order, and the retraction this test issues
    /// matches an assert the aggregate genuinely already counted.</summary>
    [Fact]
    public void DownstreamAggregateOverALatestByTableStaysCorrectAcrossARetract()
    {
        var t1 = CompileTable("SELECT order_id, stage FROM order_events LATEST BY (order_id)", OrderEvents);
        Assert.True(t1.Ok, string.Join(";", t1.Diagnostics));
        var exec1 = t1.Plan!.CreateExecutor();

        var orderStatesSchema = new SourceSchema("order_states", t1.OutputSchema!.Fields);
        var t2 = CompileTable("SELECT COUNT(*) AS cnt FROM order_states", [], [orderStatesSchema]);
        Assert.True(t2.Ok, string.Join(";", t2.Diagnostics));
        var exec2 = t2.Plan!.CreateExecutor();

        foreach (var d in exec1.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW")))
        {
            exec2.OnTableDelta("order_states", d);
        }
        foreach (var d in exec1.OnStreamEvent("order_events", OrderEvt(1000, "O2", "NEW")))
        {
            exec2.OnTableDelta("order_states", d);
        }

        var beforeRetract = Assert.Single(exec2.Snapshot());
        Assert.Equal(2L, beforeRetract.Value.Row["cnt"]);

        foreach (var d in exec1.OnStreamEvent("order_events", RetractEvt("O1")))
        {
            exec2.OnTableDelta("order_states", d);
        }

        var afterRetract = Assert.Single(exec2.Snapshot());
        Assert.Equal(1L, afterRetract.Value.Row["cnt"]); // down by exactly one, not corrupted/negative
        Assert.DoesNotContain(exec1.Snapshot().Values, v => (string)v.Row["order_id"]! == "O1");
    }
}
