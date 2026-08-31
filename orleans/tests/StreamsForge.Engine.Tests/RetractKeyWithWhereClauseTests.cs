using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Wishlist item 13 gap 2 ("A WHERE clause can silently drop a retraction"), proven end to end through
/// the public <see cref="TableExecutor.OnStreamEvent"/> entry point — the same public API
/// RetractKeyTests.cs already uses for the no-WHERE case, extended here to the documented CDC-mirror
/// shape (docs/cdc.md, pinned by LatestByTests.WhereComesBeforeLatestBy):
/// <c>WHERE stage &lt;&gt; 'CANCELLED' LATEST BY (order_id)</c>. A client retraction carries only the
/// LATEST BY key ("order_id"), never "stage" — so before the fix, evaluating the WHERE clause against a
/// row with no "stage" field at all evaluated false/null, and <see cref="StreamsForge.Engine.Runtime.Ops.TableFilterProjectOp"/>
/// dropped the retraction before <see cref="StreamsForge.Engine.Runtime.Ops.TableLatestByOp"/> ever saw
/// it — the key stayed retained forever, with no trace anywhere that a retraction was even attempted.
///
/// CHOICE MADE (see TableFilterProjectOp.cs's own class doc for the full reasoning): option (a) — a key
/// retraction bypasses WHERE outright, since it targets a KEY, not a row obligated to qualify on content
/// it does not carry — over option (b), rejecting at validate time whenever a LATEST BY table's WHERE
/// touches a non-key column. These tests prove option (a) actually closes the gap: the retraction is no
/// longer silently swallowed.
/// </summary>
public class RetractKeyWithWhereClauseTests
{
    private static EventRecord OrderEvt(long ts, string orderId, string stage) =>
        Evt(ts, "order_events", ("order_id", orderId), ("stage", stage));

    /// <summary>Deliberately carries ONLY the LATEST BY key, exactly like RetractKeyTests.RetractEvt's own
    /// doc explains a real client would — "stage" (the WHERE column) is genuinely absent, not defaulted
    /// or blanked.</summary>
    private static EventRecord RetractEvt(string orderId) =>
        Evt(0, "order_events", ("order_id", orderId), ("_retract", true));

    private static TableExecutor CreateLatestByOrderIdFilteredByStage() =>
        CompileTableAndCreate("SELECT order_id, stage FROM order_events WHERE stage <> 'CANCELLED' LATEST BY (order_id)", OrderEvents);

    [Fact]
    public void RetractStillFreesTheKey_evenThoughTheWhereColumnIsMissingFromTheRetractRow()
    {
        var exec = CreateLatestByOrderIdFilteredByStage();
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW"));
        Assert.Single(exec.Snapshot());

        var deltas = exec.OnStreamEvent("order_events", RetractEvt("O1"));

        // Before the fix this was Assert.Empty(deltas) — the retraction died in TableFilterProjectOp's
        // WHERE check and never reached TableLatestByOp at all; the key would have stayed retained
        // forever with nothing anywhere reporting that anything went wrong.
        var retraction = Assert.Single(deltas);
        Assert.Equal(-1, retraction.Weight);
        Assert.Equal("NEW", retraction.Row["stage"]); // the row it actually held, not the sparse retract row
        Assert.Empty(exec.Snapshot()); // genuinely freed
    }

    [Fact]
    public void RetractOfAnAlreadyFilteredOutKeyIsStillANoOp()
    {
        // A row that never passed WHERE in the first place (so TableLatestByOp never retained it) —
        // the retraction must still be a harmless no-op, not an error, matching TableLatestByOp's own
        // documented "unknown key" behavior.
        var exec = CreateLatestByOrderIdFilteredByStage();
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "CANCELLED")); // fails WHERE, never retained
        Assert.Empty(exec.Snapshot());

        var deltas = exec.OnStreamEvent("order_events", RetractEvt("O1"));

        Assert.Empty(deltas);
        Assert.Empty(exec.Snapshot());
    }

    [Fact]
    public void OrdinaryWhereFilteringOfNonRetractRowsIsUnaffected()
    {
        // Regression check at the public-API level, mirroring LatestByTests' own CDC-mirror coverage:
        // the fix must not turn WHERE into a no-op for anything other than a genuine key retraction.
        var exec = CreateLatestByOrderIdFilteredByStage();

        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "CANCELLED"));
        Assert.Empty(exec.Snapshot()); // still correctly filtered out

        exec.OnStreamEvent("order_events", OrderEvt(1000, "O2", "NEW"));
        Assert.Single(exec.Snapshot()); // still correctly admitted
    }

    [Fact]
    public void RetractThenReassertBehavesLikeAFreshKey()
    {
        var exec = CreateLatestByOrderIdFilteredByStage();
        exec.OnStreamEvent("order_events", OrderEvt(1000, "O1", "NEW"));
        exec.OnStreamEvent("order_events", RetractEvt("O1"));
        Assert.Empty(exec.Snapshot());

        var deltas = exec.OnStreamEvent("order_events", OrderEvt(2000, "O1", "REOPENED"));

        var assertion = Assert.Single(deltas);
        Assert.Equal(1, assertion.Weight);
        Assert.Equal("REOPENED", assertion.Row["stage"]);
    }
}
