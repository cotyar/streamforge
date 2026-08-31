using StreamsForge.Engine.Dataflow;
using StreamsForge.Engine.Runtime;
using StreamsForge.Engine.Runtime.Ops;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Wishlist item 13 gap 2, at the op level (see RetractKeyWithWhereClauseTests.cs for the same fix
/// proven end to end through TableExecutor.OnStreamEvent on the documented CDC-mirror shape): a client
/// key retraction (<see cref="IngressRowAcceptance.RetractField"/>) carries ONLY the key columns it
/// means to retract, never the rest of the row — so a WHERE clause over any OTHER column would, before
/// this fix, see a missing field, evaluate false/null, and drop the retraction in
/// <see cref="TableFilterProjectOp"/> before it ever reached <see cref="TableLatestByOp"/>. This file
/// pins the fix directly on <see cref="TableFilterProjectOp"/>, mirroring TableOpsUnitTests.cs's own
/// "instantiate the op directly, not through the façade" style for the same reason stated there: it
/// asserts on exactly the boundary that used to drop the row, independent of which op sits downstream.
/// </summary>
public class TableFilterProjectOpKeyRetractionTests
{
    private static readonly Epoch E0 = new(0);

    [Fact]
    public void OnBatch_AKeyRetractionBypassesWhereEvenWhenTheWhereColumnIsMissing()
    {
        // The exact documented CDC-mirror shape (docs/cdc.md, pinned by LatestByTests.WhereComesBeforeLatestBy):
        // WHERE filters on "stage", the LATEST BY key is "order_id" — a retraction only ever carries
        // order_id, never stage.
        var compiled = CompileTable(
            "SELECT order_id, stage FROM order_events WHERE stage <> 'CANCELLED' LATEST BY (order_id)",
            OrderEvents).Plan!.Compiled;
        var alias = compiled.Sources[0].Alias;
        var op = new TableFilterProjectOp(compiled);
        var ingest = new TableIngestOp(alias);

        var retractEvt = Evt(0, "order_events", ("order_id", "O1"), ("_retract", true)); // no "stage" at all
        var admitted = ingest.OnBatch(E0, [new TableDelta(retractEvt, 1)]);
        Assert.Equal(-1, admitted[0].Weight); // TableIngestOp already flipped it

        var filtered = op.OnBatch(E0, admitted);

        var d = Assert.Single(filtered); // NOT dropped, despite "stage" being absent from the row
        Assert.Equal(-1, d.Weight);
        Assert.False(d.Row.Fields.ContainsKey($"{alias}_stage")); // still genuinely missing — bypass, not a default
    }

    [Fact]
    public void OnBatch_AnOrdinaryRowStillObeysWhere_RegressionCheck()
    {
        // The fix must not turn WHERE into a no-op for everything — only for a genuine key retraction.
        var compiled = CompileTable(
            "SELECT order_id, stage FROM order_events WHERE stage <> 'CANCELLED' LATEST BY (order_id)",
            OrderEvents).Plan!.Compiled;
        var alias = compiled.Sources[0].Alias;
        var op = new TableFilterProjectOp(compiled);
        var ingest = new TableIngestOp(alias);

        var cancelled = ingest.OnBatch(E0, [new TableDelta(Evt(1000, "order_events", ("order_id", "O1"), ("stage", "CANCELLED")), 1)]);
        Assert.Empty(op.OnBatch(E0, cancelled)); // fails WHERE, correctly dropped — same as before the fix

        var open = ingest.OnBatch(E0, [new TableDelta(Evt(1000, "order_events", ("order_id", "O1"), ("stage", "NEW")), 1)]);
        Assert.Single(op.OnBatch(E0, open)); // passes WHERE, admitted — same as before the fix
    }

    [Fact]
    public void OnBatchTerminal_AKeyRetractionBypassesWhereAndIsProjected()
    {
        // A plain (non-LATEST-BY) terminal shape — RetractConsumerValidation would reject this table as a
        // live direct consumer of a retract-carrying source at the ingest boundary, but this op has no
        // visibility into that (see TableIngestOp's own doc on why it "unconditionally honors the flag
        // for every shape"), so it must still behave safely and consistently rather than depend on never
        // seeing one — see the class doc's "at worst incomplete" note.
        var compiled = CompileTable("SELECT order_id, stage FROM order_events WHERE stage <> 'CANCELLED'", OrderEvents).Plan!.Compiled;
        var alias = compiled.Sources[0].Alias;
        var op = new TableFilterProjectOp(compiled);
        var ingest = new TableIngestOp(alias);

        var retractEvt = Evt(0, "order_events", ("order_id", "O1"), ("_retract", true));
        var admitted = ingest.OnBatch(E0, [new TableDelta(retractEvt, 1)]);

        var outp = op.OnBatchTerminal(E0, admitted);

        var d = Assert.Single(outp);
        Assert.Equal(-1, d.Weight);
        Assert.Equal("O1", d.Row["order_id"]);
        Assert.Null(d.Row["stage"]); // projected straight through; genuinely absent, not defaulted
    }
}
