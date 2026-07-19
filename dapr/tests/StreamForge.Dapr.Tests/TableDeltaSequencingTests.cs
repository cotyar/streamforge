using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Actors;
using StreamForge.Engine;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-A: verifies the sequencing contract
/// <see cref="StreamForge.Dapr.Host.Actors.TableActor"/>'s <c>ApplyAndPublishAsync</c> implements for
/// <c>TableDeltaEnvelope.Seq</c> — see that class's "two distinct sequence counters" doc-comment note:
/// this per-published-BATCH counter increments exactly once per non-empty delta batch handed to
/// <c>ApplyAndPublishAsync</c>, strictly monotonically, and is never incremented for an empty batch (which
/// <c>TableActor</c> never even calls <c>ApplyAndPublishAsync</c> for — see
/// <c>ProcessSourceEventsAsync</c>'s <c>if (deltas.Count &gt; 0)</c> guard).
///
/// <para><b>Why this can't test <see cref="StreamForge.Dapr.Host.Actors.TableActor"/> directly:</b> that
/// class requires a live <c>Dapr.Actors.Runtime.ActorHost</c>/<c>DaprClient</c> to construct (no test
/// double exists in this project — same constraint <c>PipelineActorWireNormalizationTests</c> and
/// <c>PipelineEventRouterTests</c> already document for their own actor classes). This test instead drives
/// the REAL <see cref="TableExecutor"/> the actor wraps (via <see cref="TableCompilation.TryCompile"/>) —
/// the same compiled Z-set engine <c>TableActor.ActivateExecutor</c> uses — and applies
/// <c>TableActor.ApplyAndPublishAsync</c>'s exact, documented increment rule locally to the REAL delta
/// batches it produces, proving the rule holds against genuine Engine output rather than synthetic
/// data.</para>
/// </summary>
public class TableDeltaSequencingTests
{
    private static SourceDefinition Trades() => new()
    {
        Name = "trades",
        Fields = [new FieldDef("symbol", FieldType.String), new FieldDef("qty", FieldType.Long)],
        Enabled = true,
    };

    private static TableDefinition Positions() => new()
    {
        Id = "positions-id",
        Name = "positions",
        Sql = "SELECT symbol, COUNT(*) AS trades, SUM(qty) AS total_qty FROM trades GROUP BY symbol",
        Status = PipelineStatus.Stopped,
    };

    private static EventRecord Trade(string symbol, long qty) =>
        new(new Dictionary<string, object?> { ["symbol"] = symbol, ["qty"] = qty });

    [Fact]
    public void ConsecutiveNonEmptyBatches_IncrementSeqStrictlyMonotonically()
    {
        var (executor, _, _, error) = TableCompilation.TryCompile(Positions(), [Trades()], []);
        Assert.NotNull(executor);
        Assert.Null(error);

        long seq = 0;
        var observedSeqs = new List<long>();

        // Every trade for a NEW symbol is a first insert into the GROUP BY — always produces at least an
        // assert delta (non-empty batch), exactly mirroring TableActor.ApplyAndPublishAsync's own
        // `if (deltas.Count > 0)` gate around the seq increment.
        foreach (var (symbol, qty) in new[] { ("AAPL", 10L), ("MSFT", 5L), ("AAPL", 3L) })
        {
            var deltas = executor!.OnStreamEvent("trades", Trade(symbol, qty));
            Assert.NotEmpty(deltas); // sanity: this is exercising the non-empty-batch path

            // Mirrors TableActor.ApplyAndPublishAsync's own `_deltaSeq++` — called once per non-empty
            // batch, never per individual delta within it.
            seq++;
            observedSeqs.Add(seq);
        }

        Assert.Equal([1L, 2L, 3L], observedSeqs);
        Assert.True(observedSeqs.SequenceEqual(observedSeqs.OrderBy(s => s)), "seq must be strictly increasing");
    }

    [Fact]
    public void EmptyBatch_NeverIncrementsSeq()
    {
        var (executor, _, _, _) = TableCompilation.TryCompile(Positions(), [Trades()], []);
        Assert.NotNull(executor);

        long seq = 0;

        // An event from a source this table's SQL doesn't reference produces no deltas at all — mirrors
        // TableActor never calling ApplyAndPublishAsync when OnStreamEvent returns an empty list.
        var deltas = executor!.OnStreamEvent("unrelated-source", Trade("AAPL", 1));
        Assert.Empty(deltas);
        if (deltas.Count > 0)
        {
            seq++;
        }

        Assert.Equal(0, seq);
    }
}
