using StreamForge.Engine.Runtime;
using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Wishlist #15 — "retract/assert of one upstream change should be applied atomically downstream". Pins
/// the reported symptom end to end through three chained table-mode executors, exactly the demo's shape:
///
/// <c>ticks (stream) --LATEST BY--&gt; latest_x --GROUP BY--&gt; agg --LEFT JOIN--&gt; joined</c>
///
/// A single change to one key in `ticks` makes `latest_x` emit retract(old)+assert(new) — TWO deltas for
/// ONE upstream change. Before this fix, a host feeding that pair through a per-element loop
/// (<c>TableGrain.OnTableDeltaBatchAsync</c>'s old shape — see its class doc) gave each element its own
/// epoch on `agg`, and `agg`'s own retract+assert (again two elements, again looped one at a time
/// downstream) gave EACH of ITS elements its own epoch on `joined`. `joined`'s LEFT JOIN op
/// (<see cref="StreamForge.Engine.Runtime.Ops.TableOuterJoinOp"/>) reacts to a right-side retract by
/// checking whether the key is STILL present on the right, and between processing the retract and the
/// matching assert it is not — so it pads the joined row's right-hand columns with NULL and immediately
/// re-asserts the pad, then un-pads again once the assert follows. Observed one call at a time, that pad
/// is a real, published intermediate state: `joined.total` reads NULL for however long it takes the next
/// delta to arrive.
///
/// <see cref="TableExecutor.OnTableDeltaBatch"/> (this fix's first half) keeps a whole upstream batch under
/// ONE epoch instead of splitting it into one epoch per element; the Engine-internal
/// <c>ConsolidateEpochOutput</c> (this fix's second half, in TableExecutorImpl.cs) nets that epoch's raw
/// output by canonical row key before it is ever returned, so the assert-then-retract (or the reverse) of
/// the exact same pad row cancels out and never leaves the table. What a consumer of `joined` sees for one
/// upstream change to `ticks` is exactly the NET effect — retract(old total) and assert(new total) — never
/// the pad in between.
/// </summary>
public class EpochAtomicConsolidationTests
{
    private static readonly SourceSchema Ticks = Schema("ticks", ("g", FieldKind.String), ("x", FieldKind.Double));
    private static readonly SourceSchema Orders = Schema("orders", ("tag", FieldKind.String));

    private sealed record Chain(TableExecutor Latest, TableExecutor Agg, TableExecutor Joined);

    /// <summary>Builds the three-table chain described in the class doc. `agg` is a GROUP BY with exactly
    /// ONE contributing row per group (SUM over a LATEST BY table, one row per key) — chosen deliberately:
    /// with only one contributor, `agg`'s OWN <see cref="Ops.TableReduceOp"/> output for a value change is
    /// a clean two-element [retract(old), assert(new)] (the group's weight passes through exactly zero
    /// between the two, so it's dropped and recreated rather than flapping through a negative-weight
    /// intermediate — see TableReduceOp's own doc comment) — which is exactly the shape that, fed to the
    /// downstream LEFT JOIN one element at a time, flips its right-side presence check false in between and
    /// produces the reported NULL pad. A multi-contributor group would additionally flap on agg's own
    /// output (already covered by AggregateOverWarmTableTests' sibling scenario) — this test isolates the
    /// join-side half of the bug.</summary>
    private static Chain BuildChain()
    {
        var latest = CompileTable("SELECT g, x FROM ticks LATEST BY (g)", Ticks);
        Assert.True(latest.Ok, string.Join(";", latest.Diagnostics));
        var latestSchema = new SourceSchema("latest_x", latest.OutputSchema!.Fields);

        var agg = CompileTable("SELECT g, SUM(x) AS total FROM latest_x GROUP BY g", [], [latestSchema]);
        Assert.True(agg.Ok, string.Join(";", agg.Diagnostics));
        var aggSchema = new SourceSchema("agg", agg.OutputSchema!.Fields);

        var joined = CompileTable("SELECT o.tag, b.total FROM orders o LEFT JOIN agg b ON o.tag = b.g", [Orders], [aggSchema]);
        Assert.True(joined.Ok, string.Join(";", joined.Diagnostics));

        return new Chain(latest.Plan!.CreateExecutor(), agg.Plan!.CreateExecutor(), joined.Plan!.CreateExecutor());
    }

    [Fact]
    public void LeftJoinOntoAggregateTable_OneUpstreamChange_IsAppliedAtomically_NeverObservesAnIntermediateNull()
    {
        var (latest, agg, joined) = BuildChain();
        long ts = 1000;

        // Seed: key "a" gets its first value (5.0), fed through the whole chain via the batch entry point —
        // exactly how TableGrain.OnTableDeltaBatchAsync/TableActor.ProcessTableDeltasAsync feed a real
        // upstream table's published batch post-fix.
        var seedLatest = latest.OnStreamEvent("ticks", Evt(ts++, "ticks", ("g", "a"), ("x", 5.0)));
        var seedAgg = agg.OnTableDeltaBatch("latest_x", seedLatest);
        joined.OnTableDeltaBatch("agg", seedAgg);

        // The LEFT side row that will match it.
        joined.OnStreamEvent("orders", Evt(ts++, "orders", ("tag", "a")));

        var before = Assert.Single(joined.Snapshot());
        Assert.Equal(5.0, before.Value.Row["total"]);

        // ONE upstream change: key "a" moves from 5.0 to 9.0. LATEST BY always emits retract(old)+assert(new)
        // for an existing key — one upstream change, two deltas.
        var latestDeltas = latest.OnStreamEvent("ticks", Evt(ts++, "ticks", ("g", "a"), ("x", 9.0)));
        Assert.Equal(2, latestDeltas.Count);

        // Both hops go through the BATCH entry point, one call each — this is the fix under test.
        var aggDeltas = agg.OnTableDeltaBatch("latest_x", latestDeltas);
        var joinedDeltas = joined.OnTableDeltaBatch("agg", aggDeltas);

        // THE ASSERTION: nothing `joined` ever returns for this one upstream change carries a NULL total
        // for "a" — the reported flap never reaches a caller/consumer of this table.
        foreach (var delta in joinedDeltas)
        {
            if (Equals(delta.Row["tag"], "a"))
            {
                Assert.NotNull(delta.Row["total"]);
            }
        }

        // And the net effect is exactly the expected retract(old)+assert(new) — not zero, not a longer
        // flapping sequence collapsed to nothing it shouldn't have been.
        Assert.Equal(2, joinedDeltas.Count);
        var retracted = Assert.Single(joinedDeltas, d => d.Weight < 0);
        Assert.Equal(5.0, retracted.Row["total"]);
        var asserted = Assert.Single(joinedDeltas, d => d.Weight > 0);
        Assert.Equal(9.0, asserted.Row["total"]);

        var after = Assert.Single(joined.Snapshot());
        Assert.Equal(9.0, after.Value.Row["total"]);
    }

    /// <summary>The contrast that proves the fix is load-bearing and the test above isn't vacuous: feeding
    /// the EXACT same two-delta change through the OLD per-element shape (one <see cref="TableExecutor.OnTableDelta"/>
    /// call per element — what TableGrain.OnTableDeltaBatchAsync/TableActor.ProcessTableDeltasAsync did
    /// before this fix) DOES produce an observable intermediate NULL, because each element gets its own
    /// epoch and ConsolidateEpochOutput never sees more than one element at a time to net against.</summary>
    [Fact]
    public void WithoutBatchAdmission_TheSamePerElementLoop_DoesObserveAnIntermediateNull()
    {
        var (latest, agg, joined) = BuildChain();
        long ts = 1000;

        var seedLatest = latest.OnStreamEvent("ticks", Evt(ts++, "ticks", ("g", "a"), ("x", 5.0)));
        foreach (var d in seedLatest)
        {
            foreach (var d2 in agg.OnTableDelta("latest_x", d))
            {
                joined.OnTableDelta("agg", d2);
            }
        }
        joined.OnStreamEvent("orders", Evt(ts++, "orders", ("tag", "a")));

        var latestDeltas = latest.OnStreamEvent("ticks", Evt(ts++, "ticks", ("g", "a"), ("x", 9.0)));
        Assert.Equal(2, latestDeltas.Count);

        var observedNullTotalForA = false;
        foreach (var d in latestDeltas) // pre-fix TableGrain.OnTableDeltaBatchAsync: one OnTableDelta call per element
        {
            foreach (var d2 in agg.OnTableDelta("latest_x", d)) // pre-fix downstream loop, same shape
            {
                foreach (var d3 in joined.OnTableDelta("agg", d2))
                {
                    if (Equals(d3.Row["tag"], "a") && d3.Row["total"] is null)
                    {
                        observedNullTotalForA = true;
                    }
                }
            }
        }

        Assert.True(observedNullTotalForA, "expected the per-element admission shape to reproduce the reported NULL flap");

        // Same eventual consistency either way — only the atomicity in between differs.
        var after = Assert.Single(joined.Snapshot());
        Assert.Equal(9.0, after.Value.Row["total"]);
    }
}
