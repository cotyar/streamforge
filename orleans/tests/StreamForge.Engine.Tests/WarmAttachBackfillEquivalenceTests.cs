using StreamForge.Engine.Runtime;
using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Wishlist #14 option (a) — the real fix (backfill on attach), exercised at the Engine level exactly the
/// way <c>TableGrain.AttachToTableInputAsync</c> / <c>TableActor</c>'s Dapr mirror now use it: read an
/// upstream's current (<see cref="TableExecutor.Snapshot"/>, <see cref="TableExecutor.LastEpoch"/>) pair,
/// admit <c>Snapshot()</c>'s rows into the downstream as ONE <see cref="TableExecutor.OnTableDeltaBatch"/>
/// call (exactly like any other table-delta batch — same plan, same GROUP BY/JOIN/LATEST BY machinery, not
/// a bypass), then keep applying every later live delta the same way.
///
/// THE REGRESSION CHECK the wishlist calls for: a table attached warm and correctly backfilled must be
/// INDISTINGUISHABLE, row for row, from the identical table built cold over the identical input from the
/// start — and its <see cref="TableExecutor.UnmatchedRetractions"/> must read 0, not merely "less than
/// before" (see <see cref="AggregateOverWarmTableTests"/> for what "before" looked like: nothing at all,
/// converging only once the caller manually replays the missed deltas by hand).
///
/// Grain-level subscribe-then-attach TIMING (the actual race the epoch contract exists to close — see
/// <c>StreamForge.Engine.TableExecutor.LastEpoch</c>'s own doc comment in PublicApi.cs) is covered
/// end-to-end by orleans/tests/StreamForge.Host.Tests/BackfillOnAttachClusterTests.cs; these tests pin the
/// ARITHMETIC that timing argument depends on.
/// </summary>
public class WarmAttachBackfillEquivalenceTests
{
    private static readonly SourceSchema Ticks = Schema("ticks", ("g", FieldKind.String), ("x", FieldKind.Double));

    private static TableExecutor NewLatestBy() =>
        CompileTableAndCreate("SELECT g, x FROM ticks LATEST BY (g)", Ticks);

    private static TableExecutor NewRolled(string sql)
    {
        var latest = CompileTable("SELECT g, x FROM ticks LATEST BY (g)", Ticks);
        Assert.True(latest.Ok, string.Join(";", latest.Diagnostics));
        var input = new SourceSchema("latest_x", latest.OutputSchema!.Fields);
        var rolled = CompileTable(sql, [], [input]);
        Assert.True(rolled.Ok, string.Join(";", rolled.Diagnostics));
        return rolled.Plan!.CreateExecutor();
    }

    private static void AssertSameRows(TableExecutor expected, TableExecutor actual)
    {
        Dictionary<string, long> ToMap(TableExecutor exec) =>
            exec.Snapshot().Values.ToDictionary(v => exec.CanonicalRowKey(v.Row), v => v.Weight, StringComparer.Ordinal);

        var e = ToMap(expected);
        var a = ToMap(actual);
        Assert.Equal(e.Count, a.Count);
        foreach (var (key, weight) in e)
        {
            Assert.True(a.TryGetValue(key, out var actualWeight), $"warm-attached table is missing row '{key}'");
            Assert.Equal(weight, actualWeight);
        }
    }

    [Fact]
    public void Warm_attach_backfill_matches_a_cold_built_table_exactly_and_clears_UnmatchedRetractions()
    {
        // COLD reference: built from t=0, fed every event live — never attaches warm.
        var coldLatest = NewLatestBy();
        var coldRolled = NewRolled("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");

        // WARM subject: its own upstream is warmed first; warmRolled does not exist as a consumer yet.
        var warmLatest = NewLatestBy();
        var warmRolled = NewRolled("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");

        long ts = 1000;

        void FeedUpstreamsOnly(string g, double x)
        {
            foreach (var d in coldLatest.OnStreamEvent("ticks", Evt(ts, "ticks", ("g", g), ("x", x))))
            {
                coldRolled.OnTableDelta("latest_x", d);
            }
            warmLatest.OnStreamEvent("ticks", Evt(ts, "ticks", ("g", g), ("x", x)));
            ts++;
        }

        // Phase 1 — warm the upstream with several keys AND an update to an existing key (retract+assert:
        // exactly the shape that used to destroy the group, per AggregateOverWarmTableTests), all while
        // warmRolled does not exist yet.
        FeedUpstreamsOnly("a", 1.0);
        FeedUpstreamsOnly("b", 2.0);
        FeedUpstreamsOnly("a", 3.0);
        FeedUpstreamsOnly("c", 4.0);

        // Phase 2 — warmRolled "attaches": read (Snapshot, LastEpoch) atomically (no admission happens
        // between these two reads on this single-threaded executor) and admit the snapshot as ONE batch —
        // precisely AttachToTableInputAsync's own sequence.
        var snapshot = warmLatest.Snapshot();
        long attachEpoch = warmLatest.LastEpoch;
        Assert.True(attachEpoch >= 0, "an upstream that has admitted rows must report a real LastEpoch");
        var seedDeltas = snapshot.Values.Select(v => new TableDelta(v.Row, v.Weight)).ToList();
        Assert.Equal(3, seedDeltas.Count); // a, b, c — the update to "a" superseded, not duplicated
        warmRolled.OnTableDeltaBatch("latest_x", seedDeltas);

        // Both tables must already agree after the backfill alone, before any live traffic reaches warmRolled.
        AssertSameRows(coldRolled, warmRolled);
        Assert.Equal(0, warmRolled.UnmatchedRetractions);

        // Phase 3 — keep driving live traffic through both in lockstep, including retractions of keys that
        // predate the attach, and keep asserting equivalence at every step: this must stay true, not just
        // happen to hold once.
        void FeedBothLive(string g, double x)
        {
            foreach (var d in coldLatest.OnStreamEvent("ticks", Evt(ts, "ticks", ("g", g), ("x", x))))
            {
                coldRolled.OnTableDelta("latest_x", d);
            }

            var upstreamOut = warmLatest.OnStreamEvent("ticks", Evt(ts, "ticks", ("g", g), ("x", x)));
            if (upstreamOut.Count > 0)
            {
                warmRolled.OnTableDeltaBatch("latest_x", upstreamOut);
            }
            ts++;

            AssertSameRows(coldRolled, warmRolled);
            Assert.Equal(0, warmRolled.UnmatchedRetractions);
        }

        FeedBothLive("a", 6.0);  // update of a pre-attach key
        FeedBothLive("d", 7.0);  // a brand-new key, never seen by either table before
        FeedBothLive("b", 8.0);  // update of another pre-attach key
        FeedBothLive("c", 9.0);  // update of the last pre-attach key
        FeedBothLive("a", 10.0); // a pre-attach key updated a second time
    }

    /// <summary>Cascading backfill: TableGrain.AttachToTableInputAsync republishes what it backfills through
    /// the SAME ApplyAndPublishAsync path live traffic uses (see that method's own doc comment for why),
    /// which is what lets a THIRD table (chained off the just-backfilled second one) also backfill
    /// correctly. This test pins the underlying Engine mechanism that makes that true: feeding a
    /// downstream's OWN emitted output — including what it emits from ITS OWN backfill admission — into a
    /// third executor's backfill produces the same result as if the whole three-hop chain had been built
    /// cold and live from the start.</summary>
    [Fact]
    public void Backfill_cascades_correctly_through_a_three_hop_chain()
    {
        // COLD reference: A -> B (GROUP BY) -> C (GROUP BY over B), all live from t=0.
        var coldA = NewLatestBy();
        var coldB = NewRolled("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");
        var bOutputSchema = new SourceSchema("rolled_b", new Dictionary<string, FieldKind> { ["g"] = FieldKind.String, ["n"] = FieldKind.Long });
        var coldCCompiled = CompileTable("SELECT COUNT(*) AS groups FROM rolled_b", [], [bOutputSchema]);
        Assert.True(coldCCompiled.Ok, string.Join(";", coldCCompiled.Diagnostics));
        var coldC = coldCCompiled.Plan!.CreateExecutor();

        // WARM subject: A is warmed alone first; B then C attach afterwards, in order, each backfilling
        // from the one before it — exactly a real multi-hop "created mid-session" scenario.
        var warmA = NewLatestBy();
        var warmB = NewRolled("SELECT g, COUNT(*) AS n FROM latest_x GROUP BY g");
        var warmCCompiled = CompileTable("SELECT COUNT(*) AS groups FROM rolled_b", [], [bOutputSchema]);
        Assert.True(warmCCompiled.Ok, string.Join(";", warmCCompiled.Diagnostics));
        var warmC = warmCCompiled.Plan!.CreateExecutor();

        long ts = 1000;
        void FeedA(string g, double x)
        {
            foreach (var d in coldA.OnStreamEvent("ticks", Evt(ts, "ticks", ("g", g), ("x", x))))
            {
                var bOut = coldB.OnTableDelta("latest_x", d);
                foreach (var bd in bOut) coldC.OnTableDelta("rolled_b", bd);
            }
            warmA.OnStreamEvent("ticks", Evt(ts, "ticks", ("g", g), ("x", x)));
            ts++;
        }

        FeedA("a", 1.0);
        FeedA("b", 2.0);
        FeedA("c", 3.0);

        // B attaches to A.
        var aSnapshot = warmA.Snapshot();
        var bSeed = aSnapshot.Values.Select(v => new TableDelta(v.Row, v.Weight)).ToList();
        var bBackfillOut = warmB.OnTableDeltaBatch("latest_x", bSeed);

        // C attaches to B — B's OWN current snapshot (which now reflects its own just-applied backfill,
        // mirroring TableGrain publishing what AttachToTableInputAsync just admitted) is what C reads, not
        // the transient bBackfillOut return value — exactly what a real attach call would see.
        var bSnapshot = warmB.Snapshot();
        var cSeed = bSnapshot.Values.Select(v => new TableDelta(v.Row, v.Weight)).ToList();
        warmC.OnTableDeltaBatch("rolled_b", cSeed);

        AssertSameRows(coldB, warmB);
        AssertSameRows(coldC, warmC);
        Assert.Equal(0, warmB.UnmatchedRetractions);
        Assert.Equal(0, warmC.UnmatchedRetractions);

        // One more live tick through the whole chain, both sides, to prove it stays in lockstep afterwards.
        foreach (var d in coldA.OnStreamEvent("ticks", Evt(ts, "ticks", ("g", "d"), ("x", 4.0))))
        {
            var bOut = coldB.OnTableDelta("latest_x", d);
            foreach (var bd in bOut) coldC.OnTableDelta("rolled_b", bd);
        }
        var warmAOut = warmA.OnStreamEvent("ticks", Evt(ts, "ticks", ("g", "d"), ("x", 4.0)));
        if (warmAOut.Count > 0)
        {
            var warmBOut = warmB.OnTableDeltaBatch("latest_x", warmAOut);
            if (warmBOut.Count > 0) warmC.OnTableDeltaBatch("rolled_b", warmBOut);
        }
        ts++;

        AssertSameRows(coldB, warmB);
        AssertSameRows(coldC, warmC);
    }
}
