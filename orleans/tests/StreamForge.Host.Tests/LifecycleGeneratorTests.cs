using StreamForge.Abstractions;
using StreamForge.Host.Generators;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>Generator-invariant coverage for the "lifecycle" profile / "order_events" seed source
/// (Phase L3): pulls a large number of events from the shared static generator and checks the per-order
/// state machine invariants documented in plan 002 Phase L3 hold over the whole simulated run —
/// stage_rank monotonicity, filled_qty monotonicity/bound, FILLED completeness, and terminal-state
/// finality (CANCELED/FILLED never emit a later event for the same order_id).
///
/// NOTE: MarketDataProfiles' lifecycle pool is process-static (mirrors the existing MidPrices pattern for
/// "trades"/"quotes") — these tests only assert properties that hold no matter how much prior state the
/// static pool carries in from other test runs (monotonicity from here forward, not "this order_id has
/// never been seen before"), so they're safe under xunit's default intra-class sequential execution.</summary>
public class LifecycleGeneratorTests
{
    private static SourceDefinition OrderEventsSource() =>
        MarketDataProfiles.SeedSources().Single(s => s.Name == "order_events");

    private static readonly Dictionary<string, long> StageRankByName = new()
    {
        ["NEW"] = 1,
        ["ACK"] = 2,
        ["PART_FILL"] = 3,
        ["FILLED"] = 4,
        ["CANCELED"] = 5,
    };

    [Fact]
    public void Seed_sources_include_a_lifecycle_order_events_source_with_the_documented_schema()
    {
        var src = OrderEventsSource();
        Assert.Equal("lifecycle", src.GeneratorProfile);
        Assert.InRange(src.EventsPerSecond, 4, 6);

        var names = src.Fields.Select(f => f.Name).ToHashSet();
        Assert.Equal(
            new HashSet<string> { "order_id", "symbol", "side", "stage", "stage_rank", "stage_ts", "qty", "filled_qty", "px" },
            names);

        Assert.Equal(FieldType.Long, src.Fields.Single(f => f.Name == "stage_rank").Type);
        Assert.Equal(FieldType.Timestamp, src.Fields.Single(f => f.Name == "stage_ts").Type);
        Assert.Equal(FieldType.Long, src.Fields.Single(f => f.Name == "qty").Type);
        Assert.Equal(FieldType.Long, src.Fields.Single(f => f.Name == "filled_qty").Type);
        Assert.Equal(FieldType.Double, src.Fields.Single(f => f.Name == "px").Type);
    }

    /// <summary>Pulls a large simulated run of events and checks every documented per-order invariant in
    /// one pass, keyed by order_id: stage↔stage_rank mapping is consistent, stage_rank never decreases,
    /// filled_qty never decreases and never exceeds qty, FILLED implies filled_qty == qty, and once an
    /// order_id reaches a terminal stage (FILLED/CANCELED) it never appears again in a later event.</summary>
    [Fact]
    public void Generator_invariants_hold_over_a_simulated_run()
    {
        var src = OrderEventsSource();
        const int eventCount = 5_000;

        var lastStageRank = new Dictionary<string, long>();
        var lastFilledQty = new Dictionary<string, long>();
        var terminalOrders = new HashSet<string>();
        var sawNew = false;
        var sawAck = false;
        var sawPartFill = false;
        var sawFilled = false;
        var sawCanceled = false;

        for (var i = 0; i < eventCount; i++)
        {
            var evt = MarketDataProfiles.GenerateEvent(src);

            var orderId = Assert.IsType<string>(evt["order_id"]);
            Assert.StartsWith("ORD-", orderId);

            var stage = Assert.IsType<string>(evt["stage"]);
            Assert.True(StageRankByName.TryGetValue(stage, out var expectedRank), $"unknown stage '{stage}'");

            var stageRank = Assert.IsType<long>(evt["stage_rank"]);
            Assert.Equal(expectedRank, stageRank); // stage <-> stage_rank mapping is consistent

            var qty = Assert.IsType<long>(evt["qty"]);
            var filledQty = Assert.IsType<long>(evt["filled_qty"]);
            Assert.IsType<double>(evt["px"]);
            Assert.IsType<string>(evt["symbol"]);
            Assert.IsType<string>(evt["side"]);
            Assert.Equal(evt[StreamForge.Engine.EventRecord.TimestampField], evt["stage_ts"]);

            Assert.False(terminalOrders.Contains(orderId),
                $"order {orderId} emitted an event after reaching a terminal stage");

            Assert.InRange(filledQty, 0, qty); // filled_qty <= qty always

            if (lastStageRank.TryGetValue(orderId, out var prevRank))
            {
                Assert.True(stageRank >= prevRank, $"order {orderId} stage_rank regressed {prevRank} -> {stageRank}");
            }
            lastStageRank[orderId] = stageRank;

            if (lastFilledQty.TryGetValue(orderId, out var prevFilled))
            {
                Assert.True(filledQty >= prevFilled, $"order {orderId} filled_qty regressed {prevFilled} -> {filledQty}");
            }
            lastFilledQty[orderId] = filledQty;

            if (stage == "FILLED")
            {
                Assert.Equal(qty, filledQty); // FILLED => filled_qty == qty
            }

            if (stage is "FILLED" or "CANCELED")
            {
                terminalOrders.Add(orderId);
            }

            sawNew |= stage == "NEW";
            sawAck |= stage == "ACK";
            sawPartFill |= stage == "PART_FILL";
            sawFilled |= stage == "FILLED";
            sawCanceled |= stage == "CANCELED";
        }

        Assert.True(sawNew, "expected at least one NEW event");
        Assert.True(sawAck, "expected at least one ACK event");
        Assert.True(sawPartFill, "expected at least one PART_FILL event across a 5000-event run");
        Assert.True(sawFilled, "expected at least one FILLED event across a 5000-event run");
        Assert.True(sawCanceled, "expected at least one CANCELED event across a 5000-event run (~15% cancel rate)");
    }

    /// <summary>Roughly checks the documented ~15% cancel rate by tracking each order_id's terminal
    /// outcome (FILLED vs CANCELED) over a large run — loose bounds since it's a probabilistic draw, not
    /// an exact invariant.</summary>
    [Fact]
    public void Roughly_fifteen_percent_of_completed_orders_cancel()
    {
        var src = OrderEventsSource();
        var outcome = new Dictionary<string, string>();

        for (var i = 0; i < 8_000; i++)
        {
            var evt = MarketDataProfiles.GenerateEvent(src);
            var orderId = (string)evt["order_id"]!;
            var stage = (string)evt["stage"]!;
            if (stage is "FILLED" or "CANCELED")
            {
                outcome[orderId] = stage;
            }
        }

        Assert.True(outcome.Count > 50, "expected a good number of orders to complete over 8000 events");
        var cancelRate = outcome.Values.Count(s => s == "CANCELED") / (double)outcome.Count;
        Assert.InRange(cancelRate, 0.03, 0.35); // loose band around the documented ~15%
    }
}
