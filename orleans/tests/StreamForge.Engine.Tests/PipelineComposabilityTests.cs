using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Proves plan 003 M1 Part B's composability requirement — "such that an entire op-chain can itself be
/// wrapped as a node inside another chain" — which is exactly what plan 004 N1 (derived tables / WITH,
/// windows-in-windows) needs: "a derived node wraps a child operator chain ... an inner windowed query's
/// emissions enter the outer level as events timestamped at window end; outer WINDOW then buckets those".
///
/// This is a HAND-BUILT two-level chain, built WITHOUT any parser/planner support for a real nested-query
/// AST node (plan 004 N1 is not implemented yet — its grammar, its `DerivedTable(SelectQuery)` AST case,
/// its planner wiring don't exist). Instead: two ordinary, independently-compiled SQL statements —
/// - inner: a windowed GROUP BY over "trades" (a real PipelineExecutor, itself an IPipelineOpChain),
/// - outer: a plain (unwindowed) SELECT over a synthetic source "hot" whose schema matches the inner
///   query's OutputSchema —
/// wired by test code that forwards every row the inner chain emits (from OnEvent AND from
/// AdvanceWatermark — a closed window is exactly the "child chain's emissions" plan 004 describes) into
/// the outer chain's OnEvent, using the inner's own window-end timestamp (matching plan 004 N1's stated
/// windows-in-windows semantics: "emissions enter the outer level as events timestamped at window end").
/// This is N1's smoke test.
/// </summary>
public class PipelineComposabilityTests
{
    [Fact]
    public void SelectOverTheOutputOfAnInnerWindowedSelectProducesExpectedRows()
    {
        // Inner: 10s tumbling window, EMIT FINAL, one row per symbol per window with its average price.
        var inner = CompileAndCreate(
            "SELECT symbol, AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL",
            Trades);

        // Outer: filters the inner chain's emissions down to symbols whose windowed average exceeds 50.
        var hotSchema = Schema("hot", ("symbol", FieldKind.String), ("avg_px", FieldKind.Double));
        var outer = CompileAndCreate("SELECT symbol, avg_px FROM hot WHERE avg_px > 50", hotSchema);

        var outerResults = new List<EventRecord>();

        void Forward(IReadOnlyList<EventRecord> innerEmissions)
        {
            // This loop IS the embedding seam: the inner chain's emitted rows become the outer chain's
            // OnEvent input events, exactly as plan 004 N1 describes for a derived-table/CTE node.
            foreach (var row in innerEmissions)
            {
                outerResults.AddRange(outer.OnEvent("hot", row));
            }
        }

        Forward(inner.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true))));
        Forward(inner.OnEvent("trades", Evt(1500, "trades", ("symbol", "AAPL"), ("price", 200.0), ("qty", 1L), ("active", true))));
        Forward(inner.OnEvent("trades", Evt(2000, "trades", ("symbol", "MSFT"), ("price", 10.0), ("qty", 1L), ("active", true))));
        Forward(inner.OnEvent("trades", Evt(2500, "trades", ("symbol", "MSFT"), ("price", 20.0), ("qty", 1L), ("active", true))));

        // Advancing the inner chain's watermark closes the tumbling window — its emissions (the closed
        // window's final rows) are exactly "the child chain's emissions" that become the outer chain's
        // input events.
        Forward(inner.AdvanceWatermark(12000));

        // AAPL(avg=150) passes the outer WHERE; MSFT(avg=15) is filtered out.
        var aapl = Assert.Single(outerResults);
        Assert.Equal("AAPL", aapl["symbol"]);
        Assert.Equal(150.0, aapl["avg_px"]);
    }

    [Fact]
    public void InnerChainCanFeedAJoinInTheOuterChain_NotJustAPlainFilter()
    {
        // A slightly richer embedding: the outer chain JOINs the inner chain's windowed emissions against
        // a second live stream — proving the seam composes with the outer chain's OWN op chain (join
        // stage), not just a bare filter/project, which is what N1's "derived node wraps a child operator
        // chain" ultimately needs (the derived table can sit anywhere a normal FROM/JOIN source could).
        var inner = CompileAndCreate(
            "SELECT symbol, AVG(price) AS avg_px FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS) EMIT FINAL",
            Trades);

        var hotSchema = Schema("hot", ("symbol", FieldKind.String), ("avg_px", FieldKind.Double));
        var outer = CompileAndCreate(
            "SELECT h.symbol, h.avg_px, q.bid FROM hot h JOIN quotes q WITHIN 1 SECONDS ON h.symbol = q.symbol",
            hotSchema, Quotes);

        var outerResults = new List<EventRecord>();
        void Forward(IReadOnlyList<EventRecord> innerEmissions)
        {
            foreach (var row in innerEmissions) outerResults.AddRange(outer.OnEvent("hot", row));
        }

        Forward(inner.OnEvent("trades", Evt(1000, "trades", ("symbol", "AAPL"), ("price", 100.0), ("qty", 1L), ("active", true))));
        var closed = inner.AdvanceWatermark(12000); // closes window at t=10000, emits AAPL avg_px=100

        Assert.Empty(outer.OnEvent("hot", closed[0])); // no matching quote yet
        var matched = outer.OnEvent("quotes", Evt(closed[0].Timestamp + 200, "quotes", ("symbol", "AAPL"), ("bid", 99.0), ("ask", 101.0)));

        var row = Assert.Single(matched);
        Assert.Equal("AAPL", row["symbol"]);
        Assert.Equal(100.0, row["avg_px"]);
        Assert.Equal(99.0, row["bid"]);
    }
}
