using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>
/// Plan 008 wave 2b: composite (multi-conjunct) equi-keys for table-mode OUTER joins — `ON a.x=b.x AND
/// a.y=b.y` must become a genuine two-component key (TableOuterJoinOp's LeftKeys/RightKeys, both
/// components indexed together), not the pre-008 "first conjunct is the key, every other conjunct folds
/// into a residual" shape. That folded shape is still exactly what non-outer joins get (TableJoinOp,
/// TableSemiAntiOp, PipelineJoinOp, PipelineSubqueryOp — see Sql/Validator.cs's JoinKeyFolding); this file
/// is specifically about the NEW composite-aware path Left/Right/Full take instead.
/// </summary>
public class TableOuterJoinCompositeKeyTests
{
    private static readonly SourceSchema Orders = Schema("orders", ("sym", FieldKind.String), ("venue", FieldKind.String));
    private static readonly SourceSchema Fills = Schema("fills", ("sym", FieldKind.String), ("venue", FieldKind.String), ("qty", FieldKind.Long));

    private const string CompositeSql =
        "SELECT o.sym, o.venue, f.qty FROM orders o LEFT JOIN fills f ON o.sym = f.sym AND o.venue = f.venue";

    [Fact]
    public void BothComponentsBecomeTheKey_NotOneKeyPlusAResidual()
    {
        var compiled = CompileTable(CompositeSql, [Orders], [Fills]).Plan!.Compiled;
        var j = compiled.Joins[0];

        Assert.Equal(2, j.LeftKeys!.Count);
        Assert.Equal(2, j.RightKeys!.Count);
        // Left/Right/Full: Residual is the PURE (non-equi-only) residual — null here, since BOTH ON
        // conjuncts are equi-comparisons and neither is left over. Pre-008 (and still today for a
        // non-outer join compiling this same shape), the second conjunct would instead have folded into
        // Residual as "o.venue = f.venue", with LeftKey/RightKey holding only the first component — the
        // exact shape this composite path deliberately avoids for Left/Right/Full.
        Assert.Null(j.Residual);
    }

    [Fact]
    public void RowDifferingInSecondComponent_DoesNotMatch_Pads()
    {
        var exec = CompileTableAndCreate(CompositeSql, [Orders], [Fills]);

        // Same "sym", different "venue" — a single-key join on sym alone would wrongly match these.
        exec.OnTableDelta("fills", new TableDelta(Evt(0, "fills", ("sym", "AAPL"), ("venue", "NYSE"), ("qty", 10L)), 1));
        var step = exec.OnStreamEvent("orders", Evt(1000, "orders", ("sym", "AAPL"), ("venue", "NASDAQ")));

        var pad = Assert.Single(step);
        Assert.Equal(1, pad.Weight);
        Assert.Equal("AAPL", pad.Row["sym"]);
        Assert.Equal("NASDAQ", pad.Row["venue"]);
        Assert.Null(pad.Row["qty"]);

        var only = Assert.Single(exec.Snapshot());
        Assert.Null(only.Value.Row["qty"]);
    }

    [Fact]
    public void RowMatchingBothComponents_Products_NoPad()
    {
        var exec = CompileTableAndCreate(CompositeSql, [Orders], [Fills]);

        exec.OnTableDelta("fills", new TableDelta(Evt(0, "fills", ("sym", "AAPL"), ("venue", "NASDAQ"), ("qty", 10L)), 1));
        var step = exec.OnStreamEvent("orders", Evt(1000, "orders", ("sym", "AAPL"), ("venue", "NASDAQ")));

        var product = Assert.Single(step);
        Assert.Equal(1, product.Weight);
        Assert.Equal(10L, product.Row["qty"]);

        var only = Assert.Single(exec.Snapshot());
        Assert.Equal(10L, only.Value.Row["qty"]);
    }

    [Fact]
    public void GenuinelyNonEquiResidual_StillAppliesAlongsideTheCompositeKey()
    {
        const string sql =
            "SELECT o.sym, o.venue, f.qty FROM orders o LEFT JOIN fills f ON o.sym = f.sym AND o.venue = f.venue AND f.qty > 0";

        var compiled = CompileTable(sql, [Orders], [Fills]).Plan!.Compiled;
        var j = compiled.Joins[0];
        Assert.Equal(2, j.LeftKeys!.Count); // still just the two equi-conjuncts
        Assert.NotNull(j.Residual); // "f.qty > 0" is genuinely non-equi -> stays a residual, not a key

        var exec = CompileTableAndCreate(sql, [Orders], [Fills]);

        // Both key components match, but the residual ("qty > 0") fails -> still pads, not a product.
        exec.OnTableDelta("fills", new TableDelta(Evt(0, "fills", ("sym", "AAPL"), ("venue", "NASDAQ"), ("qty", 0L)), 1));
        var step1 = exec.OnStreamEvent("orders", Evt(1000, "orders", ("sym", "AAPL"), ("venue", "NASDAQ")));
        var pad = Assert.Single(step1);
        Assert.Null(pad.Row["qty"]);

        // A second fills row for the SAME composite key that DOES pass the residual -> product + unpad.
        var step2 = exec.OnTableDelta("fills", new TableDelta(Evt(0, "fills", ("sym", "AAPL"), ("venue", "NASDAQ"), ("qty", 5L)), 1));
        Assert.Equal(2, step2.Count);
        var product = Assert.Single(step2, d => d.Row["qty"] is not null);
        Assert.Equal(5L, product.Row["qty"]);
        var unpad = Assert.Single(step2, d => d.Row["qty"] is null);
        Assert.Equal(-1, unpad.Weight);
    }
}
