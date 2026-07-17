using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

/// <summary>JSON expressions ('->'/'->>') in table mode — the exact SQL seeded as RegistryGrain's
/// "gold_tier_orders" demo table.</summary>
public class TableJsonTests
{
    private const string GoldTierSql =
        "SELECT e.payload -> 'order' ->> 'symbol' AS symbol, COUNT(*) AS orders FROM app_events e " +
        "WHERE e.payload -> 'user' ->> 'tier' = 'gold' GROUP BY e.payload -> 'order' ->> 'symbol'";

    private static readonly SourceSchema AppEvents = Schema("app_events", ("eventType", FieldKind.String), ("payload", FieldKind.Json));

    private static EventRecord JsonEvent(string tier, string symbol) => Evt(
        1000, "app_events",
        ("eventType", "order.placed"),
        ("payload", new Dictionary<string, object?>
        {
            ["user"] = new Dictionary<string, object?> { ["tier"] = tier },
            ["order"] = new Dictionary<string, object?> { ["symbol"] = symbol },
        }));

    [Fact]
    public void GoldTierSeedSqlCompiles()
    {
        var r = CompileTable(GoldTierSql, AppEvents);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Contains("app_events", r.StreamInputs);
    }

    [Fact]
    public void GoldTierEventIncrementsOrderCountForItsSymbol()
    {
        var exec = CompileTableAndCreate(GoldTierSql, AppEvents);

        var deltas = exec.OnStreamEvent("app_events", JsonEvent("gold", "AAPL"));

        var delta = Assert.Single(deltas);
        Assert.Equal(1, delta.Weight);
        Assert.Equal("AAPL", delta.Row["symbol"]);
        Assert.Equal(1L, delta.Row["orders"]);
    }

    [Fact]
    public void NonGoldTierEventIsFilteredOutByWhere()
    {
        var exec = CompileTableAndCreate(GoldTierSql, AppEvents);

        var deltas = exec.OnStreamEvent("app_events", JsonEvent("silver", "AAPL"));

        Assert.Empty(deltas);
    }

    [Fact]
    public void SecondGoldTierEventForSameSymbolRetractsAndReasserts()
    {
        var exec = CompileTableAndCreate(GoldTierSql, AppEvents);

        exec.OnStreamEvent("app_events", JsonEvent("gold", "AAPL"));
        var deltas = exec.OnStreamEvent("app_events", JsonEvent("gold", "AAPL"));

        Assert.Equal(2, deltas.Count);
        Assert.Equal(-1, deltas[0].Weight);
        Assert.Equal(1L, deltas[0].Row["orders"]);
        Assert.Equal(1, deltas[1].Weight);
        Assert.Equal(2L, deltas[1].Row["orders"]);
    }
}
