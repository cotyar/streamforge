using Xunit;
using static StreamForge.Engine.Tests.TestHelpers;

namespace StreamForge.Engine.Tests;

public class TableOutputSchemaTests
{
    [Fact]
    public void PlainProjectionPreservesColumnKinds()
    {
        var r = CompileTable("SELECT symbol, price, qty, active FROM trades", Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.String, r.OutputSchema!.Fields["symbol"]);
        Assert.Equal(FieldKind.Double, r.OutputSchema.Fields["price"]);
        Assert.Equal(FieldKind.Long, r.OutputSchema.Fields["qty"]);
        Assert.Equal(FieldKind.Bool, r.OutputSchema.Fields["active"]);
    }

    [Fact]
    public void AggregatesProduceLongAndDoubleKinds()
    {
        var sql = "SELECT symbol, COUNT(*) AS cnt, SUM(qty) AS total_qty, AVG(price) AS avgp, MIN(price) AS low, MAX(price) AS high FROM trades GROUP BY symbol";
        var r = CompileTable(sql, Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.String, r.OutputSchema!.Fields["symbol"]);
        Assert.Equal(FieldKind.Long, r.OutputSchema.Fields["cnt"]);
        Assert.Equal(FieldKind.Long, r.OutputSchema.Fields["total_qty"]); // SUM over a Long column stays Long
        Assert.Equal(FieldKind.Double, r.OutputSchema.Fields["avgp"]); // AVG always Double
        Assert.Equal(FieldKind.Double, r.OutputSchema.Fields["low"]); // MIN over a Double column
        Assert.Equal(FieldKind.Double, r.OutputSchema.Fields["high"]);
    }

    [Fact]
    public void ArrowArrowJsonExtractionIsStringKind()
    {
        var sql = "SELECT e.payload -> 'order' ->> 'symbol' AS symbol, COUNT(*) AS orders FROM app_events e " +
                  "WHERE e.payload -> 'user' ->> 'tier' = 'gold' GROUP BY e.payload -> 'order' ->> 'symbol'";
        var appEvents = Schema("app_events", ("eventType", FieldKind.String), ("payload", FieldKind.Json));
        var r = CompileTable(sql, appEvents);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.String, r.OutputSchema!.Fields["symbol"]);
        Assert.Equal(FieldKind.Long, r.OutputSchema.Fields["orders"]);
    }

    [Fact]
    public void StarProjectionUsesSourceSchemaKinds()
    {
        var r = CompileTable("SELECT * FROM trades", Trades);
        Assert.True(r.Ok, string.Join(";", r.Diagnostics));
        Assert.Equal(FieldKind.Double, r.OutputSchema!.Fields["price"]);
        Assert.Equal(FieldKind.Long, r.OutputSchema.Fields["qty"]);
    }
}
