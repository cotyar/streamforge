using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>CompileResult.OutputSchema — projection-derived column kinds for stream pipelines
/// (mirror of TableOutputSchemaTests for table mode).</summary>
public class PipelineOutputSchemaTests
{
    [Fact]
    public void WindowedAggregate_DerivesKindsFromProjection()
    {
        var r = Compile(
            "SELECT symbol, COUNT(*) AS trades, SUM(price * qty) AS notional " +
            "FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 5 SECONDS)",
            Trades);

        Assert.True(r.Ok);
        var fields = r.OutputSchema!.Fields;
        Assert.Equal(FieldKind.String, fields["symbol"]);
        Assert.Equal(FieldKind.Long, fields["trades"]);
        Assert.Equal(FieldKind.Double, fields["notional"]);
    }

    [Fact]
    public void QualifiedStar_ExpandsWithSourceKinds()
    {
        var r = Compile("SELECT t.* FROM trades t", Trades);

        Assert.True(r.Ok);
        var fields = r.OutputSchema!.Fields;
        Assert.Equal(FieldKind.Double, fields["price"]);
        Assert.Equal(FieldKind.Long, fields["qty"]);
        Assert.Equal(FieldKind.Bool, fields["active"]);
    }

    [Fact]
    public void JsonAccess_KeepsJsonAndTextKinds()
    {
        var r = Compile(
            "SELECT eventType, payload -> 'user' AS user_obj, payload ->> 'id' AS id_text FROM events",
            Events);

        Assert.True(r.Ok);
        var fields = r.OutputSchema!.Fields;
        Assert.Equal(FieldKind.Json, fields["user_obj"]);
        Assert.Equal(FieldKind.String, fields["id_text"]);
    }

    [Fact]
    public void FailedCompile_HasNullOutputSchema()
    {
        var r = Compile("SELEKT nope", Trades);
        Assert.False(r.Ok);
        Assert.Null(r.OutputSchema);
    }
}
