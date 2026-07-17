using StreamForge.Host.Grains;
using Xunit;

namespace StreamForge.Host.Tests;

/// <summary>Pure, Orleans-free tests for TableGroupKeyExtractor (SQL-text-derived GROUP BY identity
/// columns) and RowKeyCodec (deterministic row-identity key encoding) — the two building blocks
/// TableHistoryGrain uses to derive a stable per-row identity key. See TableGroupKeyExtractor's class
/// comment for why this text-based approach was chosen over the frozen Engine's internals.</summary>
public sealed class HistoryKeyExtractorTests
{
    [Fact]
    public void ExtractIdentityColumns_BareGroupByColumn_ReturnsThatColumn()
    {
        var sql = "SELECT symbol, COUNT(*) AS trades, SUM(qty) AS total_qty FROM trades GROUP BY symbol";
        var columns = TableGroupKeyExtractor.ExtractIdentityColumns(sql);
        Assert.Equal(["symbol"], columns);
    }

    [Fact]
    public void ExtractIdentityColumns_JsonPathGroupByMatchingAliasedSelectItem_ReturnsAlias()
    {
        var sql = "SELECT e.payload -> 'order' ->> 'symbol' AS symbol, COUNT(*) AS orders FROM app_events e " +
                   "WHERE e.payload -> 'user' ->> 'tier' = 'gold' GROUP BY e.payload -> 'order' ->> 'symbol'";
        var columns = TableGroupKeyExtractor.ExtractIdentityColumns(sql);
        Assert.Equal(["symbol"], columns);
    }

    [Fact]
    public void ExtractIdentityColumns_NoGroupBy_ReturnsNull()
    {
        var sql = "SELECT p.symbol, p.trades, p.avg_price FROM positions p WHERE p.trades > 50";
        Assert.Null(TableGroupKeyExtractor.ExtractIdentityColumns(sql));
    }

    [Fact]
    public void ExtractIdentityColumns_MultipleGroupByColumns_ReturnsAllInOrder()
    {
        var sql = "SELECT symbol, side, COUNT(*) AS n FROM trades GROUP BY symbol, side";
        var columns = TableGroupKeyExtractor.ExtractIdentityColumns(sql);
        Assert.Equal(["symbol", "side"], columns);
    }

    [Fact]
    public void ExtractIdentityColumns_QualifiedGroupByColumn_MatchesDefaultAliasedSelectItem()
    {
        var sql = "SELECT t.symbol, COUNT(*) AS n FROM trades t GROUP BY t.symbol";
        var columns = TableGroupKeyExtractor.ExtractIdentityColumns(sql);
        Assert.Equal(["symbol"], columns);
    }

    [Fact]
    public void ExtractIdentityColumns_GroupByExpressionNotProjected_FallsBackToNull()
    {
        // GROUP BY references an expression that isn't textually present in the SELECT list at all —
        // the extractor can't confidently map it to an output column, so it degrades to null (whole-row
        // fallback) rather than guessing.
        var sql = "SELECT sym AS symbol, COUNT(*) AS n FROM trades GROUP BY other_expr";
        Assert.Null(TableGroupKeyExtractor.ExtractIdentityColumns(sql));
    }

    [Fact]
    public void ExtractIdentityColumns_WhitespaceVariation_StillMatches()
    {
        var sql = "SELECT   symbol,COUNT(*)   AS  trades FROM trades\nGROUP   BY\n  symbol";
        var columns = TableGroupKeyExtractor.ExtractIdentityColumns(sql);
        Assert.Equal(["symbol"], columns);
    }

    [Fact]
    public void ExtractIdentityColumns_NullOrEmptySql_ReturnsNull()
    {
        Assert.Null(TableGroupKeyExtractor.ExtractIdentityColumns(null));
        Assert.Null(TableGroupKeyExtractor.ExtractIdentityColumns(""));
        Assert.Null(TableGroupKeyExtractor.ExtractIdentityColumns("   "));
    }

    [Fact]
    public void EncodeIdentity_WithIdentityColumns_IsDeterministicAndDistinguishesValues()
    {
        var rowAapl = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["trades"] = 5L, ["avg_price"] = 101.5 };
        var rowMsft = new Dictionary<string, object?> { ["symbol"] = "MSFT", ["trades"] = 5L, ["avg_price"] = 101.5 };

        var key1 = RowKeyCodec.EncodeIdentity(rowAapl, ["symbol"]);
        var key1Again = RowKeyCodec.EncodeIdentity(rowAapl, ["symbol"]);
        var key2 = RowKeyCodec.EncodeIdentity(rowMsft, ["symbol"]);

        Assert.Equal(key1, key1Again);
        Assert.NotEqual(key1, key2);
    }

    [Fact]
    public void EncodeIdentity_WithIdentityColumns_IgnoresNonIdentityFieldChanges()
    {
        // The whole point of a group-by identity key: two versions of the same group (different
        // aggregate values) must collide to the SAME key, so history can accumulate versions.
        var v1 = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["trades"] = 1L, ["avg_price"] = 100.0 };
        var v2 = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["trades"] = 2L, ["avg_price"] = 102.5 };

        Assert.Equal(RowKeyCodec.EncodeIdentity(v1, ["symbol"]), RowKeyCodec.EncodeIdentity(v2, ["symbol"]));
    }

    [Fact]
    public void EncodeIdentity_NoIdentityColumns_FallsBackToWholeRowAndChangesWithAnyField()
    {
        var v1 = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["avg_price"] = 100.0 };
        var v2 = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["avg_price"] = 102.5 };

        Assert.NotEqual(RowKeyCodec.EncodeIdentity(v1, null), RowKeyCodec.EncodeIdentity(v2, null));
    }

    [Fact]
    public void EncodeIdentity_NoIdentityColumns_IgnoresKeyOrderAndTransportMetadataFields()
    {
        var v1 = new Dictionary<string, object?> { ["a"] = 1L, ["b"] = "x", ["_ts"] = 111L, ["_source"] = "trades" };
        var v2 = new Dictionary<string, object?> { ["_source"] = "quotes", ["b"] = "x", ["_ts"] = 999L, ["a"] = 1L };

        Assert.Equal(RowKeyCodec.EncodeIdentity(v1, null), RowKeyCodec.EncodeIdentity(v2, null));
    }
}
