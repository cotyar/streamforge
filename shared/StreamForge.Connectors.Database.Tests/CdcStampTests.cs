using StreamForge.Connectors.Database;
using Xunit;

namespace StreamForge.Connectors.Database.Tests;

/// <summary>
/// <see cref="CdcStamp"/> is the one place the native Postgres and SQL Server CDC readers write
/// <c>_op</c>/<c>_weight</c>/<c>_table</c>/<c>_ts</c>, so these tests exist to pin the exact shape of
/// that stamp — including the drift guard against <see cref="DbSinkPlanner.WeightColumn"/>, since the
/// two constants living in different files is exactly how they would silently diverge.
/// </summary>
public class CdcStampTests
{
    [Fact]
    public void WeightColumnMatchesDbSinkPlannersWeightColumn()
    {
        // The whole point of this constant: a CDC row is a sink row like any other by the time it
        // reaches a sink. If these ever disagree, a native CDC row's weight silently stops being the
        // column DbSinkPlanner looks for.
        Assert.Equal(DbSinkPlanner.WeightColumn, CdcStamp.WeightColumn);
    }

    [Fact]
    public void ColumnNamesMatchDebeziumEnvelopeVocabulary()
    {
        Assert.Equal("_op", CdcStamp.OpColumn);
        Assert.Equal("_weight", CdcStamp.WeightColumn);
        Assert.Equal("_ts", CdcStamp.TsColumn);
        Assert.Equal("_table", CdcStamp.TableColumn);
    }

    [Fact]
    public void UnavailableValueIsDebeziumsOwnLiteral()
    {
        // Deliberately Debezium's own sentinel, not a StreamForge invention — an operator's SQL
        // written against the Debezium path must keep working against the native one.
        Assert.Equal("__debezium_unavailable_value", CdcStamp.UnavailableValue);
    }

    [Fact]
    public void OpLetterConstantsMatchCdcEnvelope()
    {
        Assert.Equal("c", CdcStamp.OpCreate);
        Assert.Equal("u", CdcStamp.OpUpdate);
        Assert.Equal("d", CdcStamp.OpDelete);
    }

    [Fact]
    public void ApplyStampsCreateWithPositiveWeight()
    {
        var row = new Dictionary<string, object?> { ["id"] = 1 };
        CdcStamp.Apply(row, CdcStamp.OpCreate, "public.trades", 1_700_000_000_000L);

        Assert.Equal("c", row[CdcStamp.OpColumn]);
        Assert.Equal(1, row[CdcStamp.WeightColumn]);
        Assert.Equal("public.trades", row[CdcStamp.TableColumn]);
        Assert.Equal(1_700_000_000_000L, row[CdcStamp.TsColumn]);
    }

    [Fact]
    public void ApplyStampsUpdateWithPositiveWeight()
    {
        var row = new Dictionary<string, object?>();
        CdcStamp.Apply(row, CdcStamp.OpUpdate, "public.trades", 1_700_000_000_000L);

        Assert.Equal("u", row[CdcStamp.OpColumn]);
        Assert.Equal(1, row[CdcStamp.WeightColumn]);
    }

    [Fact]
    public void ApplyStampsDeleteWithNegativeWeight()
    {
        var row = new Dictionary<string, object?>();
        CdcStamp.Apply(row, CdcStamp.OpDelete, "public.trades", 1_700_000_000_000L);

        Assert.Equal("d", row[CdcStamp.OpColumn]);
        Assert.Equal(-1, row[CdcStamp.WeightColumn]);
    }

    [Fact]
    public void ApplyOmitsTableColumnWhenTableIsNullOrEmpty()
    {
        var rowWithNull = new Dictionary<string, object?>();
        CdcStamp.Apply(rowWithNull, CdcStamp.OpCreate, null, null);
        Assert.False(rowWithNull.ContainsKey(CdcStamp.TableColumn));

        var rowWithEmpty = new Dictionary<string, object?>();
        CdcStamp.Apply(rowWithEmpty, CdcStamp.OpCreate, "", null);
        Assert.False(rowWithEmpty.ContainsKey(CdcStamp.TableColumn));
    }

    [Fact]
    public void ApplyOmitsTsColumnWhenTsMsIsNull()
    {
        var row = new Dictionary<string, object?>();
        CdcStamp.Apply(row, CdcStamp.OpCreate, "public.trades", null);

        Assert.False(row.ContainsKey(CdcStamp.TsColumn));
        Assert.Equal("public.trades", row[CdcStamp.TableColumn]);
    }

    [Fact]
    public void ApplyIncludesTableAndTsWhenBothProvided()
    {
        var row = new Dictionary<string, object?>();
        CdcStamp.Apply(row, CdcStamp.OpCreate, "public.trades", 42L);

        Assert.True(row.ContainsKey(CdcStamp.TableColumn));
        Assert.True(row.ContainsKey(CdcStamp.TsColumn));
    }

    [Theory]
    [InlineData("c", 1)]
    [InlineData("u", 1)]
    [InlineData("d", -1)]
    [InlineData("r", 1)]
    [InlineData("", 1)]
    [InlineData("some-future-op", 1)]
    public void WeightOfMatchesCdcEnvelopesSignRuleForKnownAndUnknownOps(string op, int expectedWeight)
    {
        Assert.Equal(expectedWeight, CdcStamp.WeightOf(op));
    }

    [Fact]
    public void AnUnknownOpLetterDoesNotThrowAndStampsPositiveWeight()
    {
        var row = new Dictionary<string, object?>();
        var exception = Record.Exception(() => CdcStamp.Apply(row, "x", "public.trades", null));

        Assert.Null(exception);
        Assert.Equal("x", row[CdcStamp.OpColumn]);
        Assert.Equal(1, row[CdcStamp.WeightColumn]);
    }

    [Fact]
    public void ApplyThrowsOnNullRow()
    {
        Assert.Throws<ArgumentNullException>(() => CdcStamp.Apply(null!, CdcStamp.OpCreate, null, null));
    }
}
