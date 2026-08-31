using StreamsForge.Engine.Dataflow;
using Xunit;
using static StreamsForge.Engine.Tests.TestHelpers;

namespace StreamsForge.Engine.Tests;

/// <summary>
/// Plan 003 M4 acceptance: <see cref="TableStageKindLabel"/> is exhaustive over every
/// <see cref="TableStageKind"/> value (guards the M4 task's "stage-kind mapping covers every stage type
/// the builder emits" requirement independent of whether a given demo plan currently exercises that kind),
/// AND every stage kind the builder actually emits across a representative set of plan shapes (the same six
/// shapes PartitionedDataflowTests.cs uses for its M2 equivalence oracle: aggregate-only, join+aggregate,
/// unnest+aggregate, latest-by, semi-join, scalar broadcast) round-trips through the mapping without
/// throwing and with a sane, non-empty label.
/// </summary>
public class TableStageKindLabelTests
{
    private static readonly TableStageKind[] AllKinds = Enum.GetValues<TableStageKind>();

    /// <summary>Every enum value maps to a distinct, non-empty label — this is the exhaustiveness guarantee
    /// TableStageKindLabel's own doc comment promises (a future TableStageKind addition that forgets to
    /// extend the mapping fails HERE, not silently in production).</summary>
    [Theory]
    [MemberData(nameof(AllKindsData))]
    public void Of_NeverThrows_And_ReturnsNonEmptyLabel(TableStageKind kind)
    {
        var label = TableStageKindLabel.Of(kind);
        Assert.False(string.IsNullOrWhiteSpace(label));
    }

    [Fact]
    public void Of_LabelsAreDistinctAcrossAllKinds()
    {
        var labels = AllKinds.Select(TableStageKindLabel.Of).ToList();
        Assert.Equal(labels.Distinct(StringComparer.Ordinal).Count(), labels.Count);
    }

    [Fact]
    public void Of_MatchesExpectedWording()
    {
        Assert.Equal("Ingest", TableStageKindLabel.Of(TableStageKind.Ingest));
        Assert.Equal("Join", TableStageKindLabel.Of(TableStageKind.Join));
        Assert.Equal("SemiAnti", TableStageKindLabel.Of(TableStageKind.SemiAnti));
        Assert.Equal("Unnest", TableStageKindLabel.Of(TableStageKind.Unnest));
        Assert.Equal("FilterProject", TableStageKindLabel.Of(TableStageKind.FilterProject));
        Assert.Equal("Reduce", TableStageKindLabel.Of(TableStageKind.Reduce));
        Assert.Equal("LatestBy", TableStageKindLabel.Of(TableStageKind.LatestBy));
    }

    public static IEnumerable<object[]> AllKindsData() => AllKinds.Select(k => new object[] { k });

    // ------------------------------------------------------------------------------------------------
    // Representative plan shapes (mirrors PartitionedDataflowTests.cs's six shapes) — proves every kind
    // the builder can actually emit round-trips through the label mapping without throwing, and that the
    // UNION of kinds these six shapes emit covers the full TableStageKind enum (i.e. these six shapes are
    // still representative enough to exercise every operator — if a future engine change adds an eighth
    // TableStageKind that none of these shapes exercise, this test's final assertion catches the gap).
    // ------------------------------------------------------------------------------------------------

    private const string AggSql = "SELECT symbol, SUM(qty) AS total FROM trades GROUP BY symbol";
    private const string JoinAggSql = "SELECT t.symbol, SUM(t.qty) AS total FROM trades t JOIN ref r ON t.symbol = r.symbol GROUP BY t.symbol";
    private const string UnnestAggSql = "SELECT s.trade_id, COUNT(*) AS legcount FROM structures s CROSS JOIN UNNEST(s.legs) AS l GROUP BY s.trade_id";
    private const string LatestBySql = "SELECT order_id, stage, filled_qty FROM order_events LATEST BY (order_id)";
    private const string SemiSql = "SELECT symbol, price FROM trades WHERE symbol IN (SELECT symbol FROM ref)";
    private const string ScalarSql = "SELECT symbol, price - (SELECT AVG(price) FROM trades) AS rel FROM trades";

    private static IReadOnlyList<TableStageKind> StageKindsOf(StreamsForge.Engine.TableCompileResult compileResult)
    {
        Assert.True(compileResult.Ok, string.Join("; ", compileResult.Diagnostics.Select(d => d.Message)));
        var dataflow = compileResult.Plan!.CreateDataflow(4);
        return dataflow.Stages.Select(s => s.Kind).ToList();
    }

    [Fact]
    public void EveryStageKindTheBuilderEmitsAcrossRepresentativePlans_HasALabel_AndUnionCoversTheWholeEnum()
    {
        var shapes = new (string Sql, StreamsForge.Engine.TableCompileResult Compile)[]
        {
            (AggSql, CompileTable(AggSql, Trades)),
            (JoinAggSql, CompileTable(JoinAggSql, [Trades], [Ref])),
            (UnnestAggSql, CompileTable(UnnestAggSql, Structures)),
            (LatestBySql, CompileTable(LatestBySql, OrderEvents)),
            (SemiSql, CompileTable(SemiSql, [Trades], [Ref])),
            (ScalarSql, CompileTable(ScalarSql, Trades)),
        };

        var union = new HashSet<TableStageKind>();
        foreach (var (sql, compile) in shapes)
        {
            var kinds = StageKindsOf(compile);
            Assert.NotEmpty(kinds); // every plan shape must have produced SOME stages
            foreach (var kind in kinds)
            {
                var label = TableStageKindLabel.Of(kind); // must not throw for anything the builder actually emits
                Assert.False(string.IsNullOrWhiteSpace(label), $"{sql}: {kind} mapped to a blank label");
                union.Add(kind);
            }
        }

        // The six representative shapes, together, exercise every TableStageKind — if this fails, either a
        // shape needs to change or a new representative shape needs to be added alongside it.
        Assert.Equal(AllKinds.OrderBy(k => k).ToList(), union.OrderBy(k => k).ToList());
    }
}
