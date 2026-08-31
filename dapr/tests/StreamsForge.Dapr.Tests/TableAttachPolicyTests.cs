using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Actors;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// PARITY.md debt item D2 (table-over-table warm attach): unit tests for <see cref="TableAttachPolicy"/> —
/// the pure epoch-cutoff decision <see cref="Actors.TableActor.ProcessTableDeltasAsync"/> applies to every
/// upstream table-delta batch, extracted specifically so it is testable without a live Dapr sidecar (a
/// <see cref="Actors.TableActor"/> instance needs one to construct at all — mirrors
/// <c>TablePersistencePolicyTests</c>/<c>TableJournalPolicyTests</c>'s own extraction rationale). This is
/// the mechanism that keeps a table-over-table backfill from being applied twice: a delta whose own
/// <see cref="TableDeltaDto.Epoch"/> is at or below the cutoff <see cref="Actors.TableActor.
/// RegisterRouterAndAttachToTableInputsAsync"/> recorded from the upstream's <see cref="ITableActor.
/// AttachSnapshotAsync"/> response is already reflected in the snapshot this table backfilled from.
/// </summary>
public class TableAttachPolicyTests
{
    private static TableDeltaDto Delta(long epoch, long weight = 1) =>
        new() { Row = new Dictionary<string, object?> { ["x"] = 1 }, Weight = weight, Epoch = epoch };

    [Fact]
    public void FilterAdmissible_NegativeCutoff_AdmitsEverythingUnconditionally()
    {
        // -1 means "this table never recorded a cutoff for this upstream" (no snapshot existed to
        // backfill from, or this upstream isn't a declared table input at all) — nothing to double-count
        // against, so every delta is admitted regardless of its own Epoch, including one that is itself -1
        // (a producer that predates wishlist #14's Epoch field, or simply never admitted anything yet).
        var batch = new List<TableDeltaDto> { Delta(-1), Delta(0), Delta(5) };

        var admissible = TableAttachPolicy.FilterAdmissible(batch, cutoff: -1);

        Assert.Equal(3, admissible.Count);
    }

    [Fact]
    public void FilterAdmissible_EpochAboveCutoff_IsAdmitted()
    {
        var batch = new List<TableDeltaDto> { Delta(6), Delta(7) };

        var admissible = TableAttachPolicy.FilterAdmissible(batch, cutoff: 5);

        Assert.Equal(2, admissible.Count);
    }

    [Fact]
    public void FilterAdmissible_EpochAtCutoff_IsDropped_NotJustBelowIt()
    {
        // The comparison is strictly ">", not ">=" — a delta admitted by the upstream at EXACTLY the cutoff
        // epoch is the last one folded into the snapshot AttachSnapshotAsync returned, so re-admitting it
        // here would double its Z-set weight. This is the single line where an off-by-one would silently
        // reintroduce the double-count PARITY.md D2 exists to prevent.
        var batch = new List<TableDeltaDto> { Delta(5) };

        var admissible = TableAttachPolicy.FilterAdmissible(batch, cutoff: 5);

        Assert.Empty(admissible);
    }

    [Fact]
    public void FilterAdmissible_EpochBelowCutoff_IsDropped()
    {
        var batch = new List<TableDeltaDto> { Delta(1), Delta(4) };

        var admissible = TableAttachPolicy.FilterAdmissible(batch, cutoff: 5);

        Assert.Empty(admissible);
    }

    [Fact]
    public void FilterAdmissible_MixedBatch_KeepsOnlyElementsAboveCutoff()
    {
        // One published batch is one atomic upstream admission (wishlist #15) and therefore normally
        // shares a single Epoch across every element — but the filter itself must not assume that; it
        // makes an independent per-element decision, exactly like TableGrain.OnTableDeltaBatchAsync's own
        // Where(d => d.Epoch > cutoff) does on the Orleans side.
        var batch = new List<TableDeltaDto> { Delta(3), Delta(5), Delta(6), Delta(10) };

        var admissible = TableAttachPolicy.FilterAdmissible(batch, cutoff: 5);

        Assert.Equal(2, admissible.Count);
        Assert.All(admissible, d => Assert.True(d.Epoch > 5));
    }

    [Fact]
    public void FilterAdmissible_EmptyBatch_ReturnsEmpty()
    {
        Assert.Empty(TableAttachPolicy.FilterAdmissible([], cutoff: -1));
        Assert.Empty(TableAttachPolicy.FilterAdmissible([], cutoff: 5));
    }
}
