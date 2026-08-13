using StreamForge.Abstractions;
using StreamForge.Abstractions.Streaming;
using StreamForge.Dapr.Host.Actors;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 011 wave C2, Dapr flavor. Two independent things live here because both are pure functions of
/// <see cref="TableHistoryApplication"/> and neither needs actor/timer/sidecar machinery:
///
///  1. ROW RETENTION's effect on history — a delta carrying <see cref="TableDeltaDto.Evicted"/> reclaims
///     the key's whole version trail instead of bumping its retraction counter. Without that, a bounded
///     table would still accumulate one history entry per key it had ever seen, i.e. the bound would bound
///     the visible row count and none of the memory.
///  2. The <see cref="TablePersistAction.JournalWrite"/> defect wave C found and wrote down: history for a
///     table configured Journaled was never persisted at all on this flavor. See
///     <see cref="TableHistoryApplication.DecideHistoryFlushAction"/>'s doc comment.
/// </summary>
public class TableHistoryRetentionTests
{
    private static TableDefinition LatestByTable(TablePersistenceMode persistence = TablePersistenceMode.Batched) => new()
    {
        Name = "order_states",
        Sql = "SELECT order_id, stage FROM order_events LATEST BY (order_id)",
        HistoryEnabled = true,
        HistoryMode = TableHistoryMode.All,
        Persistence = persistence,
    };

    private static TableDeltaEnvelope Envelope(params TableDeltaDto[] deltas) =>
        new() { Table = "order_states", Deltas = [.. deltas] };

    private static TableDeltaDto Delta(string orderId, string stage, long weight, bool evicted = false) => new()
    {
        Row = new Dictionary<string, object?> { ["order_id"] = orderId, ["stage"] = stage },
        Weight = weight,
        Evicted = evicted,
    };

    // ------------------------------------------------------------------
    // 1. Retention eviction reclaims the key's history.
    // ------------------------------------------------------------------

    [Fact]
    public void EvictionRemovesTheKeysEntryEntirely()
    {
        var state = TableHistoryApplication.Reset(LatestByTable());

        TableHistoryApplication.ApplyDeltas(state, Envelope(Delta("o1", "NEW", 1)));
        Assert.Single(state.Entries);

        TableHistoryApplication.ApplyDeltas(state, Envelope(Delta("o1", "NEW", -1, evicted: true)));

        Assert.Empty(state.Entries);
        Assert.Equal(0, TableHistoryApplication.Stats(state).KeyCount);
    }

    [Fact]
    public void AnOrdinaryRetractionStillKeepsTheTrailAndCountsIt()
    {
        // The distinction is the whole point: an upstream retraction says "not true right now" (the key may
        // assert again, so the trail stays); an eviction says "this bounded table has stopped carrying it".
        var state = TableHistoryApplication.Reset(LatestByTable());

        TableHistoryApplication.ApplyDeltas(state, Envelope(Delta("o1", "NEW", 1)));
        TableHistoryApplication.ApplyDeltas(state, Envelope(Delta("o1", "NEW", -1)));

        var entry = Assert.Single(state.Entries).Value;
        Assert.Single(entry.Versions);
        Assert.Equal(1, entry.RetractionCount);
    }

    [Fact]
    public void EvictingAKeyWithNoHistoryIsHarmless()
    {
        var state = TableHistoryApplication.Reset(LatestByTable());

        TableHistoryApplication.ApplyDeltas(state, Envelope(Delta("ghost", "NEW", -1, evicted: true)));

        Assert.Empty(state.Entries);
    }

    [Fact]
    public void AKeyEvictedThenSeenAgainStartsAFreshTrail()
    {
        var state = TableHistoryApplication.Reset(LatestByTable());

        TableHistoryApplication.ApplyDeltas(state, Envelope(Delta("o1", "NEW", 1), Delta("o1", "ACK", 1)));
        Assert.Equal(2, Assert.Single(state.Entries).Value.Versions.Count);

        TableHistoryApplication.ApplyDeltas(state, Envelope(Delta("o1", "ACK", -1, evicted: true)));
        TableHistoryApplication.ApplyDeltas(state, Envelope(Delta("o1", "ACK", 1)));

        var entry = Assert.Single(state.Entries).Value;
        Assert.Single(entry.Versions); // a fresh trail, not a resurrected one
    }

    [Fact]
    public void AMixedBatchEvictsSomeKeysAndAppendsOthers()
    {
        var state = TableHistoryApplication.Reset(LatestByTable());
        TableHistoryApplication.ApplyDeltas(state, Envelope(Delta("a", "NEW", 1), Delta("b", "NEW", 1)));

        // Exactly the shape the Engine emits when a bound trips: this call's own assertion, then the
        // eviction retraction, in one batch.
        TableHistoryApplication.ApplyDeltas(state, Envelope(Delta("c", "NEW", 1), Delta("a", "NEW", -1, evicted: true)));

        Assert.Equal(2, state.Entries.Count);
        Assert.Equal(["b", "c"], state.Entries.Values
            .Select(e => (string)e.Versions[0].Row["order_id"]!)
            .OrderBy(x => x, StringComparer.Ordinal));
    }

    // ------------------------------------------------------------------
    // 2. The Journaled-history defect (separate from retention; see the class doc).
    // ------------------------------------------------------------------

    [Fact]
    public void JournaledHistoryIsPersistedAsAFullAwaitedWrite_NotSkipped()
    {
        // Before the fix this returned JournalWrite, which the actor's switch did not handle, so it fell
        // through to Skip and the history was NEVER written for a Journaled table.
        Assert.Equal(
            TablePersistAction.AwaitedWrite,
            TableHistoryApplication.DecideHistoryFlushAction(TablePersistenceMode.Journaled, dirty: true, writeInProgress: false));
    }

    [Fact]
    public void JournaledHistoryMatchesBatchedExactly()
    {
        // The claim the fix rests on: Journaled history now behaves as Batched history, which is what the
        // Orleans flavor has always done.
        foreach (var (dirty, inFlight) in new[] { (true, false), (true, true), (false, false) })
        {
            Assert.Equal(
                TableHistoryApplication.DecideHistoryFlushAction(TablePersistenceMode.Batched, dirty, inFlight),
                TableHistoryApplication.DecideHistoryFlushAction(TablePersistenceMode.Journaled, dirty, inFlight));
        }
    }

    [Theory]
    [InlineData(TablePersistenceMode.Batched, true, false, TablePersistAction.AwaitedWrite)]
    [InlineData(TablePersistenceMode.MemoryOnly, true, false, TablePersistAction.Skip)]
    [InlineData(TablePersistenceMode.FireAndForget, true, false, TablePersistAction.BackgroundWrite)]
    [InlineData(TablePersistenceMode.FireAndForget, true, true, TablePersistAction.Skip)]
    [InlineData(TablePersistenceMode.Journaled, false, false, TablePersistAction.Skip)]
    public void EveryOtherModeIsUnchangedByTheFix(TablePersistenceMode mode, bool dirty, bool writeInProgress, TablePersistAction expected)
    {
        Assert.Equal(expected, TableHistoryApplication.DecideHistoryFlushAction(mode, dirty, writeInProgress));
    }
}
