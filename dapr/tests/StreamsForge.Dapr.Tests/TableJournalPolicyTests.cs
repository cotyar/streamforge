using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Actors;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 009 A2: unit tests for <see cref="TableJournalPolicy"/> — the pure journal-compaction/replay
/// decisions <see cref="Actors.TableActor"/> dispatches on for <see cref="TablePersistenceMode.Journaled"/>,
/// extracted for the same testability reason as <see cref="TablePersistencePolicy"/>
/// (see <c>TablePersistencePolicyTests</c>'s own doc comment — <c>TableActor</c> needs a live
/// <c>ActorHost</c>/<c>DaprClient</c> to construct, so nothing in this project can drive it directly). Also
/// covers the new <see cref="TablePersistenceMode.Journaled"/> branch <see cref="TablePersistencePolicy.
/// DecideFlushAction"/> gained in Plan 009 A2 (additive — every pre-existing branch is unchanged, see
/// <c>TablePersistencePolicyTests</c>, which this file does not modify).
/// </summary>
public class TableJournalPolicyTests
{
    // ------------------------------------------------------------------
    // ResolveJournalMaxEntries — 0 -> default, positive verbatim (mirrors ResolveFlushIntervalMs's shape).
    // ------------------------------------------------------------------

    [Fact]
    public void ResolveJournalMaxEntries_Zero_ResolvesToDefault()
    {
        Assert.Equal(200, TableJournalPolicy.ResolveJournalMaxEntries(0));
        Assert.Equal(200, TableJournalPolicy.DefaultJournalMaxEntries);
    }

    [Theory]
    [InlineData(-5)]
    [InlineData(-1)]
    public void ResolveJournalMaxEntries_Negative_AlsoResolvesToDefault(int configured)
    {
        // Defensive: catalog-level validation (out of this wave's ownership) is expected to reject a
        // negative JournalMaxEntries before it ever reaches here, but the resolver itself must not
        // misbehave (e.g. ShouldCompact(0, negative) would otherwise be true for an empty journal).
        Assert.Equal(200, TableJournalPolicy.ResolveJournalMaxEntries(configured));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(50)]
    [InlineData(10_000)]
    public void ResolveJournalMaxEntries_Positive_UsedVerbatim(int configured)
    {
        Assert.Equal(configured, TableJournalPolicy.ResolveJournalMaxEntries(configured));
    }

    // ------------------------------------------------------------------
    // ShouldCompact — the pure threshold trigger.
    // ------------------------------------------------------------------

    [Theory]
    [InlineData(0, 5, false)]
    [InlineData(4, 5, false)]
    [InlineData(5, 5, true)]
    [InlineData(6, 5, true)]
    public void ShouldCompact_ComparesEntryCountAgainstThreshold(int entryCount, int max, bool expected)
    {
        Assert.Equal(expected, TableJournalPolicy.ShouldCompact(entryCount, max));
    }

    // ------------------------------------------------------------------
    // ReplayOntoSnapshot — the resurrection-bug-prevention logic under direct unit test.
    // ------------------------------------------------------------------

    [Fact]
    public void ReplayOntoSnapshot_EmptyJournal_IsANoOp()
    {
        var snapshot = new Dictionary<string, TableRowDto>
        {
            ["k1"] = new() { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL" }, Weight = 3 },
        };
        var before = new Dictionary<string, TableRowDto>(snapshot);

        TableJournalPolicy.ReplayOntoSnapshot(snapshot, new Dictionary<string, TableJournalEntry>());

        Assert.Equal(before.Keys, snapshot.Keys);
        Assert.Equal(before["k1"].Weight, snapshot["k1"].Weight);
    }

    [Fact]
    public void ReplayOntoSnapshot_PositiveWeightEntry_InsertsNewKey()
    {
        var snapshot = new Dictionary<string, TableRowDto>();
        var journal = new Dictionary<string, TableJournalEntry>
        {
            ["k1"] = new() { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL" }, Weight = 2 },
        };

        TableJournalPolicy.ReplayOntoSnapshot(snapshot, journal);

        Assert.True(snapshot.ContainsKey("k1"));
        Assert.Equal(2, snapshot["k1"].Weight);
        Assert.Equal("AAPL", snapshot["k1"].Row["symbol"]);
    }

    [Fact]
    public void ReplayOntoSnapshot_PositiveWeightEntry_OverwritesExistingKey()
    {
        var snapshot = new Dictionary<string, TableRowDto>
        {
            ["k1"] = new() { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = 1L }, Weight = 1 },
        };
        var journal = new Dictionary<string, TableJournalEntry>
        {
            ["k1"] = new() { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["qty"] = 99L }, Weight = 5 },
        };

        TableJournalPolicy.ReplayOntoSnapshot(snapshot, journal);

        Assert.Equal(5, snapshot["k1"].Weight);
        Assert.Equal(99L, snapshot["k1"].Row["qty"]);
    }

    /// <summary>The resurrection bug, directly: a Weight &lt;= 0 entry is an explicit removal tombstone —
    /// it must REMOVE the key from the snapshot, not merely be ignored (which would leave a stale prior
    /// positive entry, if any, behind).</summary>
    [Fact]
    public void ReplayOntoSnapshot_NonPositiveWeightEntry_RemovesKey_DoesNotResurrectIt()
    {
        var snapshot = new Dictionary<string, TableRowDto>
        {
            ["k1"] = new() { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL" }, Weight = 1 },
        };
        var journal = new Dictionary<string, TableJournalEntry>
        {
            ["k1"] = new() { Row = [], Weight = 0 }, // the tombstone shape TableActor.RecordJournalEntries writes
        };

        TableJournalPolicy.ReplayOntoSnapshot(snapshot, journal);

        Assert.False(snapshot.ContainsKey("k1"));
    }

    [Fact]
    public void ReplayOntoSnapshot_TombstoneForAbsentKey_IsHarmlessNoOp()
    {
        var snapshot = new Dictionary<string, TableRowDto>();
        var journal = new Dictionary<string, TableJournalEntry>
        {
            ["never-existed"] = new() { Row = [], Weight = 0 },
        };

        TableJournalPolicy.ReplayOntoSnapshot(snapshot, journal);

        Assert.Empty(snapshot);
    }

    [Fact]
    public void ReplayOntoSnapshot_MixedJournal_OnlyTouchesJournaledKeys()
    {
        var snapshot = new Dictionary<string, TableRowDto>
        {
            ["untouched"] = new() { Row = new Dictionary<string, object?> { ["symbol"] = "GOOG" }, Weight = 1 },
            ["removed"] = new() { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL" }, Weight = 1 },
        };
        var journal = new Dictionary<string, TableJournalEntry>
        {
            ["removed"] = new() { Row = [], Weight = 0 },
            ["added"] = new() { Row = new Dictionary<string, object?> { ["symbol"] = "MSFT" }, Weight = 1 },
        };

        TableJournalPolicy.ReplayOntoSnapshot(snapshot, journal);

        Assert.Equal(2, snapshot.Count);
        Assert.True(snapshot.ContainsKey("untouched"));
        Assert.True(snapshot.ContainsKey("added"));
        Assert.False(snapshot.ContainsKey("removed"));
    }

    // ------------------------------------------------------------------
    // TablePersistencePolicy.DecideFlushAction's new Journaled branch (additive — see TablePersistAction's
    // new JournalWrite member).
    // ------------------------------------------------------------------

    [Fact]
    public void DecideFlushAction_Journaled_Dirty_ProducesJournalWrite()
    {
        Assert.Equal(
            TablePersistAction.JournalWrite,
            TablePersistencePolicy.DecideFlushAction(TablePersistenceMode.Journaled, dirty: true, writeInProgress: false));
    }

    [Fact]
    public void DecideFlushAction_Journaled_Dirty_IgnoresWriteInProgressFlag()
    {
        // Journaled has no single-flight concept of its own (unlike FireAndForget) — its write is always
        // awaited inside the tick, so a stale writeInProgress=true must not change the outcome.
        Assert.Equal(
            TablePersistAction.JournalWrite,
            TablePersistencePolicy.DecideFlushAction(TablePersistenceMode.Journaled, dirty: true, writeInProgress: true));
    }

    [Fact]
    public void DecideFlushAction_Journaled_NotDirty_Skips()
    {
        Assert.Equal(
            TablePersistAction.Skip,
            TablePersistencePolicy.DecideFlushAction(TablePersistenceMode.Journaled, dirty: false, writeInProgress: false));
    }
}
