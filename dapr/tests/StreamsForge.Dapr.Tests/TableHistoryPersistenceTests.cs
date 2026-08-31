using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.Dapr.Host.Actors;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 008 (per-table durability policy): unit tests for the <see cref="TableHistoryActor"/>-specific
/// half of the persistence-mode work — <see cref="TableHistoryApplication.Reset"/> capturing
/// <see cref="TableDefinition.Persistence"/>/<see cref="TableDefinition.FlushMs"/>,
/// <see cref="TableHistoryApplication.PersistenceMatches"/> (deliberately separate from
/// <see cref="TableHistoryApplication.ConfigMatches"/> — a pure persistence-mode change must never wipe
/// accumulated <c>Entries</c>), and <see cref="TableHistoryApplication.CloneForBackgroundPersist"/>'s deep
/// clone (required because <see cref="RowHistoryEntry.Versions"/> lists are mutated IN PLACE by
/// <see cref="TableHistoryApplication.ApplyDeltas"/> — see <see cref="TableHistoryActor"/>'s class doc for
/// the full "why a bare reference copy is not safe here" rationale). Pure logic only, no actor/timer/
/// Dapr-sidecar machinery, following the same convention as <c>TableHistoryEnsureConfiguredTests.cs</c>.
/// </summary>
public class TableHistoryPersistenceTests
{
    private static TableDefinition PositionsTable(TablePersistenceMode persistence = TablePersistenceMode.Batched, int flushMs = 0) => new()
    {
        Name = "positions",
        Sql = "SELECT symbol, SUM(qty) AS total_qty FROM trades GROUP BY symbol",
        HistoryEnabled = true,
        HistoryMode = TableHistoryMode.LastN,
        HistoryLimit = 8,
        Persistence = persistence,
        FlushMs = flushMs,
    };

    private static TableHistoryActorState WithOneVersion(TableHistoryActorState state, string symbol = "AAPL", long qty = 100)
    {
        TableHistoryApplication.ApplyDeltas(state, new TableDeltaEnvelope
        {
            Table = "positions",
            Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = symbol, ["total_qty"] = qty }, Weight = 1 }],
        });
        return state;
    }

    // ------------------------------------------------------------------
    // Reset captures Persistence/FlushMs.
    // ------------------------------------------------------------------

    [Fact]
    public void Reset_CapturesPersistenceAndFlushMsFromDefinition()
    {
        var def = PositionsTable(persistence: TablePersistenceMode.FireAndForget, flushMs: 500);

        var state = TableHistoryApplication.Reset(def);

        Assert.Equal(TablePersistenceMode.FireAndForget, state.Persistence);
        Assert.Equal(500, state.FlushMs);
    }

    [Fact]
    public void Reset_DefaultDefinition_PersistenceDefaultsToBatched()
    {
        var state = TableHistoryApplication.Reset(PositionsTable());

        Assert.Equal(TablePersistenceMode.Batched, state.Persistence);
        Assert.Equal(0, state.FlushMs);
    }

    // ------------------------------------------------------------------
    // PersistenceMatches — separate from ConfigMatches, so a persistence-only change is detectable
    // without a content-config mismatch, and vice versa.
    // ------------------------------------------------------------------

    [Fact]
    public void PersistenceMatches_IdenticalPersistenceAndFlushMs_ReturnsTrue()
    {
        var def = PositionsTable(persistence: TablePersistenceMode.MemoryOnly, flushMs: 1000);
        var state = TableHistoryApplication.Reset(def);

        Assert.True(TableHistoryApplication.PersistenceMatches(state, def));
    }

    [Fact]
    public void PersistenceMatches_PersistenceModeChanged_ReturnsFalse()
    {
        var state = TableHistoryApplication.Reset(PositionsTable(persistence: TablePersistenceMode.Batched));
        var changed = PositionsTable(persistence: TablePersistenceMode.FireAndForget);

        Assert.False(TableHistoryApplication.PersistenceMatches(state, changed));
    }

    [Fact]
    public void PersistenceMatches_FlushMsChanged_ReturnsFalse()
    {
        var state = TableHistoryApplication.Reset(PositionsTable(flushMs: 1000));
        var changed = PositionsTable(flushMs: 5000);

        Assert.False(TableHistoryApplication.PersistenceMatches(state, changed));
    }

    [Fact]
    public void PersistenceMatches_ChangedButConfigMatches_LetsCallerUpdateWithoutFullReset()
    {
        // The exact scenario ITableHistoryActor.EnsureConfiguredAsync's plan-008 addendum exists for:
        // history-content config (mode/limit/identity columns) is unchanged, only persistence drifted —
        // ConfigMatches says "no content reset needed" while PersistenceMatches says "but this did change".
        var def = PositionsTable(persistence: TablePersistenceMode.Batched);
        var state = TableHistoryApplication.Reset(def);

        var changed = PositionsTable(persistence: TablePersistenceMode.FireAndForget);

        Assert.True(TableHistoryApplication.ConfigMatches(state, changed));
        Assert.False(TableHistoryApplication.PersistenceMatches(state, changed));
    }

    // ------------------------------------------------------------------
    // CloneForBackgroundPersist — deep clone: mutating the ORIGINAL after cloning must never be
    // observable through the clone (this is what makes a FireAndForget background write safe while later
    // turns keep landing).
    // ------------------------------------------------------------------

    [Fact]
    public void CloneForBackgroundPersist_CopiesScalarAndListFields()
    {
        var def = PositionsTable(persistence: TablePersistenceMode.FireAndForget, flushMs: 750);
        var state = WithOneVersion(TableHistoryApplication.Reset(def));

        var clone = TableHistoryApplication.CloneForBackgroundPersist(state);

        Assert.Equal(state.HistoryEnabled, clone.HistoryEnabled);
        Assert.Equal(state.HistoryMode, clone.HistoryMode);
        Assert.Equal(state.HistoryLimit, clone.HistoryLimit);
        Assert.Equal(state.Seq, clone.Seq);
        Assert.Equal(state.Persistence, clone.Persistence);
        Assert.Equal(state.FlushMs, clone.FlushMs);
        Assert.Equal(state.IdentityColumns, clone.IdentityColumns);
        Assert.Single(clone.Entries);
    }

    [Fact]
    public void CloneForBackgroundPersist_MutatingOriginalEntriesDictionaryAfterClone_DoesNotAffectClone()
    {
        var state = WithOneVersion(TableHistoryApplication.Reset(PositionsTable()));
        var clone = TableHistoryApplication.CloneForBackgroundPersist(state);
        Assert.Single(clone.Entries);

        // Simulate a later turn's ApplyDeltas landing a brand-new row-identity AFTER the clone was taken —
        // a bare `state.Entries` reference copy would leak this into the "captured" snapshot.
        WithOneVersion(state, symbol: "MSFT");

        Assert.Single(clone.Entries); // unaffected — still only the one entry captured at clone time
        Assert.Equal(2, state.Entries.Count); // the live state did grow
    }

    [Fact]
    public void CloneForBackgroundPersist_MutatingOriginalExistingEntryVersionsAfterClone_DoesNotAffectClone()
    {
        var state = WithOneVersion(TableHistoryApplication.Reset(PositionsTable()));
        var clone = TableHistoryApplication.CloneForBackgroundPersist(state);
        var clonedEntry = Assert.Single(clone.Entries).Value;
        Assert.Single(clonedEntry.Versions);

        // Simulate a later turn appending ANOTHER version for the SAME row identity — this mutates
        // RowHistoryEntry.Versions IN PLACE (see TableRowHistoryRetention.Append), which is exactly the
        // hazard a shallow Dictionary-only copy would not protect against.
        WithOneVersion(state, symbol: "AAPL", qty: 200);

        Assert.Single(clonedEntry.Versions); // the clone's own entry/list is untouched
        var liveEntry = Assert.Single(state.Entries).Value;
        Assert.Equal(2, liveEntry.Versions.Count); // the live state did grow
    }

    [Fact]
    public void CloneForBackgroundPersist_NullIdentityColumns_StaysNull()
    {
        var state = new TableHistoryActorState { HistoryEnabled = true, IdentityColumns = null };

        var clone = TableHistoryApplication.CloneForBackgroundPersist(state);

        Assert.Null(clone.IdentityColumns);
    }
}
