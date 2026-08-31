using StreamsForge.Abstractions;
using StreamsForge.Abstractions.Streaming;
using StreamsForge.Dapr.Host.Actors;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-B: unit tests for <see cref="TableHistoryApplication.EnsureConfigured"/>/
/// <see cref="TableHistoryApplication.ConfigMatches"/> — the fix for a live-verification gap (see
/// <see cref="Services.TableHistorySupervisorService"/>'s own doc comment for the full writeup): a seeded
/// table with <c>HistoryEnabled=true</c> bypassed <see cref="Lifecycle.ILifecycleOrchestrator.ResetTableHistoryAsync"/>
/// entirely (seeding never goes through <c>Catalog/CatalogStore.cs</c>'s <c>CreateTableAsync</c>), and every
/// host restart wiped the in-memory <see cref="Streaming.TableHistoryEnabledMap"/>. The sweep service fixes
/// both by calling <see cref="ITableHistoryActor.EnsureConfiguredAsync"/> periodically for every
/// history-enabled table — these tests prove the pure decision logic behind that call is correct: configure
/// when unconfigured or changed, but NEVER clear already-accumulated history when nothing changed (the
/// restart-safety property the whole fix exists for).
/// </summary>
public class TableHistoryEnsureConfiguredTests
{
    private static TableDefinition PositionsTable(TableHistoryMode mode = TableHistoryMode.LastN, int limit = 8) => new()
    {
        Name = "positions",
        Sql = "SELECT symbol, SUM(qty) AS total_qty FROM trades GROUP BY symbol",
        HistoryEnabled = true,
        HistoryMode = mode,
        HistoryLimit = limit,
    };

    private static TableHistoryActorState WithOneVersion(TableHistoryActorState state)
    {
        TableHistoryApplication.ApplyDeltas(state, new TableDeltaEnvelope
        {
            Table = "positions",
            Deltas = [new TableDeltaDto { Row = new Dictionary<string, object?> { ["symbol"] = "AAPL", ["total_qty"] = 100L }, Weight = 1 }],
        });
        return state;
    }

    // ------------------------------------------------------------------
    // Seed-path configuration: a NEVER-configured (fresh, default) actor state meeting a
    // history-enabled table for the first time — exactly the SEED path
    // (CatalogStore.EnsureInitialized never calls ResetTableHistoryAsync, so a seeded table's actor is
    // still the brand-new `new TableHistoryActorState()` default the first time the sweep sees it).
    // ------------------------------------------------------------------

    [Fact]
    public void EnsureConfigured_FreshUnconfiguredState_HistoryEnabledDef_ConfiguresIt()
    {
        var fresh = new TableHistoryActorState(); // exactly what TableHistoryActor starts with, never Reset
        var def = PositionsTable();

        var result = TableHistoryApplication.EnsureConfigured(fresh, def);

        Assert.True(result.Enabled());
        Assert.Equal(TableHistoryMode.LastN, result.HistoryMode);
        Assert.Equal(8, result.HistoryLimit);
        Assert.Equal(["symbol"], result.IdentityColumns);
        Assert.Empty(result.Entries);
    }

    [Fact]
    public void EnsureConfigured_FreshUnconfiguredState_HistoryDisabledDef_StaysNoOp()
    {
        var fresh = new TableHistoryActorState();
        var def = new TableDefinition { Name = "raw", Sql = "SELECT 1", HistoryEnabled = false };

        var result = TableHistoryApplication.EnsureConfigured(fresh, def);

        // Nothing to configure — a table that has never had history and still doesn't need any actor
        // state churn (mirrors this actor's own "cheap no-op when disabled" contract).
        Assert.Same(fresh, result);
    }

    // ------------------------------------------------------------------
    // Restart preserves history: EnsureConfigured with IDENTICAL config is a true no-op — the actor's
    // own accumulated Entries (which OnActivateAsync would have just reloaded from persisted Dapr state
    // after a real restart) must survive being re-swept.
    // ------------------------------------------------------------------

    [Fact]
    public void EnsureConfigured_IdenticalConfig_ReturnsSameInstanceAndPreservesAccumulatedEntries()
    {
        var def = PositionsTable();
        var configured = WithOneVersion(TableHistoryApplication.Reset(def));
        Assert.NotEmpty(configured.Entries); // sanity: there IS accumulated history to lose if this regresses

        var result = TableHistoryApplication.EnsureConfigured(configured, def);

        Assert.Same(configured, result); // same reference — proves no Reset (and no clear) happened
        Assert.NotEmpty(result.Entries);
        Assert.Equal(1, result.Seq);
    }

    [Fact]
    public void ConfigMatches_IdenticalConfig_ReturnsTrue()
    {
        var def = PositionsTable();
        var state = TableHistoryApplication.Reset(def);

        Assert.True(TableHistoryApplication.ConfigMatches(state, def));
    }

    [Fact]
    public void ConfigMatches_BothDisabled_ReturnsTrueRegardlessOfStaleModeOrLimit()
    {
        // A disabled actor's leftover Mode/Limit fields (e.g. from before it was DisableAsync'd) must
        // never matter — disabled is disabled.
        var state = new TableHistoryActorState { HistoryEnabled = false, HistoryMode = TableHistoryMode.MaxBy, HistoryLimit = 999 };
        var def = new TableDefinition { Name = "t", Sql = "SELECT 1", HistoryEnabled = false };

        Assert.True(TableHistoryApplication.ConfigMatches(state, def));
    }

    // ------------------------------------------------------------------
    // Config change clears: a genuine mismatch (mode/limit/SQL/enabled-flag change) makes
    // EnsureConfigured behave exactly like ResetAsync — clearing accumulated history, same as a real
    // config-change call through DaprLifecycleOrchestrator.ResetTableHistoryAsync would.
    // ------------------------------------------------------------------

    [Fact]
    public void EnsureConfigured_HistoryLimitChanged_ResetsAndClearsAccumulatedEntries()
    {
        var original = PositionsTable(limit: 8);
        var configured = WithOneVersion(TableHistoryApplication.Reset(original));

        var changed = PositionsTable(limit: 20); // same table, HistoryLimit changed
        var result = TableHistoryApplication.EnsureConfigured(configured, changed);

        Assert.NotSame(configured, result);
        Assert.Empty(result.Entries); // cleared, exactly like ResetAsync
        Assert.Equal(20, result.HistoryLimit);
    }

    [Fact]
    public void EnsureConfigured_HistoryModeChanged_Resets()
    {
        var configured = WithOneVersion(TableHistoryApplication.Reset(PositionsTable(mode: TableHistoryMode.LastN)));

        var result = TableHistoryApplication.EnsureConfigured(configured, PositionsTable(mode: TableHistoryMode.All));

        Assert.NotSame(configured, result);
        Assert.Empty(result.Entries);
        Assert.Equal(TableHistoryMode.All, result.HistoryMode);
    }

    [Fact]
    public void EnsureConfigured_SqlChangedIdentityColumns_Resets()
    {
        var def = PositionsTable();
        var configured = WithOneVersion(TableHistoryApplication.Reset(def));

        var reSqled = PositionsTable();
        reSqled.Sql = "SELECT symbol, venue, SUM(qty) AS total_qty FROM trades GROUP BY symbol, venue";

        var result = TableHistoryApplication.EnsureConfigured(configured, reSqled);

        Assert.NotSame(configured, result);
        Assert.Empty(result.Entries);
        Assert.Equal(["symbol", "venue"], result.IdentityColumns);
    }

    [Fact]
    public void EnsureConfigured_HistoryEnabledFlagFlippedOff_Resets()
    {
        var def = PositionsTable();
        var configured = WithOneVersion(TableHistoryApplication.Reset(def));

        var disabled = PositionsTable();
        disabled.HistoryEnabled = false;
        var result = TableHistoryApplication.EnsureConfigured(configured, disabled);

        Assert.NotSame(configured, result);
        Assert.False(result.Enabled());
        Assert.Empty(result.Entries);
    }

    [Fact]
    public void ConfigMatches_HistoryByFieldChanged_ReturnsFalse()
    {
        var def = new TableDefinition
        {
            Name = "positions", Sql = "SELECT symbol, MAX(qty) AS max_qty FROM trades GROUP BY symbol",
            HistoryEnabled = true, HistoryMode = TableHistoryMode.MaxBy, HistoryByField = "max_qty",
        };
        var state = TableHistoryApplication.Reset(def);

        var changed = new TableDefinition
        {
            Name = "positions", Sql = def.Sql,
            HistoryEnabled = true, HistoryMode = TableHistoryMode.MaxBy, HistoryByField = "other_field",
        };

        Assert.False(TableHistoryApplication.ConfigMatches(state, changed));
    }

    [Fact]
    public void ConfigMatches_HistoryWindowMsChanged_ReturnsFalse()
    {
        var def = PositionsTable();
        def.HistoryWindowMs = 60_000;
        var state = TableHistoryApplication.Reset(def);

        var changed = PositionsTable();
        changed.HistoryWindowMs = 120_000;

        Assert.False(TableHistoryApplication.ConfigMatches(state, changed));
    }
}

/// <summary>Tiny readability helper — <c>TableHistoryActorState</c> doesn't need an "Enabled" property of
/// its own (its field is literally named <c>HistoryEnabled</c>), this just avoids repeating that name
/// awkwardly in assertions above.</summary>
file static class TableHistoryActorStateExtensions
{
    public static bool Enabled(this TableHistoryActorState state) => state.HistoryEnabled;
}
