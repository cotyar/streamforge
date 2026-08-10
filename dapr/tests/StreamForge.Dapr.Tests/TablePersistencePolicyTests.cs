using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Actors;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 008 (per-table durability policy): unit tests for <see cref="TablePersistencePolicy"/> — the pure
/// per-flush-tick decision <see cref="Actors.TableActor"/>/<see cref="Actors.TableHistoryActor"/> both
/// dispatch on, extracted specifically so the three <see cref="TablePersistenceMode"/> modes' behavior
/// (including single-flight for <see cref="TablePersistenceMode.FireAndForget"/>) is testable without any
/// actor/timer/Dapr-sidecar machinery — mirrors <c>TableCompilationTests</c>/<c>ConnectorActorLogicTests</c>'
/// own extraction rationale. No sidecar, no Redis, no timing sensitivity: every input here is a plain enum/
/// bool, every output a plain enum.
/// </summary>
public class TablePersistencePolicyTests
{
    // ------------------------------------------------------------------
    // ResolveFlushIntervalMs — 0 -> default, positive verbatim.
    // ------------------------------------------------------------------

    [Fact]
    public void ResolveFlushIntervalMs_Zero_ResolvesToDefault()
    {
        Assert.Equal(2000, TablePersistencePolicy.ResolveFlushIntervalMs(0));
        Assert.Equal(2000, TablePersistencePolicy.DefaultFlushMs);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(500)]
    [InlineData(60_000)]
    public void ResolveFlushIntervalMs_Positive_UsedVerbatim(int flushMs)
    {
        Assert.Equal(flushMs, TablePersistencePolicy.ResolveFlushIntervalMs(flushMs));
    }

    // ------------------------------------------------------------------
    // Not dirty -> always Skip, regardless of mode or in-flight state.
    // ------------------------------------------------------------------

    [Theory]
    [InlineData(TablePersistenceMode.Batched)]
    [InlineData(TablePersistenceMode.FireAndForget)]
    [InlineData(TablePersistenceMode.MemoryOnly)]
    public void DecideFlushAction_NotDirty_AlwaysSkips(TablePersistenceMode mode)
    {
        Assert.Equal(TablePersistAction.Skip, TablePersistencePolicy.DecideFlushAction(mode, dirty: false, writeInProgress: false));
        Assert.Equal(TablePersistAction.Skip, TablePersistencePolicy.DecideFlushAction(mode, dirty: false, writeInProgress: true));
    }

    // ------------------------------------------------------------------
    // Batched — awaited write whenever dirty, in-flight flag irrelevant (Batched never leaves a write
    // in flight across ticks in the first place).
    // ------------------------------------------------------------------

    [Fact]
    public void DecideFlushAction_Batched_Dirty_AwaitsWrite()
    {
        Assert.Equal(TablePersistAction.AwaitedWrite, TablePersistencePolicy.DecideFlushAction(TablePersistenceMode.Batched, dirty: true, writeInProgress: false));
    }

    [Fact]
    public void DecideFlushAction_Batched_Dirty_IgnoresStaleWriteInProgressFlag()
    {
        // Defensive: even if a caller mistakenly passed writeInProgress=true for Batched, the awaited
        // path never has a concept of "in flight" — it should still await, not skip.
        Assert.Equal(TablePersistAction.AwaitedWrite, TablePersistencePolicy.DecideFlushAction(TablePersistenceMode.Batched, dirty: true, writeInProgress: true));
    }

    // ------------------------------------------------------------------
    // FireAndForget — background write when nothing is in flight; single-flight Skip otherwise.
    // ------------------------------------------------------------------

    [Fact]
    public void DecideFlushAction_FireAndForget_Dirty_NoWriteInFlight_StartsBackgroundWrite()
    {
        Assert.Equal(TablePersistAction.BackgroundWrite, TablePersistencePolicy.DecideFlushAction(TablePersistenceMode.FireAndForget, dirty: true, writeInProgress: false));
    }

    [Fact]
    public void DecideFlushAction_FireAndForget_Dirty_WriteAlreadyInFlight_SkipsInsteadOfOverlapping()
    {
        // The single-flight guarantee: never start a second background write for the same actor while
        // one is still running — the next tick retries once it completes.
        Assert.Equal(TablePersistAction.Skip, TablePersistencePolicy.DecideFlushAction(TablePersistenceMode.FireAndForget, dirty: true, writeInProgress: true));
    }

    // ------------------------------------------------------------------
    // MemoryOnly — never reaches the state manager, dirty or not, in flight or not.
    // ------------------------------------------------------------------

    [Fact]
    public void DecideFlushAction_MemoryOnly_Dirty_NeverWrites()
    {
        Assert.Equal(TablePersistAction.Skip, TablePersistencePolicy.DecideFlushAction(TablePersistenceMode.MemoryOnly, dirty: true, writeInProgress: false));
        Assert.Equal(TablePersistAction.Skip, TablePersistencePolicy.DecideFlushAction(TablePersistenceMode.MemoryOnly, dirty: true, writeInProgress: true));
    }

    [Fact]
    public void DecideFlushAction_MemoryOnly_NeverProducesAWriteAction()
    {
        // Exhaustive: across every (dirty, writeInProgress) combination, MemoryOnly must never produce
        // AwaitedWrite or BackgroundWrite — this is the "never reaches the state manager" contract.
        foreach (var dirty in new[] { false, true })
        {
            foreach (var writeInProgress in new[] { false, true })
            {
                var action = TablePersistencePolicy.DecideFlushAction(TablePersistenceMode.MemoryOnly, dirty, writeInProgress);
                Assert.Equal(TablePersistAction.Skip, action);
            }
        }
    }
}
