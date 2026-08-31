using StreamsForge.AppCore.Connectors;
using StreamsForge.AppCore.Connectors.Polling;
using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Actors;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 006 (ingestion connectors) W3-B: unit tests for <see cref="ConnectorBookkeeping"/> — the pure
/// cycle-state bookkeeping extracted from <see cref="ConnectorActor"/> (status transitions, backoff-next-
/// run computation, dedup/ledger persistence round-trip), exactly like
/// dapr/tests/StreamsForge.Dapr.Tests/GeneratorBatchingTests.cs covers <c>GeneratorBatching</c>. No actor,
/// timer, or Dapr-sidecar machinery involved — every type touched here (<see cref="ConnectorActorState"/>,
/// <see cref="PollCycleResult"/>, <see cref="DedupTracker"/>, <see cref="FileLedger"/>) is a plain CLR
/// type.
/// </summary>
public class ConnectorActorLogicTests
{
    private static SourceDefinition UrlSource(string name = "s1", ScheduleSpec? schedule = null) => new()
    {
        Name = name,
        Kind = SourceKinds.Url,
        Enabled = true,
        Connector = new ConnectorConfig
        {
            Schedule = schedule,
            Url = new UrlPollConfig { Url = "http://example.invalid/data" },
        },
    };

    // ------------------------------------------------------------------
    // EffectiveSchedule
    // ------------------------------------------------------------------

    [Fact]
    public void EffectiveSchedule_NoConnectorConfig_DefaultsTo30SecondInterval()
    {
        var def = new SourceDefinition { Name = "s1", Kind = SourceKinds.Url, Connector = null };

        var schedule = ConnectorBookkeeping.EffectiveSchedule(def);

        Assert.Equal(30_000, schedule.IntervalMs);
        Assert.Null(schedule.Cron);
    }

    [Fact]
    public void EffectiveSchedule_NoScheduleOnConnectorConfig_DefaultsTo30SecondInterval()
    {
        var def = new SourceDefinition { Name = "s1", Kind = SourceKinds.Url, Connector = new ConnectorConfig { Schedule = null } };

        var schedule = ConnectorBookkeeping.EffectiveSchedule(def);

        Assert.Equal(30_000, schedule.IntervalMs);
    }

    [Fact]
    public void EffectiveSchedule_ConfiguredSchedule_IsUsedVerbatim()
    {
        var def = UrlSource(schedule: new ScheduleSpec { IntervalMs = 5_000 });

        var schedule = ConnectorBookkeeping.EffectiveSchedule(def);

        Assert.Equal(5_000, schedule.IntervalMs);
    }

    // ------------------------------------------------------------------
    // ApplyPollResult — success path
    // ------------------------------------------------------------------

    [Fact]
    public void ApplyPollResult_Success_ResetsFailuresSetsOkStatusAndAdvancesCounters()
    {
        var def = UrlSource(schedule: new ScheduleSpec { IntervalMs = 5_000 });
        var state = new ConnectorActorState { Def = def, ConsecutiveFailures = 3, EventsEmittedTotal = 10 };
        var rows = new List<Dictionary<string, object?>> { new() { ["x"] = 1 }, new() { ["x"] = 2 } };
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

        ConnectorBookkeeping.ApplyPollResult(state, new PollCycleResult(rows, null), now);

        Assert.Equal(0, state.ConsecutiveFailures);
        Assert.Equal("ok", state.LastStatus);
        Assert.Null(state.LastError);
        Assert.Equal(2, state.LastBatchCount);
        Assert.Equal(12, state.EventsEmittedTotal); // 10 + 2
        Assert.Equal(now.ToUnixTimeMilliseconds(), state.LastRunMs);
        Assert.Equal(now.AddMilliseconds(5_000).ToUnixTimeMilliseconds(), state.NextRunMs);
    }

    [Fact]
    public void ApplyPollResult_SuccessWithNoRows_StillAdvancesLastRunAndNextRun()
    {
        var def = UrlSource(schedule: new ScheduleSpec { IntervalMs = 5_000 });
        var state = new ConnectorActorState { Def = def };
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

        ConnectorBookkeeping.ApplyPollResult(state, new PollCycleResult([], null), now);

        Assert.Equal("ok", state.LastStatus);
        Assert.Equal(0, state.LastBatchCount);
        Assert.Equal(0, state.EventsEmittedTotal);
        Assert.NotNull(state.NextRunMs);
    }

    // ------------------------------------------------------------------
    // ApplyPollResult — failure path / backoff
    // ------------------------------------------------------------------

    [Fact]
    public void ApplyPollResult_Failure_IncrementsFailuresSetsErrorStatusAndZeroBatch()
    {
        var def = UrlSource(schedule: new ScheduleSpec { IntervalMs = 5_000 });
        var state = new ConnectorActorState { Def = def, ConsecutiveFailures = 0 };
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

        ConnectorBookkeeping.ApplyPollResult(state, new PollCycleResult([], "boom"), now);

        Assert.Equal(1, state.ConsecutiveFailures);
        Assert.Equal("error", state.LastStatus);
        Assert.Equal("boom", state.LastError);
        Assert.Equal(0, state.LastBatchCount);
    }

    [Fact]
    public void ApplyPollResult_ConsecutiveFailures_NextRunHonorsExponentialBackoff()
    {
        // D-E: min(30s * 2^(k-1), 15min). At k=1 (this failure is the 1st) the delay is 30s, which
        // exceeds the configured 5s interval, so NextRun should reflect the 30s backoff floor, not the
        // configured interval (BackoffPolicy.NextRun: nowUtc + max(interval, Delay(k))).
        var def = UrlSource(schedule: new ScheduleSpec { IntervalMs = 5_000 });
        var state = new ConnectorActorState { Def = def, ConsecutiveFailures = 0 };
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

        ConnectorBookkeeping.ApplyPollResult(state, new PollCycleResult([], "boom"), now);

        Assert.Equal(now.AddSeconds(30).ToUnixTimeMilliseconds(), state.NextRunMs);
    }

    [Fact]
    public void ApplyPollResult_RepeatedFailures_NextRunKeepsAdvancingViaBackoff()
    {
        var def = UrlSource(schedule: new ScheduleSpec { IntervalMs = 1_000 });
        var state = new ConnectorActorState { Def = def };
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

        ConnectorBookkeeping.ApplyPollResult(state, new PollCycleResult([], "e1"), now);
        var firstNextRun = state.NextRunMs;

        ConnectorBookkeeping.ApplyPollResult(state, new PollCycleResult([], "e2"), now.AddSeconds(30));
        var secondNextRun = state.NextRunMs;

        Assert.Equal(2, state.ConsecutiveFailures);
        Assert.True(secondNextRun > firstNextRun);
        // k=2 -> delay = 30s * 2^1 = 60s from the second failure's "now".
        Assert.Equal(now.AddSeconds(30).AddSeconds(60).ToUnixTimeMilliseconds(), secondNextRun);
    }

    [Fact]
    public void ApplyPollResult_SuccessAfterFailures_ResetsBackoffToBaseInterval()
    {
        var def = UrlSource(schedule: new ScheduleSpec { IntervalMs = 5_000 });
        var state = new ConnectorActorState { Def = def, ConsecutiveFailures = 4 };
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

        ConnectorBookkeeping.ApplyPollResult(state, new PollCycleResult([], null), now);

        Assert.Equal(0, state.ConsecutiveFailures);
        // k=0 -> Delay is zero, so NextRun = now + interval (5s), not a lingering backoff delay.
        Assert.Equal(now.AddMilliseconds(5_000).ToUnixTimeMilliseconds(), state.NextRunMs);
    }

    // ------------------------------------------------------------------
    // ApplySubscriberBatch (gRPC kind)
    // ------------------------------------------------------------------

    [Fact]
    public void ApplySubscriberBatch_OkWithRows_ResetsFailuresAndUpdatesBatchCounters()
    {
        var state = new ConnectorActorState { ConsecutiveFailures = 2, EventsEmittedTotal = 5 };

        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 3, status: "ok", error: null);

        Assert.Equal(0, state.ConsecutiveFailures);
        Assert.Equal("ok", state.LastStatus);
        Assert.Null(state.LastError);
        Assert.Equal(3, state.LastBatchCount);
        Assert.Equal(8, state.EventsEmittedTotal);
        Assert.NotNull(state.LastRunMs);
    }

    [Fact]
    public void ApplySubscriberBatch_OkWithZeroRows_ResetsFailuresButLeavesBatchCountersUntouched()
    {
        var state = new ConnectorActorState { ConsecutiveFailures = 2, EventsEmittedTotal = 5, LastBatchCount = 7, LastRunMs = 123 };

        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 0, status: "ok", error: null);

        Assert.Equal(0, state.ConsecutiveFailures);
        Assert.Equal("ok", state.LastStatus);
        // Batch counters untouched by a pure status ping (e.g. right after a successful (re)connect).
        Assert.Equal(5, state.EventsEmittedTotal);
        Assert.Equal(7, state.LastBatchCount);
        Assert.Equal(123, state.LastRunMs);
    }

    [Fact]
    public void ApplySubscriberBatch_Error_IncrementsFailuresAndRecordsError()
    {
        var state = new ConnectorActorState { ConsecutiveFailures = 0 };

        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 0, status: "error", error: "connection refused");

        Assert.Equal(1, state.ConsecutiveFailures);
        Assert.Equal("error", state.LastStatus);
        Assert.Equal("connection refused", state.LastError);
    }

    [Fact]
    public void ApplySubscriberBatch_TransientStatus_RecordsStatusWithoutTouchingFailureStreak()
    {
        var state = new ConnectorActorState { ConsecutiveFailures = 3 };

        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 0, status: "connecting", error: null);

        Assert.Equal(3, state.ConsecutiveFailures); // untouched
        Assert.Equal("connecting", state.LastStatus);
    }

    [Fact]
    public void ApplySubscriberBatch_NextRunMs_IsNeverTouched()
    {
        // A persistent gRPC subscription has no scheduled "next run" (D-G) — reconnection timing is
        // entirely internal to GrpcSubscriberCore, so ApplySubscriberBatch must never write NextRunMs.
        var state = new ConnectorActorState { NextRunMs = 999 };

        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 5, status: "ok", error: null);
        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 0, status: "error", error: "x");

        Assert.Equal(999, state.NextRunMs);
    }

    // ------------------------------------------------------------------
    // Dedup / ledger persistence round-trip (the actor's own TickAsync flow, minus the actor)
    // ------------------------------------------------------------------

    [Fact]
    public void DedupAndLedger_RoundTripThroughConnectorActorState_PreservesSeenKeysAndFileTimes()
    {
        var state = new ConnectorActorState();

        // First cycle: two new dedup keys, one new file.
        var dedup = new DedupTracker(state.DedupKeys);
        Assert.False(dedup.Seen("k1"));
        Assert.False(dedup.Seen("k2"));
        state.DedupKeys = dedup.ToPersistable();

        var ledger = new FileLedger(state.Ledger);
        Assert.True(ledger.IsNewOrChanged("/tmp/a.ndjson", 1000));
        ledger.Record("/tmp/a.ndjson", 1000);
        state.Ledger = ledger.ToPersistable();

        // Second cycle: reconstruct from persisted state (as OnActivateAsync/TickAsync do every time).
        var dedup2 = new DedupTracker(state.DedupKeys);
        Assert.True(dedup2.Seen("k1")); // already seen — persisted correctly
        Assert.False(dedup2.Seen("k3")); // genuinely new

        var ledger2 = new FileLedger(state.Ledger);
        Assert.False(ledger2.IsNewOrChanged("/tmp/a.ndjson", 1000)); // unchanged mtime — persisted correctly
        Assert.True(ledger2.IsNewOrChanged("/tmp/a.ndjson", 2000)); // mtime bumped — detected as changed
    }
}
