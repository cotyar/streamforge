using StreamsForge.AppCore.Connectors;
using StreamsForge.Dapr.Host.Actors;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>
/// Plan 025 (PARITY.md D6 "late-consumer replay"): unit tests for <see cref="ConnectorAttachState"/> — the
/// pure hold / pending / replay-ring decision behind <see cref="ConnectorActor"/>'s attach gate, extracted
/// for the reason every other pure slice in this project was (<c>TableAttachPolicy</c>,
/// <c>ConnectorBookkeeping</c>, <c>PipelineCompilation</c>): a <see cref="ConnectorActor"/> instance cannot
/// be constructed without a live Dapr sidecar, so anything reachable only through the actor is effectively
/// untested. What the actor keeps is the I/O — the two <c>PublishEventAsync</c> calls and the 10 s one-shot
/// timer — and those remain unverified here, exactly as PARITY.md section 3 says of every Dapr claim not
/// checked against a running instance.
/// </summary>
public class ConnectorAttachStateTests
{
    private static Dictionary<string, object?> Row(int i) => new() { ["i"] = i };

    private static List<Dictionary<string, object?>> Rows(params int[] values) =>
        values.Select(Row).ToList();

    /// <summary>Publish-through-the-door, expressed the way the actor does it: ask whether the gate took
    /// the rows, and if it did not, "publish" (here: record them) yourself.</summary>
    private static void PublishThroughDoor(ConnectorAttachState state, List<Dictionary<string, object?>> rows)
    {
        if (!state.TryDefer(rows))
        {
            state.RecordPublished(rows);
        }
    }

    [Fact]
    public void WithNoHold_RowsArePublishedAndRemembered()
    {
        var state = new ConnectorAttachState();

        Assert.False(state.TryDefer(Rows(1, 2)));

        state.RecordPublished(Rows(1, 2));
        Assert.Equal(2, state.TotalSeen);
        Assert.Equal(0, state.PendingCount);
    }

    [Fact]
    public void AHoldDefersEveryRow_AndReleasingFlushesThemInOrder()
    {
        var state = new ConnectorAttachState();
        PublishThroughDoor(state, Rows(1));

        var (snapshot, totalSeen) = state.BeginAttach();
        Assert.Equal([1], snapshot.Select(r => r["i"]));
        Assert.Equal(1, totalSeen);

        // Everything produced while the hold is open is deferred — the consumer must not see it twice.
        PublishThroughDoor(state, Rows(2, 3));
        PublishThroughDoor(state, Rows(4));
        Assert.Equal(3, state.PendingCount);
        Assert.Equal(1, state.TotalSeen);

        var flush = state.Release();

        // Oldest first, across batch boundaries — the deferral list is flat, and order is what matters.
        Assert.Equal([2, 3, 4], flush.Select(r => r["i"]));
        Assert.Equal(0, state.Holds);
        Assert.Equal(0, state.PendingCount);
    }

    [Fact]
    public void NestedHolds_NeedOneReleaseEach_AndOnlyTheLastFlushes()
    {
        var state = new ConnectorAttachState();

        state.BeginAttach();
        state.BeginAttach();
        Assert.Equal(2, state.Holds);

        PublishThroughDoor(state, Rows(1));

        // First release: still held by the second attacher, so nothing may escape.
        Assert.Empty(state.Release());
        Assert.Equal(1, state.Holds);
        Assert.Equal(1, state.PendingCount);

        Assert.Equal([1], state.Release().Select(r => r["i"]));
        Assert.Equal(0, state.Holds);
    }

    [Fact]
    public void ReleasePastZero_IsANoOp()
    {
        var state = new ConnectorAttachState();

        // A retried proxy call or an over-eager consumer `finally` must not drive the count negative — a
        // negative count would let the NEXT attach's rows walk straight through the gate.
        Assert.Empty(state.Release());
        Assert.Equal(0, state.Holds);

        state.BeginAttach();
        Assert.Equal(1, state.Holds);
    }

    [Fact]
    public void ForceRelease_DropsEveryHold_AndFlushesEverythingDeferred()
    {
        var state = new ConnectorAttachState();
        state.BeginAttach();
        state.BeginAttach();
        state.BeginAttach();
        PublishThroughDoor(state, Rows(1, 2));

        // The safety timer's semantics (and StartAsync/StopAsync's): the only situation this fires in is
        // "somebody is not coming back", and there is no way to tell which holder that was.
        var flush = state.ForceRelease();

        Assert.Equal([1, 2], flush.Select(r => r["i"]));
        Assert.Equal(0, state.Holds);
    }

    [Fact]
    public void DrainingHandsTheRowsOverExactlyOnce()
    {
        var state = new ConnectorAttachState();
        state.BeginAttach();
        PublishThroughDoor(state, Rows(1));

        Assert.Single(state.Release());

        // A second release (or a force-release racing it) must not re-emit rows the first one already
        // handed to the caller — the deferral list is cleared in the same step it is handed out.
        Assert.Empty(state.Release());
        Assert.Empty(state.ForceRelease());
    }

    [Fact]
    public void Snapshot_IsACopy_SoAConsumerCannotReachTheNextOne()
    {
        var state = new ConnectorAttachState();
        PublishThroughDoor(state, Rows(1));

        var (first, _) = state.BeginAttach();
        first[0]["i"] = 999;
        first.Clear();

        var (second, _) = state.BeginAttach();
        Assert.Equal([1], second.Select(r => r["i"]));
    }

    [Fact]
    public void RecordPublished_CopiesTheRow_SoALaterMutationByTheProducerDoesNotRewriteHistory()
    {
        var state = new ConnectorAttachState();
        var row = Row(1);
        state.RecordPublished([row]);

        row["i"] = 999;

        var (snapshot, _) = state.BeginAttach();
        Assert.Equal(1, snapshot[0]["i"]);
    }

    [Fact]
    public void TotalSeen_CountsEvictedRows_SoTheConsumerCanTellItIsMissingSome()
    {
        var state = new ConnectorAttachState();

        // One past the ring's capacity: the oldest row is gone, but TotalSeen keeps counting — which is
        // exactly what turns into the operator-visible "replayed N of M row(s)" warning on the consumer
        // side (PipelineActor / TableActor).
        state.RecordPublished(Enumerable.Range(0, SourceReplayBuffer.Capacity + 1).Select(Row).ToList());

        var (snapshot, totalSeen) = state.BeginAttach();

        Assert.Equal(SourceReplayBuffer.Capacity, snapshot.Count);
        Assert.Equal(SourceReplayBuffer.Capacity + 1, totalSeen);
        Assert.Equal(1, snapshot[0]["i"]); // row 0 evicted
    }

    [Fact]
    public void RowsDeferredThenFlushed_AreNotInTheRingUntilTheActorPublishesThem()
    {
        var state = new ConnectorAttachState();
        state.BeginAttach();
        PublishThroughDoor(state, Rows(1));

        // The ring records what was ACTUALLY published. A deferred row has not been, so a SECOND consumer
        // attaching before the flush must not be told it already went out.
        var (duringHold, totalDuringHold) = state.BeginAttach();
        Assert.Empty(duringHold);
        Assert.Equal(0, totalDuringHold);

        state.Release();
        var flush = state.Release();
        state.RecordPublished(flush); // what ConnectorActor.PublishDirectAsync does after a successful publish

        Assert.Equal(1, state.TotalSeen);
    }
}
