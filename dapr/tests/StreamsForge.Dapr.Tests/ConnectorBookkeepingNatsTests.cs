using StreamsForge.Abstractions;
using StreamsForge.AppCore.Connectors;
using StreamsForge.Dapr.Host.Actors;
using Xunit;

namespace StreamsForge.Dapr.Tests;

/// <summary>Plan 009 B1/C2: <see cref="ConnectorBookkeeping"/> additions for the nats kind and for
/// coercion-failure surfacing — a sibling to dapr/tests/StreamsForge.Dapr.Tests/ConnectorActorLogicTests.cs
/// (not mine to edit — new-files-only convention), which already covers the grpc-kind/url-kind
/// baseline this file assumes as ground truth (e.g. that <c>error: null</c> alongside "ok" leaves
/// LastError null — <see cref="ApplySubscriberBatch_OkWithExplicitNullError_LeavesLastErrorNull"/>
/// re-confirms that baseline still holds after the plan 009 change described below).</summary>
public class ConnectorBookkeepingNatsTests
{
    private static SourceDefinition NatsSource(CoercionFailurePolicy policy = CoercionFailurePolicy.Null) => new()
    {
        Name = "n1",
        Kind = SourceKinds.Nats,
        OnCoercionFailure = policy,
        Connector = new ConnectorConfig { Nats = new NatsSubConfig { Url = "nats://x", Subject = "s" } },
    };

    // ------------------------------------------------------------------
    // ApplySubscriberBatch — plan 009 additive change: "ok" now honors the `error` parameter instead of
    // hardcoding null, so an informational coercion-failure note can ride along with a healthy batch.
    // ------------------------------------------------------------------

    [Fact]
    public void ApplySubscriberBatch_OkWithExplicitNullError_LeavesLastErrorNull()
    {
        // Every pre-009 call site passes error: null alongside "ok" — this must stay byte-identical.
        var state = new ConnectorActorState();

        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 3, status: "ok", error: null);

        Assert.Equal("ok", state.LastStatus);
        Assert.Null(state.LastError);
    }

    [Fact]
    public void ApplySubscriberBatch_OkWithANonNullError_CarriesItAsAnInformationalNote()
    {
        var state = new ConnectorActorState();

        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 5, status: "ok", error: "2 field coercion failure(s) this batch; policy=Null");

        Assert.Equal("ok", state.LastStatus);
        Assert.Equal("2 field coercion failure(s) this batch; policy=Null", state.LastError);
        Assert.Equal(0, state.ConsecutiveFailures); // still a healthy batch, not an error status
    }

    [Fact]
    public void ApplySubscriberBatch_dedupKeys_null_leaves_the_persisted_snapshot_untouched()
    {
        var state = new ConnectorActorState { DedupKeys = ["existing"] };

        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 1, status: "ok", error: null, dedupKeys: null);

        Assert.Equal(["existing"], state.DedupKeys);
    }

    [Fact]
    public void ApplySubscriberBatch_dedupKeys_nonNull_replaces_the_persisted_snapshot()
    {
        var state = new ConnectorActorState { DedupKeys = ["old"] };

        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 1, status: "ok", error: null, dedupKeys: ["new1", "new2"]);

        Assert.Equal(["new1", "new2"], state.DedupKeys);
    }

    [Fact]
    public void ApplySubscriberBatch_dedupKeys_applies_regardless_of_status()
    {
        var state = new ConnectorActorState();

        ConnectorBookkeeping.ApplySubscriberBatch(state, rowCount: 0, status: "error", error: "boom", dedupKeys: ["k1"]);

        Assert.Equal(["k1"], state.DedupKeys);
        Assert.Equal("error", state.LastStatus);
    }

    // ------------------------------------------------------------------
    // ApplyPollResult — plan 009 C2: a successful cycle with non-zero CoercionFailures surfaces an
    // informational LastError instead of the pre-009 unconditional null (a cycle with zero coercion
    // failures is byte-identical to before, since CoercionFailures defaults to 0).
    // ------------------------------------------------------------------

    [Fact]
    public void ApplyPollResult_SuccessWithZeroCoercionFailures_LeavesLastErrorNull()
    {
        var def = NatsSource();
        var state = new ConnectorActorState { Def = def };
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

        ConnectorBookkeeping.ApplyPollResult(state, new PollCycleResult([], null, CoercionFailures: 0), now);

        Assert.Equal("ok", state.LastStatus);
        Assert.Null(state.LastError);
    }

    [Fact]
    public void ApplyPollResult_SuccessWithCoercionFailures_SurfacesACountAndPolicyInLastError()
    {
        var def = NatsSource(CoercionFailurePolicy.DropRow);
        var state = new ConnectorActorState { Def = def };
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

        ConnectorBookkeeping.ApplyPollResult(state, new PollCycleResult([], null, CoercionFailures: 3), now);

        Assert.Equal("ok", state.LastStatus); // still a successful cycle, not an error
        Assert.Equal(0, state.ConsecutiveFailures);
        Assert.NotNull(state.LastError);
        Assert.Contains("3", state.LastError);
        Assert.Contains("DropRow", state.LastError);
    }

    [Fact]
    public void ApplyPollResult_A_RejectBatch_rejection_still_takes_the_ordinary_error_branch()
    {
        // ConnectorPollCycle.Emit turns a RejectBatch rejection into a non-null Error — ApplyPollResult
        // doesn't need to know anything about coercion for THIS case, it's just another cycle failure.
        var def = NatsSource(CoercionFailurePolicy.RejectBatch);
        var state = new ConnectorActorState { Def = def };
        var now = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

        ConnectorBookkeeping.ApplyPollResult(state, new PollCycleResult([], "coercion rejected batch: field \"price\" cannot be coerced to Double", CoercionFailures: 1), now);

        Assert.Equal("error", state.LastStatus);
        Assert.Equal(1, state.ConsecutiveFailures);
        Assert.Contains("coercion rejected batch", state.LastError);
    }
}
