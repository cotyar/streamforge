using StreamsForge.Abstractions;
using Xunit;
using Ycs;

namespace StreamsForge.Connectors.Crdt.Tests;

/// <summary>
/// Plan 020 wave F — the bounded counter's proof. Every <see cref="YDoc"/> below is real, built through
/// the Ycs API, same "no mock where there is no socket" standard <see cref="CrdtProjectorTests"/> already
/// holds itself to.
///
/// <para><b>The test that matters most</b> is
/// <see cref="ConcurrentOverspendAcrossReplicasThatNeverSyncedNeverBreachesTheBound"/>: several replica
/// documents that never apply each other's updates before every spend decision has already been made,
/// each spending up to and past its own allowance, merged only afterward. That ordering — decide, THEN
/// sync — is the only case a CRDT makes hard, and it is the one this test constructs. A test that synced
/// the replicas before spending would prove nothing: with a shared, up-to-date document, ordinary
/// last-writer-wins bookkeeping could answer the same question, and no CRDT would be needed at all.</para>
/// </summary>
public class EscrowCounterTests
{
    private static CrdtEscrowConfig Config(params (string Replica, long Initial)[] allowances) => new()
    {
        CounterMap = "escrow",
        InitialAllowance = allowances.ToDictionary(a => a.Replica, a => a.Initial),
    };

    // ------------------------------------------------------------------
    // The test that matters most.
    // ------------------------------------------------------------------

    [Fact]
    public void ConcurrentOverspendAcrossReplicasThatNeverSyncedNeverBreachesTheBound()
    {
        // Three named sites, K = 9 (3 each) — plan 020 wave F's own "named sites: a warehouse, a shop
        // floor, a vessel" scale, not thousands of tabs.
        var config = Config(("dock-a", 3), ("dock-b", 3), ("dock-c", 3));

        // THREE INDEPENDENT documents. Each simulates one site's own edge, offline from the other two —
        // no ApplyUpdateV1 crosses between them until every spend below has already been decided. If the
        // sites HAD synced first, this would not be testing anything a plain shared counter couldn't do.
        var docA = new YDoc();
        var docB = new YDoc();
        var docC = new YDoc();

        // Every site tries to spend MORE than its own 3-unit share, entirely in ignorance of the other
        // two sites doing the exact same thing at the "same" moment (there is no synchronization point
        // between these calls at all — they are not even interleaved on a clock, which is the point: no
        // coordination of any kind is used to keep the sum bounded).
        var dockASpend1 = EscrowCounter.TrySpend(docA, config, "dock-a", 3); // ok — uses its whole share
        var dockASpend2 = EscrowCounter.TrySpend(docA, config, "dock-a", 1); // refused — 0 left

        var dockBSpend1 = EscrowCounter.TrySpend(docB, config, "dock-b", 2); // ok — 1 left
        var dockBSpend2 = EscrowCounter.TrySpend(docB, config, "dock-b", 2); // refused — only 1 left

        var dockCSpend1 = EscrowCounter.TrySpend(docC, config, "dock-c", 3); // ok — uses its whole share
        var dockCSpend2 = EscrowCounter.TrySpend(docC, config, "dock-c", 1); // refused — 0 left

        // Every refusal is a REPORTED value, never a silent no-op indistinguishable from success — plan
        // 020 wave F limit 2's "must be visible to the operator, not silent", proven at the lowest level
        // this mechanism has.
        Assert.True(dockASpend1.Ok);
        Assert.False(dockASpend2.Ok);
        Assert.False(string.IsNullOrEmpty(dockASpend2.Reason));
        Assert.Contains("dock-a", dockASpend2.Reason);

        Assert.True(dockBSpend1.Ok);
        Assert.False(dockBSpend2.Ok);
        Assert.Contains("holds only 1", dockBSpend2.Reason);

        Assert.True(dockCSpend1.Ok);
        Assert.False(dockCSpend2.Ok);

        // Had every attempted spend above actually succeeded — which is exactly what "every replica
        // spends at once in ignorance of the others" without a bounded counter would risk — the total
        // would have been 3+1+2+2+3+1 = 12, three over the K=9 bound. Only 8 actually applied.
        var attemptedIfUnchecked = 3 + 1 + 2 + 2 + 3 + 1;
        Assert.Equal(12, attemptedIfUnchecked);

        // NOW merge — the first point at which any of these three documents learns anything about the
        // other two. Each is fed as its own full current state (no target state vector), the same
        // "successive full-state snapshots converge exactly like incremental updates would" technique
        // CrdtDocGrainClusterTests' own class doc already establishes for this fork.
        var merged = new YDoc();
        merged.ApplyUpdateV1(docA.EncodeStateAsUpdateV1());
        merged.ApplyUpdateV1(docB.EncodeStateAsUpdateV1());
        merged.ApplyUpdateV1(docC.EncodeStateAsUpdateV1());

        var status = EscrowCounter.Status(merged, config);

        // The property the entire mechanism exists to deliver: the sum never breaches K, proven against
        // replicas that decided everything in mutual ignorance and only reconciled afterward.
        Assert.Equal(9, status.Bound);
        Assert.Equal(8, status.TotalSpent); // 3 + 2 + 3 — exactly what each site's own gate allowed through
        Assert.True(status.TotalSpent <= status.Bound);

        var byName = status.Replicas.ToDictionary(r => r.Replica);
        Assert.Equal(3, byName["dock-a"].Spent);
        Assert.Equal(0, byName["dock-a"].LocalAllowance);
        Assert.True(byName["dock-a"].Exhausted);

        Assert.Equal(2, byName["dock-b"].Spent);
        Assert.Equal(1, byName["dock-b"].LocalAllowance);
        Assert.False(byName["dock-b"].Exhausted);

        Assert.Equal(3, byName["dock-c"].Spent);
        Assert.Equal(0, byName["dock-c"].LocalAllowance);
        Assert.True(byName["dock-c"].Exhausted);
    }

    // ------------------------------------------------------------------
    // LocalAllowance's own formula, isolated from Merge.
    // ------------------------------------------------------------------

    [Fact]
    public void LocalAllowanceStartsAtTheConfiguredInitialAmount()
    {
        var config = Config(("a", 10), ("b", 5));
        var doc = new YDoc();

        Assert.Equal(10, EscrowCounter.LocalAllowance(doc, config, "a"));
        Assert.Equal(5, EscrowCounter.LocalAllowance(doc, config, "b"));
        Assert.Equal(0, EscrowCounter.Spent(doc, config, "a")); // never spent -> absent key, reads as 0
    }

    [Fact]
    public void SpendingReducesOnlyTheSpendingReplicasOwnAllowance()
    {
        var config = Config(("a", 10), ("b", 5));
        var doc = new YDoc();

        var result = EscrowCounter.TrySpend(doc, config, "a", 4);

        Assert.True(result.Ok);
        Assert.Equal(6, result.LocalAllowance);
        Assert.Equal(6, EscrowCounter.LocalAllowance(doc, config, "a"));
        Assert.Equal(5, EscrowCounter.LocalAllowance(doc, config, "b")); // untouched
    }

    [Fact]
    public void ASpendOfExactlyTheRemainingAllowanceSucceedsAndLeavesNothing()
    {
        var config = Config(("a", 4));
        var doc = new YDoc();

        var result = EscrowCounter.TrySpend(doc, config, "a", 4);

        Assert.True(result.Ok);
        Assert.Equal(0, result.LocalAllowance);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ANonPositiveSpendIsRefusedRatherThanTreatedAsANoOp(long amount)
    {
        var config = Config(("a", 4));
        var doc = new YDoc();

        var result = EscrowCounter.TrySpend(doc, config, "a", amount);

        Assert.False(result.Ok);
        Assert.Equal(4, EscrowCounter.LocalAllowance(doc, config, "a")); // untouched
    }

    [Fact]
    public void SpendingAnUndeclaredReplicaIsRefusedNeverImplicitlyZeroAllowance()
    {
        var config = Config(("a", 10));
        var doc = new YDoc();

        var result = EscrowCounter.TrySpend(doc, config, "ghost", 1);

        Assert.False(result.Ok);
        Assert.Contains("ghost", result.Reason);
        Assert.Contains("not declared", result.Reason);
    }

    [Fact]
    public void ReadingLocalAllowanceForAnUndeclaredReplicaThrowsRatherThanGuessing()
    {
        // Plan 020 wave F limit 4: "configured, never inferred." A pure reader has no refusal channel to
        // report through, so it throws — matching CrdtEscrowConfig's own class doc.
        var config = Config(("a", 10));
        var doc = new YDoc();

        Assert.Throws<ArgumentException>(() => EscrowCounter.LocalAllowance(doc, config, "ghost"));
    }

    // ------------------------------------------------------------------
    // Transfer / rebalance arithmetic, isolated from the grain RPC (which is proven end-to-end in
    // CrdtDocGrainClusterTests — this is the pure math the RPC delegates to).
    // ------------------------------------------------------------------

    [Fact]
    public void TransferMovesAllowanceFromSenderToReceiverAndTheGlobalSumIsUnchanged()
    {
        var config = Config(("a", 10), ("b", 5));
        var doc = new YDoc();

        var before = EscrowCounter.Status(doc, config);
        Assert.Equal(15, before.Bound);

        var result = EscrowCounter.TryTransfer(doc, config, "a", "b", 4);

        Assert.True(result.Ok);
        Assert.Equal(6, result.FromAllowance);  // a: 10 - 4
        Assert.Equal(9, result.ToAllowance);    // b: 5 + 4
        Assert.Equal(6, EscrowCounter.LocalAllowance(doc, config, "a"));
        Assert.Equal(9, EscrowCounter.LocalAllowance(doc, config, "b"));

        // The bound itself — the sum of every replica's allowance — never moves. A transfer redistributes
        // it; it can neither create nor destroy it.
        var after = EscrowCounter.Status(doc, config);
        Assert.Equal(15, after.Bound);
        Assert.Equal(before.Bound, after.Bound);
    }

    [Fact]
    public void ARebalanceThatWouldPushTheSenderNegativeIsRefusedAndWritesNothing()
    {
        var config = Config(("a", 3), ("b", 5));
        var doc = new YDoc();

        var result = EscrowCounter.TryTransfer(doc, config, "a", "b", 999);

        Assert.False(result.Ok);
        Assert.Contains("holds only 3", result.Reason);
        // Nothing moved.
        Assert.Equal(3, EscrowCounter.LocalAllowance(doc, config, "a"));
        Assert.Equal(5, EscrowCounter.LocalAllowance(doc, config, "b"));
    }

    [Fact]
    public void ARebalanceCanUnstickAnExhaustedReplica()
    {
        var config = Config(("a", 3), ("b", 3));
        var doc = new YDoc();

        Assert.True(EscrowCounter.TrySpend(doc, config, "a", 3).Ok); // a is now exhausted
        Assert.False(EscrowCounter.TrySpend(doc, config, "a", 1).Ok); // stuck until it reconnects/rebalances

        var status = EscrowCounter.Status(doc, config);
        Assert.True(status.Replicas.Single(r => r.Replica == "a").Exhausted);

        var rebalance = EscrowCounter.TryTransfer(doc, config, "b", "a", 1);
        Assert.True(rebalance.Ok);

        // a can spend again, now that it has been rebalanced allowance.
        var spendAfterRebalance = EscrowCounter.TrySpend(doc, config, "a", 1);
        Assert.True(spendAfterRebalance.Ok);
        Assert.Equal(0, spendAfterRebalance.LocalAllowance);
    }

    [Fact]
    public void TransferringToOrFromAnUndeclaredReplicaIsRefused()
    {
        var config = Config(("a", 10));
        var doc = new YDoc();

        var result = EscrowCounter.TryTransfer(doc, config, "a", "ghost", 1);

        Assert.False(result.Ok);
        Assert.Contains("ghost", result.Reason);
    }

    [Fact]
    public void TransferringAReplicasAllowanceToItselfIsRefused()
    {
        var config = Config(("a", 10));
        var doc = new YDoc();

        var result = EscrowCounter.TryTransfer(doc, config, "a", "a", 1);

        Assert.False(result.Ok);
    }

    // ------------------------------------------------------------------
    // D7's idempotence, restated for the escrow counter: re-merging an already-applied spend/transfer
    // must not double-count it. This is what makes it safe for an edge to redeliver its own store-and-
    // forward batch after a reconnect, exactly as it already is for an ordinary content edit.
    // ------------------------------------------------------------------

    [Fact]
    public void ReApplyingTheSameSpendUpdateDoesNotDoubleCountIt()
    {
        var config = Config(("a", 10));

        var edge = new YDoc();
        var spend = EscrowCounter.TrySpend(edge, config, "a", 4);
        Assert.True(spend.Ok);
        var update = edge.EncodeStateAsUpdateV1();

        var server = new YDoc();
        server.ApplyUpdateV1(update);
        server.ApplyUpdateV1(update); // redelivered — a flaky link retrying the same batch
        server.ApplyUpdateV1(update);

        Assert.Equal(4, EscrowCounter.Spent(server, config, "a"));
        Assert.Equal(6, EscrowCounter.LocalAllowance(server, config, "a"));
    }
}
