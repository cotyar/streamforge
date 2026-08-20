using StreamForge.Abstractions;
using Xunit;
using Ycs;

namespace StreamForge.Connectors.Crdt.Tests;

/// <summary>
/// Plan 020 wave F, added by the orchestrator's review of that wave. The wave shipped a coordinator-written
/// transfer for ANY replica, arguing it was a stricter form of the plan's "only replica <c>i</c> writes
/// <c>T[i][*]</c>" rule. It is not stricter, it is unsound, and the first test here is the sequence that
/// proves it — every caller behaving correctly, using only sanctioned APIs, spending 16 against a bound
/// of 10.
///
/// <para>Note how this differs from the wave's own documented caveat that a FABRICATED update (writing
/// <c>d:x = 999</c> directly, bypassing <c>TrySpend</c>) breaches the bound. That one is true, is inherent
/// to a CRDT — the merge always succeeds — and is honestly documented as a cooperative protocol. This one
/// needs no bad actor at all.</para>
/// </summary>
public class EscrowCoordinatorTransferTests
{
    private static CrdtEscrowConfig WithReserve() => new()
    {
        CounterMap = "escrow",
        ReserveReplica = "reserve",
        InitialAllowance = new Dictionary<string, long> { ["reserve"] = 6, ["a"] = 4, ["b"] = 0 },
    };

    private static CrdtEscrowConfig NoReserve() => new()
    {
        CounterMap = "escrow",
        InitialAllowance = new Dictionary<string, long> { ["a"] = 10, ["b"] = 0 },
    };

    private static YDoc Merged(params YDoc[] docs)
    {
        var merged = new YDoc();
        foreach (var d in docs)
        {
            merged.ApplyUpdateV1(d.EncodeStateAsUpdateV1());
        }
        return merged;
    }

    [Fact]
    public void ACoordinatorMayNotTransferOutOfASpendingReplica_TheSequenceThatBreachedTheBound()
    {
        var config = NoReserve();
        var docA = new YDoc();     // replica 'a', offline
        var docCoord = new YDoc(); // the hosted/coordinating document

        // 1. 'a' spends its whole allowance offline. Nothing is shared.
        Assert.True(EscrowCounter.TrySpend(docA, config, "a", 10).Ok);

        // 2. The coordinator still sees 'a' holding 10 — it has not seen the spend and cannot.
        Assert.Equal(10, EscrowCounter.LocalAllowance(docCoord, config, "a"));

        // 3. ...and is refused. Before the fix this succeeded, 'b' then spent the 6 it was given, and the
        //    converged total was 16 against a bound of 10.
        var transfer = EscrowCounter.TryCoordinatorTransfer(docCoord, config, "a", "b", 6);
        Assert.False(transfer.Ok);
        Assert.Contains("spent offline", transfer.Reason, StringComparison.Ordinal);

        // 4. The bound holds through convergence, which is the property that actually matters.
        var status = EscrowCounter.Status(Merged(docA, docCoord), config);
        Assert.Equal(10, status.Bound);
        Assert.True(status.TotalSpent <= status.Bound, $"spent {status.TotalSpent} of {status.Bound}");
    }

    [Fact]
    public void ACoordinatorMayTransferOutOfTheReserveBecauseAReserveCannotHaveAnUnsyncedSpend()
    {
        var config = WithReserve();
        var docA = new YDoc();
        var docCoord = new YDoc();

        // 'a' spends everything it holds, offline and unseen — the same starting position as the test
        // above. The difference is only WHOSE allowance the coordinator gives away.
        Assert.True(EscrowCounter.TrySpend(docA, config, "a", 4).Ok);

        var transfer = EscrowCounter.TryCoordinatorTransfer(docCoord, config, "reserve", "a", 6);
        Assert.True(transfer.Ok, transfer.Reason);

        // 'a' picks up the grant and may spend it — and the total is still within K = 6 + 4 + 0 = 10.
        docA.ApplyUpdateV1(docCoord.EncodeStateAsUpdateV1());
        Assert.True(EscrowCounter.TrySpend(docA, config, "a", 6).Ok);
        Assert.False(EscrowCounter.TrySpend(docA, config, "a", 1).Ok);

        var status = EscrowCounter.Status(Merged(docA, docCoord), config);
        Assert.Equal(10, status.Bound);
        Assert.Equal(10, status.TotalSpent);
        Assert.True(status.TotalSpent <= status.Bound);
    }

    [Fact]
    public void TheReserveItselfMayNeverSpend_WhichIsWhatMakesTheAboveSafe()
    {
        var config = WithReserve();
        var doc = new YDoc();

        var spend = EscrowCounter.TrySpend(doc, config, "reserve", 1);

        Assert.False(spend.Ok);
        Assert.Contains("reserve", spend.Reason, StringComparison.Ordinal);
        Assert.Equal(0, EscrowCounter.Spent(doc, config, "reserve"));
    }

    [Fact]
    public void AGiverInitiatedTransferIsSoundEvenWhenTheGiverHasSpentOffline()
    {
        // The sound replica-to-replica path the plan actually specifies: the GIVER writes its own
        // outbound transfer, on its own document, so the deduction lands before anyone can use it.
        var config = NoReserve();
        var docA = new YDoc();
        var docB = new YDoc();

        Assert.True(EscrowCounter.TrySpend(docA, config, "a", 7).Ok);

        // 'a' now knows it holds 3 and can only give away what it still has.
        Assert.False(EscrowCounter.TryTransfer(docA, config, "a", "b", 6).Ok);
        Assert.True(EscrowCounter.TryTransfer(docA, config, "a", "b", 3).Ok);

        docB.ApplyUpdateV1(docA.EncodeStateAsUpdateV1());
        Assert.True(EscrowCounter.TrySpend(docB, config, "b", 3).Ok);
        Assert.False(EscrowCounter.TrySpend(docB, config, "b", 1).Ok);

        var status = EscrowCounter.Status(Merged(docA, docB), config);
        Assert.Equal(10, status.Bound);
        Assert.Equal(10, status.TotalSpent);
    }

    [Fact]
    public void WithNoReserveDeclaredTheCoordinatorRouteIsUnusableRatherThanUnsafe()
    {
        var result = EscrowCounter.TryCoordinatorTransfer(new YDoc(), NoReserve(), "a", "b", 1);

        Assert.False(result.Ok);
        Assert.Contains("no reserveReplica", result.Reason, StringComparison.Ordinal);
    }
}
