using StreamsForge.Abstractions;
using Ycs;

namespace StreamsForge.Connectors.Crdt;

/// <summary>
/// Plan 020 wave F — a bounded counter (Balegas et al., 2015) implemented as an ordinary <c>YMap</c>,
/// per <see cref="CrdtEscrowConfig"/>'s own class doc (read that first for the shape and the four
/// limits). This class is the "<c>Ycs</c>-side helper" the wave's brief names: everything here operates
/// on a live <see cref="YDoc"/> and is deliberately usable by anything that has one — an edge deciding
/// whether it may spend right now, a test simulating several edges that never see each other, or
/// <c>CrdtDocGrain</c> mediating an online rebalance. Nothing here is Orleans- or Dapr-shaped.
///
/// <para><b>The state, precisely, and how it maps onto the plan's T/D formulation.</b> Inside
/// <c>doc.GetMap(config.CounterMap)</c>:
/// <list type="bullet">
///   <item><c>d:&lt;replica&gt;</c> is <c>D[replica]</c> — what that replica has spent, a running total
///   only that replica's own code ever writes to (via <see cref="TrySpend"/>).</item>
///   <item><c>t:&lt;from&gt;:&lt;to&gt;</c> is <c>T[from][to]</c> — allowance moved from <c>from</c> to
///   <c>to</c>, a running total only a rebalance naming that exact ordered pair ever writes to (via
///   <see cref="TryTransfer"/>).</item>
/// </list>
/// <see cref="LocalAllowance"/> is exactly the plan's formula:
/// <c>initial_i + Σⱼ T[j][i] − Σⱼ T[i][j] − D[i]</c>.</para>
///
/// <para><b>Why this needs no new Ycs type (the plan's own load-bearing observation).</b> Every key here
/// has exactly ONE legitimate writer — replica <c>i</c> for <c>d:i</c>, and for <c>t:i:j</c> whoever owns
/// <c>i</c>'s allowance: the giving replica itself (<see cref="TryTransfer"/> on its own document), or
/// the coordinating document when and only when <c>i</c> is the non-spending reserve
/// (<see cref="TryCoordinatorTransfer"/>). An earlier version of this class let the coordinator write
/// <c>t:i:j</c> for ANY <c>i</c> and argued that was a stricter form of the same discipline; it is not,
/// and a test now pins the 16-spent-against-a-bound-of-10 sequence it allowed. Two callers never
/// legitimately race to set the SAME key, so a <c>YMap</c>'s last-writer-wins is never actually
/// exercised: two replicas spending concurrently in mutual ignorance write to two DIFFERENT keys
/// (<c>d:a</c> and <c>d:b</c>), and Yjs merges a map with two different keys added by writing BOTH,
/// never picking one over the other. That is the entire mechanism — nothing here asks Ycs for anything
/// beyond <c>YMap.Get</c>/<c>Set</c>.</para>
///
/// <para><b>Refuse, don't oversell — the two operations that enforce it.</b> <see cref="TrySpend"/>
/// reads the CURRENT local allowance and refuses (returns <see cref="EscrowSpendResult.Ok"/> false,
/// writes nothing) rather than let <c>D[replica]</c> grow past what that replica is entitled to. Because
/// every replica's own local allowance can only ever be checked against ITS OWN slice of the state
/// (<c>d:</c> for itself, <c>t:</c> pairs it is a party to), and because the SUM of every replica's local
/// allowance is transfer-invariant (a transfer subtracts from one replica's formula and adds the exact
/// same amount to another's — see the arithmetic in <see cref="TryTransfer"/>'s own comment), the global
/// total spent can never exceed <see cref="CrdtEscrowConfig.InitialAllowance"/>'s sum, EVEN WHEN two
/// replicas that never saw each other's updates both spend at once. No synchronous coordination is used
/// to prove that — <c>EscrowCounterTests</c> proves it by literally never syncing the replicas' documents
/// until after every spend decision has already been made.</para>
/// </summary>
public static class EscrowCounter
{
    private const string SpentKeyPrefix = "d:";

    private static string SpentKey(string replica) => SpentKeyPrefix + replica;

    private static string TransferKey(string from, string to) => "t:" + from + ":" + to;

    private static YMap CounterMap(YDoc doc, CrdtEscrowConfig config) =>
        doc.GetMap(string.IsNullOrEmpty(config.CounterMap) ? "escrow" : config.CounterMap);

    private static long ReadLong(YMap map, string key) => map.Has(key) ? Convert.ToInt64(map.Get(key)) : 0L;

    private static void IncrementLong(YMap map, string key, long delta) => map.Set(key, ReadLong(map, key) + delta);

    /// <summary>Throws for a replica name absent from <see cref="CrdtEscrowConfig.InitialAllowance"/> —
    /// see that config class's limit 4 ("configured, never inferred"). <see cref="TrySpend"/> and
    /// <see cref="TryTransfer"/> check this themselves and return a graceful refusal instead of letting
    /// this throw reach their own callers; it is the honest behavior for the lower-level read methods
    /// below, which have no refusal channel of their own to report through.</summary>
    private static void RequireDeclared(CrdtEscrowConfig config, string replica)
    {
        if (!config.InitialAllowance.ContainsKey(replica))
        {
            throw new ArgumentException(
                $"replica '{replica}' is not declared in InitialAllowance", nameof(replica));
        }
    }

    /// <summary><c>D[replica]</c> — the running total that replica has spent. Zero for a replica that has
    /// never spent (an absent key, not a stored zero — no write happens until the first successful
    /// <see cref="TrySpend"/>).</summary>
    public static long Spent(YDoc doc, CrdtEscrowConfig config, string replica)
    {
        RequireDeclared(config, replica);
        return ReadLong(CounterMap(doc, config), SpentKey(replica));
    }

    /// <summary><c>T[from][to]</c> — the running total transferred from <c>from</c> to <c>to</c>, in
    /// that direction only (a transfer back the other way is a DIFFERENT key, <c>t:to:from</c>, never
    /// netted against this one — the plan's own state is two separate monotone maps, not a signed
    /// balance).</summary>
    public static long Transferred(YDoc doc, CrdtEscrowConfig config, string from, string to)
    {
        RequireDeclared(config, from);
        RequireDeclared(config, to);
        return ReadLong(CounterMap(doc, config), TransferKey(from, to));
    }

    /// <summary>The plan's own formula: <c>initial_replica + Σⱼ T[j][replica] − Σⱼ T[replica][j] −
    /// D[replica]</c>. O(replicas) per call — the class doc's own "O(replicas²) total, named sites only"
    /// limit is what keeps this cheap in practice.</summary>
    public static long LocalAllowance(YDoc doc, CrdtEscrowConfig config, string replica)
    {
        RequireDeclared(config, replica);
        var counter = CounterMap(doc, config);
        var initial = config.InitialAllowance[replica];

        long received = 0;
        long sent = 0;
        foreach (var other in config.InitialAllowance.Keys)
        {
            if (string.Equals(other, replica, StringComparison.Ordinal))
            {
                continue;
            }

            received += ReadLong(counter, TransferKey(other, replica));
            sent += ReadLong(counter, TransferKey(replica, other));
        }

        return initial + received - sent - ReadLong(counter, SpentKey(replica));
    }

    /// <summary>The one write a replica ever makes to its OWN <c>d:</c> key. Refuses — writes nothing,
    /// returns <see cref="EscrowSpendResult.Ok"/> false with a reason — rather than let
    /// <c>D[replica]</c> exceed <see cref="LocalAllowance"/>'s current value. This is the entire
    /// mechanism that keeps the global sum bounded without any replica needing to ask another one first:
    /// each call only ever compares against THIS replica's own slice of state, computed from whatever
    /// this <see cref="YDoc"/> currently holds — which, for an edge that has been offline, may be stale,
    /// but staleness only ever makes this MORE conservative (a transfer the edge has not yet seen is not
    /// yet available to it), never less.</summary>
    public static EscrowSpendResult TrySpend(YDoc doc, CrdtEscrowConfig config, string replica, long amount)
    {
        if (!config.InitialAllowance.ContainsKey(replica))
        {
            return EscrowSpendResult.Refusal(
                replica, amount, 0, $"replica '{replica}' is not declared in InitialAllowance");
        }

        if (amount <= 0)
        {
            return EscrowSpendResult.Refusal(
                replica, amount, LocalAllowance(doc, config, replica), "amount to spend must be positive");
        }

        var allowance = LocalAllowance(doc, config, replica);
        // The reserve is the one replica the COORDINATOR may transfer out of, and that is sound only
        // because the reserve never spends — a replica with an unsynced spend cannot safely have its
        // allowance given away (see TryCoordinatorTransfer). So the "never spends" part is enforced
        // here rather than left as a convention somebody eventually breaks.
        if (!string.IsNullOrEmpty(config.ReserveReplica)
            && string.Equals(replica, config.ReserveReplica, StringComparison.Ordinal))
        {
            return EscrowSpendResult.Refusal(
                replica, amount, LocalAllowance(doc, config, replica),
                $"replica '{replica}' is this counter's reserve and may never spend — transfer its "
                + "allowance to a spending replica first");
        }

        if (amount > allowance)
        {
            return EscrowSpendResult.Refusal(
                replica, amount, allowance,
                $"replica '{replica}' holds only {allowance}, cannot spend {amount} — spend refused, "
                + "not partially applied");
        }

        IncrementLong(CounterMap(doc, config), SpentKey(replica), amount);
        return EscrowSpendResult.Success(replica, amount, allowance - amount);
    }

    /// <summary>Plan 020 wave F, limit 2 — the ONLINE operation. Moves <paramref name="amount"/> from
    /// <paramref name="from"/>'s local allowance to <paramref name="to"/>'s, refusing (writes nothing)
    /// when <paramref name="from"/> does not currently hold enough.
    ///
    /// <para><b>Why refusing this matters even though the GLOBAL bound cannot be breached by any
    /// transfer amount.</b> <c>Σ over all replicas of LocalAllowance</c> is invariant under a transfer
    /// for any amount at all: it subtracts <c>amount</c> from <c>from</c>'s formula (the <c>sent</c>
    /// term) and adds the SAME <c>amount</c> to <c>to</c>'s formula (the <c>received</c> term), so the
    /// sum is unchanged algebraically no matter what the amount is or whether <c>from</c> "really has
    /// it". An unchecked transfer would therefore never let the platform's own bound be breached — but it
    /// WOULD let <paramref name="from"/>'s own <see cref="LocalAllowance"/> go negative, which is a
    /// permanent local wrong answer (every future <see cref="TrySpend"/> for that replica refuses, even
    /// once nothing further is asked of it, until a compensating transfer arrives) with no corresponding
    /// upside. Refused here for the same "refuse rather than oversell" reason a spend past allowance
    /// is.</para>
    ///
    /// <para><b>Who writes <c>t:from:to</c>, and why "replica <c>i</c> writes <c>T[i][*]</c>" narrows to
    /// this in a hosted document.</b> The plan's single-writer-per-key argument names replica <c>i</c> as
    /// the writer of its own outbound transfers. In THIS implementation the write happens here, inside
    /// the coordinating document — reached only through <c>CrdtDocGrain.RebalanceAsync</c> /
    /// <c>ICrdtFacade.RebalanceAsync</c>, i.e. while the document is reachable and the call is
    /// serialized by the grain's own single-threaded turn model. That is a NARROWER, not a different,
    /// single-writer discipline: the KEY <c>t:from:to</c> still has exactly one writer for the life of
    /// the document (the grain, always), which is what actually matters for the "LWW never exercised"
    /// property this class's own doc comment states — it is stricter than "replica <c>from</c> writes it
    /// directly" would be, because it additionally rules out two concurrent rebalance calls racing to
    /// increment the SAME key from two different in-memory reads (the grain turn model serializes them;
    /// an edge-writes-directly design would not).</para></summary>
    public static EscrowRebalanceResult TryTransfer(YDoc doc, CrdtEscrowConfig config, string from, string to, long amount)
    {
        if (!config.InitialAllowance.ContainsKey(from))
        {
            return new EscrowRebalanceResult { Ok = false, Reason = $"replica '{from}' is not declared in InitialAllowance" };
        }

        if (!config.InitialAllowance.ContainsKey(to))
        {
            return new EscrowRebalanceResult { Ok = false, Reason = $"replica '{to}' is not declared in InitialAllowance" };
        }

        if (string.Equals(from, to, StringComparison.Ordinal))
        {
            return new EscrowRebalanceResult { Ok = false, Reason = "cannot transfer a replica's allowance to itself" };
        }

        if (amount <= 0)
        {
            return new EscrowRebalanceResult
            {
                Ok = false,
                Reason = "amount to transfer must be positive",
                FromAllowance = LocalAllowance(doc, config, from),
                ToAllowance = LocalAllowance(doc, config, to),
            };
        }

        var fromAllowance = LocalAllowance(doc, config, from);
        if (amount > fromAllowance)
        {
            return new EscrowRebalanceResult
            {
                Ok = false,
                Reason = $"'{from}' holds only {fromAllowance}, cannot transfer {amount} — transfer refused, "
                    + "not partially applied",
                FromAllowance = fromAllowance,
                ToAllowance = LocalAllowance(doc, config, to),
            };
        }

        IncrementLong(CounterMap(doc, config), TransferKey(from, to), amount);

        return new EscrowRebalanceResult
        {
            Ok = true,
            FromAllowance = LocalAllowance(doc, config, from),
            ToAllowance = LocalAllowance(doc, config, to),
        };
    }

    /// <summary>Plan 020 wave F — the ONLY transfer a coordinating document (the grain, an operator
    /// through the rebalance route) may write. Delegates to <see cref="TryTransfer"/> after enforcing the
    /// one rule that makes a coordinator-written transfer sound at all.
    ///
    /// <para><b>Why this restriction exists, demonstrated rather than argued.</b> Moving allowance out of
    /// a replica is safe only when that replica has ALREADY deducted it from its own view. A coordinating
    /// document cannot know what a disconnected replica has spent, so an unrestricted coordinator
    /// transfer breaches the bound the whole feature exists to hold — with every caller behaving
    /// correctly and using only sanctioned APIs:</para>
    /// <list type="number">
    ///   <item><c>a</c> holds 10, goes offline, spends 10 on its own document. Nothing is shared yet.</item>
    ///   <item>The coordinator still sees <c>a</c> holding 10 and transfers 6 of it to <c>b</c>.</item>
    ///   <item><c>b</c> receives the transfer and spends 6.</item>
    ///   <item>Everything converges: 16 spent against a bound of 10.</item>
    /// </list>
    /// <para>That sequence is pinned by <c>EscrowCounterTests</c> as a REFUSAL now, and it is exactly why
    /// plan 020's escrow section says only replica <c>i</c> writes <c>T[i][*]</c>. The reserve
    /// (<see cref="CrdtEscrowConfig.ReserveReplica"/>) is the one replica this cannot happen to, because
    /// <see cref="TrySpend"/> refuses it outright: no spends means no unsynced spends means the
    /// coordinator's view of it is never stale. Transfers between two SPENDING replicas remain available
    /// and remain sound — the giver calls <see cref="TryTransfer"/> on its OWN document, which deducts
    /// before anyone else can use it, and ships the result as an ordinary update.</para></summary>
    /// <summary>Allowance for a replica that may not be declared at all — the reserve-rule refusals above
    /// run BEFORE <see cref="TryTransfer"/>'s own declared-replica checks, so this must not throw.</summary>
    private static long AllowanceOrZero(YDoc doc, CrdtEscrowConfig config, string replica) =>
        config.InitialAllowance.ContainsKey(replica) ? LocalAllowance(doc, config, replica) : 0L;

    public static EscrowRebalanceResult TryCoordinatorTransfer(
        YDoc doc, CrdtEscrowConfig config, string from, string to, long amount)
    {
        if (string.IsNullOrEmpty(config.ReserveReplica))
        {
            return new EscrowRebalanceResult
            {
                Ok = false,
                Reason = "this counter declares no reserveReplica, so there is no replica a coordinator "
                    + "may safely transfer out of — a spending replica must move its own allowance from "
                    + "its own document, because only it knows what it has already spent offline",
            };
        }

        if (!string.Equals(from, config.ReserveReplica, StringComparison.Ordinal))
        {
            return new EscrowRebalanceResult
            {
                Ok = false,
                Reason = $"'{from}' is a spending replica, not this counter's reserve "
                    + $"('{config.ReserveReplica}') — a coordinator cannot transfer allowance out of a "
                    + "replica that may have spent offline without it knowing, which would breach the "
                    + "bound; have that replica transfer its own allowance instead",
                // Real numbers, not the struct default: a refusal reporting 0/0 reads as "this replica
                // holds nothing", which is a different and wrong answer to a different question.
                FromAllowance = AllowanceOrZero(doc, config, from),
                ToAllowance = AllowanceOrZero(doc, config, to),
            };
        }

        return TryTransfer(doc, config, from, to, amount);
    }

    /// <summary>Every declared replica's current state, sorted by name. See
    /// <see cref="EscrowStatus"/>'s own doc comment for why this is computed fresh on every call rather
    /// than cached — the <c>d:</c>/<c>t:</c> keys ARE the state.</summary>
    public static EscrowStatus Status(YDoc doc, CrdtEscrowConfig config)
    {
        var replicas = new List<EscrowReplicaStatus>();
        long bound = 0;
        long totalSpent = 0;

        foreach (var name in config.InitialAllowance.Keys.OrderBy(k => k, StringComparer.Ordinal))
        {
            var initial = config.InitialAllowance[name];
            bound += initial;

            var spent = Spent(doc, config, name);
            totalSpent += spent;

            var allowance = LocalAllowance(doc, config, name);
            replicas.Add(new EscrowReplicaStatus
            {
                Replica = name,
                InitialAllowance = initial,
                Spent = spent,
                LocalAllowance = allowance,
                Exhausted = allowance <= 0,
            });
        }

        return new EscrowStatus { Bound = bound, TotalSpent = totalSpent, Replicas = replicas };
    }
}

/// <summary>What one <see cref="EscrowCounter.TrySpend"/> call did. Not a Contracts/wire type — unlike
/// <see cref="EscrowRebalanceResult"/>, nothing on the server side ever spends on a replica's behalf (see
/// <c>CrdtEscrowConfig</c>'s own class doc: a spend is an ordinary content edit, shipped to the platform
/// as Yjs update bytes through the EXISTING <c>/crdt/updates</c> route exactly like any other edit — it
/// needs no new endpoint of its own), so this type never needs to cross the API boundary and stays where
/// its only caller — an edge, or a test standing in for one — already has Ycs.</summary>
public sealed class EscrowSpendResult
{
    public bool Ok { get; init; }
    public string Replica { get; init; } = "";
    public long Amount { get; init; }

    /// <summary>The true current local allowance: AFTER the spend when <see cref="Ok"/> is true, AS OF
    /// the refusal when it is false — either way, never a stale echo of what was asked for.</summary>
    public long LocalAllowance { get; init; }

    /// <summary>Set exactly when <see cref="Ok"/> is <c>false</c> — plan 020 wave F, limit 2's "must be
    /// visible to the operator, not silent" made concrete at the lowest level this mechanism has: a
    /// refused spend is a value, not an exception and not a bare zero indistinguishable from "nothing was
    /// asked for".</summary>
    public string? Reason { get; init; }

    public static EscrowSpendResult Success(string replica, long amount, long localAllowanceAfter) =>
        new() { Ok = true, Replica = replica, Amount = amount, LocalAllowance = localAllowanceAfter };

    public static EscrowSpendResult Refusal(string replica, long amount, long localAllowance, string reason) =>
        new() { Ok = false, Replica = replica, Amount = amount, LocalAllowance = localAllowance, Reason = reason };
}
