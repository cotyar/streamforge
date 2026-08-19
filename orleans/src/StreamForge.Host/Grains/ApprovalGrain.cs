using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using StreamForge.Abstractions;
using StreamForge.AppCore.Access;

namespace StreamForge.Host.Grains;

/// <summary>Everything the approvals singleton persists: one list. Not a dictionary, because every
/// caller either wants one id (a linear scan of a list the plan calls "small") or wants the whole set
/// in order (the inbox, the sweeper), and a dictionary would serialize the id twice for the privilege
/// of losing the order.</summary>
public sealed class ApprovalGrainState
{
    /// <summary>Filing order, oldest first. The list is the audit-adjacent record of what was asked
    /// for, so nothing ever reorders it in place; <c>ListAsync</c> reverses a copy.</summary>
    public List<ApprovalRequest> Requests { get; set; } = [];
}

/// <summary>
/// Plan 015 W4-B — the approvals store (key = <see cref="StreamConstants.ApprovalsKey"/>), persisted in
/// the same "definitions" store as the catalog, the user store and the access policy.
///
/// <para><b>THE STORE STORES; THE STATE MACHINE DECIDES.</b> Not one rule about who may vote, when a
/// request expires, what counts as enough, or how escalation works lives in this file — every
/// transition goes through AppCore's pure <see cref="ApprovalStateMachine"/>: <c>RequestAsync</c> →
/// <see cref="ApprovalStateMachine.CreateRequest"/>, <c>VoteAsync</c> →
/// <see cref="ApprovalStateMachine.ApplyVote"/>, <c>CancelAsync</c> →
/// <see cref="ApprovalStateMachine.Cancel"/>, <c>RecordOutcomeAsync</c> →
/// <see cref="ApprovalStateMachine.RecordOutcome"/>, <c>SweepAsync</c> →
/// <see cref="ApprovalStateMachine.Sweep"/>. Wave 3 of this plan produced a three-way divergence
/// between three agents implementing "the same" rule on three transports; the state machine exists so
/// that cannot happen again, and the way it stops happening is that this file never reaches for a
/// second opinion.</para>
///
/// <para><b>Persist on StateChanged, not on Accepted.</b> A refused vote can still move the request —
/// a vote arriving past the deadline expires it right there rather than trusting the sweeper to have
/// ticked — so every write below is gated on <see cref="ApprovalVoteResult.StateChanged"/>. Gating on
/// <see cref="ApprovalVoteResult.Accepted"/> instead would lose exactly that expiry, and lose it
/// silently: the in-memory copy would be Expired and the stored one Pending until something else
/// wrote.</para>
///
/// <para><b>Where eligibility comes from, and why here.</b>
/// <see cref="IApprovalFacade.VoteAsync"/> is frozen and takes no eligibility argument, so the caller
/// physically cannot supply one — but that is the smaller half of the argument. The larger half is that
/// a store which took eligibility from its caller would be a store whose second-pair-of-eyes rule is
/// enforced once per transport: REST, gRPC and the chat would each decide "is this voter an approver?"
/// for themselves, which is the wave-3 divergence rerun on the one rule that has to hold. So this grain
/// asks the access-policy singleton itself, on every vote, and a caller cannot reach the state machine
/// any other way. The cost is one grain call per vote (a vote is a human clicking a button, not a hot
/// path) and one dependency edge, approvals → access policy, which does not cycle: the policy grain
/// calls nothing.</para>
///
/// <para>ponytail: nothing ever removes a decided request, so the document grows for the life of the
/// deployment. The ceiling is real but distant — an approval is a human decision, so a busy year is
/// thousands of rows of a few hundred bytes, not millions — and the alternatives all pick a retention
/// policy nobody has been asked for yet (and the audit log, which DOES have one, is where the permanent
/// record belongs). Upgrade path: drop terminal requests older than <c>Approvals:RetainDays</c> inside
/// <see cref="SweepAsync"/>, which already walks the whole list once per tick and already writes once
/// per sweep.</para>
///
/// <para>The Dapr twin (<c>ApprovalActor</c> over a pure <c>ApprovalStore</c>) implements the same
/// <see cref="IApprovalFacade"/> with the same semantics, member for member. Because both sides call
/// the same state machine, "the same semantics" is mostly automatic — what each side still owns is the
/// four things below: the list, the id, the clock, and the eligibility lookup.</para>
/// </summary>
public sealed class ApprovalGrain(
    [PersistentState("approvals", StreamConstants.StorageName)] IPersistentState<ApprovalGrainState> state,
    ILogger<ApprovalGrain> logger)
    : Grain, IApprovalGrain
{
    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>The access-policy singleton, which owns both halves of what this grain cannot decide:
    /// the approval TEMPLATES (which action needs how many of whose eyes) and the GROUP MEMBERSHIP that
    /// says whether a voter is one of those eyes.</summary>
    private IAccessPolicyGrain Policy => GrainFactory.GetGrain<IAccessPolicyGrain>(StreamConstants.AccessKey);

    private ApprovalRequest? Find(string id) =>
        state.State.Requests.FirstOrDefault(r => string.Equals(r.Id, id, StringComparison.Ordinal));

    // ------------------------------------------------------------------------------------------
    // Filing
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// File a request, or refuse to.
    ///
    /// <para>The template is selected here (the state machine's <c>SelectTemplate</c>, over the policy
    /// document's list, in document order) and then snapshotted onto the request by
    /// <see cref="ApprovalStateMachine.CreateRequest"/>, which is a WHITELIST of six descriptive fields
    /// — so a caller sending a populated <c>Votes</c> list, a <c>State</c> of Approved or a
    /// <c>RequiredApprovals</c> of zero gets none of them. That property is the reason nothing here
    /// copies the draft.</para>
    ///
    /// <para><b>No matching template throws rather than files.</b> The state machine documents that a
    /// null template means no approval is required and that filing anyway would create a request with
    /// no approver group — one nobody can approve, which can only rot until it expires. The two callers
    /// both do the right thing with a throw: the chat filer catches it and tells the model nothing was
    /// filed (which is true), and a REST route surfaces it as a 4xx naming the action. Returning a
    /// half-request would be the lie.</para>
    ///
    /// <para><b>Identity comes from the draft's <see cref="ApprovalRequest.RequestedBy"/></b>, because
    /// this seam has no other channel for it — the facade takes one argument. That is safe here in a way
    /// it would not be inside the state machine: every caller of this facade has already authenticated
    /// the principal it stamps (<c>AccessGuard</c> for REST/gRPC, the chat filer's <c>OnBehalfOf</c>
    /// human), and the state machine still refuses an empty one. The rule that actually matters —
    /// nobody votes on their own request — is enforced against this same string, so a caller that lies
    /// about who filed a request only succeeds in disqualifying someone else from voting on it.</para>
    ///
    /// <para>ponytail: <c>SelectTemplate</c> is called with <c>null</c> resource tags, so a
    /// <c>tag:finance</c> <see cref="ApprovalTemplate.ScopePattern"/> never matches through this seam.
    /// The ceiling is a tag-scoped template that silently never fires; the reason is that
    /// <see cref="ApprovalRequest"/> is frozen and carries no tags, and the alternative — this grain
    /// calling the registry to look up an entity by scope on every filing — puts a catalog read and a
    /// second dependency edge behind a store. Upgrade path: an additive <c>[Id(17)] Tags</c> on the
    /// request, filled by the guard that already HAS the definition in hand when it decides
    /// RequiresApproval, and passed straight through here.</para>
    /// </summary>
    public async Task<ApprovalRequest> RequestAsync(ApprovalRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var policy = await Policy.GetPolicyAsync();
        var template = ApprovalStateMachine.SelectTemplate(
            policy.ApprovalTemplates,
            request.Action,
            request.Scope,
            resourceTags: null);

        if (template is null)
        {
            throw new InvalidOperationException(
                $"no enabled approval template covers '{request.Action}' at scope '{request.Scope}', so there is nobody to approve it and nothing was filed");
        }

        var stored = ApprovalStateMachine.CreateRequest(
            request,
            template,
            Guid.NewGuid().ToString("n"),
            request.RequestedBy,
            NowMs());

        state.State.Requests.Add(stored);
        await state.WriteStateAsync();

        logger.LogInformation(
            "Approval {Id} filed by '{RequestedBy}' for {Action} on {Scope} via {Origin} (template '{Template}', {Required} approval(s) from {Groups}).",
            stored.Id,
            stored.RequestedBy,
            stored.Action,
            stored.Scope,
            stored.Origin,
            stored.TemplateName,
            stored.RequiredApprovals,
            string.Join(", ", stored.ApproverGroups));

        return stored;
    }

    // ------------------------------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------------------------------

    public Task<ApprovalRequest?> GetAsync(string id) => Task.FromResult(Find(id));

    /// <summary>Newest first, because every caller is an inbox. <paramref name="limit"/> &lt;= 0 means
    /// all — the same convention <see cref="ITableShardFacade.GetKeysAsync"/> and friends already
    /// use.</summary>
    public Task<List<ApprovalRequest>> ListAsync(ApprovalState? stateFilter, int limit)
    {
        IEnumerable<ApprovalRequest> rows = state.State.Requests;
        if (stateFilter is not null)
        {
            rows = rows.Where(r => r.State == stateFilter.Value);
        }

        rows = rows.Reverse();
        if (limit > 0)
        {
            rows = rows.Take(limit);
        }

        return Task.FromResult(rows.ToList());
    }

    // ------------------------------------------------------------------------------------------
    // Transitions
    // ------------------------------------------------------------------------------------------

    /// <summary>One vote, decided entirely by <see cref="ApprovalStateMachine.ApplyVote"/>.
    ///
    /// <para>Returns the request — the facade's shape — so a refusal is visible to the caller as "the
    /// state and the vote list did not change" rather than as a sentence. The sentence is not thrown
    /// away: it is logged here, which is where an operator asking "why did my approval not count?"
    /// finds it. (Wave 5's REST layer can compare the returned request against what it sent; the
    /// contract is frozen and this wave does not widen it.)</para>
    ///
    /// <para>An unknown id returns null, and that is a genuinely different answer from a refused vote —
    /// one is "no such request", the other is "that request, unchanged".</para></summary>
    public async Task<ApprovalRequest?> VoteAsync(string id, ApprovalVote vote)
    {
        ArgumentNullException.ThrowIfNull(vote);

        var request = Find(id);
        if (request is null)
        {
            return null;
        }

        // Resolved BEFORE the state machine call and never inside it: NotAnApprover is the default, so
        // every path that fails to establish eligibility lands on the refusing value.
        var eligibility = await ResolveEligibilityAsync(request, vote.Username);
        var result = ApprovalStateMachine.ApplyVote(request, vote, eligibility, NowMs());

        await PersistIfChangedAsync(result, request, vote.Username);
        return request;
    }

    public async Task<ApprovalRequest?> CancelAsync(string id, string username)
    {
        var request = Find(id);
        if (request is null)
        {
            return null;
        }

        var result = ApprovalStateMachine.Cancel(request, username, NowMs());
        await PersistIfChangedAsync(result, request, username);
        return request;
    }

    public async Task<ApprovalRequest?> RecordOutcomeAsync(string id, bool executed, string outcome)
    {
        var request = Find(id);
        if (request is null)
        {
            return null;
        }

        var result = ApprovalStateMachine.RecordOutcome(request, executed, outcome, NowMs());
        await PersistIfChangedAsync(result, request, "system");
        return request;
    }

    /// <summary>Expiry and escalation for the whole pending set, driven by the shared hosted sweeper
    /// (<c>ApprovalSweeperService</c>) rather than by a grain timer, because the Dapr compose stack has
    /// no scheduler and one shape has to work on both flavours.
    ///
    /// <para>The templates come from the access policy — escalation groups are the one thing the state
    /// machine deliberately re-reads after filing, so a deleted template means "no escalation" while
    /// expiry (snapshotted onto the request at filing time) still works.</para>
    ///
    /// <para><b>One write per sweep, not one per change.</b> The whole document is a single state
    /// object, so N changes cost exactly one <c>WriteStateAsync</c>; a write per change would rewrite
    /// the same document N times to reach the same bytes. A sweep that changed nothing writes
    /// nothing.</para></summary>
    public async Task<int> SweepAsync(long nowMs)
    {
        var policy = await Policy.GetPolicyAsync();
        var changes = ApprovalStateMachine.Sweep(state.State.Requests, policy.ApprovalTemplates, nowMs);

        if (changes.Count == 0)
        {
            return 0;
        }

        await state.WriteStateAsync();

        foreach (var change in changes)
        {
            logger.LogInformation(
                "Approval {Id} ({Action} on {Scope}) {What}.",
                change.Request.Id,
                change.Request.Action,
                change.Request.Scope,
                change.Action == ApprovalSweepAction.Expired
                    ? "expired"
                    : $"escalated to {string.Join(", ", change.Request.ApproverGroups)}");
        }

        return changes.Count;
    }

    // ------------------------------------------------------------------------------------------
    // The two things the state machine cannot do
    // ------------------------------------------------------------------------------------------

    /// <summary>Is this voter in one of the request's approver groups?
    ///
    /// <para>Matched against the request's OWN <see cref="ApprovalRequest.ApproverGroups"/> — the
    /// snapshot taken at filing time, widened by escalation — so editing a template cannot retroactively
    /// change who was allowed to vote on a request that is already open, while an escalation deliberately
    /// can.</para>
    ///
    /// <para><b>Membership comes from AppCore's <see cref="EffectivePermissionsBuilder"/>, not from a
    /// local walk of <c>policy.Groups</c>.</b> That is not a stylistic preference: the builder is where
    /// "which groups is this user in" is already defined for the whole platform, and it carries two rules
    /// a hand-rolled loop silently would not. A <b>disabled</b> user is in no groups at all, so disabling
    /// an account stops that person approving through the very same mechanism that kills their token —
    /// otherwise a disabled approver could still cast the deciding vote, which is the opposite of what
    /// disabling means. And group membership derived from an IdP's <c>groups</c> claim
    /// (<see cref="GroupDefinition.ExternalClaimValues"/>) is understood by the same code, so when OIDC
    /// lands this method does not become the one place that never heard of it. The Dapr twin resolves
    /// eligibility through the same builder — checked, not assumed, because a rule that differs by
    /// flavour is precisely the divergence this wave's state machine exists to prevent.</para>
    ///
    /// <para>Ordinal on the group name, matching every other username and entity-name comparison in the
    /// repo. The one deliberately case-INSENSITIVE comparison in this workflow — requester vs voter —
    /// lives in the state machine and is explained there.</para>
    ///
    /// <para>ponytail: the builder is called with no <c>groupClaimValues</c>, because a store holds no
    /// principal — only the resolver does. Ceiling: a voter whose ONLY membership is claim-derived is
    /// refused, which is the closed direction and moot until OIDC ships. Upgrade path: the same shape the
    /// state machine already uses — eligibility decided by whoever holds the principal — which means
    /// widening the facade, not this method.</para>
    ///
    /// <para>A policy read that throws is not caught: a vote that could not be checked must not be
    /// counted, and turning the failure into <see cref="VoterEligibility.NotAnApprover"/> would record
    /// "you are not an approver" against a store outage.</para></summary>
    private async Task<VoterEligibility> ResolveEligibilityAsync(ApprovalRequest request, string username)
    {
        if (string.IsNullOrWhiteSpace(username) || request.ApproverGroups.Count == 0)
        {
            return VoterEligibility.NotAnApprover;
        }

        var policy = await Policy.GetPolicyAsync();
        var groups = EffectivePermissionsBuilder.Build(policy, username).Groups;

        return request.ApproverGroups.Any(g => groups.Contains(g, StringComparer.Ordinal))
            ? VoterEligibility.Eligible
            : VoterEligibility.NotAnApprover;
    }

    /// <summary>The single write gate. <see cref="ApprovalVoteResult.StateChanged"/> — never
    /// <see cref="ApprovalVoteResult.Accepted"/>: see the type remarks.</summary>
    private async Task PersistIfChangedAsync(ApprovalVoteResult result, ApprovalRequest request, string actor)
    {
        if (result.StateChanged)
        {
            await state.WriteStateAsync();
        }

        if (result.Accepted)
        {
            logger.LogInformation("Approval {Id}: {Reason} (by '{Actor}').", request.Id, result.Reason, actor);
        }
        else
        {
            // Every refusal, at Information: "why did my approval not count" is an operator question
            // asked after the fact, and a refused vote is rare by construction.
            logger.LogInformation("Approval {Id}: refused — {Reason}.", request.Id, result.Reason);
        }
    }
}
