using Dapr.Actors;
using Dapr.Actors.Client;
using Dapr.Actors.Runtime;
using StreamForge.Abstractions;
using StreamForge.AppCore.Access;
using StreamForge.Dapr.Host.Access;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 015 W4-C: Dapr counterpart of Orleans' <c>ApprovalGrain</c> — singleton actor, id =
/// <see cref="StreamConstants.ApprovalsKey"/> ("approvals"), one state entry of the same name holding
/// every request.
///
/// <para><b>Thin by design.</b> Every transition lives in <see cref="ApprovalStateMachine"/> (AppCore,
/// pure, shared with Orleans) and the find/list/dirty bookkeeping in <see cref="ApprovalStore"/> (plain
/// class, no Dapr). This actor contributes the three things neither can: load and save, mint the id and
/// read the clock, and answer "is this voter an approver?".</para>
///
/// <para><b>How eligibility is obtained, and why.</b> It is resolved here, from the access-policy
/// singleton, and never taken from the caller. Two reasons, either sufficient. First, there is no seam:
/// <see cref="IApprovalFacade.VoteAsync"/> is frozen at <c>(id, vote)</c>, so a caller could not pass an
/// eligibility even if it should. Second, and the reason the seam should not be added later: a transport
/// asserting "this voter is an approver" would make the store trust its input for the single rule the
/// whole feature exists to enforce, and every transport — REST, gRPC, chat, CLI, the sweeper — would
/// re-derive it. That is precisely the shape that produced wave 3's three-way scope divergence. One
/// policy read per vote is the price, and a vote is a human clicking a button a handful of times a day,
/// not a hot path. The computation itself is <see cref="ApprovalStore.EligibilityFor"/>, a pure static
/// over <c>EffectivePermissionsBuilder</c> — so the rule is unit-tested without a sidecar even though
/// the fetch is not.</para>
///
/// <para><b>Persist when the document changed, not when the vote was accepted</b> — see
/// <see cref="ApprovalMutation.Dirty"/>. A vote refused because the request is past its deadline still
/// expires the request, and losing that write would let a late approval land on the next attempt.</para>
///
/// <para><b>Reentrancy:</b> this actor calls exactly one other actor (the access-policy singleton, for
/// templates and group membership) and nothing calls back into it, so there is no cycle and the default
/// turn-based concurrency is correct.</para>
/// </summary>
public sealed class ApprovalActor(ActorHost host, ILogger<ApprovalActor> logger) : Actor(host), IApprovalActor
{
    private const string StateName = StreamConstants.ApprovalsKey;

    private ApprovalDocument _document = new();
    private ApprovalStore _store = null!;

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<ApprovalDocument>(StateName);
        _document = existing.HasValue ? existing.Value : new ApprovalDocument();
        _store = new ApprovalStore(_document);
    }

    private Task SaveAsync() => StateManager.SetStateAsync(StateName, _document);

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>The access-policy singleton: templates for filing and sweeping, group membership for
    /// eligibility. Resolved per call rather than cached in a field because it is called rarely and a
    /// cached proxy buys nothing measurable.</summary>
    private static Task<AccessPolicyDocument> PolicyAsync() =>
        ActorProxy.Create<IAccessPolicyActor>(
            new ActorId(StreamConstants.AccessKey), nameof(AccessPolicyActor), ActorProxyDefaults.Options)
            .GetPolicyAsync();

    public async Task<ActorResult<ApprovalRequest>> RequestAsync(ApprovalRequest draft)
    {
        try
        {
            var policy = await PolicyAsync();
            var stored = _store.Create(draft, policy.ApprovalTemplates, Guid.NewGuid().ToString("n"), NowMs());
            await SaveAsync();
            logger.LogInformation(
                "Approval {ApprovalId} filed by {RequestedBy} for {Action} on {Scope} under template {Template}.",
                stored.Id, stored.RequestedBy, stored.Action, stored.Scope, stored.TemplateName);
            return ActorResult<ApprovalRequest>.Success(stored);
        }
        catch (InvalidOperationException ex)
        {
            // The refusals ApprovalStore.Create raises deliberately (no template, no requester) — a
            // reason an operator has to see, so it crosses the boundary as a result rather than as an
            // exception the SDK would reshape. See ActorResult<T>'s class doc.
            return ActorResult<ApprovalRequest>.Failure(ex.Message);
        }
    }

    public Task<ApprovalRequest?> GetAsync(string id) => Task.FromResult(_store.Get(id));

    public Task<List<ApprovalRequest>> ListAsync(ApprovalListActorRequest request) =>
        Task.FromResult(_store.List(request.State, request.Limit));

    public async Task<ApprovalRequest?> VoteAsync(ApprovalVoteActorRequest request)
    {
        var stored = _store.Get(request.Id);
        if (stored is null)
        {
            return null;
        }

        var eligibility = ApprovalStore.EligibilityFor(await PolicyAsync(), stored, request.Vote?.Username);
        return await ApplyAsync(_store.Vote(request.Id, request.Vote!, eligibility, NowMs()));
    }

    public Task<ApprovalRequest?> CancelAsync(ApprovalCancelActorRequest request) =>
        ApplyAsync(_store.Cancel(request.Id, request.Username, NowMs()));

    public Task<ApprovalRequest?> RecordOutcomeAsync(ApprovalOutcomeActorRequest request) =>
        ApplyAsync(_store.RecordOutcome(request.Id, request.Executed, request.Outcome, NowMs()));

    public async Task<int> SweepAsync(long nowMs)
    {
        var policy = await PolicyAsync();
        var changed = _store.Sweep(policy.ApprovalTemplates, nowMs);
        if (changed > 0)
        {
            // Once per sweep, not once per change: the whole document is one state entry, so N writes
            // would be N copies of the same bytes.
            await SaveAsync();
        }

        return changed;
    }

    /// <summary>Persist if the document changed, log the state machine's sentence, and answer with the
    /// request only if the transition actually happened — the null-means-refused convention on
    /// <see cref="IApprovalActor"/>. The log line is the only place the reason survives, because the
    /// frozen facade returns no room for it.</summary>
    private async Task<ApprovalRequest?> ApplyAsync(ApprovalMutation mutation)
    {
        if (mutation.Result is null)
        {
            return null;
        }

        if (mutation.Dirty)
        {
            await SaveAsync();
        }

        if (mutation.Result.Accepted)
        {
            logger.LogInformation("Approval transition: {Reason}", mutation.Result.Reason);
        }
        else
        {
            logger.LogWarning("Approval transition refused: {Reason}", mutation.Result.Reason);
        }

        return mutation.Applied;
    }
}
