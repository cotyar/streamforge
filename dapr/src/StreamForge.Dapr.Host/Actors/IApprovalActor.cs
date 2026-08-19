using Dapr.Actors;
using StreamForge.Abstractions;

namespace StreamForge.Dapr.Host.Actors;

// Plan 015 W4-C. Request payloads for the multi-argument IApprovalFacade members: a Dapr actor method
// takes 0 or 1 parameters (unlike an Orleans grain method), so every multi-argument call is wrapped in a
// record here — the same mechanism, and the same reason, as IAccessPolicyActor's Upsert*ActorRequest and
// IUserStoreActor's ValidateCredentialsRequest.

public sealed record ApprovalListActorRequest(ApprovalState? State, int Limit);

public sealed record ApprovalVoteActorRequest(string Id, ApprovalVote Vote);

public sealed record ApprovalCancelActorRequest(string Id, string Username);

public sealed record ApprovalOutcomeActorRequest(string Id, bool Executed, string Outcome);

/// <summary>
/// Actor-invocation surface for the approvals singleton (id =
/// <see cref="StreamConstants.ApprovalsKey"/>, "approvals") — the Dapr counterpart of Orleans'
/// <c>IApprovalGrain</c>, and the thing <see cref="Facades.DaprApprovalFacade"/> adapts
/// <see cref="IApprovalFacade"/> onto.
///
/// <para><b>One singleton, deliberately not day-sharded</b> (unlike the audit log next door): the pending
/// set is small and is queried WHOLE, both by the inbox and by the escalation sweeper — see
/// <see cref="StreamConstants.ApprovalsKey"/>'s own doc comment. Sharding it would turn every sweep into
/// a fan-out over shards that mostly hold nothing.</para>
///
/// <para><b>Null means the transition did not happen</b> on every <c>ApprovalRequest?</c> member here,
/// which covers both "no such id" and "the state machine refused". That is wave 1's convention on
/// <see cref="IAccessPolicyActor"/> — "a refused mutation is a null or a false" — and the frozen
/// <see cref="IApprovalFacade"/> has no channel for a reason string anyway; the state machine's sentence
/// is logged by <see cref="ApprovalActor"/> instead. <see cref="RequestAsync"/> is the one exception,
/// because filing can fail for a reason an operator must actually SEE (no template covers the action),
/// so it carries an <see cref="ActorResult{T}"/> the facade re-throws — the established
/// <see cref="RegistryActor"/> pattern.</para>
/// </summary>
public interface IApprovalActor : IActor
{
    /// <summary>Files a request through <c>ApprovalStateMachine.CreateRequest</c>, which discards
    /// everything on the draft that decides the outcome. Fails (with a sentence) when no enabled template
    /// covers the action/scope, or when the draft names no requester.</summary>
    Task<ActorResult<ApprovalRequest>> RequestAsync(ApprovalRequest draft);

    Task<ApprovalRequest?> GetAsync(string id);

    Task<List<ApprovalRequest>> ListAsync(ApprovalListActorRequest request);

    /// <summary>One vote. The voter's eligibility is resolved HERE, from the access-policy singleton —
    /// never taken from the caller; see <see cref="ApprovalActor"/>'s class doc.</summary>
    Task<ApprovalRequest?> VoteAsync(ApprovalVoteActorRequest request);

    Task<ApprovalRequest?> CancelAsync(ApprovalCancelActorRequest request);

    Task<ApprovalRequest?> RecordOutcomeAsync(ApprovalOutcomeActorRequest request);

    /// <summary>Expiry + escalation for the whole set at <c>nowMs</c>; returns how many requests changed
    /// state. Driven by the shared hosted <c>ApprovalSweeperService</c>, because the Dapr compose stack
    /// runs with no scheduler and reminders are therefore off the table.</summary>
    Task<int> SweepAsync(long nowMs);
}
