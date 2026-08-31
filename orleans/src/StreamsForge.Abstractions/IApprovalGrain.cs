namespace StreamsForge.Abstractions;

// ============================================================================
// Plan 015 (RBAC → entitlements) W4-B: the Orleans half of the approvals store.
//
// Its own file for the same reason IAccessPolicyGrain.cs is: the Dapr twin (ApprovalActor over a pure
// ApprovalStore) has to agree with this one member for member, and a reviewer comparing the two
// flavours should be able to open one file per side.
// ============================================================================

/// <summary>Singleton (key = <see cref="StreamConstants.ApprovalsKey"/>, storage
/// <see cref="StreamConstants.StorageName"/>), and — unlike the audit log next door — deliberately NOT
/// day-sharded: the pending set is small and is queried WHOLE, both by the inbox and by the escalation
/// sweeper, so sharding it would mean fanning every sweep across shards to find the handful of rows
/// that changed.
///
/// <para>Plan 005's seam rule applies unchanged: every member lives on the runtime-neutral
/// <see cref="IApprovalFacade"/>, so shared/StreamsForge.Api depends on the facade and never on this
/// interface — this one adds <b>nothing at all</b>.</para>
///
/// <para><b>The grain decides no rule.</b> Every transition (file, vote, cancel, record outcome, expire,
/// escalate) goes through AppCore's pure <c>ApprovalStateMachine</c>, which is the single implementation
/// both flavours share. The grain's own job is exactly three things the state machine deliberately
/// cannot do: hold the list, mint an id and a clock, and answer "is this voter in an approver group?"
/// by reading the access-policy singleton — see <c>ApprovalGrain</c> for why that lookup belongs on this
/// side of the seam.</para></summary>
public interface IApprovalGrain : IApprovalFacade, IGrainWithStringKey
{
}
