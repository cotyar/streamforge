using StreamForge.Abstractions;
using StreamForge.AppCore.Access;

namespace StreamForge.Dapr.Host.Access;

/// <summary>What the approvals singleton persists. A wrapper around one list rather than a bare
/// <c>List&lt;ApprovalRequest&gt;</c> so a later counter (see the ponytail note on
/// <see cref="ApprovalStore"/> about unbounded growth) is an additive property rather than a state
/// migration.</summary>
public sealed class ApprovalDocument
{
    public List<ApprovalRequest> Requests { get; set; } = [];
}

/// <summary>The result of one attempted transition, plus the two bits the actor needs from it.
///
/// <para><see cref="Dirty"/> is <c>Accepted || StateChanged</c>, not just <c>StateChanged</c>, and both
/// halves matter. An accepted vote that leaves the request Pending still added a row to
/// <see cref="ApprovalRequest.Votes"/> and must be persisted; a REFUSED vote on a past-deadline request
/// still moved the request to <see cref="ApprovalState.Expired"/> and must ALSO be persisted (see
/// <see cref="ApprovalStateMachine.ApplyVote"/>'s note on why <see cref="ApprovalVoteResult.StateChanged"/>
/// exists separately from <see cref="ApprovalVoteResult.Accepted"/>). Persisting on only one of the two
/// loses either every partial vote or every vote-time expiry.</para></summary>
/// <param name="Request">The stored request, or null when no request has that id.</param>
/// <param name="Result">What the state machine said, or null when there was nothing to say it about.</param>
public sealed record ApprovalMutation(ApprovalRequest? Request, ApprovalVoteResult? Result)
{
    public static readonly ApprovalMutation NotFound = new(null, null);

    /// <summary>Whether the document changed and has to be written back.</summary>
    public bool Dirty => Result is not null && (Result.Accepted || Result.StateChanged);

    /// <summary>The request if the transition actually happened, else null — the shape
    /// <see cref="IApprovalFacade"/>'s <c>ApprovalRequest?</c> members return. Wave 1 pinned the
    /// convention for this flavour on <c>IAccessPolicyActor</c>: "a refused mutation is a null or a
    /// false". Null therefore means "the transition did not happen", covering both "no such request" and
    /// "the state machine refused"; the refusal's sentence is logged by the actor, because the frozen
    /// facade has nowhere to carry it.</summary>
    public ApprovalRequest? Applied => Result is { Accepted: true } ? Request : null;
}

/// <summary>
/// Plan 015 W4-C: actor-framework-free approval storage behind <see cref="Actors.ApprovalActor"/> — the
/// same split <see cref="AccessPolicyStore"/> has behind <see cref="Actors.AccessPolicyActor"/>, and for
/// the same reason: a plain class over an in-memory <see cref="ApprovalDocument"/> is unit-testable
/// without a Dapr sidecar, an actor runtime or Redis (dapr/tests/StreamForge.Dapr.Tests/ApprovalStoreTests.cs).
///
/// <para><b>This class decides nothing.</b> Every transition — who may vote, when a request expires, how
/// escalation works, what counts — is <see cref="ApprovalStateMachine"/>'s, a pure type in AppCore that
/// BOTH flavours' stores call. That is not tidiness: wave 3 produced a three-way divergence between three
/// agents implementing "the same" rule on three transports, and an approval that counted a vote on Orleans
/// and refused it on Dapr would be a security bug no single-flavour suite could see. What is left here is
/// find-by-id, the list, and knowing when to write.</para>
///
/// <para><b>The clock is a parameter, never <c>DateTimeOffset.UtcNow</c>.</b> Unlike
/// <see cref="AccessPolicyStore"/> — whose timestamps are decoration — every interesting rule here is a
/// comparison against a deadline, so a store that read the clock itself could not be tested for expiry at
/// all without sleeping.</para>
///
/// <para>ponytail: terminal requests are never pruned, so the singleton's state grows without bound —
/// one Redis value holding every approval ever filed. Ceiling: a deployment that files thousands of
/// requests a day eventually pays for all of them on every activation. Upgrade path: day-shard the
/// terminal ones exactly as the audit log is sharded (<c>StreamConstants.AuditKeyFor</c>) and keep only
/// the pending set here, which is what the key's own doc comment already assumes is small. Not built,
/// because it is a second storage layout in exchange for a problem no deployment has yet.</para>
/// </summary>
public sealed class ApprovalStore(ApprovalDocument document)
{
    /// <summary>Default page size for a <see cref="List"/> call that asks for no limit. The pending set
    /// is small by design (see <see cref="StreamConstants.ApprovalsKey"/>); a caller that genuinely wants
    /// everything passes a big number.</summary>
    public const int DefaultPageSize = 100;

    /// <summary>The live document — the actor persists THIS object, exactly as
    /// <see cref="AccessPolicyStore.Document"/> does.</summary>
    public ApprovalDocument Document => document;

    public ApprovalRequest? Get(string id) =>
        string.IsNullOrWhiteSpace(id)
            ? null
            : document.Requests.FirstOrDefault(r => string.Equals(r.Id, id, StringComparison.Ordinal));

    /// <summary>Newest first, optionally filtered by state — the inbox's order, and the order in which a
    /// truncated page is the useful half.</summary>
    public List<ApprovalRequest> List(ApprovalState? state, int limit) =>
        document.Requests
            .Where(r => state is null || r.State == state)
            .OrderByDescending(r => r.RequestedAtMs)
            .Take(limit <= 0 ? DefaultPageSize : limit)
            .ToList();

    /// <summary>
    /// File a request: pick the template, then hand everything to
    /// <see cref="ApprovalStateMachine.CreateRequest"/>, which is where the whitelist that stops a caller
    /// pre-approving its own request lives.
    ///
    /// <para><b>Identity comes off the draft here, and it has to.</b>
    /// <see cref="IApprovalFacade.RequestAsync"/> is frozen at one parameter, so
    /// <see cref="ApprovalRequest.RequestedBy"/> IS the only channel the authenticated principal can
    /// arrive through — the state machine's "identity comes from the parameter, never from the draft" is
    /// satisfied one layer up, by the transport overwriting <c>RequestedBy</c> with its own principal
    /// before calling (wave 3's chat filer already does exactly that). An empty one is refused rather
    /// than defaulted: an unattributed request makes the self-vote rule vacuous.</para>
    ///
    /// <para>Throws <see cref="InvalidOperationException"/> rather than returning null, the same
    /// convention <see cref="Catalog.CatalogStore"/> uses for a refused mutation with a reason: the actor
    /// catches it into an <see cref="Actors.ActorResult{T}"/> and the facade re-throws it client-side, so
    /// the shared endpoints' existing <c>catch (InvalidOperationException)</c> → 409 pathway carries the
    /// sentence to the caller unchanged.</para>
    /// </summary>
    /// <param name="templates">The policy document's templates, in document order. The actor reads them
    /// from the access-policy singleton; they are a parameter here so this class stays pure.</param>
    /// <param name="id">The minted request id — <c>Guid.NewGuid()</c> is the actor's, not this class's.</param>
    /// <param name="nowMs">Unix ms.</param>
    public ApprovalRequest Create(ApprovalRequest draft, IReadOnlyList<ApprovalTemplate> templates, string id, long nowMs)
    {
        ArgumentNullException.ThrowIfNull(draft);

        if (string.IsNullOrWhiteSpace(draft.RequestedBy))
        {
            throw new InvalidOperationException(
                "an approval request must name the principal that filed it; RequestedBy was empty");
        }

        // ponytail: no resource tags. ApprovalRequest carries Action and Scope and nothing else about
        // the resource (AccessModels.cs is frozen), so a `tag:finance` ScopePattern cannot match at
        // filing time and such a template is a control that silently never fires. Passing null is the
        // decision the state machine's positional parameter demands somebody type, and it is the only
        // value this seam CAN pass. Ceiling: tag-scoped approval templates are inert. Upgrade path is
        // additive — [Id(17)] Tags on ApprovalRequest, filled by the guard that already knows them when
        // it decides RequiresApproval, and one argument here.
        var template = ApprovalStateMachine.SelectTemplate(templates, draft.Action, draft.Scope, null)
            ?? throw new InvalidOperationException(
                $"no enabled approval template covers '{draft.Action}' at scope '{draft.Scope}' — "
                + "nothing to approve, so nothing was filed");

        var stored = ApprovalStateMachine.CreateRequest(draft, template, id, draft.RequestedBy, nowMs);
        document.Requests.Add(stored);
        return stored;
    }

    /// <summary>One vote, straight through <see cref="ApprovalStateMachine.ApplyVote"/>. The store adds
    /// the lookup and <see cref="ApprovalMutation.Dirty"/>; it adds no rule.</summary>
    public ApprovalMutation Vote(string id, ApprovalVote vote, VoterEligibility eligibility, long nowMs)
    {
        var request = Get(id);
        return request is null
            ? ApprovalMutation.NotFound
            : new ApprovalMutation(request, ApprovalStateMachine.ApplyVote(request, vote, eligibility, nowMs));
    }

    public ApprovalMutation Cancel(string id, string username, long nowMs)
    {
        var request = Get(id);
        return request is null
            ? ApprovalMutation.NotFound
            : new ApprovalMutation(request, ApprovalStateMachine.Cancel(request, username, nowMs));
    }

    public ApprovalMutation RecordOutcome(string id, bool executed, string outcome, long nowMs)
    {
        var request = Get(id);
        return request is null
            ? ApprovalMutation.NotFound
            : new ApprovalMutation(request, ApprovalStateMachine.RecordOutcome(request, executed, outcome, nowMs));
    }

    /// <summary>Expiry + escalation over the whole set, and how many requests changed state — the number
    /// <see cref="IApprovalFacade.SweepAsync"/> returns. The state machine has already mutated the
    /// changed requests in place by the time this returns, so the caller persists ONCE for the whole
    /// sweep rather than once per change.</summary>
    public int Sweep(IReadOnlyList<ApprovalTemplate> templates, long nowMs) =>
        ApprovalStateMachine.Sweep(document.Requests, templates, nowMs).Count;

    /// <summary>
    /// Is <paramref name="username"/> in one of <paramref name="request"/>'s approver groups?
    ///
    /// <para><b>Resolved from the policy document, never taken from the caller</b> — see
    /// <see cref="Actors.ApprovalActor"/>'s class doc for the argument. Group membership is
    /// <see cref="EffectivePermissionsBuilder"/>'s answer, not a second one written here: it is the one
    /// implementation of "which groups is this user in" that both flavours already share, it folds in
    /// <see cref="GroupDefinition.ExternalClaimValues"/> for the day OIDC lands, and it returns NO groups
    /// for a disabled user — so a disabled approver stops being an approver for free, through the same
    /// mechanism that kills their token.</para>
    ///
    /// <para>Every miss lands on <see cref="VoterEligibility.NotAnApprover"/>, including the two cases
    /// worth naming: an empty <see cref="ApprovalRequest.ApproverGroups"/> (a template that named no
    /// approver group produces a request nobody can approve — fail-closed, and it can only rot until it
    /// expires) and a null document (the policy store was unreachable). Neither is fixed up here:
    /// inventing a fallback approver is exactly the kind of local rule this wave exists to prevent.</para>
    /// </summary>
    public static VoterEligibility EligibilityFor(AccessPolicyDocument? policy, ApprovalRequest request, string? username)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (policy is null || string.IsNullOrWhiteSpace(username) || request.ApproverGroups.Count == 0)
        {
            return VoterEligibility.NotAnApprover;
        }

        var groups = EffectivePermissionsBuilder.Build(policy, username).Groups;
        return request.ApproverGroups.Any(g => groups.Contains(g, StringComparer.Ordinal))
            ? VoterEligibility.Eligible
            : VoterEligibility.NotAnApprover;
    }
}
