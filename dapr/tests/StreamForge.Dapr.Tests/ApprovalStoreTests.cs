using StreamForge.Abstractions;
using StreamForge.AppCore.Access;
using StreamForge.Dapr.Host.Access;
using Xunit;

namespace StreamForge.Dapr.Tests;

/// <summary>
/// Plan 015 W4-C: unit tests for the actor-framework-free approval store. No Dapr sidecar, no actor
/// runtime, no Redis — <see cref="ApprovalStore"/> is a plain class over an in-memory
/// <see cref="ApprovalDocument"/> with the clock as a parameter, which is exactly why it was factored out
/// of <c>ApprovalActor</c> that way (the same split CatalogStore/RegistryActor and
/// AccessPolicyStore/AccessPolicyActor already use).
///
/// <para>What these tests are really pinning is that <b>the store adds no rule of its own</b>: every
/// assertion below is an <see cref="ApprovalStateMachine"/> rule observed through the store, so a
/// shortcut taken here — a self-vote let through, an expiry not persisted, an escalation repeated on
/// every tick — fails as a store test even though the state machine's own suite stays green. Wave 3
/// produced a three-way divergence between three agents deciding the same question locally; these are
/// the tests that would have caught it on this flavour.</para>
/// </summary>
public class ApprovalStoreTests
{
    private const long T0 = 1_700_000_000_000;

    private static ApprovalStore NewStore() => new(new ApprovalDocument());

    private static ApprovalTemplate Template(
        string name = "privileged",
        int required = 1,
        int expiresAfterSeconds = 3600,
        int escalateAfterSeconds = 0,
        string[]? approvers = null,
        string[]? escalation = null) => new()
        {
            Name = name,
            ActionPattern = "pipeline.*",
            ScopePattern = "*",
            RequiredApprovals = required,
            ApproverGroups = [.. approvers ?? ["reviewers"]],
            ExpiresAfterSeconds = expiresAfterSeconds,
            EscalateAfterSeconds = escalateAfterSeconds,
            EscalationGroups = [.. escalation ?? Array.Empty<string>()],
            Enabled = true,
        };

    private static ApprovalRequest Draft(string requestedBy = "alice", string action = "pipeline.delete") => new()
    {
        RequestedBy = requestedBy,
        Action = action,
        Scope = "prod-orders",
        Reason = "because",
    };

    private static ApprovalVote Vote(string username, bool approve = true) =>
        new() { Username = username, Approve = approve };

    // -------------------------------------------------------------------------------- filing

    [Fact]
    public void Create_StampsServerFieldsAndDiscardsWhatTheCallerSent()
    {
        var store = NewStore();

        // A caller trying to pre-approve its own request: populated votes, an Approved state, a required
        // count of zero and an id of its choosing. CreateRequest's whitelist drops all of it.
        var draft = Draft();
        draft.Id = "chosen-by-the-caller";
        draft.State = ApprovalState.Approved;
        draft.RequiredApprovals = 0;
        draft.Votes = [Vote("alice")];
        draft.ExpiresAtMs = long.MaxValue;

        var stored = store.Create(draft, [Template(required: 2)], "server-minted", T0);

        Assert.Equal("server-minted", stored.Id);
        Assert.Equal(ApprovalState.Pending, stored.State);
        Assert.Empty(stored.Votes);
        Assert.Equal(2, stored.RequiredApprovals);
        Assert.Equal(T0 + 3_600_000, stored.ExpiresAtMs);
        Assert.Equal("privileged", stored.TemplateName);
        Assert.Equal(["reviewers"], stored.ApproverGroups);
        Assert.Single(store.Document.Requests);
    }

    [Fact]
    public void Create_WithNoMatchingTemplate_RefusesWithASentence()
    {
        var store = NewStore();

        // The template covers pipeline.*; this asks about a source. No template means no approval is
        // required, so there is nothing to file — filing anyway would create a request with no approver
        // group, which nobody could ever approve.
        var ex = Assert.Throws<InvalidOperationException>(
            () => store.Create(Draft(action: "source.delete"), [Template()], "id", T0));

        Assert.Contains("source.delete", ex.Message);
        Assert.Empty(store.Document.Requests);
    }

    [Fact]
    public void Create_WithNoRequester_IsRefused()
    {
        // An unattributed request makes the self-vote rule vacuous: nobody is the requester, so anybody
        // may approve it.
        var store = NewStore();

        Assert.Throws<InvalidOperationException>(() => store.Create(Draft(requestedBy: " "), [Template()], "id", T0));
        Assert.Empty(store.Document.Requests);
    }

    [Fact]
    public void Create_DisabledTemplateDoesNotShadowTheEnabledOneBelowIt()
    {
        var store = NewStore();
        var disabled = Template(name: "off", required: 9);
        disabled.Enabled = false;

        var stored = store.Create(Draft(), [disabled, Template(name: "on", required: 2)], "id", T0);

        Assert.Equal("on", stored.TemplateName);
    }

    // -------------------------------------------------------------------------------- voting

    [Fact]
    public void Vote_ByTheRequester_IsRefusedEvenWhenTheyAreAnApprover()
    {
        // THE test: the store consults the state machine rather than counting votes itself. Eligibility
        // is handed in as Eligible on purpose — an administrator asking for a second pair of eyes on
        // their own action IS in the approver group, and must still be refused.
        var store = NewStore();
        var stored = store.Create(Draft(), [Template()], "r1", T0);

        var mutation = store.Vote("r1", Vote("alice"), VoterEligibility.Eligible, T0 + 1000);

        Assert.False(mutation.Result!.Accepted);
        // Applied is the STORED request even on a refusal, and null only when no request has that id —
        // reconciled with the Orleans twin during the wave-5 merge, because null has to mean one thing.
        // What says "the vote was refused" is Result.Accepted, not the absence of a request.
        Assert.Same(stored, mutation.Applied);
        Assert.False(mutation.Dirty);                       // nothing to persist
        Assert.Contains("cannot vote on it", mutation.Result.Reason);
        Assert.Empty(stored.Votes);
        Assert.Equal(ApprovalState.Pending, stored.State);
    }

    [Fact]
    public void Vote_ByTheRequesterInDifferentCase_IsAlsoRefused()
    {
        var store = NewStore();
        store.Create(Draft(requestedBy: "alice"), [Template()], "r1", T0);

        var mutation = store.Vote("r1", Vote("ALICE"), VoterEligibility.Eligible, T0 + 1000);

        Assert.False(mutation.Result!.Accepted);
    }

    [Fact]
    public void Vote_ByANonApprover_IsRefused()
    {
        var store = NewStore();
        store.Create(Draft(), [Template()], "r1", T0);

        var mutation = store.Vote("r1", Vote("bob"), VoterEligibility.NotAnApprover, T0 + 1000);

        Assert.False(mutation.Result!.Accepted);
        Assert.False(mutation.Dirty);
        Assert.Contains("not an approver", mutation.Result.Reason);
    }

    [Fact]
    public void Vote_PastTheDeadline_IsRefusedAndTheExpiryIsPersisted()
    {
        // The refusal-that-must-still-be-written case. ApplyVote enforces the deadline itself rather than
        // trusting the sweeper to have run, so a store that persisted only on Accepted would drop the
        // expiry — and the next vote, arriving before the next sweep, would find the request Pending
        // again and land.
        var store = NewStore();
        var stored = store.Create(Draft(), [Template(expiresAfterSeconds: 60)], "r1", T0);

        var mutation = store.Vote("r1", Vote("bob"), VoterEligibility.Eligible, T0 + 60_000);

        Assert.False(mutation.Result!.Accepted);
        Assert.True(mutation.Result.StateChanged);
        Assert.True(mutation.Dirty);                        // ← the whole point
        Assert.Same(stored, mutation.Applied);              // refused, but the caller still sees Expired
        Assert.Equal(ApprovalState.Expired, stored.State);
        Assert.Equal(T0 + 60_000, stored.DecidedAtMs);
        Assert.Empty(stored.Votes);
    }

    [Fact]
    public void Vote_ThatIsAcceptedButLeavesThePendingState_IsStillDirty()
    {
        // Two approvals required, one cast: the state has not changed but the votes list has, and losing
        // that write loses the vote.
        var store = NewStore();
        store.Create(Draft(), [Template(required: 2)], "r1", T0);

        var mutation = store.Vote("r1", Vote("bob"), VoterEligibility.Eligible, T0 + 1000);

        Assert.True(mutation.Result!.Accepted);
        Assert.False(mutation.Result.StateChanged);
        Assert.True(mutation.Dirty);
        Assert.Equal(ApprovalState.Pending, mutation.Applied!.State);
    }

    [Fact]
    public void Vote_ReachingTheRequiredCount_Approves()
    {
        var store = NewStore();
        store.Create(Draft(), [Template(required: 2)], "r1", T0);

        store.Vote("r1", Vote("bob"), VoterEligibility.Eligible, T0 + 1000);
        var second = store.Vote("r1", Vote("carol"), VoterEligibility.Eligible, T0 + 2000);

        Assert.True(second.Result!.Accepted);
        Assert.True(second.Dirty);
        Assert.Equal(ApprovalState.Approved, second.Applied!.State);
    }

    [Fact]
    public void Vote_ReVoting_ReplacesRatherThanCounttingTwice()
    {
        // One human is one vote however many times they click; otherwise a 2-of-N control is satisfiable
        // by a single approver alone.
        var store = NewStore();
        var stored = store.Create(Draft(), [Template(required: 2)], "r1", T0);

        store.Vote("r1", Vote("bob"), VoterEligibility.Eligible, T0 + 1000);
        store.Vote("r1", Vote("bob"), VoterEligibility.Eligible, T0 + 2000);

        Assert.Single(stored.Votes);
        Assert.Equal(ApprovalState.Pending, stored.State);
    }

    [Fact]
    public void Vote_OnAnUnknownId_IsNotFoundAndNotDirty()
    {
        var store = NewStore();

        var mutation = store.Vote("nope", Vote("bob"), VoterEligibility.Eligible, T0);

        Assert.Null(mutation.Result);
        Assert.Null(mutation.Applied);
        Assert.False(mutation.Dirty);
    }

    // -------------------------------------------------------------------------------- cancel / outcome

    [Fact]
    public void Cancel_OnlyByTheRequester()
    {
        var store = NewStore();
        var stored = store.Create(Draft(), [Template()], "r1", T0);

        Assert.False(store.Cancel("r1", "bob", T0 + 1000).Result!.Accepted);
        Assert.Equal(ApprovalState.Pending, stored.State);

        var mine = store.Cancel("r1", "alice", T0 + 2000);
        Assert.True(mine.Result!.Accepted);
        Assert.True(mine.Dirty);
        Assert.Equal(ApprovalState.Cancelled, stored.State);
    }

    [Fact]
    public void RecordOutcome_RefusedUnlessApproved()
    {
        var store = NewStore();
        var stored = store.Create(Draft(), [Template()], "r1", T0);

        Assert.False(store.RecordOutcome("r1", true, "ran", T0 + 1000).Result!.Accepted);

        store.Vote("r1", Vote("bob"), VoterEligibility.Eligible, T0 + 2000);
        var recorded = store.RecordOutcome("r1", true, "deleted 1 pipeline", T0 + 3000);

        Assert.True(recorded.Result!.Accepted);
        Assert.True(recorded.Dirty);
        Assert.Equal(ApprovalState.Executed, stored.State);
        Assert.Equal("deleted 1 pipeline", stored.Outcome);
    }

    // -------------------------------------------------------------------------------- sweeping

    [Fact]
    public void Sweep_EscalatesExactlyOnceAcrossTwoSweeps()
    {
        var store = NewStore();
        var templates = new[]
        {
            Template(escalateAfterSeconds: 60, escalation: ["oncall"]),
        };
        var stored = store.Create(Draft(), templates, "r1", T0);

        Assert.Equal(0, store.Sweep(templates, T0 + 30_000));        // not yet due
        Assert.Null(stored.EscalatedAtMs);

        Assert.Equal(1, store.Sweep(templates, T0 + 60_000));        // due
        Assert.Equal(T0 + 60_000, stored.EscalatedAtMs);
        Assert.Equal(["reviewers", "oncall"], stored.ApproverGroups);

        Assert.Equal(0, store.Sweep(templates, T0 + 120_000));       // latched — never again
        Assert.Equal(["reviewers", "oncall"], stored.ApproverGroups);
        Assert.Equal(T0 + 60_000, stored.EscalatedAtMs);
    }

    [Fact]
    public void Sweep_ExpiresPastDeadlineRequestsAndCountsThem()
    {
        var store = NewStore();
        var templates = new[] { Template(expiresAfterSeconds: 60) };
        var a = store.Create(Draft(), templates, "r1", T0);
        var b = store.Create(Draft(requestedBy: "dan"), templates, "r2", T0);

        Assert.Equal(2, store.Sweep(templates, T0 + 60_000));
        Assert.Equal(ApprovalState.Expired, a.State);
        Assert.Equal(ApprovalState.Expired, b.State);

        Assert.Equal(0, store.Sweep(templates, T0 + 120_000));       // terminal is terminal
    }

    [Fact]
    public void Sweep_WithADeletedTemplate_StillExpires()
    {
        var store = NewStore();
        store.Create(Draft(), [Template(expiresAfterSeconds: 60, escalateAfterSeconds: 10, escalation: ["oncall"])], "r1", T0);

        // The expiry deadline was snapshotted onto the request at filing time; escalation was not.
        Assert.Equal(0, store.Sweep([], T0 + 30_000));
        Assert.Equal(1, store.Sweep([], T0 + 60_000));
        Assert.Equal(ApprovalState.Expired, store.Get("r1")!.State);
    }

    // -------------------------------------------------------------------------------- listing

    [Fact]
    public void List_IsNewestFirstAndFiltersByState()
    {
        var store = NewStore();
        var templates = new[] { Template() };
        store.Create(Draft(), templates, "r1", T0);
        store.Create(Draft(requestedBy: "dan"), templates, "r2", T0 + 1000);
        store.Cancel("r2", "dan", T0 + 2000);

        Assert.Equal(["r2", "r1"], store.List(null, 10).Select(r => r.Id));
        Assert.Equal(["r1"], store.List(ApprovalState.Pending, 10).Select(r => r.Id));
        Assert.Single(store.List(null, 1));
    }

    // -------------------------------------------------------------------------------- eligibility

    private static AccessPolicyDocument Policy(params GroupDefinition[] groups)
    {
        var doc = new AccessPolicyDocument();
        doc.Groups.AddRange(groups);
        return doc;
    }

    private static GroupDefinition Group(string name, params string[] members) =>
        new() { Name = name, Members = [.. members] };

    [Fact]
    public void EligibilityFor_MemberOfAnApproverGroup_IsEligible()
    {
        var store = NewStore();
        var stored = store.Create(Draft(), [Template(approvers: ["reviewers"])], "r1", T0);

        Assert.Equal(
            VoterEligibility.Eligible,
            ApprovalStore.EligibilityFor(Policy(Group("reviewers", "bob")), stored, "bob"));
    }

    [Fact]
    public void EligibilityFor_EverythingElseFailsClosed()
    {
        var store = NewStore();
        var stored = store.Create(Draft(), [Template(approvers: ["reviewers"])], "r1", T0);
        var policy = Policy(Group("reviewers", "bob"), Group("others", "carol"));

        // Not a member; a member of the wrong group; no username; no policy at all.
        Assert.Equal(VoterEligibility.NotAnApprover, ApprovalStore.EligibilityFor(policy, stored, "dan"));
        Assert.Equal(VoterEligibility.NotAnApprover, ApprovalStore.EligibilityFor(policy, stored, "carol"));
        Assert.Equal(VoterEligibility.NotAnApprover, ApprovalStore.EligibilityFor(policy, stored, " "));
        Assert.Equal(VoterEligibility.NotAnApprover, ApprovalStore.EligibilityFor(null, stored, "bob"));

        // A template that named no approver group produces a request nobody can approve.
        var unapprovable = store.Create(Draft(), [Template(name: "t2", approvers: [])], "r2", T0);
        Assert.Equal(VoterEligibility.NotAnApprover, ApprovalStore.EligibilityFor(policy, unapprovable, "bob"));
    }

    [Fact]
    public void EligibilityFor_ADisabledApprover_StopsBeingAnApprover()
    {
        // Free, and the reason EffectivePermissionsBuilder is reused instead of a local membership scan:
        // it returns no groups for a disabled user, so disablement kills the vote through the same
        // mechanism that kills the token.
        var store = NewStore();
        var stored = store.Create(Draft(), [Template(approvers: ["reviewers"])], "r1", T0);
        var policy = Policy(Group("reviewers", "bob"));
        policy.Users.Add(new UserAccessEntry { Username = "bob", Disabled = true });

        Assert.Equal(VoterEligibility.NotAnApprover, ApprovalStore.EligibilityFor(policy, stored, "bob"));
    }

    [Fact]
    public void EligibilityFor_AfterEscalation_IncludesTheEscalationGroup()
    {
        var store = NewStore();
        var templates = new[] { Template(escalateAfterSeconds: 60, escalation: ["oncall"]) };
        var stored = store.Create(Draft(), templates, "r1", T0);
        var policy = Policy(Group("reviewers", "bob"), Group("oncall", "dan"));

        Assert.Equal(VoterEligibility.NotAnApprover, ApprovalStore.EligibilityFor(policy, stored, "dan"));

        store.Sweep(templates, T0 + 60_000);

        Assert.Equal(VoterEligibility.Eligible, ApprovalStore.EligibilityFor(policy, stored, "dan"));
        Assert.Equal(VoterEligibility.Eligible, ApprovalStore.EligibilityFor(policy, stored, "bob"));
    }
}
