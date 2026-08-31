using StreamsForge.Abstractions;
using StreamsForge.AppCore.Access;
using Xunit;

namespace StreamsForge.AppCore.Tests.Access;

/// <summary>Plan 015 wave 4 — the approval state machine. Like its neighbours these run in BOTH
/// solutions (the project is listed in each), which is the whole reason the machine is pure: an approval
/// that counted a vote on Orleans and refused it on Dapr would be a security bug no single-flavour suite
/// could see.</summary>
public class ApprovalStateMachineTests
{
    private const long T0 = 1_700_000_000_000;

    private static ApprovalTemplate Template(
        string name = "prod-writes",
        string action = "pipeline.*",
        string scope = "prod-*",
        int required = 1,
        int expiresAfter = 3600,
        int escalateAfter = 0,
        string[]? approvers = null,
        string[]? escalation = null,
        bool enabled = true) =>
        new()
        {
            Name = name,
            ActionPattern = action,
            ScopePattern = scope,
            RequiredApprovals = required,
            ExpiresAfterSeconds = expiresAfter,
            EscalateAfterSeconds = escalateAfter,
            ApproverGroups = [.. approvers ?? ["reviewers"]],
            EscalationGroups = [.. escalation ?? []],
            Enabled = enabled,
        };

    private static ApprovalRequest Filed(
        ApprovalTemplate? template = null,
        string requestedBy = "alice",
        string action = "pipeline.write",
        string scope = "prod-orders",
        long nowMs = T0) =>
        ApprovalStateMachine.CreateRequest(
            new ApprovalRequest { Action = action, Scope = scope, Reason = "because" },
            template ?? Template(),
            "req-1",
            requestedBy,
            nowMs);

    private static ApprovalVote Yes(string who) => new() { Username = who, Approve = true };
    private static ApprovalVote No(string who) => new() { Username = who, Approve = false };

    // ================================================================ template selection

    [Fact]
    public void NoMatchingTemplateMeansNoApprovalIsRequired()
    {
        var chosen = ApprovalStateMachine.SelectTemplate([Template()], Actions.TableWrite, "prod-orders", null);

        Assert.Null(chosen);
    }

    [Fact]
    public void AnEmptyTemplateListMeansNoApprovalIsRequired()
    {
        // This is what keeps an Approvals:Enabled=false deployment byte-identical.
        Assert.Null(ApprovalStateMachine.SelectTemplate([], Actions.PipelineWrite, "prod-orders", null));
    }

    [Fact]
    public void TheFirstEnabledMatchInDocumentOrderWins()
    {
        var broad = Template("broad", "*", "*", required: 1);
        var specific = Template("specific", "pipeline.write", "prod-*", required: 3);

        Assert.Equal("broad", ApprovalStateMachine.SelectTemplate([broad, specific], Actions.PipelineWrite, "prod-orders", null)!.Name);
        Assert.Equal("specific", ApprovalStateMachine.SelectTemplate([specific, broad], Actions.PipelineWrite, "prod-orders", null)!.Name);
    }

    [Fact]
    public void ADisabledTemplateIsSkippedAndDoesNotShadowTheOneBelowIt()
    {
        var disabled = Template("broad", "*", "*", required: 1, enabled: false);
        var specific = Template("specific", "pipeline.write", "prod-*", required: 3);

        var chosen = ApprovalStateMachine.SelectTemplate([disabled, specific], Actions.PipelineWrite, "prod-orders", null);

        Assert.Equal("specific", chosen!.Name);
    }

    [Fact]
    public void ScopePatternsUseTheSameGrammarAsAnEntitlement()
    {
        var t = Template(scope: "prod-*");

        Assert.NotNull(ApprovalStateMachine.SelectTemplate([t], Actions.PipelineWrite, "prod-orders", null));
        Assert.Null(ApprovalStateMachine.SelectTemplate([t], Actions.PipelineWrite, "dev-orders", null));
        // Case-sensitive, exactly like PermissionEvaluator: a control that silently widened itself over
        // capitalisation would be the same surprise in the other direction.
        Assert.Null(ApprovalStateMachine.SelectTemplate([t], Actions.PipelineWrite, "PROD-orders", null));
    }

    [Fact]
    public void ATagScopedTemplateMatchesOnTheResourcesTagsAndMissesWhenNoneAreSupplied()
    {
        var t = Template(scope: "tag:finance");

        Assert.NotNull(ApprovalStateMachine.SelectTemplate([t], Actions.PipelineWrite, "p1", ["finance", "eu"]));
        Assert.Null(ApprovalStateMachine.SelectTemplate([t], Actions.PipelineWrite, "p1", ["eu"]));
        Assert.Null(ApprovalStateMachine.SelectTemplate([t], Actions.PipelineWrite, "p1", null));
    }

    // ================================================================ filing

    [Fact]
    public void FilingDiscardsADraftsPrePopulatedVotesAndState()
    {
        // The attack this prevents: file a request that is already Approved, or already carries the
        // votes it needs, and the second pair of eyes never opens.
        var draft = new ApprovalRequest
        {
            Id = "attacker-chosen",
            RequestedBy = "mallory",
            RequestedAtMs = 1,
            Action = Actions.PipelineWrite,
            Scope = "prod-orders",
            Reason = "trust me",
            TemplateName = "some-other-template",
            RequiredApprovals = 0,
            Votes = [Yes("alice"), Yes("bob")],
            State = ApprovalState.Approved,
            ExpiresAtMs = long.MaxValue,
            EscalatedAtMs = 5,
            DecidedAtMs = 6,
            Outcome = "already done",
            ApproverGroups = ["mallory-only"],
            Origin = "chat",
            PayloadJson = "{\"a\":1}",
        };

        var filed = ApprovalStateMachine.CreateRequest(draft, Template(required: 2), "req-real", "alice", T0);

        Assert.Empty(filed.Votes);
        Assert.Equal(ApprovalState.Pending, filed.State);
        Assert.Null(filed.EscalatedAtMs);
        Assert.Null(filed.DecidedAtMs);
        Assert.Null(filed.Outcome);
        Assert.Equal("req-real", filed.Id);
        Assert.Equal("alice", filed.RequestedBy);          // the principal, never the draft's claim
        Assert.Equal(T0, filed.RequestedAtMs);
        Assert.Equal(2, filed.RequiredApprovals);          // the template's, not the draft's 0
        Assert.Equal("prod-writes", filed.TemplateName);
        Assert.Equal(["reviewers"], filed.ApproverGroups);
        Assert.Equal(T0 + 3_600_000, filed.ExpiresAtMs);

        // …and the descriptive half of the draft does survive, because that is what is being asked for.
        Assert.Equal(Actions.PipelineWrite, filed.Action);
        Assert.Equal("prod-orders", filed.Scope);
        Assert.Equal("trust me", filed.Reason);
        Assert.Equal("chat", filed.Origin);
        Assert.Equal("{\"a\":1}", filed.PayloadJson);
    }

    [Fact]
    public void FilingAcceptsAWave3ChatDraftAsItIsBuilt()
    {
        // The exact shape ChatAccess.FileAsync hands to IChatApprovalFiler: a correlation id in Id, the
        // human in RequestedBy, Origin "chat", the tool arguments in PayloadJson, everything else default.
        var chatDraft = new ApprovalRequest
        {
            Id = Guid.NewGuid().ToString("n"),
            RequestedBy = "alice",
            RequestedAtMs = T0,
            Action = Actions.PipelineControl,
            Scope = "prod-orders",
            Reason = "proposed by gemini on behalf of alice via the AI chat tool 'start_pipeline'.",
            Origin = "chat",
            PayloadJson = "{\"id\":\"prod-orders\"}",
        };

        var filed = ApprovalStateMachine.CreateRequest(chatDraft, Template(action: "pipeline.*"), "req-7", "alice", T0);

        Assert.Equal("chat", filed.Origin);
        Assert.Equal("req-7", filed.Id);
        Assert.Equal(ApprovalState.Pending, filed.State);
    }

    [Fact]
    public void FilingClampsARequiredApprovalCountOfZeroUpToOne()
    {
        // A template asking for zero approvals is a control that approves itself on filing.
        var filed = Filed(Template(required: 0));

        Assert.Equal(1, filed.RequiredApprovals);
    }

    [Fact]
    public void AZeroExpirySecondsTemplateFilesARequestThatNeverExpires()
    {
        Assert.Equal(0, Filed(Template(expiresAfter: 0)).ExpiresAtMs);
    }

    [Fact]
    public void FilingRefusesAnUnattributedRequester()
    {
        // Without a requester the self-vote rule is vacuous and the audit row is a lie.
        Assert.Throws<ArgumentException>(() =>
            ApprovalStateMachine.CreateRequest(new ApprovalRequest(), Template(), "req-1", "  ", T0));
    }

    [Fact]
    public void FilingWithoutATemplateThrowsRatherThanCreatingAnUnapprovableRequest()
    {
        Assert.Throws<ArgumentNullException>(() =>
            ApprovalStateMachine.CreateRequest(new ApprovalRequest(), null!, "req-1", "alice", T0));
    }

    [Fact]
    public void AnEditedTemplateCannotLowerTheBarUnderARequestAlreadyCollectingVotes()
    {
        var template = Template(required: 2);
        var request = Filed(template);

        template.RequiredApprovals = 1;   // an administrator edits it while the request is open

        var first = ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 1);

        Assert.True(first.Accepted);
        Assert.Equal(ApprovalState.Pending, request.State);
    }

    // ================================================================ voting — the rules

    [Fact]
    public void TheRequestersOwnVoteNeverCounts()
    {
        // THE test. A second pair of eyes that can be the first pair is not a control.
        var request = Filed(requestedBy: "alice");

        var result = ApprovalStateMachine.ApplyVote(request, Yes("alice"), VoterEligibility.Eligible, T0 + 1);

        Assert.False(result.Accepted);
        Assert.Empty(request.Votes);
        Assert.Equal(ApprovalState.Pending, request.State);
        Assert.False(result.StateChanged);
        Assert.Contains("cannot vote on it", result.Reason);
    }

    [Fact]
    public void TheRequestersOwnVoteIsRefusedEvenWhenTheyAreAnEligibleApprover()
    {
        // The case the mechanism exists for: an administrator who could approve anything asks for a
        // second pair of eyes on their own action. The refusal must name the real reason, or somebody
        // "fixes" a misconfiguration that is not one.
        var request = Filed(requestedBy: "admin");

        var result = ApprovalStateMachine.ApplyVote(request, Yes("admin"), VoterEligibility.Eligible, T0 + 1);

        Assert.False(result.Accepted);
        Assert.Contains("filed request", result.Reason);
        Assert.DoesNotContain("not an approver", result.Reason);
    }

    [Fact]
    public void TheRequestersOwnVoteIsRefusedThroughAChangeOfCapitalisation()
    {
        var request = Filed(requestedBy: "alice");

        var result = ApprovalStateMachine.ApplyVote(request, Yes("Alice"), VoterEligibility.Eligible, T0 + 1);

        Assert.False(result.Accepted);
        Assert.Empty(request.Votes);
    }

    [Fact]
    public void AnIneligibleVoterIsRefused()
    {
        var request = Filed();

        var result = ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.NotAnApprover, T0 + 1);

        Assert.False(result.Accepted);
        Assert.Empty(request.Votes);
        Assert.Contains("not an approver", result.Reason);
    }

    [Fact]
    public void TheDefaultEligibilityValueRefuses()
    {
        // default(VoterEligibility) must be the closed one: a zeroed field or a forgotten assignment
        // fails safe.
        Assert.Equal(VoterEligibility.NotAnApprover, default(VoterEligibility));

        var request = Filed();
        Assert.False(ApprovalStateMachine.ApplyVote(request, Yes("bob"), default, T0 + 1).Accepted);
    }

    [Fact]
    public void AnAnonymousVoteIsRefused()
    {
        var request = Filed();

        var result = ApprovalStateMachine.ApplyVote(request, new ApprovalVote { Approve = true }, VoterEligibility.Eligible, T0 + 1);

        Assert.False(result.Accepted);
        Assert.Contains("must name its voter", result.Reason);
    }

    [Fact]
    public void ReVotingReplacesTheVotersPreviousVoteRatherThanAccumulating()
    {
        var request = Filed(Template(required: 2));

        Assert.True(ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 1).Accepted);
        Assert.True(ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 2).Accepted);

        // One human, one vote, however many times they click — otherwise 2-of-M is 1-of-M.
        Assert.Single(request.Votes);
        Assert.Equal(T0 + 2, request.Votes[0].AtMs);
        Assert.Equal(ApprovalState.Pending, request.State);

        Assert.True(ApprovalStateMachine.ApplyVote(request, Yes("carol"), VoterEligibility.Eligible, T0 + 3).Accepted);
        Assert.Equal(ApprovalState.Approved, request.State);
    }

    [Fact]
    public void ARequesterCannotTopUpTheCountByReVoting()
    {
        var request = Filed(Template(required: 2), requestedBy: "alice");

        ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 1);
        ApprovalStateMachine.ApplyVote(request, Yes("alice"), VoterEligibility.Eligible, T0 + 2);

        Assert.Single(request.Votes);
        Assert.Equal(ApprovalState.Pending, request.State);
    }

    [Fact]
    public void RequiredApprovalsApprovesAtExactlyTheNthVote()
    {
        var request = Filed(Template(required: 3));

        Assert.Equal(ApprovalState.Pending, ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 1).State);
        Assert.Equal(ApprovalState.Pending, ApprovalStateMachine.ApplyVote(request, Yes("carol"), VoterEligibility.Eligible, T0 + 2).State);

        var third = ApprovalStateMachine.ApplyVote(request, Yes("dave"), VoterEligibility.Eligible, T0 + 3);

        Assert.Equal(ApprovalState.Approved, third.State);
        Assert.True(third.StateChanged);
        Assert.Equal(T0 + 3, request.DecidedAtMs);
    }

    [Fact]
    public void OneRejectionIsDecisive()
    {
        // Deliberate, not an accident: requiring N rejections would let a requester shop for approvers.
        var request = Filed(Template(required: 3));

        ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 1);
        var no = ApprovalStateMachine.ApplyVote(request, No("carol"), VoterEligibility.Eligible, T0 + 2);

        Assert.True(no.Accepted);
        Assert.Equal(ApprovalState.Rejected, request.State);
        Assert.Equal(T0 + 2, request.DecidedAtMs);
    }

    [Fact]
    public void ARejectionCannotBeWalkedBackByFurtherVotes()
    {
        var request = Filed(Template(required: 1));

        ApprovalStateMachine.ApplyVote(request, No("carol"), VoterEligibility.Eligible, T0 + 1);

        // Rejected is terminal — including for the rejecter changing their mind, and including for the
        // approver who arrives a second later.
        var late = ApprovalStateMachine.ApplyVote(request, Yes("carol"), VoterEligibility.Eligible, T0 + 2);
        var other = ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 3);

        Assert.False(late.Accepted);
        Assert.False(other.Accepted);
        Assert.Equal(ApprovalState.Rejected, request.State);
        Assert.Single(request.Votes);
    }

    [Fact]
    public void AnApprovedRequestAcceptsNoFurtherVotes()
    {
        var request = Filed(Template(required: 1));
        ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 1);

        var late = ApprovalStateMachine.ApplyVote(request, No("carol"), VoterEligibility.Eligible, T0 + 2);

        Assert.False(late.Accepted);
        Assert.Equal(ApprovalState.Approved, request.State);
    }

    // ================================================================ voting — expiry

    [Fact]
    public void AnExpiredRequestRefusesALateApproval()
    {
        var request = Filed(Template(required: 1));
        var sweep = ApprovalStateMachine.Sweep([request], [Template()], T0 + 3_600_001);
        Assert.Equal(ApprovalSweepAction.Expired, Assert.Single(sweep).Action);

        var late = ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 3_600_002);

        Assert.False(late.Accepted);
        Assert.Equal(ApprovalState.Expired, request.State);
        Assert.Empty(request.Votes);
    }

    [Fact]
    public void APastDeadlineRequestExpiresAtVoteTimeEvenIfTheSweeperHasNotRun()
    {
        // The race the sweeper cannot close: it runs on an interval, so "State is still Pending" and
        // "the deadline has not passed" are different statements. Expiry is deadline-driven.
        var request = Filed(Template(required: 1));

        var late = ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 3_600_000);

        Assert.False(late.Accepted);
        Assert.Equal(ApprovalState.Expired, request.State);
        Assert.True(late.StateChanged);      // refused, but the store must still persist
        Assert.Contains("expired", late.Reason);
    }

    [Fact]
    public void AVoteOneMillisecondBeforeTheDeadlineStillCounts()
    {
        var request = Filed(Template(required: 1));

        var inTime = ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 3_599_999);

        Assert.True(inTime.Accepted);
        Assert.Equal(ApprovalState.Approved, request.State);
    }

    [Fact]
    public void ARequestThatNeverExpiresIsNotExpiredByAnyClock()
    {
        var request = Filed(Template(expiresAfter: 0));

        Assert.Empty(ApprovalStateMachine.Sweep([request], [Template(expiresAfter: 0)], long.MaxValue));
        Assert.True(ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, long.MaxValue - 1).Accepted);
    }

    // ================================================================ sweep — expiry and escalation

    [Fact]
    public void SweepingBeforeAnyDeadlineChangesNothing()
    {
        var request = Filed(Template(escalateAfter: 600, escalation: ["oncall"]));

        Assert.Empty(ApprovalStateMachine.Sweep([request], [Template(escalateAfter: 600, escalation: ["oncall"])], T0 + 1000));
        Assert.Equal(ApprovalState.Pending, request.State);
        Assert.Null(request.EscalatedAtMs);
    }

    [Fact]
    public void EscalationHappensExactlyOnceAcrossTwoSweeps()
    {
        var template = Template(escalateAfter: 600, escalation: ["oncall", "security"]);
        var request = Filed(template);

        var first = ApprovalStateMachine.Sweep([request], [template], T0 + 600_000);
        var second = ApprovalStateMachine.Sweep([request], [template], T0 + 900_000);

        Assert.Equal(ApprovalSweepAction.Escalated, Assert.Single(first).Action);
        Assert.Empty(second);                                                   // not on every tick
        Assert.Equal(T0 + 600_000, request.EscalatedAtMs);                      // the first sweep's stamp
        Assert.Equal(["reviewers", "oncall", "security"], request.ApproverGroups);
        Assert.Equal(ApprovalState.Pending, request.State);                     // escalation is not a decision
    }

    [Fact]
    public void EscalationWidensTheApproverPoolWithoutDuplicatingWhatIsAlreadyThere()
    {
        var template = Template(approvers: ["reviewers"], escalateAfter: 600, escalation: ["reviewers", "oncall"]);
        var request = Filed(template);

        ApprovalStateMachine.Sweep([request], [template], T0 + 600_000);

        Assert.Equal(["reviewers", "oncall"], request.ApproverGroups);
    }

    [Fact]
    public void EscalationDoesNotChangeHowManyApprovalsAreNeeded()
    {
        var template = Template(required: 2, escalateAfter: 600, escalation: ["oncall"]);
        var request = Filed(template);

        ApprovalStateMachine.Sweep([request], [template], T0 + 600_000);

        Assert.Equal(2, request.RequiredApprovals);
    }

    [Fact]
    public void ZeroEscalateAfterSecondsNeverEscalates()
    {
        var template = Template(escalateAfter: 0, escalation: ["oncall"]);
        var request = Filed(template);

        Assert.Empty(ApprovalStateMachine.Sweep([request], [template], T0 + 3_599_000));
        Assert.Null(request.EscalatedAtMs);
    }

    [Fact]
    public void EscalatingToNobodyIsNotAnEscalationAndIsNotReported()
    {
        // Otherwise the sweeper's own change count is noise: every tick would report this request.
        var template = Template(escalateAfter: 600, escalation: []);
        var request = Filed(template);

        Assert.Empty(ApprovalStateMachine.Sweep([request], [template], T0 + 600_000));
        Assert.Null(request.EscalatedAtMs);
    }

    [Fact]
    public void ExpiryWinsOverEscalationWhenBothDeadlinesHavePassed()
    {
        var template = Template(expiresAfter: 60, escalateAfter: 30, escalation: ["oncall"]);
        var request = Filed(template);

        var changes = ApprovalStateMachine.Sweep([request], [template], T0 + 120_000);

        Assert.Equal(ApprovalSweepAction.Expired, Assert.Single(changes).Action);
        Assert.Null(request.EscalatedAtMs);
        Assert.Equal(ApprovalState.Expired, request.State);
    }

    [Fact]
    public void ADeletedTemplateStopsEscalationButNotExpiry()
    {
        var template = Template(expiresAfter: 3600, escalateAfter: 600, escalation: ["oncall"]);
        var request = Filed(template);

        // The template is gone from the document: nothing knows which groups to escalate to…
        Assert.Empty(ApprovalStateMachine.Sweep([request], [], T0 + 700_000));
        Assert.Null(request.EscalatedAtMs);

        // …but the expiry deadline was snapshotted onto the request when it was filed.
        Assert.Equal(ApprovalSweepAction.Expired, Assert.Single(ApprovalStateMachine.Sweep([request], [], T0 + 3_600_000)).Action);
    }

    [Fact]
    public void SweepSkipsEverythingThatIsNotPending()
    {
        var template = Template(required: 1, escalateAfter: 1, escalation: ["oncall"]);
        var approved = Filed(template);
        ApprovalStateMachine.ApplyVote(approved, Yes("bob"), VoterEligibility.Eligible, T0 + 1);
        var rejected = Filed(template);
        ApprovalStateMachine.ApplyVote(rejected, No("bob"), VoterEligibility.Eligible, T0 + 1);

        var changes = ApprovalStateMachine.Sweep([approved, rejected], [template], T0 + 99_999_999);

        Assert.Empty(changes);
        Assert.Equal(ApprovalState.Approved, approved.State);
        Assert.Equal(ApprovalState.Rejected, rejected.State);
    }

    [Fact]
    public void SweepReturnsOnlyTheRequestsItChanged()
    {
        var template = Template(expiresAfter: 60);
        var stale = Filed(template);
        var fresh = Filed(template, nowMs: T0 + 100_000);

        var changes = ApprovalStateMachine.Sweep([stale, fresh], [template], T0 + 61_000);

        var change = Assert.Single(changes);
        Assert.Same(stale, change.Request);
        Assert.Equal(ApprovalState.Pending, fresh.State);
    }

    // ================================================================ cancel and outcome

    [Fact]
    public void OnlyTheRequesterMayCancelAndOnlyWhilePending()
    {
        var request = Filed(requestedBy: "alice");

        Assert.False(ApprovalStateMachine.Cancel(request, "bob", T0 + 1).Accepted);
        Assert.Equal(ApprovalState.Pending, request.State);

        var ok = ApprovalStateMachine.Cancel(request, "alice", T0 + 2);
        Assert.True(ok.Accepted);
        Assert.Equal(ApprovalState.Cancelled, request.State);
        Assert.Equal(T0 + 2, request.DecidedAtMs);

        // Cancelled is terminal: no resurrection by a late approval.
        Assert.False(ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 3).Accepted);
        Assert.False(ApprovalStateMachine.Cancel(request, "alice", T0 + 4).Accepted);
    }

    [Fact]
    public void AnOutcomeIsOnlyRecordedForAnApprovedRequest()
    {
        var pending = Filed(Template(required: 1));
        Assert.False(ApprovalStateMachine.RecordOutcome(pending, true, "ran", T0 + 1).Accepted);
        Assert.Equal(ApprovalState.Pending, pending.State);

        ApprovalStateMachine.ApplyVote(pending, Yes("bob"), VoterEligibility.Eligible, T0 + 2);

        var executed = ApprovalStateMachine.RecordOutcome(pending, true, "pipeline started", T0 + 3);
        Assert.True(executed.Accepted);
        Assert.Equal(ApprovalState.Executed, pending.State);
        Assert.Equal("pipeline started", pending.Outcome);
    }

    [Fact]
    public void AFailedExecutionIsRecordedAsFailedAndNotAsStillApproved()
    {
        var request = Filed(Template(required: 1));
        ApprovalStateMachine.ApplyVote(request, Yes("bob"), VoterEligibility.Eligible, T0 + 1);

        ApprovalStateMachine.RecordOutcome(request, false, "grain threw", T0 + 2);

        Assert.Equal(ApprovalState.Failed, request.State);
        Assert.Equal("grain threw", request.Outcome);
        // And a failed execution is not a second chance to vote it through again.
        Assert.False(ApprovalStateMachine.ApplyVote(request, Yes("carol"), VoterEligibility.Eligible, T0 + 3).Accepted);
    }
}
