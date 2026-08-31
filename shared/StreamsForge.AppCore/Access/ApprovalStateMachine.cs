using StreamsForge.Abstractions;

namespace StreamsForge.AppCore.Access;

/// <summary>
/// Plan 015 wave 4 — the approval workflow's every state transition, and nothing else.
///
/// <para>Like <see cref="PermissionEvaluator"/> next door, this is a pure function of (data in, data
/// out): no store, no clock, no logger, no ASP.NET, no Orleans, no Dapr. <c>nowMs</c> is always a
/// parameter and identity is always a parameter. That is what lets the tests live in
/// <c>StreamsForge.AppCore.Tests</c>, a project listed in BOTH solutions — and the reason it matters here
/// is the same reason it mattered for the evaluator, only sharper: <b>an approval that counted a vote on
/// one flavour and refused it on the other would be a security bug that no single-flavour suite could
/// see.</b> Plan 015 wave 3 already produced one three-way divergence between agents implementing "the
/// same" rule on three transports; the cure is that there be exactly one implementation.</para>
///
/// <para><b>The rules, and why each one is the way it is.</b>
/// <list type="bullet">
///   <item><b>The requester's own vote never counts.</b> A second pair of eyes that can be the first
///   pair is not a control, it is a formality with an audit trail. This is the single most important
///   line in the file — see <see cref="ApplyVote"/>.</item>
///   <item><b>Re-voting replaces.</b> One human is one vote however many times they click; the
///   alternative is an N-of-M control that any single approver can satisfy alone.</item>
///   <item><b>One rejection is decisive.</b> Deliberate, not an accident of implementation: requiring N
///   rejections would let a requester shop for approvers — file, collect a no, keep asking until the
///   yes-count arrives first. A reviewer saying no IS the mechanism; it does not need a quorum.</item>
///   <item><b>A request that is not Pending accepts nothing.</b> Terminal is terminal. In particular an
///   expired request must not be resurrectable by a late approval, which is also why
///   <see cref="ApplyVote"/> enforces the deadline itself instead of trusting the sweeper to have
///   run — see the note there.</item>
///   <item><b>Eligibility is not decided here.</b> "Is this voter in an approver group?" needs the
///   policy document, which this type deliberately cannot see. It arrives as a decided input, in a
///   dedicated type whose <c>default</c> is the refusing one — see
///   <see cref="VoterEligibility"/>.</item>
/// </list></para>
///
/// <para><b>Mutation, deliberately.</b> <see cref="ApplyVote"/>, <see cref="Cancel"/>,
/// <see cref="RecordOutcome"/> and <see cref="Sweep"/> mutate the <see cref="ApprovalRequest"/> they are
/// handed rather than returning a copy. Copying would mean hand-writing a 17-field clone of a
/// <b>frozen, additively-evolving</b> contract, and the failure mode of that clone is silent: the day
/// somebody adds <c>[Id(17)]</c>, every vote quietly drops it. Mutating in place cannot drift. The one
/// place a fresh object IS built is <see cref="CreateRequest"/>, and there the whitelist is the whole
/// security property.</para>
/// </summary>
public static class ApprovalStateMachine
{
    // ---------------------------------------------------------------- which template applies

    /// <summary>
    /// The first <b>enabled</b> template whose patterns cover this (action, scope) pair, in document
    /// order — or <c>null</c>, which means <b>no approval is required</b>.
    ///
    /// <para><b>Why first-match and not specificity scoring.</b> Two rules were available. Specificity
    /// scoring (count literal characters, rank exact &gt; prefix &gt; tag &gt; <c>*</c>) needs a metric
    /// nobody can predict from reading the list, a documented tie-break, and a UI that explains why the
    /// template an operator expected lost. First-match needs one sentence — "the list is ordered, put
    /// the specific ones first" — and it hands the administrator ordering as a deliberate tool: moving a
    /// row is how you say "this one first", and there is no second mechanism competing with it. It also
    /// matches how the folder already thinks: <see cref="PermissionEvaluator"/> walks a flat list with no
    /// specificity ladder either, for the same stated reason.
    /// <br/>ponytail: ceiling — a template list whose order matters is a list an editor can silently
    /// break by inserting at the top; there is no warning for a template that can never win because a
    /// broader one precedes it. Upgrade path: keep first-match as the runtime rule and add a
    /// <i>shadowing</i> check in the admin UI that flags an unreachable row, which is the same
    /// information without changing a single decision this version makes.</para>
    ///
    /// <para><b>Note the fail-open direction, because it is the opposite of the evaluator's.</b> No
    /// matching template means no approval — that is exactly what keeps an
    /// <c>Approvals:Enabled=false</c> deployment byte-identical, and it is why a misspelled
    /// <see cref="ApprovalTemplate.ActionPattern"/> is a control that silently does not exist rather
    /// than a route that starts refusing. Callers that must not fail open (the config-replace path, a
    /// break-glass action) belong behind a <c>Deny</c> grant, not behind a template.</para>
    /// </summary>
    /// <param name="templates">The document's templates, in document order.</param>
    /// <param name="action">A concrete action, never a pattern — one of the <see cref="Actions"/>
    /// constants.</param>
    /// <param name="scope">The resource's name (wave 3 settled that it is the NAME, never the id), or
    /// <c>"*"</c> for an operation with no single resource.</param>
    /// <param name="resourceTags">The resource's <c>Tags</c>, so a <c>tag:finance</c>
    /// <see cref="ApprovalTemplate.ScopePattern"/> can match. <b>Required positionally, unlike the
    /// evaluator's optional one</b>, so that passing <c>null</c> is a decision somebody typed: a caller
    /// that silently omitted tags would turn every tag-scoped template into a control that never fires,
    /// and unlike the evaluator's fail-closed miss, that failure is invisible.</param>
    public static ApprovalTemplate? SelectTemplate(
        IReadOnlyList<ApprovalTemplate> templates,
        string action,
        string scope,
        IReadOnlyCollection<string>? resourceTags)
    {
        ArgumentNullException.ThrowIfNull(templates);

        foreach (var template in templates)
        {
            if (!template.Enabled)
            {
                // Disabled means "not in the list", not "matches and then does nothing": a disabled
                // broad template must not shadow the specific enabled one below it, because the whole
                // point of the flag is to switch a control off without deleting how it was configured.
                continue;
            }

            if (PermissionEvaluator.GlobMatch(template.ActionPattern, action)
                && PermissionEvaluator.ScopeMatches(template.ScopePattern, scope, resourceTags))
            {
                return template;
            }
        }

        return null;
    }

    // ---------------------------------------------------------------- filing

    /// <summary>
    /// Turn a caller's draft plus the applicable template into the request that should be stored.
    ///
    /// <para><b>This is a whitelist, not a copy, and that is the security property.</b>
    /// <see cref="IApprovalFacade.RequestAsync"/>'s doc comment promises that "a caller cannot
    /// pre-approve its own request by sending a populated Votes list"; promising it in a comment and
    /// hoping two independently-written stores both remember is how it stops being true. Here it is
    /// structural: exactly six fields are read off the draft (<see cref="ApprovalRequest.Action"/>,
    /// <see cref="ApprovalRequest.Scope"/>, <see cref="ApprovalRequest.Reason"/>,
    /// <see cref="ApprovalRequest.PayloadJson"/>, <see cref="ApprovalRequest.Origin"/> — the description
    /// of what is being asked for) and everything that decides the outcome is stamped from the template,
    /// the clock and the authenticated principal. <c>Votes</c>, <c>State</c>, <c>ExpiresAtMs</c>,
    /// <c>EscalatedAtMs</c>, <c>DecidedAtMs</c>, <c>Outcome</c>, <c>RequiredApprovals</c>,
    /// <c>ApproverGroups</c>, <c>TemplateName</c> and <c>Id</c> on the draft are <b>discarded</b>,
    /// whatever they contain.</para>
    ///
    /// <para>The consequence for future edits, stated so it is not discovered by surprise: a field added
    /// to <see cref="ApprovalRequest"/> is dropped by this method until somebody adds it to the
    /// whitelist. That is the safe direction — a new field defaults to a server value rather than to
    /// whatever the caller sent.</para>
    ///
    /// <para><b>Identity comes from the parameter, never from the draft.</b>
    /// <see cref="ApprovalRequest.RequestedBy"/> on the draft is ignored, so no caller can file "as"
    /// somebody else and then approve it themselves — which would defeat the self-vote rule by the
    /// simplest possible route. Wave 3's chat filer sets the draft's <c>RequestedBy</c> to the human
    /// behind the model, and its own principal is that same human, so nothing about that path
    /// changes.</para>
    /// </summary>
    /// <param name="draft">What the caller wants approved. Only the six descriptive fields are read.</param>
    /// <param name="template">The template <see cref="SelectTemplate"/> returned. Non-null by contract: a
    /// <c>null</c> template means no approval is required and there is therefore nothing to file, and
    /// filing anyway would create a request with no approver group — one nobody is able to approve and
    /// that can only rot until it expires. Throwing is how a store that forgot to check finds out.</param>
    /// <param name="id">The request id. Minted by the caller because <c>Guid.NewGuid()</c> is exactly
    /// the kind of ambient non-determinism that would stop this being a pure function.</param>
    /// <param name="requestedBy">The authenticated principal. Empty is rejected: an unattributed request
    /// makes the self-vote rule vacuous and the audit row a lie.</param>
    /// <param name="nowMs">Unix ms.</param>
    public static ApprovalRequest CreateRequest(
        ApprovalRequest draft,
        ApprovalTemplate template,
        string id,
        string requestedBy,
        long nowMs)
    {
        ArgumentNullException.ThrowIfNull(draft);
        ArgumentNullException.ThrowIfNull(template);
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(requestedBy);

        return new ApprovalRequest
        {
            Id = id,
            RequestedBy = requestedBy,
            RequestedAtMs = nowMs,

            // --- from the draft: what is being asked for, and nothing that decides the answer.
            Action = draft.Action,
            Scope = draft.Scope,
            Reason = draft.Reason,
            PayloadJson = draft.PayloadJson,
            Origin = string.IsNullOrWhiteSpace(draft.Origin) ? "rest" : draft.Origin,

            // --- from the template, snapshotted. The template may be edited or deleted while this
            // request is open, and re-reading it at vote time would let an administrator lower
            // RequiredApprovals under a request that is already collecting votes. What was required
            // when it was filed is what stays required.
            TemplateName = template.Name,
            RequiredApprovals = Math.Max(1, template.RequiredApprovals),
            ApproverGroups = [.. template.ApproverGroups],
            ExpiresAtMs = DeadlineFrom(nowMs, template.ExpiresAfterSeconds),

            // --- server-owned, always. Listed explicitly rather than left to field initializers so
            // that reading this method answers "can a caller pre-set it?" without leaving the file.
            Votes = [],
            State = ApprovalState.Pending,
            EscalatedAtMs = null,
            DecidedAtMs = null,
            Outcome = null,
        };
    }

    /// <summary><c>&lt;= 0</c> seconds means never, matching what
    /// <see cref="ApprovalTemplate.EscalateAfterSeconds"/> already documents for escalation; a request
    /// that never expires carries <see cref="ApprovalRequest.ExpiresAtMs"/> <c>= 0</c>.</summary>
    private static long DeadlineFrom(long nowMs, int afterSeconds) =>
        afterSeconds <= 0 ? 0 : nowMs + (afterSeconds * 1000L);

    // ---------------------------------------------------------------- voting

    /// <summary>
    /// Apply one vote. Mutates <paramref name="request"/> in place (see the type remarks) and reports
    /// what happened.
    ///
    /// <para><b>The order of the refusals is itself a decision</b>, because the first one that fires is
    /// the sentence a human reads:
    /// <list type="number">
    ///   <item>Not <see cref="ApprovalState.Pending"/> → refused. Terminal is terminal.</item>
    ///   <item>Past its deadline → the request is expired <i>right here</i> and the vote refused. The
    ///   sweeper is a background service on an interval, so "State is still Pending" and "the deadline
    ///   has not passed" are not the same statement; trusting the former would make a late approval land
    ///   or not depending on how recently a timer ticked. Expiry is deadline-driven and the sweeper only
    ///   notices it for requests nobody is voting on. This is the mutation-on-refusal case, which is why
    ///   <see cref="ApprovalVoteResult.StateChanged"/> exists separately from
    ///   <see cref="ApprovalVoteResult.Accepted"/>: the store must persist even though it refused.</item>
    ///   <item><b>The voter is the requester → refused.</b> The most important line in the file. Note
    ///   it is checked before eligibility on purpose: a requester who IS in the approver group (an
    ///   administrator asking for a second pair of eyes on their own privileged action — the very case
    ///   this mechanism exists for) must be told "you cannot approve your own request", not "you are not
    ///   an approver", which would read as a misconfiguration and get "fixed".</item>
    ///   <item>Not eligible → refused. Decided by the caller; see <see cref="VoterEligibility"/>.</item>
    /// </list></para>
    /// </summary>
    /// <param name="request">The stored request. Mutated in place.</param>
    /// <param name="vote">Who, yes/no, and an optional comment. <see cref="ApprovalVote.AtMs"/> is
    /// overwritten with <paramref name="nowMs"/> — a caller-supplied vote timestamp is a caller-supplied
    /// place in the ordering.</param>
    /// <param name="eligibility">Whether this voter is in one of the request's approver groups. NOT
    /// computable here: it needs the policy document, which this type cannot see, and pretending
    /// otherwise would mean shipping half the answer twice.</param>
    /// <param name="nowMs">Unix ms.</param>
    public static ApprovalVoteResult ApplyVote(
        ApprovalRequest request,
        ApprovalVote vote,
        VoterEligibility eligibility,
        long nowMs)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(vote);

        if (request.State != ApprovalState.Pending)
        {
            return new ApprovalVoteResult(
                false,
                $"request {request.Id} is {request.State.ToString().ToLowerInvariant()} and accepts no further votes",
                request.State,
                false);
        }

        if (IsPastDeadline(request, nowMs))
        {
            Expire(request, nowMs);
            return new ApprovalVoteResult(
                false,
                $"request {request.Id} expired at {request.ExpiresAtMs} and accepts no further votes",
                request.State,
                true);
        }

        if (string.IsNullOrWhiteSpace(vote.Username))
        {
            // An anonymous vote cannot be deduplicated, cannot be checked against the requester, and
            // cannot be audited. Three reasons, any one of them sufficient.
            return new ApprovalVoteResult(false, "a vote must name its voter", request.State, false);
        }

        if (IsRequester(request, vote.Username))
        {
            return new ApprovalVoteResult(
                false,
                $"'{vote.Username}' filed request {request.Id} and cannot vote on it",
                request.State,
                false);
        }

        if (eligibility != VoterEligibility.Eligible)
        {
            return new ApprovalVoteResult(
                false,
                $"'{vote.Username}' is not an approver for request {request.Id}",
                request.State,
                false);
        }

        // Replace, never accumulate: one human is one vote however many times they click. Ordinal,
        // because a username IS the exact stored string everywhere else in this repo (login compares
        // with `==`), so folding case here would merge two distinct accounts into one vote.
        var existing = request.Votes.FindIndex(v => string.Equals(v.Username, vote.Username, StringComparison.Ordinal));
        var stored = new ApprovalVote
        {
            Username = vote.Username,
            Approve = vote.Approve,
            AtMs = nowMs,
            Comment = vote.Comment,
        };

        if (existing >= 0)
        {
            request.Votes[existing] = stored;
        }
        else
        {
            request.Votes.Add(stored);
        }

        // One rejection is decisive, and it is checked before the approval count so that a rejection
        // arriving alongside the final approval loses to nobody: whichever vote is applied last decides,
        // and a list containing a single "no" is Rejected regardless of how many yeses sit beside it.
        // Requiring N rejections would let a requester shop for approvers — file, collect a no, and keep
        // asking until the yes-count arrives first. A reviewer saying no is the entire mechanism.
        if (request.Votes.Any(v => !v.Approve))
        {
            request.State = ApprovalState.Rejected;
            request.DecidedAtMs = nowMs;
            return new ApprovalVoteResult(true, $"request {request.Id} rejected by '{vote.Username}'", request.State, true);
        }

        var approvals = request.Votes.Count(v => v.Approve);
        var required = Math.Max(1, request.RequiredApprovals);
        if (approvals >= required)
        {
            request.State = ApprovalState.Approved;
            request.DecidedAtMs = nowMs;
            return new ApprovalVoteResult(true, $"request {request.Id} approved ({approvals}/{required})", request.State, true);
        }

        return new ApprovalVoteResult(true, $"vote recorded ({approvals}/{required})", request.State, false);
    }

    /// <summary>Case-INSENSITIVE, and the one place in this repo that deviates from ordinal username
    /// comparison. The asymmetry is the argument: refusing "ALICE" a vote on "alice"'s request when they
    /// are genuinely two accounts is an inconvenience somebody notices and works around; letting a
    /// requester approve their own request through a change of capitalisation is the control silently not
    /// existing. Login is case-sensitive today so the two cannot in fact be the same person — this is
    /// insurance against the day it is not.</summary>
    private static bool IsRequester(ApprovalRequest request, string username) =>
        string.Equals(request.RequestedBy, username, StringComparison.OrdinalIgnoreCase);

    // ---------------------------------------------------------------- cancel / outcome

    /// <summary>Withdraw a request. Pending only, and only by whoever filed it.
    /// <para>ponytail: no administrative override — an administrator who wants a request gone lets it
    /// expire or rejects it, and a rejection is the more honest record anyway ("somebody said no", with
    /// a name on it, rather than "it vanished"). Ceiling: a request filed by a user who has since left
    /// sits in the inbox until its deadline. Upgrade path: a second overload taking a
    /// <c>mayOverride</c> decided by the caller, exactly as <see cref="VoterEligibility"/> is.</para>
    /// <para>Lives here rather than in each store for the reason the whole file exists: it is a state
    /// transition with an authorization rule in it, and two stores are being written in parallel right
    /// now.</para></summary>
    public static ApprovalVoteResult Cancel(ApprovalRequest request, string username, long nowMs)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.State != ApprovalState.Pending)
        {
            return new ApprovalVoteResult(
                false,
                $"request {request.Id} is {request.State.ToString().ToLowerInvariant()} and cannot be cancelled",
                request.State,
                false);
        }

        if (!IsRequester(request, username))
        {
            return new ApprovalVoteResult(
                false,
                $"only '{request.RequestedBy}' may cancel request {request.Id}",
                request.State,
                false);
        }

        request.State = ApprovalState.Cancelled;
        request.DecidedAtMs = nowMs;
        return new ApprovalVoteResult(true, $"request {request.Id} cancelled by '{username}'", request.State, true);
    }

    /// <summary>Stamp what happened when the approved action actually ran. Approved only: executing
    /// something that was rejected, expired or never approved is precisely the event this whole plan
    /// exists to prevent, so it is refused here rather than recorded.
    /// <para>Separate from <see cref="ApplyVote"/> because approval and execution are two events with
    /// two actors, and the audit log wants both — <see cref="IApprovalFacade.RecordOutcomeAsync"/> says
    /// so already.</para></summary>
    public static ApprovalVoteResult RecordOutcome(ApprovalRequest request, bool executed, string outcome, long nowMs)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Plan 015 wave 8: one more transition than "out of Approved", and exactly one.
        //
        // The executor has to CLAIM a request before it can know how the run went — the claim is what
        // makes execution at-most-once when two approvers vote at the same moment — so it records
        // `executed: true` first and only then finds out whether the action threw. Without the second
        // clause below, a run that threw left the request reading Executed forever: the audit row was
        // correct and the request's own state over-read it. A wave about the record being true cannot
        // ship a record that says an action succeeded because it was attempted.
        //
        // Narrow on purpose — Executed may be corrected to Failed and nothing else. A general
        // re-statement would let any terminal state be rewritten, which is how an append-only-ish record
        // stops being one; and correcting a Failed to Executed would let a retry launder a failure.
        var correctingAnOverclaim = request.State == ApprovalState.Executed && !executed;

        if (request.State != ApprovalState.Approved && !correctingAnOverclaim)
        {
            return new ApprovalVoteResult(
                false,
                $"request {request.Id} is {request.State.ToString().ToLowerInvariant()}, not approved; no outcome to record",
                request.State,
                false);
        }

        request.State = executed ? ApprovalState.Executed : ApprovalState.Failed;
        request.Outcome = outcome;
        request.DecidedAtMs = nowMs;
        return new ApprovalVoteResult(true, $"request {request.Id} {request.State.ToString().ToLowerInvariant()}", request.State, true);
    }

    // ---------------------------------------------------------------- expiry and escalation

    /// <summary>
    /// One sweep over a batch: which pending requests change state at <paramref name="nowMs"/>, and how.
    /// Mutates the ones that change and returns exactly those, so the caller persists the changed rows
    /// and audits them without re-deriving anything.
    ///
    /// <para><b>Expiry is checked before escalation</b>, and expiry is terminal: escalating a request
    /// that is already past its deadline would widen an approver pool for a request nobody can approve
    /// any more.</para>
    ///
    /// <para><b>Escalation happens exactly once</b>, latched on
    /// <see cref="ApprovalRequest.EscalatedAtMs"/> rather than on a deadline field. There is no
    /// <c>EscalateAtMs</c> on the request — the contract is frozen and this wave needs nothing new — so
    /// the escalation deadline is derived as <c>RequestedAtMs + EscalateAfterSeconds</c> from the
    /// template, which is the one place a template IS re-read after filing. That is deliberate and it is
    /// the harmless direction: the worst an edited template can do here is move when the approver pool
    /// widens, never how many approvals are needed. A deleted template means no escalation at all
    /// (nothing knows which groups to add) while expiry still works, because the expiry deadline was
    /// snapshotted onto the request at filing time.</para>
    ///
    /// <para>Every sweep after the first therefore reports nothing for an already-escalated request,
    /// which is what stops the approver group growing on every tick and the inbox notifying every
    /// tick.</para>
    /// </summary>
    /// <param name="requests">Candidates. Non-pending ones are skipped, so the caller may hand over
    /// everything it has rather than pre-filtering.</param>
    /// <param name="templates">The document's templates, looked up by
    /// <see cref="ApprovalRequest.TemplateName"/>. A request whose template is gone still expires.</param>
    /// <param name="nowMs">Unix ms.</param>
    public static IReadOnlyList<ApprovalSweepChange> Sweep(
        IEnumerable<ApprovalRequest> requests,
        IReadOnlyList<ApprovalTemplate> templates,
        long nowMs)
    {
        ArgumentNullException.ThrowIfNull(requests);
        ArgumentNullException.ThrowIfNull(templates);

        var changes = new List<ApprovalSweepChange>();

        foreach (var request in requests)
        {
            if (request is null || request.State != ApprovalState.Pending)
            {
                continue;
            }

            if (IsPastDeadline(request, nowMs))
            {
                Expire(request, nowMs);
                changes.Add(new ApprovalSweepChange(request, ApprovalSweepAction.Expired));
                continue;
            }

            if (request.EscalatedAtMs is not null)
            {
                continue;
            }

            var template = templates.FirstOrDefault(t => string.Equals(t.Name, request.TemplateName, StringComparison.Ordinal));
            if (template is null || template.EscalateAfterSeconds <= 0 || template.EscalationGroups.Count == 0)
            {
                // 0 = never, and escalating to nobody is not an escalation. Both are silent no-ops
                // rather than reported changes: a sweep that reported a change every tick for a template
                // with no escalation groups would make the sweeper's own count meaningless.
                continue;
            }

            if (nowMs < request.RequestedAtMs + (template.EscalateAfterSeconds * 1000L))
            {
                continue;
            }

            // Widen, never replace: the original approvers do not lose their say because a deadline
            // passed. Deduped ordinally against what is already there so a second escalation path (or a
            // template listing a group twice) cannot double an entry the UI would then show twice.
            foreach (var group in template.EscalationGroups)
            {
                if (!request.ApproverGroups.Contains(group, StringComparer.Ordinal))
                {
                    request.ApproverGroups.Add(group);
                }
            }

            request.EscalatedAtMs = nowMs;
            changes.Add(new ApprovalSweepChange(request, ApprovalSweepAction.Escalated));
        }

        return changes;
    }

    /// <summary><c>0</c> means never, so the comparison is deliberately not a bare <c>nowMs &gt;=
    /// ExpiresAtMs</c>. The boundary is inclusive: at exactly the deadline, the request is over.</summary>
    private static bool IsPastDeadline(ApprovalRequest request, long nowMs) =>
        request.ExpiresAtMs > 0 && nowMs >= request.ExpiresAtMs;

    private static void Expire(ApprovalRequest request, long nowMs)
    {
        request.State = ApprovalState.Expired;
        request.DecidedAtMs = nowMs;
    }
}

/// <summary>
/// Whether a voter is in one of a request's approver groups — <b>decided by the caller</b>, because
/// answering it needs the policy document and <see cref="ApprovalStateMachine"/> deliberately cannot see
/// one.
///
/// <para>This is an enum rather than a <c>bool</c> for two reasons that are both about the call site.
/// <c>ApplyVote(request, vote, true, now)</c> does not say what is true; <c>VoterEligibility.Eligible</c>
/// does. And <c>default(VoterEligibility)</c> is <see cref="NotAnApprover"/>, so a zeroed field, a
/// forgotten assignment or a <c>default</c> in a test fails closed — a bool has the same property in one
/// direction only if you remember which way round it was written.</para>
///
/// <para>It has no default parameter value anywhere it is used, so it cannot be accidentally
/// omitted: the compiler asks for it.</para>
/// </summary>
public enum VoterEligibility
{
    /// <summary>The refusing value, and the <c>default</c>, on purpose.</summary>
    NotAnApprover = 0,
    Eligible = 1,
}

/// <summary>What a transition did. <see cref="Reason"/> is a sentence for a 4xx body and an audit row —
/// the same argument <see cref="AccessResult"/> makes: "no" with no explanation is the failure mode that
/// makes a control unusable.
///
/// <para><see cref="StateChanged"/> is NOT <c>Accepted</c>. A vote arriving after the deadline is
/// refused <i>and</i> moves the request to <see cref="ApprovalState.Expired"/>, so the store has to
/// persist a request whose vote it just rejected.</para></summary>
public sealed record ApprovalVoteResult(bool Accepted, string Reason, ApprovalState State, bool StateChanged);

public enum ApprovalSweepAction { Expired = 0, Escalated = 1 }

/// <summary>One request the sweep changed, already mutated. <see cref="ApprovalSweepChange.Request"/> is
/// the same instance the caller passed in.</summary>
public sealed record ApprovalSweepChange(ApprovalRequest Request, ApprovalSweepAction Action);
