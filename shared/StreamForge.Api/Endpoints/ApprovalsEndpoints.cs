using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;
using StreamForge.AppCore.Access;

namespace StreamForge.Api;

/// <summary>
/// Plan 015 wave 5-A — the REST surface over approvals: file, list, read, approve, reject, cancel.
///
/// <para><b>Two gates on every route, copied verbatim from wave 2-C's
/// <see cref="AccessEndpoints"/>.</b> The group carries <c>RequireAuthorization("Viewer")</c> — here
/// that is not a compatibility floor (these routes are new and have no legacy equivalent) but the
/// deliberate statement that <i>asking</i> for a second pair of eyes is not a privilege: the built-in
/// Viewer role already holds <see cref="Actions.ApprovalRequest"/> at <c>*</c>
/// (<see cref="BuiltInRoleCatalog"/> says why), so a build where only privileged people could file a
/// request would be the feature shipping dead. Each handler then checks its own action through
/// <see cref="AccessGuard"/>: <see cref="Actions.ApprovalRequest"/> to file,
/// <see cref="Actions.ApprovalDecide"/> to vote.</para>
///
/// <para><b>Voting is behind TWO independent controls and both are required.</b> The route checks
/// <see cref="Actions.ApprovalDecide"/> at the request's scope; the STORE, separately and
/// unreachably-from-here, checks that the voter is in one of the request's approver groups and is not
/// the requester (<see cref="ApprovalStateMachine.ApplyVote"/>). Neither substitutes for the other, and
/// the store's half is the one that still holds in <c>Auth:Mode=legacy</c>, where
/// <see cref="AccessGuard"/> allows everything by definition. That is why the route may safely sit on a
/// Viewer floor: the second-pair-of-eyes rule does not live at this layer at all.</para>
///
/// <para><b>Three things the store cannot do and this file must.</b>
/// <list type="number">
///   <item><see cref="ApprovalRequest.RequestedBy"/> is <b>always</b> the authenticated principal,
///   overwritten server-side and never read off the body — see <see cref="FileAsync"/>. The frozen
///   <see cref="IApprovalFacade.RequestAsync"/> takes the requester off the draft precisely because the
///   route is supposed to have done this, and the entire self-vote rule rests on it: a caller who could
///   file "as" somebody else defeats the control in one field.</item>
///   <item>Approve and reject are the SAME transition with a different boolean, and the vote's
///   <see cref="ApprovalVote.Username"/>/<see cref="ApprovalVote.AtMs"/> are server-set — a
///   caller-supplied voter is an impersonation and a caller-supplied timestamp is a caller-supplied
///   place in the ordering.</item>
///   <item>The facade answers a refused transition with a bare <c>null</c> (the Dapr flavour) or with
///   the unchanged request (the Orleans flavour) — the reason sentence cannot cross the frozen
///   interface. So the routes below re-read the request and turn the observable post-state into
///   something a human can act on. See <see cref="ExplainRefusedVote"/>: that is an EXPLANATION of a
///   decision the store already made, never a second decision.</item>
/// </list></para>
///
/// <para><b>Who sees which approvals</b> — <see cref="Visibility"/>, and the one interesting design
/// question in this file. <b>When approvals are disabled</b> — the shipped default — every route
/// answers 503 with a sentence; see <see cref="Disabled"/> for why that and not an empty list.</para>
/// </summary>
public static class ApprovalsEndpoints
{
    public static void MapApprovalsEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/approvals").RequireAuthorization("Viewer");

        // POST /api/approvals — file a request. Origin is stamped "rest": the chat files through
        // ApprovalStoreChatFiler with its own origin, and the two must never collapse (an LLM-proposed
        // action has to be visibly distinguishable in the inbox, not only in the audit log).
        group.MapPost("/", FileAsync);

        // GET /api/approvals — the inbox, filtered by Visibility.
        group.MapGet("/", ListAsync);

        // GET /api/approvals/{id} — one request, subject to the same visibility rule as the listing.
        group.MapGet("/{id}", GetAsync);

        // The two halves of one transition. Two routes rather than one with a boolean because a UI
        // button maps onto a URL, and "POST /reject" is a thing you can find in an access log.
        group.MapPost("/{id}/approve", (string id, ApprovalDecisionRequest? body, ClaimsPrincipal principal, AccessGuard guard, IApprovalFacade approvals, ApprovalOptions options) =>
            VoteAsync(id, approve: true, body, principal, guard, approvals, options));

        group.MapPost("/{id}/reject", (string id, ApprovalDecisionRequest? body, ClaimsPrincipal principal, AccessGuard guard, IApprovalFacade approvals, ApprovalOptions options) =>
            VoteAsync(id, approve: false, body, principal, guard, approvals, options));

        // POST /api/approvals/{id}/cancel — withdrawing your own request. NO approval.decide check: a
        // cancel is not a vote, it is the requester taking back what they asked for, and requiring the
        // deciding entitlement would mean the only people who could withdraw a request are the people
        // who could have approved it. The state machine already refuses everybody else.
        group.MapPost("/{id}/cancel", CancelAsync);
    }

    // ==============================================================================================
    // Filing
    // ==============================================================================================

    /// <summary>
    /// File a request for a privileged action.
    ///
    /// <para><b>The draft is built here, field by field, from a four-field DTO</b> rather than binding
    /// <see cref="ApprovalRequest"/> off the wire. <see cref="ApprovalStateMachine.CreateRequest"/> is
    /// already a whitelist that discards everything a caller could use to pre-decide its own request, so
    /// binding the full model would be safe — but it would also publish a request shape carrying
    /// <c>Votes</c>, <c>State</c> and <c>RequiredApprovals</c> as if a client had any business sending
    /// them. <see cref="FileApprovalRequest"/> has exactly the four fields a caller genuinely owns.</para>
    ///
    /// <para><b>The scope is checked, not just recorded.</b> <see cref="Actions.ApprovalRequest"/> is
    /// asked at the scope being requested, so "you may ask about <c>dev-*</c> but not about
    /// <c>prod-*</c>" is expressible. The built-in Viewer grant is at <c>*</c>, so nothing an existing
    /// deployment could do stops working.</para>
    /// </summary>
    private static async Task<IResult> FileAsync(
        FileApprovalRequest body,
        ClaimsPrincipal principal,
        AccessGuard guard,
        IApprovalFacade approvals,
        ApprovalOptions options)
    {
        if (Disabled(options) is { } off)
        {
            return off;
        }

        if (string.IsNullOrWhiteSpace(body.Action))
        {
            return Results.BadRequest(new ErrorResponse("an approval request must name the action it is asking for"));
        }

        var actor = ActorOf(principal);
        if (string.IsNullOrWhiteSpace(actor))
        {
            // The store refuses an unattributed request (an anonymous filing makes the self-vote rule
            // vacuous), and it refuses it by throwing. Answering here keeps that a 403 with a sentence
            // rather than a 500 with a stack trace — and it cannot normally happen behind the group's
            // authorization floor, which is exactly why it would be baffling if it did.
            return Results.Json(
                new ErrorResponse("an approval request must be filed by a named principal; this token carries no username"),
                statusCode: StatusCodes.Status403Forbidden);
        }

        var scope = string.IsNullOrWhiteSpace(body.Scope) ? "*" : body.Scope;
        if (await RefuseAsync(guard, principal, Actions.ApprovalRequest, scope) is { } refusal)
        {
            return refusal;
        }

        var draft = new ApprovalRequest
        {
            Action = body.Action,
            Scope = scope,
            Reason = body.Reason ?? "",
            PayloadJson = body.PayloadJson,

            // ALWAYS the authenticated principal. Not from the body, not defaulted, not trusted from
            // anywhere else — see the type remarks.
            RequestedBy = actor,

            // Everything else on the draft is discarded by ApprovalStateMachine.CreateRequest, which is
            // where that property is structural rather than promised.
            Origin = "rest",
        };

        try
        {
            var stored = await approvals.RequestAsync(draft);
            return Results.Created($"/api/approvals/{stored.Id}", stored);
        }
        catch (InvalidOperationException ex)
        {
            // "No enabled template covers this action/scope" — i.e. nobody is configured to approve it,
            // so nothing was filed. A 409 carrying the store's own sentence, the same convention the
            // catalog routes already use for a refused mutation with a reason.
            return Results.Conflict(new ErrorResponse(ex.Message));
        }
    }

    // ==============================================================================================
    // Reads
    // ==============================================================================================

    /// <summary>
    /// The inbox. <paramref name="state"/> filters by <see cref="ApprovalState"/> (the common query is
    /// <c>?state=Pending</c>); <paramref name="limit"/> is the store page size.
    ///
    /// <para><b>The limit is always sent as a positive number, deliberately.</b> The two flavours
    /// disagree about <c>limit &lt;= 0</c> — Orleans returns everything, Dapr returns its own default
    /// page — so a route that forwarded a caller's 0 would return different data on different runtimes.
    /// Clamped to [1, <see cref="MaxListLimit"/>] here, so both answer identically.</para>
    ///
    /// <para>ponytail: the store applies the limit BEFORE this handler applies
    /// <see cref="Visibility"/>, so a caller whose visible requests are all older than the newest N sees
    /// fewer than N rows — or none. Ceiling: the inbox is "your requests among the most recent N", not
    /// "your N most recent requests". Upgrade path: pass the visibility predicate into the store (which
    /// means the store reading the policy document it already reads for eligibility) or page until N
    /// visible rows are found. Neither is worth it while the pending set is small by design — which is
    /// what <c>StreamConstants.ApprovalsKey</c>'s own doc comment assumes.</para>
    /// </summary>
    private static async Task<IResult> ListAsync(
        ClaimsPrincipal principal,
        PermissionResolver resolver,
        IApprovalFacade approvals,
        ApprovalOptions options,
        ApprovalState? state = null,
        int limit = DefaultListLimit)
    {
        if (Disabled(options) is { } off)
        {
            return off;
        }

        var rows = await approvals.ListAsync(state, Math.Clamp(limit, 1, MaxListLimit));
        var visibility = await Visibility.ForAsync(principal, resolver);

        // Lists filter; they do not refuse — wave 3's rule, and for the same reason: a caller entitled
        // to nothing gets 200 [] rather than a 403, because a list is not an entity.
        return Results.Ok(rows.Where(visibility.CanSee).ToList());
    }

    private static async Task<IResult> GetAsync(
        string id,
        ClaimsPrincipal principal,
        PermissionResolver resolver,
        IApprovalFacade approvals,
        ApprovalOptions options)
    {
        if (Disabled(options) is { } off)
        {
            return off;
        }

        var request = await approvals.GetAsync(id);
        if (request is null)
        {
            return Results.NotFound();
        }

        var visibility = await Visibility.ForAsync(principal, resolver);
        if (!visibility.CanSee(request))
        {
            // 403 with a reason rather than 404. Wave 3 hid existence behind the guard because entity
            // NAMES are guessable and a 404-vs-403 difference enumerates the catalog; an approval id is
            // a random GUID, so there is nothing to enumerate and the actionable answer wins — somebody
            // who was handed a link needs to be told to ask an administrator, not that the link is dead.
            return Results.Json(
                new ErrorResponse(
                    $"request {id} was filed by '{request.RequestedBy}' and you are neither its requester nor an entitled approver for it"),
                statusCode: StatusCodes.Status403Forbidden);
        }

        return Results.Ok(request);
    }

    // ==============================================================================================
    // Transitions
    // ==============================================================================================

    /// <summary>
    /// One vote. Approve and reject differ by <paramref name="approve"/> and by nothing else.
    ///
    /// <para><b>The request is loaded before the guard runs</b>, which is the opposite of wave 3's
    /// "guard before the 404" rule and for a reason that only applies here: the entitlement is checked
    /// at the REQUEST's scope, and there is no way to know that scope without reading the request. The
    /// enumeration risk wave 3 was closing does not exist for a random GUID.</para>
    ///
    /// <para><b>Did the vote land?</b> The facade cannot say — see the type remarks. So: the stored
    /// request is re-read, and the vote counted if it now carries this caller's vote with this caller's
    /// answer AND the request is in a state that could have been reached with it counted. Deliberately
    /// clock-free: comparing the stored <see cref="ApprovalVote.AtMs"/> against a timestamp taken here
    /// would make the answer depend on the skew between this process's clock and whichever silo the
    /// store activated on.</para>
    /// </summary>
    private static async Task<IResult> VoteAsync(
        string id,
        bool approve,
        ApprovalDecisionRequest? body,
        ClaimsPrincipal principal,
        AccessGuard guard,
        IApprovalFacade approvals,
        ApprovalOptions options)
    {
        if (Disabled(options) is { } off)
        {
            return off;
        }

        var before = await approvals.GetAsync(id);
        if (before is null)
        {
            return Results.NotFound();
        }

        var actor = ActorOf(principal);
        if (await RefuseAsync(guard, principal, Actions.ApprovalDecide, before.Scope) is { } refusal)
        {
            return refusal;
        }

        await approvals.VoteAsync(id, new ApprovalVote
        {
            Username = actor,
            Approve = approve,

            // AtMs and the vote's identity are server-owned. ApprovalStateMachine.ApplyVote overwrites
            // AtMs with its own clock in any case; leaving it 0 here says so at the call site.
            Comment = body?.Comment,
        });

        // Re-read rather than trusting the return: Orleans hands back the request whether the vote was
        // accepted or refused, Dapr hands back null when it was refused. One extra read (a vote is a
        // human clicking a button, not a hot path) makes both flavours answer identically.
        var after = await approvals.GetAsync(id) ?? before;

        return VoteLanded(after, actor, approve)
            ? Results.Ok(after)
            : ExplainRefusedVote(after, actor);
    }

    /// <summary>Withdraw your own request. No entitlement check — see the map site.</summary>
    private static async Task<IResult> CancelAsync(
        string id,
        ClaimsPrincipal principal,
        IApprovalFacade approvals,
        ApprovalOptions options)
    {
        if (Disabled(options) is { } off)
        {
            return off;
        }

        var actor = ActorOf(principal);
        var before = await approvals.GetAsync(id);
        if (before is null)
        {
            return Results.NotFound();
        }

        await approvals.CancelAsync(id, actor);
        var after = await approvals.GetAsync(id) ?? before;

        if (after.State == ApprovalState.Cancelled)
        {
            // Includes "it was already cancelled", which the state machine refuses as a transition and
            // which is nevertheless exactly what the caller asked for. Idempotent on purpose: the body
            // says Cancelled either way and there is nothing for a client to do differently.
            return Results.Ok(after);
        }

        return string.Equals(after.RequestedBy, actor, StringComparison.OrdinalIgnoreCase)
            ? Results.Conflict(new ErrorResponse(
                $"request {id} is {Lower(after.State)} and cannot be cancelled"))
            : Results.Json(
                new ErrorResponse($"only '{after.RequestedBy}' may cancel request {id}"),
                statusCode: StatusCodes.Status403Forbidden);
    }

    // ==============================================================================================
    // Visibility — the rule for who sees which approvals
    // ==============================================================================================

    /// <summary>
    /// Who may see one approval request.
    ///
    /// <para><b>Three ways in, and the argument for each.</b>
    /// <list type="number">
    ///   <item><b>The administrator</b> — a caller entitled to <see cref="Actions.AccessRead"/> at
    ///   <c>*</c> sees everything. That is the same entitlement that already reads the whole access
    ///   policy document (<see cref="AccessEndpoints"/>), so it grants nothing new; and somebody has to
    ///   be able to answer "what is stuck in this deployment".</item>
    ///   <item><b>The requester</b> — you always see what you filed, whatever became of it. Anything
    ///   else means filing a request and then losing track of it, which is the one thing a requester
    ///   needs the inbox for. Compared case-INSENSITIVELY, matching
    ///   <see cref="ApprovalStateMachine"/>'s self-vote comparison exactly: the two rules read the same
    ///   field, and having them disagree about capitalisation is how you get a request you can see and
    ///   cannot cancel (or worse, the reverse).</item>
    ///   <item><b>The entitled approver</b> — a member of one of the request's own
    ///   <see cref="ApprovalRequest.ApproverGroups"/> who is also allowed
    ///   <see cref="Actions.ApprovalDecide"/> at the request's scope. <b>Both halves are required, and
    ///   that is the whole design.</b> Group membership alone would show "the reviewers group" every
    ///   request routed to it even where the viewer is not entitled to act; the entitlement alone would
    ///   turn <c>approval.decide</c> at <c>*</c> into a firehose of what every other team is doing.
    ///   Together they mean the listing shows exactly what the caller can act on — the same two controls
    ///   the vote route enforces, asked one at a time.</item>
    /// </list></para>
    ///
    /// <para><b>Evaluated directly, never through <see cref="AccessGuard"/>.</b> The guard audits every
    /// refusal, always, and it is right to — but a FILTER is not a refusal. Running one guard check per
    /// candidate row would write a denied audit row for every approval a caller merely did not have in
    /// their inbox, on every poll of the list, which is precisely the flood that makes an audit log
    /// unreadable and makes "a refusal is rare by construction" stop being true. The evaluator is the
    /// same code the guard calls; it just has no side effect.</para>
    ///
    /// <para>A consequence worth stating rather than discovering: because this reads the policy DOCUMENT
    /// rather than the guard, it behaves identically in <c>Auth:Mode=legacy</c>, where the guard allows
    /// everything. The inbox is therefore stricter than the routes in that mode. That is the closed
    /// direction, it keeps one rule instead of two, and approvals ship disabled anyway.</para>
    /// </summary>
    private sealed class Visibility(string username, EffectivePermissions permissions, bool seesEverything)
    {
        public static async Task<Visibility> ForAsync(ClaimsPrincipal principal, PermissionResolver resolver)
        {
            var permissions = await resolver.ResolveAsync(principal);
            return new Visibility(
                principal.Identity?.Name ?? "",
                permissions,
                PermissionEvaluator.Evaluate(permissions, Actions.AccessRead, "*").IsAllowed);
        }

        public bool CanSee(ApprovalRequest request) =>
            seesEverything
            || string.Equals(request.RequestedBy, username, StringComparison.OrdinalIgnoreCase)
            || (request.ApproverGroups.Any(g => permissions.Groups.Contains(g, StringComparer.Ordinal))
                && PermissionEvaluator.Evaluate(permissions, Actions.ApprovalDecide, request.Scope).IsAllowed);
    }

    // ==============================================================================================
    // Helpers
    // ==============================================================================================

    private const int DefaultListLimit = 100;
    private const int MaxListLimit = 500;

    /// <summary>
    /// Every route's first line when <c>Approvals:Enabled=false</c> — the shipped default.
    ///
    /// <para><b>503 with a sentence, not an empty list.</b> Wave 3-C's chat filer settled the principle:
    /// be honest rather than mint an id nobody can act on. The same argument runs the other way for a
    /// read — an inbox that answers <c>[]</c> when the feature is switched off is indistinguishable from
    /// an inbox with nothing in it, so the one person who needs to know that approvals are inert (the
    /// administrator wondering why nothing is escalating) is told "all clear". A 503 naming the config
    /// key is a thing a console can render and a thing an operator can fix.</para>
    ///
    /// <para>503 rather than 404: the routes exist, the feature is not turned on. That is the same
    /// answer <c>POST /api/chat</c> gives when no model key is configured, which is the precedent this
    /// repo already set for "the route is real, the capability is not configured".</para>
    /// </summary>
    private static IResult? Disabled(ApprovalOptions options) =>
        options.Enabled
            ? null
            : Results.Json(
                new ErrorResponse(
                    $"approvals are not enabled on this deployment ({ApprovalOptions.EnabledKey}=false), so nothing can be filed, "
                    + "listed or decided. An administrator turns them on; nothing is queued in the meantime."),
                statusCode: StatusCodes.Status503ServiceUnavailable);

    /// <summary>Did this caller's vote actually get recorded?
    ///
    /// <para>True when the stored request carries a vote from this caller with this caller's answer AND
    /// is in a state the vote could have produced. The state test is what keeps a LATE vote on an
    /// already-expired request — whose earlier vote from the same caller is still in the list — from
    /// reading as success.</para></summary>
    private static bool VoteLanded(ApprovalRequest after, string actor, bool approve) =>
        after.State is ApprovalState.Pending or ApprovalState.Approved or ApprovalState.Rejected
        && after.Votes.Any(v => string.Equals(v.Username, actor, StringComparison.Ordinal) && v.Approve == approve);

    /// <summary>
    /// Why the store refused, reconstructed from what the store stored.
    ///
    /// <para><b>This is an explanation, not a decision.</b> The decision was made by
    /// <see cref="ApprovalStateMachine.ApplyVote"/> and is already final by the time this runs — the
    /// only reason it is rebuilt here is that <see cref="IApprovalFacade"/> is frozen and has nowhere to
    /// carry a sentence (the store logs it). The order matches the state machine's own refusal order, so
    /// the sentence a caller reads is the sentence the log holds: terminal state first, then the
    /// requester's own vote, then eligibility. If the two ever disagreed the request would still be
    /// refused — the vote did not land, that is what got us here.</para>
    ///
    /// <para>409 for "the request is not open any more" (nothing about the caller is wrong; the world
    /// moved) and 403 for the two authorization refusals, which is the distinction a client needs to
    /// decide between "reload the inbox" and "ask an administrator".</para>
    /// </summary>
    private static IResult ExplainRefusedVote(ApprovalRequest after, string actor)
    {
        if (after.State != ApprovalState.Pending)
        {
            return Results.Conflict(new ErrorResponse(
                $"request {after.Id} is {Lower(after.State)} and accepts no further votes"));
        }

        if (string.Equals(after.RequestedBy, actor, StringComparison.OrdinalIgnoreCase))
        {
            return Results.Json(
                new ErrorResponse($"'{actor}' filed request {after.Id} and cannot vote on it"),
                statusCode: StatusCodes.Status403Forbidden);
        }

        return Results.Json(
            new ErrorResponse(
                $"'{actor}' is not an approver for request {after.Id}"
                + (after.ApproverGroups.Count == 0
                    ? " — its template named no approver group, so nobody can approve it and it can only expire"
                    : $" — it is decided by {string.Join(", ", after.ApproverGroups)}")),
            statusCode: StatusCodes.Status403Forbidden);
    }

    private static string Lower(ApprovalState state) => state.ToString().ToLowerInvariant();

    /// <summary>Null when the caller may proceed; the ready-made 403 when they may not. Identical to
    /// <see cref="AccessEndpoints"/>' helper, including its treatment of
    /// <see cref="AccessDecision.RequiresApproval"/> as a refusal — an approval request that itself
    /// needed approving is a loop, and the honest answer is the guard's own sentence.</summary>
    private static async Task<IResult?> RefuseAsync(AccessGuard guard, ClaimsPrincipal principal, string action, string scope)
    {
        var result = await guard.CheckAsync(principal, action, scope);
        return result.IsAllowed ? null : AccessGuard.Deny(result);
    }

    /// <summary>The authenticated caller, and nothing from the request body.</summary>
    private static string ActorOf(ClaimsPrincipal principal) => principal.Identity?.Name ?? "";
}
