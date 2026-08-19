using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.AppCore.Access;

namespace StreamForge.Api.Auth;

/// <summary>
/// Plan 015 wave 2 — the one thing an endpoint calls to ask "may this principal do X to Y".
///
/// <para>Deliberately tiny, because every endpoint in the platform is going to call it and its
/// <i>shape</i> matters far more than its cleverness. Two members do the whole job:</para>
/// <code>
/// var decision = await guard.CheckAsync(user, Actions.PipelineWrite, id, definition.Tags);
/// if (decision.Decision == AccessDecision.Denied)           return AccessGuard.Deny(decision);
/// if (decision.Decision == AccessDecision.RequiresApproval) return /* wave 3/5: file an approval */;
/// </code>
///
/// <para><b>RequiresApproval is not a 403.</b> It comes back as its own
/// <see cref="AccessDecision"/> so a caller cannot accidentally collapse it into a refusal — waves 3 and
/// 5 turn it into "file an approval request", and a guard that returned a 403 for it would have made the
/// whole approval feature unreachable from the outside.</para>
///
/// <para><b>In <c>Auth:Mode=legacy</c> the resolver is not consulted at all</b> — not cached, not polled,
/// not constructed on the hot path. That is what makes the flag a genuine one-flag rollback rather than
/// a mode that still pays for the feature it disabled. <see cref="CheckAsync"/> answers
/// <see cref="AccessDecision.Allowed"/> with a reason that says so, so a caller that logs the reason
/// leaves an honest audit trail of "not enforced".</para>
/// </summary>
/// <param name="audit">Plan 015 wave 4-D. Optional, and optional on purpose: the guard's job is the
/// decision, and a deployment (or a unit test) without an audit sink must behave identically. What it
/// records and — more importantly — what it deliberately does not is
/// <see cref="AuditActionPolicy"/>.</param>
/// <param name="recordAllowedMutations"><c>Audit:RecordAllowedMutations</c>, default true. The one knob
/// worth having: a deployment whose mutation rate makes even that too much can turn it off and keep the
/// refusals, which are the rows nobody should ever be allowed to turn off.</param>
public sealed class AccessGuard(
    PermissionResolver resolver,
    bool entitlementsEnabled,
    IAuditSink? audit = null,
    bool recordAllowedMutations = true)
{
    /// <summary><c>Auth:Mode == entitlements</c>. Public because the authorization handlers have to know
    /// not to call <see cref="CheckAsync"/> at all in legacy mode — an unconditional Allowed would
    /// satisfy the Editor policy for everybody.</summary>
    public bool EntitlementsEnabled { get; } = entitlementsEnabled;

    private static readonly AccessResult LegacyPass =
        new(AccessDecision.Allowed, "Auth:Mode=legacy — entitlements are not enforced", null);

    /// <summary>The decision, tri-state, with a reason fit for a 403 body and an audit row.</summary>
    /// <param name="user">The authenticated principal.</param>
    /// <param name="action">A concrete <see cref="Actions"/> constant, never a pattern.</param>
    /// <param name="scope">The resource's id or name, or <c>"*"</c> for an operation with no single
    /// resource. Note that <c>"*"</c> is answered only by a <c>*</c>-scoped grant.</param>
    /// <param name="resourceTags">The resource's <c>Tags</c>, so <c>tag:finance</c> scopes can match.
    /// Omitting them can only ever narrow the answer.</param>
    public async Task<AccessResult> CheckAsync(
        ClaimsPrincipal user,
        string action,
        string scope,
        IReadOnlyCollection<string>? resourceTags = null)
    {
        if (!EntitlementsEnabled)
        {
            // Nothing is enforced, so there is no decision to record: a log full of "allowed — not
            // enforced" is noise that makes the real rows harder to find, and Auth:Mode=legacy is
            // defined as "the feature is off", audit included. Wave 5's mutation-site rows are a
            // different question and are written wherever the mutation happens, not here.
            return LegacyPass;
        }

        var permissions = await resolver.ResolveAsync(user).ConfigureAwait(false);
        var result = PermissionEvaluator.Evaluate(permissions, action, scope, resourceTags);
        Audit(user, action, scope, result);
        return result;
    }

    /// <summary>
    /// One audit row, when <see cref="AuditActionPolicy"/> says this decision is worth one.
    ///
    /// <para><b>This is the only place a plain REST/gRPC/SignalR decision becomes an audit row</b>,
    /// because <see cref="CheckAsync"/> is the only place every decision passes through — putting it at
    /// the call sites would mean sixty of them and a hole wherever somebody forgot.</para>
    ///
    /// <para>The try/catch is not defensive decoration. The rule the whole audit design is built on is
    /// that <b>audit must never make a request fail or slow</b>; <see cref="AuditChannelSink"/> cannot
    /// throw, but the guard must not depend on that being true of whatever sink is registered next
    /// year.</para>
    /// </summary>
    private void Audit(ClaimsPrincipal user, string action, string scope, AccessResult result)
    {
        if (audit is null)
        {
            return;
        }

        if (result.Decision == AccessDecision.Allowed
            && (!recordAllowedMutations || !AuditActionPolicy.RecordsAllowed(action)))
        {
            return;
        }

        try
        {
            audit.Record(new AuditEntry
            {
                Id = Guid.NewGuid().ToString("n"),
                AtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                // The human. The chat's rows come from ChatToolGate instead, which sets Actor to the
                // model and OnBehalfOf to the human — those two must never collapse into one field, and
                // this guard has no way to know a model is involved, which is exactly why the chat
                // builds its own rows.
                Actor = user.Identity?.Name ?? "(anonymous)",
                Action = action,
                Scope = scope,
                Outcome = AuditActionPolicy.OutcomeOf(result.Decision),
                Detail = result.Reason,
                // ponytail: every guard-level row is attributed to "rest". Ceiling: a gRPC or SignalR
                // decision is recorded as if it arrived over REST. Fixing it honestly means an origin
                // parameter on CheckAsync and an edit at every call site — three of which belong to
                // other agents this wave. Upgrade path is exactly that parameter, defaulted to "rest",
                // on the day the call sites are open. The chat, the one origin that genuinely matters
                // for attribution, already carries its own.
                Origin = "rest",
            });
        }
        catch (Exception)
        {
            // Swallowed deliberately and without a log call: a sink that throws on every decision would
            // otherwise turn one bad dependency into a log storm on the request path — the second-worst
            // version of the thing being prevented. The sink owns its own reporting.
        }
    }

    /// <summary>The ready-made 403, carrying the decision's reason.
    ///
    /// <para>Reuses the platform's existing <see cref="ErrorResponse"/> body rather than inventing a
    /// richer one: <c>ConfigEndpoints</c> already answers a refusal with exactly this shape
    /// (<c>{"error":"replace mode requires the Admin role"}</c>), and every client in
    /// <c>clients/**</c> already reads it. <see cref="AccessResult.Reason"/> is written to be read by a
    /// human staring at a 403 — it names the grant that denied, or says that none matched.</para></summary>
    public static IResult Deny(AccessResult result) =>
        Results.Json(new ErrorResponse(result.Reason), statusCode: StatusCodes.Status403Forbidden);
}
