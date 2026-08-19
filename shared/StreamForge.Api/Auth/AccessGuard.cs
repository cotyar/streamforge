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
public sealed class AccessGuard(PermissionResolver resolver, bool entitlementsEnabled)
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
            return LegacyPass;
        }

        var permissions = await resolver.ResolveAsync(user).ConfigureAwait(false);
        return PermissionEvaluator.Evaluate(permissions, action, scope, resourceTags);
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
