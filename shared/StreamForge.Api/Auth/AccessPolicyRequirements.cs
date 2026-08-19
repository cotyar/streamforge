using Microsoft.AspNetCore.Authorization;
using StreamForge.Abstractions;

namespace StreamForge.Api.Auth;

/// <summary>
/// Plan 015 wave 2 — backward compatibility, expressed structurally rather than promised.
///
/// <para>The three policy names <c>Viewer</c> / <c>Editor</c> / <c>Admin</c> stay registered, so all 68
/// <c>RequireAuthorization(…)</c> sites and all 30 gRPC <c>[Authorize(Policy = …)]</c> attributes keep
/// compiling and behaving exactly as they do today. What changes is what satisfies them: an
/// <b>OR</b> of the entitlement and the legacy role, never an AND. Wave 3 migrates the call sites to
/// scoped per-resource guards because it should, not because it must.</para>
///
/// <para>This is why the policies cannot be written with <c>RequireRole(…)</c>: ASP.NET AND-s the
/// requirements in a policy, and the requirement here has to succeed on <i>either</i> route. One
/// requirement holding both halves is the only shape that gets an OR.</para>
/// </summary>
/// <param name="permission">The entitlement that stands for this policy. Checked at scope <c>*</c> —
/// see the ponytail note on <see cref="LegacyPolicyHandler"/>.</param>
/// <param name="legacyRoles">The roles <c>RequireRole</c> named before this plan.</param>
public sealed class LegacyPolicyRequirement(string permission, string[] legacyRoles) : IAuthorizationRequirement
{
    public string Permission { get; } = permission;
    public IReadOnlyList<string> LegacyRoles { get; } = legacyRoles;
}

/// <summary>Plan 015's one intentional behaviour change (<c>Auth:StrictViewer</c>, default
/// <c>true</c>): the <c>Viewer</c> policy — today <c>RequireAuthenticatedUser()</c> and nothing else —
/// stops admitting a principal whose user is disabled or whose role no longer exists. Without it,
/// disabling an account means nothing until the account's 12-hour token expires, and "disable the
/// compromised login" is the first thing anybody actually does.</summary>
public sealed class StrictViewerRequirement : IAuthorizationRequirement;

/// <summary>Satisfies a <see cref="LegacyPolicyRequirement"/> from either direction.</summary>
internal sealed class LegacyPolicyHandler(AccessGuard guard) : AuthorizationHandler<LegacyPolicyRequirement>
{
    protected override async Task HandleRequirementAsync(
        AuthorizationHandlerContext context,
        LegacyPolicyRequirement requirement)
    {
        // The legacy route first: it reads a claim already in memory, and in Auth:Mode=legacy it is the
        // ONLY thing that runs. ClaimsPrincipal.IsInRole is false for an unauthenticated principal, so
        // this is behaviourally identical to the RolesAuthorizationRequirement it replaces.
        foreach (var role in requirement.LegacyRoles)
        {
            if (context.User.IsInRole(role))
            {
                context.Succeed(requirement);
                return;
            }
        }

        if (!guard.EntitlementsEnabled)
        {
            // Not merely "skip the check": AccessGuard.CheckAsync answers Allowed unconditionally in
            // legacy mode, so calling it here would satisfy the Editor policy for every authenticated
            // user. Legacy means the resolver is never consulted, and this line is where that is true.
            return;
        }

        // ponytail: the policy asks at scope "*", so it is satisfied only by a "*"-scoped grant.
        // Ceiling: a principal entitled to catalog.write on `prod-*` and nothing else fails the Editor
        // POLICY and never reaches the route that would have scoped the check properly. Acceptable
        // because the three built-in roles are all "*"-scoped (so nothing that works today breaks) and
        // because a coarse gate has no resource to scope against — it runs before the handler has read
        // one. Upgrade path is wave 3 itself: as each route replaces RequireAuthorization("Editor") with
        // a scoped AccessGuard.CheckAsync(user, action, thatResource), this requirement stops being the
        // thing that decides. Widening it here instead — "holds the action at ANY scope" — would silently
        // defeat a Deny written at "*", which is the one direction an authorization change must not go.
        var result = await guard.CheckAsync(context.User, requirement.Permission, "*").ConfigureAwait(false);
        if (result.IsAllowed)
        {
            context.Succeed(requirement);
        }

        // A RequiresApproval answer deliberately does NOT satisfy a coarse policy: there is nothing here
        // to file an approval against, and treating "needs a second pair of eyes" as "yes" at the door
        // would make the approval flow bypassable by any route that had not been migrated yet.
    }
}

/// <summary>Enforces <see cref="StrictViewerRequirement"/>.</summary>
internal sealed class StrictViewerHandler(AccessGuard guard, PermissionResolver resolver, bool strict)
    : AuthorizationHandler<StrictViewerRequirement>
{
    protected override async Task HandleRequirementAsync(
        AuthorizationHandlerContext context,
        StrictViewerRequirement requirement)
    {
        if (!guard.EntitlementsEnabled || !strict)
        {
            // Byte-identical to today: the policy is then RequireAuthenticatedUser() and nothing more.
            context.Succeed(requirement);
            return;
        }

        var document = await resolver.GetPolicyAsync().ConfigureAwait(false);

        // Two ways to have no policy to be strict about, and locking every account out of a running
        // cluster is the wrong answer to both:
        //   - the store has never answered (HasSnapshot false) — a grain or sidecar problem, during
        //     which every other endpoint is failing anyway and a mass 403 only obscures the real fault;
        //   - the document has no roles at all — a pre-upgrade catalog whose AccessBootstrapService
        //     migration has not landed yet, where EVERY principal's role would look like it "no longer
        //     exists".
        if (!resolver.HasSnapshot || document.Roles.Count == 0)
        {
            context.Succeed(requirement);
            return;
        }

        var permissions = PermissionResolver.Build(document, context.User);
        if (permissions.Disabled)
        {
            return;
        }

        // "…or whose role no longer exists." EffectivePermissionsBuilder keeps stale role names in the
        // list on purpose (an admin screen must show that a user references a deleted role), so the
        // check is against the document. A direct grant or a group membership is an independent reason
        // to be here, so either one is enough on its own — a user can legitimately hold entitlements
        // and no role at all.
        var stillKnown =
            permissions.Grants.Count > 0
            || permissions.Groups.Count > 0
            || permissions.Roles.Any(name => document.Roles.Any(r => string.Equals(r.Name, name, StringComparison.Ordinal)));

        if (stillKnown)
        {
            context.Succeed(requirement);
        }
    }
}

/// <summary>The permission each legacy policy name is satisfied by, named once so the registration and
/// the tests cannot drift.</summary>
public static class LegacyPolicyPermissions
{
    /// <summary><c>catalog.write</c> — stated by <see cref="Actions.CatalogWrite"/>'s own doc comment as
    /// the permission the Editor policy is satisfied by, and held by the built-in Editor and Admin
    /// roles.</summary>
    public const string Editor = Actions.CatalogWrite;

    /// <summary><c>access.write</c>. The only Admin-gated surface today is <c>/api/users</c>, so
    /// <c>user.write</c> was the obvious candidate — and the wrong one. Whichever permission stands for
    /// the Admin policy becomes the key to <i>every</i> Admin-gated route, including the
    /// <c>/api/access</c> routes landing in this same wave. Pick <c>user.write</c> and a narrowly
    /// intended "user administrator" role silently gains the power to rewrite the entitlement document;
    /// pick <c>access.write</c> and the same role is merely refused <c>/api/users</c> until wave 3
    /// migrates that group to a per-action guard. Over-granting and under-granting are not symmetric
    /// mistakes, so the choice goes to the one that fails closed. It is also the honest reading of what
    /// this policy means: <c>access.write</c> is the entitlement from which every other entitlement can
    /// be self-granted, which is exactly what "Admin" claimed to be.</summary>
    public const string Admin = Actions.AccessWrite;
}
