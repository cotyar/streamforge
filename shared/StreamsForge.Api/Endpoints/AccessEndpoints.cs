using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;
using StreamsForge.AppCore.Access;

namespace StreamsForge.Api;

/// <summary>
/// Plan 015 wave 2-C — the REST surface over the access policy: roles, groups, per-user entitlements and
/// approval templates, plus the one read route that answers "what can this user actually do".
///
/// <para><b>Two gates on every route, on purpose.</b> The group carries
/// <c>RequireAuthorization("Admin")</c> — the compatibility floor, meaning exactly what it means on
/// <c>/api/users</c> — and each handler then checks its specific action
/// (<see cref="Actions.AccessRead"/> or <see cref="Actions.AccessWrite"/>) through
/// <see cref="AccessGuard"/>. That is the pattern wave 3 rolls out across the whole surface, and doing
/// it here first is deliberate: the routes that administer entitlements should be the first routes
/// expressed in entitlements. Nothing that works today stops working, because the Admin policy is
/// satisfied by <c>access.write</c> at <c>*</c> <b>or</b> the legacy Admin role claim — see
/// <see cref="LegacyPolicyPermissions.Admin"/> for why <c>access.write</c> and not <c>user.write</c>.</para>
///
/// <para>ponytail: the READ routes are gated by <c>access.read</c> in the handler but still sit behind
/// the group's Admin floor, so a would-be "auditor" holding <c>access.read</c> alone never reaches
/// them. Ceiling: read-only visibility into the policy cannot be granted separately yet. Upgrade path
/// is wave 3's own job — when it replaces this group's <c>RequireAuthorization("Admin")</c> with the
/// per-action guards already written below, the read routes open to <c>access.read</c> and the write
/// routes stay closed, with no change to any handler. Splitting the group in two now would mean two
/// <c>MapGroup</c>s and two policies for a distinction nothing can express until then.</para>
///
/// <para><b>Why the request bodies are the stored models themselves.</b> <see cref="RoleDefinition"/>,
/// <see cref="GroupDefinition"/>, <see cref="UserAccessEntry"/> and <see cref="ApprovalTemplate"/> are
/// bound straight off the wire rather than mirrored into four near-identical request DTOs: wave 0
/// already published every one of them in <c>web/src/api/types.ts</c>, so a PUT body is a shape the
/// client already has a type for, and a fifth copy of each would be four more things to keep in step
/// for no gain. What the server does NOT trust from those bodies is stated per route below — the name
/// comes from the path, the actor comes from the token, and the store stamps its own timestamps.</para>
/// </summary>
public static class AccessEndpoints
{
    public static void MapAccessEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/access").RequireAuthorization("Admin");

        // ------------------------------------------------------------------------------------------
        // Reads
        // ------------------------------------------------------------------------------------------

        // GET /api/access — the whole document in one call: roles, groups, user entries, approval
        // templates and the version they were read at. Deliberately not four list routes; this is the
        // same small document the resolver already fetches whole on every version change, and an admin
        // screen needs all four lists at once to render a single grant against a role and a group.
        group.MapGet("/", async (ClaimsPrincipal principal, AccessGuard guard, IAccessPolicyFacade policy) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessRead, "*") is { } refusal)
            {
                return refusal;
            }

            // Read through the FACADE, not the resolver's cache: an administrator who just wrote must
            // see their own write, and the resolver's snapshot is allowed to be up to a TTL stale.
            return Results.Ok(await policy.GetPolicyAsync());
        });

        // GET /api/access/effective/{username} — "what can this user actually do", flattened exactly the
        // way an authorization decision flattens it.
        group.MapGet("/effective/{username}", async (string username, ClaimsPrincipal principal, AccessGuard guard, PermissionResolver resolver) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessRead, username) is { } refusal)
            {
                return refusal;
            }

            // Built from the RESOLVER's snapshot, and that is the point of the route: a misconfigured
            // entitlement is debuggable only if the admin sees the document the evaluator is actually
            // answering from, version stamp and all. Reading the store instead would show what the
            // cluster is about to believe rather than what it believes now, which is the harder thing
            // to reason about at 3am — and the version in the response says which snapshot answered.
            var document = await resolver.GetPolicyAsync();

            // No role claim and no group claim are passed: this answers for a NAME, not for a live
            // principal, so there is no token to fall back to and no IdP claim to map. Against a
            // pre-upgrade catalog whose migration has not run, that means an empty answer for a user
            // whose capability today comes from their token's role — which is the honest report, not a
            // gap: it says "the document grants this user nothing", and that is exactly true.
            return Results.Ok(EffectivePermissionsBuilder.Build(document, username));
        });

        // ------------------------------------------------------------------------------------------
        // Roles
        // ------------------------------------------------------------------------------------------

        group.MapPut("/roles/{name}", async (string name, RoleDefinition body, ClaimsPrincipal principal, AccessGuard guard, IAccessPolicyFacade policy, PermissionResolver resolver) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessWrite, name) is { } refusal)
            {
                return refusal;
            }

            body.Name = name;

            // BuiltIn is derived, never accepted. The store's delete guard keys off the NAME, so a
            // forged flag could not make Viewer deletable — but it COULD make the SPA offer a delete
            // button that always fails, or hide one that would work. Deriving it here means the flag
            // and the rule that reads it can never disagree.
            body.BuiltIn = BuiltInRoles.All.Contains(name, StringComparer.Ordinal);

            var stored = await policy.UpsertRoleAsync(body, ActorOf(principal));
            if (stored is null)
            {
                return Results.BadRequest(new ErrorResponse("role name is required"));
            }

            resolver.Invalidate();
            return Results.Ok(stored);
        });

        group.MapDelete("/roles/{name}", async (string name, ClaimsPrincipal principal, AccessGuard guard, IAccessPolicyFacade policy, PermissionResolver resolver) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessWrite, name) is { } refusal)
            {
                return refusal;
            }

            // The store answers false for BOTH "built-in" and "not there", which as a 404 would tell an
            // operator that Viewer does not exist. Splitting the two here costs one line and is the
            // difference between a confusing bug report and none.
            if (BuiltInRoles.All.Contains(name, StringComparer.Ordinal))
            {
                return Results.Json(
                    new ErrorResponse($"'{name}' is a built-in role and cannot be deleted — deleting it would strand every pre-upgrade token. Edit its grants instead."),
                    statusCode: StatusCodes.Status409Conflict);
            }

            if (!await policy.DeleteRoleAsync(name))
            {
                return Results.NotFound();
            }

            resolver.Invalidate();
            return Results.NoContent();
        });

        // ------------------------------------------------------------------------------------------
        // Groups
        // ------------------------------------------------------------------------------------------

        group.MapPut("/groups/{name}", async (string name, GroupDefinition body, ClaimsPrincipal principal, AccessGuard guard, IAccessPolicyFacade policy, PermissionResolver resolver) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessWrite, name) is { } refusal)
            {
                return refusal;
            }

            body.Name = name;

            var stored = await policy.UpsertGroupAsync(body, ActorOf(principal));
            if (stored is null)
            {
                return Results.BadRequest(new ErrorResponse("group name is required"));
            }

            resolver.Invalidate();
            // The STORED object, not the body: the store carries CreatedAtMs forward on an update and
            // ignores whatever the caller sent, and returning the caller's copy would hide that.
            return Results.Ok(stored);
        });

        group.MapDelete("/groups/{name}", async (string name, ClaimsPrincipal principal, AccessGuard guard, IAccessPolicyFacade policy, PermissionResolver resolver) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessWrite, name) is { } refusal)
            {
                return refusal;
            }

            if (!await policy.DeleteGroupAsync(name))
            {
                return Results.NotFound();
            }

            resolver.Invalidate();
            return Results.NoContent();
        });

        // ------------------------------------------------------------------------------------------
        // Per-user access entries
        // ------------------------------------------------------------------------------------------

        group.MapPut("/users/{username}", async (string username, UserAccessEntry body, ClaimsPrincipal principal, AccessGuard guard, IAccessPolicyFacade policy, PermissionResolver resolver) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessWrite, username) is { } refusal)
            {
                return refusal;
            }

            body.Username = username;

            var stored = await policy.UpsertUserAccessAsync(body, ActorOf(principal));
            if (stored is null)
            {
                return Results.BadRequest(new ErrorResponse("username is required"));
            }

            resolver.Invalidate();
            return Results.Ok(stored);
        });

        // PUT /api/access/users/{username}/disabled — the cheap 90% of token revocation, as one call.
        //
        // It exists separately from the whole-entry PUT above for the reason the whole-entry PUT is
        // dangerous in a hurry: disabling a compromised login is done under pressure, and a caller that
        // had to send the complete entry to flip one boolean would sooner or later send it without the
        // grants it did not know about. This one reads, flips, writes — nothing else on the entry moves.
        // It is also why UpdateUserRequest's shape is untouched: "disabled" is policy, and policy lives
        // in the access document, not on the credential record (AccessModels.cs's file header).
        group.MapPut("/users/{username}/disabled", async (string username, SetAccessDisabledRequest req, ClaimsPrincipal principal, AccessGuard guard, IAccessPolicyFacade policy, IUserStoreFacade userStore, PermissionResolver resolver) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessWrite, username) is { } refusal)
            {
                return refusal;
            }

            var document = await policy.GetPolicyAsync();
            var existing = document.Users.FirstOrDefault(u => string.Equals(u.Username, username, StringComparison.Ordinal));

            var entry = new UserAccessEntry
            {
                Username = username,
                Disabled = req.Disabled,
                // Everything on an EXISTING entry survives verbatim.
                //
                // When there is no entry, the roles are seeded from the credential record instead of
                // left empty, and that is not tidiness — it is the difference between disabling a login
                // and demoting it permanently. EffectivePermissionsBuilder consults the JWT's role claim
                // ONLY while the document has no entry for the user (`entry is not null` suppresses the
                // fallback, whatever Roles holds), so an entry carrying nothing but Disabled=true is not
                // a neutral placeholder: it is an entry that says "this user has no roles". Re-enabling
                // then returns a user with zero grants who can sign in and do nothing. Wave 6's console
                // reproduced it end to end on a seeded `editor` — disable, enable, 403 on
                // POST /api/pipelines.
                //
                // The original comment here reasoned that LegacyRoleMigration would fill the gap in
                // later, and it does — at the NEXT HOST START. Between the write and that restart the
                // demotion is live and silent, which is the whole window an administrator disabling an
                // account during an incident is operating in.
                //
                // Seeded exactly the way UsersEndpoints.MirrorUserRoleAsync seeds it, from the stored
                // record rather than from anything the caller sent. A user store that cannot answer
                // leaves the list empty, which is the pre-existing behaviour and no worse than it.
                Roles = existing is not null
                    ? [.. existing.Roles]
                    : await SeedRolesFromCredentialAsync(userStore, username),
                Grants = existing is null ? [] : [.. existing.Grants],
            };

            var stored = await policy.UpsertUserAccessAsync(entry, ActorOf(principal));
            if (stored is null)
            {
                return Results.BadRequest(new ErrorResponse("username is required"));
            }

            // Without this the replica that just disabled the account would keep serving its own
            // pre-disable snapshot for up to a full TTL — to the very administrator watching to see
            // whether the revocation took.
            resolver.Invalidate();
            return Results.Ok(stored);
        });

        group.MapDelete("/users/{username}", async (string username, ClaimsPrincipal principal, AccessGuard guard, IAccessPolicyFacade policy, PermissionResolver resolver) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessWrite, username) is { } refusal)
            {
                return refusal;
            }

            // Deleting the ENTRY is not deleting the user: the credential record in the user store is
            // untouched and the login keeps working, now falling back to the token's role claim. The
            // route that removes both is DELETE /api/users/{username}.
            if (!await policy.DeleteUserAccessAsync(username))
            {
                return Results.NotFound();
            }

            resolver.Invalidate();
            return Results.NoContent();
        });

        // ------------------------------------------------------------------------------------------
        // Approval templates — stored and editable now, inert until Approvals:Enabled (wave 4)
        // ------------------------------------------------------------------------------------------

        group.MapPut("/approval-templates/{name}", async (string name, ApprovalTemplate body, ClaimsPrincipal principal, AccessGuard guard, IAccessPolicyFacade policy, PermissionResolver resolver) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessWrite, name) is { } refusal)
            {
                return refusal;
            }

            body.Name = name;

            var stored = await policy.UpsertApprovalTemplateAsync(body, ActorOf(principal));
            if (stored is null)
            {
                return Results.BadRequest(new ErrorResponse("template name is required"));
            }

            resolver.Invalidate();
            return Results.Ok(stored);
        });

        group.MapDelete("/approval-templates/{name}", async (string name, ClaimsPrincipal principal, AccessGuard guard, IAccessPolicyFacade policy, PermissionResolver resolver) =>
        {
            if (await RefuseAsync(guard, principal, Actions.AccessWrite, name) is { } refusal)
            {
                return refusal;
            }

            if (!await policy.DeleteApprovalTemplateAsync(name))
            {
                return Results.NotFound();
            }

            resolver.Invalidate();
            return Results.NoContent();
        });
    }

    /// <summary>Null when the caller may proceed; the ready-made 403 when they may not.
    ///
    /// <para>A <see cref="AccessDecision.RequiresApproval"/> answer is refused here too, carrying its own
    /// reason text ("grant … requires approval"). Filing the request instead is waves 4-5's job and the
    /// machinery does not exist yet; refusing is the only answer that cannot be wrong in the meantime,
    /// and it fails closed rather than treating "needs a second pair of eyes" as yes.</para></summary>
    private static async Task<IResult?> RefuseAsync(AccessGuard guard, ClaimsPrincipal principal, string action, string scope)
    {
        var result = await guard.CheckAsync(principal, action, scope);
        return result.IsAllowed ? null : AccessGuard.Deny(result);
    }

    /// <summary>Who the store records as having made the change. The authenticated caller and never
    /// anything from the request body — the models carry an <c>UpdatedBy</c> field, and a caller that
    /// could set it could write somebody else's name into the record of their own edit.</summary>
    /// <summary>The one role the credential record carries, or nothing when there is no such record or
    /// the store cannot be read. Never throws: this runs on the disable path, and failing to disable a
    /// login because a lookup that only improves the entry's completeness went wrong would be strictly
    /// worse than an incomplete entry.</summary>
    public static async Task<List<string>> SeedRolesFromCredentialAsync(IUserStoreFacade userStore, string username)
    {
        try
        {
            var user = (await userStore.GetUsersAsync())
                .FirstOrDefault(u => string.Equals(u.Username, username, StringComparison.Ordinal));
            return user is null || string.IsNullOrWhiteSpace(user.Role) ? [] : [user.Role];
        }
        catch (Exception)
        {
            return [];
        }
    }

    private static string ActorOf(ClaimsPrincipal principal) => principal.Identity?.Name ?? "";
}
