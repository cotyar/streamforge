using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;

namespace StreamForge.Api;

/// <summary>
/// Admin-gated CRUD over the credential store — and, since plan 015 wave 2-C, the place where
/// <c>UserRecord.Role</c> is mirrored into <see cref="UserAccessEntry.Roles"/>.
///
/// <para><b>The gap this closes.</b> Plan 015 moved the effective role list off the credential record
/// and into the access document, on purpose: a resolver that had to read the user store to learn a user
/// is disabled would be caching exactly the password hashes the split exists to keep out of memory. The
/// consequence is that the two now have to be kept in step, and nothing was doing it at runtime —
/// <c>AccessBootstrapService</c> mirrors every user once per host start, so a user created or
/// role-changed through these routes had NO entry (or a stale one) until the next restart and silently
/// fell back to whatever their 12-hour token claimed. Safe, but it made "revocation lands in ~10s" only
/// half true, and the missing half is the half an administrator actually tries.</para>
///
/// <para><b>Why the mirror here overwrites a role list the startup migration refuses to touch.</b>
/// <c>LegacyRoleMigration</c> leaves a non-empty <see cref="UserAccessEntry.Roles"/> alone because it
/// cannot tell an administrator's deliberate edit from a stale mirror, and re-mirroring would be a
/// privilege change performed by a migration. This code can tell: the administrator is asking for it,
/// in this request. The flip side is stated where it happens, on
/// <see cref="MirrorUserRoleAsync"/>.</para>
/// </summary>
public static class UsersEndpoints
{
    /// <summary>Named once so the two failure paths log under the same category.</summary>
    private const string LoggerCategory = "StreamForge.Api.UsersEndpoints";

    public static void MapUsersEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/users").RequireAuthorization("Admin");

        group.MapGet("/", async (IUserStoreFacade userStore) =>
        {
            var users = await userStore.GetUsersAsync();
            return Results.Ok(users.Select(ToUserInfo).ToList());
        });

        group.MapPost("/", async (
            CreateUserRequest req,
            ClaimsPrincipal principal,
            IUserStoreFacade userStore,
            IAccessPolicyFacade policy,
            PermissionResolver resolver,
            ILoggerFactory loggerFactory) =>
        {
            if (string.IsNullOrWhiteSpace(req.Username) || string.IsNullOrWhiteSpace(req.Password))
            {
                return Results.BadRequest(new ErrorResponse("username and password are required"));
            }

            var created = await userStore.CreateUserAsync(req.Username, req.DisplayName, req.Role, req.Password);
            if (!created)
            {
                return Results.BadRequest(new ErrorResponse("username already exists"));
            }

            var users = await userStore.GetUsersAsync();
            var user = users.First(u => u.Username == req.Username);

            // Mirrored from the STORED record rather than from req.Role, so whatever normalization or
            // defaulting the user store applied is what the access document ends up agreeing with.
            if (await MirrorOrExplainAsync(policy, resolver, loggerFactory, user, ActorOf(principal), "created") is { } failure)
            {
                return failure;
            }

            return Results.Created($"/api/users/{user.Username}", ToUserInfo(user));
        });

        group.MapPut("/{username}", async (
            string username,
            UpdateUserRequest req,
            ClaimsPrincipal principal,
            IUserStoreFacade userStore,
            IAccessPolicyFacade policy,
            PermissionResolver resolver,
            ILoggerFactory loggerFactory) =>
        {
            var updated = await userStore.UpdateUserAsync(username, req.DisplayName, req.Role, req.Password);
            if (!updated)
            {
                return Results.NotFound();
            }

            var users = await userStore.GetUsersAsync();
            var user = users.First(u => u.Username == username);

            // Unconditional, not "only when req.Role is non-null": a display-name-only edit on a user
            // whose entry is missing or stale is a free opportunity to repair it, and the mirror skips
            // the write when the document already agrees, so the repeated call costs one read and no
            // version bump.
            if (await MirrorOrExplainAsync(policy, resolver, loggerFactory, user, ActorOf(principal), "updated") is { } failure)
            {
                return failure;
            }

            return Results.Ok(ToUserInfo(user));
        });

        group.MapDelete("/{username}", async (
            string username,
            ClaimsPrincipal principal,
            IUserStoreFacade userStore,
            IAccessPolicyFacade policy,
            PermissionResolver resolver,
            ILoggerFactory loggerFactory) =>
        {
            if (string.Equals(principal.Identity?.Name, username, StringComparison.Ordinal))
            {
                return Results.BadRequest(new ErrorResponse("cannot delete yourself"));
            }

            var removed = await userStore.DeleteUserAsync(username);
            if (!removed)
            {
                return Results.NotFound();
            }

            // The access entry has to go with the credential record, and not for tidiness: a username
            // recreated later would otherwise inherit the deleted user's Disabled flag and direct
            // grants, because the mirror below preserves precisely those two things. "Create alice, she
            // is immediately disabled" is a genuinely baffling bug, and this line is what prevents it.
            try
            {
                if (await policy.DeleteUserAccessAsync(username))
                {
                    resolver.Invalidate();
                }
            }
            catch (Exception ex)
            {
                var message =
                    $"user '{username}' was deleted, but removing their access entry failed: {ex.Message}. "
                    + "The credential record IS gone and the login no longer works, so nothing is over-granted "
                    + "right now — but the stale entry would be inherited by a user recreated under the same "
                    + "name, Disabled flag and direct grants included. Delete it explicitly with "
                    + $"DELETE /api/access/users/{username}, or retry this request.";

                loggerFactory.CreateLogger(LoggerCategory).LogError(ex, "{Message}", message);
                return Results.Json(new ErrorResponse(message), statusCode: StatusCodes.Status500InternalServerError);
            }

            return Results.NoContent();
        });
    }

    /// <summary>Mirrors <see cref="UserRecord.Role"/> into the user's <see cref="UserAccessEntry"/>,
    /// preserving everything else on the entry.
    ///
    /// <para><b>What must survive a role edit, and why.</b> <see cref="UserAccessEntry.Disabled"/> and
    /// <see cref="UserAccessEntry.Grants"/> are carried forward verbatim. Clearing <c>Disabled</c> would
    /// mean a routine "make bob a Viewer" silently re-enabled a login somebody disabled during an
    /// incident — a role edit re-granting access is the exact failure this whole plan exists to prevent.
    /// Clearing <c>Grants</c> would throw away every per-resource entitlement an administrator wrote,
    /// with no warning and no record of what was lost.</para>
    ///
    /// <para><b>What does NOT survive: extra role names.</b> The list is replaced with exactly the one
    /// role the credential record carries, because <see cref="UserRecord.Role"/> is a single string and
    /// this mirror's whole contract is "the two say the same thing". So a second role added through
    /// <c>PUT /api/access/users/{username}</c> is dropped by the next <c>/api/users</c> role edit.
    /// ponytail: that is the accepted ceiling of mirroring a one-valued field onto a list. Upgrade path,
    /// when multi-role users become a real feature rather than a shape the model allows: read the record
    /// BEFORE the update, and swap only the role name it used to hold instead of replacing the list.
    /// Doing that now would need the pre-update read on every request to buy a behaviour nothing in the
    /// product yet produces.</para>
    ///
    /// <para>Reads through the facade rather than the resolver's snapshot: the snapshot is allowed to be
    /// up to a TTL stale, and a stale read here would write back grants a concurrent administrative edit
    /// had just removed.</para>
    ///
    /// <para>Returns the stored entry, or the existing one when nothing needed writing. Exceptions are
    /// deliberately NOT caught — the caller has a user write it cannot roll back and has to say so.</para></summary>
    public static async Task<UserAccessEntry?> MirrorUserRoleAsync(
        IAccessPolicyFacade policy,
        PermissionResolver resolver,
        string username,
        string role,
        string actor)
    {
        var document = await policy.GetPolicyAsync();
        var existing = document.Users.FirstOrDefault(u => string.Equals(u.Username, username, StringComparison.Ordinal));

        // Already in step: skip the write. Every write bumps AccessPolicyDocument.Version and therefore
        // invalidates every replica's policy cache, so a mirror that wrote unconditionally would do that
        // on every display-name edit in the system.
        if (existing is not null
            && existing.Roles.Count == 1
            && string.Equals(existing.Roles[0], role, StringComparison.Ordinal))
        {
            return existing;
        }

        var stored = await policy.UpsertUserAccessAsync(
            new UserAccessEntry
            {
                Username = username,
                Roles = [role],
                Disabled = existing?.Disabled ?? false,
                Grants = existing is null ? [] : [.. existing.Grants],
            },
            actor);

        // The write moved the version; without this the replica that made it would keep serving its own
        // pre-change snapshot for up to a full TTL, to the administrator who just made the change.
        resolver.Invalidate();
        return stored;
    }

    /// <summary>Null when the mirror landed; a 500 that names the split state when it did not.
    ///
    /// <para><b>Why 500 and not a quiet 200.</b> The user write has already committed and there is no
    /// transaction spanning the two stores, so one of the two answers has to be partly wrong. A 200
    /// would claim the role change is in force when it is not — an administrator who just demoted a
    /// compromised account would walk away believing the demotion took. A 500 overstates only the
    /// blast radius, and the body says exactly what did and did not happen, including that the fix is
    /// to send the same request again (the mirror is an idempotent upsert) or to restart the host,
    /// after which AccessBootstrapService mirrors every user once.</para></summary>
    private static async Task<IResult?> MirrorOrExplainAsync(
        IAccessPolicyFacade policy,
        PermissionResolver resolver,
        ILoggerFactory loggerFactory,
        UserRecord user,
        string actor,
        string what)
    {
        try
        {
            await MirrorUserRoleAsync(policy, resolver, user.Username, user.Role, actor);
            return null;
        }
        catch (Exception ex)
        {
            var message =
                $"user '{user.Username}' was {what}, but mirroring role '{user.Role}' into the access policy "
                + $"failed: {ex.Message}. The credential record IS written; until the mirror lands, this user's "
                + "entitlements are whatever the access policy said before this request — their previous roles, "
                + "or their token's role claim if they have no entry at all — so a role CHANGE is not yet "
                + "enforced. Retry the same request (the mirror is an idempotent upsert), or restart the host: "
                + "AccessBootstrapService mirrors every user's role once per start.";

            loggerFactory.CreateLogger(LoggerCategory).LogError(ex, "{Message}", message);
            return Results.Json(new ErrorResponse(message), statusCode: StatusCodes.Status500InternalServerError);
        }
    }

    private static string ActorOf(ClaimsPrincipal principal) => principal.Identity?.Name ?? "";

    private static UserInfo ToUserInfo(UserRecord u) => new(u.Username, u.DisplayName, u.Role, u.CreatedAtMs);
}
