using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;

namespace StreamForge.Api;

/// <summary>
/// Plan 021 wave 1 (track C) — <c>/api/environments</c> CRUD over <see cref="IEnvironmentFacade"/>.
///
/// <para>Three routes, not four: there is no rename route. D7 refuses renaming an environment outright,
/// for the same reason plan 011 D2 refuses renaming a sharded table — the name is qualified into every
/// grain key, actor id and stream id that entity ever produced, and a rename would silently orphan
/// everything already written under the old name.</para>
///
/// <para><b>Two gates on the write routes, the wave 2-C pattern <see cref="AccessEndpoints"/>
/// established.</b> <c>RequireAuthorization("Admin")</c> on the route is the compatibility floor; the
/// handler then asks <see cref="AccessGuard"/> a specific question. Creating and deleting an environment
/// is D7's own description of "the one genuinely destructive operation this plan adds" — force-deleting
/// one erases catalog AND runtime state for everything in it — so both stay Admin-only twice over.</para>
/// </summary>
public static class EnvironmentsEndpoints
{
    public static void MapEnvironmentsEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/environments");

        // GET /api/environments — Viewer. Actions.CatalogRead is the closest fit already in the ViewerActions
        // bundle (BuiltInRoleCatalog.cs) for "read something about the catalog as a whole" — an
        // environment list is exactly that, one level up from any single source/pipeline/table.
        group.MapGet("/", async (ClaimsPrincipal principal, AccessGuard guard, IEnvironmentFacade environments) =>
        {
            var check = await guard.CheckAsync(principal, Actions.CatalogRead, "*");
            if (!check.IsAllowed)
            {
                return AccessGuard.Deny(check);
            }

            return Results.Ok(await environments.ListAsync());
        }).RequireAuthorization("Viewer");

        // POST /api/environments — Admin.
        group.MapPost("/", async (CreateEnvironmentRequest req, ClaimsPrincipal principal, AccessGuard guard, IEnvironmentFacade environments) =>
        {
            // ponytail: reusing Actions.AccessWrite — literally the permission LegacyPolicyPermissions.Admin
            // uses to define the coarse Admin policy itself — because there is no environment-scoped
            // action and AccessModels.cs is frozen for this wave. Ceiling: "may rewrite the access
            // policy" and "may create/delete environments" cannot be granted apart; no entitlement today
            // can hand out one without the other. Upgrade path is one more Actions constant (e.g.
            // `environment.write`) once AccessModels.cs reopens — the route's own
            // RequireAuthorization("Admin") is the real ceiling regardless, since D7 calls this the one
            // genuinely destructive operation the plan adds and wants it Admin-only on principle, not
            // only until a narrower entitlement exists.
            var check = await guard.CheckAsync(principal, Actions.AccessWrite, "*");
            if (!check.IsAllowed)
            {
                return AccessGuard.Deny(check);
            }

            if (string.IsNullOrWhiteSpace(req.Name))
            {
                return Results.BadRequest(new ErrorResponse("name is required"));
            }

            try
            {
                var created = await environments.CreateAsync(req.Name.Trim(), req.Description ?? "", ActorOf(principal));
                return Results.Ok(created);
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new ErrorResponse(ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Admin");

        // DELETE /api/environments/{name}?force=true — Admin. force=false (the default) refuses a
        // non-empty environment; force=true deletes its catalog AND the runtime state of everything in
        // it (D7). "default" is refused by the facade itself, always — see IEnvironmentFacade.DeleteAsync.
        group.MapDelete("/{name}", async (string name, bool? force, ClaimsPrincipal principal, AccessGuard guard, IEnvironmentFacade environments) =>
        {
            // Same action, same ceiling comment as the create route above.
            var check = await guard.CheckAsync(principal, Actions.AccessWrite, name);
            if (!check.IsAllowed)
            {
                return AccessGuard.Deny(check);
            }

            try
            {
                var deleted = await environments.DeleteAsync(name, force ?? false);
                return deleted ? Results.NoContent() : Results.NotFound();
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new ErrorResponse(ex.Message));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new ErrorResponse(ex.Message));
            }
        }).RequireAuthorization("Admin");
    }

    private static string ActorOf(ClaimsPrincipal principal) => principal.Identity?.Name ?? "";
}
