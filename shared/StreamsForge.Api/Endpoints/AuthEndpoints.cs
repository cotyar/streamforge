using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamsForge.Abstractions;
using StreamsForge.Api.Auth;

namespace StreamsForge.Api;

public static class AuthEndpoints
{
    public static void MapAuthEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/auth");

        group.MapPost("/login", async (LoginRequest req, HttpContext http, IUserStoreFacade userStore, JwtTokenService jwt) =>
        {
            var user = await userStore.ValidateCredentialsAsync(req.Username, req.Password);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            var token = jwt.CreateToken(user);

            // Same token, second carrier: an httpOnly cookie the JWT wiring honours only on the
            // documentation paths (see DocsAuthCookie), because a Scalar page cannot send a header.
            // The response body is unchanged — every other caller keeps using the Bearer token.
            DocsAuthCookie.Append(http, token, JwtTokenService.Lifetime);

            return Results.Ok(new LoginResponse(token, user.Username, user.DisplayName, user.Role));
        // Anonymous by construction — it is how you GET a token. Declared rather than left implicit
        // because plan 015's endpoint-metadata test reads authorization metadata off every /api/ route,
        // and "nobody marked it" and "deliberately open" have to be distinguishable from the outside.
        }).AllowAnonymous();

        // Drops the documentation cookie. Anonymous and side-effect-free by construction: it can only
        // clear a cookie on the caller's own browser. The SPA's logout is otherwise purely client-side
        // (it just forgets the token), so without this the cookie would outlive the session by up to
        // the token's 12h.
        group.MapPost("/logout", (HttpContext http) =>
        {
            DocsAuthCookie.Delete(http);
            return Results.NoContent();
        }).AllowAnonymous();

        // The caller's own identity, and since plan 015 wave 2-C the caller's own entitlements with it.
        //
        // WHY THIS ROUTE AND NOT A NEW ONE. The SPA already calls /me on every load to learn who it is
        // talking as; a separate /api/auth/permissions would be a second round trip on the critical path
        // to answer half of the same question, and the two answers could disagree.
        //
        // WHAT THE PAYLOAD PROMISES. The four original fields are unchanged and unconditional. The five
        // added ones are optional in web/src/api/types.ts and are omitted from the JSON entirely when
        // null (see UserInfo in Dtos.cs), because the SPA reads a MISSING permissions[] as "an old
        // server" and falls back to today's ordinal Viewer < Editor < Admin semantics. That is what makes
        // a rolling deploy safe in both directions: an old client ignores the extra fields, and a new
        // client against an old server degrades to role ordering instead of locking the user out of
        // every button.
        group.MapGet("/me", async (ClaimsPrincipal principal, IUserStoreFacade userStore, PermissionResolver resolver, AccessGuard guard) =>
        {
            var username = principal.Identity?.Name ?? "";
            var users = await userStore.GetUsersAsync();
            var user = users.FirstOrDefault(u => u.Username == username);
            if (user is null)
            {
                return Results.NotFound();
            }

            if (!guard.EntitlementsEnabled)
            {
                // Auth:Mode=legacy is a rollback of the whole feature, which has to include not paying
                // for it: the resolver is not consulted, not polled and not cached. The client then sees
                // no permissions[] — indistinguishable from a pre-015 server, which is precisely the
                // behaviour a rollback is supposed to restore.
                return Results.Ok(new UserInfo(user.Username, user.DisplayName, user.Role, user.CreatedAtMs));
            }

            var permissions = await resolver.ResolveAsync(principal);
            return Results.Ok(new UserInfo(
                user.Username,
                user.DisplayName,
                user.Role,
                user.CreatedAtMs,
                permissions.Grants,
                permissions.Roles,
                permissions.Groups,
                permissions.Disabled,
                // The snapshot the answer was computed from. A client that caches permissions can tell
                // that they moved, and a bug report that quotes it says which document decided.
                permissions.Version));
        }).RequireAuthorization("Viewer");
    }
}
