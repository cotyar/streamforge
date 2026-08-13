using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.Api.Auth;

namespace StreamForge.Api;

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
        });

        // Drops the documentation cookie. Anonymous and side-effect-free by construction: it can only
        // clear a cookie on the caller's own browser. The SPA's logout is otherwise purely client-side
        // (it just forgets the token), so without this the cookie would outlive the session by up to
        // the token's 12h.
        group.MapPost("/logout", (HttpContext http) =>
        {
            DocsAuthCookie.Delete(http);
            return Results.NoContent();
        }).AllowAnonymous();

        group.MapGet("/me", async (ClaimsPrincipal principal, IUserStoreFacade userStore) =>
        {
            var username = principal.Identity?.Name ?? "";
            var users = await userStore.GetUsersAsync();
            var user = users.FirstOrDefault(u => u.Username == username);
            return user is null
                ? Results.NotFound()
                : Results.Ok(new UserInfo(user.Username, user.DisplayName, user.Role, user.CreatedAtMs));
        }).RequireAuthorization("Viewer");
    }
}
