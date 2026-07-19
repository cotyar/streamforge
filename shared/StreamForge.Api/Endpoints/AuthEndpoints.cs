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

        group.MapPost("/login", async (LoginRequest req, IUserStoreFacade userStore, JwtTokenService jwt) =>
        {
            var user = await userStore.ValidateCredentialsAsync(req.Username, req.Password);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            var token = jwt.CreateToken(user);
            return Results.Ok(new LoginResponse(token, user.Username, user.DisplayName, user.Role));
        });

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
