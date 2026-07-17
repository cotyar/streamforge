using System.Security.Claims;
using Orleans;
using StreamForge.Abstractions;
using StreamForge.Host.Auth;

namespace StreamForge.Host.Api;

public static class AuthEndpoints
{
    public static void MapAuthEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/auth");

        group.MapPost("/login", async (LoginRequest req, IClusterClient client, JwtTokenService jwt) =>
        {
            var userStore = client.GetGrain<IUserStoreGrain>(StreamConstants.UsersKey);
            var user = await userStore.ValidateCredentialsAsync(req.Username, req.Password);
            if (user is null)
            {
                return Results.Unauthorized();
            }

            var token = jwt.CreateToken(user);
            return Results.Ok(new LoginResponse(token, user.Username, user.DisplayName, user.Role));
        });

        group.MapGet("/me", async (ClaimsPrincipal principal, IClusterClient client) =>
        {
            var username = principal.Identity?.Name ?? "";
            var userStore = client.GetGrain<IUserStoreGrain>(StreamConstants.UsersKey);
            var users = await userStore.GetUsersAsync();
            var user = users.FirstOrDefault(u => u.Username == username);
            return user is null
                ? Results.NotFound()
                : Results.Ok(new UserInfo(user.Username, user.DisplayName, user.Role, user.CreatedAtMs));
        }).RequireAuthorization("Viewer");
    }
}
