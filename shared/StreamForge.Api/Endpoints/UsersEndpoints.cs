using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;

namespace StreamForge.Api;

public static class UsersEndpoints
{
    public static void MapUsersEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/users").RequireAuthorization("Admin");

        group.MapGet("/", async (IUserStoreFacade userStore) =>
        {
            var users = await userStore.GetUsersAsync();
            return Results.Ok(users.Select(ToUserInfo).ToList());
        });

        group.MapPost("/", async (CreateUserRequest req, IUserStoreFacade userStore) =>
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
            return Results.Created($"/api/users/{user.Username}", ToUserInfo(user));
        });

        group.MapPut("/{username}", async (string username, UpdateUserRequest req, IUserStoreFacade userStore) =>
        {
            var updated = await userStore.UpdateUserAsync(username, req.DisplayName, req.Role, req.Password);
            if (!updated)
            {
                return Results.NotFound();
            }

            var users = await userStore.GetUsersAsync();
            var user = users.First(u => u.Username == username);
            return Results.Ok(ToUserInfo(user));
        });

        group.MapDelete("/{username}", async (string username, ClaimsPrincipal principal, IUserStoreFacade userStore) =>
        {
            if (string.Equals(principal.Identity?.Name, username, StringComparison.Ordinal))
            {
                return Results.BadRequest(new ErrorResponse("cannot delete yourself"));
            }

            var removed = await userStore.DeleteUserAsync(username);
            return removed ? Results.NoContent() : Results.NotFound();
        });
    }

    private static UserInfo ToUserInfo(UserRecord u) => new(u.Username, u.DisplayName, u.Role, u.CreatedAtMs);
}
