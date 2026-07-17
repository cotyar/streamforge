using Orleans;
using Orleans.Runtime;
using StreamForge.Abstractions;
using StreamForge.Host.Auth;

namespace StreamForge.Host.Grains;

public sealed class UserStoreState
{
    public List<UserRecord> Users { get; set; } = [];
}

/// <summary>Singleton grain (key = StreamConstants.UsersKey). Seeds admin/editor/viewer on first run.</summary>
public sealed class UserStoreGrain(
    [PersistentState("users", StreamConstants.StorageName)] IPersistentState<UserStoreState> state)
    : Grain, IUserStoreGrain
{
    public async Task EnsureInitializedAsync()
    {
        if (state.State.Users.Count > 0)
        {
            return;
        }

        AddUser("admin", "Administrator", "Admin", "admin123!");
        AddUser("editor", "Editor", "Editor", "editor123!");
        AddUser("viewer", "Viewer", "Viewer", "viewer123!");
        await state.WriteStateAsync();
    }

    private void AddUser(string username, string displayName, string role, string password)
    {
        var (hash, salt) = PasswordHasher.Hash(password);
        state.State.Users.Add(new UserRecord
        {
            Username = username,
            DisplayName = displayName,
            Role = role,
            PasswordHash = hash,
            PasswordSalt = salt,
            CreatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        });
    }

    public Task<UserRecord?> ValidateCredentialsAsync(string username, string password)
    {
        var user = state.State.Users.FirstOrDefault(u => u.Username == username);
        if (user is null || !PasswordHasher.Verify(password, user.PasswordHash, user.PasswordSalt))
        {
            return Task.FromResult<UserRecord?>(null);
        }

        return Task.FromResult<UserRecord?>(user);
    }

    public Task<List<UserRecord>> GetUsersAsync() => Task.FromResult(state.State.Users.ToList());

    public async Task<bool> CreateUserAsync(string username, string displayName, string role, string password)
    {
        if (state.State.Users.Any(u => u.Username == username))
        {
            return false;
        }

        AddUser(username, displayName, role, password);
        await state.WriteStateAsync();
        return true;
    }

    public async Task<bool> UpdateUserAsync(string username, string? displayName, string? role, string? password)
    {
        var user = state.State.Users.FirstOrDefault(u => u.Username == username);
        if (user is null)
        {
            return false;
        }

        if (displayName is not null)
        {
            user.DisplayName = displayName;
        }

        if (role is not null)
        {
            user.Role = role;
        }

        if (password is not null)
        {
            var (hash, salt) = PasswordHasher.Hash(password);
            user.PasswordHash = hash;
            user.PasswordSalt = salt;
        }

        await state.WriteStateAsync();
        return true;
    }

    public async Task<bool> DeleteUserAsync(string username)
    {
        var removed = state.State.Users.RemoveAll(u => u.Username == username) > 0;
        if (removed)
        {
            await state.WriteStateAsync();
        }

        return removed;
    }
}
