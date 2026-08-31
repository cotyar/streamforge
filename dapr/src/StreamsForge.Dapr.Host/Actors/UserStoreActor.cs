using Dapr.Actors.Runtime;
using StreamsForge.Abstractions;
using StreamsForge.AppCore;
using StreamsForge.Host.Auth;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>Persisted shape of the "users" actor state — mirrors Orleans' <c>UserStoreState</c>
/// (orleans/src/StreamsForge.Host/Grains/UserStoreGrain.cs).</summary>
public sealed class UserStoreState
{
    public List<UserRecord> Users { get; set; } = [];
}

/// <summary>
/// Plan 005 W4: Dapr counterpart of Orleans' <c>UserStoreGrain</c> — singleton actor, id =
/// <see cref="StreamConstants.UsersKey"/> ("users"). Same PBKDF2 credential store (shared
/// <see cref="PasswordHasher"/>), seeded from the same shared <see cref="SeedCatalog.Users"/>. Small
/// enough that (unlike RegistryActor/CatalogStore) the logic lives directly in the actor rather than a
/// separate pure class — there is no branching/validation here worth isolating for unit testing beyond
/// what <see cref="PasswordHasher"/>'s own tests (ported alongside JsonValueNormalizer's) already cover.
/// </summary>
public sealed class UserStoreActor(ActorHost host) : Actor(host), IUserStoreActor
{
    private const string StateName = "users";

    private UserStoreState _state = new();

    protected override async Task OnActivateAsync()
    {
        var existing = await StateManager.TryGetStateAsync<UserStoreState>(StateName);
        _state = existing.HasValue ? existing.Value : new UserStoreState();
    }

    private Task SaveAsync() => StateManager.SetStateAsync(StateName, _state);

    public async Task EnsureInitializedAsync()
    {
        if (_state.Users.Count > 0)
        {
            return;
        }

        foreach (var seed in SeedCatalog.Users)
        {
            AddUser(seed.Username, seed.DisplayName, seed.Role, seed.Password);
        }

        await SaveAsync();
    }

    private void AddUser(string username, string displayName, string role, string password)
    {
        var (hash, salt) = PasswordHasher.Hash(password);
        _state.Users.Add(new UserRecord
        {
            Username = username,
            DisplayName = displayName,
            Role = role,
            PasswordHash = hash,
            PasswordSalt = salt,
            CreatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        });
    }

    public Task<UserRecord?> ValidateCredentialsAsync(ValidateCredentialsRequest request)
    {
        var user = _state.Users.FirstOrDefault(u => u.Username == request.Username);
        if (user is null || !PasswordHasher.Verify(request.Password, user.PasswordHash, user.PasswordSalt))
        {
            return Task.FromResult<UserRecord?>(null);
        }

        return Task.FromResult<UserRecord?>(user);
    }

    public Task<List<UserRecord>> GetUsersAsync() => Task.FromResult(_state.Users.ToList());

    public async Task<bool> CreateUserAsync(CreateUserActorRequest request)
    {
        if (_state.Users.Any(u => u.Username == request.Username))
        {
            return false;
        }

        AddUser(request.Username, request.DisplayName, request.Role, request.Password);
        await SaveAsync();
        return true;
    }

    public async Task<bool> UpdateUserAsync(UpdateUserActorRequest request)
    {
        var user = _state.Users.FirstOrDefault(u => u.Username == request.Username);
        if (user is null)
        {
            return false;
        }

        if (request.DisplayName is not null)
        {
            user.DisplayName = request.DisplayName;
        }

        if (request.Role is not null)
        {
            user.Role = request.Role;
        }

        if (request.Password is not null)
        {
            var (hash, salt) = PasswordHasher.Hash(request.Password);
            user.PasswordHash = hash;
            user.PasswordSalt = salt;
        }

        await SaveAsync();
        return true;
    }

    public async Task<bool> DeleteUserAsync(string username)
    {
        var removed = _state.Users.RemoveAll(u => u.Username == username) > 0;
        if (removed)
        {
            await SaveAsync();
        }

        return removed;
    }
}
