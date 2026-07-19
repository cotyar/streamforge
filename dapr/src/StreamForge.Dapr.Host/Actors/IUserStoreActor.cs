using Dapr.Actors;
using StreamForge.Abstractions;

namespace StreamForge.Dapr.Host.Actors;

public sealed record ValidateCredentialsRequest(string Username, string Password);

public sealed record CreateUserActorRequest(string Username, string DisplayName, string Role, string Password);

/// <summary>Null members leave the corresponding field unchanged — mirrors IUserStoreFacade.UpdateUserAsync.</summary>
public sealed record UpdateUserActorRequest(string Username, string? DisplayName, string? Role, string? Password);

/// <summary>
/// Actor-invocation surface for the "users" singleton actor (id = <see cref="StreamConstants.UsersKey"/>).
/// See <see cref="IRegistryActor"/>'s class doc for why this doesn't inherit
/// <see cref="IUserStoreFacade"/> directly (multi-argument facade members need a request record here);
/// <see cref="Facades.DaprUserStoreFacade"/> is the adapter. None of these methods can fail validation in
/// a way that needs an <see cref="ActorResult{T}"/> wrapper (mirrors Orleans' UserStoreGrain, which
/// signals failure via a bool return, never a thrown exception).
/// </summary>
public interface IUserStoreActor : IActor
{
    /// <summary>Seeds admin/editor/viewer on first activation (empty state). Idempotent.</summary>
    Task EnsureInitializedAsync();

    Task<UserRecord?> ValidateCredentialsAsync(ValidateCredentialsRequest request);
    Task<List<UserRecord>> GetUsersAsync();
    Task<bool> CreateUserAsync(CreateUserActorRequest request);
    Task<bool> UpdateUserAsync(UpdateUserActorRequest request);
    Task<bool> DeleteUserAsync(string username);
}
