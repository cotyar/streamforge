using Dapr.Actors;
using StreamsForge.Abstractions;

namespace StreamsForge.Dapr.Host.Actors;

public sealed record CreateEnvironmentRequest(string Name, string Description, string CreatedBy);

public sealed record DeleteEnvironmentRequest(string Name, bool Force);

/// <summary>
/// Actor-invocation surface for the "environments" singleton actor (id =
/// <c>StreamConstants.EnvironmentsKey</c>, itself never environment-qualified — see that constant's own
/// doc comment). Same reason <see cref="IRegistryActor"/> is not simply <c>ICatalogFacade</c> under a
/// different name: a Dapr actor method takes at most one parameter, so <see cref="CreateAsync"/>/
/// <see cref="DeleteAsync"/> pack their arguments into <see cref="CreateEnvironmentRequest"/>/
/// <see cref="DeleteEnvironmentRequest"/> — <see cref="Facades.DaprEnvironmentFacade"/> is the adapter
/// translating each <see cref="IEnvironmentFacade"/> call into one of these and unwrapping
/// <see cref="ActorResult{T}"/> back into a return value or a thrown exception, mirroring
/// <see cref="Facades.DaprCatalogFacade"/>'s own shape.
/// </summary>
public interface IEnvironmentRegistryActor : IActor
{
    Task<List<EnvironmentRecord>> ListAsync();

    Task<bool> ExistsAsync(string name);

    Task<ActorResult<EnvironmentRecord>> CreateAsync(CreateEnvironmentRequest request);

    Task<ActorResult<bool>> DeleteAsync(DeleteEnvironmentRequest request);
}
