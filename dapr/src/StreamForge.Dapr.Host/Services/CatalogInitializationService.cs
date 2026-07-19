using Dapr.Actors;
using Dapr.Actors.Client;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Actors;

namespace StreamForge.Dapr.Host.Services;

/// <summary>
/// Plan 005 W4: Dapr counterpart of the Orleans host's <c>InitializeGrainsAsync</c> (Program.cs,
/// registered on <c>ApplicationStarted</c>) — calls the boot-only <c>EnsureInitializedAsync</c> on both
/// singleton actors once the app (and, in practice, the Dapr sidecar) is up. Seeds the demo
/// catalog/users on an empty Redis actor-state store; a no-op on a populated one (both actors'
/// EnsureInitializedAsync check Count == 0 first). Best-effort, matching the Orleans host's own
/// try/catch-and-log — no retry loop; a failure here just means the demo world isn't seeded yet, visible
/// immediately as empty catalogs on first login.
/// </summary>
public sealed class CatalogInitializationService(
    IHostApplicationLifetime lifetime,
    ILogger<CatalogInitializationService> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        lifetime.ApplicationStarted.Register(() => _ = InitializeAsync());
        return Task.CompletedTask;
    }

    private async Task InitializeAsync()
    {
        try
        {
            var users = ActorProxy.Create<IUserStoreActor>(new ActorId(StreamConstants.UsersKey), nameof(UserStoreActor), ActorProxyDefaults.Options);
            await users.EnsureInitializedAsync();

            var registry = ActorProxy.Create<IRegistryActor>(new ActorId(StreamConstants.RegistryKey), nameof(RegistryActor), ActorProxyDefaults.Options);
            await registry.EnsureInitializedAsync();

            logger.LogInformation("StreamForge.Dapr.Host: catalog/users actors initialized.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "StreamForge.Dapr.Host: actor initialization failed.");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
