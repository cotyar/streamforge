using Dapr.Actors;
using Dapr.Actors.Client;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
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
///
/// <para><b>Plan 021:</b> the registry actor addressed below is <c>StreamConstants.RegistryKey</c>
/// UNQUALIFIED — i.e. always the DEFAULT environment's catalog (<see cref="EnvKeys.Qualify"/> is a no-op
/// for it). Seeding intentionally does not iterate <see cref="IEnvironmentFacade"/>: there is nothing to
/// seed in a named environment (nobody has created one yet at boot, and D7 says creation is deliberate,
/// never implicit), so "seed the default catalog, exactly as before this plan" is not a simplification —
/// it is the whole of what seeding is supposed to do. The boot-RESUME sweep for a named environment's
/// already-existing, already-Running entities is a different job, done by the four
/// <c>Services.*SupervisorService</c> classes, which DO iterate every environment (see each one's own doc
/// comment for why).</para>
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
