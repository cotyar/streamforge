using Dapr.Actors.Runtime;
using Dapr.Client;
using StreamsForge.Dapr.Host.Lifecycle;
using StreamsForge.Dapr.Host.Services;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 W5-A seam: generator runtime registration, called from Program.cs (which is frozen during
/// wave W5 so the two parallel agents never edit it). <see cref="RegisterActors"/> registers
/// <see cref="GeneratorActor"/> with the Dapr actor runtime; <see cref="AddServices"/> registers a
/// <see cref="DaprClient"/> (needed by both <see cref="GeneratorActor"/> and
/// <see cref="DaprLifecycleOrchestrator"/> to publish), the generator supervisor hosted service, and
/// swaps <see cref="ILifecycleOrchestrator"/> for the real Dapr implementation — this registration runs
/// AFTER Program.cs's <see cref="NoopLifecycleOrchestrator"/> line, so it wins (last registration for a
/// given service type is what <c>IServiceProvider</c> resolves for a non-keyed singleton lookup).
/// </summary>
public static class GeneratorRuntimeSetup
{
    public static void RegisterActors(ActorRuntimeOptions options)
    {
        options.Actors.RegisterActor<GeneratorActor>();
    }

    public static void AddServices(IServiceCollection services)
    {
        services.AddDaprClient();
        services.AddSingleton<ILifecycleOrchestrator, DaprLifecycleOrchestrator>();
        services.AddHostedService<GeneratorSupervisorService>();
    }
}
