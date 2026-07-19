using Dapr.Actors.Runtime;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 W5-A seam: generator runtime registration, called from Program.cs (which is frozen during
/// wave W5 so the two parallel agents never edit it). W5-A fills these in: RegisterActors adds
/// GeneratorActor; AddServices registers the generator supervisor hosted service and swaps
/// ILifecycleOrchestrator for the real Dapr implementation (registrations here run AFTER Program.cs's
/// NoopLifecycleOrchestrator line, so the last-registered implementation wins).
/// </summary>
public static class GeneratorRuntimeSetup
{
    public static void RegisterActors(ActorRuntimeOptions options)
    {
        // ponytail: W5-A fills this (GeneratorActor registration).
    }

    public static void AddServices(IServiceCollection services)
    {
        // ponytail: W5-A fills this (supervisor hosted service + DaprLifecycleOrchestrator).
    }
}
