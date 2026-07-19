using Dapr.Actors.Runtime;
using StreamForge.Dapr.Host.Services;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W6 seam: pipeline runtime registration, called from Program.cs.
/// Mirrors <see cref="GeneratorRuntimeSetup"/>'s own split: <see cref="RegisterActors"/> registers
/// <see cref="PipelineActor"/> with the Dapr actor runtime; <see cref="AddServices"/> registers the
/// pipeline supervisor hosted service (boot resume — see <see cref="Services.PipelineSupervisorService"/>'s
/// own doc comment). <see cref="Streaming.PipelineEventRouter"/> and the real
/// <see cref="Facades.DaprPipelineReadFacade"/> are registered elsewhere
/// (<c>Streaming/StreamingRuntimeSetup.cs</c> and <c>Facades/DaprFacades.cs</c> respectively) since both
/// already own their own registration surface this wave.
/// </summary>
public static class PipelineRuntimeSetup
{
    public static void RegisterActors(ActorRuntimeOptions options)
    {
        options.Actors.RegisterActor<PipelineActor>();
    }

    public static void AddServices(IServiceCollection services)
    {
        services.AddHostedService<PipelineSupervisorService>();
    }
}
