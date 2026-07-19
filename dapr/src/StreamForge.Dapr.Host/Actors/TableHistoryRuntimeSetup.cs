using Dapr.Actors.Runtime;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Facades;
using StreamForge.Dapr.Host.Streaming;

namespace StreamForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 W7-B seam, called from Program.cs (frozen during wave W7). Mirrors
/// <see cref="PipelineRuntimeSetup"/>'s own split: <see cref="RegisterActors"/> registers
/// <see cref="TableHistoryActor"/> with the Dapr actor runtime; <see cref="AddServices"/> registers the
/// real <see cref="ITableHistoryFacade"/> (last registration wins over
/// <c>Facades/DaprFacades.cs</c>'s <see cref="StubTableHistoryFacade"/> — Program.cs calls
/// <c>AddDaprFacades()</c> before this method), the shared
/// <see cref="TableHistoryEnabledMap"/> singleton, and <see cref="TableHistoryDeltaSink"/>'s
/// <see cref="ITableDeltaSink"/> registration (an ADDITIONAL registration alongside
/// <c>Streaming/StreamingRuntimeSetup.cs</c>'s own <see cref="DaprStreamBridge"/> one — see
/// <c>Streaming/Sinks.cs</c>'s class doc: "no change to this file [Sinks.cs] beyond what's already
/// declared", and no change to <c>StreamingRuntimeSetup.cs</c> either, since <c>IEnumerable&lt;T&gt;</c>
/// DI resolution fans out across every setup method that registers one, regardless of which file it's
/// registered from).
/// </summary>
public static class TableHistoryRuntimeSetup
{
    public static void RegisterActors(ActorRuntimeOptions options)
    {
        options.Actors.RegisterActor<TableHistoryActor>();
    }

    public static void AddServices(IServiceCollection services)
    {
        services.AddSingleton<ITableHistoryFacade, DaprTableHistoryFacade>();

        // See TableHistoryEnabledMap's own doc comment for why this registers the pre-existing static
        // Instance rather than letting the container construct its own — DaprLifecycleOrchestrator.History.cs
        // reads/writes TableHistoryEnabledMap.Instance directly (it can't take a constructor-injected
        // dependency this wave), so TableHistoryDeltaSink must be handed that SAME instance, not a second one.
        services.AddSingleton(TableHistoryEnabledMap.Instance);
        services.AddSingleton<ITableDeltaSink, TableHistoryDeltaSink>();
    }
}
