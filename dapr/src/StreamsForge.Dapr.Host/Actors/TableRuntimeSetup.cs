using Dapr.Actors.Runtime;
using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Facades;
using StreamsForge.Dapr.Host.Services;
using StreamsForge.Dapr.Host.Streaming;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W7-A seam, called from Program.cs (frozen during wave W7).
/// <see cref="RegisterActors"/> registers <see cref="TableActor"/> with the Dapr actor runtime;
/// <see cref="AddServices"/> registers:
/// <list type="bullet">
/// <item><see cref="TableEventRouter"/> as a singleton, forwarded to <see cref="ISourceEventsSink"/>,
/// <see cref="ITableDeltaSink"/> AND (plan 025, table-over-pipeline) <see cref="IPipelineResultsSink"/> —
/// see that class's own doc comment for why registration lives here rather than in
/// <c>Streaming/StreamingRuntimeSetup.cs</c> (this wave's own owned-file boundary;
/// <c>IEnumerable&lt;T&gt;</c> DI resolution picks up singletons regardless of which file calls
/// <c>AddSingleton</c>, and Program.cs already calls this method AFTER
/// <c>StreamingRuntimeSetup.AddServices</c>).</item>
/// <item>The real <see cref="ITableReadFacade"/> (<see cref="DaprTableReadFacade"/>) — registered here,
/// AFTER <c>Facades.DaprFacadesExtensions.AddDaprFacades</c>'s stub registration (Program.cs calls this
/// method after that one), so this last registration wins per ASP.NET Core DI's singleton
/// resolution rule. Per the wave brief: do NOT edit <c>Facades/StubFacades.cs</c> or
/// <c>Facades/DaprFacades.cs</c> to make this swap — this registration alone is sufficient.</item>
/// <item><see cref="TableSupervisorService"/> — the boot-resume sweep (see that class's own doc
/// comment).</item>
/// </list>
/// </summary>
public static class TableRuntimeSetup
{
    public static void RegisterActors(ActorRuntimeOptions options)
    {
        options.Actors.RegisterActor<TableActor>();
    }

    public static void AddServices(IServiceCollection services)
    {
        services.AddSingleton<TableEventRouter>();
        services.AddSingleton<ISourceEventsSink>(sp => sp.GetRequiredService<TableEventRouter>());
        services.AddSingleton<ITableDeltaSink>(sp => sp.GetRequiredService<TableEventRouter>());
        // Plan 025 (table-over-pipeline, PARITY.md D6): TableEventRouter also fans out sf-pipeline-out —
        // see that class's own doc comment for why a third routing table lives on the same singleton
        // rather than a separate router type.
        services.AddSingleton<IPipelineResultsSink>(sp => sp.GetRequiredService<TableEventRouter>());

        services.AddSingleton<ITableReadFacade, DaprTableReadFacade>();

        services.AddHostedService<TableSupervisorService>();
    }
}
