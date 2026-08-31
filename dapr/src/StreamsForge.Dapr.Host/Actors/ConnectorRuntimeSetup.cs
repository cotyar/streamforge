using Dapr.Actors.Runtime;
using StreamsForge.Abstractions;
using StreamsForge.Dapr.Host.Facades;

namespace StreamsForge.Dapr.Host.Actors;

/// <summary>
/// Plan 006 (ingestion connectors) W3-B seam, mirroring <see cref="GeneratorRuntimeSetup"/>: connector
/// runtime registration, called from Program.cs. <see cref="RegisterActors"/> registers
/// <see cref="ConnectorActor"/> with the Dapr actor runtime; <see cref="AddServices"/> registers
/// <see cref="Facades.DaprConnectorStatusFacade"/> as the real <see cref="IConnectorStatusFacade"/>.
///
/// <para><b>Why the facade registration lives here and not in <c>DaprFacadesExtensions.AddDaprFacades</c>
/// (Facades/DaprFacades.cs):</b> <see cref="Facades.DaprConnectorStatusFacade"/> resolves
/// <see cref="IConnectorActor"/> proxies, and <see cref="IConnectorActor"/>/<see cref="ConnectorActor"/>
/// are this wave's own scope — keeping <c>AddDaprFacades()</c> ignorant of the connector actor type (just
/// like it stays ignorant of <see cref="IGeneratorActor"/>/<see cref="GeneratorActor"/>, whose lifecycle
/// wiring lives in <see cref="GeneratorRuntimeSetup"/> instead) keeps that method a pure "facade =
/// singleton actor proxy adapter" registry, unaffected by which optional runtime wave-seams have been
/// wired up. Program.cs calls this AFTER <c>builder.Services.AddDaprFacades()</c> — no other
/// registration currently targets <see cref="IConnectorStatusFacade"/> (unlike
/// <see cref="Lifecycle.ILifecycleOrchestrator"/>, which has a Noop registered first for genuine
/// last-registration-wins override semantics), so this is a first-and-only registration rather than an
/// override — but it follows the same "wave-seam registers its own DI surface, after the shared
/// baseline" placement convention as every other <c>*RuntimeSetup.AddServices</c> in this project.</para>
/// </summary>
public static class ConnectorRuntimeSetup
{
    public static void RegisterActors(ActorRuntimeOptions options)
    {
        options.Actors.RegisterActor<ConnectorActor>();
    }

    public static void AddServices(IServiceCollection services)
    {
        services.AddSingleton<IConnectorStatusFacade, DaprConnectorStatusFacade>();
    }
}
