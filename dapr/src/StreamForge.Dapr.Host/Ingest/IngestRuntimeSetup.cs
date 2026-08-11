using StreamForge.Abstractions;
using StreamForge.AppCore.Ingest;

namespace StreamForge.Dapr.Host.Ingest;

/// <summary>
/// Plan 008 W4c seam, mirroring <see cref="Actors.ConnectorRuntimeSetup"/>: client-push ingress runtime
/// registration, called from Program.cs. Registers <see cref="SourceIngressRegistry"/> (the
/// host-process singleton every ingest-kind source's buffer lives in — see that class's own doc
/// comment), the real <see cref="IIngressFacade"/>, and <see cref="IngestDrainPumpService"/>, which
/// periodically drains whatever <see cref="DaprIngressFacade"/> admits.
///
/// <para>No actor is registered here — deliberately: see <see cref="IIngressFacade"/>'s class doc
/// ("implementations call it directly rather than through a grain/actor") and
/// <see cref="DaprIngressFacade"/>'s own doc comment for why.</para>
/// </summary>
public static class IngestRuntimeSetup
{
    public static void AddServices(IServiceCollection services)
    {
        services.AddSingleton<SourceIngressRegistry>();
        services.AddSingleton<IIngressFacade, DaprIngressFacade>();
        services.AddHostedService<IngestDrainPumpService>();
    }
}
