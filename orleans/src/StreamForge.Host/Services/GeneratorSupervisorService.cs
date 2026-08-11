using Orleans;
using StreamForge.Abstractions;

namespace StreamForge.Host.Services;

/// <summary>Every 15s, pings enabled generators AND connector-kind sources (plan 006 D-C) to keep them
/// activated (or reactivate them if evicted).</summary>
public sealed class GeneratorSupervisorService(IClusterClient client, IHostApplicationLifetime lifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await StartupSignal.WaitForApplicationStartedAsync(lifetime, stoppingToken);

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        do
        {
            try
            {
                var registry = client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
                var sources = await registry.GetSourcesAsync();
                foreach (var src in sources.Where(s => s.Enabled))
                {
                    try
                    {
                        // Plan 006 D-C / plan 008 W4 / plan 009 wave D: Kind dispatch via the shared
                        // SourceKindDispatch.Classify, mirroring RegistryGrain's UpsertSourceAsync/
                        // EnsureInitializedAsync three-way split — Generator (or unset) pings
                        // IGeneratorGrain, Ingest backs onto no grain at all (rows arrive through
                        // IIngressFacade — nothing to ping), Connector pings IConnectorGrain.
                        switch (SourceKindDispatch.Classify(src.Kind))
                        {
                            case SourceKindDispatch.ActorKind.Generator:
                                await client.GetGrain<IGeneratorGrain>(src.Name).PingAsync();
                                break;
                            case SourceKindDispatch.ActorKind.Connector:
                                await client.GetGrain<IConnectorGrain>(src.Name).PingAsync();
                                break;
                        }
                    }
                    catch
                    {
                        // best-effort; try again next tick
                    }
                }
            }
            catch
            {
                // best-effort; try again next tick
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
