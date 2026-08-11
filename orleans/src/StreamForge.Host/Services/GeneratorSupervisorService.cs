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
                        // Plan 006 D-C / plan 008 W4: Kind dispatch, mirroring RegistryGrain's
                        // UpsertSourceAsync/EnsureInitializedAsync three-way split — "generator" (or
                        // unset) pings IGeneratorGrain, "ingest" backs onto no grain at all (rows arrive
                        // through IIngressFacade — nothing to ping), everything else pings IConnectorGrain.
                        if (string.IsNullOrEmpty(src.Kind) || src.Kind == SourceKinds.Generator)
                        {
                            await client.GetGrain<IGeneratorGrain>(src.Name).PingAsync();
                        }
                        else if (src.Kind != SourceKinds.Ingest)
                        {
                            await client.GetGrain<IConnectorGrain>(src.Name).PingAsync();
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
