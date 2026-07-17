using Orleans;
using StreamForge.Abstractions;

namespace StreamForge.Host.Services;

/// <summary>Every 15s, pings enabled generators to keep them activated (or reactivate them if evicted).</summary>
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
                        await client.GetGrain<IGeneratorGrain>(src.Name).PingAsync();
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
