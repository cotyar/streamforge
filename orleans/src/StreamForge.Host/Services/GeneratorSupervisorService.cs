using Orleans;
using StreamForge.Abstractions;
using StreamForge.AppCore.Environments;
using StreamForge.Host.Facades;

namespace StreamForge.Host.Services;

/// <summary>Every 15s, pings enabled generators AND connector-kind sources (plan 006 D-C) to keep them
/// activated (or reactivate them if evicted).
///
/// <para><b>Plan 021 environment strategy: ITERATES EVERY ENVIRONMENT</b> (re-listed every tick, so a
/// newly created environment is picked up within one 15s cycle) rather than reading an ambient — this
/// service runs on a timer, outside any request, where the ambient is always empty (see
/// <c>EnvironmentAmbient</c>'s own doc on why background work must never read it). <b>Known, deliberate
/// gap this wave leaves</b>: <c>IGeneratorGrain</c>/<c>IConnectorGrain</c> are still keyed by bare source
/// name (D3's qualification of the 50 name-keyed grain kinds is a later wave's scope), so two environments
/// that happen to share a source name ping the SAME physical grain twice per tick — redundant but harmless
/// (PingAsync is idempotent), and no worse than what a single shared catalog already did before this
/// plan.</para></summary>
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
                var environments = await client.GetGrain<IEnvironmentRegistryGrain>(StreamConstants.EnvironmentsKey).ListAsync();
                foreach (var env in environments)
                {
                    await PingEnvironmentAsync(EnvKeys.Normalize(env.Name));
                }
            }
            catch
            {
                // best-effort; try again next tick
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    private async Task PingEnvironmentAsync(string env)
    {
        var registry = client.RegistryFor(env);
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
}
