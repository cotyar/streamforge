using Orleans;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Host.Facades;

namespace StreamsForge.Host.Services;

/// <summary>Every 15s, pings enabled generators AND connector-kind sources (plan 006 D-C) to keep them
/// activated (or reactivate them if evicted).
///
/// <para><b>Plan 021 environment strategy: ITERATES EVERY ENVIRONMENT</b> (re-listed every tick, so a
/// newly created environment is picked up within one 15s cycle) rather than reading an ambient — this
/// service runs on a timer, outside any request, where the ambient is always empty (see
/// <c>EnvironmentAmbient</c>'s own doc on why background work must never read it). <c>IGeneratorGrain</c>/
/// <c>IConnectorGrain</c> are D3-qualified with the environment being iterated (<c>PingEnvironmentAsync</c>'s
/// own <c>env</c> parameter, not the ambient), so two environments sharing a source name now ping two
/// distinct grains, closing the gap wave 1 left open here.</para></summary>
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
                // Plan 006 D-C / plan 008 W4 / plan 009 wave D / plan 020 wave B-2: Kind dispatch via the
                // shared SourceKindDispatch.Classify, mirroring RegistryGrain's UpsertSourceAsync/
                // EnsureInitializedAsync split — Generator (or unset) pings IGeneratorGrain, Ingest backs
                // onto no grain at all (rows arrive through IIngressFacade — nothing to ping), Crdt pings
                // ICrdtDocGrain (D3 — never IConnectorGrain), Connector pings IConnectorGrain.
                switch (SourceKindDispatch.Classify(src.Kind))
                {
                    case SourceKindDispatch.ActorKind.Generator:
                        await client.GetGrain<IGeneratorGrain>(EnvKeys.Qualify(env, src.Name)).PingAsync();
                        break;
                    case SourceKindDispatch.ActorKind.Connector:
                        await client.GetGrain<IConnectorGrain>(EnvKeys.Qualify(env, src.Name)).PingAsync();
                        break;
                    case SourceKindDispatch.ActorKind.Crdt:
                        await client.GetGrain<ICrdtDocGrain>(EnvKeys.Qualify(env, src.Name)).PingAsync();
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
