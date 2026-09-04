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

        // Awaited BEFORE the source enumeration below, and that ordering is the whole point of this call.
        // Pinging a source ACTIVATES its connector grain, and ConnectorGrain.OnActivateAsync self-resumes:
        // an overdue timer fires an immediate poll. This sweep's first tick lands 15s after startup, which
        // on a slow boot is comfortably inside the window in which the registry has not yet resumed this
        // environment's pipelines and tables — so without this, the supervisor could be the thing that
        // makes a persisted-Running connector publish its first rows into streams NOBODY HAS SUBSCRIBED TO
        // YET. Memory streams have no replay, and a source with a dedup key never re-emits those rows, so
        // they are gone. RegistryGrain.EnsureInitializedAsync resumes consumers before producers and is
        // latched to once per activation, so after the first boot pass this is one cheap grain call per
        // environment per 15s.
        //
        // What this does NOT close (documented rather than pretended away): any OTHER caller can still
        // activate a connector during the boot window — GET /api/sources/{name}/status is enough — and
        // that activation self-resumes the same way. Ordering cannot fix that; the source-side replay ring
        // is what hands those rows to a table when it eventually attaches.
        await registry.EnsureInitializedAsync();

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
