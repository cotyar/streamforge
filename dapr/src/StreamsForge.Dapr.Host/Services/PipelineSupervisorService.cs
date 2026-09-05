using Dapr.Actors;
using Dapr.Actors.Client;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Environments;
using StreamsForge.Dapr.Host.Actors;
using StreamsForge.Dapr.Host.Facades;
using StreamsForge.Dapr.Host.Streaming;

namespace StreamsForge.Dapr.Host.Services;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W6: Dapr counterpart of
/// <see cref="GeneratorSupervisorService"/>'s own "boot resume" role — every ~15s, lists pipelines via
/// <see cref="ICatalogFacade"/> and ensures every catalog-<c>Running</c> one's <see cref="PipelineActor"/>
/// is actually processing, mirroring Orleans' own one-shot
/// <c>RegistryGrain.EnsureInitializedAsync</c> boot-resume loop but as a periodic sweep — same rationale
/// as <see cref="GeneratorSupervisorService"/>'s own doc comment: actor-proxy calls fail until the Dapr
/// sidecar is up, and the natural sweep period is the backoff, so a boot-time resume attempt that races
/// sidecar startup simply retries next tick instead of leaving a seeded "Running" pipeline silently inert
/// forever.
///
/// <para><b>Deliberately NOT a blind "always restart" sweep (unlike <see cref="GeneratorSupervisorService"/>
/// calling <c>GeneratorActor.StartAsync</c> unconditionally every tick).</b> Restarting an
/// ALREADY-running <see cref="PipelineActor"/> discards its in-flight window/join state and (via a fresh
/// <c>PipelineExecutor</c>) would visibly disrupt a pipeline that's been running fine for hours, every
/// ~15s — a regression Orleans never has (its own boot-resume loop runs exactly once). So this sweep
/// checks <see cref="IPipelineActor.IsRunningAsync"/> first:
/// <list type="bullet">
/// <item>NOT running (first-ever start, or a transient failure that left it never actually started) →
/// go through <see cref="ICatalogFacade.SetPipelineStatusAsync"/> (id, Running) — the exact same code
/// path <c>POST /api/pipelines/{id}/start</c> uses, so compiling, starting the actor, registering
/// <see cref="PipelineEventRouter"/>, and persisting Failed/Running/Error bookkeeping on failure all
/// happen identically whether triggered by a user or by this sweep.</item>
/// <item>Already running (self-healed via <see cref="PipelineActor.OnActivateAsync"/> on some earlier
/// reactivation this process never orchestrated, e.g. right after a host restart) → the actor's own
/// executor/timer are already fine; only <see cref="PipelineEventRouter"/>'s in-memory routing table
/// needs repair (it does NOT survive a host restart the way the actor's persisted Dapr state does), read
/// cheaply via <see cref="IPipelineActor.GetSourceNamesAsync"/> with no recompile.</item>
/// </list></para>
///
/// <para><b>Plan 025 (PARITY.md D6 bullet 2): this sweep is no longer the boot resume — it waits for the
/// one that is.</b> The paragraph above still describes what this class does from its SECOND tick onward;
/// what changed is the first. <see cref="CatalogInitializationService"/> now runs one ordered pass
/// (pipelines → tables in dependency order → sources) across every environment, and this loop awaits
/// <see cref="BootGate"/> before sweeping, so it can no longer race the table or source sweep into starting
/// a producer before its consumers are routable. The two branches themselves moved verbatim into
/// <see cref="EntityResume.EnsurePipelineRunningAsync"/>, shared with that boot pass so the "repair, never
/// restart" discipline cannot drift between the two callers.</para>
/// </summary>
public sealed class PipelineSupervisorService(
    ICatalogFacadeFactory catalogFactory,
    IEnvironmentFacade environments,
    PipelineEventRouter router,
    IHostApplicationLifetime lifetime,
    ILogger<PipelineSupervisorService> logger) : BackgroundService
{
    // Plan 021 D5: same reasoning as GeneratorSupervisorService — a boot-resume sweep must cover every
    // environment, never just the (empty, here) ambient one.
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await WaitForApplicationStartedAsync(stoppingToken);
        await BootGateWait.AwaitBootPassAsync(logger, nameof(PipelineSupervisorService), stoppingToken);

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        do
        {
            try
            {
                foreach (var env in await environments.ListAsync())
                {
                    var catalog = catalogFactory.For(EnvKeys.Normalize(env.Name));
                    var pipelines = await catalog.GetPipelinesAsync();
                    foreach (var pipeline in pipelines.Where(p => p.Status == PipelineStatus.Running))
                    {
                        try
                        {
                            await EntityResume.EnsurePipelineRunningAsync(catalog, router, pipeline);
                        }
                        catch (Exception ex)
                        {
                            // Best-effort per pipeline, mirroring GeneratorSupervisorService's own per-source
                            // try/catch — one misbehaving pipeline must never stop the sweep from reaching
                            // the rest.
                            logger.LogDebug(ex,
                                "PipelineSupervisorService: failed to (re)start/repair pipeline '{PipelineId}' — will retry next sweep.",
                                pipeline.Id);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex,
                    "PipelineSupervisorService: sweep failed (Dapr sidecar likely not ready yet) — will retry next tick.");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    /// <summary>Small inline equivalent of the Orleans host's internal <c>StartupSignal</c> helper — see
    /// <see cref="GeneratorSupervisorService"/>'s identical private method for the same rationale (that
    /// type is internal to a different assembly).</summary>
    private async Task WaitForApplicationStartedAsync(CancellationToken ct)
    {
        if (lifetime.ApplicationStarted.IsCancellationRequested)
        {
            return;
        }

        var tcs = new TaskCompletionSource();
        await using var registration = lifetime.ApplicationStarted.Register(() => tcs.TrySetResult());
        await tcs.Task.WaitAsync(ct);
    }
}
