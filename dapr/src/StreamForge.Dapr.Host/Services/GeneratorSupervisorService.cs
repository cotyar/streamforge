using Dapr.Actors;
using Dapr.Actors.Client;
using StreamForge.Abstractions;
using StreamForge.Dapr.Host.Actors;
using StreamForge.Dapr.Host.Lifecycle;

namespace StreamForge.Dapr.Host.Services;

/// <summary>
/// Plan 005 (Dapr sibling runtime) W5-A: Dapr counterpart of the Orleans host's
/// <c>GeneratorSupervisorService</c> (orleans/src/StreamForge.Host/Services/GeneratorSupervisorService.cs)
/// — every ~15s, lists sources via <see cref="ICatalogFacade"/> and ensures every enabled one's
/// <see cref="GeneratorActor"/> has a live timer, by calling <c>StartAsync</c> with its current
/// definition (idempotent — see that method's own doc comment).
///
/// <para><b>Why this is still needed even though GeneratorActor self-heals</b> (see that class's own
/// class doc): self-healing only fires when an actor that was PREVIOUSLY started gets reactivated. It
/// does nothing for a source that has never been started at all — e.g. one enabled directly in a fresh
/// seed, or one whose very first <c>StartAsync</c> call (via
/// <see cref="Lifecycle.DaprLifecycleOrchestrator"/> from <c>CatalogStore.UpsertSourceAsync</c>) happened
/// to fail transiently (sidecar hiccup). This sweep is that safety net, exactly mirroring the Orleans
/// side's own doc comment ("keep them activated (or reactivate them if evicted)").</para>
///
/// <para><b>Sidecar readiness:</b> actor proxy calls (both <see cref="ICatalogFacade"/>, which is itself
/// backed by an actor proxy on this flavor, and the per-source <see cref="IGeneratorActor"/> proxies
/// created below) fail until the Dapr sidecar is up. Rather than a bespoke retry-with-backoff loop, this
/// mirrors <c>CatalogInitializationService</c>'s own best-effort philosophy (try, log on failure, don't
/// crash the host) — the natural ~15s sweep period already IS the backoff: a sweep that fails because the
/// sidecar isn't ready yet simply tries again next tick, with no separate startup gate required beyond
/// waiting for <see cref="IHostApplicationLifetime.ApplicationStarted"/> before the first sweep.</para>
///
/// <para><b>Plan 006 (ingestion connectors) W3-B addendum — connector-kind sources join the same sweep,
/// but with a not-running GUARD the generator branch doesn't need.</b> <see cref="IGeneratorActor.StartAsync"/>
/// is safe to call unconditionally every ~15s (it just re-arms a fixed 200ms cadence timer — see that
/// class's own doc comment), but <see cref="IConnectorActor.StartAsync"/> is a genuine "fresh start" that
/// resets the failure streak and reschedules from "now" (see <see cref="ConnectorActor.StartAsync"/>'s doc
/// comment) — calling it unconditionally on a source whose sweep-15s period is SHORTER than its own poll
/// schedule would perpetually reset/restart the connector before its own timer ever gets to fire,
/// defeating both its schedule and any in-progress backoff. So this sweep first checks
/// <see cref="IConnectorActor.IsRunningAsync"/> and only calls <c>StartAsync</c> when it answers false —
/// exactly the "Start enabled connector-kind sources whose actor isn't running" the plan calls for. Which
/// sources are eligible for this connector branch at all (enabled AND non-generator-kind) is the pure,
/// separately unit-tested <see cref="ConnectorSourceSweep.SelectConnectorSources"/> — see
/// dapr/tests/StreamForge.Dapr.Tests/ConnectorSupervisorSweepTests.cs.</para>
/// </summary>
public sealed class GeneratorSupervisorService(
    ICatalogFacade catalog,
    IHostApplicationLifetime lifetime,
    ILogger<GeneratorSupervisorService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await WaitForApplicationStartedAsync(stoppingToken);

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        do
        {
            try
            {
                var sources = await catalog.GetSourcesAsync();

                foreach (var src in sources.Where(s => s.Enabled && SourceKindDispatch.Classify(s.Kind) == SourceKindDispatch.ActorKind.Generator))
                {
                    try
                    {
                        var actor = ActorProxy.Create<IGeneratorActor>(
                            new ActorId(src.Name), nameof(GeneratorActor), ActorProxyDefaults.Options);
                        await actor.StartAsync(src);
                    }
                    catch (Exception ex)
                    {
                        // Best-effort per source, mirroring the Orleans supervisor's per-grain try/catch —
                        // one misbehaving generator must never stop the sweep from reaching the rest.
                        logger.LogDebug(ex,
                            "GeneratorSupervisorService: failed to (re)start generator for source '{Source}' — will retry next sweep.",
                            src.Name);
                    }
                }

                foreach (var src in ConnectorSourceSweep.SelectConnectorSources(sources))
                {
                    try
                    {
                        var actor = ActorProxy.Create<IConnectorActor>(
                            new ActorId(src.Name), nameof(ConnectorActor), ActorProxyDefaults.Options);
                        if (!await actor.IsRunningAsync())
                        {
                            await actor.StartAsync(src);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogDebug(ex,
                            "GeneratorSupervisorService: failed to (re)start connector for source '{Source}' — will retry next sweep.",
                            src.Name);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex,
                    "GeneratorSupervisorService: sweep failed (Dapr sidecar likely not ready yet) — will retry next tick.");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    /// <summary>Small inline equivalent of the Orleans host's internal <c>StartupSignal</c> helper
    /// (orleans/src/StreamForge.Host/Services/StartupSignal.cs) — that type is internal to a different
    /// assembly (StreamForge.Host), so it isn't reachable from here; this is the same handful of lines
    /// rather than a shared abstraction for one four-line helper.</summary>
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

/// <summary>
/// Plan 006 (ingestion connectors) W3-B: the pure "which sources does the connector sweep branch even
/// consider" filter behind <see cref="GeneratorSupervisorService"/>'s connector loop — extracted so it is
/// unit-testable without any actor/Dapr-sidecar machinery (the actual <c>IsRunningAsync</c>/
/// <c>StartAsync</c> calls per eligible source are inherently I/O and stay in the service itself, exactly
/// as <see cref="ConnectorBookkeeping"/> and <see cref="SourceKindDispatch"/> keep their own pure slices
/// separate from their actor-bound callers). See
/// dapr/tests/StreamForge.Dapr.Tests/ConnectorSupervisorSweepTests.cs.
/// </summary>
public static class ConnectorSourceSweep
{
    /// <summary>Enabled, non-generator-kind sources — the set <see cref="GeneratorSupervisorService"/>'s
    /// connector branch checks <see cref="IConnectorActor.IsRunningAsync"/> for and conditionally
    /// <see cref="IConnectorActor.StartAsync"/>s.</summary>
    public static IEnumerable<SourceDefinition> SelectConnectorSources(IEnumerable<SourceDefinition> sources) =>
        sources.Where(s => s.Enabled && SourceKindDispatch.Classify(s.Kind) == SourceKindDispatch.ActorKind.Connector);
}
